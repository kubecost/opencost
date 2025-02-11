package prom

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/util"
)

var (
	// Static Warnings for data point parsing
	InfWarning warning = newWarning("Found Inf value parsing vector data point for metric")
	NaNWarning warning = newWarning("Found NaN value parsing vector data point for metric")
)

func DataFieldFormatErr(query string) error {
	return fmt.Errorf("Data field improperly formatted in prometheus response fetching query '%s'", query)
}

func DataPointFormatErr(query string) error {
	return fmt.Errorf("Improperly formatted datapoint from Prometheus fetching query '%s'", query)
}

func MetricFieldDoesNotExistErr(query string) error {
	return fmt.Errorf("Metric field does not exist in data result vector fetching query '%s'", query)
}

func MetricFieldFormatErr(query string) error {
	return fmt.Errorf("Metric field is improperly formatted fetching query '%s'", query)
}

func NoDataErr(query string) error {
	return source.NewNoDataError(query)
}

func PromUnexpectedResponseErr(query string) error {
	return fmt.Errorf("Unexpected response from Prometheus fetching query '%s'", query)
}

func QueryResultNilErr(query string) error {
	return source.NewCommError(query)
}

func ResultFieldDoesNotExistErr(query string) error {
	return fmt.Errorf("Result field not does not exist in prometheus response fetching query '%s'", query)
}

func ResultFieldFormatErr(query string) error {
	return fmt.Errorf("Result field improperly formatted in prometheus response fetching query '%s'", query)
}

func ResultFormatErr(query string) error {
	return fmt.Errorf("Result is improperly formatted fetching query '%s'", query)
}

func ValueFieldDoesNotExistErr(query string) error {
	return fmt.Errorf("Value field does not exist in data result vector fetching query '%s'", query)
}

func ValueFieldFormatErr(query string) error {
	return fmt.Errorf("Values field is improperly formatted fetching query '%s'", query)
}

// NewQueryResultError returns a QueryResults object with an error set and does not parse a result.
func NewQueryResultError(query string, err error) *source.QueryResults {
	qrs := source.NewQueryResults(query)
	qrs.Error = err
	return qrs
}

// NewQueryResults accepts the raw prometheus query result and returns an array of
// QueryResult objects
func NewQueryResults(query string, queryResult interface{}, resultKeys *source.ResultKeys) *source.QueryResults {
	qrs := source.NewQueryResults(query)

	if queryResult == nil {
		qrs.Error = QueryResultNilErr(query)
		return qrs
	}

	data, ok := queryResult.(map[string]interface{})["data"]
	if !ok {
		e, err := wrapPrometheusError(query, queryResult)
		if err != nil {
			qrs.Error = err
			return qrs
		}
		qrs.Error = fmt.Errorf("%s", e)
		return qrs
	}

	// Deep Check for proper formatting
	d, ok := data.(map[string]interface{})
	if !ok {
		qrs.Error = DataFieldFormatErr(query)
		return qrs
	}
	resultData, ok := d["result"]
	if !ok {
		qrs.Error = ResultFieldDoesNotExistErr(query)
		return qrs
	}
	resultsData, ok := resultData.([]interface{})
	if !ok {
		qrs.Error = ResultFieldFormatErr(query)
		return qrs
	}

	// Result vectors from the query
	var results []*source.QueryResult

	// Parse raw results and into QueryResults
	for _, val := range resultsData {
		resultInterface, ok := val.(map[string]interface{})
		if !ok {
			qrs.Error = ResultFormatErr(query)
			return qrs
		}

		metricInterface, ok := resultInterface["metric"]
		if !ok {
			qrs.Error = MetricFieldDoesNotExistErr(query)
			return qrs
		}
		metricMap, ok := metricInterface.(map[string]interface{})
		if !ok {
			qrs.Error = MetricFieldFormatErr(query)
			return qrs
		}

		// Define label string for values to ensure that we only run labelsForMetric once
		// if we receive multiple warnings.
		var labelString string = ""

		// Determine if the result is a ranged data set or single value
		_, isRange := resultInterface["values"]

		var vectors []*util.Vector
		if !isRange {
			dataPoint, ok := resultInterface["value"]
			if !ok {
				qrs.Error = ValueFieldDoesNotExistErr(query)
				return qrs
			}

			// Append new data point, log warnings
			v, warn, err := parseDataPoint(query, dataPoint)
			if err != nil {
				qrs.Error = err
				return qrs
			}
			if warn != nil {
				log.DedupedWarningf(5, "%s\nQuery: %s\nLabels: %s", warn.Message(), query, labelsForMetric(metricMap))
			}

			vectors = append(vectors, v)
		} else {
			values, ok := resultInterface["values"].([]interface{})
			if !ok {
				qrs.Error = fmt.Errorf("Values field is improperly formatted")
				return qrs
			}

			// Append new data points, log warnings
			for _, value := range values {
				v, warn, err := parseDataPoint(query, value)
				if err != nil {
					qrs.Error = err
					return qrs
				}
				if warn != nil {
					if labelString == "" {
						labelString = labelsForMetric(metricMap)
					}
					log.DedupedWarningf(5, "%s\nQuery: %s\nLabels: %s", warn.Message(), query, labelString)
				}

				vectors = append(vectors, v)
			}
		}

		results = append(results, source.NewQueryResult(metricMap, vectors, resultKeys))
	}

	qrs.Results = results
	return qrs
}

// parseDataPoint parses a data point from raw prometheus query results and returns
// a new Vector instance containing the parsed data along with any warnings or errors.
func parseDataPoint(query string, dataPoint interface{}) (*util.Vector, warning, error) {
	var w warning = nil

	value, ok := dataPoint.([]interface{})
	if !ok || len(value) != 2 {
		return nil, w, DataPointFormatErr(query)
	}

	strVal := value[1].(string)
	v, err := strconv.ParseFloat(strVal, 64)
	if err != nil {
		return nil, w, err
	}

	// Test for +Inf and -Inf (sign: 0), Test for NaN
	if math.IsInf(v, 0) {
		w = InfWarning
		v = 0.0
	} else if math.IsNaN(v) {
		w = NaNWarning
		v = 0.0
	}

	return &util.Vector{
		Timestamp: math.Round(value[0].(float64)/10) * 10,
		Value:     v,
	}, w, nil
}

func labelsForMetric(metricMap map[string]interface{}) string {
	var pairs []string
	for k, v := range metricMap {
		pairs = append(pairs, fmt.Sprintf("%s: %+v", k, v))
	}

	return fmt.Sprintf("{%s}", strings.Join(pairs, ", "))
}

func wrapPrometheusError(query string, qr interface{}) (string, error) {
	e, ok := qr.(map[string]interface{})["error"]
	if !ok {
		return "", PromUnexpectedResponseErr(query)
	}
	eStr, ok := e.(string)
	return fmt.Sprintf("'%s' parsing query '%s'", eStr, query), nil
}
