package prom

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util"
)

var (
	// Static Warnings for data point parsing
	InfWarning warning = newWarning("Found Inf value parsing vector data point for metric")
	NaNWarning warning = newWarning("Found NaN value parsing vector data point for metric")
)

func DataFieldFormatErr(query string, promResponse interface{}) error {
	return fmt.Errorf("Error parsing Prometheus response: 'data' field improperly formatted. Query: '%s'. Response: '%+v'", query, promResponse)
}

func DataPointFormatErr(query string, promResponse interface{}) error {
	return fmt.Errorf("Error parsing Prometheus response: improperly formatted datapoint. Query: '%s'. Response: '%+v'", query, promResponse)
}

func MetricFieldDoesNotExistErr(query string, promResponse interface{}) error {
	return fmt.Errorf("Error parsing Prometheus response: 'metric' field does not exist in data result vector. Query: '%s'. Response: '%+v'", query, promResponse)
}

func MetricFieldFormatErr(query string, promResponse interface{}) error {
	return fmt.Errorf("Error parsing Prometheus response: 'metric' field improperly formatted. Query: '%s'. Response: '%+v'", query, promResponse)
}

func NoDataErr(query string) error {
	return NewNoDataError(query)
}

func PromUnexpectedResponseErr(query string, promResponse interface{}) error {
	return fmt.Errorf("Error parsing Prometheus response: unexpected response. Query: '%s'. Response: '%+v'", query, promResponse)
}

func QueryResultNilErr(query string) error {
	return NewCommError(query)
}

func ResultFieldDoesNotExistErr(query string, promResponse interface{}) error {
	return fmt.Errorf("Error parsing Prometheus response: 'result' field does not exist. Query: '%s'. Response: '%+v'", query, promResponse)
}

func ResultFieldFormatErr(query string, promResponse interface{}) error {
	return fmt.Errorf("Error parsing Prometheus response: 'result' field improperly formatted. Query: '%s'. Response: '%+v'", query, promResponse)
}

func ResultFormatErr(query string, promResponse interface{}) error {
	return fmt.Errorf("Error parsing Prometheus response: 'result' field improperly formatted. Query: '%s'. Response: '%+v'", query, promResponse)
}

func ValueFieldDoesNotExistErr(query string, promResponse interface{}) error {
	return fmt.Errorf("Error parsing Prometheus response: 'value' field does not exist in data result vector. Query: '%s'. Response: '%+v'", query, promResponse)
}

// QueryResultsChan is a channel of query results
type QueryResultsChan chan *QueryResults

// Await returns query results, blocking until they are made available, and
// deferring the closure of the underlying channel
func (qrc QueryResultsChan) Await() ([]*QueryResult, error) {
	defer close(qrc)

	results := <-qrc
	if results.Error != nil {
		return nil, results.Error
	}

	return results.Results, nil
}

// QueryResults contains all of the query results and the source query string.
type QueryResults struct {
	Query   string
	Error   error
	Results []*QueryResult
}

// QueryResult contains a single result from a prometheus query. It's common
// to refer to query results as a slice of QueryResult
type QueryResult struct {
	Metric map[string]interface{} `json:"metric"`
	Values []*util.Vector         `json:"values"`
}

// NewQueryResults accepts the raw prometheus query result and returns an array of
// QueryResult objects
func NewQueryResults(query string, queryResult interface{}) *QueryResults {
	qrs := &QueryResults{Query: query}

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
		qrs.Error = fmt.Errorf(e)
		return qrs
	}

	// Deep Check for proper formatting
	d, ok := data.(map[string]interface{})
	if !ok {
		qrs.Error = DataFieldFormatErr(query, data)
		return qrs
	}
	resultData, ok := d["result"]
	if !ok {
		qrs.Error = ResultFieldDoesNotExistErr(query, d)
		return qrs
	}
	resultsData, ok := resultData.([]interface{})
	if !ok {
		qrs.Error = ResultFieldFormatErr(query, resultData)
		return qrs
	}

	// Result vectors from the query
	var results []*QueryResult

	// Parse raw results and into QueryResults
	for _, val := range resultsData {
		resultInterface, ok := val.(map[string]interface{})
		if !ok {
			qrs.Error = ResultFormatErr(query, val)
			return qrs
		}

		metricInterface, ok := resultInterface["metric"]
		if !ok {
			qrs.Error = MetricFieldDoesNotExistErr(query, resultInterface)
			return qrs
		}
		metricMap, ok := metricInterface.(map[string]interface{})
		if !ok {
			qrs.Error = MetricFieldFormatErr(query, metricInterface)
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
				qrs.Error = ValueFieldDoesNotExistErr(query, resultInterface)
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

		results = append(results, &QueryResult{
			Metric: metricMap,
			Values: vectors,
		})
	}

	qrs.Results = results
	return qrs
}

// GetString returns the requested field, or an error if it does not exist
func (qr *QueryResult) GetString(field string) (string, error) {
	f, ok := qr.Metric[field]
	if !ok {
		return "", fmt.Errorf("'%s' field does not exist in data result vector", field)
	}

	strField, ok := f.(string)
	if !ok {
		return "", fmt.Errorf("'%s' field is improperly formatted and cannot be converted to string", field)
	}

	return strField, nil
}

// GetStrings returns the requested fields, or an error if it does not exist
func (qr *QueryResult) GetStrings(fields ...string) (map[string]string, error) {
	values := map[string]string{}

	for _, field := range fields {
		f, ok := qr.Metric[field]
		if !ok {
			return nil, fmt.Errorf("'%s' field does not exist in data result vector", field)
		}

		value, ok := f.(string)
		if !ok {
			return nil, fmt.Errorf("'%s' field is improperly formatted and cannot be converted to string", field)
		}

		values[field] = value
	}

	return values, nil
}

// GetLabels returns all labels and their values from the query result
func (qr *QueryResult) GetLabels() map[string]string {
	result := make(map[string]string)

	// Find All keys with prefix label_, remove prefix, add to labels
	for k, v := range qr.Metric {
		if !strings.HasPrefix(k, "label_") {
			continue
		}

		label := strings.TrimPrefix(k, "label_")
		value, ok := v.(string)
		if !ok {
			log.Warnf("Failed to parse label value for label: '%s'", label)
			continue
		}

		result[label] = value
	}

	return result
}

// GetAnnotations returns all annotations and their values from the query result
func (qr *QueryResult) GetAnnotations() map[string]string {
	result := make(map[string]string)

	// Find All keys with prefix annotation_, remove prefix, add to annotations
	for k, v := range qr.Metric {
		if !strings.HasPrefix(k, "annotation_") {
			continue
		}

		annotations := strings.TrimPrefix(k, "annotation_")
		value, ok := v.(string)
		if !ok {
			log.Warnf("Failed to parse label value for label: '%s'", annotations)
			continue
		}

		result[annotations] = value
	}

	return result
}

// parseDataPoint parses a data point from raw prometheus query results and returns
// a new Vector instance containing the parsed data along with any warnings or errors.
func parseDataPoint(query string, dataPoint interface{}) (*util.Vector, warning, error) {
	var w warning = nil

	value, ok := dataPoint.([]interface{})
	if !ok || len(value) != 2 {
		return nil, w, DataPointFormatErr(query, dataPoint)
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
		return "", PromUnexpectedResponseErr(query, qr)
	}
	eStr, ok := e.(string)
	return fmt.Sprintf("'%s' parsing query '%s'", eStr, query), nil
}
