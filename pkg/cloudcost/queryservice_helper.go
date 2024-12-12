package cloudcost

import (
	"encoding/csv"
	"fmt"
	"net/http"

	"github.com/opencost/opencost/core/pkg/filter"
	"github.com/opencost/opencost/core/pkg/filter/cloudcost"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/httputil"
)

func ParseCloudCostRequest(qp httputil.QueryParams) (*QueryRequest, error) {

	windowStr := qp.Get("window", "")
	if windowStr == "" {
		return nil, fmt.Errorf("missing require window param")
	}

	window, err := opencost.ParseWindowUTC(windowStr)
	if err != nil {
		return nil, fmt.Errorf("invalid window parameter: %w", err)
	}
	if window.IsOpen() {
		return nil, fmt.Errorf("invalid window parameter: %s", window.String())
	}

	aggregateByRaw := qp.GetList("aggregate", ",")
	var aggregateBy []string
	for _, aggBy := range aggregateByRaw {
		prop, err := opencost.ParseCloudCostProperty(aggBy)
		if err != nil {
			return nil, fmt.Errorf("error parsing aggregate by %v", err)
		}
		aggregateBy = append(aggregateBy, string(prop))
	}

	// if we're aggregating by nothing (aka `item` on the frontend) then aggregate by all
	if len(aggregateBy) == 0 {
		aggregateBy = []string{opencost.CloudCostInvoiceEntityIDProp, opencost.CloudCostAccountIDProp, opencost.CloudCostProviderProp, opencost.CloudCostProviderIDProp, opencost.CloudCostCategoryProp, opencost.CloudCostServiceProp}
	}

	accumulate := opencost.ParseAccumulate(qp.Get("accumulate", ""))

	var filter filter.Filter
	filterString := qp.Get("filter", "")
	if filterString != "" {
		parser := cloudcost.NewCloudCostFilterParser()
		filter, err = parser.Parse(filterString)
		if err != nil {
			return nil, fmt.Errorf("Parsing 'filter' parameter: %s", err)
		}
	}

	opts := &QueryRequest{
		Start:       *window.Start(),
		End:         *window.End(),
		AggregateBy: aggregateBy,
		Accumulate:  accumulate,
		Filter:      filter,
	}

	return opts, nil
}

func ParseCloudCostViewRequest(qp httputil.QueryParams) (*ViewQueryRequest, error) {
	qr, err := ParseCloudCostRequest(qp)
	if err != nil {
		return nil, err
	}

	// parse cost metric
	costMetricName, err := opencost.ParseCostMetricName(qp.Get("costMetric", string(opencost.CostMetricAmortizedNetCost)))
	if err != nil {
		return nil, fmt.Errorf("error parsing 'costMetric': %w", err)
	}

	limit := qp.GetInt("limit", 0)
	if limit < 0 {
		return nil, fmt.Errorf("invalid value for limit %d", limit)
	}
	offset := qp.GetInt("offset", 0)
	if offset < 0 {
		return nil, fmt.Errorf("invalid value for offset %d", offset)
	}

	// parse order
	order, err := ParseSortDirection(qp.Get("sortByOrder", "desc"))
	if err != nil {
		return nil, fmt.Errorf("error parsing 'sortByOrder: %w", err)
	}

	sortColumn, err := ParseSortField(qp.Get("sortBy", "cost"))
	if err != nil {
		return nil, fmt.Errorf("error parsing 'sortBy': %w", err)
	}

	return &ViewQueryRequest{
		QueryRequest:     *qr,
		CostMetricName:   costMetricName,
		ChartItemsLength: DefaultChartItemsLength,
		Limit:            limit,
		Offset:           offset,
		SortDirection:    order,
		SortColumn:       sortColumn,
	}, nil
}

// CloudCostViewTableRowsToCSV takes the csv writer and writes the ViewTableRows into the writer.
func CloudCostViewTableRowsToCSV(writer *csv.Writer, ctr ViewTableRows, window string) error {
	defer writer.Flush()
	// Write the column headers
	headers := []string{
		"Name",
		"K8s Utilization",
		"Total",
		"Window",
	}
	err := writer.Write(headers)
	if err != nil {
		return fmt.Errorf("CloudCostViewTableRowsToCSV: failed to convert ViewTableRows to csv with error: %w", err)
	}

	// Write one row per entry in the ViewTableRows
	for _, row := range ctr {
		err = writer.Write([]string{
			row.Name,
			fmt.Sprintf("%.3f", row.KubernetesPercent),
			fmt.Sprintf("%.3f", row.Cost),
			window,
		})
		if err != nil {
			return fmt.Errorf("CloudCostViewTableRowsToCSV: failed to convert ViewTableRows to csv with error: %w", err)
		}
	}

	return nil
}

func writeCloudCostViewTableRowsAsCSV(w http.ResponseWriter, ctr ViewTableRows, window string) {
	writer := csv.NewWriter(w)

	err := CloudCostViewTableRowsToCSV(writer, ctr, window)
	if err != nil {
		protocol.WriteError(w, protocol.InternalServerError(err.Error()))
		return
	}
}
