package cloudcost

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/filter"
	"github.com/opencost/opencost/core/pkg/opencost"
)

// Querier allows for querying ranges of CloudCost data
type Querier interface {
	Query(context.Context, QueryRequest) (*opencost.CloudCostSetRange, error)
}

type QueryRequest struct {
	Start       time.Time
	End         time.Time
	AggregateBy []string
	Accumulate  opencost.AccumulateOption
	Filter      filter.Filter
}

// DefaultChartItemsLength the default max number of items for a ViewGraphDataSet
const DefaultChartItemsLength int = 10

// ViewQuerier defines a contract for return View types to the QueryService to service the View Api
type ViewQuerier interface {
	QueryViewGraph(context.Context, ViewQueryRequest) (ViewGraphData, error)
	QueryViewTotals(context.Context, ViewQueryRequest) (*ViewTotals, error)
	QueryViewTable(context.Context, ViewQueryRequest) (ViewTableRows, error)
}

type ViewQueryRequest struct {
	QueryRequest
	CostMetricName   opencost.CostMetricName
	ChartItemsLength int
	Offset           int
	Limit            int
	SortDirection    SortDirection
	SortColumn       SortField
}

// SortDirection a string type that acts as an enumeration of possible request options
type SortDirection string

const (
	SortDirectionNone       SortDirection = ""
	SortDirectionAscending  SortDirection = "asc"
	SortDirectionDescending SortDirection = "desc"
)

// ParseSortDirection provides a resilient way to parse one of the enumerated SortDirection types from a string
// or throws an error if it is not able to.
func ParseSortDirection(sortDirection string) (SortDirection, error) {
	switch strings.ToLower(sortDirection) {
	case strings.ToLower(string(SortDirectionAscending)):
		return SortDirectionAscending, nil
	case strings.ToLower(string(SortDirectionDescending)):
		return SortDirectionDescending, nil
	}
	return SortDirectionNone, fmt.Errorf("failed to parse a valid CostMetricName from '%s'", sortDirection)
}

// SortField a string type that acts as an enumeration of possible request options
type SortField string

const (
	SortFieldNone              SortField = ""
	SortFieldName              SortField = "name"
	SortFieldCost              SortField = "cost"
	SortFieldKubernetesPercent SortField = "kubernetesPercent"
)

// ParseSortField provides a resilient way to parse one of the enumerated SortField types from a string
// or throws an error if it is not able to.
func ParseSortField(sortColumn string) (SortField, error) {
	switch strings.ToLower(sortColumn) {
	case strings.ToLower(string(SortFieldName)):
		return SortFieldName, nil
	case strings.ToLower(string(SortFieldCost)):
		return SortFieldCost, nil
	case strings.ToLower(string(SortFieldKubernetesPercent)):
		return SortFieldKubernetesPercent, nil
	}
	return SortFieldNone, fmt.Errorf("failed to parse a valid CostMetricName from '%s'", sortColumn)
}
