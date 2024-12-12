package customcost

import (
	"fmt"

	"github.com/opencost/opencost/core/pkg/filter"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/mapper"
)

func ParseCustomCostTotalRequest(qp mapper.PrimitiveMap) (*CostTotalRequest, error) {
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
	aggregateBy, err := ParseCustomCostProperties(aggregateByRaw)
	if err != nil {
		return nil, err
	}

	accumulate := opencost.ParseAccumulate(qp.Get("accumulate", "day"))

	var filter filter.Filter
	filterString := qp.Get("filter", "")
	if filterString != "" {
		parser := NewCustomCostFilterParser()
		filter, err = parser.Parse(filterString)
		if err != nil {
			return nil, fmt.Errorf("parsing 'filter' parameter: %s", err)
		}
	}

	costTypeStr := qp.Get("costType", string(CostTypeBlended))
	parsedCostType, err := ParseCostType(costTypeStr)
	if err != nil {
		return nil, fmt.Errorf("parsing 'costType' parameter: %s", err)
	}

	sortByStr := qp.Get("sortBy", string(SortPropertyCost))
	parsedSortBy, err := ParseSortBy(sortByStr)
	if err != nil {
		return nil, fmt.Errorf("parsing 'sortBy' parameter: %s", err)
	}

	sortDirStr := qp.Get("sortDirection", string(SortDirectionDesc))
	parsedSortDir, err := ParseSortDirection(sortDirStr)
	if err != nil {
		return nil, fmt.Errorf("parsing 'sortDirection' parameter: %s", err)
	}

	opts := &CostTotalRequest{
		Start:         *window.Start(),
		End:           *window.End(),
		AggregateBy:   aggregateBy,
		Accumulate:    accumulate,
		Filter:        filter,
		CostType:      parsedCostType,
		SortBy:        parsedSortBy,
		SortDirection: parsedSortDir,
	}

	return opts, nil
}

func ParseSortDirection(sortDirStr string) (SortDirection, error) {
	switch sortDirStr {
	case string(SortDirectionAsc):
		return SortDirectionAsc, nil
	case string(SortDirectionDesc):
		return SortDirectionDesc, nil
	default:
		return "", fmt.Errorf("unrecognized sortDirection field: %s", sortDirStr)
	}
}

func ParseSortBy(sortByStr string) (SortProperty, error) {
	switch sortByStr {
	case string(SortPropertyCost):
		return SortPropertyCost, nil
	case string(SortPropertyAggregate):
		return SortPropertyAggregate, nil
	case string(SortPropertyCostType):
		return SortPropertyCostType, nil
	default:
		return "", fmt.Errorf("unrecognized sortBy field: %s", sortByStr)
	}
}
func ParseCustomCostTimeseriesRequest(qp mapper.PrimitiveMap) (*CostTimeseriesRequest, error) {
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
	aggregateBy, err := ParseCustomCostProperties(aggregateByRaw)
	if err != nil {
		return nil, err
	}

	accumulate := opencost.ParseAccumulate(qp.Get("accumulate", "day"))

	var filter filter.Filter
	filterString := qp.Get("filter", "")
	if filterString != "" {
		parser := NewCustomCostFilterParser()
		filter, err = parser.Parse(filterString)
		if err != nil {
			return nil, fmt.Errorf("parsing 'filter' parameter: %s", err)
		}
	}
	costTypeStr := qp.Get("costType", string(CostTypeBlended))
	parsedCostType, err := ParseCostType(costTypeStr)
	if err != nil {
		return nil, fmt.Errorf("parsing 'costType' parameter: %s", err)
	}

	sortByStr := qp.Get("sortBy", string(SortPropertyCost))
	parsedSortBy, err := ParseSortBy(sortByStr)
	if err != nil {
		return nil, fmt.Errorf("parsing 'sortBy' parameter: %s", err)
	}

	sortDirStr := qp.Get("sortDirection", string(SortDirectionDesc))
	parsedSortDir, err := ParseSortDirection(sortDirStr)
	if err != nil {
		return nil, fmt.Errorf("parsing 'sortDirection' parameter: %s", err)
	}

	opts := &CostTimeseriesRequest{
		Start:         *window.Start(),
		End:           *window.End(),
		AggregateBy:   aggregateBy,
		Accumulate:    accumulate,
		Filter:        filter,
		CostType:      parsedCostType,
		SortBy:        parsedSortBy,
		SortDirection: parsedSortDir,
	}

	return opts, nil
}
