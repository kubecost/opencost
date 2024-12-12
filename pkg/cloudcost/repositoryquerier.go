package cloudcost

import (
	"context"
	"fmt"
	"sort"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
)

// RepositoryQuerier is an implementation of Querier and ViewQuerier which pulls directly from a Repository
type RepositoryQuerier struct {
	repo Repository
}

func NewRepositoryQuerier(repo Repository) *RepositoryQuerier {
	return &RepositoryQuerier{repo: repo}
}

func (rq *RepositoryQuerier) Query(ctx context.Context, request QueryRequest) (*opencost.CloudCostSetRange, error) {
	repoKeys, err := rq.repo.Keys()
	if err != nil {
		return nil, fmt.Errorf("RepositoryQuerier: Query: failed to get list of keys from repository: %w", err)
	}

	// create filter
	compiler := opencost.NewCloudCostMatchCompiler()
	matcher, err := compiler.Compile(request.Filter)
	if err != nil {
		return nil, fmt.Errorf("RepositoryQuerier: Query: failed to compile filters: %w", err)
	}

	// Create a Cloud Cost Set Range in the resolution of the repository
	ccsr, err := opencost.NewCloudCostSetRange(request.Start, request.End, opencost.AccumulateOptionDay, "")
	if err != nil {
		return nil, fmt.Errorf("RepositoryQuerier: Query: failed to create Cloud Cost Set Range: %w", err)
	}
	for _, cloudCostSet := range ccsr.CloudCostSets {
		// Setting this values creates
		cloudCostSet.AggregationProperties = request.AggregateBy
		for _, key := range repoKeys {
			ccs, err := rq.repo.Get(cloudCostSet.Window.Start().UTC(), key)
			if err != nil {
				log.Errorf("RepositoryQuerier: Query: %s", err.Error())
				continue
			}
			if ccs == nil {
				continue
			}

			for _, cc := range ccs.CloudCosts {
				if matcher.Matches(cc) {
					cloudCostSet.Insert(cc)
				}
			}
		}
	}

	if request.Accumulate != opencost.AccumulateOptionNone {
		ccsr, err = ccsr.Accumulate(request.Accumulate)
		if err != nil {
			return nil, fmt.Errorf("RepositoryQuerier: Query: error accumulating: %w", err)
		}
	}

	return ccsr, nil
}

func (rq *RepositoryQuerier) QueryViewGraph(ctx context.Context, request ViewQueryRequest) (ViewGraphData, error) {
	ccasr, err := rq.Query(ctx, request.QueryRequest)
	if err != nil {
		return nil, fmt.Errorf("QueryViewGraph: query failed: %w", err)
	}

	if ccasr.IsEmpty() {
		return make([]*ViewGraphDataSet, 0), nil
	}
	var sets ViewGraphData
	for _, ccas := range ccasr.CloudCostSets {
		items := make([]ViewGraphDataSetItem, 0)

		for key, cc := range ccas.CloudCosts {
			costMetric, err := cc.GetCostMetric(request.CostMetricName)
			if err != nil {
				return nil, fmt.Errorf("QueryViewGraph: failed to get cost metric: %w", err)
			}
			items = append(items, ViewGraphDataSetItem{
				Name:  key,
				Value: costMetric.Cost,
			})
		}
		sort.SliceStable(items, func(i, j int) bool {
			return items[i].Value > items[j].Value
		})

		if len(items) > request.ChartItemsLength {
			otherItems := items[request.ChartItemsLength:]
			newItems := items[:request.ChartItemsLength]
			// Rename last item other and add all other values into it
			newItems[request.ChartItemsLength-1].Name = "Other"
			for _, item := range otherItems {
				newItems[request.ChartItemsLength-1].Value += item.Value
			}
			items = newItems
		}

		sets = append(sets, &ViewGraphDataSet{
			Start: *ccas.Window.Start(),
			End:   *ccas.Window.End(),
			Items: items,
		})
	}
	return sets, nil
}

func (rq *RepositoryQuerier) QueryViewTotals(ctx context.Context, request ViewQueryRequest) (*ViewTotals, error) {
	ccasr, err := rq.Query(ctx, request.QueryRequest)
	if err != nil {
		return nil, fmt.Errorf("QueryViewTotals: query failed: %w", err)
	}
	acc, err := ccasr.AccumulateAll()
	if err != nil {
		return nil, fmt.Errorf("QueryViewTotals: accumulate failed: %w", err)
	}
	if acc.IsEmpty() {
		return nil, nil
	}
	count := len(acc.CloudCosts)

	total, err := acc.Aggregate([]string{})
	if err != nil {
		return nil, fmt.Errorf("QueryViewTotals: aggregate total failed: %w", err)
	}

	if total.IsEmpty() {
		return nil, fmt.Errorf("QueryViewTotals: missing total: %w", err)
	}

	if len(total.CloudCosts) != 1 {
		return nil, fmt.Errorf("QueryViewTotals: total did not aggregate: %w", err)
	}

	cm, err := total.CloudCosts[""].GetCostMetric(request.CostMetricName)
	if err != nil {
		return nil, fmt.Errorf("QueryViewTotals: failed to retrieve cost metric: %w", err)
	}
	return &ViewTotals{
		NumResults: count,
		Combined: &ViewTableRow{
			Name:              "Totals",
			KubernetesPercent: cm.KubernetesPercent,
			Cost:              cm.Cost,
		},
	}, nil
}

func (rq *RepositoryQuerier) QueryViewTable(ctx context.Context, request ViewQueryRequest) (ViewTableRows, error) {
	ccasr, err := rq.Query(ctx, request.QueryRequest)
	if err != nil {
		return nil, fmt.Errorf("QueryViewTable: query failed: %w", err)
	}
	acc, err := ccasr.AccumulateAll()
	if err != nil {
		return nil, fmt.Errorf("QueryViewTable: accumulate failed: %w", err)
	}

	var rows ViewTableRows
	for key, cloudCost := range acc.CloudCosts {
		costMetric, err2 := cloudCost.GetCostMetric(request.CostMetricName)
		if err2 != nil {
			return nil, fmt.Errorf("QueryViewTable: failed to retrieve cost metric: %w", err)
		}
		var labels map[string]string
		if cloudCost.Properties != nil {
			labels = cloudCost.Properties.Labels
		}

		vtr := &ViewTableRow{
			Name:              key,
			Labels:            labels,
			KubernetesPercent: costMetric.KubernetesPercent,
			Cost:              costMetric.Cost,
		}
		rows = append(rows, vtr)
	}
	// Sort Results

	// Sort by Name to ensure consistent return
	sort.SliceStable(rows, func(i, j int) bool {
		return rows[i].Name > rows[j].Name
	})

	switch request.SortColumn {
	case SortFieldName:
		if request.SortDirection == SortDirectionAscending {
			sort.SliceStable(rows, func(i, j int) bool {
				return rows[i].Name < rows[j].Name
			})
		}

	case SortFieldCost:
		if request.SortDirection == SortDirectionAscending {
			sort.SliceStable(rows, func(i, j int) bool {
				return rows[i].Cost < rows[j].Cost
			})
		} else {
			sort.SliceStable(rows, func(i, j int) bool {
				return rows[i].Cost > rows[j].Cost
			})
		}
	case SortFieldKubernetesPercent:
		if request.SortDirection == SortDirectionAscending {
			sort.SliceStable(rows, func(i, j int) bool {
				return rows[i].KubernetesPercent < rows[j].KubernetesPercent
			})
		} else {
			sort.SliceStable(rows, func(i, j int) bool {
				return rows[i].KubernetesPercent > rows[j].KubernetesPercent
			})
		}

	default:
		return nil, fmt.Errorf("invalid sort field '%s'", string(request.SortColumn))
	}

	// paginate sorted results
	if request.Offset > len(rows) {
		return make([]*ViewTableRow, 0), nil
	}

	if request.Limit > 0 {
		limit := request.Offset + request.Limit
		if limit > len(rows) {
			return rows[request.Offset:], nil
		}
		return rows[request.Offset:limit], nil
	}

	if request.Offset > 0 {
		return rows[request.Offset:], nil
	}

	return rows, nil
}
