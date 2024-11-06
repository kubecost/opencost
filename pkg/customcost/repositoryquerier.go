package customcost

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
)

type RepositoryQuerier struct {
	hourlyRepo     Repository
	dailyRepo      Repository
	hourlyDuration time.Duration
	dailyDuration  time.Duration
}

func NewRepositoryQuerier(hourlyRepo, dailyRepo Repository, hourlyDuration, dailyDuration time.Duration) *RepositoryQuerier {
	return &RepositoryQuerier{
		hourlyRepo:     hourlyRepo,
		dailyRepo:      dailyRepo,
		hourlyDuration: hourlyDuration,
		dailyDuration:  dailyDuration,
	}
}

func (rq *RepositoryQuerier) QueryTotal(ctx context.Context, request CostTotalRequest) (*CostResponse, error) {
	window := opencost.NewClosedWindow(request.Start, request.End)
	window, accumulate, err := GetCustomCostWindowAccumulation(window, request.Accumulate)
	if err != nil {
		return nil, fmt.Errorf("error getting custom cost total window accumulation: %w", err)
	}

	repo := rq.dailyRepo
	step := timeutil.Day
	if accumulate == opencost.AccumulateOptionHour {
		repo = rq.hourlyRepo
		step = time.Hour
	}
	domains, err := repo.Keys()
	if err != nil {
		return nil, fmt.Errorf("QueryTotal: %w", err)
	}

	compiler := NewCustomCostMatchCompiler()
	matcher, err := compiler.Compile(request.Filter)
	if err != nil {
		return nil, fmt.Errorf("RepositoryQuerier: Query: failed to compile filters: %w", err)
	}

	ccs := NewCustomCostSet(window)
	queryStart := *window.Start()
	for queryStart.Before(*window.End()) {
		queryEnd := queryStart.Add(step)

		for _, domain := range domains {
			ccResponse, err := repo.Get(queryStart, domain)
			if err != nil {
				return nil, fmt.Errorf("QueryTotal: %w", err)
			} else if ccResponse == nil || ccResponse.Start == nil || ccResponse.End == nil {
				continue
			}

			customCosts := ParseCustomCostResponse(ccResponse, request.CostType)
			for _, customCost := range customCosts {
				if matcher.Matches(customCost) {
					ccs.Add(customCost)
				}
			}
		}

		queryStart = queryEnd
	}

	err = ccs.Aggregate(request.AggregateBy)
	if err != nil {
		return nil, err
	}

	ccs.Sort(request.SortBy, request.SortDirection)
	return NewCostResponse(ccs, request.CostType), nil
}

func (rq *RepositoryQuerier) QueryTimeseries(ctx context.Context, request CostTimeseriesRequest) (*CostTimeseriesResponse, error) {
	window := opencost.NewClosedWindow(request.Start, request.End)
	window, accumulate, err := GetCustomCostWindowAccumulation(window, request.Accumulate)
	if err != nil {
		return nil, fmt.Errorf("error getting custom cost timeseries window accumulation: %w", err)
	}

	windows, err := window.GetAccumulateWindows(accumulate)
	if err != nil {
		return nil, fmt.Errorf("error getting timeseries windows: %w", err)
	}

	totals := make([]*CostResponse, len(windows))
	errors := make([]error, len(windows))

	// Query concurrently for each result, error
	var wg sync.WaitGroup
	wg.Add(len(windows))

	for i, w := range windows {
		go func(i int, window opencost.Window, res []*CostResponse) {
			defer wg.Done()
			totals[i], errors[i] = rq.QueryTotal(ctx, CostTotalRequest{
				Start:         *window.Start(),
				End:           *window.End(),
				AggregateBy:   request.AggregateBy,
				Filter:        request.Filter,
				Accumulate:    accumulate,
				CostType:      request.CostType,
				SortBy:        request.SortBy,
				SortDirection: request.SortDirection,
			})
		}(i, w, totals)
	}

	wg.Wait()

	// Return an error if any errors occurred
	for i, err := range errors {
		if err != nil {
			return nil, fmt.Errorf("one of %d errors: error querying costs for %s: %w", numErrors(errors), windows[i], err)
		}
	}

	result := &CostTimeseriesResponse{
		Window:     window,
		Timeseries: totals,
	}

	return result, nil
}

func numErrors(errors []error) int {
	numErrs := 0
	for i := range errors {
		if errors[i] != nil {
			numErrs++
		}
	}
	return numErrs
}
