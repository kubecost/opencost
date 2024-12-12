package customcost

import (
	"context"
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
	"github.com/opencost/opencost/pkg/env"
)

type Querier interface {
	QueryTotal(ctx context.Context, request CostTotalRequest) (*CostResponse, error)
	QueryTimeseries(ctx context.Context, request CostTimeseriesRequest) (*CostTimeseriesResponse, error)
}

func GetCustomCostWindowAccumulation(window opencost.Window, accumulate opencost.AccumulateOption) (opencost.Window, opencost.AccumulateOption, error) {
	var err error
	if accumulate == opencost.AccumulateOptionNone {
		accumulate, err = getCustomCostAccumulateOption(window, nil)
		if err != nil {
			return opencost.Window{}, opencost.AccumulateOptionNone, fmt.Errorf("failed to determine custom cost accumulation option: %v", err)
		}
	}
	window, err = window.GetAccumulateWindow(accumulate)
	if err != nil {
		return opencost.Window{}, opencost.AccumulateOptionNone, fmt.Errorf("failed to determine custom cost accumulation option: %v", err)
	}

	return window, accumulate, nil
}

// getCustomCostAccumulateOption determines defaults in a way that matches options presented in the UI
func getCustomCostAccumulateOption(window opencost.Window, from []opencost.AccumulateOption) (opencost.AccumulateOption, error) {
	if window.IsOpen() || window.IsNegative() {
		return opencost.AccumulateOptionNone, fmt.Errorf("invalid window '%s'", window.String())
	}

	if len(from) == 0 {
		from = allSteppedAccumulateOptions
	}

	hourlyStoreHours := env.GetDataRetentionHourlyResolutionHours()
	hourlySteps := time.Duration(hourlyStoreHours) * time.Hour
	oldestHourly := time.Now().Add(-1 * hourlySteps)

	// Use hourly if...
	//  (1) hourly is an option;
	//  (2) we have hourly store coverage; and
	//  (3) the window duration is less than the hourly break point.
	if hasHourly(from) && oldestHourly.Before(*window.Start()) && window.Duration() <= hourlySteps {
		return opencost.AccumulateOptionHour, nil
	}

	dailyStoreDays := env.GetDataRetentionDailyResolutionDays()
	dailySteps := time.Duration(dailyStoreDays) * timeutil.Day
	oldestDaily := time.Now().Add(-1 * dailySteps)
	// Use daily if...
	//  (1) daily is an option
	// It is acceptable to query a range for which we only have partial data
	if hasDaily(from) {
		return opencost.AccumulateOptionDay, nil
	}

	if oldestDaily.After(*window.Start()) {
		return opencost.AccumulateOptionDay, fmt.Errorf("data store does not have coverage for %v", window)
	}

	return opencost.AccumulateOptionNone, fmt.Errorf("no valid accumulate option in %v for %s", from, window)
}

var allSteppedAccumulateOptions = []opencost.AccumulateOption{
	opencost.AccumulateOptionHour,
	opencost.AccumulateOptionDay,
}

func hasHourly(opts []opencost.AccumulateOption) bool {
	for _, opt := range opts {
		if opt == opencost.AccumulateOptionHour {
			return true
		}
	}

	return false
}

func hasDaily(opts []opencost.AccumulateOption) bool {
	for _, opt := range opts {
		if opt == opencost.AccumulateOptionDay {
			return true
		}
	}

	return false
}
