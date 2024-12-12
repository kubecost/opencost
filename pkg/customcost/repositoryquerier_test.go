package customcost

import (
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
)

func TestGetCustomCostAccumulateOption(t *testing.T) {
	now := time.Now().UTC()
	nextHour := opencost.RoundForward(now, time.Hour)
	midnight := opencost.RoundForward(now, timeutil.Day)

	tests := map[string]struct {
		window  opencost.Window
		want    opencost.AccumulateOption
		from    []opencost.AccumulateOption
		wantErr bool
	}{
		"open window": {
			window:  opencost.NewWindow(nil, nil),
			from:    nil,
			want:    opencost.AccumulateOptionNone,
			wantErr: true,
		},
		"negative window": {
			window:  opencost.NewClosedWindow(midnight, midnight.Add(-1)),
			from:    nil,
			want:    opencost.AccumulateOptionNone,
			wantErr: true,
		},
		"hourly max": {
			window:  opencost.NewClosedWindow(nextHour.Add(-time.Hour*49).Add(-1), nextHour),
			from:    nil,
			want:    opencost.AccumulateOptionDay,
			wantErr: false,
		},
		"daily min": {
			window:  opencost.NewClosedWindow(nextHour.Add(-time.Hour*49).Add(-1), nextHour),
			from:    nil,
			want:    opencost.AccumulateOptionDay,
			wantErr: false,
		},
		"daily max": {
			window:  opencost.NewClosedWindow(midnight.Add(-timeutil.Day*7), midnight),
			from:    nil,
			want:    opencost.AccumulateOptionDay,
			wantErr: false,
		},
		"out of range": {
			window:  opencost.NewClosedWindow(midnight.Add(-timeutil.Day*120), midnight.Add(-timeutil.Day*30)),
			from:    nil,
			want:    opencost.AccumulateOptionDay,
			wantErr: false,
		},
		"daily from daily, monthly": {
			window: opencost.NewClosedWindow(nextHour.Add(-time.Hour*24), nextHour),
			from: []opencost.AccumulateOption{
				opencost.AccumulateOptionDay,
				opencost.AccumulateOptionMonth,
			},
			want:    opencost.AccumulateOptionDay,
			wantErr: false,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := getCustomCostAccumulateOption(tt.window, tt.from)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetAccumulateOption() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("GetAccumulateOption() got = %v, want %v", got, tt.want)
			}
		})
	}
}
