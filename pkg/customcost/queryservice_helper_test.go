package customcost

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/httputil"
)

// Test_ParseCustomCostRequest_Accumulate focuses on testing that both Custom Cost request parsing functions properly
// set the `accumulate` field, inspired by a desire to prevent a regression of https://kubecost.atlassian.net/browse/ENG-2212
func Test_ParseCustomCostRequest_Accumulate(t *testing.T) {
	testCases := map[string]struct {
		accumulateString   string
		expectedAccumulate opencost.AccumulateOption
	}{
		"no accumulate": {
			accumulateString:   "",
			expectedAccumulate: opencost.AccumulateOptionDay,
		},
		"hour accumulate": {
			accumulateString:   "hour",
			expectedAccumulate: opencost.AccumulateOptionHour,
		},
		"day accumulate": {
			accumulateString:   "day",
			expectedAccumulate: opencost.AccumulateOptionDay,
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			qp := httputil.NewQueryParams(map[string][]string{})
			qp.Set("window", "7d")
			if len(tc.accumulateString) > 0 {
				qp.Set("accumulate", tc.accumulateString)
			}

			totalRequest, err := ParseCustomCostTotalRequest(qp)
			if err != nil {
				t.Fatalf("expected no error, got: %v", err)
			} else if totalRequest.Accumulate != tc.expectedAccumulate {
				t.Fatalf("expected %v, got %v", tc.expectedAccumulate, totalRequest.Accumulate)
			}

			timeseriesRequest, err := ParseCustomCostTimeseriesRequest(qp)
			if err != nil {
				t.Fatalf("expected no error, got: %v", err)
			} else if timeseriesRequest.Accumulate != tc.expectedAccumulate {
				t.Fatalf("expected %v, got %v", tc.expectedAccumulate, timeseriesRequest.Accumulate)
			}
		})
	}
}
