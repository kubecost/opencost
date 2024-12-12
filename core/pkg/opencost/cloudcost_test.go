package opencost

import (
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/util/timeutil"
)

var ccProperties1 = &CloudCostProperties{
	ProviderID:      "providerid1",
	Provider:        "provider1",
	AccountID:       "workgroup1",
	InvoiceEntityID: "billing1",
	Service:         "service1",
	Category:        "category1",
	Labels: map[string]string{
		"label1": "value1",
		"label2": "value2",
	},
}

// TestCloudCost_LoadCloudCost checks that loaded CloudCosts end up in the correct set in the
// correct proportions
func TestCloudCost_LoadCloudCost(t *testing.T) {
	cc1Key := ccProperties1.GenerateKey(nil)
	// create values for 3 day Range tests
	end := RoundBack(time.Now().UTC(), timeutil.Day)
	start := end.Add(-3 * timeutil.Day)
	dayWindows, _ := GetWindows(start, end, timeutil.Day)
	emtpyCCSR, _ := NewCloudCostSetRange(start, end, AccumulateOptionDay, "integration")
	testCases := map[string]struct {
		cc       []*CloudCost
		ccsr     *CloudCostSetRange
		expected []*CloudCostSet
	}{
		"Load Single Day On Grid": {
			cc: []*CloudCost{
				{
					Properties:       ccProperties1,
					Window:           dayWindows[0],
					ListCost:         CostMetric{Cost: 100, KubernetesPercent: 1},
					NetCost:          CostMetric{Cost: 80, KubernetesPercent: 1},
					AmortizedNetCost: CostMetric{Cost: 90, KubernetesPercent: 1},
					InvoicedCost:     CostMetric{Cost: 95, KubernetesPercent: 1},
					AmortizedCost:    CostMetric{Cost: 85, KubernetesPercent: 1},
				},
			},
			ccsr: emtpyCCSR.Clone(),
			expected: []*CloudCostSet{
				{
					Integration: "integration",
					Window:      dayWindows[0],
					CloudCosts: map[string]*CloudCost{
						cc1Key: {
							Properties:       ccProperties1,
							Window:           dayWindows[0],
							ListCost:         CostMetric{Cost: 100, KubernetesPercent: 1},
							NetCost:          CostMetric{Cost: 80, KubernetesPercent: 1},
							AmortizedNetCost: CostMetric{Cost: 90, KubernetesPercent: 1},
							InvoicedCost:     CostMetric{Cost: 95, KubernetesPercent: 1},
							AmortizedCost:    CostMetric{Cost: 85, KubernetesPercent: 1},
						},
					},
				},
				{
					Integration: "integration",
					Window:      dayWindows[1],
					CloudCosts:  map[string]*CloudCost{},
				},
				{
					Integration: "integration",
					Window:      dayWindows[2],
					CloudCosts:  map[string]*CloudCost{},
				},
			},
		},
		"Load Single Day Off Grid": {
			cc: []*CloudCost{
				{
					Properties:       ccProperties1,
					Window:           NewClosedWindow(start.Add(12*time.Hour), start.Add(36*time.Hour)),
					ListCost:         CostMetric{Cost: 100, KubernetesPercent: 1},
					NetCost:          CostMetric{Cost: 80, KubernetesPercent: 1},
					AmortizedNetCost: CostMetric{Cost: 90, KubernetesPercent: 1},
					InvoicedCost:     CostMetric{Cost: 95, KubernetesPercent: 1},
					AmortizedCost:    CostMetric{Cost: 85, KubernetesPercent: 1},
				},
			},
			ccsr: emtpyCCSR.Clone(),
			expected: []*CloudCostSet{
				{
					Integration: "integration",
					Window:      dayWindows[0],
					CloudCosts: map[string]*CloudCost{
						cc1Key: {
							Properties:       ccProperties1,
							Window:           NewClosedWindow(start.Add(12*time.Hour), start.Add(24*time.Hour)),
							ListCost:         CostMetric{Cost: 50, KubernetesPercent: 1},
							NetCost:          CostMetric{Cost: 40, KubernetesPercent: 1},
							AmortizedNetCost: CostMetric{Cost: 45, KubernetesPercent: 1},
							InvoicedCost:     CostMetric{Cost: 47.5, KubernetesPercent: 1},
							AmortizedCost:    CostMetric{Cost: 42.5, KubernetesPercent: 1},
						},
					},
				},
				{
					Integration: "integration",
					Window:      dayWindows[1],
					CloudCosts: map[string]*CloudCost{
						cc1Key: {
							Properties:       ccProperties1,
							Window:           NewClosedWindow(start.Add(24*time.Hour), start.Add(36*time.Hour)),
							ListCost:         CostMetric{Cost: 50, KubernetesPercent: 1},
							NetCost:          CostMetric{Cost: 40, KubernetesPercent: 1},
							AmortizedNetCost: CostMetric{Cost: 45, KubernetesPercent: 1},
							InvoicedCost:     CostMetric{Cost: 47.5, KubernetesPercent: 1},
							AmortizedCost:    CostMetric{Cost: 42.5, KubernetesPercent: 1},
						},
					},
				},
				{
					Integration: "integration",
					Window:      dayWindows[2],
					CloudCosts:  map[string]*CloudCost{},
				},
			},
		},
		"Load Single Day Off Grid Before Range Window": {
			cc: []*CloudCost{
				{
					Properties:       ccProperties1,
					Window:           NewClosedWindow(start.Add(-12*time.Hour), start.Add(12*time.Hour)),
					ListCost:         CostMetric{Cost: 100, KubernetesPercent: 1},
					NetCost:          CostMetric{Cost: 80, KubernetesPercent: 1},
					AmortizedNetCost: CostMetric{Cost: 90, KubernetesPercent: 1},
					InvoicedCost:     CostMetric{Cost: 95, KubernetesPercent: 1},
					AmortizedCost:    CostMetric{Cost: 85, KubernetesPercent: 1},
				},
			},
			ccsr: emtpyCCSR.Clone(),
			expected: []*CloudCostSet{
				{
					Integration: "integration",
					Window:      dayWindows[0],
					CloudCosts: map[string]*CloudCost{
						cc1Key: {
							Properties:       ccProperties1,
							Window:           NewClosedWindow(start, start.Add(12*time.Hour)),
							ListCost:         CostMetric{Cost: 50, KubernetesPercent: 1},
							NetCost:          CostMetric{Cost: 40, KubernetesPercent: 1},
							AmortizedNetCost: CostMetric{Cost: 45, KubernetesPercent: 1},
							InvoicedCost:     CostMetric{Cost: 47.5, KubernetesPercent: 1},
							AmortizedCost:    CostMetric{Cost: 42.5, KubernetesPercent: 1},
						},
					},
				},
				{
					Integration: "integration",
					Window:      dayWindows[1],
					CloudCosts:  map[string]*CloudCost{},
				},
				{
					Integration: "integration",
					Window:      dayWindows[2],
					CloudCosts:  map[string]*CloudCost{},
				},
			},
		},
		"Load Single Day Off Grid After Range Window": {
			cc: []*CloudCost{
				{
					Properties:       ccProperties1,
					Window:           NewClosedWindow(end.Add(-12*time.Hour), end.Add(12*time.Hour)),
					ListCost:         CostMetric{Cost: 100, KubernetesPercent: 1},
					NetCost:          CostMetric{Cost: 80, KubernetesPercent: 1},
					AmortizedNetCost: CostMetric{Cost: 90, KubernetesPercent: 1},
					InvoicedCost:     CostMetric{Cost: 95, KubernetesPercent: 1},
					AmortizedCost:    CostMetric{Cost: 85, KubernetesPercent: 1},
				},
			},
			ccsr: emtpyCCSR.Clone(),
			expected: []*CloudCostSet{
				{
					Integration: "integration",
					Window:      dayWindows[0],
					CloudCosts:  map[string]*CloudCost{},
				},
				{
					Integration: "integration",
					Window:      dayWindows[1],
					CloudCosts:  map[string]*CloudCost{},
				},
				{
					Integration: "integration",
					Window:      dayWindows[2],
					CloudCosts: map[string]*CloudCost{
						cc1Key: {
							Properties:       ccProperties1,
							Window:           NewClosedWindow(end.Add(-12*time.Hour), end),
							ListCost:         CostMetric{Cost: 50, KubernetesPercent: 1},
							NetCost:          CostMetric{Cost: 40, KubernetesPercent: 1},
							AmortizedNetCost: CostMetric{Cost: 45, KubernetesPercent: 1},
							InvoicedCost:     CostMetric{Cost: 47.5, KubernetesPercent: 1},
							AmortizedCost:    CostMetric{Cost: 42.5, KubernetesPercent: 1},
						},
					},
				},
			},
		},
		"Single Day Kubernetes Percent": {
			cc: []*CloudCost{
				{
					Properties:       ccProperties1,
					Window:           dayWindows[1],
					ListCost:         CostMetric{Cost: 75, KubernetesPercent: 1},
					NetCost:          CostMetric{Cost: 40, KubernetesPercent: 1},
					AmortizedNetCost: CostMetric{Cost: 60, KubernetesPercent: 1},
					InvoicedCost:     CostMetric{Cost: 50, KubernetesPercent: 1},
					AmortizedCost:    CostMetric{Cost: 80, KubernetesPercent: 1},
				},
				{
					Properties:       ccProperties1,
					Window:           dayWindows[1],
					ListCost:         CostMetric{Cost: 25, KubernetesPercent: 0},
					NetCost:          CostMetric{Cost: 60, KubernetesPercent: 0},
					AmortizedNetCost: CostMetric{Cost: 40, KubernetesPercent: 0},
					InvoicedCost:     CostMetric{Cost: 50, KubernetesPercent: 0},
					AmortizedCost:    CostMetric{Cost: 20, KubernetesPercent: 0},
				},
			},
			ccsr: emtpyCCSR.Clone(),
			expected: []*CloudCostSet{
				{
					Integration: "integration",
					Window:      dayWindows[0],
					CloudCosts:  map[string]*CloudCost{},
				},
				{
					Integration: "integration",
					Window:      dayWindows[1],
					CloudCosts: map[string]*CloudCost{
						cc1Key: {
							Properties:       ccProperties1,
							Window:           dayWindows[1],
							ListCost:         CostMetric{Cost: 100, KubernetesPercent: 0.75},
							NetCost:          CostMetric{Cost: 100, KubernetesPercent: 0.4},
							AmortizedNetCost: CostMetric{Cost: 100, KubernetesPercent: 0.6},
							InvoicedCost:     CostMetric{Cost: 100, KubernetesPercent: 0.5},
							AmortizedCost:    CostMetric{Cost: 100, KubernetesPercent: 0.8},
						},
					},
				},
				{
					Integration: "integration",
					Window:      dayWindows[2],
					CloudCosts:  map[string]*CloudCost{},
				},
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			// load Cloud Costs
			for _, cc := range tc.cc {
				tc.ccsr.LoadCloudCost(cc)
			}

			if len(tc.ccsr.CloudCostSets) != len(tc.expected) {
				t.Errorf("the CloudCostSetRanges did not have the expected length")
			}

			for i, ccs := range tc.ccsr.CloudCostSets {
				if !ccs.Equal(tc.expected[i]) {
					t.Errorf("CloudCostSet at index: %d did not match expected", i)
				}
			}
		})
	}

}
