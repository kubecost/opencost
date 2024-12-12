package opencost

import (
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/util/mathutil"
)

func TestAllocation_MarshalJSON(t *testing.T) {
	start := time.Date(2021, time.January, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2021, time.January, 2, 0, 0, 0, 0, time.UTC)
	hrs := 24.0

	gib := 1024.0 * 1024.0 * 1024.0

	cpuPrice := 0.02
	gpuPrice := 2.00
	ramPrice := 0.01
	pvPrice := 0.00005

	before := &Allocation{
		Name: "cluster1/namespace1/node1/pod1/container1",
		Properties: &AllocationProperties{
			Cluster:   "cluster1",
			Node:      "node1",
			Namespace: "namespace1",
			Pod:       "pod1",
			Container: "container1",
		},
		Window:                NewWindow(&start, &end),
		Start:                 start,
		End:                   end,
		CPUCoreHours:          2.0 * hrs,
		CPUCoreRequestAverage: 2.0,
		CPUCoreUsageAverage:   1.0,
		CPUCost:               2.0 * hrs * cpuPrice,
		CPUCostAdjustment:     3.0,
		GPUHours:              1.0 * hrs,
		GPUCost:               1.0 * hrs * gpuPrice,
		GPUCostAdjustment:     2.0,
		NetworkCost:           0.05,
		LoadBalancerCost:      0.02,
		PVs: PVAllocations{
			disk: {
				ByteHours: 100.0 * gib * hrs,
				Cost:      100.0 * hrs * pvPrice,
			},
		},
		PVCostAdjustment:       4.0,
		RAMByteHours:           8.0 * gib * hrs,
		RAMBytesRequestAverage: 8.0 * gib,
		RAMBytesUsageAverage:   4.0 * gib,
		RAMCost:                8.0 * hrs * ramPrice,
		RAMCostAdjustment:      1.0,
		SharedCost:             2.00,
		ExternalCost:           1.00,
		RawAllocationOnly:      &RawAllocationOnlyData{},
	}

	data, err := json.Marshal(before)
	if err != nil {
		t.Fatalf("Allocation.MarshalJSON: unexpected error: %s", err)
	}

	after := &Allocation{}
	err = json.Unmarshal(data, after)
	if err != nil {
		t.Fatalf("Allocation.UnmarshalJSON: unexpected error: %s", err)
	}

	// TODO:CLEANUP fix json marshaling of Window so that all of this works.
	// In the meantime, just set the Window so that we can test the rest.
	after.Window = before.Window.Clone()
	if !after.Equal(before) {
		t.Fatalf("Allocation.MarshalJSON: before and after are not equal")
	}
}

func TestPVAllocations_MarshalJSON(t *testing.T) {
	testCases := map[string]PVAllocations{
		"empty": {},
		"single": {
			{
				Cluster: "cluster1",
				Name:    "pv1",
			}: {
				ByteHours: 100,
				Cost:      1,
			},
		},
		"multi": {
			{
				Cluster: "cluster1",
				Name:    "pv1",
			}: {
				ByteHours: 100,
				Cost:      1,
			},
			{
				Cluster: "cluster1",
				Name:    "pv2",
			}: {
				ByteHours: 200,
				Cost:      2,
			},
		},
		"emptyPV": {
			{
				Cluster: "cluster1",
				Name:    "pv1",
			}: {},
		},
		"emptyKey": {
			{}: {
				ByteHours: 100,
				Cost:      1,
			},
		},
	}
	for name, before := range testCases {
		t.Run(name, func(t *testing.T) {
			data, err := json.Marshal(before)
			if err != nil {
				t.Fatalf("PVAllocations.MarshalJSON: unexpected error: %s", err)
			}

			after := PVAllocations{}
			err = json.Unmarshal(data, &after)
			if err != nil {
				t.Fatalf("PVAllocations.UnmarshalJSON: unexpected error: %s", err)
			}

			if len(before) != len(after) {
				t.Fatalf("PVAllocations.MarshalJSON: before and after are not equal")
			}

			for pvKey, beforePV := range before {
				afterPV, ok := after[pvKey]
				if !ok {
					t.Fatalf("PVAllocations.MarshalJSON: after missing PVKey %s", pvKey)
				}
				if beforePV.Cost != afterPV.Cost {
					t.Fatalf("PVAllocations.MarshalJSON: PVAllocation Cost not equal for PVKey %s", pvKey)
				}

				if beforePV.ByteHours != afterPV.ByteHours {
					t.Fatalf("PVAllocations.MarshalJSON: PVAllocation ByteHours not equal for PVKey %s", pvKey)
				}
			}

		})
	}

}

func TestLbAllocation_MarshalJSON(t *testing.T) {
	testCases := map[string]LbAllocations{
		"empty": {},
		"single": {
			"cluster1/namespace1/ingress": {
				Service: "namespace1/ingress",
				Cost:    1,
				Private: false,
				Ip:      "127.0.0.1",
			},
		},
		"multi": {
			"cluster1/namespace1/ingress": {
				Service: "namespace1/ingress",
				Cost:    1,
				Private: false,
				Ip:      "127.0.0.1",
			},
			"cluster1/namespace1/frontend": {
				Service: "namespace1/frontend",
				Cost:    1,
				Private: false,
				Ip:      "127.0.0.2",
			},
		},
		"emptyLB": {
			"cluster1/namespace1/pod": {},
		},
	}

	for name, before := range testCases {
		t.Run(name, func(t *testing.T) {
			data, err := json.Marshal(before)
			if err != nil {
				t.Fatalf("LbAllocations.MarshalJSON: unexpected error: %s", err)
			}

			after := LbAllocations{}
			err = json.Unmarshal(data, &after)
			if err != nil {
				t.Fatalf("LbAllocations.UnmarshalJSON: unexpected error: %s", err)
			}

			if len(before) != len(after) {
				t.Fatalf("LbAllocations.MarshalJSON: before and after are not equal")
			}

			for serviceKey, beforeLB := range before {
				afterLB, ok := after[serviceKey]
				if !ok {
					t.Fatalf("LbAllocations.MarshalJSON: after missing serviceKey %s", serviceKey)
				}
				if beforeLB.Cost != afterLB.Cost {
					t.Fatalf("LbAllocations.MarshalJSON: LbAllocation Cost not equal for serviceKey %s", serviceKey)
				}

				if beforeLB.Ip != afterLB.Ip {
					t.Fatalf("LbAllocations.MarshalJSON: LbAllocation Ip not equal for serviceKey %s", serviceKey)
				}
			}

		})
	}

}

func TestFormatFloat64ForResponse(t *testing.T) {
	type formatTestCase struct {
		name          string
		input         float64
		expectedNil   bool
		expectedValue float64
	}
	testCases := []formatTestCase{
		{
			name:          "zero",
			input:         0.0,
			expectedNil:   false,
			expectedValue: 0.0,
		},
		{
			name:          "round to zero",
			input:         0.000000001,
			expectedNil:   false,
			expectedValue: 0,
		},
		{
			name:          "valid value, no rounding",
			input:         14.123456,
			expectedNil:   false,
			expectedValue: 14.123456,
		},
		{
			name:          "valid value, with rounding",
			input:         14.1234567,
			expectedNil:   false,
			expectedValue: 14.123457,
		},
		{
			name:        "NaN is nil",
			input:       math.NaN(),
			expectedNil: true,
		},
		{
			name:        "infinite is nil",
			input:       math.Inf(1),
			expectedNil: true,
		},
		{
			name:        "negative infinite is nil",
			input:       math.Inf(-1),
			expectedNil: true,
		},
	}
	for _, tc := range testCases {
		result := formatFloat64ForResponse(tc.input)
		if result == nil && tc.expectedNil == false {
			t.Fatalf("test case: %s: expected a value %f, got nil instead", tc.name, tc.expectedValue)
		}
		if result != nil && tc.expectedNil == true {
			t.Fatalf("test case: %s: expected nil, got value %f instead", tc.name, *result)
		}
		if result != nil && !mathutil.Approximately(*result, tc.expectedValue) {
			t.Fatalf("test case: %s: expected %f, got %f", tc.name, tc.expectedValue, *result)
		}
	}
}
