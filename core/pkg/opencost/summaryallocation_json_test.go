package opencost

import (
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/util/timeutil"
)

func TestSummaryAllocationSetRangeResponse_MarshalJSON(t *testing.T) {
	// Set a 1-day (start, end)
	s := time.Date(2023, time.March, 13, 0, 0, 0, 0, time.UTC)
	e := s.Add(timeutil.Day)

	// Set some basic numbers that can be used to asset accuracy later
	adjustment := -0.14
	bytes := 2183471523842.00
	cores := 2.78
	cost := 12.50
	gpus := 1.00

	// Test a normal allocation for numerical accuracy
	alloc := &Allocation{
		Name: "cluster/node/namespace/pod/alloc",
		Properties: &AllocationProperties{
			Cluster:   "cluster",
			Node:      "node",
			Namespace: "namespace",
			Pod:       "pod",
			Container: "alloc",
		},
		CPUCoreHours:               cores,
		CPUCoreRequestAverage:      cores,
		CPUCoreUsageAverage:        cores,
		CPUCost:                    cost,
		CPUCostAdjustment:          adjustment,
		GPUHours:                   gpus,
		GPUCost:                    cost,
		GPUCostAdjustment:          adjustment,
		NetworkTransferBytes:       bytes,
		NetworkReceiveBytes:        bytes,
		NetworkCost:                cost,
		NetworkCrossZoneCost:       cost,
		NetworkCrossRegionCost:     cost,
		NetworkInternetCost:        cost,
		NetworkCostAdjustment:      adjustment,
		LoadBalancerCost:           cost,
		LoadBalancerCostAdjustment: adjustment,
		PVs: PVAllocations{
			PVKey{Cluster: "cluster", Name: "pv"}: &PVAllocation{
				ByteHours: bytes,
				Cost:      cost,
			},
		},
		PVCostAdjustment:       adjustment,
		RAMByteHours:           bytes,
		RAMBytesRequestAverage: bytes,
		RAMBytesUsageAverage:   bytes,
		RAMCost:                cost,
		RAMCostAdjustment:      adjustment,
		SharedCost:             cost,
		ExternalCost:           cost,
	}

	// Test an allocation with NaN values for JSON marshal errors
	allocWithNaN := &Allocation{
		Name: "cluster/node/namespace/pod/nan",
		Properties: &AllocationProperties{
			Cluster:   "cluster",
			Node:      "node",
			Namespace: "namespace",
			Pod:       "pod",
			Container: "nan",
		},
		CPUCoreHours:               math.NaN(),
		CPUCoreRequestAverage:      math.NaN(),
		CPUCoreUsageAverage:        math.NaN(),
		CPUCost:                    math.NaN(),
		CPUCostAdjustment:          math.NaN(),
		GPUHours:                   gpus,
		GPUCost:                    cost,
		GPUCostAdjustment:          adjustment,
		NetworkTransferBytes:       bytes,
		NetworkReceiveBytes:        bytes,
		NetworkCost:                cost,
		NetworkCrossZoneCost:       cost,
		NetworkCrossRegionCost:     cost,
		NetworkInternetCost:        cost,
		NetworkCostAdjustment:      adjustment,
		LoadBalancerCost:           cost,
		LoadBalancerCostAdjustment: adjustment,
		PVs: PVAllocations{
			PVKey{Cluster: "cluster", Name: "pv"}: &PVAllocation{
				ByteHours: bytes,
				Cost:      cost,
			},
		},
		PVCostAdjustment:       adjustment,
		RAMByteHours:           bytes,
		RAMBytesRequestAverage: bytes,
		RAMBytesUsageAverage:   bytes,
		RAMCost:                cost,
		RAMCostAdjustment:      adjustment,
		SharedCost:             cost,
		ExternalCost:           cost,
	}

	// Convert to SummaryAllocationSetRange
	as := NewAllocationSet(s, e, alloc, allocWithNaN)
	sas := NewSummaryAllocationSet(as, nil, nil, true, true)
	sasr := NewSummaryAllocationSetRange(sas)

	// Confirm that SummaryAllocationSetRange does error because on NaN
	_, err := json.Marshal(sasr)
	if err == nil {
		t.Fatalf("expected NaN values to cause error")
	}

	// Convert to response
	sasrr := sasr.ToResponse()

	// Confirm that same SummaryAllocationSetRangeResponse does NOT error
	_, err = json.Marshal(sasrr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
