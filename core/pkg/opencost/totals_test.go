package opencost

import (
	"math"
	"testing"
)

func TestComputeIdleCoefficients(t *testing.T) {
	// test that passing totals where total + adjustment == 0 returns a 0 coefficient
	at := make(map[string]*AllocationTotals)

	at["item1"] = &AllocationTotals{
		CPUCost:           1,
		CPUCostAdjustment: -1,
		RAMCost:           2,
		RAMCostAdjustment: -2,
		GPUCost:           3,
		GPUCostAdjustment: -3,
	}

	cpu, gpu, ram := ComputeIdleCoefficients("weighted", "item1", 100, 100, 100, at)

	if math.IsNaN(cpu) || math.IsNaN(gpu) || math.IsNaN(ram) || math.IsInf(cpu, 0) || math.IsInf(gpu, 0) || math.IsInf(ram, 0) {
		t.Errorf("Idle coefficients should not be NaN or Inf")
	}
}
