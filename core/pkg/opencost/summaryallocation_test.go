package opencost

import (
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/util"
)

func TestSummaryAllocation_Add(t *testing.T) {
	window, _ := ParseWindowUTC("yesterday")

	var sa1, sa2, osa1, osa2, nilsa *SummaryAllocation
	var err error

	sa1Start := *window.Start()

	sa1End := *window.End()

	sa1 = &SummaryAllocation{
		Name: "cluster1/namespace1/pod1/container1",
		Properties: &AllocationProperties{
			Cluster:   "cluster1",
			Namespace: "namespace1",
			Pod:       "pod1",
			Container: "container1",
		},
		Start:                  sa1Start,
		End:                    sa1End,
		CPUCoreRequestAverage:  0.5,
		CPUCoreUsageAverage:    0.1,
		CPUCost:                0.2,
		GPUCost:                1.0,
		NetworkCost:            0.1,
		LoadBalancerCost:       0.6,
		PVCost:                 0.005,
		RAMBytesRequestAverage: 50.0 * 1024.0 * 1024.0,
		RAMBytesUsageAverage:   10.0 * 1024.0 * 1024.0,
		RAMCost:                0.05,
		SharedCost:             1.0,
		ExternalCost:           1.0,
	}
	osa1 = sa1.Clone()

	// sa2 is just as expensive, with twice as much usage and request, and half
	// the time compared to sa1

	sa2Start := *window.Start()
	sa2Start = sa2Start.Add(6 * time.Hour)

	sa2End := *window.End()
	sa2End = sa2End.Add(-6 * time.Hour)

	sa2 = &SummaryAllocation{
		Name: "cluster1/namespace1/pod2/container2",
		Properties: &AllocationProperties{
			Cluster:   "cluster1",
			Namespace: "namespace1",
			Pod:       "pod2",
			Container: "container2",
		},
		Start:                  sa2Start,
		End:                    sa2End,
		CPUCoreRequestAverage:  sa1.CPUCoreRequestAverage * 2.0,
		CPUCoreUsageAverage:    sa1.CPUCoreUsageAverage * 2.0,
		CPUCost:                sa1.CPUCost,
		GPUCost:                sa1.GPUCost,
		NetworkCost:            sa1.NetworkCost,
		LoadBalancerCost:       sa1.LoadBalancerCost,
		PVCost:                 sa1.PVCost,
		RAMBytesRequestAverage: sa1.RAMBytesRequestAverage * 2.0,
		RAMBytesUsageAverage:   sa1.RAMBytesUsageAverage * 2.0,
		RAMCost:                sa1.RAMCost,
		SharedCost:             sa1.SharedCost,
		ExternalCost:           sa1.ExternalCost,
	}
	osa2 = sa2.Clone()

	// add nil to nil, expect and error
	t.Run("nil.Add(nil)", func(t *testing.T) {
		err = nilsa.Add(nilsa)
		if err == nil {
			t.Fatalf("expected error: cannot add nil SummaryAllocations")
		}
	})

	// reset
	sa1 = osa1.Clone()
	sa2 = osa2.Clone()

	// add sa1 to nil, expect and error
	t.Run("nil.Add(sa1)", func(t *testing.T) {
		err = nilsa.Add(sa1)
		if err == nil {
			t.Fatalf("expected error: cannot add nil SummaryAllocations")
		}
	})

	// reset
	sa1 = osa1.Clone()
	sa2 = osa2.Clone()

	// add nil to sa1, expect and error
	t.Run("sa1.Add(nil)", func(t *testing.T) {
		err = sa1.Add(nilsa)
		if err == nil {
			t.Fatalf("expected error: cannot add nil SummaryAllocations")
		}
	})

	// reset
	sa1 = osa1.Clone()
	sa2 = osa2.Clone()

	// add sa1 to sa2 and expect same averages, but double costs
	t.Run("sa2.Add(sa1)", func(t *testing.T) {
		err = sa2.Add(sa1)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		if sa2.Properties != nil {
			t.Fatalf("expected properties to be nil; actual: %s", sa1.Properties)
		}
		if !util.IsApproximately(sa2.CPUCoreRequestAverage, (0.5*osa2.CPUCoreRequestAverage)+osa1.CPUCoreRequestAverage) {
			t.Fatalf("incorrect CPUCoreRequestAverage: expected %.5f; actual %.5f", (0.5*osa2.CPUCoreRequestAverage)+osa1.CPUCoreRequestAverage, sa2.CPUCoreRequestAverage)
		}
		if !util.IsApproximately(sa2.CPUCoreUsageAverage, (0.5*osa2.CPUCoreUsageAverage)+osa1.CPUCoreUsageAverage) {
			t.Fatalf("incorrect CPUCoreUsageAverage: expected %.5f; actual %.5f", (0.5*osa2.CPUCoreUsageAverage)+osa1.CPUCoreRequestAverage, sa2.CPUCoreUsageAverage)
		}
		if !util.IsApproximately(sa2.RAMBytesRequestAverage, (0.5*osa2.RAMBytesRequestAverage)+osa1.RAMBytesRequestAverage) {
			t.Fatalf("incorrect RAMBytesRequestAverage: expected %.5f; actual %.5f", (0.5*osa2.RAMBytesRequestAverage)+osa1.RAMBytesRequestAverage, sa2.RAMBytesRequestAverage)
		}
		if !util.IsApproximately(sa2.RAMBytesUsageAverage, (0.5*osa2.RAMBytesUsageAverage)+osa1.RAMBytesUsageAverage) {
			t.Fatalf("incorrect RAMBytesUsageAverage: expected %.5f; actual %.5f", (0.5*osa2.RAMBytesUsageAverage)+osa1.RAMBytesRequestAverage, sa2.RAMBytesUsageAverage)
		}
		if !util.IsApproximately(sa2.CPUCost, osa2.CPUCost+osa1.CPUCost) {
			t.Fatalf("incorrect CPUCost: expected %.5f; actual %.5f", osa2.CPUCost+osa1.CPUCost, sa2.CPUCost)
		}
		if !util.IsApproximately(sa2.GPUCost, osa2.GPUCost+osa1.GPUCost) {
			t.Fatalf("incorrect GPUCost: expected %.5f; actual %.5f", osa2.GPUCost+osa1.GPUCost, sa2.GPUCost)
		}
		if !util.IsApproximately(sa2.NetworkCost, osa2.NetworkCost+osa1.NetworkCost) {
			t.Fatalf("incorrect NetworkCost: expected %.5f; actual %.5f", osa2.NetworkCost+osa1.NetworkCost, sa2.NetworkCost)
		}
		if !util.IsApproximately(sa2.LoadBalancerCost, osa2.LoadBalancerCost+osa1.LoadBalancerCost) {
			t.Fatalf("incorrect LoadBalancerCost: expected %.5f; actual %.5f", osa2.LoadBalancerCost+osa1.LoadBalancerCost, sa2.LoadBalancerCost)
		}
		if !util.IsApproximately(sa2.PVCost, osa2.PVCost+osa1.PVCost) {
			t.Fatalf("incorrect PVCost: expected %.5f; actual %.5f", osa2.PVCost+osa1.PVCost, sa2.PVCost)
		}
		if !util.IsApproximately(sa2.RAMCost, osa2.RAMCost+osa1.RAMCost) {
			t.Fatalf("incorrect RAMCost: expected %.5f; actual %.5f", osa2.RAMCost+osa1.RAMCost, sa2.RAMCost)
		}
		if !util.IsApproximately(sa2.SharedCost, osa2.SharedCost+osa1.SharedCost) {
			t.Fatalf("incorrect SharedCost: expected %.5f; actual %.5f", osa2.SharedCost+osa1.SharedCost, sa2.SharedCost)
		}
		if !util.IsApproximately(sa2.ExternalCost, osa2.ExternalCost+osa1.ExternalCost) {
			t.Fatalf("incorrect ExternalCost: expected %.5f; actual %.5f", osa2.ExternalCost+osa1.ExternalCost, sa2.ExternalCost)
		}
	})

	// reset
	sa1 = osa1.Clone()
	sa2 = osa2.Clone()

	// add sa2 to sa1 and expect same averages, but double costs
	t.Run("sa1.Add(sa2)", func(t *testing.T) {
		err = sa1.Add(sa2)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		if sa1.Properties != nil {
			t.Fatalf("expected properties to be nil; actual: %s", sa1.Properties)
		}
		if !util.IsApproximately(sa1.CPUCoreRequestAverage, (0.5*osa2.CPUCoreRequestAverage)+osa1.CPUCoreRequestAverage) {
			t.Fatalf("incorrect CPUCoreRequestAverage: expected %.5f; actual %.5f", (0.5*osa2.CPUCoreRequestAverage)+osa1.CPUCoreRequestAverage, sa2.CPUCoreRequestAverage)
		}
		if !util.IsApproximately(sa1.CPUCoreUsageAverage, (0.5*osa2.CPUCoreUsageAverage)+osa1.CPUCoreUsageAverage) {
			t.Fatalf("incorrect CPUCoreUsageAverage: expected %.5f; actual %.5f", (0.5*osa2.CPUCoreUsageAverage)+osa1.CPUCoreRequestAverage, sa2.CPUCoreUsageAverage)
		}
		if !util.IsApproximately(sa1.RAMBytesRequestAverage, (0.5*osa2.RAMBytesRequestAverage)+osa1.RAMBytesRequestAverage) {
			t.Fatalf("incorrect RAMBytesRequestAverage: expected %.5f; actual %.5f", (0.5*osa2.RAMBytesRequestAverage)+osa1.RAMBytesRequestAverage, sa2.RAMBytesRequestAverage)
		}
		if !util.IsApproximately(sa1.RAMBytesUsageAverage, (0.5*osa2.RAMBytesUsageAverage)+osa1.RAMBytesUsageAverage) {
			t.Fatalf("incorrect RAMBytesUsageAverage: expected %.5f; actual %.5f", (0.5*osa2.RAMBytesUsageAverage)+osa1.RAMBytesRequestAverage, sa2.RAMBytesUsageAverage)
		}
		if !util.IsApproximately(sa1.CPUCost, osa2.CPUCost+osa1.CPUCost) {
			t.Fatalf("incorrect CPUCost: expected %.5f; actual %.5f", osa2.CPUCost+osa1.CPUCost, sa2.CPUCost)
		}
		if !util.IsApproximately(sa1.GPUCost, osa2.GPUCost+osa1.GPUCost) {
			t.Fatalf("incorrect GPUCost: expected %.5f; actual %.5f", osa2.GPUCost+osa1.GPUCost, sa2.GPUCost)
		}
		if !util.IsApproximately(sa1.NetworkCost, osa2.NetworkCost+osa1.NetworkCost) {
			t.Fatalf("incorrect NetworkCost: expected %.5f; actual %.5f", osa2.NetworkCost+osa1.NetworkCost, sa2.NetworkCost)
		}
		if !util.IsApproximately(sa1.LoadBalancerCost, osa2.LoadBalancerCost+osa1.LoadBalancerCost) {
			t.Fatalf("incorrect LoadBalancerCost: expected %.5f; actual %.5f", osa2.LoadBalancerCost+osa1.LoadBalancerCost, sa2.LoadBalancerCost)
		}
		if !util.IsApproximately(sa1.PVCost, osa2.PVCost+osa1.PVCost) {
			t.Fatalf("incorrect PVCost: expected %.5f; actual %.5f", osa2.PVCost+osa1.PVCost, sa2.PVCost)
		}
		if !util.IsApproximately(sa1.RAMCost, osa2.RAMCost+osa1.RAMCost) {
			t.Fatalf("incorrect RAMCost: expected %.5f; actual %.5f", osa2.RAMCost+osa1.RAMCost, sa2.RAMCost)
		}
		if !util.IsApproximately(sa1.SharedCost, osa2.SharedCost+osa1.SharedCost) {
			t.Fatalf("incorrect SharedCost: expected %.5f; actual %.5f", osa2.SharedCost+osa1.SharedCost, sa2.SharedCost)
		}
		if !util.IsApproximately(sa1.ExternalCost, osa2.ExternalCost+osa1.ExternalCost) {
			t.Fatalf("incorrect ExternalCost: expected %.5f; actual %.5f", osa2.ExternalCost+osa1.ExternalCost, sa2.ExternalCost)
		}
	})
}

func TestSummaryAllocationSet_RAMEfficiency(t *testing.T) {
	// Generating 6 sample summary allocations for testing
	var sa1, sa2, sa3, sa4, sa5, sa6, idlesa *SummaryAllocation

	// Generating accumulated summary allocation sets for testing
	var sas1, sas2, sas3, sas4, sas5, sas6 *SummaryAllocationSet

	window, _ := ParseWindowUTC("7d")

	saStart := *window.Start()

	saEnd := *window.End()

	sa1 = &SummaryAllocation{
		Name: "cluster1/namespace1/pod1/container1",
		Properties: &AllocationProperties{
			Cluster:   "cluster1",
			Namespace: "namespace1",
			Pod:       "pod1",
			Container: "container1",
		},
		Start:                  saStart,
		End:                    saEnd,
		RAMBytesRequestAverage: 50.0 * 1024.0 * 1024.0,
		RAMBytesUsageAverage:   10.0 * 1024.0 * 1024.0,
		RAMCost:                0.05,
	}

	sa2 = &SummaryAllocation{
		Name: "cluster1/namespace1/pod1/container2",
		Properties: &AllocationProperties{
			Cluster:   "cluster1",
			Namespace: "namespace1",
			Pod:       "pod1",
			Container: "container2",
		},
		Start:                  saStart,
		End:                    saEnd,
		RAMBytesRequestAverage: 50.0 * 1024.0 * 1024.0,
		RAMBytesUsageAverage:   15.0 * 1024.0 * 1024.0,
		RAMCost:                0.10,
	}

	sa3 = &SummaryAllocation{
		Name: "cluster1/namespace1/pod1/container3",
		Properties: &AllocationProperties{
			Cluster:   "cluster1",
			Namespace: "namespace1",
			Pod:       "pod1",
			Container: "container3",
		},
		Start:                  saStart,
		End:                    saEnd,
		RAMBytesRequestAverage: 0.0,
		RAMBytesUsageAverage:   10.0 * 1024.0 * 1024.0,
		RAMCost:                0.0,
	}

	sa4 = &SummaryAllocation{
		Name: "cluster1/namespace1/pod1/container4",
		Properties: &AllocationProperties{
			Cluster:   "cluster1",
			Namespace: "namespace1",
			Pod:       "pod1",
			Container: "container4",
		},
		Start:                  saStart,
		End:                    saEnd,
		RAMBytesRequestAverage: 0.0,
		RAMBytesUsageAverage:   10.0 * 1024.0 * 1024.0,
		RAMCost:                0.0,
	}

	sa5 = &SummaryAllocation{
		Name: "cluster1/namespace1/pod1/container4",
		Properties: &AllocationProperties{
			Cluster:   "cluster1",
			Namespace: "namespace1",
			Pod:       "pod1",
			Container: "container5",
		},
		Start:                  saStart,
		End:                    saEnd,
		RAMBytesRequestAverage: 0.0,
		RAMBytesUsageAverage:   10.0 * 1024.0 * 1024.0,
		RAMCost:                0.10,
	}

	sa6 = &SummaryAllocation{
		Name: "cluster1/namespace1/pod1/container4",
		Properties: &AllocationProperties{
			Cluster:   "cluster1",
			Namespace: "namespace1",
			Pod:       "pod1",
			Container: "container6",
		},
		Start:                  saStart,
		End:                    saEnd,
		RAMBytesRequestAverage: 0.0,
		RAMBytesUsageAverage:   10.0 * 1024.0 * 1024.0,
		RAMCost:                0.10,
	}

	idlesa = &SummaryAllocation{
		Name: IdleSuffix,
		Properties: &AllocationProperties{
			Cluster:   "cluster1",
			Namespace: "namespace1",
			Pod:       "pod1",
			Container: "container7",
		},
		Start:   saStart,
		End:     saEnd,
		CPUCost: 1.0,
		RAMCost: 1.0,
	}

	testcase1Map := map[string]*SummaryAllocation{
		"cluster1/namespace1/pod1/container1": sa1,
		"cluster1/namespace1/pod1/container2": sa2,
	}

	testcase2Map := map[string]*SummaryAllocation{
		"cluster1/namespace1/pod1/container3": sa3,
		"cluster1/namespace1/pod1/container4": sa4,
	}

	testcase3Map := map[string]*SummaryAllocation{
		"cluster1/namespace1/pod1/container5": sa5,
		"cluster1/namespace1/pod1/container6": sa6,
	}

	testcase4Map := map[string]*SummaryAllocation{}

	testcase5Map := map[string]*SummaryAllocation{
		"cluster1/namespace1/pod1/container1": sa1,
		"cluster1/namespace1/pod1/container2": sa2,
		"cluster1/namespace1/pod1/container3": sa3,
		"cluster1/namespace1/pod1/container4": sa4,
		"cluster1/namespace1/pod1/container5": sa5,
		"cluster1/namespace1/pod1/container6": sa6,
	}

	testcase6Map := map[string]*SummaryAllocation{
		"cluster1/namespace1/pod1/container1": sa1,
		"cluster1/namespace1/pod1/container2": sa2,
		"cluster1/__idle__":                   idlesa,
	}

	sas1 = &SummaryAllocationSet{
		SummaryAllocations: testcase1Map,
		Window:             window,
	}

	sas2 = &SummaryAllocationSet{
		SummaryAllocations: testcase2Map,
		Window:             window,
	}

	sas3 = &SummaryAllocationSet{
		SummaryAllocations: testcase3Map,
		Window:             window,
	}

	sas4 = &SummaryAllocationSet{
		SummaryAllocations: testcase4Map,
		Window:             window,
	}

	sas5 = &SummaryAllocationSet{
		SummaryAllocations: testcase5Map,
		Window:             window,
	}

	sas6 = &SummaryAllocationSet{
		SummaryAllocations: testcase6Map,
		Window:             window,
	}

	cases := []struct {
		name               string
		testsas            *SummaryAllocationSet
		expectedEfficiency float64
	}{
		{
			name:               "Check RAMEfficiency when totalRAMBytesRequest over allocation summary set is greater than 0",
			testsas:            sas1,
			expectedEfficiency: 0.25,
		},
		{
			name:               "Check RAMEfficiency when totalRAMBytesRequest is 0 and totalRAMCost or totalRAMBytesUsage equal to 0",
			testsas:            sas2,
			expectedEfficiency: 0.0,
		},
		{
			name:               "Check RAMEfficiency when totalRAMBytesRequest is 0 and totalRAMCost or totalRAMBytesUsage is not 0",
			testsas:            sas3,
			expectedEfficiency: 1.0,
		},
		{
			name:               "Check RAMEfficiency when allocation summary set is empty",
			testsas:            sas4,
			expectedEfficiency: 0.0,
		},
		{
			name:               "Check RAMEfficiency over combination of all allocation summaries",
			testsas:            sas5,
			expectedEfficiency: 0.65,
		},
		{
			name:               "Check RAMEfficiency in presence of an idle allocation",
			testsas:            sas6,
			expectedEfficiency: 0.25,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			returnEfficiency := c.testsas.RAMEfficiency()
			if !util.IsApproximately(c.expectedEfficiency, returnEfficiency) {
				t.Errorf("Case %s failed: Expected RAM Efficiency %.2f but got RAM Efficiency of as %.2f", c.name, c.expectedEfficiency, returnEfficiency)
				t.Fail()
			}
		})
	}
}

func TestSummaryAllocationSet_CPUEfficiency(t *testing.T) {
	// Generating 6 sample summary allocations for testing
	var sa1, sa2, sa3, sa4, sa5, sa6, idlesa *SummaryAllocation

	// Generating accumulated summary allocation sets for testing
	var sas1, sas2, sas3, sas4, sas5, sas6 *SummaryAllocationSet

	window, _ := ParseWindowUTC("7d")

	saStart := *window.Start()

	saEnd := *window.End()

	sa1 = &SummaryAllocation{
		Name: "cluster1/namespace1/pod1/container1",
		Properties: &AllocationProperties{
			Cluster:   "cluster1",
			Namespace: "namespace1",
			Pod:       "pod1",
			Container: "container1",
		},
		Start:                 saStart,
		End:                   saEnd,
		CPUCoreRequestAverage: 0.5,
		CPUCoreUsageAverage:   0.1,
		CPUCost:               0.2,
	}

	sa2 = &SummaryAllocation{
		Name: "cluster1/namespace1/pod1/container2",
		Properties: &AllocationProperties{
			Cluster:   "cluster1",
			Namespace: "namespace1",
			Pod:       "pod1",
			Container: "container2",
		},
		Start:                 saStart,
		End:                   saEnd,
		CPUCoreRequestAverage: 0.5,
		CPUCoreUsageAverage:   0.2,
		CPUCost:               0.2,
	}

	sa3 = &SummaryAllocation{
		Name: "cluster1/namespace1/pod1/container3",
		Properties: &AllocationProperties{
			Cluster:   "cluster1",
			Namespace: "namespace1",
			Pod:       "pod1",
			Container: "container3",
		},
		Start:                 saStart,
		End:                   saEnd,
		CPUCoreRequestAverage: 0.0,
		CPUCoreUsageAverage:   0.0,
		CPUCost:               1.0,
	}

	sa4 = &SummaryAllocation{
		Name: "cluster1/namespace1/pod1/container4",
		Properties: &AllocationProperties{
			Cluster:   "cluster1",
			Namespace: "namespace1",
			Pod:       "pod1",
			Container: "container4",
		},
		Start:                 saStart,
		End:                   saEnd,
		CPUCoreRequestAverage: 0.0,
		CPUCoreUsageAverage:   0.0,
		CPUCost:               2.0,
	}

	sa5 = &SummaryAllocation{
		Name: "cluster1/namespace1/pod1/container4",
		Properties: &AllocationProperties{
			Cluster:   "cluster1",
			Namespace: "namespace1",
			Pod:       "pod1",
			Container: "container5",
		},
		Start:                 saStart,
		End:                   saEnd,
		CPUCoreRequestAverage: 0.0,
		CPUCoreUsageAverage:   0.1,
		CPUCost:               0.2,
	}

	sa6 = &SummaryAllocation{
		Name: "cluster1/namespace1/pod1/container4",
		Properties: &AllocationProperties{
			Cluster:   "cluster1",
			Namespace: "namespace1",
			Pod:       "pod1",
			Container: "container6",
		},
		Start:                 saStart,
		End:                   saEnd,
		CPUCoreRequestAverage: 0.0,
		CPUCoreUsageAverage:   0.1,
		CPUCost:               0.2,
	}

	idlesa = &SummaryAllocation{
		Name: IdleSuffix,
		Properties: &AllocationProperties{
			Cluster:   "cluster1",
			Namespace: "namespace1",
			Pod:       "pod1",
			Container: "container7",
		},
		Start:   saStart,
		End:     saEnd,
		CPUCost: 1.0,
		RAMCost: 1.0,
	}

	testcase1Map := map[string]*SummaryAllocation{
		"cluster1/namespace1/pod1/container1": sa1,
		"cluster1/namespace1/pod1/container2": sa2,
	}

	testcase2Map := map[string]*SummaryAllocation{
		"cluster1/namespace1/pod1/container3": sa3,
		"cluster1/namespace1/pod1/container4": sa4,
	}

	testcase3Map := map[string]*SummaryAllocation{
		"cluster1/namespace1/pod1/container5": sa5,
		"cluster1/namespace1/pod1/container6": sa6,
	}

	testcase4Map := map[string]*SummaryAllocation{}

	testcase5Map := map[string]*SummaryAllocation{
		"cluster1/namespace1/pod1/container1": sa1,
		"cluster1/namespace1/pod1/container2": sa2,
		"cluster1/namespace1/pod1/container3": sa3,
		"cluster1/namespace1/pod1/container4": sa4,
		"cluster1/namespace1/pod1/container5": sa5,
		"cluster1/namespace1/pod1/container6": sa6,
	}

	testcase6Map := map[string]*SummaryAllocation{
		"cluster1/namespace1/pod1/container1": sa1,
		"cluster1/namespace1/pod1/container2": sa2,
		"cluster1/__idle__":                   idlesa,
	}

	sas1 = &SummaryAllocationSet{
		SummaryAllocations: testcase1Map,
		Window:             window,
	}

	sas2 = &SummaryAllocationSet{
		SummaryAllocations: testcase2Map,
		Window:             window,
	}

	sas3 = &SummaryAllocationSet{
		SummaryAllocations: testcase3Map,
		Window:             window,
	}

	sas4 = &SummaryAllocationSet{
		SummaryAllocations: testcase4Map,
		Window:             window,
	}

	sas5 = &SummaryAllocationSet{
		SummaryAllocations: testcase5Map,
		Window:             window,
	}

	sas6 = &SummaryAllocationSet{
		SummaryAllocations: testcase6Map,
		Window:             window,
	}

	cases := []struct {
		name               string
		testsas            *SummaryAllocationSet
		expectedEfficiency float64
	}{
		{
			name:               "Check CPUEfficiency when totalCPUCoreRequest is greater than 0 over allocation summary set",
			testsas:            sas1,
			expectedEfficiency: 0.30,
		},
		{
			name:               "Check CPUEfficiency when totalCPUCoreRequest is 0 and totalCPUCost or totalCPUCoreUsage equal to 0",
			testsas:            sas2,
			expectedEfficiency: 0.0,
		},
		{
			name:               "Check CPUEfficiency when totalCPUCoreRequest is 0 and totalCPUCost or totalCPUCoreUsage is not 0",
			testsas:            sas3,
			expectedEfficiency: 1.0,
		},
		{
			name:               "Check CPUEfficiency when allocation summary set is empty",
			testsas:            sas4,
			expectedEfficiency: 0.0,
		},
		{
			name:               "Check CPUEfficiency over combination of all allocation summaries",
			testsas:            sas5,
			expectedEfficiency: 0.50,
		},
		{
			name:               "Check CPUEfficiency in presence of an idle allocation",
			testsas:            sas6,
			expectedEfficiency: 0.30,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			returnEfficiency := c.testsas.CPUEfficiency()
			if !util.IsApproximately(c.expectedEfficiency, returnEfficiency) {
				t.Errorf("Case %s failed: Expected CPU Efficiency %.2f but got CPU Efficiency of as %.2f", c.name, c.expectedEfficiency, returnEfficiency)
				t.Fail()
			}
		})
	}
}

func TestSummaryAllocationSet_TotalEfficiency(t *testing.T) {
	// Generating 6 sample summary allocations for testing
	var sa1, sa2, sa3, sa4, sa5, sa6, idlesa *SummaryAllocation

	// Generating accumulated summary allocation sets for testing
	var sas1, sas2, sas3, sas4 *SummaryAllocationSet

	window, _ := ParseWindowUTC("7d")

	saStart := *window.Start()

	saEnd := *window.End()

	sa1 = &SummaryAllocation{
		Name: "cluster1/namespace1/pod1/container1",
		Properties: &AllocationProperties{
			Cluster:   "cluster1",
			Namespace: "namespace1",
			Pod:       "pod1",
			Container: "container1",
		},
		Start:                  saStart,
		End:                    saEnd,
		CPUCoreRequestAverage:  0.5,
		CPUCoreUsageAverage:    0.1,
		CPUCost:                0.0,
		RAMBytesRequestAverage: 0.0,
		RAMBytesUsageAverage:   10.0 * 1024.0 * 1024.0,
		RAMCost:                0.0,
	}

	sa2 = &SummaryAllocation{
		Name: "cluster1/namespace1/pod1/container2",
		Properties: &AllocationProperties{
			Cluster:   "cluster1",
			Namespace: "namespace1",
			Pod:       "pod1",
			Container: "container2",
		},
		Start:                  saStart,
		End:                    saEnd,
		CPUCoreRequestAverage:  0.5,
		CPUCoreUsageAverage:    0.2,
		CPUCost:                0.0,
		RAMBytesRequestAverage: 0.0,
		RAMBytesUsageAverage:   10.0 * 1024.0 * 1024.0,
		RAMCost:                0.0,
	}

	sa3 = &SummaryAllocation{
		Name: "cluster1/namespace1/pod1/container3",
		Properties: &AllocationProperties{
			Cluster:   "cluster1",
			Namespace: "namespace1",
			Pod:       "pod1",
			Container: "container3",
		},
		Start:                  saStart,
		End:                    saEnd,
		CPUCoreRequestAverage:  0.5,
		CPUCoreUsageAverage:    0.2,
		CPUCost:                1.0,
		RAMBytesRequestAverage: 50.0 * 1024.0 * 1024.0,
		RAMBytesUsageAverage:   10.0 * 1024.0 * 1024.0,
		RAMCost:                1.0,
	}

	sa4 = &SummaryAllocation{
		Name: "cluster1/namespace1/pod1/container4",
		Properties: &AllocationProperties{
			Cluster:   "cluster1",
			Namespace: "namespace1",
			Pod:       "pod1",
			Container: "container4",
		},
		Start:                  saStart,
		End:                    saEnd,
		CPUCoreRequestAverage:  0.5,
		CPUCoreUsageAverage:    0.1,
		CPUCost:                1.0,
		RAMBytesRequestAverage: 50.0 * 1024.0 * 1024.0,
		RAMBytesUsageAverage:   20.0 * 1024.0 * 1024.0,
		RAMCost:                1.0,
	}

	sa5 = &SummaryAllocation{
		Name: "cluster1/namespace1/pod1/container4",
		Properties: &AllocationProperties{
			Cluster:   "cluster1",
			Namespace: "namespace1",
			Pod:       "pod1",
			Container: "container5",
		},
		Start:                  saStart,
		End:                    saEnd,
		CPUCoreRequestAverage:  0.5,
		CPUCoreUsageAverage:    0.1,
		CPUCost:                1.0,
		RAMBytesRequestAverage: 50.0 * 1024.0 * 1024.0,
		RAMBytesUsageAverage:   10.0 * 1024.0 * 1024.0,
		RAMCost:                1.0,
	}

	sa6 = &SummaryAllocation{
		Name: "cluster1/namespace1/pod1/container4",
		Properties: &AllocationProperties{
			Cluster:   "cluster1",
			Namespace: "namespace1",
			Pod:       "pod1",
			Container: "container6",
		},
		Start:                  saStart,
		End:                    saEnd,
		CPUCoreRequestAverage:  0.5,
		CPUCoreUsageAverage:    0.2,
		CPUCost:                1.0,
		RAMBytesRequestAverage: 50.0 * 1024.0 * 1024.0,
		RAMBytesUsageAverage:   20.0 * 1024.0 * 1024.0,
		RAMCost:                1.0,
	}

	idlesa = &SummaryAllocation{
		Name: IdleSuffix,
		Properties: &AllocationProperties{
			Cluster:   "cluster1",
			Namespace: "namespace1",
			Pod:       "pod1",
			Container: "container7",
		},
		Start:   saStart,
		End:     saEnd,
		CPUCost: 1.0,
		RAMCost: 1.0,
	}

	testcase1Map := map[string]*SummaryAllocation{
		"cluster1/namespace1/pod1/container1": sa1,
		"cluster1/namespace1/pod1/container2": sa2,
	}

	testcase2Map := map[string]*SummaryAllocation{}

	testcase3Map := map[string]*SummaryAllocation{
		"cluster1/namespace1/pod1/container3": sa3,
		"cluster1/namespace1/pod1/container4": sa4,
		"cluster1/namespace1/pod1/container5": sa5,
		"cluster1/namespace1/pod1/container6": sa6,
	}

	testcase4Map := map[string]*SummaryAllocation{
		"cluster1/namespace1/pod1/container5": sa5,
		"cluster1/namespace1/pod1/container6": sa6,
		"cluster1/__idle__":                   idlesa,
	}

	sas1 = &SummaryAllocationSet{
		SummaryAllocations: testcase1Map,
		Window:             window,
	}

	sas2 = &SummaryAllocationSet{
		SummaryAllocations: testcase2Map,
		Window:             window,
	}

	sas3 = &SummaryAllocationSet{
		SummaryAllocations: testcase3Map,
		Window:             window,
	}

	sas4 = &SummaryAllocationSet{
		SummaryAllocations: testcase4Map,
		Window:             window,
	}

	cases := []struct {
		name               string
		testsas            *SummaryAllocationSet
		expectedEfficiency float64
	}{
		{
			name:               "When TotalEfficiency when sum of TotalRAMCost and TotalCPUCost is 0",
			testsas:            sas1,
			expectedEfficiency: 0.0,
		},
		{
			name:               "Check TotalEfficiency when allocation summary set is empty",
			testsas:            sas2,
			expectedEfficiency: 0.0,
		},
		{
			name:               "Check TotalEfficiency over all 4 allocation summaries",
			testsas:            sas3,
			expectedEfficiency: 0.30,
		},
		{
			name:               "Check TotalEfficiency with idle cost",
			testsas:            sas4,
			expectedEfficiency: 0.30,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			returnEfficiency := c.testsas.TotalEfficiency()
			if !util.IsApproximately(c.expectedEfficiency, returnEfficiency) {
				t.Errorf("Case %s failed: Expected Total Efficiency %.2f but got Total Efficiency of as %.2f", c.name, c.expectedEfficiency, returnEfficiency)
				t.Fail()
			}
		})
	}
}

func TestSummaryAllocationSetRange_AccumulateBy_None(t *testing.T) {
	ago4d := time.Now().UTC().Truncate(day).Add(-4 * day)
	ago3d := time.Now().UTC().Truncate(day).Add(-3 * day)
	ago2d := time.Now().UTC().Truncate(day).Add(-2 * day)
	yesterday := time.Now().UTC().Truncate(day).Add(-day)
	today := time.Now().UTC().Truncate(day)

	ago4dSAS := NewMockUnitSummaryAllocationSet(ago4d, day)
	ago4dSAS.Insert(NewMockUnitSummaryAllocation("4", ago4d, day, nil))
	ago3dSAS := NewMockUnitSummaryAllocationSet(ago3d, day)
	ago3dSAS.Insert(NewMockUnitSummaryAllocation("a", ago3d, day, nil))
	ago2dSAS := NewMockUnitSummaryAllocationSet(ago2d, day)
	ago2dSAS.Insert(NewMockUnitSummaryAllocation("", ago2d, day, nil))
	yesterdaySAS := NewMockUnitSummaryAllocationSet(yesterday, day)
	yesterdaySAS.Insert(NewMockUnitSummaryAllocation("", yesterday, day, nil))
	todaySAS := NewMockUnitSummaryAllocationSet(today, day)
	todaySAS.Insert(NewMockUnitSummaryAllocation("", today, day, nil))

	asr := NewSummaryAllocationSetRange(ago4dSAS, ago3dSAS, ago2dSAS, yesterdaySAS, todaySAS)
	asr, err := asr.Accumulate(AccumulateOptionNone)
	if err != nil {
		t.Fatalf("unexpected error calling accumulateBy: %s", err)
	}

	if len(asr.SummaryAllocationSets) != 5 {
		t.Fatalf("expected 5 allocation sets, got:%d", len(asr.SummaryAllocationSets))
	}
}

func TestSummaryAllocationSetRange_AccumulateBy_All(t *testing.T) {
	ago4d := time.Now().UTC().Truncate(day).Add(-4 * day)
	ago3d := time.Now().UTC().Truncate(day).Add(-3 * day)
	ago2d := time.Now().UTC().Truncate(day).Add(-2 * day)
	yesterday := time.Now().UTC().Truncate(day).Add(-day)
	today := time.Now().UTC().Truncate(day)

	ago4dSAS := NewMockUnitSummaryAllocationSet(ago4d, day)
	ago4dSAS.Insert(NewMockUnitSummaryAllocation("4", ago4d, day, nil))
	ago3dSAS := NewMockUnitSummaryAllocationSet(ago3d, day)
	ago3dSAS.Insert(NewMockUnitSummaryAllocation("a", ago3d, day, nil))
	ago2dSAS := NewMockUnitSummaryAllocationSet(ago2d, day)
	ago2dSAS.Insert(NewMockUnitSummaryAllocation("", ago2d, day, nil))
	yesterdaySAS := NewMockUnitSummaryAllocationSet(yesterday, day)
	yesterdaySAS.Insert(NewMockUnitSummaryAllocation("", yesterday, day, nil))
	todaySAS := NewMockUnitSummaryAllocationSet(today, day)
	todaySAS.Insert(NewMockUnitSummaryAllocation("", today, day, nil))

	asr := NewSummaryAllocationSetRange(ago4dSAS, ago3dSAS, ago2dSAS, yesterdaySAS, todaySAS)
	asr, err := asr.Accumulate(AccumulateOptionAll)
	if err != nil {
		t.Fatalf("unexpected error calling accumulateBy: %s", err)
	}

	if len(asr.SummaryAllocationSets) != 1 {
		t.Fatalf("expected 1 allocation set, got:%d", len(asr.SummaryAllocationSets))
	}

	allocMap := asr.SummaryAllocationSets[0].SummaryAllocations
	alloc := allocMap["cluster1/namespace1/pod1/container1"]
	if alloc.Minutes() != 4320.0 {
		t.Errorf("accumulating AllocationSetRange: expected %f minutes; actual %f", 4320.0, alloc.Minutes())
	}
}

func TestSummaryAllocationSetRange_AccumulateBy_Hour(t *testing.T) {
	ago4h := time.Now().UTC().Truncate(time.Hour).Add(-4 * time.Hour)
	ago3h := time.Now().UTC().Truncate(time.Hour).Add(-3 * time.Hour)
	ago2h := time.Now().UTC().Truncate(time.Hour).Add(-2 * time.Hour)
	ago1h := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour)
	currentHour := time.Now().UTC().Truncate(time.Hour)

	ago4hAS := NewMockUnitSummaryAllocationSet(ago4h, time.Hour)
	ago4hAS.Insert(NewMockUnitSummaryAllocation("4", ago4h, time.Hour, nil))
	ago3hAS := NewMockUnitSummaryAllocationSet(ago3h, time.Hour)
	ago3hAS.Insert(NewMockUnitSummaryAllocation("a", ago3h, time.Hour, nil))
	ago2hAS := NewMockUnitSummaryAllocationSet(ago2h, time.Hour)
	ago2hAS.Insert(NewMockUnitSummaryAllocation("", ago2h, time.Hour, nil))
	ago1hAS := NewMockUnitSummaryAllocationSet(ago1h, time.Hour)
	ago1hAS.Insert(NewMockUnitSummaryAllocation("", ago1h, time.Hour, nil))
	currentHourAS := NewMockUnitSummaryAllocationSet(currentHour, time.Hour)
	currentHourAS.Insert(NewMockUnitSummaryAllocation("", currentHour, time.Hour, nil))

	asr := NewSummaryAllocationSetRange(ago4hAS, ago3hAS, ago2hAS, ago1hAS, currentHourAS)
	asr, err := asr.Accumulate(AccumulateOptionHour)
	if err != nil {
		t.Fatalf("unexpected error calling accumulateBy: %s", err)
	}

	if len(asr.SummaryAllocationSets) != 5 {
		t.Fatalf("expected 5 allocation sets, got:%d", len(asr.SummaryAllocationSets))
	}

	allocMap := asr.SummaryAllocationSets[0].SummaryAllocations
	alloc := allocMap["4"]
	if alloc.Minutes() != 60.0 {
		t.Errorf("accumulating AllocationSetRange: expected %f minutes; actual %f", 60.0, alloc.Minutes())
	}
}

func TestSummaryAllocationSetRange_AccumulateBy_Day_From_Day(t *testing.T) {
	ago4d := time.Now().UTC().Truncate(day).Add(-4 * day)
	ago3d := time.Now().UTC().Truncate(day).Add(-3 * day)
	ago2d := time.Now().UTC().Truncate(day).Add(-2 * day)
	yesterday := time.Now().UTC().Truncate(day).Add(-day)
	today := time.Now().UTC().Truncate(day)

	ago4dSAS := NewMockUnitSummaryAllocationSet(ago4d, day)
	ago4dSAS.Insert(NewMockUnitSummaryAllocation("4", ago4d, day, nil))
	ago3dSAS := NewMockUnitSummaryAllocationSet(ago3d, day)
	ago3dSAS.Insert(NewMockUnitSummaryAllocation("a", ago3d, day, nil))
	ago2dSAS := NewMockUnitSummaryAllocationSet(ago2d, day)
	ago2dSAS.Insert(NewMockUnitSummaryAllocation("", ago2d, day, nil))
	yesterdaySAS := NewMockUnitSummaryAllocationSet(yesterday, day)
	yesterdaySAS.Insert(NewMockUnitSummaryAllocation("", yesterday, day, nil))
	todaySAS := NewMockUnitSummaryAllocationSet(today, day)
	todaySAS.Insert(NewMockUnitSummaryAllocation("", today, day, nil))

	asr := NewSummaryAllocationSetRange(ago4dSAS, ago3dSAS, ago2dSAS, yesterdaySAS, todaySAS)
	asr, err := asr.Accumulate(AccumulateOptionNone)
	if err != nil {
		t.Fatalf("unexpected error calling accumulateBy: %s", err)
	}

	if len(asr.SummaryAllocationSets) != 5 {
		t.Fatalf("expected 5 allocation sets, got:%d", len(asr.SummaryAllocationSets))
	}
	allocMap := asr.SummaryAllocationSets[0].SummaryAllocations
	alloc := allocMap["4"]
	if alloc.Minutes() != 1440.0 {
		t.Errorf("accumulating AllocationSetRange: expected %f minutes; actual %f", 1440.0, alloc.Minutes())
	}
}

func TestSummaryAllocationSetRange_AccumulateBy_Day_From_Hours(t *testing.T) {
	ago4h := time.Now().UTC().Truncate(time.Hour).Add(-4 * time.Hour)
	ago3h := time.Now().UTC().Truncate(time.Hour).Add(-3 * time.Hour)
	ago2h := time.Now().UTC().Truncate(time.Hour).Add(-2 * time.Hour)
	ago1h := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour)
	currentHour := time.Now().UTC().Truncate(time.Hour)

	ago4hAS := NewMockUnitSummaryAllocationSet(ago4h, time.Hour)
	ago4hAS.Insert(NewMockUnitSummaryAllocation("", ago4h, time.Hour, nil))
	ago3hAS := NewMockUnitSummaryAllocationSet(ago3h, time.Hour)
	ago3hAS.Insert(NewMockUnitSummaryAllocation("", ago3h, time.Hour, nil))
	ago2hAS := NewMockUnitSummaryAllocationSet(ago2h, time.Hour)
	ago2hAS.Insert(NewMockUnitSummaryAllocation("", ago2h, time.Hour, nil))
	ago1hAS := NewMockUnitSummaryAllocationSet(ago1h, time.Hour)
	ago1hAS.Insert(NewMockUnitSummaryAllocation("", ago1h, time.Hour, nil))
	currentHourAS := NewMockUnitSummaryAllocationSet(currentHour, time.Hour)
	currentHourAS.Insert(NewMockUnitSummaryAllocation("", currentHour, time.Hour, nil))

	asr := NewSummaryAllocationSetRange(ago4hAS, ago3hAS, ago2hAS, ago1hAS, currentHourAS)
	asr, err := asr.Accumulate(AccumulateOptionDay)
	if err != nil {
		t.Fatalf("unexpected error calling accumulateBy: %s", err)
	}

	if len(asr.SummaryAllocationSets) != 1 && len(asr.SummaryAllocationSets) != 2 {
		t.Fatalf("expected 1 allocation set, got:%d", len(asr.SummaryAllocationSets))
	}

	allocMap := asr.SummaryAllocationSets[0].SummaryAllocations
	alloc := allocMap["cluster1/namespace1/pod1/container1"]
	if alloc.Minutes() > 300.0 {
		t.Errorf("accumulating AllocationSetRange: expected %f or less minutes; actual %f", 300.0, alloc.Minutes())
	}
}

func TestSummaryAllocationSetRange_AccumulateBy_Week(t *testing.T) {
	ago9d := time.Now().UTC().Truncate(day).Add(-9 * day)
	ago8d := time.Now().UTC().Truncate(day).Add(-8 * day)
	ago7d := time.Now().UTC().Truncate(day).Add(-7 * day)
	ago6d := time.Now().UTC().Truncate(day).Add(-6 * day)
	ago5d := time.Now().UTC().Truncate(day).Add(-5 * day)
	ago4d := time.Now().UTC().Truncate(day).Add(-4 * day)
	ago3d := time.Now().UTC().Truncate(day).Add(-3 * day)
	ago2d := time.Now().UTC().Truncate(day).Add(-2 * day)
	yesterday := time.Now().UTC().Truncate(day).Add(-day)
	today := time.Now().UTC().Truncate(day)

	ago9dAS := NewMockUnitSummaryAllocationSet(ago9d, day)
	ago9dAS.Insert(NewMockUnitSummaryAllocation("4", ago9d, day, nil))
	ago8dAS := NewMockUnitSummaryAllocationSet(ago8d, day)
	ago8dAS.Insert(NewMockUnitSummaryAllocation("4", ago8d, day, nil))
	ago7dAS := NewMockUnitSummaryAllocationSet(ago7d, day)
	ago7dAS.Insert(NewMockUnitSummaryAllocation("4", ago7d, day, nil))
	ago6dAS := NewMockUnitSummaryAllocationSet(ago6d, day)
	ago6dAS.Insert(NewMockUnitSummaryAllocation("4", ago6d, day, nil))
	ago5dAS := NewMockUnitSummaryAllocationSet(ago5d, day)
	ago5dAS.Insert(NewMockUnitSummaryAllocation("4", ago5d, day, nil))
	ago4dAS := NewMockUnitSummaryAllocationSet(ago4d, day)
	ago4dAS.Insert(NewMockUnitSummaryAllocation("4", ago4d, day, nil))
	ago3dAS := NewMockUnitSummaryAllocationSet(ago3d, day)
	ago3dAS.Insert(NewMockUnitSummaryAllocation("4", ago3d, day, nil))
	ago2dAS := NewMockUnitSummaryAllocationSet(ago2d, day)
	ago2dAS.Insert(NewMockUnitSummaryAllocation("4", ago2d, day, nil))
	yesterdayAS := NewMockUnitSummaryAllocationSet(yesterday, day)
	yesterdayAS.Insert(NewMockUnitSummaryAllocation("", yesterday, day, nil))
	todayAS := NewMockUnitSummaryAllocationSet(today, day)
	todayAS.Insert(NewMockUnitSummaryAllocation("4", today, day, nil))

	asr := NewSummaryAllocationSetRange(ago9dAS, ago8dAS, ago7dAS, ago6dAS, ago5dAS, ago4dAS, ago3dAS, ago2dAS, yesterdayAS, todayAS)
	asr, err := asr.Accumulate(AccumulateOptionWeek)
	if err != nil {
		t.Fatalf("unexpected error calling accumulateBy: %s", err)
	}

	if len(asr.SummaryAllocationSets) != 2 && len(asr.SummaryAllocationSets) != 3 {
		t.Fatalf("expected 2 allocation sets, got:%d", len(asr.SummaryAllocationSets))
	}

	for _, as := range asr.SummaryAllocationSets {
		if as.Window.Duration() < time.Hour*24 || as.Window.Duration() > time.Hour*24*7 {
			t.Fatalf("expected window duration to be between 1 and 7 days, got:%s", as.Window.Duration().String())
		}
	}
}

func TestSummaryAllocationSetRange_AccumulateBy_Month(t *testing.T) {
	prevMonth1stDay := time.Date(2020, 01, 29, 0, 0, 0, 0, time.UTC)
	prevMonth2ndDay := time.Date(2020, 01, 30, 0, 0, 0, 0, time.UTC)
	prevMonth3ndDay := time.Date(2020, 01, 31, 0, 0, 0, 0, time.UTC)
	nextMonth1stDay := time.Date(2020, 02, 01, 0, 0, 0, 0, time.UTC)

	prev1AS := NewMockUnitSummaryAllocationSet(prevMonth1stDay, day)
	prev1AS.Insert(NewMockUnitSummaryAllocation("", prevMonth1stDay, day, nil))
	prev2AS := NewMockUnitSummaryAllocationSet(prevMonth2ndDay, day)
	prev2AS.Insert(NewMockUnitSummaryAllocation("", prevMonth2ndDay, day, nil))
	prev3AS := NewMockUnitSummaryAllocationSet(prevMonth3ndDay, day)
	prev3AS.Insert(NewMockUnitSummaryAllocation("", prevMonth3ndDay, day, nil))
	nextAS := NewMockUnitSummaryAllocationSet(nextMonth1stDay, day)
	nextAS.Insert(NewMockUnitSummaryAllocation("", nextMonth1stDay, day, nil))
	asr := NewSummaryAllocationSetRange(prev1AS, prev2AS, prev3AS, nextAS)
	asr, err := asr.Accumulate(AccumulateOptionMonth)
	if err != nil {
		t.Fatalf("unexpected error calling accumulateBy: %s", err)
	}

	if len(asr.SummaryAllocationSets) != 2 {
		t.Fatalf("expected 2 allocation sets, got:%d", len(asr.SummaryAllocationSets))
	}

	for _, as := range asr.SummaryAllocationSets {
		if as.Window.Duration() < time.Hour*24 || as.Window.Duration() > time.Hour*24*31 {
			t.Fatalf("expected window duration to be between 1 and 7 days, got:%s", as.Window.Duration().String())
		}
	}
}
