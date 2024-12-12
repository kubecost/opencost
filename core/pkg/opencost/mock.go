package opencost

import (
	"fmt"
	"time"
)

const gb = 1024 * 1024 * 1024
const day = 24 * time.Hour

var disk = PVKey{
	Cluster: "cluster1",
	Name:    "pv1",
}

// NewMockUnitAllocation creates an *Allocation with all of its float64 values set to 1 and generic properties if not provided in arg
func NewMockUnitAllocation(name string, start time.Time, resolution time.Duration, props *AllocationProperties) *Allocation {
	if name == "" {
		name = "cluster1/namespace1/pod1/container1"
	}

	properties := &AllocationProperties{}
	if props == nil {
		properties.Cluster = "cluster1"
		properties.Node = "node1"
		properties.Namespace = "namespace1"
		properties.ControllerKind = "deployment"
		properties.Controller = "deployment1"
		properties.Pod = "pod1"
		properties.Container = "container1"
	} else {
		properties = props
	}

	end := start.Add(resolution)

	alloc := &Allocation{
		Name:                  name,
		Properties:            properties,
		Window:                NewWindow(&start, &end).Clone(),
		Start:                 start,
		End:                   end,
		CPUCoreHours:          1,
		CPUCost:               1,
		CPUCoreRequestAverage: 1,
		CPUCoreUsageAverage:   1,
		GPUHours:              1,
		GPUCost:               1,
		NetworkCost:           1,
		LoadBalancerCost:      1,
		PVs: PVAllocations{
			disk: {
				ByteHours: 1,
				Cost:      1,
			},
		},
		RAMByteHours:           1,
		RAMCost:                1,
		RAMBytesRequestAverage: 1,
		RAMBytesUsageAverage:   1,
		RawAllocationOnly: &RawAllocationOnlyData{
			CPUCoreUsageMax:  1,
			RAMBytesUsageMax: 1,
		},
		ProportionalAssetResourceCosts: nil,
	}

	// If idle allocation, remove non-idle costs, but maintain total cost
	if alloc.IsIdle() {
		alloc.PVs = nil
		alloc.NetworkCost = 0.0
		alloc.LoadBalancerCost = 0.0
		alloc.CPUCoreHours += 1.0
		alloc.CPUCost += 1.0
		alloc.RAMByteHours += 1.0
		alloc.RAMCost += 1.0
	}

	return alloc
}

// GenerateMockAllocationSetClusterIdle creates generic allocation set which includes an idle set broken down by cluster
func GenerateMockAllocationSetClusterIdle(start time.Time) *AllocationSet {
	// Cluster Idle allocations
	a1i := NewMockUnitAllocation(fmt.Sprintf("cluster1/%s", IdleSuffix), start, day, &AllocationProperties{
		Cluster: "cluster1",
	})
	a1i.CPUCost = 5.0
	a1i.RAMCost = 15.0
	a1i.GPUCost = 0.0

	a2i := NewMockUnitAllocation(fmt.Sprintf("cluster2/%s", IdleSuffix), start, day, &AllocationProperties{
		Cluster: "cluster2",
	})
	a2i.CPUCost = 5.0
	a2i.RAMCost = 5.0
	a2i.GPUCost = 0.0

	as := GenerateMockAllocationSet(start)
	as.Insert(a1i)
	as.Insert(a2i)
	return as
}

// GenerateMockAllocationSetNodeIdle creates generic allocation set which includes an idle set broken down by node
func GenerateMockAllocationSetNodeIdle(start time.Time) *AllocationSet {
	// Node Idle allocations
	a11i := NewMockUnitAllocation(fmt.Sprintf("c1nodes/%s", IdleSuffix), start, day, &AllocationProperties{
		Cluster:    "cluster1",
		Node:       "c1nodes",
		ProviderID: "c1nodes",
	})
	a11i.CPUCost = 5.0
	a11i.RAMCost = 15.0
	a11i.GPUCost = 0.0

	a21i := NewMockUnitAllocation(fmt.Sprintf("node1/%s", IdleSuffix), start, day, &AllocationProperties{
		Cluster:    "cluster2",
		Node:       "node1",
		ProviderID: "node1",
	})
	a21i.CPUCost = 1.666667
	a21i.RAMCost = 1.666667
	a21i.GPUCost = 0.0

	a22i := NewMockUnitAllocation(fmt.Sprintf("node2/%s", IdleSuffix), start, day, &AllocationProperties{
		Cluster:    "cluster2",
		Node:       "node2",
		ProviderID: "node2",
	})
	a22i.CPUCost = 1.666667
	a22i.RAMCost = 1.666667
	a22i.GPUCost = 0.0

	a23i := NewMockUnitAllocation(fmt.Sprintf("node3/%s", IdleSuffix), start, day, &AllocationProperties{
		Cluster:    "cluster2",
		Node:       "node3",
		ProviderID: "node3",
		Namespace:  "",
	})
	a23i.CPUCost = 1.666667
	a23i.RAMCost = 1.666667
	a23i.GPUCost = 0.0

	as := GenerateMockAllocationSet(start)
	as.Insert(a11i)
	as.Insert(a21i)
	as.Insert(a22i)
	as.Insert(a23i)
	return as
}

// GenerateMockAllocationSet creates generic allocation set without idle allocations
func GenerateMockAllocationSet(start time.Time) *AllocationSet {

	// Active allocations
	a1111 := NewMockUnitAllocation("cluster1/namespace1/pod1/container1", start, day, &AllocationProperties{
		Cluster:    "cluster1",
		Namespace:  "namespace1",
		Pod:        "pod1",
		Container:  "container1",
		ProviderID: "c1nodes",
		Node:       "c1nodes",
	})
	a1111.RAMCost = 11.00
	a1111.PVs = PVAllocations{
		PVKey{Cluster: "cluster1", Name: "pv-a1111"}: {
			ByteHours: 1,
			Cost:      1,
		},
	}

	a11abc2 := NewMockUnitAllocation("cluster1/namespace1/pod-abc/container2", start, day, &AllocationProperties{
		Cluster:    "cluster1",
		Namespace:  "namespace1",
		Pod:        "pod-abc",
		Container:  "container2",
		ProviderID: "c1nodes",
		Node:       "c1nodes",
	})
	a11abc2.PVs = PVAllocations{
		PVKey{Cluster: "cluster1", Name: "pv-a11abc2"}: {
			ByteHours: 1,
			Cost:      1,
		},
	}

	a11def3 := NewMockUnitAllocation("cluster1/namespace1/pod-def/container3", start, day, &AllocationProperties{
		Cluster:    "cluster1",
		Namespace:  "namespace1",
		Pod:        "pod-def",
		Container:  "container3",
		ProviderID: "c1nodes",
		Node:       "c1nodes",
	})
	a11def3.PVs = PVAllocations{
		PVKey{Cluster: "cluster1", Name: "pv-a11def3"}: {
			ByteHours: 1,
			Cost:      1,
		},
	}

	a12ghi4 := NewMockUnitAllocation("cluster1/namespace2/pod-ghi/container4", start, day, &AllocationProperties{
		Cluster:    "cluster1",
		Namespace:  "namespace2",
		Pod:        "pod-ghi",
		Container:  "container4",
		ProviderID: "c1nodes",
		Node:       "c1nodes",
	})
	a12ghi4.PVs = PVAllocations{
		PVKey{Cluster: "cluster1", Name: "pv-a12ghi4"}: {
			ByteHours: 1,
			Cost:      1,
		},
	}

	a12ghi5 := NewMockUnitAllocation("cluster1/namespace2/pod-ghi/container5", start, day, &AllocationProperties{
		Cluster:    "cluster1",
		Namespace:  "namespace2",
		Pod:        "pod-ghi",
		Container:  "container5",
		ProviderID: "c1nodes",
		Node:       "c1nodes",
	})
	a12ghi5.PVs = PVAllocations{
		PVKey{Cluster: "cluster1", Name: "pv-a12ghi5"}: {
			ByteHours: 1,
			Cost:      1,
		},
	}

	a12jkl6 := NewMockUnitAllocation("cluster1/namespace2/pod-jkl/container6", start, day, &AllocationProperties{
		Cluster:    "cluster1",
		Namespace:  "namespace2",
		Pod:        "pod-jkl",
		Container:  "container6",
		ProviderID: "c1nodes",
		Node:       "c1nodes",
	})
	a12jkl6.PVs = PVAllocations{
		PVKey{Cluster: "cluster1", Name: "pv-a12jkl6"}: {
			ByteHours: 1,
			Cost:      1,
		},
	}

	a22mno4 := NewMockUnitAllocation("cluster2/namespace2/pod-mno/container4", start, day, &AllocationProperties{
		Cluster:    "cluster2",
		Namespace:  "namespace2",
		Pod:        "pod-mno",
		Container:  "container4",
		ProviderID: "node1",
		Node:       "node1",
	})
	a22mno4.PVs = PVAllocations{
		PVKey{Cluster: "cluster2", Name: "pv-a22mno4"}: {
			ByteHours: 1,
			Cost:      1,
		},
	}

	a22mno5 := NewMockUnitAllocation("cluster2/namespace2/pod-mno/container5", start, day, &AllocationProperties{
		Cluster:    "cluster2",
		Namespace:  "namespace2",
		Pod:        "pod-mno",
		Container:  "container5",
		ProviderID: "node1",
		Node:       "node1",
	})
	a22mno5.PVs = PVAllocations{
		PVKey{Cluster: "cluster2", Name: "pv-a22mno5"}: {
			ByteHours: 1,
			Cost:      1,
		},
	}

	a22pqr6 := NewMockUnitAllocation("cluster2/namespace2/pod-pqr/container6", start, day, &AllocationProperties{
		Cluster:    "cluster2",
		Namespace:  "namespace2",
		Pod:        "pod-pqr",
		Container:  "container6",
		ProviderID: "node2",
		Node:       "node2",
	})
	a22pqr6.PVs = PVAllocations{
		PVKey{Cluster: "cluster2", Name: "pv-a22pqr6"}: {
			ByteHours: 1,
			Cost:      1,
		},
	}

	a23stu7 := NewMockUnitAllocation("cluster2/namespace3/pod-stu/container7", start, day, &AllocationProperties{
		Cluster:    "cluster2",
		Namespace:  "namespace3",
		Pod:        "pod-stu",
		Container:  "container7",
		ProviderID: "node2",
		Node:       "node2",
	})
	a23stu7.PVs = PVAllocations{
		PVKey{Cluster: "cluster2", Name: "pv-a23stu7"}: {
			ByteHours: 1,
			Cost:      1,
		},
	}

	a23vwx8 := NewMockUnitAllocation("cluster2/namespace3/pod-vwx/container8", start, day, &AllocationProperties{
		Cluster:    "cluster2",
		Namespace:  "namespace3",
		Pod:        "pod-vwx",
		Container:  "container8",
		ProviderID: "node3",
		Node:       "node3",
	})
	a23vwx8.PVs = PVAllocations{
		PVKey{Cluster: "cluster2", Name: "pv-a23vwx8"}: {
			ByteHours: 1,
			Cost:      1,
		},
	}

	a23vwx9 := NewMockUnitAllocation("cluster2/namespace3/pod-vwx/container9", start, day, &AllocationProperties{
		Cluster:    "cluster2",
		Namespace:  "namespace3",
		Pod:        "pod-vwx",
		Container:  "container9",
		ProviderID: "node3",
		Node:       "node3",
	})
	a23vwx9.PVs = PVAllocations{
		PVKey{Cluster: "cluster2", Name: "pv-a23vwx9"}: {
			ByteHours: 1,
			Cost:      1,
		},
	}

	// Controllers

	a11abc2.Properties.ControllerKind = "deployment"
	a11abc2.Properties.Controller = "deployment1"
	a11def3.Properties.ControllerKind = "deployment"
	a11def3.Properties.Controller = "deployment1"

	a12ghi4.Properties.ControllerKind = "deployment"
	a12ghi4.Properties.Controller = "deployment2"
	a12ghi5.Properties.ControllerKind = "deployment"
	a12ghi5.Properties.Controller = "deployment2"
	a22mno4.Properties.ControllerKind = "deployment"
	a22mno4.Properties.Controller = "deployment2"
	a22mno5.Properties.ControllerKind = "deployment"
	a22mno5.Properties.Controller = "deployment2"

	a23stu7.Properties.ControllerKind = "deployment"
	a23stu7.Properties.Controller = "deployment3"

	a12jkl6.Properties.ControllerKind = "daemonset"
	a12jkl6.Properties.Controller = "daemonset1"
	a22pqr6.Properties.ControllerKind = "daemonset"
	a22pqr6.Properties.Controller = "daemonset1"

	a23vwx8.Properties.ControllerKind = "statefulset"
	a23vwx8.Properties.Controller = "statefulset1"
	a23vwx9.Properties.ControllerKind = "statefulset"
	a23vwx9.Properties.Controller = "statefulset1"

	// Labels

	a1111.Properties.Labels = map[string]string{"app": "app1", "env": "env1"}
	a12ghi4.Properties.Labels = map[string]string{"app": "app2", "env": "env2"}
	a12ghi5.Properties.Labels = map[string]string{"app": "app2", "env": "env2"}
	a22mno4.Properties.Labels = map[string]string{"app": "app2"}
	a22mno5.Properties.Labels = map[string]string{"app": "app2"}

	//Annotations
	a23stu7.Properties.Annotations = map[string]string{"team": "team1"}
	a23vwx8.Properties.Annotations = map[string]string{"team": "team2"}
	a23vwx9.Properties.Annotations = map[string]string{"team": "team1"}

	// Services
	a12jkl6.Properties.Services = []string{"service1"}
	a22pqr6.Properties.Services = []string{"service1"}

	return NewAllocationSet(start, start.Add(day),
		// cluster 1, namespace1
		a1111, a11abc2, a11def3,
		// cluster 1, namespace 2
		a12ghi4, a12ghi5, a12jkl6,
		// cluster 2, namespace 2
		a22mno4, a22mno5, a22pqr6,
		// cluster 2, namespace 3
		a23stu7, a23vwx8, a23vwx9,
	)
}

// GenerateMockAllocationSetWithAssetProperties with no idle and connections to Assets in properties
func GenerateMockAllocationSetWithAssetProperties(start time.Time) *AllocationSet {
	as := GenerateMockAllocationSet(start)
	disk1 := PVKey{
		Cluster: "cluster2",
		Name:    "disk1",
	}
	disk2 := PVKey{
		Cluster: "cluster2",
		Name:    "disk2",
	}
	for _, a := range as.Allocations {
		// add reconcilable pvs to pod-mno
		if a.Properties.Pod == "pod-mno" {
			a.PVs = a.PVs.Add(PVAllocations{
				disk1: {
					Cost:      2.5,
					ByteHours: 2.5 * gb,
				},
				disk2: {
					Cost:      5,
					ByteHours: 5 * gb,
				},
			})
		}
		// add loadBalancer service to allocations
		if a.Name == "cluster2/namespace2/pod-mno/container4" {
			a.Properties.Services = append(a.Properties.Services, "loadBalancer1")
		}
		if a.Name == "cluster2/namespace2/pod-mno/container5" {
			a.Properties.Services = append(a.Properties.Services, "loadBalancer2")
		}
		if a.Name == "cluster2/namespace2/pod-pqr/container6" {
			a.Properties.Services = append(a.Properties.Services, "loadBalancer1")
			a.Properties.Services = append(a.Properties.Services, "loadBalancer2")
		}
	}
	return as
}

// GenerateMockAssetSets creates generic AssetSets
func GenerateMockAssetSets(start, end time.Time) []*AssetSet {
	var assetSets []*AssetSet

	// Create an AssetSet representing cluster costs for two clusters (cluster1
	// and cluster2). Include Nodes and Disks for both, even though only
	// Nodes will be counted. Whereas in practice, Assets should be aggregated
	// by type, here we will provide multiple Nodes for one of the clusters to
	// make sure the function still holds.

	// NOTE: we're re-using GenerateMockAllocationSet so this has to line up with
	// the allocated node costs from that function. See table above.

	// | Hierarchy                               | Cost |  CPU |  RAM |  GPU | Adjustment |
	// +-----------------------------------------+------+------+------+------+------------+
	//   cluster1:
	//     nodes                                  100.00  55.00  44.00  11.00      -10.00
	// +-----------------------------------------+------+------+------+------+------------+
	//   cluster1 subtotal (adjusted)             100.00  50.00  40.00  10.00        0.00
	// +-----------------------------------------+------+------+------+------+------------+
	//   cluster1 allocated                        48.00   6.00  16.00   6.00        0.00
	// +-----------------------------------------+------+------+------+------+------------+
	//   cluster1 idle                             72.00  44.00  24.00   4.00        0.00
	// +-----------------------------------------+------+------+------+------+------------+
	//   cluster2:
	//     node1                                   35.00  20.00  15.00   0.00        0.00
	//     node2                                   35.00  20.00  15.00   0.00        0.00
	//     node3                                   30.00  10.00  10.00  10.00        0.00
	//     (disks should not matter for idle)
	// +-----------------------------------------+------+------+------+------+------------+
	//   cluster2 subtotal                        100.00  50.00  40.00  10.00        0.00
	// +-----------------------------------------+------+------+------+------+------------+
	//   cluster2 allocated                        28.00   6.00   6.00   6.00        0.00
	// +-----------------------------------------+------+------+------+------+------------+
	//   cluster2 idle                             82.00  44.00  34.00   4.00        0.00
	// +-----------------------------------------+------+------+------+------+------------+

	cluster1Nodes := NewNode("c1nodes", "cluster1", "c1nodes", start, end, NewWindow(&start, &end))
	cluster1Nodes.CPUCost = 55.0
	cluster1Nodes.RAMCost = 44.0
	cluster1Nodes.GPUCost = 11.0
	cluster1Nodes.Adjustment = -10.00
	cluster1Nodes.CPUCoreHours = 8
	cluster1Nodes.RAMByteHours = 6
	cluster1Nodes.GPUHours = 24

	cluster2Node1 := NewNode("node1", "cluster2", "node1", start, end, NewWindow(&start, &end))
	cluster2Node1.CPUCost = 20.0
	cluster2Node1.RAMCost = 15.0
	cluster2Node1.GPUCost = 0.0
	cluster2Node1.CPUCoreHours = 4
	cluster2Node1.RAMByteHours = 3
	cluster2Node1.GPUHours = 0

	cluster2Node2 := NewNode("node2", "cluster2", "node2", start, end, NewWindow(&start, &end))
	cluster2Node2.CPUCost = 20.0
	cluster2Node2.RAMCost = 15.0
	cluster2Node2.GPUCost = 0.0
	cluster2Node2.CPUCoreHours = 3
	cluster2Node2.RAMByteHours = 2
	cluster2Node2.GPUHours = 0

	cluster2Node3 := NewNode("node3", "cluster2", "node3", start, end, NewWindow(&start, &end))
	cluster2Node3.CPUCost = 10.0
	cluster2Node3.RAMCost = 10.0
	cluster2Node3.GPUCost = 10.0
	cluster2Node3.CPUCoreHours = 2
	cluster2Node3.RAMByteHours = 2
	cluster2Node3.GPUHours = 24

	// Add PVs
	cluster2Disk1 := NewDisk("disk1", "cluster2", "disk1", start, end, NewWindow(&start, &end))
	cluster2Disk1.Cost = 5.0
	cluster2Disk1.Adjustment = 1.0
	cluster2Disk1.ByteHours = 5 * gb

	cluster2Disk2 := NewDisk("disk2", "cluster2", "disk2", start, end, NewWindow(&start, &end))
	cluster2Disk2.Cost = 10.0
	cluster2Disk2.Adjustment = 3.0
	cluster2Disk2.ByteHours = 10 * gb

	cluster2Node1Disk := NewDisk("node1", "cluster2", "node1", start, end, NewWindow(&start, &end))
	cluster2Node1Disk.Cost = 1.0
	cluster2Node1Disk.ByteHours = 5 * gb

	// Add Attached Disks
	cluster2Node2Disk := NewDisk("node2", "cluster2", "node2", start, end, NewWindow(&start, &end))
	cluster2Node2Disk.Cost = 2.0
	cluster2Node2Disk.ByteHours = 5 * gb

	cluster2Node3Disk := NewDisk("node3", "cluster2", "node3", start, end, NewWindow(&start, &end))
	cluster2Node3Disk.Cost = 3.0
	cluster2Node3Disk.ByteHours = 5 * gb

	// Add Cluster Management
	cluster1ClusterManagement := NewClusterManagement("", "cluster1", NewWindow(&start, &end))
	cluster1ClusterManagement.Cost = 2.0

	cluster2ClusterManagement := NewClusterManagement("", "cluster2", NewWindow(&start, &end))
	cluster2ClusterManagement.Cost = 2.0

	// Add Networks
	c1Network := NewNetwork("", "cluster1", "c1nodes", start, end, NewWindow(&start, &end))
	c1Network.Cost = 3.0

	node1Network := NewNetwork("node1", "cluster2", "node1", start, end, NewWindow(&start, &end))
	node1Network.Cost = 4.0

	node2Network := NewNetwork("node2", "cluster2", "node2", start, end, NewWindow(&start, &end))
	node2Network.Cost = 5.0

	node3Network := NewNetwork("node3", "cluster2", "node3", start, end, NewWindow(&start, &end))
	node3Network.Cost = 2.0

	// Add LoadBalancers
	cluster2LoadBalancer1 := NewLoadBalancer("namespace2/loadBalancer1", "cluster2", "lb1", start, end, NewWindow(&start, &end), false, "127.0.0.1")
	cluster2LoadBalancer1.Cost = 10.0

	cluster2LoadBalancer2 := NewLoadBalancer("namespace2/loadBalancer2", "cluster2", "lb2", start, end, NewWindow(&start, &end), false, "127.0.0.1")
	cluster2LoadBalancer2.Cost = 15.0

	assetSet1 := NewAssetSet(start, end, cluster1Nodes, cluster2Node1, cluster2Node2, cluster2Node3, cluster2Disk1,
		cluster2Disk2, cluster2Node1Disk, cluster2Node2Disk, cluster2Node3Disk, cluster1ClusterManagement,
		cluster2ClusterManagement, c1Network, node1Network, node2Network, node3Network, cluster2LoadBalancer1, cluster2LoadBalancer2)
	assetSets = append(assetSets, assetSet1)

	// NOTE: we're re-using GenerateMockAllocationSet so this has to line up with
	// the allocated node costs from that function. See table above.

	// | Hierarchy                               | Cost |  CPU |  RAM |  GPU | Adjustment |
	// +-----------------------------------------+------+------+------+------+------------+
	//   cluster1:
	//     nodes                                  100.00   5.00   4.00   1.00       90.00
	// +-----------------------------------------+------+------+------+------+------------+
	//   cluster1 subtotal (adjusted)             100.00  50.00  40.00  10.00        0.00
	// +-----------------------------------------+------+------+------+------+------------+
	//   cluster1 allocated                        48.00   6.00  16.00   6.00        0.00
	// +-----------------------------------------+------+------+------+------+------------+
	//   cluster1 idle                             72.00  44.00  24.00   4.00        0.00
	// +-----------------------------------------+------+------+------+------+------------+
	//   cluster2:
	//     node1                                   35.00  20.00  15.00   0.00        0.00
	//     node2                                   35.00  20.00  15.00   0.00        0.00
	//     node3                                   30.00  10.00  10.00  10.00        0.00
	//     (disks should not matter for idle)
	// +-----------------------------------------+------+------+------+------+------------+
	//   cluster2 subtotal                        100.00  50.00  40.00  10.00        0.00
	// +-----------------------------------------+------+------+------+------+------------+
	//   cluster2 allocated                        28.00   6.00   6.00   6.00        0.00
	// +-----------------------------------------+------+------+------+------+------------+
	//   cluster2 idle                             82.00  44.00  34.00   4.00        0.00
	// +-----------------------------------------+------+------+------+------+------------+

	cluster1Nodes = NewNode("", "cluster1", "c1nodes", start, end, NewWindow(&start, &end))
	cluster1Nodes.CPUCost = 5.0
	cluster1Nodes.RAMCost = 4.0
	cluster1Nodes.GPUCost = 1.0
	cluster1Nodes.Adjustment = 90.00
	cluster1Nodes.CPUCoreHours = 8
	cluster1Nodes.RAMByteHours = 6
	cluster1Nodes.GPUHours = 24

	cluster2Node1 = NewNode("node1", "cluster2", "node1", start, end, NewWindow(&start, &end))
	cluster2Node1.CPUCost = 20.0
	cluster2Node1.RAMCost = 15.0
	cluster2Node1.GPUCost = 0.0
	cluster2Node1.CPUCoreHours = 4
	cluster2Node1.RAMByteHours = 3
	cluster2Node1.GPUHours = 0

	cluster2Node2 = NewNode("node2", "cluster2", "node2", start, end, NewWindow(&start, &end))
	cluster2Node2.CPUCost = 20.0
	cluster2Node2.RAMCost = 15.0
	cluster2Node2.GPUCost = 0.0
	cluster2Node2.CPUCoreHours = 3
	cluster2Node2.RAMByteHours = 2
	cluster2Node2.GPUHours = 0

	cluster2Node3 = NewNode("node3", "cluster2", "node3", start, end, NewWindow(&start, &end))
	cluster2Node3.CPUCost = 10.0
	cluster2Node3.RAMCost = 10.0
	cluster2Node3.GPUCost = 10.0
	cluster2Node3.CPUCoreHours = 2
	cluster2Node3.RAMByteHours = 2
	cluster2Node3.GPUHours = 24

	// Add PVs
	cluster2Disk1 = NewDisk("disk1", "cluster2", "disk1", start, end, NewWindow(&start, &end))
	cluster2Disk1.Cost = 5.0
	cluster2Disk1.Adjustment = 1.0
	cluster2Disk1.ByteHours = 5 * gb

	cluster2Disk2 = NewDisk("disk2", "cluster2", "disk2", start, end, NewWindow(&start, &end))
	cluster2Disk2.Cost = 12.0
	cluster2Disk2.Adjustment = 4.0
	cluster2Disk2.ByteHours = 20 * gb

	assetSet2 := NewAssetSet(start, end, cluster1Nodes, cluster2Node1, cluster2Node2, cluster2Node3, cluster2Disk1,
		cluster2Disk2, cluster2Node1Disk, cluster2Node2Disk, cluster2Node3Disk, cluster1ClusterManagement,
		cluster2ClusterManagement, c1Network, node1Network, node2Network, node3Network, cluster2LoadBalancer1, cluster2LoadBalancer2)
	assetSets = append(assetSets, assetSet2)
	return assetSets
}

// GenerateMockAssetSet generates the following topology:
//
// | Asset                        | Cost |  Adj |
// +------------------------------+------+------+
//
//	cluster1:
//	  node1:                        6.00   1.00
//	  node2:                        4.00   1.50
//	  node3:                        7.00  -0.50
//	  disk1:                        2.50   0.00
//	  disk2:                        1.50   0.00
//	  clusterManagement1:           3.00   0.00
//
// +------------------------------+------+------+
//
//	cluster1 subtotal              24.00   2.00
//
// +------------------------------+------+------+
//
//	cluster2:
//	  node4:                       12.00  -1.00
//	  disk3:                        2.50   0.00
//	  disk4:                        1.50   0.00
//	  clusterManagement2:           0.00   0.00
//
// +------------------------------+------+------+
//
//	cluster2 subtotal              16.00  -1.00
//
// +------------------------------+------+------+
//
//	cluster3:
//	  node5:                       17.00   2.00
//
// +------------------------------+------+------+
//
//	cluster3 subtotal              17.00   2.00
//
// +------------------------------+------+------+
//
//	total                          57.00   3.00
//
// +------------------------------+------+------+
func GenerateMockAssetSet(start time.Time, duration time.Duration) *AssetSet {
	end := start.Add(duration)
	window := NewWindow(&start, &end)

	hours := window.Duration().Hours()

	node1 := NewNode("node1", "cluster1", "gcp-node1", *window.Clone().start, *window.Clone().end, window.Clone())
	node1.CPUCost = 4.0
	node1.RAMCost = 4.0
	node1.GPUCost = 2.0
	node1.Discount = 0.5
	node1.CPUCoreHours = 2.0 * hours
	node1.RAMByteHours = 4.0 * gb * hours
	node1.GPUHours = 1.0 * hours
	node1.SetAdjustment(1.0)
	node1.SetLabels(map[string]string{"test": "test"})

	node2 := NewNode("node2", "cluster1", "gcp-node2", *window.Clone().start, *window.Clone().end, window.Clone())
	node2.CPUCost = 4.0
	node2.RAMCost = 4.0
	node2.GPUCost = 0.0
	node2.Discount = 0.5
	node2.CPUCoreHours = 2.0 * hours
	node2.RAMByteHours = 4.0 * gb * hours
	node2.GPUHours = 0.0 * hours
	node2.SetAdjustment(1.5)

	node3 := NewNode("node3", "cluster1", "gcp-node3", *window.Clone().start, *window.Clone().end, window.Clone())
	node3.CPUCost = 4.0
	node3.RAMCost = 4.0
	node3.GPUCost = 3.0
	node3.Discount = 0.5
	node3.CPUCoreHours = 2.0 * hours
	node3.RAMByteHours = 4.0 * gb * hours
	node3.GPUHours = 2.0 * hours
	node3.SetAdjustment(-0.5)

	node4 := NewNode("node4", "cluster2", "gcp-node4", *window.Clone().start, *window.Clone().end, window.Clone())
	node4.CPUCost = 10.0
	node4.RAMCost = 6.0
	node4.GPUCost = 0.0
	node4.Discount = 0.25
	node4.CPUCoreHours = 4.0 * hours
	node4.RAMByteHours = 12.0 * gb * hours
	node4.GPUHours = 0.0 * hours
	node4.SetAdjustment(-1.0)

	node5 := NewNode("node5", "cluster3", "aws-node5", *window.Clone().start, *window.Clone().end, window.Clone())
	node5.CPUCost = 10.0
	node5.RAMCost = 7.0
	node5.GPUCost = 0.0
	node5.Discount = 0.0
	node5.CPUCoreHours = 8.0 * hours
	node5.RAMByteHours = 24.0 * gb * hours
	node5.GPUHours = 0.0 * hours
	node5.SetAdjustment(2.0)

	disk1 := NewDisk("disk1", "cluster1", "gcp-disk1", *window.Clone().start, *window.Clone().end, window.Clone())
	disk1.Cost = 2.5
	disk1.ByteHours = 100 * gb * hours

	disk2 := NewDisk("disk2", "cluster1", "gcp-disk2", *window.Clone().start, *window.Clone().end, window.Clone())
	disk2.Cost = 1.5
	disk2.ByteHours = 60 * gb * hours

	disk3 := NewDisk("disk3", "cluster2", "gcp-disk3", *window.Clone().start, *window.Clone().end, window.Clone())
	disk3.Cost = 2.5
	disk3.ByteHours = 100 * gb * hours

	disk4 := NewDisk("disk4", "cluster2", "gcp-disk4", *window.Clone().start, *window.Clone().end, window.Clone())
	disk4.Cost = 1.5
	disk4.ByteHours = 100 * gb * hours

	cm1 := NewClusterManagement(GCPProvider, "cluster1", window.Clone())
	cm1.Cost = 3.0

	cm2 := NewClusterManagement(GCPProvider, "cluster2", window.Clone())
	cm2.Cost = 0.0

	return NewAssetSet(
		start, end,
		// cluster 1
		node1, node2, node3, disk1, disk2, cm1,
		// cluster 2
		node4, disk3, disk4, cm2,
		// cluster 3
		node5,
	)
}

// NewMockUnitSummaryAllocation creates an *SummaryAllocation with all of its float64 values set to 1 and generic properties if not provided in arg
func NewMockUnitSummaryAllocation(name string, start time.Time, resolution time.Duration, props *AllocationProperties) *SummaryAllocation {
	if name == "" {
		name = "cluster1/namespace1/pod1/container1"
	}

	properties := &AllocationProperties{}
	if props == nil {
		properties.Cluster = "cluster1"
		properties.Node = "node1"
		properties.Namespace = "namespace1"
		properties.ControllerKind = "deployment"
		properties.Controller = "deployment1"
		properties.Pod = "pod1"
		properties.Container = "container1"
	} else {
		properties = props
	}

	end := start.Add(resolution)

	alloc := &SummaryAllocation{
		Name:                   name,
		Properties:             properties,
		Start:                  start,
		End:                    end,
		CPUCost:                1,
		CPUCoreRequestAverage:  1,
		CPUCoreUsageAverage:    1,
		GPUCost:                1,
		NetworkCost:            1,
		LoadBalancerCost:       1,
		RAMCost:                1,
		RAMBytesRequestAverage: 1,
		RAMBytesUsageAverage:   1,
	}

	// If idle allocation, remove non-idle costs, but maintain total cost
	if alloc.IsIdle() {
		alloc.NetworkCost = 0.0
		alloc.LoadBalancerCost = 0.0
		alloc.CPUCost += 1.0
		alloc.RAMCost += 1.0
	}

	return alloc
}

// NewMockUnitSummaryAllocationSet creates an *SummaryAllocationSet
func NewMockUnitSummaryAllocationSet(start time.Time, resolution time.Duration) *SummaryAllocationSet {

	end := start.Add(resolution)
	sas := &SummaryAllocationSet{
		Window: NewWindow(&start, &end),
	}

	return sas
}
