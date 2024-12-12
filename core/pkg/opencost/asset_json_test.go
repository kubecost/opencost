package opencost

import (
	"github.com/opencost/opencost/core/pkg/util/json"

	"testing"
	"time"
)

var s = time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC)
var e = start1.Add(day)
var unmarshalWindow = NewWindow(&s, &e)

func TestAny_Unmarshal(t *testing.T) {

	any1 := NewAsset(*unmarshalWindow.start, *unmarshalWindow.end, unmarshalWindow)
	any1.SetProperties(&AssetProperties{
		Name:       "any1",
		Cluster:    "cluster1",
		ProviderID: "any1",
	})
	any1.Cost = 9.0
	any1.SetAdjustment(1.0)

	bytes, _ := json.Marshal(any1)

	var testany Any
	any2 := &testany

	err := json.Unmarshal(bytes, any2)

	// Check if unmarshal was successful
	if err != nil {
		t.Fatalf("Any Unmarshal: unexpected error: %s", err)
	}

	// Check if all fields in initial Any equal those in Any from unmarshal
	if !any1.Properties.Equal(any2.Properties) {
		t.Fatalf("Any Unmarshal: properties mutated in unmarshal")
	}
	if !any1.Labels.Equal(any2.Labels) {
		t.Fatalf("Any Unmarshal: labels mutated in unmarshal")
	}
	if !any1.Window.Equal(any2.Window) {
		t.Fatalf("Any Unmarshal: window mutated in unmarshal")
	}
	if !any1.Start.Equal(any2.Start) {
		t.Fatalf("Any Unmarshal: start mutated in unmarshal")
	}
	if !any1.End.Equal(any2.End) {
		t.Fatalf("Any Unmarshal: end mutated in unmarshal")
	}
	if any1.Adjustment != any2.Adjustment {
		t.Fatalf("Any Unmarshal: adjustment mutated in unmarshal")
	}
	if any1.Cost != any2.Cost {
		t.Fatalf("Any Unmarshal: cost mutated in unmarshal")
	}

	// As a final check, make sure the above checks out
	if !any1.Equal(any2) {
		t.Fatalf("Any Unmarshal: Any mutated in unmarshal")
	}

}

func TestCloud_Unmarshal(t *testing.T) {

	cloud1 := NewCloud("Compute", "provider1", *unmarshalWindow.start, *unmarshalWindow.end, unmarshalWindow)
	cloud1.SetLabels(map[string]string{
		"namespace": "namespace1",
		"env":       "env1",
		"product":   "product1",
	})
	cloud1.Cost = 10.00
	cloud1.Credit = -1.0

	bytes, _ := json.Marshal(cloud1)

	var testcloud Cloud
	cloud2 := &testcloud

	err := json.Unmarshal(bytes, cloud2)

	// Check if unmarshal was successful
	if err != nil {
		t.Fatalf("Cloud Unmarshal: unexpected error: %s", err)
	}

	// Check if all fields in initial Cloud equal those in Cloud from unmarshal
	if !cloud1.Properties.Equal(cloud2.Properties) {
		t.Fatalf("Cloud Unmarshal: properties mutated in unmarshal")
	}
	if !cloud1.Labels.Equal(cloud2.Labels) {
		t.Fatalf("Cloud Unmarshal: labels mutated in unmarshal")
	}
	if !cloud1.Window.Equal(cloud2.Window) {
		t.Fatalf("Cloud Unmarshal: window mutated in unmarshal")
	}
	if !cloud1.Start.Equal(cloud2.Start) {
		t.Fatalf("Cloud Unmarshal: start mutated in unmarshal")
	}
	if !cloud1.End.Equal(cloud2.End) {
		t.Fatalf("Cloud Unmarshal: end mutated in unmarshal")
	}
	if cloud1.Adjustment != cloud2.Adjustment {
		t.Fatalf("Cloud Unmarshal: adjustment mutated in unmarshal")
	}
	if cloud1.Cost != cloud2.Cost {
		t.Fatalf("Cloud Unmarshal: cost mutated in unmarshal")
	}
	if cloud1.Credit != cloud2.Credit {
		t.Fatalf("Cloud Unmarshal: credit mutated in unmarshal")
	}

	// As a final check, make sure the above checks out
	if !cloud1.Equal(cloud2) {
		t.Fatalf("Cloud Unmarshal: Cloud mutated in unmarshal")
	}

}

func TestClusterManagement_Unmarshal(t *testing.T) {

	cm1 := NewClusterManagement(GCPProvider, "cluster1", unmarshalWindow)
	cm1.Cost = 9.0

	bytes, _ := json.Marshal(cm1)

	var testcm ClusterManagement
	cm2 := &testcm

	err := json.Unmarshal(bytes, cm2)

	// Check if unmarshal was successful
	if err != nil {
		t.Fatalf("ClusterManagement Unmarshal: unexpected error: %s", err)
	}

	// Check if all fields in initial ClusterManagement equal those in ClusterManagement from unmarshal
	if !cm1.Properties.Equal(cm2.Properties) {
		t.Fatalf("ClusterManagement Unmarshal: properties mutated in unmarshal")
	}
	if !cm1.Labels.Equal(cm2.Labels) {
		t.Fatalf("ClusterManagement Unmarshal: labels mutated in unmarshal")
	}
	if !cm1.Window.Equal(cm2.Window) {
		t.Fatalf("ClusterManagement Unmarshal: window mutated in unmarshal")
	}
	if cm1.Cost != cm2.Cost {
		t.Fatalf("ClusterManagement Unmarshal: cost mutated in unmarshal")
	}

	// As a final check, make sure the above checks out
	if !cm1.Equal(cm2) {
		t.Fatalf("ClusterManagement Unmarshal: ClusterManagement mutated in unmarshal")
	}

}

func TestDisk_Unmarshal(t *testing.T) {

	hours := unmarshalWindow.Duration().Hours()

	disk1 := NewDisk("disk1", "cluster1", "disk1", *unmarshalWindow.start, *unmarshalWindow.end, unmarshalWindow)
	disk1.ByteHours = 60.0 * gb * hours
	used := 40.0 * gb * hours
	disk1.ByteHoursUsed = &used
	max := 50.0 * gb * hours
	disk1.ByteUsageMax = &max
	disk1.Cost = 4.0
	disk1.Local = 1.0
	disk1.SetAdjustment(1.0)
	disk1.Breakdown = &Breakdown{
		Idle:   0.1,
		System: 0.2,
		User:   0.3,
		Other:  0.4,
	}

	bytes, _ := json.Marshal(disk1)

	var testdisk Disk
	disk2 := &testdisk

	err := json.Unmarshal(bytes, disk2)

	// Check if unmarshal was successful
	if err != nil {
		t.Fatalf("Disk Unmarshal: unexpected error: %s", err)
	}

	// Check if all fields in initial Disk equal those in Disk from unmarshal
	if !disk1.Properties.Equal(disk2.Properties) {
		t.Fatalf("Disk Unmarshal: properties mutated in unmarshal")
	}
	if !disk1.Labels.Equal(disk2.Labels) {
		t.Fatalf("Disk Unmarshal: labels mutated in unmarshal")
	}
	if !disk1.Window.Equal(disk2.Window) {
		t.Fatalf("Disk Unmarshal: window mutated in unmarshal")
	}
	if !disk1.Breakdown.Equal(disk2.Breakdown) {
		t.Fatalf("Disk Unmarshal: Breakdown mutated in unmarshal")
	}
	if !disk1.Start.Equal(disk2.Start) {
		t.Fatalf("Disk Unmarshal: start mutated in unmarshal")
	}
	if !disk1.End.Equal(disk2.End) {
		t.Fatalf("Disk Unmarshal: end mutated in unmarshal")
	}
	if disk1.Adjustment != disk2.Adjustment {
		t.Fatalf("Disk Unmarshal: adjustment mutated in unmarshal")
	}
	if disk1.ByteHours != disk2.ByteHours {
		t.Fatalf("Disk Unmarshal: ByteHours mutated in unmarshal")
	}
	if *disk1.ByteHoursUsed != *disk2.ByteHoursUsed {
		t.Fatalf("Disk Unmarshal: ByteHoursUsed mutated in unmarshal")
	}
	if *disk1.ByteUsageMax != *disk2.ByteUsageMax {
		t.Fatalf("Disk Unmarshal: ByteUsageMax mutated in unmarshal")
	}
	if disk1.Cost != disk2.Cost {
		t.Fatalf("Disk Unmarshal: cost mutated in unmarshal")
	}

	// Local from Disk is not marhsaled, and cannot be calculated from marshaled values.
	// Currently, it is just ignored and not set in the resulting unmarshal to Disk. Thus,
	// it is also ignored in this test; be aware that this means a resulting Disk from an
	// unmarshal is therefore NOT equal to the originally marshaled Disk.

	disk3 := NewDisk("disk3", "cluster1", "disk3", *unmarshalWindow.start, *unmarshalWindow.end, unmarshalWindow)

	disk3.ByteHours = 60.0 * gb * hours
	disk3.ByteHoursUsed = nil
	disk3.ByteUsageMax = nil
	disk3.Cost = 4.0
	disk3.Local = 1.0
	disk3.SetAdjustment(1.0)
	disk3.Breakdown = &Breakdown{
		Idle:   0.1,
		System: 0.2,
		User:   0.3,
		Other:  0.4,
	}

	bytes, _ = json.Marshal(disk3)

	var testdisk2 Disk
	disk4 := &testdisk2

	err = json.Unmarshal(bytes, disk4)

	// Check if unmarshal was successful
	if err != nil {
		t.Fatalf("Disk Unmarshal: unexpected error: %s", err)
	}

	// Check that both disks have nil usage
	if disk3.ByteHoursUsed != disk4.ByteHoursUsed {
		t.Fatalf("Disk Unmarshal: ByteHoursUsed mutated in unmarshal")
	}
	// Check that both disks have nil max usage
	if disk3.ByteUsageMax != disk4.ByteUsageMax {
		t.Fatalf("Disk Unmarshal: ByteUsageMax mutated in unmarshal")
	}

}

func TestNetwork_Unmarshal(t *testing.T) {

	network1 := NewNetwork("network1", "cluster1", "provider1", *unmarshalWindow.start, *unmarshalWindow.end, unmarshalWindow)
	network1.Cost = 4.0
	network1.SetAdjustment(1.0)

	bytes, _ := json.Marshal(network1)

	var testnw Network
	network2 := &testnw

	err := json.Unmarshal(bytes, network2)

	// Check if unmarshal was successful
	if err != nil {
		t.Fatalf("Network Unmarshal: unexpected error: %s", err)
	}

	// Check if all fields in initial Network equal those in Network from unmarshal
	if !network1.Properties.Equal(network2.Properties) {
		t.Fatalf("Network Unmarshal: properties mutated in unmarshal")
	}
	if !network1.Labels.Equal(network2.Labels) {
		t.Fatalf("Network Unmarshal: labels mutated in unmarshal")
	}
	if !network1.Window.Equal(network2.Window) {
		t.Fatalf("Network Unmarshal: window mutated in unmarshal")
	}
	if !network1.Start.Equal(network2.Start) {
		t.Fatalf("Network Unmarshal: start mutated in unmarshal")
	}
	if !network1.End.Equal(network2.End) {
		t.Fatalf("Network Unmarshal: end mutated in unmarshal")
	}
	if network1.Adjustment != network2.Adjustment {
		t.Fatalf("Network Unmarshal: adjustment mutated in unmarshal")
	}
	if network1.Cost != network2.Cost {
		t.Fatalf("Network Unmarshal: cost mutated in unmarshal")
	}

	// As a final check, make sure the above checks out
	if !network1.Equal(network2) {
		t.Fatalf("Network Unmarshal: Network mutated in unmarshal")
	}

}

func TestNode_Unmarshal(t *testing.T) {

	hours := unmarshalWindow.Duration().Hours()

	node1 := NewNode("node1", "cluster1", "provider1", *unmarshalWindow.start, *unmarshalWindow.end, unmarshalWindow)
	node1.CPUCoreHours = 1.0 * hours
	node1.RAMByteHours = 2.0 * gb * hours
	node1.GPUHours = 0.0 * hours
	node1.GPUCost = 0.0
	node1.CPUCost = 8.0
	node1.RAMCost = 4.0
	node1.Discount = 0.3
	node1.CPUBreakdown = &Breakdown{
		Idle:   0.6,
		System: 0.2,
		User:   0.2,
		Other:  0.0,
	}
	node1.RAMBreakdown = &Breakdown{
		Idle:   0.6,
		System: 0.2,
		User:   0.2,
		Other:  0.0,
	}
	node1.SetAdjustment(1.6)

	bytes, _ := json.Marshal(node1)

	var testnode Node
	node2 := &testnode

	err := json.Unmarshal(bytes, node2)

	// Check if unmarshal was successful
	if err != nil {
		t.Fatalf("Node Unmarshal: unexpected error: %s", err)
	}

	// Check if all fields in initial Node equal those in Node from unmarshal
	if !node1.Properties.Equal(node2.Properties) {
		t.Fatalf("Node Unmarshal: properties mutated in unmarshal")
	}
	if !node1.Labels.Equal(node2.Labels) {
		t.Fatalf("Node Unmarshal: labels mutated in unmarshal")
	}
	if !node1.Window.Equal(node2.Window) {
		t.Fatalf("Node Unmarshal: window mutated in unmarshal")
	}
	if !node1.CPUBreakdown.Equal(node2.CPUBreakdown) {
		t.Fatalf("Node Unmarshal: CPUBreakdown mutated in unmarshal")
	}
	if !node1.RAMBreakdown.Equal(node2.RAMBreakdown) {
		t.Fatalf("Node Unmarshal: RAMBreakdown mutated in unmarshal")
	}
	if !node1.Start.Equal(node2.Start) {
		t.Fatalf("Node Unmarshal: start mutated in unmarshal")
	}
	if !node1.End.Equal(node2.End) {
		t.Fatalf("Node Unmarshal: end mutated in unmarshal")
	}
	if node1.Adjustment != node2.Adjustment {
		t.Fatalf("Node Unmarshal: adjustment mutated in unmarshal")
	}
	if node1.NodeType != node2.NodeType {
		t.Fatalf("Node Unmarshal: NodeType mutated in unmarshal")
	}
	if node1.CPUCoreHours != node2.CPUCoreHours {
		t.Fatalf("Node Unmarshal: CPUCoreHours mutated in unmarshal")
	}
	if node1.RAMByteHours != node2.RAMByteHours {
		t.Fatalf("Node Unmarshal: RAMByteHours mutated in unmarshal")
	}
	if node1.GPUHours != node2.GPUHours {
		t.Fatalf("Node Unmarshal: GPUHours mutated in unmarshal")
	}
	if node1.CPUCost != node2.CPUCost {
		t.Fatalf("Node Unmarshal: CPUCost mutated in unmarshal")
	}
	if node1.GPUCost != node2.GPUCost {
		t.Fatalf("Node Unmarshal: GPUCost mutated in unmarshal")
	}
	if node1.GPUCount != node2.GPUCount {
		t.Fatalf("Node Unmarshal: GPUCount mutated in unmarshal")
	}
	if node1.RAMCost != node2.RAMCost {
		t.Fatalf("Node Unmarshal: RAMCost mutated in unmarshal")
	}
	if node1.Discount != node2.Discount {
		t.Fatalf("Node Unmarshal: Discount mutated in unmarshal")
	}
	if node1.Preemptible != node2.Preemptible {
		t.Fatalf("Node Unmarshal: Preemptible mutated in unmarshal")
	}

	// As a final check, make sure the above checks out
	if !node1.Equal(node2) {
		t.Fatalf("Node Unmarshal: Node mutated in unmarshal")
	}

}

func TestLoadBalancer_Unmarshal(t *testing.T) {

	lb1 := NewLoadBalancer("loadbalancer1", "cluster1", "provider1", *unmarshalWindow.start, *unmarshalWindow.end, unmarshalWindow, false, "127.0.0.1")
	lb1.Cost = 12.0
	lb1.SetAdjustment(4.0)

	bytes, _ := json.Marshal(lb1)

	var testlb LoadBalancer
	lb2 := &testlb

	err := json.Unmarshal(bytes, lb2)

	// Check if unmarshal was successful
	if err != nil {
		t.Fatalf("LoadBalancer Unmarshal: unexpected error: %s", err)
	}

	// Check if all fields in initial LoadBalancer equal those in LoadBalancer from unmarshal
	if !lb1.Properties.Equal(lb2.Properties) {
		t.Fatalf("LoadBalancer Unmarshal: properties mutated in unmarshal")
	}
	if !lb1.Labels.Equal(lb2.Labels) {
		t.Fatalf("LoadBalancer Unmarshal: labels mutated in unmarshal")
	}
	if !lb1.Window.Equal(lb2.Window) {
		t.Fatalf("LoadBalancer Unmarshal: window mutated in unmarshal")
	}
	if !lb1.Start.Equal(lb2.Start) {
		t.Fatalf("LoadBalancer Unmarshal: start mutated in unmarshal")
	}
	if !lb1.End.Equal(lb2.End) {
		t.Fatalf("LoadBalancer Unmarshal: end mutated in unmarshal")
	}
	if lb1.Adjustment != lb2.Adjustment {
		t.Fatalf("LoadBalancer Unmarshal: adjustment mutated in unmarshal")
	}
	if lb1.Cost != lb2.Cost {
		t.Fatalf("LoadBalancer Unmarshal: cost mutated in unmarshal")
	}
	if lb1.Private != lb2.Private {
		t.Fatalf("LoadBalancer Unmarshal: private mutated in unmarshal")
	}
	if lb1.Ip != lb2.Ip {
		t.Fatalf("LoadBalancer Unmarshal: ip mutated in unmarshal")
	}

	// As a final check, make sure the above checks out
	if !lb1.Equal(lb2) {
		t.Fatalf("LoadBalancer Unmarshal: LoadBalancer mutated in unmarshal")
	}

}

func TestSharedAsset_Unmarshal(t *testing.T) {

	sa1 := NewSharedAsset("sharedasset1", unmarshalWindow)
	sa1.Cost = 7.0

	bytes, _ := json.Marshal(sa1)

	var testsa SharedAsset
	sa2 := &testsa

	err := json.Unmarshal(bytes, sa2)

	// Check if unmarshal was successful
	if err != nil {
		t.Fatalf("SharedAsset Unmarshal: unexpected error: %s", err)
	}

	// Check if all fields in initial SharedAsset equal those in SharedAsset from unmarshal
	if !sa1.Properties.Equal(sa2.Properties) {
		t.Fatalf("SharedAsset Unmarshal: properties mutated in unmarshal")
	}
	if !sa1.Labels.Equal(sa2.Labels) {
		t.Fatalf("SharedAsset Unmarshal: labels mutated in unmarshal")
	}
	if !sa1.Window.Equal(sa2.Window) {
		t.Fatalf("SharedAsset Unmarshal: window mutated in unmarshal")
	}
	if sa1.Cost != sa2.Cost {
		t.Fatalf("SharedAsset Unmarshal: cost mutated in unmarshal")
	}

	// As a final check, make sure the above checks out
	if !sa1.Equal(sa2) {
		t.Fatalf("SharedAsset Unmarshal: SharedAsset mutated in unmarshal")
	}

}

func TestAssetset_Unmarshal(t *testing.T) {

	var s time.Time
	var e time.Time
	unmarshalWindow := NewWindow(&s, &e)

	any := NewAsset(*unmarshalWindow.start, *unmarshalWindow.end, unmarshalWindow)
	cloud := NewCloud("Compute", "provider1", *unmarshalWindow.start, *unmarshalWindow.end, unmarshalWindow)
	cm := NewClusterManagement(GCPProvider, "cluster1", unmarshalWindow)
	disk := NewDisk("disk1", "cluster1", "disk1", *unmarshalWindow.start, *unmarshalWindow.end, unmarshalWindow)
	network := NewNetwork("network1", "cluster1", "provider1", *unmarshalWindow.start, *unmarshalWindow.end, unmarshalWindow)
	node := NewNode("node1", "cluster1", "provider1", *unmarshalWindow.start, *unmarshalWindow.end, unmarshalWindow)
	lb := NewLoadBalancer("loadbalancer1", "cluster1", "provider1", *unmarshalWindow.start, *unmarshalWindow.end, unmarshalWindow, false, "127.0.0.1")
	sa := NewSharedAsset("sharedasset1", unmarshalWindow)

	assetList := []Asset{any, cloud, cm, disk, network, node, lb, sa}

	assetset := NewAssetSet(s, e, assetList...)
	bytes, _ := json.Marshal(assetset)

	var assetSetResponse AssetSetResponse
	assetUnmarshalResponse := &assetSetResponse

	err := json.Unmarshal(bytes, assetUnmarshalResponse)

	// Check if unmarshal was successful
	if err != nil {
		t.Fatalf("AssetSet Unmarshal: unexpected error: %s", err)
	}

	// For each asset in unmarshaled AssetSetResponse, check if it is equal to the corresponding AssetSet asset
	for key, asset := range assetset.Assets {

		if unmarshaledAsset, exists := assetUnmarshalResponse.Assets[key]; exists {

			// As Disk is not marshaled with all fields, the resultant Disk will be unequal. Test all fields we have instead.
			if unmarshaledAsset.Type().String() == "Disk" {

				udiskEq := func(d1 Asset, d2 Asset) bool {

					asset, _ := asset.(*Disk)
					unmarshaledAsset, _ := unmarshaledAsset.(*Disk)

					if !asset.GetLabels().Equal(unmarshaledAsset.Labels) {
						return false
					}
					if !asset.Properties.Equal(unmarshaledAsset.Properties) {
						return false
					}

					if !asset.GetStart().Equal(unmarshaledAsset.Start) {
						return false
					}
					if !asset.End.Equal(unmarshaledAsset.End) {
						return false
					}
					if !asset.Window.Equal(unmarshaledAsset.Window) {
						return false
					}
					if asset.Adjustment != unmarshaledAsset.Adjustment {
						return false
					}
					if asset.Cost != unmarshaledAsset.Cost {
						return false
					}
					if asset.ByteHours != unmarshaledAsset.ByteHours {
						return false
					}
					if !asset.Breakdown.Equal(unmarshaledAsset.Breakdown) {
						return false
					}

					return true
				}

				if res := udiskEq(asset, unmarshaledAsset); !res {
					t.Fatalf("AssetSet Unmarshal: asset at key '%s' from unmarshaled AssetSetResponse does not match corresponding asset from AssetSet", key)
				}

			} else {

				if !asset.Equal(unmarshaledAsset) {
					t.Fatalf("AssetSet Unmarshal: asset at key '%s' from unmarshaled AssetSetResponse does not match corresponding asset from AssetSet", key)
				}

			}

		} else {
			t.Fatalf("AssetSet Unmarshal: key '%s' from marshaled AssetSet does not exist in AssetSetResponse", key)
		}

	}

}
