package opencost

import (
	"testing"
	"time"
)

func TestAllocation_BinaryEncoding(t *testing.T) {
	// TODO niko/etl
}

func TestAllocationSet_BinaryEncoding(t *testing.T) {
	// TODO niko/etl
}

func BenchmarkAllocationSetRange_BinaryEncoding(b *testing.B) {
	endYesterday := time.Now().UTC().Truncate(day)
	startYesterday := endYesterday.Add(-day)
	startD2 := startYesterday
	startD1 := startD2.Add(-day)
	startD0 := startD1.Add(-day)

	var asr0, asr1 *AllocationSetRange
	var bs []byte
	var err error

	asr0 = NewAllocationSetRange(
		GenerateMockAllocationSetClusterIdle(startD0),
		GenerateMockAllocationSetClusterIdle(startD1),
		GenerateMockAllocationSetClusterIdle(startD2),
	)

	for it := 0; it < b.N; it++ {
		bs, err = asr0.MarshalBinary()
		if err != nil {
			b.Fatalf("AllocationSetRange.Binary: unexpected error: %s", err)
			return
		}

		asr1 = &AllocationSetRange{}
		err = asr1.UnmarshalBinary(bs)
		if err != nil {
			b.Fatalf("AllocationSetRange.Binary: unexpected error: %s", err)
			return
		}

		if asr0.Length() != asr1.Length() {
			b.Fatalf("AllocationSetRange.Binary: expected %d; found %d", asr0.Length(), asr1.Length())
		}
		if !asr0.Window().Equal(asr1.Window()) {
			b.Fatalf("AllocationSetRange.Binary: expected %s; found %s", asr0.Window(), asr1.Window())
		}

		for i, as0 := range asr0.Allocations {
			as1, err := asr1.Get(i)
			if err != nil {
				b.Fatalf("AllocationSetRange.Binary: unexpected error: %s", err)
			}

			if as0.Length() != as1.Length() {
				b.Fatalf("AllocationSetRange.Binary: expected %d; found %d", as0.Length(), as1.Length())
			}
			if !as0.Window.Equal(as1.Window) {
				b.Fatalf("AllocationSetRange.Binary: expected %s; found %s", as0.Window, as1.Window)
			}

			for k, a0 := range as0.Allocations {
				a1 := as1.Get(k)
				if a1 == nil {
					b.Fatalf("AllocationSetRange.Binary: missing Allocation: %s", a0)
				}

				if !a0.Equal(a1) {
					b.Fatalf("AllocationSetRange.Binary: unequal Allocations \"%s\": expected %s; found %s", k, a0, a1)
				}
			}
		}
	}
}

func TestAllocationSetRange_BinaryEncoding(t *testing.T) {
	endYesterday := time.Now().UTC().Truncate(day)
	startYesterday := endYesterday.Add(-day)
	startD2 := startYesterday
	startD1 := startD2.Add(-day)
	startD0 := startD1.Add(-day)

	var asr0, asr1 *AllocationSetRange
	var bs []byte
	var err error

	asr0 = NewAllocationSetRange(
		GenerateMockAllocationSetClusterIdle(startD0),
		GenerateMockAllocationSetClusterIdle(startD1),
		GenerateMockAllocationSetClusterIdle(startD2),
	)

	bs, err = asr0.MarshalBinary()
	if err != nil {
		t.Fatalf("AllocationSetRange.Binary: unexpected error: %s", err)
		return
	}

	asr1 = &AllocationSetRange{}
	err = asr1.UnmarshalBinary(bs)
	if err != nil {
		t.Fatalf("AllocationSetRange.Binary: unexpected error: %s", err)
		return
	}

	if asr0.Length() != asr1.Length() {
		t.Fatalf("AllocationSetRange.Binary: expected %d; found %d", asr0.Length(), asr1.Length())
	}
	if !asr0.Window().Equal(asr1.Window()) {
		t.Fatalf("AllocationSetRange.Binary: expected %s; found %s", asr0.Window(), asr1.Window())
	}

	for i, as0 := range asr0.Allocations {
		as1, err := asr1.Get(i)
		if err != nil {
			t.Fatalf("AllocationSetRange.Binary: unexpected error: %s", err)
		}

		if as0.Length() != as1.Length() {
			t.Fatalf("AllocationSetRange.Binary: expected %d; found %d", as0.Length(), as1.Length())
		}
		if !as0.Window.Equal(as1.Window) {
			t.Fatalf("AllocationSetRange.Binary: expected %s; found %s", as0.Window, as1.Window)
		}

		for k, a0 := range as0.Allocations {
			a1 := as1.Get(k)
			if a1 == nil {
				t.Fatalf("AllocationSetRange.Binary: missing Allocation: %s", a0)
			}

			// TODO Sean: fix JSON marshaling of PVs
			a1.PVs = a0.PVs
			if !a0.Equal(a1) {
				t.Fatalf("AllocationSetRange.Binary: unequal Allocations \"%s\": expected \"%s\"; found \"%s\"", k, a0, a1)
			}
		}
	}
}

func TestAny_BinaryEncoding(t *testing.T) {
	start := time.Date(2020, time.September, 16, 0, 0, 0, 0, time.UTC)
	end := start.Add(24 * time.Hour)
	window := NewWindow(&start, &end)

	var a0, a1 *Any
	var bs []byte
	var err error

	a0 = NewAsset(*window.start, *window.end, window)
	a0.SetProperties(&AssetProperties{
		Name:       "any1",
		Cluster:    "cluster1",
		ProviderID: "世界",
	})
	a0.Cost = 123.45
	a0.SetAdjustment(1.23)

	bs, err = a0.MarshalBinary()
	if err != nil {
		t.Fatalf("Any.Binary: unexpected error: %s", err)
	}

	a1 = &Any{}
	err = a1.UnmarshalBinary(bs)
	if err != nil {
		t.Fatalf("Any.Binary: unexpected error: %s", err)
	}

	if a1.Properties.Name != a0.Properties.Name {
		t.Fatalf("Any.Binary: expected %s, found %s", a0.Properties.Name, a1.Properties.Name)
	}
	if a1.Properties.Cluster != a0.Properties.Cluster {
		t.Fatalf("Any.Binary: expected %s, found %s", a0.Properties.Cluster, a1.Properties.Cluster)
	}
	if a1.Properties.ProviderID != a0.Properties.ProviderID {
		t.Fatalf("Any.Binary: expected %s, found %s", a0.Properties.ProviderID, a1.Properties.ProviderID)
	}
	if a1.Adjustment != a0.Adjustment {
		t.Fatalf("Any.Binary: expected %f, found %f", a0.Adjustment, a1.Adjustment)
	}
	if a1.TotalCost() != a0.TotalCost() {
		t.Fatalf("Any.Binary: expected %f, found %f", a0.TotalCost(), a1.TotalCost())
	}
	if !a1.Window.Equal(a0.Window) {
		t.Fatalf("Any.Binary: expected %s, found %s", a0.Window, a1.Window)
	}
}

func TestAsset_BinaryEncoding(t *testing.T) {
	// TODO niko/etl
}

func TestAssetSet_BinaryEncoding(t *testing.T) {
	// TODO niko/etl
}

func TestAssetSetRange_BinaryEncoding(t *testing.T) {
	endYesterday := time.Now().UTC().Truncate(day)
	startYesterday := endYesterday.Add(-day)
	startD2 := startYesterday
	startD1 := startD2.Add(-day)
	startD0 := startD1.Add(-day)

	var asr0, asr1 *AssetSetRange
	var bs []byte
	var err error

	asr0 = NewAssetSetRange(
		GenerateMockAssetSet(startD0, day),
		GenerateMockAssetSet(startD1, day),
		GenerateMockAssetSet(startD2, day),
	)

	bs, err = asr0.MarshalBinary()
	if err != nil {
		t.Fatalf("AssetSetRange.Binary: unexpected error: %s", err)
		return
	}

	asr1 = &AssetSetRange{}
	err = asr1.UnmarshalBinary(bs)
	if err != nil {
		t.Fatalf("AssetSetRange.Binary: unexpected error: %s", err)
		return
	}

	if asr0.Length() != asr1.Length() {
		t.Fatalf("AssetSetRange.Binary: expected %d; found %d", asr0.Length(), asr1.Length())
	}
	if !asr0.Window().Equal(asr1.Window()) {
		t.Fatalf("AssetSetRange.Binary: expected %s; found %s", asr0.Window(), asr1.Window())
	}

	for i, as0 := range asr0.Assets {
		as1, err := asr1.Get(i)
		if err != nil {
			t.Fatalf("AssetSetRange.Binary: unexpected error: %s", err)
		}

		if as0.Length() != as1.Length() {
			t.Fatalf("AssetSetRange.Binary: expected %d; found %d", as0.Length(), as1.Length())
		}
		if !as0.Window.Equal(as1.Window) {
			t.Fatalf("AssetSetRange.Binary: expected %s; found %s", as0.Window, as1.Window)
		}

		for k, a0 := range as0.Assets {
			a1, ok := as1.Get(k)
			if !ok {
				t.Fatalf("AssetSetRange.Binary: missing Asset: %s", a0)
			}

			if !a0.Equal(a1) {
				t.Fatalf("AssetSetRange.Binary: unequal Assets \"%s\": expected %s; found %s", k, a0, a1)
			}
		}
	}
}

func TestBreakdown_BinaryEncoding(t *testing.T) {
	var b0, b1 *Breakdown
	var bs []byte
	var err error

	b0 = &Breakdown{
		Idle:   0.75,
		Other:  0.1,
		System: 0.0,
		User:   0.15,
	}

	bs, err = b0.MarshalBinary()
	if err != nil {
		t.Fatalf("Breakdown.Binary: unexpected error: %s", err)
	}

	b1 = &Breakdown{}
	err = b1.UnmarshalBinary(bs)
	if err != nil {
		t.Fatalf("Breakdown.Binary: unexpected error: %s", err)
	}

	if b1.Idle != b0.Idle {
		t.Fatalf("Breakdown.Binary: expected %f, found %f", b0.Idle, b1.Idle)
	}
	if b1.Other != b0.Other {
		t.Fatalf("Breakdown.Binary: expected %f, found %f", b0.Other, b1.Other)
	}
	if b1.System != b0.System {
		t.Fatalf("Breakdown.Binary: expected %f, found %f", b0.System, b1.System)
	}
	if b1.User != b0.User {
		t.Fatalf("Breakdown.Binary: expected %f, found %f", b0.User, b1.User)
	}
}

func TestCloudAny_BinaryEncoding(t *testing.T) {
	ws := time.Date(2020, time.September, 16, 0, 0, 0, 0, time.UTC)
	we := ws.Add(24 * time.Hour)
	window := NewWindow(&ws, &we)

	var a0, a1 *Cloud
	var bs []byte
	var err error

	a0 = NewCloud(ComputeCategory, "providerid1", *window.start, *window.end, window)
	a0.Cost = 6.09
	a0.SetAdjustment(-1.23)

	bs, err = a0.MarshalBinary()
	if err != nil {
		t.Fatalf("CloudAny.Binary: unexpected error: %s", err)
	}

	a1 = &Cloud{}
	err = a1.UnmarshalBinary(bs)
	if err != nil {
		t.Fatalf("CloudAny.Binary: unexpected error: %s", err)
	}

	if !a0.Equal(a1) {
		t.Fatalf("CloudAny.Binary: expected %v, found %v", a0, a1)
	}
}

func TestClusterManagement_BinaryEncoding(t *testing.T) {
	ws := time.Date(2020, time.September, 16, 0, 0, 0, 0, time.UTC)
	we := ws.Add(24 * time.Hour)
	window := NewWindow(&ws, &we)

	var a0, a1 *ClusterManagement
	var bs []byte
	var err error

	a0 = NewClusterManagement(AWSProvider, "cluster1", window)
	a0.Cost = 4.003
	a0.SetAdjustment(-3.23)

	bs, err = a0.MarshalBinary()
	if err != nil {
		t.Fatalf("ClusterManagement.Binary: unexpected error: %s", err)
	}

	a1 = &ClusterManagement{}
	err = a1.UnmarshalBinary(bs)
	if err != nil {
		t.Fatalf("ClusterManagement.Binary: unexpected error: %s", err)
	}

	if !a0.Equal(a1) {
		t.Fatalf("ClusterManagement.Binary: expected %v, found %v", a0, a1)
	}
}

func TestDisk_BinaryEncoding(t *testing.T) {
	ws := time.Date(2020, time.September, 16, 0, 0, 0, 0, time.UTC)
	we := ws.Add(24 * time.Hour)
	window := NewWindow(&ws, &we)
	hours := window.Duration().Hours()

	start := time.Date(2020, time.September, 16, 3, 0, 0, 0, time.UTC)
	end := time.Date(2020, time.September, 16, 15, 12, 0, 0, time.UTC)

	var a0, a1 *Disk
	var bs []byte
	var err error

	a0 = NewDisk("any1", "cluster1", "世界", start, end, window)
	a0.ByteHours = 100 * 1024 * 1024 * 1024 * hours
	a0.Cost = 4.003
	a0.Local = 0.4
	a0.Breakdown = &Breakdown{
		Idle:   0.9,
		Other:  0.05,
		System: 0.05,
		User:   0.0,
	}
	a0.SetAdjustment(-3.23)

	bs, err = a0.MarshalBinary()
	if err != nil {
		t.Fatalf("Disk.Binary: unexpected error: %s", err)
	}

	a1 = &Disk{}
	err = a1.UnmarshalBinary(bs)
	if err != nil {
		t.Fatalf("Disk.Binary: unexpected error: %s", err)
	}

	if !a0.Equal(a1) {
		t.Fatalf("Disk.Binary: expected %v, found %v", a0, a1)
	}
}

func TestNode_BinaryEncoding(t *testing.T) {
	ws := time.Date(2020, time.September, 16, 0, 0, 0, 0, time.UTC)
	we := ws.Add(24 * time.Hour)
	window := NewWindow(&ws, &we)
	hours := window.Duration().Hours()

	start := time.Date(2020, time.September, 16, 3, 0, 0, 0, time.UTC)
	end := time.Date(2020, time.September, 16, 15, 12, 0, 0, time.UTC)

	var a0, a1 *Node
	var bs []byte
	var err error

	a0 = NewNode("any1", "cluster1", "世界", start, end, window)
	a0.NodeType = "n2-standard"
	a0.Preemptible = 1.0
	a0.CPUCoreHours = 2.0 * hours
	a0.RAMByteHours = 12.0 * gb * hours
	a0.CPUCost = 1.50
	a0.GPUCost = 30.44
	a0.RAMCost = 15.0
	a0.Discount = 0.9
	a0.CPUBreakdown = &Breakdown{
		Idle:   0.9,
		Other:  0.05,
		System: 0.05,
		User:   0.0,
	}
	a0.RAMBreakdown = &Breakdown{
		Idle:   0.4,
		Other:  0.05,
		System: 0.05,
		User:   0.5,
	}
	a0.SetAdjustment(1.23)

	bs, err = a0.MarshalBinary()
	if err != nil {
		t.Fatalf("Node.Binary: unexpected error: %s", err)
	}

	a1 = &Node{}
	err = a1.UnmarshalBinary(bs)
	if err != nil {
		t.Fatalf("Node.Binary: unexpected error: %s", err)
	}

	if !a0.Equal(a1) {
		t.Fatalf("Node.Binary: expected %v, found %v", a0, a1)
	}
}

func TestProperties_BinaryEncoding(t *testing.T) {
	var p0, p1 *AllocationProperties
	var bs []byte
	var err error

	// empty properties
	p0 = &AllocationProperties{}
	bs, err = p0.MarshalBinary()
	if err != nil {
		t.Fatalf("AllocationProperties.Binary: unexpected error: %s", err)
	}

	p1 = &AllocationProperties{}
	err = p1.UnmarshalBinary(bs)
	if err != nil {
		t.Fatalf("AllocationProperties.Binary: unexpected error: %s", err)
	}

	if !p0.Equal(p1) {
		t.Fatalf("AllocationProperties.Binary: expected %s; found %s", p0, p1)
	}

	// complete properties
	p0 = &AllocationProperties{}
	p0.Cluster = "cluster1"
	p0.Container = "container-abc-1"
	p0.Controller = "daemonset-abc"
	p0.ControllerKind = "daemonset"
	p0.Namespace = "namespace1"
	p0.NamespaceLabels = map[string]string{
		"app":                "cost-analyzer-namespace",
		"kubernetes.io/name": "cost-analyzer",
	}
	p0.NamespaceAnnotations = map[string]string{
		"com.kubernetes.io/managed-by":             "helm",
		"kubernetes.io/last-applied-configuration": "cost-analyzer",
	}
	p0.Node = "node1"
	p0.Pod = "daemonset-abc-123"
	p0.Labels = map[string]string{
		"app":  "cost-analyzer",
		"tier": "frontend",
	}
	p0.Services = []string{"kubecost-frontend"}
	bs, err = p0.MarshalBinary()
	if err != nil {
		t.Fatalf("AllocationProperties.Binary: unexpected error: %s", err)
	}

	p1 = &AllocationProperties{}
	err = p1.UnmarshalBinary(bs)
	if err != nil {
		t.Fatalf("AllocationProperties.Binary: unexpected error: %s", err)
	}

	if !p0.Equal(p1) {
		t.Fatalf("AllocationProperties.Binary: expected %s; found %s", p0, p1)
	}

	// incomplete properties
	p0 = &AllocationProperties{}
	p0.Cluster = ("cluster1")
	p0.Controller = "daemonset-abc"
	p0.ControllerKind = "daemonset"
	p0.Namespace = "namespace1"
	p0.NamespaceAnnotations = map[string]string{
		"com.kubernetes.io/managed-by":             "helm",
		"kubernetes.io/last-applied-configuration": "cost-analyzer",
	}
	p0.Services = []string{}
	bs, err = p0.MarshalBinary()
	if err != nil {
		t.Fatalf("AllocationProperties.Binary: unexpected error: %s", err)
	}

	p1 = &AllocationProperties{}
	err = p1.UnmarshalBinary(bs)
	if err != nil {
		t.Fatalf("AllocationProperties.Binary: unexpected error: %s", err)
	}

	if !p0.Equal(p1) {
		t.Fatalf("AllocationProperties.Binary: expected %s; found %s", p0, p1)
	}
}

func TestShared_BinaryEncoding(t *testing.T) {
	ws := time.Date(2020, time.September, 16, 0, 0, 0, 0, time.UTC)
	we := ws.Add(24 * time.Hour)
	window := NewWindow(&ws, &we)

	var a0, a1 *SharedAsset
	var bs []byte
	var err error

	a0 = NewSharedAsset("any1", window)
	a0.Cost = 4.04
	a0.SetAdjustment(1.23)

	bs, err = a0.MarshalBinary()
	if err != nil {
		t.Fatalf("SharedAsset.Binary: unexpected error: %s", err)
	}

	a1 = &SharedAsset{}
	err = a1.UnmarshalBinary(bs)
	if err != nil {
		t.Fatalf("SharedAsset.Binary: unexpected error: %s", err)
	}

	if !a0.Equal(a1) {
		t.Fatalf("SharedAsset.Binary: expected %v, found %v", a0, a1)
	}
}

func TestWindow_BinaryEncoding(t *testing.T) {
	var w0, w1 Window
	var bs []byte
	var err error

	// Window (nil, nil)
	w0 = NewWindow(nil, nil)
	bs, err = w0.MarshalBinary()
	if err != nil {
		t.Fatalf("Window.Binary: unexpected error: %s", err)
	}

	err = w1.UnmarshalBinary(bs)
	if err != nil {
		t.Fatalf("Window.Binary: unexpected error: %s", err)
	}

	if w1.Start() != w0.Start() {
		t.Fatalf("Window.Binary: expected %v; found %v", w0.Start(), w1.Start())
	}
	if w1.End() != w0.End() {
		t.Fatalf("Window.Binary: expected %v; found %v", w0.End(), w1.End())
	}

	// Window (time, nil)
	ts := time.Now()
	w0 = NewWindow(&ts, nil)
	bs, err = w0.MarshalBinary()
	if err != nil {
		t.Fatalf("Window.Binary: unexpected error: %s", err)
	}

	err = w1.UnmarshalBinary(bs)
	if err != nil {
		t.Fatalf("Window.Binary: unexpected error: %s", err)
	}

	if !w1.Start().Equal(*w0.Start()) {
		t.Fatalf("Window.Binary: expected %v; found %v", w0.Start(), w1.Start())
	}
	if w1.End() != w0.End() {
		t.Fatalf("Window.Binary: expected %v; found %v", w0.End(), w1.End())
	}

	// Window (nil, time)
	te := time.Now()
	w0 = NewWindow(nil, &te)
	bs, err = w0.MarshalBinary()
	if err != nil {
		t.Fatalf("Window.Binary: unexpected error: %s", err)
	}

	err = w1.UnmarshalBinary(bs)
	if err != nil {
		t.Fatalf("Window.Binary: unexpected error: %s", err)
	}

	if w1.Start() != w0.Start() {
		t.Fatalf("Window.Binary: expected %v; found %v", w0.Start(), w1.Start())
	}
	if !w1.End().Equal(*w0.End()) {
		t.Fatalf("Window.Binary: expected %v; found %v", w0.End(), w1.End())
	}

	// Window (time, time)
	ts, te = time.Now(), time.Now()
	w0 = NewWindow(&ts, &te)
	bs, err = w0.MarshalBinary()
	if err != nil {
		t.Fatalf("Window.Binary: unexpected error: %s", err)
	}

	err = w1.UnmarshalBinary(bs)
	if err != nil {
		t.Fatalf("Window.Binary: unexpected error: %s", err)
	}

	if !w1.Start().Equal(*w0.Start()) {
		t.Fatalf("Window.Binary: expected %v; found %v", w0.Start(), w1.Start())
	}
	if !w1.End().Equal(*w0.End()) {
		t.Fatalf("Window.Binary: expected %v; found %v", w0.End(), w1.End())
	}
}
