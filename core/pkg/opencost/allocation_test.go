package opencost

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/opencost/opencost/core/pkg/filter"
	"github.com/opencost/opencost/core/pkg/filter/allocation"
	"github.com/opencost/opencost/core/pkg/filter/ast"
	"github.com/opencost/opencost/core/pkg/filter/ops"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util"
	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
)

var filterParser = allocation.NewAllocationFilterParser()
var matcherCompiler = NewAllocationMatchCompiler(nil)

// useful for creating filters on the fly when testing. panics
// on parse errors!
func mustParseFilter(s string) filter.Filter {
	filter, err := filterParser.Parse(s)
	if err != nil {
		panic(err)
	}
	return filter
}

// useful for creating filters on the fly when testing. panics
// on parse or compile errors!
func mustCompileFilter(s string) AllocationMatcher {
	filter := mustParseFilter(s)
	m, err := matcherCompiler.Compile(filter)
	if err != nil {
		panic(err)
	}
	return m
}

func TestAllocation_Add(t *testing.T) {
	var nilAlloc *Allocation
	zeroAlloc := &Allocation{}

	// nil + nil == nil
	nilNilSum, err := nilAlloc.Add(nilAlloc)
	if err != nil {
		t.Fatalf("Allocation.Add unexpected error: %s", err)
	}
	if nilNilSum != nil {
		t.Fatalf("Allocation.Add failed; exp: nil; act: %s", nilNilSum)
	}

	// nil + zero == zero
	nilZeroSum, err := nilAlloc.Add(zeroAlloc)
	if err != nil {
		t.Fatalf("Allocation.Add unexpected error: %s", err)
	}
	if nilZeroSum == nil || nilZeroSum.TotalCost() != 0.0 {
		t.Fatalf("Allocation.Add failed; exp: 0.0; act: %s", nilZeroSum)
	}

	cpuPrice := 0.02
	gpuPrice := 2.00
	ramPrice := 0.01
	pvPrice := 0.00005
	gib := 1024.0 * 1024.0 * 1024.0

	s1 := time.Date(2021, time.January, 1, 0, 0, 0, 0, time.UTC)
	e1 := time.Date(2021, time.January, 1, 12, 0, 0, 0, time.UTC)
	hrs1 := e1.Sub(s1).Hours()
	a1 := &Allocation{
		Start:                 s1,
		End:                   e1,
		Properties:            &AllocationProperties{},
		CPUCoreHours:          2.0 * hrs1,
		CPUCoreRequestAverage: 2.0,
		CPUCoreUsageAverage:   1.0,
		CPUCost:               2.0 * hrs1 * cpuPrice,
		CPUCostAdjustment:     3.0,
		GPUHours:              1.0 * hrs1,
		GPUCost:               1.0 * hrs1 * gpuPrice,
		GPUCostAdjustment:     2.0,
		PVs: PVAllocations{
			disk: {
				ByteHours: 100.0 * gib * hrs1,
				Cost:      100.0 * hrs1 * pvPrice,
			},
		},
		PVCostAdjustment:       4.0,
		RAMByteHours:           8.0 * gib * hrs1,
		RAMBytesRequestAverage: 8.0 * gib,
		RAMBytesUsageAverage:   4.0 * gib,
		RAMCost:                8.0 * hrs1 * ramPrice,
		RAMCostAdjustment:      1.0,
		SharedCost:             2.00,
		ExternalCost:           1.00,
		RawAllocationOnly:      &RawAllocationOnlyData{},
	}
	a1b := a1.Clone()

	s2 := time.Date(2021, time.January, 1, 6, 0, 0, 0, time.UTC)
	e2 := time.Date(2021, time.January, 1, 24, 0, 0, 0, time.UTC)
	hrs2 := e1.Sub(s1).Hours()
	a2 := &Allocation{
		Start:                  s2,
		End:                    e2,
		Properties:             &AllocationProperties{},
		CPUCoreHours:           1.0 * hrs2,
		CPUCoreRequestAverage:  1.0,
		CPUCoreUsageAverage:    1.0,
		CPUCost:                1.0 * hrs2 * cpuPrice,
		GPUHours:               0.0,
		GPUCost:                0.0,
		RAMByteHours:           8.0 * gib * hrs2,
		RAMBytesRequestAverage: 0.0,
		RAMBytesUsageAverage:   8.0 * gib,
		RAMCost:                8.0 * hrs2 * ramPrice,
		NetworkCost:            0.01,
		LoadBalancerCost:       0.05,
		SharedCost:             0.00,
		ExternalCost:           1.00,
		RawAllocationOnly:      &RawAllocationOnlyData{},
	}
	a2b := a2.Clone()

	act, err := a1.Add(a2)
	if err != nil {
		t.Fatalf("Allocation.Add: unexpected error: %s", err)
	}

	// Neither Allocation should be mutated
	if !a1.Equal(a1b) {
		t.Fatalf("Allocation.Add: a1 illegally mutated")
	}
	if !a2.Equal(a2b) {
		t.Fatalf("Allocation.Add: a1 illegally mutated")
	}

	// Costs should be cumulative
	if !util.IsApproximately(a1.TotalCost()+a2.TotalCost(), act.TotalCost()) {
		t.Fatalf("Allocation.Add: expected %f; actual %f", a1.TotalCost()+a2.TotalCost(), act.TotalCost())
	}
	if !util.IsApproximately(a1.CPUCost+a2.CPUCost, act.CPUCost) {
		t.Fatalf("Allocation.Add: expected %f; actual %f", a1.CPUCost+a2.CPUCost, act.CPUCost)
	}
	if !util.IsApproximately(a1.CPUCostAdjustment+a2.CPUCostAdjustment, act.CPUCostAdjustment) {
		t.Fatalf("Allocation.Add: expected %f; actual %f", a1.CPUCostAdjustment+a2.CPUCostAdjustment, act.CPUCostAdjustment)
	}
	if !util.IsApproximately(a1.GPUCost+a2.GPUCost, act.GPUCost) {
		t.Fatalf("Allocation.Add: expected %f; actual %f", a1.GPUCost+a2.GPUCost, act.GPUCost)
	}
	if !util.IsApproximately(a1.GPUCostAdjustment+a2.GPUCostAdjustment, act.GPUCostAdjustment) {
		t.Fatalf("Allocation.Add: expected %f; actual %f", a1.GPUCostAdjustment+a2.GPUCostAdjustment, act.GPUCostAdjustment)
	}
	if !util.IsApproximately(a1.RAMCost+a2.RAMCost, act.RAMCost) {
		t.Fatalf("Allocation.Add: expected %f; actual %f", a1.RAMCost+a2.RAMCost, act.RAMCost)
	}
	if !util.IsApproximately(a1.RAMCostAdjustment+a2.RAMCostAdjustment, act.RAMCostAdjustment) {
		t.Fatalf("Allocation.Add: expected %f; actual %f", a1.RAMCostAdjustment+a2.RAMCostAdjustment, act.RAMCostAdjustment)
	}
	if !util.IsApproximately(a1.PVCost()+a2.PVCost(), act.PVCost()) {
		t.Fatalf("Allocation.Add: expected %f; actual %f", a1.PVCost()+a2.PVCost(), act.PVCost())
	}
	if !util.IsApproximately(a1.NetworkCost+a2.NetworkCost, act.NetworkCost) {
		t.Fatalf("Allocation.Add: expected %f; actual %f", a1.NetworkCost+a2.NetworkCost, act.NetworkCost)
	}
	if !util.IsApproximately(a1.LoadBalancerCost+a2.LoadBalancerCost, act.LoadBalancerCost) {
		t.Fatalf("Allocation.Add: expected %f; actual %f", a1.LoadBalancerCost+a2.LoadBalancerCost, act.LoadBalancerCost)
	}
	if !util.IsApproximately(a1.SharedCost+a2.SharedCost, act.SharedCost) {
		t.Fatalf("Allocation.Add: expected %f; actual %f", a1.SharedCost+a2.SharedCost, act.SharedCost)
	}
	if !util.IsApproximately(a1.ExternalCost+a2.ExternalCost, act.ExternalCost) {
		t.Fatalf("Allocation.Add: expected %f; actual %f", a1.ExternalCost+a2.ExternalCost, act.ExternalCost)
	}

	// ResourceHours should be cumulative
	if !util.IsApproximately(a1.CPUCoreHours+a2.CPUCoreHours, act.CPUCoreHours) {
		t.Fatalf("Allocation.Add: expected %f; actual %f", a1.CPUCoreHours+a2.CPUCoreHours, act.CPUCoreHours)
	}
	if !util.IsApproximately(a1.RAMByteHours+a2.RAMByteHours, act.RAMByteHours) {
		t.Fatalf("Allocation.Add: expected %f; actual %f", a1.RAMByteHours+a2.RAMByteHours, act.RAMByteHours)
	}
	if !util.IsApproximately(a1.PVByteHours()+a2.PVByteHours(), act.PVByteHours()) {
		t.Fatalf("Allocation.Add: expected %f; actual %f", a1.PVByteHours()+a2.PVByteHours(), act.PVByteHours())
	}

	// Minutes should be the duration between min(starts) and max(ends)
	if !act.Start.Equal(a1.Start) || !act.End.Equal(a2.End) {
		t.Fatalf("Allocation.Add: expected %s; actual %s", NewWindow(&a1.Start, &a2.End), NewWindow(&act.Start, &act.End))
	}
	if act.Minutes() != 1440.0 {
		t.Fatalf("Allocation.Add: expected %f; actual %f", 1440.0, act.Minutes())
	}

	// Requests and Usage should be averaged correctly
	// CPU requests = (2.0*12.0 + 1.0*18.0)/(24.0) = 1.75
	// CPU usage = (1.0*12.0 + 1.0*18.0)/(24.0) = 1.25
	// RAM requests = (8.0*12.0 + 0.0*18.0)/(24.0) = 4.00
	// RAM usage = (4.0*12.0 + 8.0*18.0)/(24.0) = 8.00
	if !util.IsApproximately(1.75, act.CPUCoreRequestAverage) {
		t.Fatalf("Allocation.Add: expected %f; actual %f", 1.75, act.CPUCoreRequestAverage)
	}
	if !util.IsApproximately(1.25, act.CPUCoreUsageAverage) {
		t.Fatalf("Allocation.Add: expected %f; actual %f", 1.25, act.CPUCoreUsageAverage)
	}
	if !util.IsApproximately(4.00*gib, act.RAMBytesRequestAverage) {
		t.Fatalf("Allocation.Add: expected %f; actual %f", 4.00*gib, act.RAMBytesRequestAverage)
	}
	if !util.IsApproximately(8.00*gib, act.RAMBytesUsageAverage) {
		t.Fatalf("Allocation.Add: expected %f; actual %f", 8.00*gib, act.RAMBytesUsageAverage)
	}

	// Efficiency should be computed accurately from new request/usage
	// CPU efficiency = 1.25/1.75 = 0.7142857
	// RAM efficiency = 8.00/4.00 = 2.0000000
	// Total efficiency = (0.7142857*0.72 + 2.0*1.92)/(2.64) = 1.6493506
	if !util.IsApproximately(0.7142857, act.CPUEfficiency()) {
		t.Fatalf("Allocation.Add: expected %f; actual %f", 0.7142857, act.CPUEfficiency())
	}
	if !util.IsApproximately(2.0000000, act.RAMEfficiency()) {
		t.Fatalf("Allocation.Add: expected %f; actual %f", 2.0000000, act.RAMEfficiency())
	}
	if !util.IsApproximately(1.279690, act.TotalEfficiency()) {
		t.Fatalf("Allocation.Add: expected %f; actual %f", 1.279690, act.TotalEfficiency())
	}

	if act.RawAllocationOnly != nil {
		t.Errorf("Allocation.Add: Raw only data must be nil after an add")
	}
}

func TestAllocation_Share(t *testing.T) {
	cpuPrice := 0.02
	gpuPrice := 2.00
	ramPrice := 0.01
	pvPrice := 0.00005
	gib := 1024.0 * 1024.0 * 1024.0

	s1 := time.Date(2021, time.January, 1, 0, 0, 0, 0, time.UTC)
	e1 := time.Date(2021, time.January, 1, 12, 0, 0, 0, time.UTC)
	hrs1 := e1.Sub(s1).Hours()
	a1 := &Allocation{
		Start:                 s1,
		End:                   e1,
		Properties:            &AllocationProperties{},
		CPUCoreHours:          2.0 * hrs1,
		CPUCoreRequestAverage: 2.0,
		CPUCoreUsageAverage:   1.0,
		CPUCost:               2.0 * hrs1 * cpuPrice,
		CPUCostAdjustment:     3.0,
		GPUHours:              1.0 * hrs1,
		GPUCost:               1.0 * hrs1 * gpuPrice,
		GPUCostAdjustment:     2.0,
		PVs: PVAllocations{
			disk: {
				ByteHours: 100.0 * gib * hrs1,
				Cost:      100.0 * hrs1 * pvPrice,
			},
		},
		PVCostAdjustment:       4.0,
		RAMByteHours:           8.0 * gib * hrs1,
		RAMBytesRequestAverage: 8.0 * gib,
		RAMBytesUsageAverage:   4.0 * gib,
		RAMCost:                8.0 * hrs1 * ramPrice,
		RAMCostAdjustment:      1.0,
		SharedCost:             2.00,
		ExternalCost:           1.00,
	}
	a1b := a1.Clone()

	s2 := time.Date(2021, time.January, 1, 6, 0, 0, 0, time.UTC)
	e2 := time.Date(2021, time.January, 1, 24, 0, 0, 0, time.UTC)
	hrs2 := e1.Sub(s1).Hours()
	a2 := &Allocation{
		Start:                  s2,
		End:                    e2,
		Properties:             &AllocationProperties{},
		CPUCoreHours:           1.0 * hrs2,
		CPUCoreRequestAverage:  1.0,
		CPUCoreUsageAverage:    1.0,
		CPUCost:                1.0 * hrs2 * cpuPrice,
		GPUHours:               0.0,
		GPUCost:                0.0,
		RAMByteHours:           8.0 * gib * hrs2,
		RAMBytesRequestAverage: 0.0,
		RAMBytesUsageAverage:   8.0 * gib,
		RAMCost:                8.0 * hrs2 * ramPrice,
		NetworkCost:            0.01,
		LoadBalancerCost:       0.05,
		SharedCost:             0.00,
		ExternalCost:           1.00,
	}
	a2b := a2.Clone()

	act, err := a1.Share(a2)
	if err != nil {
		t.Fatalf("Allocation.Share: unexpected error: %s", err)
	}

	// Neither Allocation should be mutated
	if !a1.Equal(a1b) {
		t.Fatalf("Allocation.Share: a1 illegally mutated")
	}
	if !a2.Equal(a2b) {
		t.Fatalf("Allocation.Share: a1 illegally mutated")
	}

	// SharedCost and TotalCost should reflect increase by a2.TotalCost
	if !util.IsApproximately(a1.TotalCost()+a2.TotalCost(), act.TotalCost()) {
		t.Fatalf("Allocation.Share: expected %f; actual %f", a1.TotalCost()+a2.TotalCost(), act.TotalCost())
	}
	if !util.IsApproximately(a1.SharedCost+a2.TotalCost(), act.SharedCost) {
		t.Fatalf("Allocation.Share: expected %f; actual %f", a1.SharedCost+a2.TotalCost(), act.SharedCost)
	}

	// Costs should match before (expect TotalCost and SharedCost)
	if !util.IsApproximately(a1.CPUTotalCost(), act.CPUTotalCost()) {
		t.Fatalf("Allocation.Share: expected %f; actual %f", a1.CPUTotalCost(), act.CPUTotalCost())
	}
	if !util.IsApproximately(a1.GPUTotalCost(), act.GPUTotalCost()) {
		t.Fatalf("Allocation.Share: expected %f; actual %f", a1.GPUTotalCost(), act.GPUTotalCost())
	}
	if !util.IsApproximately(a1.RAMTotalCost(), act.RAMTotalCost()) {
		t.Fatalf("Allocation.Share: expected %f; actual %f", a1.RAMTotalCost(), act.RAMTotalCost())
	}
	if !util.IsApproximately(a1.PVTotalCost(), act.PVTotalCost()) {
		t.Fatalf("Allocation.Share: expected %f; actual %f", a1.PVTotalCost(), act.PVTotalCost())
	}
	if !util.IsApproximately(a1.NetworkCost, act.NetworkCost) {
		t.Fatalf("Allocation.Share: expected %f; actual %f", a1.NetworkCost, act.NetworkCost)
	}
	if !util.IsApproximately(a1.LoadBalancerCost, act.LoadBalancerCost) {
		t.Fatalf("Allocation.Share: expected %f; actual %f", a1.LoadBalancerCost, act.LoadBalancerCost)
	}
	if !util.IsApproximately(a1.ExternalCost, act.ExternalCost) {
		t.Fatalf("Allocation.Share: expected %f; actual %f", a1.ExternalCost, act.ExternalCost)
	}

	// ResourceHours should match before
	if !util.IsApproximately(a1.CPUCoreHours, act.CPUCoreHours) {
		t.Fatalf("Allocation.Share: expected %f; actual %f", a1.CPUCoreHours, act.CPUCoreHours)
	}
	if !util.IsApproximately(a1.RAMByteHours, act.RAMByteHours) {
		t.Fatalf("Allocation.Share: expected %f; actual %f", a1.RAMByteHours, act.RAMByteHours)
	}
	if !util.IsApproximately(a1.PVByteHours(), act.PVByteHours()) {
		t.Fatalf("Allocation.Share: expected %f; actual %f", a1.PVByteHours(), act.PVByteHours())
	}

	// Minutes should match before
	if !act.Start.Equal(a1.Start) || !act.End.Equal(a1.End) {
		t.Fatalf("Allocation.Share: expected %s; actual %s", NewWindow(&a1.Start, &a1.End), NewWindow(&act.Start, &act.End))
	}
	if act.Minutes() != a1.Minutes() {
		t.Fatalf("Allocation.Share: expected %f; actual %f", a1.Minutes(), act.Minutes())
	}

	// Requests and Usage should match before
	if !util.IsApproximately(a1.CPUCoreRequestAverage, act.CPUCoreRequestAverage) {
		t.Fatalf("Allocation.Share: expected %f; actual %f", a1.CPUCoreRequestAverage, act.CPUCoreRequestAverage)
	}
	if !util.IsApproximately(a1.CPUCoreUsageAverage, act.CPUCoreUsageAverage) {
		t.Fatalf("Allocation.Share: expected %f; actual %f", a1.CPUCoreUsageAverage, act.CPUCoreUsageAverage)
	}
	if !util.IsApproximately(a1.RAMBytesRequestAverage, act.RAMBytesRequestAverage) {
		t.Fatalf("Allocation.Share: expected %f; actual %f", a1.RAMBytesRequestAverage, act.RAMBytesRequestAverage)
	}
	if !util.IsApproximately(a1.RAMBytesUsageAverage, act.RAMBytesUsageAverage) {
		t.Fatalf("Allocation.Share: expected %f; actual %f", a1.RAMBytesUsageAverage, act.RAMBytesUsageAverage)
	}

	// Efficiency should match before
	if !util.IsApproximately(a1.CPUEfficiency(), act.CPUEfficiency()) {
		t.Fatalf("Allocation.Share: expected %f; actual %f", a1.CPUEfficiency(), act.CPUEfficiency())
	}
	if !util.IsApproximately(a1.RAMEfficiency(), act.RAMEfficiency()) {
		t.Fatalf("Allocation.Share: expected %f; actual %f", a1.RAMEfficiency(), act.RAMEfficiency())
	}
	if !util.IsApproximately(a1.TotalEfficiency(), act.TotalEfficiency()) {
		t.Fatalf("Allocation.Share: expected %f; actual %f", a1.TotalEfficiency(), act.TotalEfficiency())
	}
}

func TestAllocation_AddDifferentController(t *testing.T) {
	a1 := &Allocation{
		Properties: &AllocationProperties{
			Container:  "container",
			Pod:        "pod",
			Namespace:  "ns",
			Cluster:    "cluster",
			Controller: "controller 1",
		},
	}
	a2 := a1.Clone()
	a2.Properties.Controller = "controller 2"

	result, err := a1.Add(a2)
	if err != nil {
		t.Fatalf("Allocation.Add: unexpected error: %s", err)
	}

	if result.Properties.Controller == "" {
		t.Errorf("Adding allocations whose properties only differ in controller name should not result in an empty string controller name.")
	}

}

func TestAllocationSet_generateKey(t *testing.T) {
	var alloc *Allocation
	var key string

	props := []string{
		AllocationClusterProp,
	}

	key = alloc.generateKey(props, nil)
	if key != "" {
		t.Fatalf("generateKey: expected \"\"; actual \"%s\"", key)
	}

	alloc = &Allocation{}
	alloc.Properties = &AllocationProperties{
		Cluster: "cluster1",
		Labels: map[string]string{
			"app": "app1",
			"env": "env1",
		},
	}

	key = alloc.generateKey(props, nil)
	if key != "cluster1" {
		t.Fatalf("generateKey: expected \"cluster1\"; actual \"%s\"", key)
	}

	props = []string{
		AllocationClusterProp,
		AllocationNamespaceProp,
		"label:app",
	}

	key = alloc.generateKey(props, nil)
	if key != "cluster1//app1" {
		t.Fatalf("generateKey: expected \"cluster1//app1\"; actual \"%s\"", key)
	}

	alloc.Properties = &AllocationProperties{
		Cluster:   "cluster1",
		Namespace: "namespace1",
		Labels: map[string]string{
			"app": "app1",
			"env": "env1",
		},
	}
	key = alloc.generateKey(props, nil)
	if key != "cluster1/namespace1/app1" {
		t.Fatalf("generateKey: expected \"cluster1/namespace1/app1\"; actual \"%s\"", key)
	}

	props = []string{
		AllocationDepartmentProp,
		AllocationEnvironmentProp,
		AllocationOwnerProp,
		AllocationProductProp,
		AllocationTeamProp,
	}

	labelConfig := NewLabelConfig()

	alloc.Properties = &AllocationProperties{
		Cluster:   "cluster1",
		Namespace: "namespace1",
		Labels: map[string]string{
			labelConfig.DepartmentLabel:  "dept1",
			labelConfig.EnvironmentLabel: "envt1",
			labelConfig.OwnerLabel:       "ownr1",
			labelConfig.ProductLabel:     "prod1",
			labelConfig.TeamLabel:        "team1",
		},
	}
	key = alloc.generateKey(props, nil)
	if key != "dept1/envt1/ownr1/prod1/team1" {
		t.Fatalf("generateKey: expected \"dept1/envt1/ownr1/prod1/team1\"; actual \"%s\"", key)
	}

	// Ensure that labels with illegal Prometheus characters in LabelConfig
	// still match their sanitized values. Ensure also that multiple comma-
	// separated values work.

	labelConfig.DepartmentLabel = "prom/illegal-department"
	labelConfig.EnvironmentLabel = " env "
	labelConfig.OwnerLabel = "$owner%"
	labelConfig.ProductLabel = "app.kubernetes.io/app"
	labelConfig.TeamLabel = "team,app.kubernetes.io/team,k8s-team"

	alloc.Properties = &AllocationProperties{
		Cluster:   "cluster1",
		Namespace: "namespace1",
		Labels: map[string]string{
			"prom_illegal_department": "dept1",
			"env":                     "envt1",
			"_owner_":                 "ownr1",
			"team":                    "team1",
			"app_kubernetes_io_app":   "prod1",
			"app_kubernetes_io_team":  "team2",
		},
	}

	props = []string{
		AllocationDepartmentProp,
		AllocationEnvironmentProp,
		AllocationOwnerProp,
		AllocationProductProp,
		AllocationTeamProp,
	}

	key = alloc.generateKey(props, labelConfig)
	if key != "dept1/envt1/ownr1/prod1/team1/team2/__unallocated__" {
		t.Fatalf("generateKey: expected \"dept1/envt1/ownr1/prod1/team1/team2/__unallocated__\"; actual \"%s\"", key)
	}
}

func TestNewAllocationSet(t *testing.T) {
	// TODO niko/etl
}

func assertAllocationSetTotals(t *testing.T, as *AllocationSet, msg string, err error, length int, totalCost float64) {
	if err != nil {
		t.Fatalf("AllocationSet.AggregateBy[%s]: unexpected error: %s", msg, err)
	}
	if as.Length() != length {
		t.Fatalf("AllocationSet.AggregateBy[%s]: expected set of length %d, actual %d", msg, length, as.Length())
	}
	if math.Round(as.TotalCost()*100) != math.Round(totalCost*100) {
		t.Fatalf("AllocationSet.AggregateBy[%s]: expected total cost %.2f, actual %.2f", msg, totalCost, as.TotalCost())
	}
}

func assertParcResults(t *testing.T, as *AllocationSet, msg string, exps map[string]ProportionalAssetResourceCosts) {
	for allocKey, a := range as.Allocations {
		for key, actualParc := range a.ProportionalAssetResourceCosts {
			expectedParcs := exps[allocKey]

			// round to prevent floating point issues from failing tests at ultra high precision
			actualParc.NodeResourceCostPercentage = roundFloat(actualParc.NodeResourceCostPercentage)
			actualParc.CPUPercentage = roundFloat(actualParc.CPUPercentage)
			actualParc.RAMPercentage = roundFloat(actualParc.RAMPercentage)
			actualParc.GPUPercentage = roundFloat(actualParc.GPUPercentage)
			actualParc.PVPercentage = roundFloat(actualParc.PVPercentage)
			if !reflect.DeepEqual(expectedParcs[key], actualParc) {
				t.Fatalf("actual PARC %+v did not match expected PARC %+v", actualParc, expectedParcs[key])
			}
		}

	}
}
func roundFloat(val float64) float64 {
	ratio := math.Pow(10, float64(5))
	return math.Round(val*ratio) / ratio
}

func assertAllocationTotals(t *testing.T, as *AllocationSet, msg string, exps map[string]float64) {
	for _, a := range as.Allocations {
		if exp, ok := exps[a.Name]; ok {
			if math.Round(a.TotalCost()*100) != math.Round(exp*100) {
				t.Fatalf("AllocationSet.AggregateBy[%s]: expected total cost %f, actual %f", msg, exp, a.TotalCost())
			}
		} else {
			t.Fatalf("AllocationSet.AggregateBy[%s]: unexpected allocation: %s", msg, a.Name)
		}
	}
}

func assertAllocationWindow(t *testing.T, as *AllocationSet, msg string, expStart, expEnd time.Time, expMinutes float64) {
	for _, a := range as.Allocations {
		if !a.Start.Equal(expStart) {
			t.Fatalf("AllocationSet.AggregateBy[%s]: expected start %s, actual %s", msg, expStart, a.Start)
		}
		if !a.End.Equal(expEnd) {
			t.Fatalf("AllocationSet.AggregateBy[%s]: expected end %s, actual %s", msg, expEnd, a.End)
		}
		if a.Minutes() != expMinutes {
			t.Fatalf("AllocationSet.AggregateBy[%s]: expected minutes %f, actual %f", msg, expMinutes, a.Minutes())
		}
	}
}

func printAllocationSet(msg string, as *AllocationSet) {
	fmt.Printf("--- %s ---\n", msg)
	for _, a := range as.Allocations {
		fmt.Printf(" > %s\n", a)
	}
}

func TestAllocationSet_AggregateBy(t *testing.T) {
	// Test AggregateBy against the following workload topology, which is
	// generated by GenerateMockAllocationSet:

	// | Hierarchy                              | Cost |  CPU |  RAM |  GPU |   PV |  Net |  LB  |
	// +----------------------------------------+------+------+------+------+------+------+------+
	//   cluster1:
	//     idle:                                  20.00   5.00  15.00   0.00   0.00   0.00   0.00
	//     namespace1:
	//       pod1:
	//         container1: [app1, env1]   16.00   1.00  11.00   1.00   1.00   1.00   1.00
	//       pod-abc: (deployment1)
	//         container2:                         6.00   1.00   1.00   1.00   1.00   1.00   1.00
	//       pod-def: (deployment1)
	//         container3:                         6.00   1.00   1.00   1.00   1.00   1.00   1.00
	//     namespace2:
	//       pod-ghi: (deployment2)
	//         container4: [app2, env2]    6.00   1.00   1.00   1.00   1.00   1.00   1.00
	//         container5: [app2, env2]    6.00   1.00   1.00   1.00   1.00   1.00   1.00
	//       pod-jkl: (daemonset1)
	//         container6: {service1}              6.00   1.00   1.00   1.00   1.00   1.00   1.00
	// +-----------------------------------------+------+------+------+------+------+------+------+
	//   cluster1 subtotal                        66.00  11.00  31.00   6.00   6.00   6.00   6.00
	// +-----------------------------------------+------+------+------+------+------+------+------+
	//   cluster2:
	//     idle:                                  10.00   5.00   5.00   0.00   0.00   0.00   0.00
	//     namespace2:
	//       pod-mno: (deployment2)
	//         container4: [app2]              6.00   1.00   1.00   1.00   1.00   1.00   1.00
	//         container5: [app2]              6.00   1.00   1.00   1.00   1.00   1.00   1.00
	//       pod-pqr: (daemonset1)
	//         container6: {service1}              6.00   1.00   1.00   1.00   1.00   1.00   1.00
	//     namespace3:
	//       pod-stu: (deployment3)
	//         container7: an[team1]          6.00   1.00   1.00   1.00   1.00   1.00   1.00
	//       pod-vwx: (statefulset1)
	//         container8: an[team2]          6.00   1.00   1.00   1.00   1.00   1.00   1.00
	//         container9: an[team1]          6.00   1.00   1.00   1.00   1.00   1.00   1.00
	// +----------------------------------------+------+------+------+------+------+------+------+
	//   cluster2 subtotal                        46.00  11.00  11.00   6.00   6.00   6.00   6.00
	// +----------------------------------------+------+------+------+------+------+------+------+
	//   total                                   112.00  22.00  42.00  12.00  12.00  12.00  12.00
	// +----------------------------------------+------+------+------+------+------+------+------+

	// Scenarios to test:

	// 1  Single-aggregation
	// 1a AggregationProperties=(Cluster)
	// 1b AggregationProperties=(Namespace)
	// 1c AggregationProperties=(Pod)
	// 1d AggregationProperties=(Container)
	// 1e AggregationProperties=(ControllerKind)
	// 1f AggregationProperties=(Controller)
	// 1g AggregationProperties=(Service)
	// 1h AggregationProperties=(Label:app)

	// 2  Multi-aggregation
	// 2a AggregationProperties=(Cluster, Namespace)
	// 2b AggregationProperties=(Namespace, Label:app)
	// 2c AggregationProperties=(Cluster, Namespace, Pod, Container)
	// 2d AggregationProperties=(Label:app, Label:environment)

	// 3  Share idle
	// 3a AggregationProperties=(Namespace) ShareIdle=ShareWeighted
	// 3b AggregationProperties=(Namespace) ShareIdle=ShareEven (TODO niko/etl)

	// 4  Share resources
	// 4a Share namespace ShareEven
	// 4b Share cluster ShareWeighted
	// 4c Share label ShareEven
	// 4d Share overhead ShareWeighted

	// 5  Filters
	// 5a Filter by cluster with separate idle
	// 5b Filter by cluster with shared idle
	// TODO niko/idle more filter tests

	// 6  Combinations and options
	// 6a SplitIdle
	// 6b Share idle with filters
	// 6c Share resources with filters
	// 6d Share idle and share resources
	// 6e IdleByNode

	// 7  Edge cases and errors
	// 7a Empty AggregationProperties
	// 7b Filter all
	// 7c Share all
	// 7d Share and filter the same allocations

	// Definitions and set-up:

	var as *AllocationSet
	var err error

	endYesterday := time.Now().UTC().Truncate(day)
	startYesterday := endYesterday.Add(-day)

	numClusters := 2
	numNamespaces := 3
	numPods := 9
	numContainers := 9
	numControllerKinds := 3
	numControllers := 5
	numServices := 1
	numLabelApps := 2

	// By default, idle is reported as a single, merged allocation
	numIdle := 1
	// There will only ever be one __unallocated__
	numUnallocated := 1
	// There are two clusters, so each gets an idle entry when they are split
	numSplitIdleCluster := 2

	// There are two clusters, so each gets an idle entry when they are split
	numSplitIdleNode := 4

	activeTotalCost := 82.0
	idleTotalCost := 30.0
	sharedOverheadHourlyCost := 7.0

	// Match filters

	// This is ugly, but required because cannot import filterutil due to import cycle
	namespaceEquals := func(ns string) *ast.EqualOp {
		return &ast.EqualOp{
			Left: ast.Identifier{
				Field: ast.NewField(allocation.FieldNamespace),
				Key:   "",
			},
			Right: ns,
		}
	}

	// This is ugly, but required because cannot import filterutil due to import cycle
	labelEquals := func(name, value string) *ast.EqualOp {
		return &ast.EqualOp{
			Left: ast.Identifier{
				Field: ast.NewField(allocation.FieldLabel),
				Key:   name,
			},
			Right: value,
		}
	}

	end := time.Now().UTC().Truncate(day)
	start := end.Add(-day)

	// Tests:
	cases := map[string]struct {
		start               time.Time
		aggBy               []string
		aggOpts             *AllocationAggregationOptions
		numResults          int
		totalCost           float64
		results             map[string]float64
		windowStart         time.Time
		windowEnd           time.Time
		expMinutes          float64
		expectedParcResults map[string]ProportionalAssetResourceCosts
	}{
		// 1  Single-aggregation

		// 1a AggregationProperties=(Cluster)
		"1a": {
			start: start,
			aggBy: []string{AllocationClusterProp},
			aggOpts: &AllocationAggregationOptions{
				IncludeProportionalAssetResourceCosts: true,
			},
			numResults: numClusters + numIdle,
			totalCost:  activeTotalCost + idleTotalCost,
			results: map[string]float64{
				"cluster1": 46.00,
				"cluster2": 36.00,
				IdleSuffix: 30.00,
			},
			windowStart: startYesterday,
			windowEnd:   endYesterday,
			expMinutes:  1440.0,
			expectedParcResults: map[string]ProportionalAssetResourceCosts{
				"cluster1": {
					"cluster1": ProportionalAssetResourceCost{
						Cluster:             "cluster1",
						Name:                "",
						Type:                "",
						ProviderID:          "",
						GPUProportionalCost: 6.0,
						CPUProportionalCost: 6.0,
						RAMProportionalCost: 16.0,
						PVProportionalCost:  6.0,
					},
				},
				"cluster2": {
					"cluster2": ProportionalAssetResourceCost{
						Cluster:             "cluster2",
						Name:                "",
						Type:                "",
						ProviderID:          "",
						GPUProportionalCost: 6,
						CPUProportionalCost: 6,
						RAMProportionalCost: 6,
						PVProportionalCost:  6,
					},
				},
			},
		},
		// 1b AggregationProperties=(Namespace)
		"1b": {
			start:      start,
			aggBy:      []string{AllocationNamespaceProp},
			aggOpts:    nil,
			numResults: numNamespaces + numIdle,
			totalCost:  activeTotalCost + idleTotalCost,
			results: map[string]float64{
				"namespace1": 28.00,
				"namespace2": 36.00,
				"namespace3": 18.00,
				IdleSuffix:   30.00,
			},
			windowStart: startYesterday,
			windowEnd:   endYesterday,
			expMinutes:  1440.0,
		},
		// 1c AggregationProperties=(Pod)
		"1c": {
			start: start,
			aggBy: []string{AllocationPodProp},
			aggOpts: &AllocationAggregationOptions{
				IncludeProportionalAssetResourceCosts: true,
			},
			numResults: numPods + numIdle,
			totalCost:  activeTotalCost + idleTotalCost,
			results: map[string]float64{
				"pod-jkl":  6.00,
				"pod-stu":  6.00,
				"pod-abc":  6.00,
				"pod-pqr":  6.00,
				"pod-def":  6.00,
				"pod-vwx":  12.00,
				"pod1":     16.00,
				"pod-mno":  12.00,
				"pod-ghi":  12.00,
				IdleSuffix: 30.00,
			},
			windowStart: startYesterday,
			windowEnd:   endYesterday,
			expMinutes:  1440.0,
			expectedParcResults: map[string]ProportionalAssetResourceCosts{
				"pod1": {
					"cluster1": ProportionalAssetResourceCost{
						Cluster:             "cluster1",
						Name:                "",
						Type:                "",
						ProviderID:          "",
						GPUProportionalCost: 1.0,
						CPUProportionalCost: 1.0,
						RAMProportionalCost: 11.0,
						PVProportionalCost:  1.0,
					},
				},
				"pod-abc": {
					"cluster1": ProportionalAssetResourceCost{
						Cluster:             "cluster1",
						Name:                "",
						Type:                "",
						ProviderID:          "",
						GPUProportionalCost: 1.0,
						CPUProportionalCost: 1.0,
						RAMProportionalCost: 1.0,
						PVProportionalCost:  1.0,
					},
				},
				"pod-def": {
					"cluster1": ProportionalAssetResourceCost{
						Cluster:             "cluster1",
						Name:                "",
						Type:                "",
						ProviderID:          "",
						GPUProportionalCost: 1.0,
						CPUProportionalCost: 1.0,
						RAMProportionalCost: 1.0,
						PVProportionalCost:  1.0,
					},
				},
				"pod-ghi": {
					"cluster1": ProportionalAssetResourceCost{
						Cluster:             "cluster1",
						Name:                "",
						Type:                "",
						ProviderID:          "",
						GPUProportionalCost: 2.0,
						CPUProportionalCost: 2.0,
						RAMProportionalCost: 2.0,
						PVProportionalCost:  2.0,
					},
				},
				"pod-jkl": {
					"cluster1": ProportionalAssetResourceCost{
						Cluster:             "cluster1",
						Name:                "",
						Type:                "",
						ProviderID:          "",
						GPUProportionalCost: 1.0,
						CPUProportionalCost: 1.0,
						RAMProportionalCost: 1.0,
						PVProportionalCost:  1.0,
					},
				},
				"pod-mno": {
					"cluster2": ProportionalAssetResourceCost{
						Cluster:             "cluster2",
						Name:                "",
						Type:                "",
						ProviderID:          "",
						GPUProportionalCost: 2.0,
						CPUProportionalCost: 2.0,
						RAMProportionalCost: 2.0,
						PVProportionalCost:  2.0,
					},
				},
				"pod-pqr": {
					"cluster2": ProportionalAssetResourceCost{
						Cluster:             "cluster2",
						Name:                "",
						Type:                "",
						ProviderID:          "",
						GPUProportionalCost: 1.0,
						CPUProportionalCost: 1.0,
						RAMProportionalCost: 1.0,
						PVProportionalCost:  1.0,
					},
				},
				"pod-stu": {
					"cluster2": ProportionalAssetResourceCost{
						Cluster:             "cluster2",
						Name:                "",
						Type:                "",
						ProviderID:          "",
						GPUProportionalCost: 1.0,
						CPUProportionalCost: 1.0,
						RAMProportionalCost: 1.0,
						PVProportionalCost:  1.0,
					},
				},
				"pod-vwx": {
					"cluster2": ProportionalAssetResourceCost{
						Cluster:             "cluster2",
						Name:                "",
						Type:                "",
						ProviderID:          "",
						GPUProportionalCost: 2.0,
						CPUProportionalCost: 2.0,
						RAMProportionalCost: 2.0,
						PVProportionalCost:  2.0,
					},
				},
			},
		},
		// 1d AggregationProperties=(Container)
		"1d": {
			start:      start,
			aggBy:      []string{AllocationContainerProp},
			aggOpts:    nil,
			numResults: numContainers + numIdle,
			totalCost:  activeTotalCost + idleTotalCost,
			results: map[string]float64{
				"container2": 6.00,
				"container9": 6.00,
				"container6": 12.00,
				"container3": 6.00,
				"container4": 12.00,
				"container7": 6.00,
				"container8": 6.00,
				"container5": 12.00,
				"container1": 16.00,
				IdleSuffix:   30.00,
			},
			windowStart: startYesterday,
			windowEnd:   endYesterday,
			expMinutes:  1440.0,
		},
		// 1e AggregationProperties=(ControllerKind)
		"1e": {
			start:      start,
			aggBy:      []string{AllocationControllerKindProp},
			aggOpts:    nil,
			numResults: numControllerKinds + numIdle + numUnallocated,
			totalCost:  activeTotalCost + idleTotalCost,
			results: map[string]float64{
				"daemonset":       12.00,
				"deployment":      42.00,
				"statefulset":     12.00,
				IdleSuffix:        30.00,
				UnallocatedSuffix: 16.00,
			},
			windowStart: startYesterday,
			windowEnd:   endYesterday,
			expMinutes:  1440.0,
		},
		// 1f AggregationProperties=(Controller)
		"1f": {
			start:      start,
			aggBy:      []string{AllocationControllerProp},
			aggOpts:    nil,
			numResults: numControllers + numIdle + numUnallocated,
			totalCost:  activeTotalCost + idleTotalCost,
			results: map[string]float64{
				"deployment:deployment2":   24.00,
				"daemonset:daemonset1":     12.00,
				"deployment:deployment3":   6.00,
				"statefulset:statefulset1": 12.00,
				"deployment:deployment1":   12.00,
				IdleSuffix:                 30.00,
				UnallocatedSuffix:          16.00,
			},
			windowStart: startYesterday,
			windowEnd:   endYesterday,
			expMinutes:  1440.0,
		},
		// 1g AggregationProperties=(Service)
		"1g": {
			start:      start,
			aggBy:      []string{AllocationServiceProp},
			aggOpts:    nil,
			numResults: numServices + numIdle + numUnallocated,
			totalCost:  activeTotalCost + idleTotalCost,
			results: map[string]float64{
				"service1":        12.00,
				IdleSuffix:        30.00,
				UnallocatedSuffix: 70.00,
			},
			windowStart: startYesterday,
			windowEnd:   endYesterday,
			expMinutes:  1440.0,
		},
		// 1h AggregationProperties=(Label:app)
		"1h": {
			start:      start,
			aggBy:      []string{"label:app"},
			aggOpts:    nil,
			numResults: numLabelApps + numIdle + numUnallocated,
			totalCost:  activeTotalCost + idleTotalCost,
			results: map[string]float64{
				"app1":            16.00,
				"app2":            24.00,
				IdleSuffix:        30.00,
				UnallocatedSuffix: 42.00,
			},
			windowStart: startYesterday,
			windowEnd:   endYesterday,
			expMinutes:  1440.0,
		},
		// 1i AggregationProperties=(deployment)
		"1i": {
			start:      start,
			aggBy:      []string{AllocationDeploymentProp},
			aggOpts:    nil,
			numResults: 3 + numIdle + numUnallocated,
			totalCost:  activeTotalCost + idleTotalCost,
			results: map[string]float64{
				"deployment1":     12.00,
				"deployment2":     24.00,
				"deployment3":     6.00,
				IdleSuffix:        30.00,
				UnallocatedSuffix: 40.00,
			},
			windowStart: startYesterday,
			windowEnd:   endYesterday,
			expMinutes:  1440.0,
		},
		// 1j AggregationProperties=(Annotation:team)
		"1j": {
			start:      start,
			aggBy:      []string{"annotation:team"},
			aggOpts:    nil,
			numResults: 2 + numIdle + numUnallocated,
			totalCost:  activeTotalCost + idleTotalCost,
			results: map[string]float64{
				"team1":           12.00,
				"team2":           6.00,
				IdleSuffix:        30.00,
				UnallocatedSuffix: 64.00,
			},
			windowStart: startYesterday,
			windowEnd:   endYesterday,
			expMinutes:  1440.0,
		},
		// 1k AggregationProperties=(daemonSet)
		"1k": {
			start:      start,
			aggBy:      []string{AllocationDaemonSetProp},
			aggOpts:    nil,
			numResults: 1 + numIdle + numUnallocated,
			totalCost:  activeTotalCost + idleTotalCost,
			results: map[string]float64{
				"daemonset1":      12.00,
				IdleSuffix:        30.00,
				UnallocatedSuffix: 70.00,
			},
			windowStart: startYesterday,
			windowEnd:   endYesterday,
			expMinutes:  1440.0,
		},
		// 1l AggregationProperties=(statefulSet)
		"1l": {
			start:      start,
			aggBy:      []string{AllocationStatefulSetProp},
			aggOpts:    nil,
			numResults: 1 + numIdle + numUnallocated,
			totalCost:  activeTotalCost + idleTotalCost,
			results: map[string]float64{
				"statefulset1":    12.00,
				IdleSuffix:        30.00,
				UnallocatedSuffix: 70.00,
			},
			windowStart: startYesterday,
			windowEnd:   endYesterday,
			expMinutes:  1440.0,
		},
		// 2  Multi-aggregation

		// 2a AggregationProperties=(Cluster, Namespace)
		// 2b AggregationProperties=(Namespace, Label:app)
		// 2c AggregationProperties=(Cluster, Namespace, Pod, Container)
		// 2d AggregationProperties=(Label:app, Label:environment)
		"2d": {
			start:      start,
			aggBy:      []string{"label:app", "label:env"},
			aggOpts:    nil,
			numResults: 3 + numIdle + numUnallocated,
			totalCost:  activeTotalCost + idleTotalCost,
			// sets should be {idle, unallocated, app1/env1, app2/env2, app2/unallocated}
			results: map[string]float64{
				"app1/env1":                 16.00,
				"app2/env2":                 12.00,
				"app2/" + UnallocatedSuffix: 12.00,
				IdleSuffix:                  30.00,
				UnallocatedSuffix + "/" + UnallocatedSuffix: 42.00,
			},
			windowStart: startYesterday,
			windowEnd:   endYesterday,
			expMinutes:  1440.0,
		},
		// 2e AggregationProperties=(Cluster, Label:app, Label:environment)
		"2e": {
			start:      start,
			aggBy:      []string{AllocationClusterProp, "label:app", "label:env"},
			aggOpts:    nil,
			numResults: 6,
			totalCost:  activeTotalCost + idleTotalCost,
			results: map[string]float64{
				"cluster1/app2/env2": 12.00,
				"__idle__":           30.00,
				"cluster1/app1/env1": 16.00,
				"cluster1/" + UnallocatedSuffix + "/" + UnallocatedSuffix: 18.00,
				"cluster2/app2/" + UnallocatedSuffix:                      12.00,
				"cluster2/" + UnallocatedSuffix + "/" + UnallocatedSuffix: 24.00,
			},
			windowStart: startYesterday,
			windowEnd:   endYesterday,
			expMinutes:  1440.0,
		},
		// 2f AggregationProperties=(annotation:team, pod)
		"2f": {
			start:      start,
			aggBy:      []string{AllocationPodProp, "annotation:team"},
			aggOpts:    nil,
			numResults: 11,
			totalCost:  activeTotalCost + idleTotalCost,
			results: map[string]float64{
				"pod-jkl/" + UnallocatedSuffix: 6.00,
				"pod-stu/team1":                6.00,
				"pod-abc/" + UnallocatedSuffix: 6.00,
				"pod-pqr/" + UnallocatedSuffix: 6.00,
				"pod-def/" + UnallocatedSuffix: 6.00,
				"pod-vwx/team1":                6.00,
				"pod-vwx/team2":                6.00,
				"pod1/" + UnallocatedSuffix:    16.00,
				"pod-mno/" + UnallocatedSuffix: 12.00,
				"pod-ghi/" + UnallocatedSuffix: 12.00,
				IdleSuffix:                     30.00,
			},
			windowStart: startYesterday,
			windowEnd:   endYesterday,
			expMinutes:  1440.0,
		},
		// 3  Share idle

		// 3a AggregationProperties=(Namespace) ShareIdle=ShareWeighted
		// namespace1: 42.6875 = 28.00 + 5.00*(3.00/6.00) + 15.0*(13.0/16.0)
		// namespace2: 46.3125 = 36.00 + 5.0*(3.0/6.0) + 15.0*(3.0/16.0) + 5.0*(3.0/6.0) + 5.0*(3.0/6.0)
		// namespace3: 23.0000 = 18.00 + 5.0*(3.0/6.0) + 5.0*(3.0/6.0)
		"3a": {
			start:      start,
			aggBy:      []string{AllocationNamespaceProp},
			aggOpts:    &AllocationAggregationOptions{ShareIdle: ShareWeighted},
			numResults: numNamespaces,
			totalCost:  activeTotalCost + idleTotalCost,
			results: map[string]float64{
				"namespace1": 42.69,
				"namespace2": 46.31,
				"namespace3": 23.00,
			},
			windowStart: startYesterday,
			windowEnd:   endYesterday,
			expMinutes:  1440.0,
		},

		// 3b: sharing idle evenly is deprecated

		// 4  Share resources

		// 4a Share namespace ShareEven
		// namespace1: 37.5000 = 28.00 + 18.00*(1.0/2.0)
		// namespace2: 45.5000 = 36.00 + 18.00*(1.0/2.0)
		// idle:       30.0000
		"4a": {
			start: start,
			aggBy: []string{AllocationNamespaceProp},
			aggOpts: &AllocationAggregationOptions{
				Share:      namespaceEquals("namespace3"),
				ShareSplit: ShareEven,
			},
			numResults: numNamespaces,
			totalCost:  activeTotalCost + idleTotalCost,
			results: map[string]float64{
				"namespace1": 37.00,
				"namespace2": 45.00,
				IdleSuffix:   30.00,
			},
			windowStart: startYesterday,
			windowEnd:   endYesterday,
			expMinutes:  1440.0,
		},
		// 4b Share namespace ShareWeighted
		// namespace1: 32.5000 =
		// namespace2: 37.5000 =
		// idle:       30.0000
		"4b": {
			start: start,
			aggBy: []string{AllocationNamespaceProp},
			aggOpts: &AllocationAggregationOptions{
				Share:                                 namespaceEquals("namespace3"),
				ShareSplit:                            ShareWeighted,
				IncludeProportionalAssetResourceCosts: true,
			},
			numResults: numNamespaces,
			totalCost:  activeTotalCost + idleTotalCost,
			results: map[string]float64{
				"namespace1": 35.88,
				"namespace2": 46.125,
				IdleSuffix:   30.00,
			},
			windowStart: startYesterday,
			windowEnd:   endYesterday,
			expMinutes:  1440.0,
			expectedParcResults: map[string]ProportionalAssetResourceCosts{
				"namespace1": {
					"cluster1": ProportionalAssetResourceCost{
						Cluster:             "cluster1",
						Name:                "",
						Type:                "",
						ProviderID:          "",
						GPUProportionalCost: 3,
						CPUProportionalCost: 3,
						RAMProportionalCost: 13,
						PVProportionalCost:  3,
					},
				},
				"namespace2": {
					"cluster1": ProportionalAssetResourceCost{
						Cluster:             "cluster1",
						Name:                "",
						Type:                "",
						ProviderID:          "",
						GPUProportionalCost: 3,
						CPUProportionalCost: 3,
						RAMProportionalCost: 3,
						PVProportionalCost:  3,
					},
					"cluster2": ProportionalAssetResourceCost{
						Cluster:             "cluster2",
						Name:                "",
						Type:                "",
						ProviderID:          "",
						GPUProportionalCost: 3,
						CPUProportionalCost: 3,
						RAMProportionalCost: 3,
						PVProportionalCost:  3,
					},
				},
			},
		},
		// 4c Share label ShareEven
		// namespace1: 17.3333 = 28.00 - 16.00 + 16.00*(1.0/3.0)
		// namespace2: 41.3333 = 36.00 + 16.00*(1.0/3.0)
		// namespace3: 23.3333 = 18.00 + 16.00*(1.0/3.0)
		// idle:       30.0000
		"4c": {
			start: start,
			aggBy: []string{AllocationNamespaceProp},
			aggOpts: &AllocationAggregationOptions{
				Share:      labelEquals("app", "app1"),
				ShareSplit: ShareEven,
			},
			numResults: numNamespaces + numIdle,
			totalCost:  activeTotalCost + idleTotalCost,
			results: map[string]float64{
				"namespace1": 17.33,
				"namespace2": 41.33,
				"namespace3": 23.33,
				IdleSuffix:   30.00,
			},
			windowStart: startYesterday,
			windowEnd:   endYesterday,
			expMinutes:  1440.0,
		},
		// 4d Share overhead ShareWeighted
		// namespace1: 85.366 = 28.00 + (7.0*24.0)*(28.00/82.00)
		// namespace2: 109.756 = 36.00 + (7.0*24.0)*(36.00/82.00)
		// namespace3: 54.878 = 18.00 + (7.0*24.0)*(18.00/82.00)
		// idle:       30.0000
		"4d": {
			start: start,
			aggBy: []string{AllocationNamespaceProp},
			aggOpts: &AllocationAggregationOptions{
				SharedHourlyCosts: map[string]float64{"total": sharedOverheadHourlyCost},
				ShareSplit:        ShareWeighted,
			},
			numResults: numNamespaces + numIdle,
			totalCost:  activeTotalCost + idleTotalCost + (sharedOverheadHourlyCost * 24.0),
			results: map[string]float64{
				"namespace1": 85.366,
				"namespace2": 109.756,
				"namespace3": 54.878,
				IdleSuffix:   30.00,
			},
			windowStart: startYesterday,
			windowEnd:   endYesterday,
			expMinutes:  1440.0,
		},
		// 5  Filters

		// 5a Filter by cluster with separate idle
		"5a": {
			start: start,
			aggBy: []string{AllocationClusterProp},
			aggOpts: &AllocationAggregationOptions{
				Filter:    mustParseFilter(`cluster:"cluster1"`),
				ShareIdle: ShareNone,
			},
			numResults: 1 + numIdle,
			totalCost:  66.0,
			results: map[string]float64{
				"cluster1": 46.00,
				IdleSuffix: 20.00,
			},
			windowStart: startYesterday,
			windowEnd:   endYesterday,
			expMinutes:  1440.0,
		},
		// 5b Filter by cluster with shared idle
		"5b": {
			start: start,
			aggBy: []string{AllocationClusterProp},
			aggOpts: &AllocationAggregationOptions{
				Filter:    mustParseFilter(`cluster:"cluster1"`),
				ShareIdle: ShareWeighted,
			},
			numResults: 1,
			totalCost:  66.0,
			results: map[string]float64{
				"cluster1": 66.00,
			},
			windowStart: startYesterday,
			windowEnd:   endYesterday,
			expMinutes:  1440.0,
		},
		// 5c Filter by cluster, agg by namespace, with separate idle
		"5c": {
			start: start,
			aggBy: []string{AllocationNamespaceProp},
			aggOpts: &AllocationAggregationOptions{
				Filter:    mustParseFilter(`cluster:"cluster1"`),
				ShareIdle: ShareNone,
			},
			numResults: 2 + numIdle,
			totalCost:  66.0,
			results: map[string]float64{
				"namespace1": 28.00,
				"namespace2": 18.00,
				IdleSuffix:   20.00,
			},
			windowStart: startYesterday,
			windowEnd:   endYesterday,
			expMinutes:  1440.0,
		},
		// 5d Filter by namespace, agg by cluster, with separate idle
		"5d": {
			start: start,
			aggBy: []string{AllocationClusterProp},
			aggOpts: &AllocationAggregationOptions{
				Filter:    mustParseFilter(`namespace:"namespace2"`),
				ShareIdle: ShareNone,
			},
			numResults: numClusters + numIdle,
			totalCost:  46.31,
			results: map[string]float64{
				"cluster1": 18.00,
				"cluster2": 18.00,
				IdleSuffix: 10.31,
			},
			windowStart: startYesterday,
			windowEnd:   endYesterday,
			expMinutes:  1440.0,
		},
		// 6  Combinations and options

		// 6a SplitIdle
		"6a": {
			start: start,
			aggBy: []string{AllocationNamespaceProp},
			aggOpts: &AllocationAggregationOptions{
				SplitIdle: true,
			},
			numResults: numNamespaces + numSplitIdleCluster,
			totalCost:  activeTotalCost + idleTotalCost,
			results: map[string]float64{
				"namespace1":                           28.00,
				"namespace2":                           36.00,
				"namespace3":                           18.00,
				fmt.Sprintf("cluster1/%s", IdleSuffix): 20.00,
				fmt.Sprintf("cluster2/%s", IdleSuffix): 10.00,
			},
			windowStart: startYesterday,
			windowEnd:   endYesterday,
			expMinutes:  1440.0,
		},
		// 6b Share idle weighted with filters
		// Should match values from unfiltered aggregation (3a)
		// namespace2: 46.3125 = 36.00 + 5.0*(3.0/6.0) + 15.0*(3.0/16.0) + 5.0*(3.0/6.0) + 5.0*(3.0/6.0)
		"6b": {
			start: start,
			aggBy: []string{AllocationNamespaceProp},
			aggOpts: &AllocationAggregationOptions{
				Filter:    mustParseFilter(`namespace:"namespace2"`),
				ShareIdle: ShareWeighted,
			},
			numResults: 1,
			totalCost:  46.31,
			results: map[string]float64{
				"namespace2": 46.31,
			},
			windowStart: startYesterday,
			windowEnd:   endYesterday,
			expMinutes:  1440.0,
		},

		// 6c Share idle even with filters (share idle even is deprecated)

		// 6d Share overhead with filters
		// namespace1: 85.366 = 28.00 + (7.0*24.0)*(28.00/82.00)
		// namespace2: 109.756 = 36.00 + (7.0*24.0)*(36.00/82.00)
		// namespace3: 54.878 = 18.00 + (7.0*24.0)*(18.00/82.00)
		// idle:       10.3125 = % of idle paired with namespace2
		// Then namespace 2 is filtered.
		"6d": {
			start: start,
			aggBy: []string{AllocationNamespaceProp},
			aggOpts: &AllocationAggregationOptions{
				Filter:            mustParseFilter(`namespace:"namespace2"`),
				SharedHourlyCosts: map[string]float64{"total": sharedOverheadHourlyCost},
				ShareSplit:        ShareWeighted,
			},
			numResults: 1 + numIdle,
			totalCost:  120.0686,
			results: map[string]float64{
				"namespace2": 109.7561,
				IdleSuffix:   10.3125,
			},
			windowStart: startYesterday,
			windowEnd:   endYesterday,
			expMinutes:  1440.0,
		},
		// 6e Share resources with filters
		"6e": {
			start: start,
			aggBy: []string{AllocationNamespaceProp},
			aggOpts: &AllocationAggregationOptions{
				Filter:     mustParseFilter(`namespace:"namespace2"`),
				Share:      namespaceEquals("namespace1"),
				ShareSplit: ShareWeighted,
			},
			numResults: 1 + numIdle,
			totalCost:  79.6667, // should be 74.7708, but I'm punting -- too difficult (NK)
			results: map[string]float64{
				"namespace2": 54.6667,
				IdleSuffix:   25.000, // should be 20.1042, but I'm punting -- too difficult (NK)
			},
			windowStart: startYesterday,
			windowEnd:   endYesterday,
			expMinutes:  1440.0,
		},
		// 6f Share resources with filters and share idle
		"6f": {
			start: start,
			aggBy: []string{AllocationNamespaceProp},
			aggOpts: &AllocationAggregationOptions{
				Filter:     mustParseFilter(`namespace:"namespace2"`),
				Share:      namespaceEquals("namespace1"),
				ShareSplit: ShareWeighted,
				ShareIdle:  ShareWeighted,
			},
			numResults: 1,
			totalCost:  74.77083,
			results: map[string]float64{
				"namespace2": 74.77083,
			},
			windowStart: startYesterday,
			windowEnd:   endYesterday,
			expMinutes:  1440.0,
		},
		// 6g Share idle weighted and share resources weighted
		//
		// First, share idle weighted produces:
		//
		// namespace1:      42.6875
		//   initial cost   28.0000
		//   cluster1.cpu    2.5000 = 5.00*(3.00/6.00)
		//   cluster1.ram   12.1875 = 15.00*(13.0/16.0)
		//
		// namespace2:      46.3125
		//   initial cost   36.0000
		//   cluster1.cpu    2.5000 = 5.00*(3.0/6.0)
		//   cluster1.ram    2.8125 = 15.00*(3.0/16.0)
		//   cluster2.cpu    2.5000 = 5.00*(3.0/6.0)
		//   cluster2.ram    2.5000 = 5.00*(3.0/6.0)
		//
		// namespace3:      23.0000
		//   initial cost   18.0000
		//   cluster2.cpu    2.5000 = 5.00*(3.0/6.0)
		//   cluster2.ram    2.5000 = 5.00*(3.0/6.0)
		//
		// Then, sharing namespace1 means sharing 39.6875 according to coefficients
		// computed before allocating idle (so that weighting idle differently
		// doesn't adversely affect the sharing mechanism):
		//
		// namespace2:      74.7708
		//   initial cost   30.0000
		//   idle cost      10.3125
		//   shared cost    28.4583 = (42.6875)*(36.0/54.0)
		//
		// namespace3:      37.2292
		//   initial cost   18.0000
		//   idle cost       5.0000
		//   shared cost    14.2292 = (42.6875)*(18.0/54.0)
		"6g": {
			start: start,
			aggBy: []string{AllocationNamespaceProp},
			aggOpts: &AllocationAggregationOptions{
				Share:      namespaceEquals("namespace1"),
				ShareSplit: ShareWeighted,
				ShareIdle:  ShareWeighted,
			},
			numResults: 2,
			totalCost:  activeTotalCost + idleTotalCost,
			results: map[string]float64{
				"namespace2": 74.77,
				"namespace3": 37.23,
			},
			windowStart: startYesterday,
			windowEnd:   endYesterday,
			expMinutes:  1440.0,
		},
		// 6h Share idle, share resources, and filter
		//
		// First, share idle weighted produces:
		//
		// namespace1:      42.6875
		//   initial cost   28.0000
		//   cluster1.cpu    2.5000 = 5.00*(3.00/6.00)
		//   cluster1.ram   12.1875 = 15.00*(13.0/16.0)
		//
		// namespace2:      46.3125
		//   initial cost   36.0000
		//   cluster1.cpu    2.5000 = 5.00*(3.0/6.0)
		//   cluster1.ram    2.8125 = 15.00*(3.0/16.0)
		//   cluster2.cpu    2.5000 = 5.00*(3.0/6.0)
		//   cluster2.ram    2.5000 = 5.00*(3.0/6.0)
		//
		// namespace3:      23.0000
		//   initial cost   18.0000
		//   cluster2.cpu    2.5000 = 5.00*(3.0/6.0)
		//   cluster2.ram    2.5000 = 5.00*(3.0/6.0)
		//
		// Then, sharing namespace1 means sharing 39.6875 according to coefficients
		// computed before allocating idle (so that weighting idle differently
		// doesn't adversely affect the sharing mechanism):
		//
		// namespace2:      74.7708
		//   initial cost   36.0000
		//   idle cost      10.3125
		//   shared cost    28.4583 = (42.6875)*(36.0/54.0)
		//
		// namespace3:      37.2292
		//   initial cost   18.0000
		//   idle cost       5.0000
		//   shared cost    14.2292 = (42.6875)*(18.0/54.0)
		//
		// Then, filter for namespace2: 74.7708
		"6h": {
			start: start,
			aggBy: []string{AllocationNamespaceProp},
			aggOpts: &AllocationAggregationOptions{
				Filter:     mustParseFilter(`namespace:"namespace2"`),
				Share:      namespaceEquals("namespace1"),
				ShareSplit: ShareWeighted,
				ShareIdle:  ShareWeighted,
			},
			numResults: 1,
			totalCost:  74.77,
			results: map[string]float64{
				"namespace2": 74.77,
			},
			windowStart: startYesterday,
			windowEnd:   endYesterday,
			expMinutes:  1440.0,
		},
		// 6i Share idle, share resources, share overhead
		//
		// Share idle weighted:
		//
		// namespace1:      42.6875
		//   initial cost   28.0000
		//   cluster1.cpu    2.5000 = 5.00*(3.00/6.00)
		//   cluster1.ram   12.1875 = 15.00*(13.0/16.0)
		//
		// namespace2:      46.3125
		//   initial cost   36.0000
		//   cluster1.cpu    2.5000 = 5.00*(3.0/6.0)
		//   cluster1.ram    2.8125 = 15.00*(3.0/16.0)
		//   cluster2.cpu    2.5000 = 5.00*(3.0/6.0)
		//   cluster2.ram    2.5000 = 5.00*(3.0/6.0)
		//
		// namespace3:      23.0000
		//   initial cost   18.0000
		//   cluster2.cpu    2.5000 = 5.00*(3.0/6.0)
		//   cluster2.ram    2.5000 = 5.00*(3.0/6.0)
		//
		// Then share overhead:
		//
		// namespace1:     100.0533 = 42.6875 + (7.0*24.0)*(28.00/82.00)
		// namespace2:     120.0686 = 46.3125 + (7.0*24.0)*(36.00/82.00)
		// namespace3:      59.8780 = 23.0000 + (7.0*24.0)*(18.00/82.00)
		//
		// Then namespace 2 is filtered.
		"6i": {
			start: start,
			aggBy: []string{AllocationNamespaceProp},
			aggOpts: &AllocationAggregationOptions{
				Filter:            mustParseFilter(`namespace:"namespace2"`),
				ShareSplit:        ShareWeighted,
				ShareIdle:         ShareWeighted,
				SharedHourlyCosts: map[string]float64{"total": sharedOverheadHourlyCost},
			},
			numResults: 1,
			totalCost:  120.07,
			results: map[string]float64{
				"namespace2": 120.07,
			},
			windowStart: startYesterday,
			windowEnd:   endYesterday,
			expMinutes:  1440.0,
		},
		// 6j Idle by Node
		"6j": {
			start: start,
			aggBy: []string{AllocationNamespaceProp},
			aggOpts: &AllocationAggregationOptions{
				IdleByNode:                            true,
				IncludeProportionalAssetResourceCosts: true,
			},
			numResults: numNamespaces + numIdle,
			totalCost:  activeTotalCost + idleTotalCost,
			results: map[string]float64{
				"namespace1": 28.00,
				"namespace2": 36.00,
				"namespace3": 18.00,
				IdleSuffix:   30.00,
			},
			windowStart: startYesterday,
			windowEnd:   endYesterday,
			expMinutes:  1440.0,
			expectedParcResults: map[string]ProportionalAssetResourceCosts{
				"namespace1": {
					"cluster1,c1nodes": ProportionalAssetResourceCost{
						Cluster:             "cluster1",
						Name:                "c1nodes",
						Type:                "Node",
						ProviderID:          "c1nodes",
						GPUProportionalCost: 3,
						CPUProportionalCost: 3,
						RAMProportionalCost: 13,
					},
					"cluster2,node2": ProportionalAssetResourceCost{
						Cluster:             "cluster2",
						Name:                "node2",
						Type:                "Node",
						ProviderID:          "node2",
						GPUProportionalCost: 3,
						CPUProportionalCost: 3,
						RAMProportionalCost: 3,
					},
					"cluster1,pv-a1111": {
						Cluster:            "cluster1",
						Name:               "pv-a1111",
						Type:               "PV",
						PVProportionalCost: 1,
					},
					"cluster1,pv-a11abc2": {
						Cluster:            "cluster1",
						Name:               "pv-a11abc2",
						Type:               "PV",
						PVProportionalCost: 1,
					},
					"cluster1,pv-a11def3": {
						Cluster:            "cluster1",
						Name:               "pv-a11def3",
						Type:               "PV",
						PVProportionalCost: 1,
					},
				},
				"namespace2": {
					"cluster1,c1nodes": ProportionalAssetResourceCost{
						Cluster:             "cluster1",
						Name:                "c1nodes",
						Type:                "Node",
						ProviderID:          "c1nodes",
						GPUProportionalCost: 3,
						CPUProportionalCost: 3,
						RAMProportionalCost: 3,
					},
					"cluster2,node1": ProportionalAssetResourceCost{
						Cluster:             "cluster2",
						Name:                "node1",
						Type:                "Node",
						ProviderID:          "node1",
						GPUProportionalCost: 2,
						CPUProportionalCost: 2,
						RAMProportionalCost: 2,
					},
					"cluster2,node2": ProportionalAssetResourceCost{
						Cluster:             "cluster2",
						Name:                "node2",
						Type:                "Node",
						ProviderID:          "node2",
						GPUProportionalCost: 1,
						CPUProportionalCost: 1,
						RAMProportionalCost: 1,
					},
					"cluster1,pv-a12ghi4": {
						Cluster:            "cluster1",
						Name:               "pv-a12ghi4",
						Type:               "PV",
						PVProportionalCost: 1,
					},
					"cluster1,pv-a12ghi5": {
						Cluster:            "cluster1",
						Name:               "pv-a12ghi5",
						Type:               "PV",
						PVProportionalCost: 1,
					},
					"cluster1,pv-a12jkl6": {
						Cluster:            "cluster1",
						Name:               "pv-a12jkl6",
						Type:               "PV",
						PVProportionalCost: 1,
					},
					"cluster2,pv-a22mno4": {
						Cluster:            "cluster2",
						Name:               "pv-a22mno4",
						Type:               "PV",
						PVProportionalCost: 1,
					},
					"cluster2,pv-a22mno5": {
						Cluster:            "cluster2",
						Name:               "pv-a22mno5",
						Type:               "PV",
						PVProportionalCost: 1,
					},
					"cluster2,pv-a22pqr6": {
						Cluster:            "cluster2",
						Name:               "pv-a22pqr6",
						Type:               "PV",
						PVProportionalCost: 1,
					},
				},
				"namespace3": {
					"cluster2,node3": ProportionalAssetResourceCost{
						Cluster:             "cluster2",
						Name:                "node3",
						Type:                "Node",
						ProviderID:          "node3",
						GPUProportionalCost: 2,
						CPUProportionalCost: 2,
						RAMProportionalCost: 2,
					},
					"cluster2,node2": ProportionalAssetResourceCost{
						Cluster:             "cluster2",
						Name:                "node2",
						Type:                "Node",
						ProviderID:          "node2",
						GPUProportionalCost: 1,
						CPUProportionalCost: 1,
						RAMProportionalCost: 1,
					},
					"cluster2,pv-a23stu7": {
						Cluster:            "cluster2",
						Name:               "pv-a23stu7",
						Type:               "PV",
						PVProportionalCost: 1,
					},
					"cluster2,pv-a23vwx8": {
						Cluster:            "cluster2",
						Name:               "pv-a23vwx8",
						Type:               "PV",
						PVProportionalCost: 1,
					},
					"cluster2,pv-a23vwx9": {
						Cluster:            "cluster2",
						Name:               "pv-a23vwx9",
						Type:               "PV",
						PVProportionalCost: 1,
					},
				},
			},
		},
		// 6k Split Idle, Idle by Node
		"6k": {
			start: start,
			aggBy: []string{AllocationNamespaceProp},
			aggOpts: &AllocationAggregationOptions{
				SplitIdle:  true,
				IdleByNode: true,
			},
			numResults: numNamespaces + numSplitIdleNode,
			totalCost:  activeTotalCost + idleTotalCost,
			results: map[string]float64{
				"namespace1":                          28.00,
				"namespace2":                          36.00,
				"namespace3":                          18.00,
				fmt.Sprintf("c1nodes/%s", IdleSuffix): 20.00,
				fmt.Sprintf("node1/%s", IdleSuffix):   3.333333,
				fmt.Sprintf("node2/%s", IdleSuffix):   3.333333,
				fmt.Sprintf("node3/%s", IdleSuffix):   3.333333,
			},
			windowStart: startYesterday,
			windowEnd:   endYesterday,
			expMinutes:  1440.0,
		},

		// Old 6k Share idle Even Idle by Node (share idle even deprecated)

		// 6l Share idle weighted with filters, Idle by Node
		// Should match values from unfiltered aggregation (3a)
		// namespace2: 46.3125 = 36.00 + 5.0*(3.0/6.0) + 15.0*(3.0/16.0) + 5.0*(3.0/6.0) + 5.0*(3.0/6.0)
		"6l": {
			start: start,
			aggBy: []string{AllocationNamespaceProp},
			aggOpts: &AllocationAggregationOptions{
				Filter:     mustParseFilter(`namespace:"namespace2"`),
				ShareIdle:  ShareWeighted,
				IdleByNode: true,
			},
			numResults: 1,
			totalCost:  46.31,
			results: map[string]float64{
				"namespace2": 46.31,
			},
			windowStart: startYesterday,
			windowEnd:   endYesterday,
			expMinutes:  1440.0,
		},
		// 7  Edge cases and errors

		// 7a Empty AggregationProperties
		// 7b Filter all
		// 7c Share all
		// 7d Share and filter the same allocations
	}

	for name, testcase := range cases {
		if name != "4a" {
			continue
		}
		t.Run(name, func(t *testing.T) {
			if testcase.aggOpts != nil && testcase.aggOpts.IdleByNode {
				as = GenerateMockAllocationSetNodeIdle(testcase.start)
			} else {
				as = GenerateMockAllocationSetClusterIdle(testcase.start)
			}
			err = as.AggregateBy(testcase.aggBy, testcase.aggOpts)

			log.Infof("RESULTS")
			for name, alloc := range as.Allocations {
				log.Infof("  %s = %f", name, alloc.TotalCost())
			}

			assertAllocationSetTotals(t, as, name, err, testcase.numResults, testcase.totalCost)
			assertAllocationTotals(t, as, name, testcase.results)
			assertParcResults(t, as, name, testcase.expectedParcResults)
			assertAllocationWindow(t, as, name, testcase.windowStart, testcase.windowEnd, testcase.expMinutes)
		})
	}
}

func TestAllocationSet_AggregateBy_SharedCostBreakdown(t *testing.T) {
	// Set generated by GenerateMockAllocationSet
	// | Hierarchy                              | Cost |  CPU |  RAM |  GPU |   PV |  Net |  LB  |
	// +----------------------------------------+------+------+------+------+------+------+------+
	//   cluster1:
	//     idle:                                  20.00   5.00  15.00   0.00   0.00   0.00   0.00
	//     namespace1:
	//       pod1:
	//         container1: [app1, env1]   16.00   1.00  11.00   1.00   1.00   1.00   1.00
	//       pod-abc: (deployment1)
	//         container2:                         6.00   1.00   1.00   1.00   1.00   1.00   1.00
	//       pod-def: (deployment1)
	//         container3:                         6.00   1.00   1.00   1.00   1.00   1.00   1.00
	//     namespace2:
	//       pod-ghi: (deployment2)
	//         container4: [app2, env2]    6.00   1.00   1.00   1.00   1.00   1.00   1.00
	//         container5: [app2, env2]    6.00   1.00   1.00   1.00   1.00   1.00   1.00
	//       pod-jkl: (daemonset1)
	//         container6: {service1}              6.00   1.00   1.00   1.00   1.00   1.00   1.00
	// +-----------------------------------------+------+------+------+------+------+------+------+
	//   cluster1 subtotal                        66.00  11.00  31.00   6.00   6.00   6.00   6.00
	// +-----------------------------------------+------+------+------+------+------+------+------+
	//   cluster2:
	//     idle:                                  10.00   5.00   5.00   0.00   0.00   0.00   0.00
	//     namespace2:
	//       pod-mno: (deployment2)
	//         container4: [app2]              6.00   1.00   1.00   1.00   1.00   1.00   1.00
	//         container5: [app2]              6.00   1.00   1.00   1.00   1.00   1.00   1.00
	//       pod-pqr: (daemonset1)
	//         container6: {service1}              6.00   1.00   1.00   1.00   1.00   1.00   1.00
	//     namespace3:
	//       pod-stu: (deployment3)
	//         container7: an[team1]          6.00   1.00   1.00   1.00   1.00   1.00   1.00
	//       pod-vwx: (statefulset1)
	//         container8: an[team2]          6.00   1.00   1.00   1.00   1.00   1.00   1.00
	//         container9: an[team1]          6.00   1.00   1.00   1.00   1.00   1.00   1.00
	// +----------------------------------------+------+------+------+------+------+------+------+
	//   cluster2 subtotal                        46.00  11.00  11.00   6.00   6.00   6.00   6.00
	// +----------------------------------------+------+------+------+------+------+------+------+
	//   total                                   112.00  22.00  42.00  12.00  12.00  12.00  12.00
	// +----------------------------------------+------+------+------+------+------+------+------+
	end := time.Now().UTC().Truncate(day)
	start := end.Add(-day)

	// This is ugly, but required because cannot import filterutil due to import cycle
	namespaceEquals := func(ns string) *ast.EqualOp {
		return &ast.EqualOp{
			Left: ast.Identifier{
				Field: ast.NewField(allocation.FieldNamespace),
				Key:   "",
			},
			Right: ns,
		}
	}

	cases := map[string]struct {
		start   time.Time
		aggBy   []string
		aggOpts *AllocationAggregationOptions
	}{
		"agg cluster, flat shared cost": {
			start: start,
			aggBy: []string{"cluster"},
			aggOpts: &AllocationAggregationOptions{
				SharedHourlyCosts:          map[string]float64{"share_hourly": 10.0 / timeutil.HoursPerDay},
				IncludeSharedCostBreakdown: true,
			},
		},
		"agg namespace, shared namespace: namespace1": {
			start: start,
			aggBy: []string{"namespace"},
			aggOpts: &AllocationAggregationOptions{
				Share:                      namespaceEquals("namespace1"),
				IncludeSharedCostBreakdown: true,
			},
		},
		"agg namespace, shared namespace: namespace3": {
			start: start,
			aggBy: []string{"namespace"},
			aggOpts: &AllocationAggregationOptions{
				Share:                      namespaceEquals("namespace3"),
				IncludeSharedCostBreakdown: true,
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			as := GenerateMockAllocationSetClusterIdle(tc.start)
			err := as.AggregateBy(tc.aggBy, tc.aggOpts)
			if err != nil {
				t.Fatalf("error aggregating: %s", err)
			}
			for _, alloc := range as.Allocations {
				var breakdownTotal float64
				// ignore idle since it should never have shared costs
				if strings.Contains(alloc.Name, IdleSuffix) {
					continue
				}
				for _, sharedAlloc := range alloc.SharedCostBreakdown {
					breakdownTotal += sharedAlloc.TotalCost
					totalInternal := sharedAlloc.CPUCost + sharedAlloc.GPUCost + sharedAlloc.RAMCost + sharedAlloc.NetworkCost + sharedAlloc.LBCost + sharedAlloc.PVCost + sharedAlloc.ExternalCost
					// check that the total cost of a single item in the breakdown equals the sum of its parts
					// we can ignore the overheadCost breakdown since it only has a total
					if totalInternal != sharedAlloc.TotalCost && sharedAlloc.Name != "overheadCost" {
						t.Errorf("expected internal total: %f; got %f", sharedAlloc.TotalCost, totalInternal)
					}
				}
				// check that the totals of all shared cost breakdowns equal the allocation's SharedCost
				if breakdownTotal != alloc.SharedCost {
					t.Errorf("expected breakdown total: %f; got %f", alloc.SharedCost, breakdownTotal)
				}
			}
		})
	}
}

// TODO niko/etl
//func TestAllocationSet_Clone(t *testing.T) {}

// TODO niko/etl
//func TestAllocationSet_Delete(t *testing.T) {}

// TODO niko/etl
//func TestAllocationSet_End(t *testing.T) {}

// TODO niko/etl
//func TestAllocationSet_IdleAllocations(t *testing.T) {}

// TODO niko/etl
//func TestAllocationSet_Insert(t *testing.T) {}

// Asserts that all Allocations within an AllocationSet have a Window that
// matches that of the AllocationSet.
func TestAllocationSet_insertMatchingWindow(t *testing.T) {
	setStart := time.Now().Round(time.Hour)
	setEnd := setStart.Add(1 * time.Hour)

	a1WindowStart := setStart.Add(5 * time.Minute)
	a1WindowEnd := setStart.Add(50 * time.Minute)

	a2WindowStart := setStart.Add(17 * time.Minute)
	a2WindowEnd := setStart.Add(34 * time.Minute)

	a1 := &Allocation{
		Name:   "allocation-1",
		Window: Window(NewClosedWindow(a1WindowStart, a1WindowEnd)),
	}

	a2 := &Allocation{
		Name:   "allocation-2",
		Window: Window(NewClosedWindow(a2WindowStart, a2WindowEnd)),
	}

	as := NewAllocationSet(setStart, setEnd)
	as.Insert(a1)
	as.Insert(a2)

	if as.Length() != 2 {
		t.Errorf("AS length got %d, expected %d", as.Length(), 2)
	}

	for _, a := range as.Allocations {
		if !(*a.Window.Start()).Equal(setStart) {
			t.Errorf("Allocation %s window start is %s, expected %s", a.Name, *a.Window.Start(), setStart)
		}
		if !(*a.Window.End()).Equal(setEnd) {
			t.Errorf("Allocation %s window end is %s, expected %s", a.Name, *a.Window.End(), setEnd)
		}
	}
}

// This tests PARC accumulation. Assuming Node cost is $1 per core per hour
// From https://github.com/opencost/opencost/pull/1867#discussion_r1174109388:
// Over the span of hour 1:

//     Pod 1 runs for 30 minutes, consuming 1 CPU while alive. PARC: 12.5% (0.5 core-hours / 4 available core-hours)
//     Pod 2 runs for 1 hour, consuming 2 CPU while alive. PARC: 50% (2 core-hours)
//     Pod 3 runs for 1 hour, consuming 1 CPU while alive. PARC: 25% (1 core-hour)

// Over the span of hour 2:

//     Pod 1 does not run. PARC: 0% (0 core-hours / 4 available core-hours)
//     Pod 2 runs for 30 minutes, consuming 2 CPU while active. PARC: 25% (1 core-hour)
//     Pod 3 runs for 1 hour, consuming 1 CPU while active. PARC: 25% (1 core-hour)

// Over the span of hour 3:

//     Pod 1 does not run. PARC: 0% (0 core-hours / 4 available)
//     Pod 2 runs for 30 minutes, consuming 3 CPU while active. PARC: 37.5% (1.5 core-hours)
//     Pod 3 runs for 1 hour, consuming 1 CPU while active. PARC: 25% (1 core-hour)

// We expect the following accumulated PARC:

//     Pod 1: (0.5 + 0 + 0) core-hours used / (4 + 4 + 4) core-hours available = 0.5/12 = 4.16%
//     Pod 2: (2 + 1 + 1.5) / (4 + 4 + 4) = 4.5/12 = 37.5%
//     Pod 3: (1 + 1 + 1) / (4 + 4 + 4) = 3/12 = 25%

func TestParcInsert(t *testing.T) {
	pod1_hour1 := ProportionalAssetResourceCost{
		Cluster:                    "cluster1",
		Name:                       "node1",
		Type:                       "Node",
		ProviderID:                 "i-1234",
		CPUPercentage:              0.125,
		GPUPercentage:              0,
		RAMPercentage:              0,
		NodeResourceCostPercentage: 0,
		CPUTotalCost:               4,
		CPUProportionalCost:        0.5,
	}

	pod1_hour2 := ProportionalAssetResourceCost{
		Cluster:                    "cluster1",
		Name:                       "node1",
		Type:                       "Node",
		ProviderID:                 "i-1234",
		CPUPercentage:              0.0,
		GPUPercentage:              0,
		RAMPercentage:              0,
		NodeResourceCostPercentage: 0,
		CPUTotalCost:               4,
	}

	pod1_hour3 := ProportionalAssetResourceCost{
		Cluster:                    "cluster1",
		Name:                       "node1",
		Type:                       "Node",
		ProviderID:                 "i-1234",
		CPUPercentage:              0.0,
		GPUPercentage:              0,
		RAMPercentage:              0,
		NodeResourceCostPercentage: 0,
		CPUTotalCost:               4,
	}

	pod2_hour1 := ProportionalAssetResourceCost{
		Cluster:                    "cluster1",
		Name:                       "node2",
		Type:                       "Node",
		ProviderID:                 "i-1234",
		CPUPercentage:              0.0,
		GPUPercentage:              0,
		RAMPercentage:              0,
		NodeResourceCostPercentage: 0,
		CPUTotalCost:               4,
		CPUProportionalCost:        2,
	}

	pod2_hour2 := ProportionalAssetResourceCost{
		Cluster:                    "cluster1",
		Name:                       "node2",
		Type:                       "Node",
		ProviderID:                 "i-1234",
		CPUPercentage:              0.0,
		GPUPercentage:              0,
		RAMPercentage:              0,
		NodeResourceCostPercentage: 0,
		CPUTotalCost:               4,
		CPUProportionalCost:        1,
	}

	pod2_hour3 := ProportionalAssetResourceCost{
		Cluster:                    "cluster1",
		Name:                       "node2",
		Type:                       "Node",
		ProviderID:                 "i-1234",
		CPUPercentage:              0.0,
		GPUPercentage:              0,
		RAMPercentage:              0,
		NodeResourceCostPercentage: 0,
		CPUTotalCost:               4,
		CPUProportionalCost:        1.5,
	}

	pod3_hour1 := ProportionalAssetResourceCost{
		Cluster:                    "cluster1",
		Name:                       "node3",
		Type:                       "Node",
		ProviderID:                 "i-1234",
		CPUPercentage:              0.0,
		GPUPercentage:              0,
		RAMPercentage:              0,
		NodeResourceCostPercentage: 0,
		CPUTotalCost:               4,
		CPUProportionalCost:        1,
	}

	pod3_hour2 := ProportionalAssetResourceCost{
		Cluster:                    "cluster1",
		Name:                       "node3",
		Type:                       "Node",
		ProviderID:                 "i-1234",
		CPUPercentage:              0.0,
		GPUPercentage:              0,
		RAMPercentage:              0,
		NodeResourceCostPercentage: 0,
		CPUTotalCost:               4,
		CPUProportionalCost:        1,
	}

	pod3_hour3 := ProportionalAssetResourceCost{
		Cluster:                    "cluster1",
		Name:                       "node3",
		Type:                       "Node",
		ProviderID:                 "i-1234",
		CPUPercentage:              0.0,
		GPUPercentage:              0,
		RAMPercentage:              0,
		NodeResourceCostPercentage: 0,
		CPUTotalCost:               4,
		CPUProportionalCost:        1,
	}

	parcs := ProportionalAssetResourceCosts{}
	parcs.Insert(pod1_hour1, true)
	parcs.Insert(pod1_hour2, true)
	parcs.Insert(pod1_hour3, true)
	parcs.Insert(pod2_hour1, true)
	parcs.Insert(pod2_hour2, true)
	parcs.Insert(pod2_hour3, true)
	parcs.Insert(pod3_hour1, true)
	parcs.Insert(pod3_hour2, true)
	parcs.Insert(pod3_hour3, true)
	log.Debug("added all parcs")

	// set totals, compute percentaves
	parc1, ok := parcs["cluster1,node1"]
	if !ok {
		t.Fatalf("parc1 not found")
	}
	parc1.CPUTotalCost = 12

	parc2, ok := parcs["cluster1,node2"]
	if !ok {
		t.Fatalf("parc2 not found")
	}
	parc2.CPUTotalCost = 12

	parc3, ok := parcs["cluster1,node3"]
	if !ok {
		t.Fatalf("parc1 not found")
	}
	parc3.CPUTotalCost = 12

	ComputePercentages(&parc1)
	ComputePercentages(&parc2)
	ComputePercentages(&parc3)
	parcs["cluster1,node1"] = parc1
	parcs["cluster1,node2"] = parc2
	parcs["cluster1,node3"] = parc3

	expectedParcs := ProportionalAssetResourceCosts{
		"cluster1,node1": ProportionalAssetResourceCost{
			CPUPercentage:              0.041666666666666664,
			NodeResourceCostPercentage: 0.041666666666666664,
		},
		"cluster1,node2": ProportionalAssetResourceCost{
			CPUPercentage:              0.375,
			NodeResourceCostPercentage: 0.375,
		},
		"cluster1,node3": ProportionalAssetResourceCost{
			CPUPercentage:              0.25,
			NodeResourceCostPercentage: 0.25,
		},
	}

	for key, expectedParc := range expectedParcs {
		actualParc, ok := parcs[key]
		if !ok {
			t.Fatalf("did not find expected PARC: %s", key)
		}

		if actualParc.CPUPercentage != expectedParc.CPUPercentage {
			t.Fatalf("actual parc cpu percentage: %f did not match expected: %f", actualParc.CPUPercentage, expectedParc.CPUPercentage)
		}
		if actualParc.NodeResourceCostPercentage != expectedParc.NodeResourceCostPercentage {
			t.Fatalf("actual parc node percentage: %f did not match expected: %f", actualParc.NodeResourceCostPercentage, expectedParc.NodeResourceCostPercentage)
		}
	}
}

// TODO niko/etl
//func TestAllocationSet_IsEmpty(t *testing.T) {}

// TODO niko/etl
//func TestAllocationSet_Length(t *testing.T) {}

// TODO niko/etl
//func TestAllocationSet_Map(t *testing.T) {}

// TODO niko/etl
//func TestAllocationSet_MarshalJSON(t *testing.T) {}

// TODO niko/etl
//func TestAllocationSet_Resolution(t *testing.T) {}

// TODO niko/etl
//func TestAllocationSet_Seconds(t *testing.T) {}

// TODO niko/etl
//func TestAllocationSet_Set(t *testing.T) {}

// TODO niko/etl
//func TestAllocationSet_Start(t *testing.T) {}

// TODO niko/etl
//func TestAllocationSet_TotalCost(t *testing.T) {}

// TODO niko/etl
//func TestNewAllocationSetRange(t *testing.T) {}

func TestAllocationSetRange_AccumulateRepeat(t *testing.T) {
	ago2d := time.Now().UTC().Truncate(day).Add(-2 * day)
	yesterday := time.Now().UTC().Truncate(day).Add(-day)
	today := time.Now().UTC().Truncate(day)
	tomorrow := time.Now().UTC().Truncate(day).Add(day)

	a := GenerateMockAllocationSet(ago2d)
	b := GenerateMockAllocationSet(yesterday)
	c := GenerateMockAllocationSet(today)
	d := GenerateMockAllocationSet(tomorrow)

	asr := NewAllocationSetRange(a, b, c, d)

	// Take Total Cost
	totalCost := asr.TotalCost()

	// NewAccumulation does not mutate
	result, err := asr.newAccumulation()
	if err != nil {
		t.Fatal(err)
	}
	asr2 := NewAllocationSetRange(result)

	// Ensure Costs Match
	if totalCost != asr2.TotalCost() {
		t.Fatalf("Accumulated Total Cost does not match original Total Cost")
	}

	// Next NewAccumulation() call should prove that there is no mutation of inner data
	result, err = asr.newAccumulation()
	if err != nil {
		t.Fatal(err)
	}
	asr3 := NewAllocationSetRange(result)

	// Costs should be correct, as multiple calls to NewAccumulation() should not alter
	// the internals of the AllocationSetRange
	if totalCost != asr3.TotalCost() {
		t.Fatalf("Accumulated Total Cost does not match original Total Cost. %f != %f", totalCost, asr3.TotalCost())
	}
}

func TestAllocationSetRange_Accumulate(t *testing.T) {
	ago2d := time.Now().UTC().Truncate(day).Add(-2 * day)
	yesterday := time.Now().UTC().Truncate(day).Add(-day)
	today := time.Now().UTC().Truncate(day)
	tomorrow := time.Now().UTC().Truncate(day).Add(day)

	// Accumulating any combination of nil and/or empty set should result in empty set
	result, err := NewAllocationSetRange(nil).accumulate()
	if err != nil {
		t.Fatalf("unexpected error accumulating nil AllocationSetRange: %s", err)
	}
	if !result.IsEmpty() {
		t.Fatalf("accumulating nil AllocationSetRange: expected empty; actual %s", result)
	}

	result, err = NewAllocationSetRange(nil, nil).accumulate()
	if err != nil {
		t.Fatalf("unexpected error accumulating nil AllocationSetRange: %s", err)
	}
	if !result.IsEmpty() {
		t.Fatalf("accumulating nil AllocationSetRange: expected empty; actual %s", result)
	}

	result, err = NewAllocationSetRange(NewAllocationSet(yesterday, today)).accumulate()
	if err != nil {
		t.Fatalf("unexpected error accumulating nil AllocationSetRange: %s", err)
	}
	if !result.IsEmpty() {
		t.Fatalf("accumulating nil AllocationSetRange: expected empty; actual %s", result)
	}

	result, err = NewAllocationSetRange(nil, NewAllocationSet(ago2d, yesterday), nil, NewAllocationSet(today, tomorrow), nil).accumulate()
	if err != nil {
		t.Fatalf("unexpected error accumulating nil AllocationSetRange: %s", err)
	}
	if !result.IsEmpty() {
		t.Fatalf("accumulating nil AllocationSetRange: expected empty; actual %s", result)
	}

	todayAS := NewAllocationSet(today, tomorrow)
	todayAS.Set(NewMockUnitAllocation("", today, day, nil))

	yesterdayAS := NewAllocationSet(yesterday, today)
	yesterdayAS.Set(NewMockUnitAllocation("", yesterday, day, nil))

	// Accumulate non-nil with nil should result in copy of non-nil, regardless of order
	result, err = NewAllocationSetRange(nil, todayAS).accumulate()
	if err != nil {
		t.Fatalf("unexpected error accumulating AllocationSetRange of length 1: %s", err)
	}
	if result == nil {
		t.Fatalf("accumulating AllocationSetRange: expected AllocationSet; actual %s", result)
	}
	if result.TotalCost() != 6.0 {
		t.Fatalf("accumulating AllocationSetRange: expected total cost 6.0; actual %f", result.TotalCost())
	}

	result, err = NewAllocationSetRange(todayAS, nil).accumulate()
	if err != nil {
		t.Fatalf("unexpected error accumulating AllocationSetRange of length 1: %s", err)
	}
	if result == nil {
		t.Fatalf("accumulating AllocationSetRange: expected AllocationSet; actual %s", result)
	}
	if result.TotalCost() != 6.0 {
		t.Fatalf("accumulating AllocationSetRange: expected total cost 6.0; actual %f", result.TotalCost())
	}

	result, err = NewAllocationSetRange(nil, todayAS, nil).accumulate()
	if err != nil {
		t.Fatalf("unexpected error accumulating AllocationSetRange of length 1: %s", err)
	}
	if result == nil {
		t.Fatalf("accumulating AllocationSetRange: expected AllocationSet; actual %s", result)
	}
	if result.TotalCost() != 6.0 {
		t.Fatalf("accumulating AllocationSetRange: expected total cost 6.0; actual %f", result.TotalCost())
	}

	// Accumulate two non-nil should result in sum of both with appropriate start, end
	result, err = NewAllocationSetRange(yesterdayAS, todayAS).accumulate()
	if err != nil {
		t.Fatalf("unexpected error accumulating AllocationSetRange of length 1: %s", err)
	}
	if result == nil {
		t.Fatalf("accumulating AllocationSetRange: expected AllocationSet; actual %s", result)
	}
	if result.TotalCost() != 12.0 {
		t.Fatalf("accumulating AllocationSetRange: expected total cost 12.0; actual %f", result.TotalCost())
	}
	allocMap := result.Allocations
	if len(allocMap) != 1 {
		t.Fatalf("accumulating AllocationSetRange: expected length 1; actual length %d", len(allocMap))
	}
	alloc := allocMap["cluster1/namespace1/pod1/container1"]
	if alloc == nil {
		t.Fatalf("accumulating AllocationSetRange: expected allocation 'cluster1/namespace1/pod1/container1'")
	}
	if alloc.CPUCoreHours != 2.0 {
		t.Fatalf("accumulating AllocationSetRange: expected 2.0; actual %f", result.TotalCost())
	}
	if alloc.CPUCost != 2.0 {
		t.Fatalf("accumulating AllocationSetRange: expected 2.0; actual %f", alloc.CPUCost)
	}
	if alloc.CPUEfficiency() != 1.0 {
		t.Fatalf("accumulating AllocationSetRange: expected 1.0; actual %f", alloc.CPUEfficiency())
	}
	if alloc.GPUHours != 2.0 {
		t.Fatalf("accumulating AllocationSetRange: expected 2.0; actual %f", alloc.GPUHours)
	}
	if alloc.GPUCost != 2.0 {
		t.Fatalf("accumulating AllocationSetRange: expected 2.0; actual %f", alloc.GPUCost)
	}
	if alloc.NetworkCost != 2.0 {
		t.Fatalf("accumulating AllocationSetRange: expected 2.0; actual %f", alloc.NetworkCost)
	}
	if alloc.LoadBalancerCost != 2.0 {
		t.Fatalf("accumulating AllocationSetRange: expected 2.0; actual %f", alloc.LoadBalancerCost)
	}
	if alloc.PVByteHours() != 2.0 {
		t.Fatalf("accumulating AllocationSetRange: expected 2.0; actual %f", alloc.PVByteHours())
	}
	if alloc.PVCost() != 2.0 {
		t.Fatalf("accumulating AllocationSetRange: expected 2.0; actual %f", alloc.PVCost())
	}
	if alloc.RAMByteHours != 2.0 {
		t.Fatalf("accumulating AllocationSetRange: expected 2.0; actual %f", alloc.RAMByteHours)
	}
	if alloc.RAMCost != 2.0 {
		t.Fatalf("accumulating AllocationSetRange: expected 2.0; actual %f", alloc.RAMCost)
	}
	if alloc.RAMEfficiency() != 1.0 {
		t.Fatalf("accumulating AllocationSetRange: expected 1.0; actual %f", alloc.RAMEfficiency())
	}
	if alloc.TotalCost() != 12.0 {
		t.Fatalf("accumulating AllocationSetRange: expected 12.0; actual %f", alloc.TotalCost())
	}
	if alloc.TotalEfficiency() != 1.0 {
		t.Fatalf("accumulating AllocationSetRange: expected 1.0; actual %f", alloc.TotalEfficiency())
	}
	if !alloc.Start.Equal(yesterday) {
		t.Fatalf("accumulating AllocationSetRange: expected to start %s; actual %s", yesterday, alloc.Start)
	}
	if !alloc.End.Equal(tomorrow) {
		t.Fatalf("accumulating AllocationSetRange: expected to end %s; actual %s", tomorrow, alloc.End)
	}
	if alloc.Minutes() != 2880.0 {
		t.Fatalf("accumulating AllocationSetRange: expected %f minutes; actual %f", 2880.0, alloc.Minutes())
	}
}

func TestAllocationSetRange_AccumulateBy_None(t *testing.T) {
	ago4d := time.Now().UTC().Truncate(day).Add(-4 * day)
	ago3d := time.Now().UTC().Truncate(day).Add(-3 * day)
	ago2d := time.Now().UTC().Truncate(day).Add(-2 * day)
	yesterday := time.Now().UTC().Truncate(day).Add(-day)
	today := time.Now().UTC().Truncate(day)
	tomorrow := time.Now().UTC().Truncate(day).Add(day)

	ago4dAS := NewAllocationSet(ago4d, ago3d)
	ago4dAS.Set(NewMockUnitAllocation("4", ago4d, day, nil))
	ago3dAS := NewAllocationSet(ago3d, ago2d)
	ago3dAS.Set(NewMockUnitAllocation("a", ago3d, day, nil))
	ago2dAS := NewAllocationSet(ago2d, yesterday)
	ago2dAS.Set(NewMockUnitAllocation("", ago2d, day, nil))
	yesterdayAS := NewAllocationSet(yesterday, today)
	yesterdayAS.Set(NewMockUnitAllocation("", yesterday, day, nil))
	todayAS := NewAllocationSet(today, tomorrow)
	todayAS.Set(NewMockUnitAllocation("", today, day, nil))

	asr := NewAllocationSetRange(ago4dAS, ago3dAS, ago2dAS, yesterdayAS, todayAS)
	asr, err := asr.Accumulate(AccumulateOptionNone)
	if err != nil {
		t.Fatalf("unexpected error calling accumulateBy: %s", err)
	}

	if len(asr.Allocations) != 5 {
		t.Fatalf("expected 5 allocation sets, got:%d", len(asr.Allocations))
	}
}

func TestAllocationSetRange_AccumulateBy_All(t *testing.T) {
	ago4d := time.Now().UTC().Truncate(day).Add(-4 * day)
	ago3d := time.Now().UTC().Truncate(day).Add(-3 * day)
	ago2d := time.Now().UTC().Truncate(day).Add(-2 * day)
	yesterday := time.Now().UTC().Truncate(day).Add(-day)
	today := time.Now().UTC().Truncate(day)
	tomorrow := time.Now().UTC().Truncate(day).Add(day)

	ago4dAS := NewAllocationSet(ago4d, ago3d)
	ago4dAS.Set(NewMockUnitAllocation("4", ago4d, day, nil))
	ago3dAS := NewAllocationSet(ago3d, ago2d)
	ago3dAS.Set(NewMockUnitAllocation("a", ago3d, day, nil))
	ago2dAS := NewAllocationSet(ago2d, yesterday)
	ago2dAS.Set(NewMockUnitAllocation("", ago2d, day, nil))
	yesterdayAS := NewAllocationSet(yesterday, today)
	yesterdayAS.Set(NewMockUnitAllocation("", yesterday, day, nil))
	todayAS := NewAllocationSet(today, tomorrow)
	todayAS.Set(NewMockUnitAllocation("", today, day, nil))

	asr := NewAllocationSetRange(ago4dAS, ago3dAS, ago2dAS, yesterdayAS, todayAS)
	asr, err := asr.Accumulate(AccumulateOptionAll)
	if err != nil {
		t.Fatalf("unexpected error calling accumulateBy: %s", err)
	}

	if len(asr.Allocations) != 1 {
		t.Fatalf("expected 1 allocation set, got:%d", len(asr.Allocations))
	}

	allocMap := asr.Allocations[0].Allocations
	alloc := allocMap["cluster1/namespace1/pod1/container1"]
	if alloc.Minutes() != 4320.0 {
		t.Errorf("accumulating AllocationSetRange: expected %f minutes; actual %f", 4320.0, alloc.Minutes())
	}
}

func TestAllocationSetRange_AccumulateBy_Hour(t *testing.T) {
	ago4h := time.Now().UTC().Truncate(time.Hour).Add(-4 * time.Hour)
	ago3h := time.Now().UTC().Truncate(time.Hour).Add(-3 * time.Hour)
	ago2h := time.Now().UTC().Truncate(time.Hour).Add(-2 * time.Hour)
	ago1h := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour)
	currentHour := time.Now().UTC().Truncate(time.Hour)
	nextHour := time.Now().UTC().Truncate(time.Hour).Add(time.Hour)

	ago4hAS := NewAllocationSet(ago4h, ago3h)
	ago4hAS.Set(NewMockUnitAllocation("4", ago4h, time.Hour, nil))
	ago3hAS := NewAllocationSet(ago3h, ago2h)
	ago3hAS.Set(NewMockUnitAllocation("a", ago3h, time.Hour, nil))
	ago2hAS := NewAllocationSet(ago2h, ago1h)
	ago2hAS.Set(NewMockUnitAllocation("", ago2h, time.Hour, nil))
	ago1hAS := NewAllocationSet(ago1h, currentHour)
	ago1hAS.Set(NewMockUnitAllocation("", ago1h, time.Hour, nil))
	currentHourAS := NewAllocationSet(currentHour, nextHour)
	currentHourAS.Set(NewMockUnitAllocation("", currentHour, time.Hour, nil))

	asr := NewAllocationSetRange(ago4hAS, ago3hAS, ago2hAS, ago1hAS, currentHourAS)
	asr, err := asr.Accumulate(AccumulateOptionHour)
	if err != nil {
		t.Fatalf("unexpected error calling accumulateBy: %s", err)
	}

	if len(asr.Allocations) != 5 {
		t.Fatalf("expected 5 allocation sets, got:%d", len(asr.Allocations))
	}

	allocMap := asr.Allocations[0].Allocations
	alloc := allocMap["4"]
	if alloc.Minutes() != 60.0 {
		t.Errorf("accumulating AllocationSetRange: expected %f minutes; actual %f", 60.0, alloc.Minutes())
	}
}

func TestAllocationSetRange_AccumulateBy_Day_From_Day(t *testing.T) {
	ago4d := time.Now().UTC().Truncate(day).Add(-4 * day)
	ago3d := time.Now().UTC().Truncate(day).Add(-3 * day)
	ago2d := time.Now().UTC().Truncate(day).Add(-2 * day)
	yesterday := time.Now().UTC().Truncate(day).Add(-day)
	today := time.Now().UTC().Truncate(day)
	tomorrow := time.Now().UTC().Truncate(day).Add(day)

	ago4dAS := NewAllocationSet(ago4d, ago3d)
	ago4dAS.Set(NewMockUnitAllocation("4", ago4d, day, nil))
	ago3dAS := NewAllocationSet(ago3d, ago2d)
	ago3dAS.Set(NewMockUnitAllocation("a", ago3d, day, nil))
	ago2dAS := NewAllocationSet(ago2d, yesterday)
	ago2dAS.Set(NewMockUnitAllocation("", ago2d, day, nil))
	yesterdayAS := NewAllocationSet(yesterday, today)
	yesterdayAS.Set(NewMockUnitAllocation("", yesterday, day, nil))
	todayAS := NewAllocationSet(today, tomorrow)
	todayAS.Set(NewMockUnitAllocation("", today, day, nil))

	asr := NewAllocationSetRange(ago4dAS, ago3dAS, ago2dAS, yesterdayAS, todayAS)
	asr, err := asr.Accumulate(AccumulateOptionDay)
	if err != nil {
		t.Fatalf("unexpected error calling accumulateBy: %s", err)
	}

	if len(asr.Allocations) != 5 {
		t.Fatalf("expected 5 allocation sets, got:%d", len(asr.Allocations))
	}
	allocMap := asr.Allocations[0].Allocations
	alloc := allocMap["4"]
	if alloc.Minutes() != 1440.0 {
		t.Errorf("accumulating AllocationSetRange: expected %f minutes; actual %f", 1440.0, alloc.Minutes())
	}
}

func TestAllocationSetRange_AccumulateBy_Day_From_Hours(t *testing.T) {
	ago4h := time.Now().UTC().Truncate(time.Hour).Add(-4 * time.Hour)
	ago3h := time.Now().UTC().Truncate(time.Hour).Add(-3 * time.Hour)
	ago2h := time.Now().UTC().Truncate(time.Hour).Add(-2 * time.Hour)
	ago1h := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour)
	currentHour := time.Now().UTC().Truncate(time.Hour)
	nextHour := time.Now().UTC().Truncate(time.Hour).Add(time.Hour)

	ago4hAS := NewAllocationSet(ago4h, ago3h)
	ago4hAS.Set(NewMockUnitAllocation("", ago4h, time.Hour, nil))
	ago3hAS := NewAllocationSet(ago3h, ago2h)
	ago3hAS.Set(NewMockUnitAllocation("", ago3h, time.Hour, nil))
	ago2hAS := NewAllocationSet(ago2h, ago1h)
	ago2hAS.Set(NewMockUnitAllocation("", ago2h, time.Hour, nil))
	ago1hAS := NewAllocationSet(ago1h, currentHour)
	ago1hAS.Set(NewMockUnitAllocation("", ago1h, time.Hour, nil))
	currentHourAS := NewAllocationSet(currentHour, nextHour)
	currentHourAS.Set(NewMockUnitAllocation("", currentHour, time.Hour, nil))

	asr := NewAllocationSetRange(ago4hAS, ago3hAS, ago2hAS, ago1hAS, currentHourAS)
	asr, err := asr.Accumulate(AccumulateOptionDay)
	if err != nil {
		t.Fatalf("unexpected error calling accumulateBy: %s", err)
	}

	if len(asr.Allocations) != 1 && len(asr.Allocations) != 2 {
		t.Fatalf("expected 1 allocation set, got:%d", len(asr.Allocations))
	}

	allocMap := asr.Allocations[0].Allocations
	alloc := allocMap["cluster1/namespace1/pod1/container1"]
	if alloc.Minutes() > 300.0 {
		t.Errorf("accumulating AllocationSetRange: expected %f or less minutes; actual %f", 300.0, alloc.Minutes())
	}
}

func TestAllocationSetRange_AccumulateBy_Week(t *testing.T) {
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
	tomorrow := time.Now().UTC().Truncate(day).Add(day)

	ago9dAS := NewAllocationSet(ago9d, ago8d)
	ago9dAS.Set(NewMockUnitAllocation("4", ago9d, day, nil))
	ago8dAS := NewAllocationSet(ago8d, ago7d)
	ago8dAS.Set(NewMockUnitAllocation("4", ago8d, day, nil))
	ago7dAS := NewAllocationSet(ago7d, ago6d)
	ago7dAS.Set(NewMockUnitAllocation("4", ago7d, day, nil))
	ago6dAS := NewAllocationSet(ago6d, ago5d)
	ago6dAS.Set(NewMockUnitAllocation("4", ago6d, day, nil))
	ago5dAS := NewAllocationSet(ago5d, ago4d)
	ago5dAS.Set(NewMockUnitAllocation("4", ago5d, day, nil))
	ago4dAS := NewAllocationSet(ago4d, ago3d)
	ago4dAS.Set(NewMockUnitAllocation("4", ago4d, day, nil))
	ago3dAS := NewAllocationSet(ago3d, ago2d)
	ago3dAS.Set(NewMockUnitAllocation("a", ago3d, day, nil))
	ago2dAS := NewAllocationSet(ago2d, yesterday)
	ago2dAS.Set(NewMockUnitAllocation("", ago2d, day, nil))
	yesterdayAS := NewAllocationSet(yesterday, today)
	yesterdayAS.Set(NewMockUnitAllocation("", yesterday, day, nil))
	todayAS := NewAllocationSet(today, tomorrow)
	todayAS.Set(NewMockUnitAllocation("", today, day, nil))

	asr := NewAllocationSetRange(ago9dAS, ago8dAS, ago7dAS, ago6dAS, ago5dAS, ago4dAS, ago3dAS, ago2dAS, yesterdayAS, todayAS)
	asr, err := asr.Accumulate(AccumulateOptionWeek)
	if err != nil {
		t.Fatalf("unexpected error calling accumulateBy: %s", err)
	}

	if len(asr.Allocations) != 2 && len(asr.Allocations) != 3 {
		t.Fatalf("expected 2 or 3 allocation sets, got:%d", len(asr.Allocations))
	}

	for _, as := range asr.Allocations {
		if as.Window.Duration() < time.Hour*24 || as.Window.Duration() > time.Hour*24*7 {
			t.Fatalf("expected window duration to be between 1 and 7 days, got:%s", as.Window.Duration().String())
		}
	}
}

func TestAllocationSetRange_AccumulateBy_Month(t *testing.T) {
	prevMonth1stDay := time.Date(2020, 01, 29, 0, 0, 0, 0, time.UTC)
	prevMonth2ndDay := time.Date(2020, 01, 30, 0, 0, 0, 0, time.UTC)
	prevMonth3ndDay := time.Date(2020, 01, 31, 0, 0, 0, 0, time.UTC)
	nextMonth1stDay := time.Date(2020, 02, 01, 0, 0, 0, 0, time.UTC)
	nextMonth2ndDay := time.Date(2020, 02, 02, 0, 0, 0, 0, time.UTC)

	prev1AS := NewAllocationSet(prevMonth1stDay, prevMonth2ndDay)
	prev1AS.Set(NewMockUnitAllocation("", prevMonth1stDay, day, nil))
	prev2AS := NewAllocationSet(prevMonth2ndDay, prevMonth3ndDay)
	prev2AS.Set(NewMockUnitAllocation("", prevMonth2ndDay, day, nil))

	prev3AS := NewAllocationSet(prevMonth3ndDay, nextMonth1stDay)
	prev3AS.Set(NewMockUnitAllocation("", prevMonth3ndDay, day, nil))

	nextAS := NewAllocationSet(nextMonth1stDay, nextMonth2ndDay)
	nextAS.Set(NewMockUnitAllocation("", nextMonth1stDay, day, nil))

	asr := NewAllocationSetRange(prev1AS, prev2AS, prev3AS, nextAS)
	asr, err := asr.Accumulate(AccumulateOptionMonth)
	if err != nil {
		t.Fatalf("unexpected error calling accumulateBy: %s", err)
	}

	if len(asr.Allocations) != 2 {
		t.Fatalf("expected 2 allocation sets, got:%d", len(asr.Allocations))
	}

	for _, as := range asr.Allocations {
		if as.Window.Duration() < time.Hour*24 || as.Window.Duration() > time.Hour*24*31 {
			t.Fatalf("expected window duration to be between 1 and 7 days, got:%s", as.Window.Duration().String())
		}
	}
}

// TODO niko/etl
// func TestAllocationSetRange_AggregateBy(t *testing.T) {}

// TODO niko/etl
// func TestAllocationSetRange_Append(t *testing.T) {}

// TODO niko/etl
// func TestAllocationSetRange_Each(t *testing.T) {}

// TODO niko/etl
// func TestAllocationSetRange_Get(t *testing.T) {}

func TestAllocationSetRange_InsertRange(t *testing.T) {
	// Set up
	ago2d := time.Now().UTC().Truncate(day).Add(-2 * day)
	yesterday := time.Now().UTC().Truncate(day).Add(-day)
	today := time.Now().UTC().Truncate(day)
	tomorrow := time.Now().UTC().Truncate(day).Add(day)

	unit := NewMockUnitAllocation("", today, day, nil)

	ago2dAS := NewAllocationSet(ago2d, yesterday)
	ago2dAS.Set(NewMockUnitAllocation("a", ago2d, day, nil))
	ago2dAS.Set(NewMockUnitAllocation("b", ago2d, day, nil))
	ago2dAS.Set(NewMockUnitAllocation("c", ago2d, day, nil))

	yesterdayAS := NewAllocationSet(yesterday, today)
	yesterdayAS.Set(NewMockUnitAllocation("a", yesterday, day, nil))
	yesterdayAS.Set(NewMockUnitAllocation("b", yesterday, day, nil))
	yesterdayAS.Set(NewMockUnitAllocation("c", yesterday, day, nil))

	todayAS := NewAllocationSet(today, tomorrow)
	todayAS.Set(NewMockUnitAllocation("a", today, day, nil))
	todayAS.Set(NewMockUnitAllocation("b", today, day, nil))
	todayAS.Set(NewMockUnitAllocation("c", today, day, nil))

	var nilASR *AllocationSetRange
	thisASR := NewAllocationSetRange(yesterdayAS.Clone(), todayAS.Clone())
	thatASR := NewAllocationSetRange(yesterdayAS.Clone())
	longASR := NewAllocationSetRange(ago2dAS.Clone(), yesterdayAS.Clone(), todayAS.Clone())
	var err error

	// Expect an error calling InsertRange on nil
	err = nilASR.InsertRange(thatASR)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}

	// Expect nothing to happen calling InsertRange(nil) on non-nil ASR
	err = thisASR.InsertRange(nil)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	for _, as := range thisASR.Allocations {
		for k, a := range as.Allocations {
			if !util.IsApproximately(a.CPUCoreHours, unit.CPUCoreHours) {
				t.Fatalf("allocation %s: expected %f; got %f", k, unit.CPUCoreHours, a.CPUCoreHours)
			}
			if !util.IsApproximately(a.CPUCost, unit.CPUCost) {
				t.Fatalf("allocation %s: expected %f; got %f", k, unit.CPUCost, a.CPUCost)
			}
			if !util.IsApproximately(a.RAMByteHours, unit.RAMByteHours) {
				t.Fatalf("allocation %s: expected %f; got %f", k, unit.RAMByteHours, a.RAMByteHours)
			}
			if !util.IsApproximately(a.RAMCost, unit.RAMCost) {
				t.Fatalf("allocation %s: expected %f; got %f", k, unit.RAMCost, a.RAMCost)
			}
			if !util.IsApproximately(a.GPUHours, unit.GPUHours) {
				t.Fatalf("allocation %s: expected %f; got %f", k, unit.GPUHours, a.GPUHours)
			}
			if !util.IsApproximately(a.GPUCost, unit.GPUCost) {
				t.Fatalf("allocation %s: expected %f; got %f", k, unit.GPUCost, a.GPUCost)
			}
			if !util.IsApproximately(a.PVByteHours(), unit.PVByteHours()) {
				t.Fatalf("allocation %s: expected %f; got %f", k, unit.PVByteHours(), a.PVByteHours())
			}
			if !util.IsApproximately(a.PVCost(), unit.PVCost()) {
				t.Fatalf("allocation %s: expected %f; got %f", k, unit.PVCost(), a.PVCost())
			}
			if !util.IsApproximately(a.NetworkCost, unit.NetworkCost) {
				t.Fatalf("allocation %s: expected %f; got %f", k, unit.NetworkCost, a.NetworkCost)
			}
			if !util.IsApproximately(a.LoadBalancerCost, unit.LoadBalancerCost) {
				t.Fatalf("allocation %s: expected %f; got %f", k, unit.LoadBalancerCost, a.LoadBalancerCost)
			}
			if !util.IsApproximately(a.TotalCost(), unit.TotalCost()) {
				t.Fatalf("allocation %s: expected %f; got %f", k, unit.TotalCost(), a.TotalCost())
			}
		}
	}

	// Expect an error calling InsertRange with a range exceeding the receiver
	err = thisASR.InsertRange(longASR)
	if err == nil {
		t.Fatalf("expected error calling InsertRange with a range exceeding the receiver")
	}

	// Expect each Allocation in "today" to stay the same, but "yesterday" to
	// precisely double when inserting a range that only has a duplicate of
	// "yesterday", but no entry for "today"
	err = thisASR.InsertRange(thatASR)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	yAS, err := thisASR.Get(0)
	for k, a := range yAS.Allocations {
		if !util.IsApproximately(a.CPUCoreHours, 2*unit.CPUCoreHours) {
			t.Fatalf("allocation %s: expected %f; got %f", k, unit.CPUCoreHours, a.CPUCoreHours)
		}
		if !util.IsApproximately(a.CPUCost, 2*unit.CPUCost) {
			t.Fatalf("allocation %s: expected %f; got %f", k, unit.CPUCost, a.CPUCost)
		}
		if !util.IsApproximately(a.RAMByteHours, 2*unit.RAMByteHours) {
			t.Fatalf("allocation %s: expected %f; got %f", k, unit.RAMByteHours, a.RAMByteHours)
		}
		if !util.IsApproximately(a.RAMCost, 2*unit.RAMCost) {
			t.Fatalf("allocation %s: expected %f; got %f", k, unit.RAMCost, a.RAMCost)
		}
		if !util.IsApproximately(a.GPUHours, 2*unit.GPUHours) {
			t.Fatalf("allocation %s: expected %f; got %f", k, unit.GPUHours, a.GPUHours)
		}
		if !util.IsApproximately(a.GPUCost, 2*unit.GPUCost) {
			t.Fatalf("allocation %s: expected %f; got %f", k, unit.GPUCost, a.GPUCost)
		}
		if !util.IsApproximately(a.PVByteHours(), 2*unit.PVByteHours()) {
			t.Fatalf("allocation %s: expected %f; got %f", k, unit.PVByteHours(), a.PVByteHours())
		}
		if !util.IsApproximately(a.PVCost(), 2*unit.PVCost()) {
			t.Fatalf("allocation %s: expected %f; got %f", k, unit.PVCost(), a.PVCost())
		}
		if !util.IsApproximately(a.NetworkCost, 2*unit.NetworkCost) {
			t.Fatalf("allocation %s: expected %f; got %f", k, unit.NetworkCost, a.NetworkCost)
		}
		if !util.IsApproximately(a.LoadBalancerCost, 2*unit.LoadBalancerCost) {
			t.Fatalf("allocation %s: expected %f; got %f", k, unit.LoadBalancerCost, a.LoadBalancerCost)
		}

		if !util.IsApproximately(a.TotalCost(), 2*unit.TotalCost()) {
			t.Fatalf("allocation %s: expected %f; got %f", k, unit.TotalCost(), a.TotalCost())
		}
	}
	tAS, err := thisASR.Get(1)
	for k, a := range tAS.Allocations {
		if !util.IsApproximately(a.CPUCoreHours, unit.CPUCoreHours) {
			t.Fatalf("allocation %s: expected %f; got %f", k, unit.CPUCoreHours, a.CPUCoreHours)
		}
		if !util.IsApproximately(a.CPUCost, unit.CPUCost) {
			t.Fatalf("allocation %s: expected %f; got %f", k, unit.CPUCost, a.CPUCost)
		}
		if !util.IsApproximately(a.RAMByteHours, unit.RAMByteHours) {
			t.Fatalf("allocation %s: expected %f; got %f", k, unit.RAMByteHours, a.RAMByteHours)
		}
		if !util.IsApproximately(a.RAMCost, unit.RAMCost) {
			t.Fatalf("allocation %s: expected %f; got %f", k, unit.RAMCost, a.RAMCost)
		}
		if !util.IsApproximately(a.GPUHours, unit.GPUHours) {
			t.Fatalf("allocation %s: expected %f; got %f", k, unit.GPUHours, a.GPUHours)
		}
		if !util.IsApproximately(a.GPUCost, unit.GPUCost) {
			t.Fatalf("allocation %s: expected %f; got %f", k, unit.GPUCost, a.GPUCost)
		}
		if !util.IsApproximately(a.PVByteHours(), unit.PVByteHours()) {
			t.Fatalf("allocation %s: expected %f; got %f", k, unit.PVByteHours(), a.PVByteHours())
		}
		if !util.IsApproximately(a.PVCost(), unit.PVCost()) {
			t.Fatalf("allocation %s: expected %f; got %f", k, unit.PVCost(), a.PVCost())
		}
		if !util.IsApproximately(a.NetworkCost, unit.NetworkCost) {
			t.Fatalf("allocation %s: expected %f; got %f", k, unit.NetworkCost, a.NetworkCost)
		}
		if !util.IsApproximately(a.LoadBalancerCost, unit.LoadBalancerCost) {
			t.Fatalf("allocation %s: expected %f; got %f", k, unit.LoadBalancerCost, a.LoadBalancerCost)
		}
		if !util.IsApproximately(a.TotalCost(), unit.TotalCost()) {
			t.Fatalf("allocation %s: expected %f; got %f", k, unit.TotalCost(), a.TotalCost())
		}
	}
}

// TODO niko/etl
// func TestAllocationSetRange_Length(t *testing.T) {}

func TestAllocationSetRange_MarshalJSON(t *testing.T) {

	tests := []struct {
		name     string
		arg      *AllocationSetRange
		expected *AllocationSetRange
	}{
		{
			name: "Nil ASR",
			arg:  nil,
		},
		{
			name: "Nil AS in ASR",
			arg:  NewAllocationSetRange(nil),
		},
		{
			name: "Normal ASR",
			arg: &AllocationSetRange{
				Allocations: []*AllocationSet{
					{
						Allocations: map[string]*Allocation{
							"a": {
								Start: time.Now().UTC().Truncate(day),
							},
						},
					},
				},
			},
		},
	}

	for _, test := range tests {

		bytes, err := json.Marshal(test.arg)
		if err != nil {
			t.Fatalf("ASR Marshal: test %s, unexpected error: %s", test.name, err)
		}

		var testASR []*AllocationSet
		marshaled := &testASR

		err = json.Unmarshal(bytes, marshaled)

		if err != nil {
			t.Fatalf("ASR Unmarshal: test %s: unexpected error: %s", test.name, err)
		}

		if test.arg.Length() != len(testASR) {
			t.Fatalf("ASR Unmarshal: test %s: length mutated in encoding: expected %d but got %d", test.name, test.arg.Length(), len(testASR))
		}

		// Allocations don't unmarshal back from json
	}
}

// TODO niko/etl
// func TestAllocationSetRange_Slice(t *testing.T) {}

// TODO niko/etl
// func TestAllocationSetRange_Window(t *testing.T) {}

func TestAllocationSetRange_Start(t *testing.T) {
	tests := []struct {
		name string
		arg  *AllocationSetRange

		expectError bool
		expected    time.Time
	}{
		{
			name: "Empty ASR",
			arg:  nil,

			expectError: true,
		},
		{
			name: "Single allocation",
			arg: &AllocationSetRange{
				Allocations: []*AllocationSet{
					{
						Allocations: map[string]*Allocation{
							"a": {
								Start: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
							},
						},
					},
				},
			},

			expected: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "Two allocations",
			arg: &AllocationSetRange{
				Allocations: []*AllocationSet{
					{
						Allocations: map[string]*Allocation{
							"a": {
								Start: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
							},
							"b": {
								Start: time.Date(1970, 1, 2, 0, 0, 0, 0, time.UTC),
							},
						},
					},
				},
			},

			expected: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "Two AllocationSets",
			arg: &AllocationSetRange{
				Allocations: []*AllocationSet{
					{
						Allocations: map[string]*Allocation{
							"a": {
								Start: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
							},
						},
					},
					{
						Allocations: map[string]*Allocation{
							"b": {
								Start: time.Date(1970, 1, 2, 0, 0, 0, 0, time.UTC),
							},
						},
					},
				},
			},

			expected: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, test := range tests {
		result, err := test.arg.Start()
		if test.expectError && err != nil {
			continue
		}

		if test.expectError && err == nil {
			t.Errorf("%s: expected error and got none", test.name)
		} else if result != test.expected {
			t.Errorf("%s: expected %s but got %s", test.name, test.expected, result)
		}
	}
}

func TestAllocationSetRange_End(t *testing.T) {
	tests := []struct {
		name string
		arg  *AllocationSetRange

		expectError bool
		expected    time.Time
	}{
		{
			name: "Empty ASR",
			arg:  nil,

			expectError: true,
		},
		{
			name: "Single allocation",
			arg: &AllocationSetRange{
				Allocations: []*AllocationSet{
					{
						Allocations: map[string]*Allocation{
							"a": {
								End: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
							},
						},
					},
				},
			},

			expected: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "Two allocations",
			arg: &AllocationSetRange{
				Allocations: []*AllocationSet{
					{
						Allocations: map[string]*Allocation{
							"a": {
								End: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
							},
							"b": {
								End: time.Date(1970, 1, 2, 0, 0, 0, 0, time.UTC),
							},
						},
					},
				},
			},

			expected: time.Date(1970, 1, 2, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "Two AllocationSets",
			arg: &AllocationSetRange{
				Allocations: []*AllocationSet{
					{
						Allocations: map[string]*Allocation{
							"a": {
								End: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
							},
						},
					},
					{
						Allocations: map[string]*Allocation{
							"b": {
								End: time.Date(1970, 1, 2, 0, 0, 0, 0, time.UTC),
							},
						},
					},
				},
			},

			expected: time.Date(1970, 1, 2, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, test := range tests {
		result, err := test.arg.End()
		if test.expectError && err != nil {
			continue
		}

		if test.expectError && err == nil {
			t.Errorf("%s: expected error and got none", test.name)
		} else if result != test.expected {
			t.Errorf("%s: expected %s but got %s", test.name, test.expected, result)
		}
	}
}

func TestAllocationSetRange_Minutes(t *testing.T) {
	tests := []struct {
		name string
		arg  *AllocationSetRange

		expected float64
	}{
		{
			name: "Empty ASR",
			arg:  nil,

			expected: 0,
		},
		{
			name: "Single allocation",
			arg: &AllocationSetRange{
				Allocations: []*AllocationSet{
					{
						Allocations: map[string]*Allocation{
							"a": {
								Start: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
								End:   time.Date(1970, 1, 2, 0, 0, 0, 0, time.UTC),
							},
						},
					},
				},
			},

			expected: 24 * 60,
		},
		{
			name: "Two allocations",
			arg: &AllocationSetRange{
				Allocations: []*AllocationSet{
					{
						Allocations: map[string]*Allocation{
							"a": {
								Start: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
								End:   time.Date(1970, 1, 2, 0, 0, 0, 0, time.UTC),
							},
							"b": {
								Start: time.Date(1970, 1, 2, 0, 0, 0, 0, time.UTC),
								End:   time.Date(1970, 1, 3, 0, 0, 0, 0, time.UTC),
							},
						},
					},
				},
			},

			expected: 2 * 24 * 60,
		},
		{
			name: "Two AllocationSets",
			arg: &AllocationSetRange{
				Allocations: []*AllocationSet{
					{
						Allocations: map[string]*Allocation{
							"a": {
								Start: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
								End:   time.Date(1970, 1, 2, 0, 0, 0, 0, time.UTC),
							},
						},
					},
					{
						Allocations: map[string]*Allocation{
							"b": {
								Start: time.Date(1970, 1, 2, 0, 0, 0, 0, time.UTC),
								End:   time.Date(1970, 1, 3, 0, 0, 0, 0, time.UTC),
							},
						},
					},
				},
			},

			expected: 2 * 24 * 60,
		},
	}

	for _, test := range tests {
		result := test.arg.Minutes()
		if result != test.expected {
			t.Errorf("%s: expected %f but got %f", test.name, test.expected, result)
		}
	}
}

func TestAllocationSet_Accumulate_Equals_AllocationSetRange_Accumulate(t *testing.T) {

	today := time.Now().Round(day)
	start := today.AddDate(0, 0, -4)

	var allocationSets []*AllocationSet

	for i := 0; i < 4; i++ {
		allocationSets = append(allocationSets, GenerateMockAllocationSet(start))
		start = start.AddDate(0, 0, 1)
	}

	var originalAllocationSets []*AllocationSet

	for _, as := range allocationSets {
		originalAllocationSets = append(originalAllocationSets, as.Clone())
	}

	asr := NewAllocationSetRange()
	for _, as := range allocationSets {
		asr.Append(as.Clone())
	}

	expected, err := asr.accumulate()
	if err != nil {
		t.Errorf("TestAllocationSet_Accumulate_Equals_AllocationSetRange_Accumulate: AllocationSetRange.Accumulate() returned an error\n")
	}

	var got *AllocationSet

	for i := 0; i < len(allocationSets); i++ {
		got, err = got.Accumulate(allocationSets[i])
		if err != nil {
			t.Errorf("TestAllocationSet_Accumulate_Equals_AllocationSetRange_Accumulate: got.Accumulate(allocationSets[%d]) returned an error\n", i)
		}
	}

	// compare the got and expected Allocation sets, ensure that they match
	if len(got.Allocations) != len(expected.Allocations) {
		t.Fatalf("TestAllocationSet_Accumulate_Equals_AllocationSetRange_Accumulate: length of got.Allocations does not match length of expected.Allocations\n")
	}
	for key, a := range got.Allocations {
		if _, ok := expected.Allocations[key]; !ok {
			t.Fatalf("TestAllocationSet_Accumulate_Equals_AllocationSetRange_Accumulate: got.Allocations[%s] not found in expected.Allocations\n", key)
		}

		if !a.Equal(expected.Allocations[key]) {
			t.Fatalf("TestAllocationSet_Accumulate_Equals_AllocationSetRange_Accumulate: got.Allocations[%s] did not match expected.Allocations[%[1]s]", key)
		}
	}

	if len(got.ExternalKeys) != len(expected.ExternalKeys) {
		t.Fatalf("TestAllocationSet_Accumulate_Equals_AllocationSetRange_Accumulate: length of got.ExternalKeys does not match length of expected.ExternalKeys\n")
	}

	if len(got.IdleKeys) != len(expected.IdleKeys) {
		t.Fatalf("TestAllocationSet_Accumulate_Equals_AllocationSetRange_Accumulate: length of got.IdleKeys does not match length of expected.IdleKeys\n")
	}

	if !got.Window.Start().UTC().Equal(expected.Window.Start().UTC()) {
		t.Fatalf("TestAllocationSet_Accumulate_Equals_AllocationSetRange_Accumulate: Window.start: got:%s, expected:%s\n", got.Window.Start(), expected.Window.Start())
	}
	if !got.Window.End().UTC().Equal(expected.Window.End().UTC()) {
		t.Fatalf("TestAllocationSet_Accumulate_Equals_AllocationSetRange_Accumulate: Window.end: got:%s, expected:%s\n", got.Window.End(), expected.Window.End())
	}

	for i := range allocationSets {
		for key, allocation := range allocationSets[i].Allocations {
			if !allocation.Equal(originalAllocationSets[i].Allocations[key]) {
				t.Fatalf("TestAllocationSet_Accumulate_Equals_AllocationSetRange_Accumulate: allocationSet has been mutated in Accumulate; allocationSet: %d, allocation: %s\n", i, key)
			}
		}
	}
}

func Test_AggregateByService_UnmountedLBs(t *testing.T) {
	end := time.Now().UTC().Truncate(day)
	start := end.Add(-day)

	normalProps := &AllocationProperties{
		Cluster:        "cluster-one",
		Container:      "nginx-plus-nginx-ingress",
		Controller:     "nginx-plus-nginx-ingress",
		ControllerKind: "deployment",
		Namespace:      "nginx-plus",
		Pod:            "nginx-plus-nginx-ingress-123a4b5678-ab12c",
		ProviderID:     "test",
		Node:           "testnode",
		Services: []string{
			"nginx-plus-nginx-ingress",
		},
	}

	problematicProps := &AllocationProperties{
		Cluster:    "cluster-one",
		Container:  UnmountedSuffix,
		Namespace:  UnmountedSuffix,
		Pod:        UnmountedSuffix,
		ProviderID: "test",
		Node:       "testnode",
		Services: []string{
			"nginx-plus-nginx-ingress",
			"ingress-nginx-controller",
			"pacman",
		},
	}

	idle := NewMockUnitAllocation(fmt.Sprintf("cluster-one/%s", IdleSuffix), start, day, &AllocationProperties{
		Cluster: "cluster-one",
	})
	// this allocation is the main point of the test; an unmounted LB that has services
	problematicAllocation := NewMockUnitAllocation("cluster-one//__unmounted__/__unmounted__/__unmounted__", start, day, problematicProps)

	two := NewMockUnitAllocation("cluster-one//nginx-plus/nginx-plus-nginx-ingress-123a4b5678-ab12c/nginx-plus-nginx-ingress", start, day, normalProps)
	three := NewMockUnitAllocation("cluster-one//nginx-plus/nginx-plus-nginx-ingress-123a4b5678-ab12c/nginx-plus-nginx-ingress", start, day, normalProps)
	four := NewMockUnitAllocation("cluster-one//nginx-plus/nginx-plus-nginx-ingress-123a4b5678-ab12c/nginx-plus-nginx-ingress", start, day, normalProps)

	problematicAllocation.ExternalCost = 2.35
	two.ExternalCost = 1.35
	three.ExternalCost = 2.60
	four.ExternalCost = 4.30
	set := NewAllocationSet(start, start.Add(day), problematicAllocation, two, three, four)

	set.Insert(idle)

	set.AggregateBy([]string{AllocationServiceProp}, &AllocationAggregationOptions{
		Filter: ops.Contains(allocation.FieldServices, "nginx-plus-nginx-ingress"),
	})

	for _, alloc := range set.Allocations {
		if !strings.Contains(UnmountedSuffix, alloc.Name) {
			props := alloc.Properties
			if props.Cluster == UnmountedSuffix {
				t.Error("cluster unmounted")
			}
			if props.Container == UnmountedSuffix {
				t.Error("container unmounted")
			}
			if props.Namespace == UnmountedSuffix {
				t.Error("namespace unmounted")
			}
			if props.Pod == UnmountedSuffix {
				t.Error("pod unmounted")
			}
			if props.Controller == UnmountedSuffix {
				t.Error("controller unmounted")
			}
		}
	}

	spew.Config.DisableMethods = true
	t.Logf("%s", spew.Sdump(set.Allocations))
}

func Test_DetermineSharingName(t *testing.T) {
	var alloc *Allocation
	var name string
	var err error

	// test nil allocation with nil options
	name, err = alloc.determineSharingName(nil)
	if err == nil {
		t.Fatalf("determineSharingName: expected error; actual nil")
	}

	// test nil with non-nil options
	name, err = alloc.determineSharingName(&AllocationAggregationOptions{})
	if err == nil {
		t.Fatalf("determineSharingName: expected error; actual nil")
	}

	alloc = &Allocation{}
	alloc.Properties = &AllocationProperties{
		Cluster: "cluster1",
		Labels: map[string]string{
			"app": "app1",
			"env": "env1",
		},
		Namespace: "namespace1",
	}

	// test non-nil allocation with nil options
	name, err = alloc.determineSharingName(nil)
	if err != nil {
		t.Fatalf("determineSharingName: expected no error; actual \"%s\"", err)
	} else if err != nil || name != "unknown" {
		t.Fatalf("determineSharingName: expected \"unknown\"; actual \"%s\"", name)
	}

	// test non-nil allocation with empty options
	options := &AllocationAggregationOptions{}
	name, err = alloc.determineSharingName(options)
	if err != nil {
		t.Fatalf("determineSharingName: expected no error; actual \"%s\"", err)
	} else if err != nil || name != "unknown" {
		t.Fatalf("determineSharingName: expected \"unknown\"; actual \"%s\"", name)
	}

	// test non-nil allocation with matching namespace options
	options.SharedNamespaces = []string{"namespace1"}
	name, err = alloc.determineSharingName(options)
	if err != nil {
		t.Fatalf("determineSharingName: expected no error; actual \"%s\"", err)
	} else if err != nil || name != "namespace1" {
		t.Fatalf("determineSharingName: expected \"namespace1\"; actual \"%s\"", name)
	}

	// test non-nil allocation with non-matching namespace options
	options.SharedNamespaces = []string{"namespace2"}
	name, err = alloc.determineSharingName(options)
	if err != nil {
		t.Fatalf("determineSharingName: expected no error; actual \"%s\"", err)
	} else if err != nil || name != "unknown" {
		t.Fatalf("determineSharingName: expected \"unknown\"; actual \"%s\"", name)
	}

	// test non-nil allocation with matching label options
	options.SharedNamespaces = nil
	options.SharedLabels = map[string][]string{
		"app": {"app1"},
	}
	name, err = alloc.determineSharingName(options)
	if err != nil {
		t.Fatalf("determineSharingName: expected no error; actual \"%s\"", err)
	} else if err != nil || name != "app1" {
		t.Fatalf("determineSharingName: expected \"app1\"; actual \"%s\"", name)
	}

	// test non-nil allocation with partial-matching label options
	options.SharedLabels = map[string][]string{
		"app": {"app1", "app2"},
	}
	name, err = alloc.determineSharingName(options)
	if err != nil {
		t.Fatalf("determineSharingName: expected no error; actual \"%s\"", err)
	} else if err != nil || name != "app1" {
		t.Fatalf("determineSharingName: expected \"app1\"; actual \"%s\"", name)
	}

	// test non-nil allocation with non-matching label options
	options.SharedLabels = map[string][]string{
		"app": {"app2"},
	}
	name, err = alloc.determineSharingName(options)
	if err != nil {
		t.Fatalf("determineSharingName: expected no error; actual \"%s\"", err)
	} else if err != nil || name != "unknown" {
		t.Fatalf("determineSharingName: expected \"unknown\"; actual \"%s\"", name)
	}

	// test non-nil allocation with matching namespace and label options
	options.SharedNamespaces = []string{"namespace1"}
	options.SharedLabels = map[string][]string{
		"app": {"app1"},
	}
	name, err = alloc.determineSharingName(options)
	if err != nil {
		t.Fatalf("determineSharingName: expected no error; actual \"%s\"", err)
	} else if err != nil || name != "app1" {
		t.Fatalf("determineSharingName: expected \"app1\"; actual \"%s\"", name)
	}

	// test non-nil allocation with non-matching namespace and matching label options
	options.SharedNamespaces = []string{"namespace2"}
	options.SharedLabels = map[string][]string{
		"app": {"app1"},
	}
	name, err = alloc.determineSharingName(options)
	if err != nil {
		t.Fatalf("determineSharingName: expected no error; actual \"%s\"", err)
	} else if err != nil || name != "app1" {
		t.Fatalf("determineSharingName: expected \"app1\"; actual \"%s\"", name)
	}

	// test non-nil allocation with non-matching namespace and non-matching label options
	options.SharedNamespaces = []string{"namespace2"}
	options.SharedLabels = map[string][]string{
		"app": {"app2"},
	}
	name, err = alloc.determineSharingName(options)
	if err != nil {
		t.Fatalf("determineSharingName: expected no error; actual \"%s\"", err)
	} else if err != nil || name != "unknown" {
		t.Fatalf("determineSharingName: expected \"unknown\"; actual \"%s\"", name)
	}

	// test non-nil allocation with multiple matching label options
	alloc.Properties.Labels = map[string]string{
		"app": "app1",
		"env": "env1",
	}
	options.SharedNamespaces = nil
	options.SharedLabels = map[string][]string{
		"app": {"app1"},
		"env": {"env1"},
	}
	name, err = alloc.determineSharingName(options)
	if err != nil {
		t.Fatalf("determineSharingName: expected no error; actual \"%s\"", err)
	} else if err != nil || name != "app1" {
		t.Fatalf("determineSharingName: expected \"app1\"; actual \"%s\"", name)
	}

	// test non-nil allocation with one matching label option
	alloc.Properties.Labels = map[string]string{
		"app": "app2",
		"env": "env1",
	}
	options.SharedNamespaces = nil
	options.SharedLabels = map[string][]string{
		"app": {"app1"},
		"env": {"env1"},
	}
	name, err = alloc.determineSharingName(options)
	if err != nil {
		t.Fatalf("determineSharingName: expected no error; actual \"%s\"", err)
	} else if err != nil || name != "env1" {
		t.Fatalf("determineSharingName: expected \"env1\"; actual \"%s\"", name)
	}

	// test non-nil allocation with one matching namespace option
	alloc.Properties.Namespace = "namespace1"
	options.SharedNamespaces = []string{"namespace1", "namespace2"}
	options.SharedLabels = nil
	name, err = alloc.determineSharingName(options)
	if err != nil {
		t.Fatalf("determineSharingName: expected no error; actual \"%s\"", err)
	} else if err != nil || name != "namespace1" {
		t.Fatalf("determineSharingName: expected \"namespace1\"; actual \"%s\"", name)
	}

	// test non-nil allocation with another one matching namespace option
	alloc.Properties.Namespace = "namespace2"
	options.SharedNamespaces = []string{"namespace1", "namespace2"}
	options.SharedLabels = nil
	name, err = alloc.determineSharingName(options)
	if err != nil {
		t.Fatalf("determineSharingName: expected no error; actual \"%s\"", err)
	} else if err != nil || name != "namespace2" {
		t.Fatalf("determineSharingName: expected \"namespace2\"; actual \"%s\"", name)
	}

	// test non-nil allocation with non-matching namespace options
	alloc.Properties.Namespace = "namespace3"
	options.SharedNamespaces = []string{"namespace1", "namespace2"}
	name, err = alloc.determineSharingName(options)
	if err != nil {
		t.Fatalf("determineSharingName: expected no error; actual \"%s\"", err)
	} else if err != nil || name != "unknown" {
		t.Fatalf("determineSharingName: expected \"unknown\"; actual \"%s\"", name)
	}
}

func TestIsFilterEmptyTrue(t *testing.T) {
	compiler := NewAllocationMatchCompiler(nil)
	matcher, err := compiler.Compile(nil)
	if err != nil {
		t.Fatalf("compiling nil filter: %s", err)
	}

	result := isFilterEmpty(matcher)
	if !result {
		t.Errorf("matcher '%+v' should be reported empty but wasn't", matcher)
	}
}

func TestIsFilterEmptyFalse(t *testing.T) {
	compiler := NewAllocationMatchCompiler(nil)
	matcher, err := compiler.Compile(ops.Eq(allocation.FieldClusterID, "test"))
	if err != nil {
		t.Fatalf("compiling nil filter: %s", err)
	}
	result := isFilterEmpty(matcher)
	if result {
		t.Errorf("matcher '%+v' should be not be reported empty but was", matcher)
	}
}

func TestAllocation_SanitizeNaN(t *testing.T) {
	tcName := "TestAllocation_SanitizeNaN"
	alloc := getMockAllocation(math.NaN())
	alloc.SanitizeNaN()
	checkAllocation(t, tcName, alloc)
}

func checkAllocation(t *testing.T, tcName string, alloc Allocation) {
	v := reflect.ValueOf(alloc)
	checkAllFloat64sForNaN(t, v, tcName)

	vRaw := reflect.ValueOf(*alloc.RawAllocationOnly)
	checkAllFloat64sForNaN(t, vRaw, tcName)

	for _, pv := range alloc.PVs {
		vPV := reflect.ValueOf(*pv)
		checkAllFloat64sForNaN(t, vPV, tcName)
	}

	for _, parc := range alloc.ProportionalAssetResourceCosts {
		vParc := reflect.ValueOf(parc)
		checkAllFloat64sForNaN(t, vParc, tcName)
	}

	for _, scb := range alloc.SharedCostBreakdown {
		vScb := reflect.ValueOf(scb)
		checkAllFloat64sForNaN(t, vScb, tcName)
	}

	for _, lb := range alloc.LoadBalancers {
		vLb := reflect.ValueOf(*lb)
		checkAllFloat64sForNaN(t, vLb, tcName)
	}
}

func TestAllocationSet_SanitizeNaN(t *testing.T) {
	allocNaN := getMockAllocation(math.NaN())
	allocNotNaN := getMockAllocation(1.2)
	allocSet := AllocationSet{
		Allocations: map[string]*Allocation{"NaN": &allocNaN, "notNaN": &allocNotNaN},
	}

	allocSet.SanitizeNaN()

	for _, a := range allocSet.Allocations {
		checkAllocation(t, "TestAllocationSet_SanitizeNaN", *a)
	}

}

func getMockAllocation(f float64) Allocation {
	alloc := Allocation{
		Name:                           "mockAllocation",
		Properties:                     nil,
		Window:                         Window{},
		Start:                          time.Time{},
		End:                            time.Time{},
		CPUCoreHours:                   f,
		CPUCoreRequestAverage:          f,
		CPUCoreUsageAverage:            f,
		CPUCost:                        f,
		CPUCostAdjustment:              f,
		GPUHours:                       f,
		GPUCost:                        f,
		GPUCostAdjustment:              f,
		NetworkTransferBytes:           f,
		NetworkReceiveBytes:            f,
		NetworkCost:                    f,
		NetworkCrossZoneCost:           f,
		NetworkCrossRegionCost:         f,
		NetworkInternetCost:            f,
		NetworkCostAdjustment:          f,
		LoadBalancerCost:               f,
		LoadBalancerCostAdjustment:     f,
		PVs:                            PVAllocations{{Cluster: "testPV", Name: "PVName"}: getMockPVAllocation(math.NaN())},
		PVCostAdjustment:               f,
		RAMByteHours:                   f,
		RAMBytesRequestAverage:         f,
		RAMBytesUsageAverage:           f,
		RAMCost:                        f,
		RAMCostAdjustment:              f,
		SharedCost:                     f,
		ExternalCost:                   f,
		RawAllocationOnly:              getMockRawAllocationOnlyData(f),
		ProportionalAssetResourceCosts: ProportionalAssetResourceCosts{"NaN": *getMockPARC(f)},
		SharedCostBreakdown:            SharedCostBreakdowns{"NaN": *getMockSharedCostBreakdown(f)},
		LoadBalancers:                  LbAllocations{"NaN": getMockLbAllocation(f)},
	}
	return alloc
}

func TestPVAllocation_SanitizeNaN(t *testing.T) {
	pva := getMockPVAllocation(math.NaN())
	pva.SanitizeNaN()
	v := reflect.ValueOf(*pva)
	checkAllFloat64sForNaN(t, v, "TestPVAllocation_SanitizeNaN")
}

func TestPVAllocations_SanitizeNaN(t *testing.T) {
	pvaNaN := getMockPVAllocation(math.NaN())
	pvaNotNaN := getMockPVAllocation(1.2)
	pvs := PVAllocations{{Cluster: "testPV", Name: "PVName1"}: pvaNaN, {Cluster: "testPV", Name: "PVName2"}: pvaNotNaN}
	pvs.SanitizeNaN()
	for _, pv := range pvs {
		v := reflect.ValueOf(*pv)
		checkAllFloat64sForNaN(t, v, "TestPVAllocations_SanitizeNaN")
	}

}

func getMockPVAllocation(f float64) *PVAllocation {
	return &PVAllocation{
		ByteHours: f,
		Cost:      f,
	}
}

func TestRawAllocationOnlyData_SanitizeNaN(t *testing.T) {
	raw := getMockRawAllocationOnlyData(math.NaN())
	raw.SanitizeNaN()
	v := reflect.ValueOf(*raw)
	checkAllFloat64sForNaN(t, v, "TestRawAllocationOnlyData_SanitizeNaN")

	nan := math.NaN()
	nilRawAllocation := &RawAllocationOnlyData{
		CPUCoreUsageMax:  nan,
		RAMBytesUsageMax: nan,
		GPUUsageMax:      &nan,
	}

	nilRawAllocation.SanitizeNaN()

	// SanitizeNaN allocates nil if NaN is passed
	if nilRawAllocation.GPUUsageMax != nil {
		t.Fatalf("want: nil, got: %v", nilRawAllocation.GPUUsageMax)
	}

	// SanitizeNaN allocates 0.0 if NaN is passed
	if nilRawAllocation.CPUCoreUsageMax != 0.0 {
		t.Fatalf("want: 0.0, got: %v", nilRawAllocation.CPUCoreUsageMax)
	}

	// SanitizeNaN allocates 0.0 if NaN is passed
	if nilRawAllocation.RAMBytesUsageMax != 0.0 {
		t.Fatalf("want: 0.0, got: %v", nilRawAllocation.RAMBytesUsageMax)
	}

}

func getMockRawAllocationOnlyData(f float64) *RawAllocationOnlyData {
	return &RawAllocationOnlyData{
		CPUCoreUsageMax:  f,
		RAMBytesUsageMax: f,
		GPUUsageMax:      &f,
	}
}

func TestLbAllocation_SanitizeNaN(t *testing.T) {
	lbaNaN := getMockLbAllocation(math.NaN())
	lbaNaN.SanitizeNaN()
	v := reflect.ValueOf(*lbaNaN)
	checkAllFloat64sForNaN(t, v, "TestLbAllocation_SanitizeNaN")
}

func TestLbAllocations_SanitizeNaN(t *testing.T) {
	lbaNaN := getMockLbAllocation(math.NaN())
	lbaValid := getMockLbAllocation(1.2)

	lbas := LbAllocations{"NaN": lbaNaN, "notNaN": lbaValid}
	lbas.SanitizeNaN()
	for _, lba := range lbas {
		v := reflect.ValueOf(*lba)
		checkAllFloat64sForNaN(t, v, "TestLbAllocations_SanitizeNaN")
	}
}

func getMockLbAllocation(f float64) *LbAllocation {
	return &LbAllocation{
		Service: "testLoadBalancer",
		Cost:    f,
		Private: false,
	}
}

func TestProportionalAssetResourceCosts_SanitizeNaN(t *testing.T) {
	parcAllNaN := getMockPARC(math.NaN())
	parcNotNaN := getMockPARC(1.2)

	parcs := ProportionalAssetResourceCosts{"NaN": *parcAllNaN, "notNaN": *parcNotNaN}
	parcs.SanitizeNaN()

	for _, parc := range parcs {
		v := reflect.ValueOf(parc)
		checkAllFloat64sForNaN(t, v, "TestProportionalAssetResourceCosts_SanitizeNaN")
	}
}

func getMockPARC(f float64) *ProportionalAssetResourceCost {
	return &ProportionalAssetResourceCost{
		Cluster:                      "testCluster",
		Name:                         "testName",
		Type:                         "testType",
		ProviderID:                   "testProvider",
		CPUPercentage:                f,
		GPUPercentage:                f,
		RAMPercentage:                f,
		LoadBalancerPercentage:       f,
		PVPercentage:                 f,
		NodeResourceCostPercentage:   f,
		GPUTotalCost:                 f,
		GPUProportionalCost:          f,
		CPUTotalCost:                 f,
		CPUProportionalCost:          f,
		RAMTotalCost:                 f,
		RAMProportionalCost:          f,
		LoadBalancerProportionalCost: f,
		LoadBalancerTotalCost:        f,
		PVProportionalCost:           f,
		PVTotalCost:                  f,
	}
}

func TestSharedCostBreakdowns_SanitizeNaN(t *testing.T) {
	scbNaN := getMockSharedCostBreakdown(math.NaN())
	scbNotNaN := getMockSharedCostBreakdown(1.2)

	scbs := SharedCostBreakdowns{"NaN": *scbNaN, "notNaN": *scbNotNaN}
	scbs.SanitizeNaN()
	for _, scb := range scbs {
		v := reflect.ValueOf(scb)
		checkAllFloat64sForNaN(t, v, "TestSharedCostBreakdowns_SanitizeNaN")
	}
}

func getMockSharedCostBreakdown(f float64) *SharedCostBreakdown {
	return &SharedCostBreakdown{
		Name:         "testBreakdown",
		TotalCost:    f,
		CPUCost:      f,
		GPUCost:      f,
		RAMCost:      f,
		PVCost:       f,
		NetworkCost:  f,
		LBCost:       f,
		ExternalCost: f,
	}
}

func checkAllFloat64sForNaN(t *testing.T, v reflect.Value, testCaseName string) {
	vType := v.Type()

	// go through each field on the struct
	for i := 0; i < v.NumField(); i++ {
		// Check if field is public and can be converted to a float
		if v.Field(i).CanInterface() && v.Field(i).CanFloat() {
			f := v.Field(i).Float()
			if math.IsNaN(f) {
				t.Fatalf("%s: expected not NaN for field: %s, got:NaN", testCaseName, vType.Field(i).Name)
			}
		}
	}
}
