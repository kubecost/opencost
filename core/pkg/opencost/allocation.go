package opencost

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/filter"
	"github.com/opencost/opencost/core/pkg/filter/ast"
	"github.com/opencost/opencost/core/pkg/filter/matcher"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
	"golang.org/x/exp/slices"
)

// TODO Clean-up use of IsEmpty; nil checks should be separated for safety.

// TODO Consider making Allocation an interface, which is fulfilled by structs
// like KubernetesAllocation, IdleAllocation, and ExternalAllocation.

// ExternalSuffix indicates an external allocation
const ExternalSuffix = "__external__"

// IdleSuffix indicates an idle allocation property
const IdleSuffix = "__idle__"

// SharedSuffix indicates an shared allocation property
const SharedSuffix = "__shared__"

// UnallocatedSuffix indicates an unallocated allocation property
const UnallocatedSuffix = "__unallocated__"

// UnmountedSuffix indicated allocation to an unmounted resource (PV or LB)
const UnmountedSuffix = "__unmounted__"

// ShareWeighted indicates that a shared resource should be shared as a
// proportion of the cost of the remaining allocations.
const ShareWeighted = "__weighted__"

// ShareEven indicates that a shared resource should be shared evenly across
// all remaining allocations.
const ShareEven = "__even__"

// ShareNone indicates that a shareable resource should not be shared
const ShareNone = "__none__"

// Allocation is a unit of resource allocation and cost for a given window
// of time and for a given kubernetes construct with its associated set of
// properties.
// TODO:CLEANUP consider dropping name in favor of just Allocation and an
// Assets-style key() function for AllocationSet.
type Allocation struct {
	Name                       string                `json:"name"`
	Properties                 *AllocationProperties `json:"properties,omitempty"`
	Window                     Window                `json:"window"`
	Start                      time.Time             `json:"start"`
	End                        time.Time             `json:"end"`
	CPUCoreHours               float64               `json:"cpuCoreHours"`
	CPUCoreRequestAverage      float64               `json:"cpuCoreRequestAverage"`
	CPUCoreUsageAverage        float64               `json:"cpuCoreUsageAverage"`
	CPUCost                    float64               `json:"cpuCost"`
	CPUCostAdjustment          float64               `json:"cpuCostAdjustment"`
	CPUCostIdle                float64               `json:"cpuCostIdle"` //@bingen:field[ignore]
	GPUHours                   float64               `json:"gpuHours"`
	GPUCost                    float64               `json:"gpuCost"`
	GPUCostAdjustment          float64               `json:"gpuCostAdjustment"`
	GPUCostIdle                float64               `json:"gpuCostIdle"` //@bingen:field[ignore]
	NetworkTransferBytes       float64               `json:"networkTransferBytes"`
	NetworkReceiveBytes        float64               `json:"networkReceiveBytes"`
	NetworkCost                float64               `json:"networkCost"`
	NetworkCrossZoneCost       float64               `json:"networkCrossZoneCost"`   // @bingen:field[version=16]
	NetworkCrossRegionCost     float64               `json:"networkCrossRegionCost"` // @bingen:field[version=16]
	NetworkInternetCost        float64               `json:"networkInternetCost"`    // @bingen:field[version=16]
	NetworkCostAdjustment      float64               `json:"networkCostAdjustment"`
	LoadBalancerCost           float64               `json:"loadBalancerCost"`
	LoadBalancerCostAdjustment float64               `json:"loadBalancerCostAdjustment"`
	PVs                        PVAllocations         `json:"pvs"`
	PVCostAdjustment           float64               `json:"pvCostAdjustment"`
	RAMByteHours               float64               `json:"ramByteHours"`
	RAMBytesRequestAverage     float64               `json:"ramByteRequestAverage"`
	RAMBytesUsageAverage       float64               `json:"ramByteUsageAverage"`
	RAMCost                    float64               `json:"ramCost"`
	RAMCostAdjustment          float64               `json:"ramCostAdjustment"`
	RAMCostIdle                float64               `json:"ramCostIdle"` //@bingen:field[ignore]
	SharedCost                 float64               `json:"sharedCost"`
	ExternalCost               float64               `json:"externalCost"`
	// RawAllocationOnly is a pointer so if it is not present it will be
	// marshalled as null rather than as an object with Go default values.
	RawAllocationOnly *RawAllocationOnlyData `json:"rawAllocationOnly"`
	// ProportionalAssetResourceCost represents the per-resource costs of the
	// allocation as a percentage of the per-resource total cost of the
	// asset on which the allocation was run. It is optionally computed
	// and appended to an Allocation, and so by default is is nil.
	ProportionalAssetResourceCosts ProportionalAssetResourceCosts `json:"proportionalAssetResourceCosts"` //@bingen:field[ignore]
	SharedCostBreakdown            SharedCostBreakdowns           `json:"sharedCostBreakdown"`            //@bingen:field[ignore]
	LoadBalancers                  LbAllocations                  `json:"LoadBalancers"`                  //@bingen:field[version=18]
	// UnmountedPVCost is used to track how much of the cost in PVs is for an
	// unmounted PV. It is not additive of PVCost() and need not be sent in API
	// responses.
	UnmountedPVCost             float64        `json:"-"`             //@bingen:field[ignore]
	deprecatedGPURequestAverage float64        `json:"-"`             //@bingen:field[version=22]
	deprecatedGPUUsageAverage   float64        `json:"-"`             //@bingen:field[version=22]
	GPUAllocation               *GPUAllocation `json:"GPUAllocation"` //@bingen:field[version=23]
}

type GPUAllocation struct {
	GPUDevice string `json:"gpuDevice,omitempty"`
	GPUModel  string `json:"gpuModel,omitempty"`
	GPUUUID   string `json:"gpuUUID,omitempty"`

	IsGPUShared       *bool    `json:"isGPUShared"`
	GPUUsageAverage   *float64 `json:"gpuUsageAverage"`
	GPURequestAverage *float64 `json:"gpuRequestAverage"`
}

func (orig *GPUAllocation) SanitizeNaN() {
	if orig == nil {
		return
	}
	if orig.GPURequestAverage == nil || math.IsNaN(*orig.GPURequestAverage) {
		orig.GPURequestAverage = nil
	}
	if orig.GPUUsageAverage == nil || math.IsNaN(*orig.GPUUsageAverage) {
		orig.GPUUsageAverage = nil
	}
}

func (orig *GPUAllocation) Clone() *GPUAllocation {
	if orig == nil {
		return nil
	}

	return &GPUAllocation{
		GPUDevice:         orig.GPUDevice,
		GPUModel:          orig.GPUModel,
		GPUUUID:           orig.GPUUUID,
		IsGPUShared:       orig.IsGPUShared,
		GPUUsageAverage:   orig.GPUUsageAverage,
		GPURequestAverage: orig.GPURequestAverage,
	}
}

func (orig *GPUAllocation) Equal(that *GPUAllocation) bool {
	if orig == nil && that == nil {
		return true
	}
	if orig == nil || that == nil {
		return false
	}

	return orig.GPUDevice == that.GPUDevice &&
		orig.GPUModel == that.GPUModel &&
		orig.GPUUUID == that.GPUUUID &&
		orig.IsGPUShared == that.IsGPUShared &&
		orig.GPUUsageAverage == that.GPUUsageAverage &&
		orig.GPURequestAverage == that.GPURequestAverage

}

type LbAllocations map[string]*LbAllocation

func (orig LbAllocations) Clone() LbAllocations {
	if orig == nil {
		return nil
	}

	newAllocs := LbAllocations{}

	for key, lbAlloc := range orig {
		newAllocs[key] = &LbAllocation{
			Service: lbAlloc.Service,
			Cost:    lbAlloc.Cost,
			Private: lbAlloc.Private,
			Ip:      lbAlloc.Ip,
			Hours:   lbAlloc.Hours,
		}
	}
	return newAllocs
}

type LbAllocation struct {
	Service    string  `json:"service"`
	Cost       float64 `json:"cost"`
	Private    bool    `json:"private"`
	Ip         string  `json:"ip"`         //@bingen:field[version=19]
	Hours      float64 `json:"hours"`      //@bingen:field[version=21]
	Adjustment float64 `json:"adjustment"` //@bingen:field[ignore]
}

func (lba *LbAllocation) SanitizeNaN() {
	if lba == nil {
		return
	}
	if math.IsNaN(lba.Cost) {
		log.DedupedWarningf(5, "LBAllocation: Unexpected NaN found for Cost service:%s", lba.Service)
		lba.Cost = 0
	}
	if math.IsNaN(lba.Hours) {
		log.DedupedWarningf(5, "LBAllocation: Unexpected NaN found for Hours service:%s", lba.Service)
		lba.Hours = 0
	}
}

// RawAllocationOnlyData is information that only belong in "raw" Allocations,
// those which have not undergone aggregation, accumulation, or any other form
// of combination to produce a new Allocation from other Allocations.
//
// Max usage data belongs here because computing the overall maximum from two
// or more Allocations is a non-trivial operation that cannot be defined without
// maintaining a large amount of state. Consider the following example:
// _______________________________________________
//
// A1 Using 3 CPU    ----      -----     ------
// A2 Using 2 CPU      ----      -----      ----
// A3 Using 1 CPU         ---       --
// _______________________________________________
//
//	Time ---->
//
// The logical maximum CPU usage is 5, but this cannot be calculated iteratively,
// which is how we calculate aggregations and accumulations of Allocations currently.
// This becomes a problem I could call "maximum sum of overlapping intervals" and is
// essentially a variant of an interval scheduling algorithm.
//
// If we had types to differentiate between regular Allocations and AggregatedAllocations
// then this type would be unnecessary and its fields would go into the regular Allocation
// and not in the AggregatedAllocation.
type RawAllocationOnlyData struct {
	CPUCoreUsageMax  float64  `json:"cpuCoreUsageMax"`
	RAMBytesUsageMax float64  `json:"ramByteUsageMax"`
	GPUUsageMax      *float64 `json:"gpuUsageMax"` //@bingen:field[version=23]
}

// Clone returns a deep copy of the given RawAllocationOnlyData
func (r *RawAllocationOnlyData) Clone() *RawAllocationOnlyData {
	if r == nil {
		return nil
	}

	return &RawAllocationOnlyData{
		CPUCoreUsageMax:  r.CPUCoreUsageMax,
		RAMBytesUsageMax: r.RAMBytesUsageMax,
		GPUUsageMax:      r.GPUUsageMax,
	}
}

// Equal returns true if the RawAllocationOnlyData is approximately equal
func (r *RawAllocationOnlyData) Equal(that *RawAllocationOnlyData) bool {
	if r == nil && that == nil {
		return true
	}
	if r == nil || that == nil {
		return false
	}
	cmpResult := util.IsApproximately(r.CPUCoreUsageMax, that.CPUCoreUsageMax) &&
		util.IsApproximately(r.RAMBytesUsageMax, that.RAMBytesUsageMax)

	if r.GPUUsageMax != nil && that.GPUUsageMax != nil {
		cmpResult = cmpResult && util.IsApproximately(*r.GPUUsageMax, *that.GPUUsageMax)
	} else if !(r.GPUUsageMax == nil && that.GPUUsageMax == nil) {
		cmpResult = false
	}

	return cmpResult
}

func (r *RawAllocationOnlyData) SanitizeNaN() {
	if r == nil {
		return
	}
	if math.IsNaN(r.CPUCoreUsageMax) {
		log.DedupedWarningf(5, "RawAllocationOnlyData: Unexpected NaN found for CPUCoreUsageMax")
		r.CPUCoreUsageMax = 0
	}
	if math.IsNaN(r.RAMBytesUsageMax) {
		log.DedupedWarningf(5, "RawAllocationOnlyData: Unexpected NaN found for RAMBytesUsageMax")
		r.RAMBytesUsageMax = 0
	}
	if r.GPUUsageMax != nil && math.IsNaN(*r.GPUUsageMax) {
		log.DedupedWarningf(5, "RawAllocationOnlyData: Unexpected NaN found for GPUUsageMax")
		r.GPUUsageMax = nil
	}
}

// PVAllocations is a map of Disk Asset Identifiers to the
// usage of them by an Allocation as recorded in a PVAllocation
type PVAllocations map[PVKey]*PVAllocation

// Clone creates a deep copy of a PVAllocations
func (pv PVAllocations) Clone() PVAllocations {
	if pv == nil {
		return nil
	}
	clonePV := make(map[PVKey]*PVAllocation, len(pv))
	for k, v := range pv {
		clonePV[k] = &PVAllocation{
			ByteHours:  v.ByteHours,
			Cost:       v.Cost,
			ProviderID: v.ProviderID,
		}
	}
	return clonePV
}

// Add adds contents of that to the calling PVAllocations
func (pv PVAllocations) Add(that PVAllocations) PVAllocations {
	apv := pv.Clone()
	if that != nil {
		if apv == nil {
			apv = PVAllocations{}
		}
		for pvKey, thatPVAlloc := range that {
			apvAlloc, ok := apv[pvKey]
			if !ok {
				apvAlloc = &PVAllocation{}
			}
			apvAlloc.Cost += thatPVAlloc.Cost
			apvAlloc.ByteHours += thatPVAlloc.ByteHours
			apv[pvKey] = apvAlloc
			if apvAlloc.ProviderID == thatPVAlloc.ProviderID {
				apv[pvKey].ProviderID = apvAlloc.ProviderID
			} else {
				apv[pvKey].ProviderID = ""
			}
		}
	}
	return apv
}

// Equal returns true if the two PVAllocations are equal in length and contain the same keys
// and values.
func (this PVAllocations) Equal(that PVAllocations) bool {
	if this == nil && that == nil {
		return true
	}
	if this == nil || that == nil {
		return false
	}

	if len(this) != len(that) {
		return false
	}

	for k, pv := range this {
		tv, ok := that[k]
		if !ok || !pv.Equal(tv) {
			return false
		}
	}

	return true
}

func (pvs PVAllocations) SanitizeNaN() {
	for _, pv := range pvs {
		pv.SanitizeNaN()
	}
}

// PVKey for identifying Disk type assets
type PVKey struct {
	Cluster string `json:"cluster"`
	Name    string `json:"name"`
}

func (pvk *PVKey) String() string {
	return fmt.Sprintf("cluster=%s:name=%s", pvk.Cluster, pvk.Name)
}

// FromString populates PVKey fields from string
func (pvk *PVKey) FromString(key string) error {
	splitKey := strings.Split(key, ":")
	if len(splitKey) != 2 {
		return fmt.Errorf("PVKey string '%s' has the incorrect format", key)
	}
	pvk.Cluster = strings.TrimPrefix(splitKey[0], "cluster=")
	pvk.Name = strings.TrimPrefix(splitKey[1], "name=")
	return nil
}

// PVAllocation contains the byte hour usage
// and cost of an Allocation for a single PV
type PVAllocation struct {
	ByteHours  float64 `json:"byteHours"`
	Cost       float64 `json:"cost"`
	ProviderID string  `json:"providerID"` // @bingen:field[version=20]
	Adjustment float64 `json:"adjustment"` //@bingen:field[ignore]
}

// Equal returns true if the two PVAllocation instances contain approximately the same
// values.
func (pva *PVAllocation) Equal(that *PVAllocation) bool {
	if pva == nil && that == nil {
		return true
	}
	if pva == nil || that == nil {
		return false
	}
	return util.IsApproximately(pva.ByteHours, that.ByteHours) &&
		util.IsApproximately(pva.Cost, that.Cost)
}

func (pva *PVAllocation) SanitizeNaN() {
	if pva == nil {
		return
	}
	if math.IsNaN(pva.ByteHours) {
		log.DedupedWarningf(5, "PVAllocation: Unexpected NaN found for ByteHours")
		pva.ByteHours = 0
	}
	if math.IsNaN(pva.Cost) {
		log.DedupedWarningf(5, "PVAllocation: Unexpected NaN found for Cost")
		pva.Cost = 0
	}
}

type ProportionalAssetResourceCost struct {
	Cluster                      string  `json:"cluster"`
	Name                         string  `json:"name,omitempty"`
	Type                         string  `json:"type,omitempty"`
	ProviderID                   string  `json:"providerID,omitempty"`
	CPUPercentage                float64 `json:"cpuPercentage"`
	GPUPercentage                float64 `json:"gpuPercentage"`
	RAMPercentage                float64 `json:"ramPercentage"`
	LoadBalancerPercentage       float64 `json:"loadBalancerPercentage"`
	PVPercentage                 float64 `json:"pvPercentage"`
	NodeResourceCostPercentage   float64 `json:"nodeResourceCostPercentage"`
	GPUTotalCost                 float64 `json:"-"`
	GPUProportionalCost          float64 `json:"-"`
	CPUTotalCost                 float64 `json:"-"`
	CPUProportionalCost          float64 `json:"-"`
	RAMTotalCost                 float64 `json:"-"`
	RAMProportionalCost          float64 `json:"-"`
	LoadBalancerProportionalCost float64 `json:"-"`
	LoadBalancerTotalCost        float64 `json:"-"`
	PVProportionalCost           float64 `json:"-"`
	PVTotalCost                  float64 `json:"-"`
}

func (parc ProportionalAssetResourceCost) Key(insertByName bool) string {
	if insertByName {
		return parc.Cluster + "," + parc.Name
	} else {
		return parc.Cluster
	}

}

type ProportionalAssetResourceCosts map[string]ProportionalAssetResourceCost

func (parcs ProportionalAssetResourceCosts) Clone() ProportionalAssetResourceCosts {
	cloned := ProportionalAssetResourceCosts{}

	for key, parc := range parcs {
		cloned[key] = parc
	}
	return cloned
}

func (parcs ProportionalAssetResourceCosts) Insert(parc ProportionalAssetResourceCost, insertByName bool) {
	if !insertByName {
		parc.Name = ""
		parc.Type = ""
		parc.ProviderID = ""
	}

	if curr, ok := parcs[parc.Key(insertByName)]; ok {
		toInsert := ProportionalAssetResourceCost{
			Name:                         curr.Name,
			Type:                         curr.Type,
			Cluster:                      curr.Cluster,
			ProviderID:                   curr.ProviderID,
			CPUProportionalCost:          curr.CPUProportionalCost + parc.CPUProportionalCost,
			RAMProportionalCost:          curr.RAMProportionalCost + parc.RAMProportionalCost,
			GPUProportionalCost:          curr.GPUProportionalCost + parc.GPUProportionalCost,
			PVProportionalCost:           curr.PVProportionalCost + parc.PVProportionalCost,
			LoadBalancerProportionalCost: curr.LoadBalancerProportionalCost + parc.LoadBalancerProportionalCost,
		}

		ComputePercentages(&toInsert)
		parcs[parc.Key(insertByName)] = toInsert
	} else {
		ComputePercentages(&parc)
		parcs[parc.Key(insertByName)] = parc
	}
}

func ComputePercentages(toInsert *ProportionalAssetResourceCost) {
	totalNodeCost := toInsert.RAMTotalCost + toInsert.CPUTotalCost + toInsert.GPUTotalCost

	if toInsert.CPUTotalCost > 0 {
		toInsert.CPUPercentage = toInsert.CPUProportionalCost / toInsert.CPUTotalCost
	}

	if toInsert.GPUTotalCost > 0 {
		toInsert.GPUPercentage = toInsert.GPUProportionalCost / toInsert.GPUTotalCost
	}

	if toInsert.LoadBalancerTotalCost > 0 {
		toInsert.LoadBalancerPercentage = toInsert.LoadBalancerProportionalCost / toInsert.LoadBalancerTotalCost
	}

	if toInsert.RAMTotalCost > 0 {
		toInsert.RAMPercentage = toInsert.RAMProportionalCost / toInsert.RAMTotalCost
	}

	if toInsert.PVTotalCost > 0 {
		toInsert.PVPercentage = toInsert.PVProportionalCost / toInsert.PVTotalCost
	}

	ramFraction := toInsert.RAMTotalCost / totalNodeCost
	if ramFraction != ramFraction || ramFraction < 0 {
		ramFraction = 0
	}

	cpuFraction := toInsert.CPUTotalCost / totalNodeCost
	if cpuFraction != cpuFraction || cpuFraction < 0 {
		cpuFraction = 0
	}

	gpuFraction := toInsert.GPUTotalCost / totalNodeCost
	if gpuFraction != gpuFraction || gpuFraction < 0 {
		gpuFraction = 0
	}

	toInsert.NodeResourceCostPercentage = (toInsert.RAMPercentage * ramFraction) +
		(toInsert.CPUPercentage * cpuFraction) + (toInsert.GPUPercentage * gpuFraction)
}

func (parcs ProportionalAssetResourceCosts) Add(that ProportionalAssetResourceCosts) {
	for _, parc := range that {
		// if name field is empty, we know this is a cluster level PARC aggregation
		insertByName := true
		if parc.Name == "" {
			insertByName = false
		}
		parcs.Insert(parc, insertByName)
	}
}

func (parcs ProportionalAssetResourceCosts) SanitizeNaN() {
	for key, parc := range parcs {
		if math.IsNaN(parc.CPUPercentage) {
			log.DedupedWarningf(5, "ProportionalAssetResourceCosts: Unexpected NaN found for CPUPercentage name:%s", parc.Name)
			parc.CPUPercentage = 0
		}
		if math.IsNaN(parc.GPUPercentage) {
			log.DedupedWarningf(5, "ProportionalAssetResourceCosts: Unexpected NaN found for GPUPercentage name:%s", parc.Name)
			parc.GPUPercentage = 0
		}
		if math.IsNaN(parc.RAMPercentage) {
			log.DedupedWarningf(5, "ProportionalAssetResourceCosts: Unexpected NaN found for RAMPercentage name:%s", parc.Name)
			parc.RAMPercentage = 0
		}
		if math.IsNaN(parc.LoadBalancerPercentage) {
			log.DedupedWarningf(5, "ProportionalAssetResourceCosts: Unexpected NaN found for LoadBalancerPercentage name:%s", parc.Name)
			parc.LoadBalancerPercentage = 0
		}
		if math.IsNaN(parc.PVPercentage) {
			log.DedupedWarningf(5, "ProportionalAssetResourceCosts: Unexpected NaN found for PVPercentage name:%s", parc.Name)
			parc.PVPercentage = 0
		}
		if math.IsNaN(parc.NodeResourceCostPercentage) {
			log.DedupedWarningf(5, "ProportionalAssetResourceCosts: Unexpected NaN found for NodeResourceCostPercentage name:%s", parc.Name)
			parc.NodeResourceCostPercentage = 0
		}
		if math.IsNaN(parc.GPUTotalCost) {
			log.DedupedWarningf(5, "ProportionalAssetResourceCosts: Unexpected NaN found for GPUTotalCost name:%s", parc.Name)
			parc.GPUTotalCost = 0
		}
		if math.IsNaN(parc.GPUProportionalCost) {
			log.DedupedWarningf(5, "ProportionalAssetResourceCosts: Unexpected NaN found for GPUProportionalCost name:%s", parc.Name)
			parc.GPUProportionalCost = 0
		}
		if math.IsNaN(parc.CPUTotalCost) {
			log.DedupedWarningf(5, "ProportionalAssetResourceCosts: Unexpected NaN found for CPUTotalCost name:%s", parc.Name)
			parc.CPUTotalCost = 0
		}
		if math.IsNaN(parc.CPUProportionalCost) {
			log.DedupedWarningf(5, "ProportionalAssetResourceCosts: Unexpected NaN found for CPUProportionalCost name:%s", parc.Name)
			parc.CPUProportionalCost = 0
		}
		if math.IsNaN(parc.RAMTotalCost) {
			log.DedupedWarningf(5, "ProportionalAssetResourceCosts: Unexpected NaN found for RAMTotalCost name:%s", parc.Name)
			parc.RAMTotalCost = 0
		}
		if math.IsNaN(parc.RAMProportionalCost) {
			log.DedupedWarningf(5, "ProportionalAssetResourceCosts: Unexpected NaN found for RAMProportionalCost name:%s", parc.Name)
			parc.RAMProportionalCost = 0
		}
		if math.IsNaN(parc.LoadBalancerProportionalCost) {
			log.DedupedWarningf(5, "ProportionalAssetResourceCosts: Unexpected NaN found for LoadBalancerProportionalCost name:%s", parc.Name)
			parc.LoadBalancerProportionalCost = 0
		}
		if math.IsNaN(parc.LoadBalancerTotalCost) {
			log.DedupedWarningf(5, "ProportionalAssetResourceCosts: Unexpected NaN found for LoadBalancerTotalCost name:%s", parc.Name)
			parc.LoadBalancerTotalCost = 0
		}
		if math.IsNaN(parc.PVProportionalCost) {
			log.DedupedWarningf(5, "ProportionalAssetResourceCosts: Unexpected NaN found for PVProportionalCost name:%s", parc.Name)
			parc.PVProportionalCost = 0
		}
		if math.IsNaN(parc.PVTotalCost) {
			log.DedupedWarningf(5, "ProportionalAssetResourceCosts: Unexpected NaN found for PVTotalCost name:%s", parc.Name)
			parc.PVTotalCost = 0
		}

		parcs[key] = parc
	}
}

type SharedCostBreakdown struct {
	Name         string  `json:"name"`
	TotalCost    float64 `json:"totalCost"`
	CPUCost      float64 `json:"cpuCost,omitempty"`
	GPUCost      float64 `json:"gpuCost,omitempty"`
	RAMCost      float64 `json:"ramCost,omitempty"`
	PVCost       float64 `json:"pvCost,omitempty"`
	NetworkCost  float64 `json:"networkCost,omitempty"`
	LBCost       float64 `json:"loadBalancerCost,omitempty"`
	ExternalCost float64 `json:"externalCost,omitempty"`
}

type SharedCostBreakdowns map[string]SharedCostBreakdown

func (scbs SharedCostBreakdowns) Clone() SharedCostBreakdowns {
	cloned := SharedCostBreakdowns{}

	for key, scb := range scbs {
		cloned[key] = scb
	}
	return cloned
}

func (scbs SharedCostBreakdowns) Insert(scb SharedCostBreakdown) {
	if curr, ok := scbs[scb.Name]; ok {
		scbs[scb.Name] = SharedCostBreakdown{
			Name:         curr.Name,
			TotalCost:    curr.TotalCost + scb.TotalCost,
			CPUCost:      curr.CPUCost + scb.CPUCost,
			GPUCost:      curr.GPUCost + scb.GPUCost,
			RAMCost:      curr.RAMCost + scb.RAMCost,
			PVCost:       curr.PVCost + scb.PVCost,
			NetworkCost:  curr.NetworkCost + scb.NetworkCost,
			LBCost:       curr.LBCost + scb.LBCost,
			ExternalCost: curr.ExternalCost + scb.ExternalCost,
		}
	} else {
		scbs[scb.Name] = scb
	}
}

func (scbs SharedCostBreakdowns) Add(that SharedCostBreakdowns) {
	for _, scb := range that {
		scbs.Insert(scb)
	}
}

func (scbs SharedCostBreakdowns) SanitizeNaN() {
	for key, scb := range scbs {
		if math.IsNaN(scb.CPUCost) {
			log.DedupedWarningf(5, "SharedCostBreakdown: Unexpected NaN found for CPUCost name:%s", scb.Name)
			scb.CPUCost = 0
		}
		if math.IsNaN(scb.GPUCost) {
			log.DedupedWarningf(5, "SharedCostBreakdown: Unexpected NaN found for GPUCost name:%s", scb.Name)
			scb.GPUCost = 0
		}
		if math.IsNaN(scb.RAMCost) {
			log.DedupedWarningf(5, "SharedCostBreakdown: Unexpected NaN found for RAMCost name:%s", scb.Name)
			scb.RAMCost = 0
		}
		if math.IsNaN(scb.PVCost) {
			log.DedupedWarningf(5, "SharedCostBreakdown: Unexpected NaN found for PVCost name:%s", scb.Name)
			scb.PVCost = 0
		}
		if math.IsNaN(scb.NetworkCost) {
			log.DedupedWarningf(5, "SharedCostBreakdown: Unexpected NaN found for NetworkCost name:%s", scb.Name)
			scb.NetworkCost = 0
		}
		if math.IsNaN(scb.LBCost) {
			log.DedupedWarningf(5, "SharedCostBreakdown: Unexpected NaN found for LBCost name:%s", scb.Name)
			scb.LBCost = 0
		}
		if math.IsNaN(scb.ExternalCost) {
			log.DedupedWarningf(5, "SharedCostBreakdown: Unexpected NaN found for ExternalCost name:%s", scb.Name)
			scb.ExternalCost = 0
		}
		if math.IsNaN(scb.TotalCost) {
			log.DedupedWarningf(5, "SharedCostBreakdown: Unexpected NaN found for TotalCost name:%s", scb.Name)
			scb.TotalCost = 0
		}
		scbs[key] = scb
	}
}

// GetWindow returns the window of the struct
func (a *Allocation) GetWindow() Window {
	return a.Window
}

// AllocationMatchFunc is a function that can be used to match Allocations by
// returning true for any given Allocation if a condition is met.
type AllocationMatchFunc func(*Allocation) bool

// Add returns the result of summing the two given Allocations, which sums the
// summary fields (e.g. costs, resources) and recomputes efficiency. Neither of
// the two original Allocations are mutated in the process.
func (a *Allocation) Add(that *Allocation) (*Allocation, error) {
	if a == nil {
		return that.Clone(), nil
	}

	if that == nil {
		return a.Clone(), nil
	}

	// Note: no need to clone "that", as add only mutates the receiver
	agg := a.Clone()
	agg.add(that)

	return agg, nil
}

// Clone returns a deep copy of the given Allocation
func (a *Allocation) Clone() *Allocation {
	if a == nil {
		return nil
	}

	return &Allocation{
		Name:                           a.Name,
		Properties:                     a.Properties.Clone(),
		Window:                         a.Window.Clone(),
		Start:                          a.Start,
		End:                            a.End,
		CPUCoreHours:                   a.CPUCoreHours,
		CPUCoreRequestAverage:          a.CPUCoreRequestAverage,
		CPUCoreUsageAverage:            a.CPUCoreUsageAverage,
		CPUCost:                        a.CPUCost,
		CPUCostIdle:                    a.CPUCostIdle,
		CPUCostAdjustment:              a.CPUCostAdjustment,
		GPUHours:                       a.GPUHours,
		deprecatedGPURequestAverage:    a.deprecatedGPURequestAverage,
		deprecatedGPUUsageAverage:      a.deprecatedGPUUsageAverage,
		GPUCost:                        a.GPUCost,
		GPUCostIdle:                    a.GPUCostIdle,
		GPUCostAdjustment:              a.GPUCostAdjustment,
		NetworkTransferBytes:           a.NetworkTransferBytes,
		NetworkReceiveBytes:            a.NetworkReceiveBytes,
		NetworkCost:                    a.NetworkCost,
		NetworkCrossZoneCost:           a.NetworkCrossZoneCost,
		NetworkCrossRegionCost:         a.NetworkCrossRegionCost,
		NetworkInternetCost:            a.NetworkInternetCost,
		NetworkCostAdjustment:          a.NetworkCostAdjustment,
		LoadBalancerCost:               a.LoadBalancerCost,
		LoadBalancerCostAdjustment:     a.LoadBalancerCostAdjustment,
		PVs:                            a.PVs.Clone(),
		PVCostAdjustment:               a.PVCostAdjustment,
		RAMByteHours:                   a.RAMByteHours,
		RAMBytesRequestAverage:         a.RAMBytesRequestAverage,
		RAMBytesUsageAverage:           a.RAMBytesUsageAverage,
		RAMCost:                        a.RAMCost,
		RAMCostIdle:                    a.RAMCostIdle,
		RAMCostAdjustment:              a.RAMCostAdjustment,
		SharedCost:                     a.SharedCost,
		ExternalCost:                   a.ExternalCost,
		RawAllocationOnly:              a.RawAllocationOnly.Clone(),
		ProportionalAssetResourceCosts: a.ProportionalAssetResourceCosts.Clone(),
		SharedCostBreakdown:            a.SharedCostBreakdown.Clone(),
		LoadBalancers:                  a.LoadBalancers.Clone(),
		UnmountedPVCost:                a.UnmountedPVCost,
		GPUAllocation:                  a.GPUAllocation.Clone(),
	}
}

// Equal returns true if the values held in the given Allocation precisely
// match those of the receiving Allocation. nil does not match nil. Floating
// point values need to match according to util.IsApproximately, which accounts
// for small, reasonable floating point error margins.
func (a *Allocation) Equal(that *Allocation) bool {
	if a == nil || that == nil {
		return false
	}

	if a.Name != that.Name {
		return false
	}
	if !a.Properties.Equal(that.Properties) {
		return false
	}
	if !a.Window.Equal(that.Window) {
		return false
	}
	if !a.Start.Equal(that.Start) {
		return false
	}
	if !a.End.Equal(that.End) {
		return false
	}
	if !util.IsApproximately(a.CPUCoreHours, that.CPUCoreHours) {
		return false
	}
	if !util.IsApproximately(a.CPUCost, that.CPUCost) {
		return false
	}
	if !util.IsApproximately(a.CPUCostIdle, that.CPUCostIdle) {
		return false
	}
	if !util.IsApproximately(a.CPUCostAdjustment, that.CPUCostAdjustment) {
		return false
	}
	if !util.IsApproximately(a.GPUHours, that.GPUHours) {
		return false
	}
	if !util.IsApproximately(a.GPUCost, that.GPUCost) {
		return false
	}
	if !util.IsApproximately(a.GPUCostIdle, that.GPUCostIdle) {
		return false
	}
	if !util.IsApproximately(a.GPUCostAdjustment, that.GPUCostAdjustment) {
		return false
	}
	if !util.IsApproximately(a.NetworkTransferBytes, that.NetworkTransferBytes) {
		return false
	}
	if !util.IsApproximately(a.NetworkReceiveBytes, that.NetworkReceiveBytes) {
		return false
	}
	if !util.IsApproximately(a.NetworkCost, that.NetworkCost) {
		return false
	}
	if !util.IsApproximately(a.NetworkCrossZoneCost, that.NetworkCrossZoneCost) {
		return false
	}
	if !util.IsApproximately(a.NetworkCrossRegionCost, that.NetworkCrossRegionCost) {
		return false
	}
	if !util.IsApproximately(a.NetworkInternetCost, that.NetworkInternetCost) {
		return false
	}
	if !util.IsApproximately(a.NetworkCostAdjustment, that.NetworkCostAdjustment) {
		return false
	}
	if !util.IsApproximately(a.LoadBalancerCost, that.LoadBalancerCost) {
		return false
	}
	if !util.IsApproximately(a.LoadBalancerCostAdjustment, that.LoadBalancerCostAdjustment) {
		return false
	}
	if !util.IsApproximately(a.PVCostAdjustment, that.PVCostAdjustment) {
		return false
	}
	if !util.IsApproximately(a.RAMByteHours, that.RAMByteHours) {
		return false
	}
	if !util.IsApproximately(a.RAMCost, that.RAMCost) {
		return false
	}
	if !util.IsApproximately(a.RAMCostIdle, that.RAMCostIdle) {
		return false
	}
	if !util.IsApproximately(a.RAMCostAdjustment, that.RAMCostAdjustment) {
		return false
	}
	if !util.IsApproximately(a.SharedCost, that.SharedCost) {
		return false
	}
	if !util.IsApproximately(a.ExternalCost, that.ExternalCost) {
		return false
	}

	if !a.RawAllocationOnly.Equal(that.RawAllocationOnly) {
		return false
	}

	if !a.PVs.Equal(that.PVs) {
		return false
	}

	if !util.IsApproximately(a.UnmountedPVCost, that.UnmountedPVCost) {
		return false
	}

	if !a.GPUAllocation.Equal(that.GPUAllocation) {
		return false
	}

	return true
}

// TotalCost is the total cost of the Allocation including adjustments
func (a *Allocation) TotalCost() float64 {
	if a == nil {
		return 0.0
	}

	return a.CPUTotalCost() + a.GPUTotalCost() + a.RAMTotalCost() + a.PVTotalCost() + a.NetworkTotalCost() + a.LBTotalCost() + a.SharedTotalCost() + a.ExternalCost
}

// CPUTotalCost calculates total CPU cost of Allocation including adjustment
func (a *Allocation) CPUTotalCost() float64 {
	if a == nil {
		return 0.0
	}

	return a.CPUCost + a.CPUCostAdjustment
}

// GPUTotalCost calculates total GPU cost of Allocation including adjustment
func (a *Allocation) GPUTotalCost() float64 {
	if a == nil {
		return 0.0
	}

	return a.GPUCost + a.GPUCostAdjustment
}

// RAMTotalCost calculates total RAM cost of Allocation including adjustment
func (a *Allocation) RAMTotalCost() float64 {
	if a == nil {
		return 0.0
	}

	return a.RAMCost + a.RAMCostAdjustment
}

// PVTotalCost calculates total PV cost of Allocation including adjustment
func (a *Allocation) PVTotalCost() float64 {
	if a == nil {
		return 0.0
	}

	return a.PVCost() + a.PVCostAdjustment
}

// NetworkTotalCost calculates total Network cost of Allocation including adjustment
func (a *Allocation) NetworkTotalCost() float64 {
	if a == nil {
		return 0.0
	}

	return a.NetworkCost + a.NetworkCostAdjustment
}

// LBTotalCost calculates total LB cost of Allocation including adjustment
// TODO deprecate
func (a *Allocation) LBTotalCost() float64 {
	return a.LoadBalancerTotalCost()
}

// LoadBalancerTotalCost calculates total LB cost of Allocation including adjustment
func (a *Allocation) LoadBalancerTotalCost() float64 {
	if a == nil {
		return 0.0
	}

	return a.LoadBalancerCost + a.LoadBalancerCostAdjustment
}

// SharedTotalCost calculates total shared cost of Allocation including adjustment
func (a *Allocation) SharedTotalCost() float64 {
	if a == nil {
		return 0.0
	}

	return a.SharedCost
}

// PVCost calculate cumulative cost of all PVs that Allocation is attached to
func (a *Allocation) PVCost() float64 {
	if a == nil {
		return 0.0
	}

	cost := 0.0
	for _, pv := range a.PVs {
		cost += pv.Cost
	}
	return cost
}

// PVByteHours calculate cumulative ByteHours of all PVs that Allocation is attached to
func (a *Allocation) PVByteHours() float64 {
	if a == nil {
		return 0.0
	}

	byteHours := 0.0
	for _, pv := range a.PVs {
		byteHours += pv.ByteHours
	}
	return byteHours
}

// CPUEfficiency is the ratio of usage to request. If there is no request and
// no usage or cost, then efficiency is zero. If there is no request, but there
// is usage or cost, then efficiency is 100%.
func (a *Allocation) CPUEfficiency() float64 {
	if a == nil {
		return 0.0
	}

	if a.CPUCoreRequestAverage > 0 {
		return a.CPUCoreUsageAverage / a.CPUCoreRequestAverage
	}

	if a.CPUCoreUsageAverage == 0.0 || a.CPUTotalCost() == 0.0 {
		return 0.0
	}

	return 1.0
}

// RAMEfficiency is the ratio of usage to request. If there is no request and
// no usage or cost, then efficiency is zero. If there is no request, but there
// is usage or cost, then efficiency is 100%.
func (a *Allocation) RAMEfficiency() float64 {
	if a == nil {
		return 0.0
	}

	if a.RAMBytesRequestAverage > 0 {
		return a.RAMBytesUsageAverage / a.RAMBytesRequestAverage
	}

	if a.RAMBytesUsageAverage == 0.0 || a.RAMTotalCost() == 0.0 {
		return 0.0
	}

	return 1.0
}

// GPUEfficiency is the ratio of usage to request. Note that, without the NVIDIA
// DCGM exporter providing Prometheus with usage metrics, this will always be
// zero, as deprecatedGPUUsageAverage will be zero (the default value).
func (a *Allocation) GPUEfficiency() float64 {
	if a == nil {
		return 0.0
	}
	if a.GPUAllocation == nil {
		return 0.0
	}

	if a.GPUAllocation.GPURequestAverage == nil || a.GPUAllocation.GPUUsageAverage == nil {
		return 0.0
	}

	if *a.GPUAllocation.GPURequestAverage > 0 && *a.GPUAllocation.GPUUsageAverage > 0 {
		return *a.GPUAllocation.GPUUsageAverage / *a.GPUAllocation.GPURequestAverage
	}

	if *a.GPUAllocation.GPURequestAverage == 0.0 || a.GPUTotalCost() == 0.0 {
		return 0.0
	}

	return 1.0
}

// TotalEfficiency is the cost-weighted average of CPU and RAM efficiency. If
// there is no cost at all, then efficiency is zero.
func (a *Allocation) TotalEfficiency() float64 {
	if a == nil {
		return 0.0
	}

	if a.RAMTotalCost()+a.CPUTotalCost() > 0 {
		ramCostEff := a.RAMEfficiency() * a.RAMTotalCost()
		cpuCostEff := a.CPUEfficiency() * a.CPUTotalCost()
		return (ramCostEff + cpuCostEff) / (a.CPUTotalCost() + a.RAMTotalCost())
	}

	return 0.0
}

// CPUCores converts the Allocation's CPUCoreHours into average CPUCores
func (a *Allocation) CPUCores() float64 {
	if a.Minutes() <= 0.0 {
		return 0.0
	}
	return a.CPUCoreHours / (a.Minutes() / 60.0)
}

// RAMBytes converts the Allocation's RAMByteHours into average RAMBytes
func (a *Allocation) RAMBytes() float64 {
	if a.Minutes() <= 0.0 {
		return 0.0
	}
	return a.RAMByteHours / (a.Minutes() / 60.0)
}

// GPUs converts the Allocation's GPUHours into average GPUs
func (a *Allocation) GPUs() float64 {
	if a.Minutes() <= 0.0 {
		return 0.0
	}
	return a.GPUHours / (a.Minutes() / 60.0)
}

// PVBytes converts the Allocation's PVByteHours into average PVBytes
func (a *Allocation) PVBytes() float64 {
	if a.Minutes() <= 0.0 {
		return 0.0
	}
	return a.PVByteHours() / (a.Minutes() / 60.0)
}

// ResetAdjustments sets all cost adjustment fields to zero
func (a *Allocation) ResetAdjustments() {
	if a == nil {
		return
	}

	a.CPUCostAdjustment = 0.0
	a.GPUCostAdjustment = 0.0
	a.RAMCostAdjustment = 0.0
	a.PVCostAdjustment = 0.0
	a.NetworkCostAdjustment = 0.0
	a.LoadBalancerCostAdjustment = 0.0
}

// Resolution returns the duration of time covered by the Allocation
func (a *Allocation) Resolution() time.Duration {
	return a.End.Sub(a.Start)
}

// IsAggregated is true if the given Allocation has been aggregated, which we
// define by a lack of AllocationProperties.
func (a *Allocation) IsAggregated() bool {
	return a == nil || a.Properties == nil
}

// IsExternal is true if the given Allocation represents external costs.
func (a *Allocation) IsExternal() bool {
	if a == nil {
		return false
	}

	return strings.Contains(a.Name, ExternalSuffix)
}

// IsIdle is true if the given Allocation represents idle costs.
func (a *Allocation) IsIdle() bool {
	if a == nil {
		return false
	}

	return strings.Contains(a.Name, IdleSuffix)
}

// IsUnallocated is true if the given Allocation represents unallocated costs.
func (a *Allocation) IsUnallocated() bool {
	if a == nil {
		return false
	}

	return strings.Contains(a.Name, UnallocatedSuffix)
}

// IsUnmounted is true if the given Allocation represents unmounted volume costs.
func (a *Allocation) IsUnmounted() bool {
	if a == nil {
		return false
	}

	return strings.Contains(a.Name, UnmountedSuffix)
}

// Minutes returns the number of minutes the Allocation represents, as defined
// by the difference between the end and start times.
func (a *Allocation) Minutes() float64 {
	if a == nil {
		return 0.0
	}

	return a.End.Sub(a.Start).Minutes()
}

// SetUnmountedPVCost determines if the Allocation is unmounted and, if so, it
// sets the UnmountedPVCost field appropriately.
func (a *Allocation) SetUnmountedPVCost() float64 {
	if a == nil {
		return 0.0
	}

	if a.IsUnmounted() {
		a.UnmountedPVCost = a.PVTotalCost()
		return a.UnmountedPVCost
	}

	return 0.0
}

// Share adds the TotalCost of the given Allocation to the SharedCost of the
// receiving Allocation. No Start, End, Window, or AllocationProperties are considered.
// Neither Allocation is mutated; a new Allocation is always returned.
func (a *Allocation) Share(that *Allocation) (*Allocation, error) {
	if that == nil {
		return a.Clone(), nil
	}

	if a == nil {
		return nil, fmt.Errorf("cannot share with nil Allocation")
	}

	agg := a.Clone()
	agg.SharedCost += that.TotalCost()

	return agg, nil
}

// String represents the given Allocation as a string
func (a *Allocation) String() string {
	if a == nil {
		return "<nil>"
	}

	return fmt.Sprintf("%s%s=%.2f", a.Name, NewWindow(&a.Start, &a.End), a.TotalCost())
}

func (a *Allocation) add(that *Allocation) {
	if a == nil {
		log.Warnf("Allocation.AggregateBy: trying to add a nil receiver")
		return
	}

	// Generate keys for each allocation to allow for special logic to set the controller
	// in the case of keys matching but controllers not matching.
	aggByForKey := []string{"cluster", "node", "namespace", "pod", "container"}
	leftKey := a.generateKey(aggByForKey, nil)
	rightKey := that.generateKey(aggByForKey, nil)
	leftProperties := a.Properties
	rightProperties := that.Properties

	// Preserve string properties that are matching between the two allocations
	a.Properties = a.Properties.Intersection(that.Properties)

	// If both Allocations have ProportionalAssetResourceCosts, then
	// add those from the given Allocation into the receiver.
	if a.ProportionalAssetResourceCosts != nil || that.ProportionalAssetResourceCosts != nil {
		// init empty PARCs if either operand has nil PARCs
		if a.ProportionalAssetResourceCosts == nil {
			a.ProportionalAssetResourceCosts = ProportionalAssetResourceCosts{}
		}
		if that.ProportionalAssetResourceCosts == nil {
			that.ProportionalAssetResourceCosts = ProportionalAssetResourceCosts{}
		}
		a.ProportionalAssetResourceCosts.Add(that.ProportionalAssetResourceCosts)
	}

	// If both Allocations have SharedCostBreakdowns, then
	// add those from the given Allocation into the receiver.
	if a.SharedCostBreakdown != nil || that.SharedCostBreakdown != nil {
		if a.SharedCostBreakdown == nil {
			a.SharedCostBreakdown = SharedCostBreakdowns{}
		}
		if that.SharedCostBreakdown == nil {
			that.SharedCostBreakdown = SharedCostBreakdowns{}
		}
		a.SharedCostBreakdown.Add(that.SharedCostBreakdown)
	}

	// Overwrite regular intersection logic for the controller name property in the
	// case that the Allocation keys are the same but the controllers are not.
	if leftKey == rightKey &&
		leftProperties != nil &&
		rightProperties != nil &&
		leftProperties.Controller != rightProperties.Controller {
		if leftProperties.Controller == "" {
			a.Properties.Controller = rightProperties.Controller
		} else if rightProperties.Controller == "" {
			a.Properties.Controller = leftProperties.Controller
		} else {
			controllers := []string{
				leftProperties.Controller,
				rightProperties.Controller,
			}
			sort.Strings(controllers)
			a.Properties.Controller = controllers[0]
		}
	}

	// Expand the window to encompass both Allocations
	a.Window = a.Window.Expand(that.Window)

	// Sum non-cumulative fields by turning them into cumulative, adding them,
	// and then converting them back into averages after minutes have been
	// combined (just below).
	cpuReqCoreMins := a.CPUCoreRequestAverage * a.Minutes()
	cpuReqCoreMins += that.CPUCoreRequestAverage * that.Minutes()

	cpuUseCoreMins := a.CPUCoreUsageAverage * a.Minutes()
	cpuUseCoreMins += that.CPUCoreUsageAverage * that.Minutes()

	ramReqByteMins := a.RAMBytesRequestAverage * a.Minutes()
	ramReqByteMins += that.RAMBytesRequestAverage * that.Minutes()

	ramUseByteMins := a.RAMBytesUsageAverage * a.Minutes()
	ramUseByteMins += that.RAMBytesUsageAverage * that.Minutes()

	var gpuReqMins *float64 = nil
	if a.GPUAllocation != nil && a.GPUAllocation.GPURequestAverage != nil {
		result := *a.GPUAllocation.GPURequestAverage * a.Minutes()
		gpuReqMins = &result
	}

	if that.GPUAllocation != nil && that.GPUAllocation.GPURequestAverage != nil {
		if gpuReqMins == nil {
			result := *that.GPUAllocation.GPURequestAverage * that.Minutes()
			gpuReqMins = &result
		} else {
			result := *gpuReqMins + *that.GPUAllocation.GPURequestAverage*that.Minutes()
			gpuReqMins = &result
		}
	}

	var gpuUseMins *float64 = nil
	if a.GPUAllocation != nil && a.GPUAllocation.GPUUsageAverage != nil {
		result := *a.GPUAllocation.GPUUsageAverage * a.Minutes()
		gpuUseMins = &result
	}

	if that.GPUAllocation != nil && that.GPUAllocation.GPUUsageAverage != nil {
		if gpuUseMins == nil {
			result := *that.GPUAllocation.GPUUsageAverage * that.Minutes()
			gpuUseMins = &result
		} else {
			result := *gpuUseMins + *that.GPUAllocation.GPUUsageAverage*that.Minutes()
			gpuUseMins = &result
		}
	}

	// Expand Start and End to be the "max" of among the given Allocations
	if that.Start.Before(a.Start) {
		a.Start = that.Start
	}
	if that.End.After(a.End) {
		a.End = that.End
	}

	// Convert cumulative request and usage back into rates
	// TODO:TEST write a unit test that fails if this is done incorrectly
	if a.Minutes() > 0 {
		a.CPUCoreRequestAverage = cpuReqCoreMins / a.Minutes()
		a.CPUCoreUsageAverage = cpuUseCoreMins / a.Minutes()
		a.RAMBytesRequestAverage = ramReqByteMins / a.Minutes()
		a.RAMBytesUsageAverage = ramUseByteMins / a.Minutes()

		if a.GPUAllocation != nil {
			if gpuReqMins != nil {
				gpuReqMinsRes := *gpuReqMins / a.Minutes()
				a.GPUAllocation.GPURequestAverage = &gpuReqMinsRes
			} else {
				a.GPUAllocation.GPURequestAverage = nil
			}

			if gpuUseMins != nil {
				gpuUsageMinsRes := *gpuUseMins / a.Minutes()
				a.GPUAllocation.GPUUsageAverage = &gpuUsageMinsRes
			} else {
				a.GPUAllocation.GPUUsageAverage = nil
			}
		}
	} else {
		a.CPUCoreRequestAverage = 0.0
		a.CPUCoreUsageAverage = 0.0
		a.RAMBytesRequestAverage = 0.0
		a.RAMBytesUsageAverage = 0.0

		if a.GPUAllocation != nil {
			a.GPUAllocation.GPURequestAverage = nil
			a.GPUAllocation.GPUUsageAverage = nil
		}
	}

	// Sum all cumulative resource fields
	a.CPUCoreHours += that.CPUCoreHours
	a.GPUHours += that.GPUHours
	a.RAMByteHours += that.RAMByteHours
	a.NetworkTransferBytes += that.NetworkTransferBytes
	a.NetworkReceiveBytes += that.NetworkReceiveBytes

	// Sum all cumulative cost fields
	a.CPUCost += that.CPUCost
	a.GPUCost += that.GPUCost
	a.RAMCost += that.RAMCost
	a.CPUCostIdle += that.CPUCostIdle
	a.GPUCostIdle += that.GPUCostIdle
	a.RAMCostIdle += that.RAMCostIdle
	a.NetworkCost += that.NetworkCost
	a.NetworkCrossZoneCost += that.NetworkCrossZoneCost
	a.NetworkCrossRegionCost += that.NetworkCrossRegionCost
	a.NetworkInternetCost += that.NetworkInternetCost
	a.LoadBalancerCost += that.LoadBalancerCost
	a.SharedCost += that.SharedCost
	a.ExternalCost += that.ExternalCost
	a.UnmountedPVCost += that.UnmountedPVCost

	// Sum PVAllocations
	a.PVs = a.PVs.Add(that.PVs)

	// Sum all cumulative adjustment fields
	a.CPUCostAdjustment += that.CPUCostAdjustment
	a.RAMCostAdjustment += that.RAMCostAdjustment
	a.GPUCostAdjustment += that.GPUCostAdjustment
	a.PVCostAdjustment += that.PVCostAdjustment
	a.NetworkCostAdjustment += that.NetworkCostAdjustment
	a.LoadBalancerCostAdjustment += that.LoadBalancerCostAdjustment

	// Sum LoadBalancer Allocations
	a.LoadBalancers = a.LoadBalancers.Add(that.LoadBalancers)

	// Any data that is in a "raw allocation only" is not valid in any
	// sort of cumulative Allocation (like one that is added).
	a.RawAllocationOnly = nil
}

func (thisLbAllocs LbAllocations) Add(thatLbAllocs LbAllocations) LbAllocations {
	// loop through both sets of LB allocations, building a new LBAllocations that has the summed set
	mergedLbAllocs := thisLbAllocs.Clone()
	if thatLbAllocs != nil {
		if mergedLbAllocs == nil {
			mergedLbAllocs = LbAllocations{}
		}
		for lbKey, thatlbAlloc := range thatLbAllocs {
			thisLbAlloc, ok := mergedLbAllocs[lbKey]
			if !ok {
				thisLbAlloc = &LbAllocation{
					Service: thatlbAlloc.Service,
					Cost:    thatlbAlloc.Cost,
					Hours:   thatlbAlloc.Hours,
				}
				mergedLbAllocs[lbKey] = thisLbAlloc
			} else {
				thisLbAlloc.Cost += thatlbAlloc.Cost
				thisLbAlloc.Hours += thatlbAlloc.Hours
			}

		}
	}
	return mergedLbAllocs
}

func (thisLbAllocs LbAllocations) SanitizeNaN() {
	for _, lba := range thisLbAllocs {
		lba.SanitizeNaN()
	}
}

// AllocationSet stores a set of Allocations, each with a unique name, that share
// a window. An AllocationSet is mutable, so treat it like a threadsafe map.
type AllocationSet struct {
	Allocations  map[string]*Allocation
	ExternalKeys map[string]bool
	IdleKeys     map[string]bool
	FromSource   string // stores the name of the source used to compute the data
	Window       Window
	Warnings     []string
	Errors       []string
}

// NewAllocationSet instantiates a new AllocationSet and, optionally, inserts
// the given list of Allocations
func NewAllocationSet(start, end time.Time, allocs ...*Allocation) *AllocationSet {
	as := &AllocationSet{
		Allocations:  map[string]*Allocation{},
		ExternalKeys: map[string]bool{},
		IdleKeys:     map[string]bool{},
		Window:       NewWindow(&start, &end),
	}

	for _, a := range allocs {
		as.Insert(a)
	}

	return as
}

// AllocationAggregationOptions provide advanced functionality to AggregateBy, including
// filtering results and sharing allocations. FilterFuncs are a list of match
// functions such that, if any function fails, the allocation is ignored.
// ShareFuncs are a list of match functions such that, if any function
// succeeds, the allocation is marked as a shared resource. ShareIdle is a
// simple flag for sharing idle resources.
type AllocationAggregationOptions struct {
	AllocationTotalsStore                 AllocationTotalsStore
	Filter                                filter.Filter
	IdleByNode                            bool
	IncludeProportionalAssetResourceCosts bool
	LabelConfig                           *LabelConfig
	MergeUnallocated                      bool
	Reconcile                             bool
	ReconcileNetwork                      bool
	Share                                 filter.Filter
	SharedNamespaces                      []string
	SharedLabels                          map[string][]string
	ShareIdle                             string
	ShareSplit                            string
	SharedHourlyCosts                     map[string]float64
	IncludeSharedCostBreakdown            bool
	SplitIdle                             bool
	IncludeAggregatedMetadata             bool
}

func isFilterEmpty(filter AllocationMatcher) bool {
	if _, isAllPass := filter.(*matcher.AllPass[*Allocation]); isAllPass {
		return true
	}
	return false
}

// AggregateBy aggregates the Allocations in the given AllocationSet by the given
// AllocationProperty. This will only be legal if the AllocationSet is divisible by the
// given AllocationProperty; e.g. Containers can be divided by Namespace, but not vice-a-versa.
func (as *AllocationSet) AggregateBy(aggregateBy []string, options *AllocationAggregationOptions) error {
	// The order of operations for aggregating allocations is as follows:
	//
	//  1. Partition external, idle, and shared allocations into separate sets.
	//     Also, create the aggSet into which the results will be aggregated.
	//
	//  2. Compute sharing coefficients for idle and shared resources
	//     a) if idle allocation is to be shared, or if proportional asset
	//        resource costs are to be included, then compute idle coefficients
	//        (proportional asset resource costs are derived from idle coefficients)
	//     b) if proportional asset costs are to be included, derive them from
	//        idle coefficients and add them to the allocations.
	//     c) if idle allocation is NOT shared, but filters are present, compute
	//        idle filtration coefficients for the purpose of only returning the
	//        portion of idle allocation that would have been shared with the
	//        unfiltered results. (See unit tests 5.a,b,c)
	//     d) generate shared allocation for them given shared overhead, which
	//        must happen after (2a) and (2b)
	//     e) if there are shared resources, compute share coefficients
	//
	//  3. Drop any allocation that fails any of the filters
	//
	//  4. Distribute idle allocations according to the idle coefficients
	//
	//  5. Generate aggregation key and insert allocation into the output set
	//
	//  6. If idle is shared and resources are shared, some idle might be shared
	//     with a shared resource. Distribute that to the shared resources
	//     prior to sharing them with the aggregated results.
	//
	//  7. Apply idle filtration coefficients from step (2b)
	//
	//  8. Distribute shared allocations according to the share coefficients.
	//
	//  9. If there are external allocations that can be aggregated into
	//     the output (i.e. they can be used to generate a valid key for
	//     the given properties) then aggregate; otherwise... ignore them?
	//
	// 10. Distribute any undistributed idle, in the case that idle
	//     coefficients end up being zero and some idle is not shared.
	//
	// 11. If the merge idle option is enabled, merge any remaining idle
	//     allocations into a single idle allocation. If there was any idle
	//	   whose costs were not distributed because there was no usage of a
	//     specific resource type, re-add the idle to the aggregation with
	//     only that type.

	if as.IsEmpty() {
		return nil
	}

	if options == nil {
		options = &AllocationAggregationOptions{}
	}

	if options.LabelConfig == nil {
		options.LabelConfig = NewLabelConfig()
	}

	// idleFiltrationCoefficients relies on this being explicitly set
	if options.ShareIdle != ShareWeighted {
		options.ShareIdle = ShareNone
	}

	var filter AllocationMatcher
	if options.Filter == nil {
		filter = &matcher.AllPass[*Allocation]{}
	} else {
		compiler := NewAllocationMatchCompiler(options.LabelConfig)
		var err error
		filter, err = compiler.Compile(options.Filter)
		if err != nil {
			return fmt.Errorf("compiling filter '%s': %w", ast.ToPreOrderShortString(options.Filter), err)
		}
	}
	if filter == nil {
		return fmt.Errorf("unexpected nil filter")
	}

	var sharer AllocationMatcher
	if options.Share != nil {
		compiler := NewAllocationMatchCompiler(options.LabelConfig)
		var err error
		sharer, err = compiler.Compile(options.Share)
		if err != nil {
			return fmt.Errorf("compiling sharer '%s': %w", ast.ToPreOrderShortString(options.Filter), err)
		}
	}

	var allocatedTotalsMap map[string]map[string]float64

	// If aggregateBy is nil, we don't aggregate anything. On the other hand,
	// an empty slice implies that we should aggregate everything. See
	// generateKey for why that makes sense.
	shouldAggregate := aggregateBy != nil
	shouldFilter := !isFilterEmpty(filter)
	shouldShare := len(options.SharedHourlyCosts) > 0 || sharer != nil
	if !shouldAggregate && !shouldFilter && !shouldShare && options.ShareIdle == ShareNone && !options.IncludeProportionalAssetResourceCosts {
		// There is nothing for AggregateBy to do, so simply return nil
		// before returning, set aggregated metadata inclusion in properties
		if options.IncludeAggregatedMetadata {
			for index := range as.Allocations {
				as.Allocations[index].Properties.AggregatedMetadata = true
			}
		}
		return nil
	}

	// aggSet will collect the aggregated allocations
	aggSet := &AllocationSet{
		Window: as.Window.Clone(),
	}

	// externalSet will collect external allocations
	externalSet := &AllocationSet{
		Window: as.Window.Clone(),
	}

	// idleSet will be shared among aggSet after initial aggregation
	// is complete
	idleSet := &AllocationSet{
		Window: as.Window.Clone(),
	}

	// parcSet is used to compute proportionalAssetResourceCosts
	// for surfacing in the API
	parcSet := &AllocationSet{
		Window: as.Window.Clone(),
	}

	// shareSet will be shared among aggSet after initial aggregation
	// is complete
	shareSet := &AllocationSet{
		Window: as.Window.Clone(),
	}

	// (1) Loop and find all of the external, idle, and shared allocations. Add
	// them to their respective sets, removing them from the set of allocations
	// to aggregate.
	for _, alloc := range as.Allocations {
		alloc.Properties.AggregatedMetadata = options.IncludeAggregatedMetadata
		// build a parallel set of allocations to only be used
		// for computing PARCs
		if options.IncludeProportionalAssetResourceCosts {
			parcSet.Insert(alloc.Clone())
		}

		// External allocations get aggregated post-hoc (see step 6) and do
		// not necessarily contain complete sets of properties, so they are
		// moved to a separate AllocationSet.
		if alloc.IsExternal() {
			delete(as.ExternalKeys, alloc.Name)
			delete(as.Allocations, alloc.Name)
			externalSet.Insert(alloc)
			continue
		}

		// Idle allocations should be separated into idleSet if they are to be
		// shared later on. If they are not to be shared, then add them to the
		// aggSet like any other allocation.
		if alloc.IsIdle() {
			delete(as.IdleKeys, alloc.Name)
			delete(as.Allocations, alloc.Name)

			if options.ShareIdle == ShareEven || options.ShareIdle == ShareWeighted {
				idleSet.Insert(alloc)
			} else {
				aggSet.Insert(alloc)
			}

			continue
		}

		// Shared allocations must be identified and separated prior to
		// aggregation and filtering. That is, if any of the ShareFuncs return
		// true for the allocation, then move it to shareSet.
		if sharer != nil && sharer.Matches(alloc) {
			delete(as.IdleKeys, alloc.Name)
			delete(as.Allocations, alloc.Name)
			shareSet.Insert(alloc)
			continue
		}
	}

	// It's possible that no more un-shared, non-idle, non-external allocations
	// remain at this point. This always results in an emptySet, so return early.
	if len(as.Allocations) == 0 {
		emptySet := &AllocationSet{
			Window: as.Window.Clone(),
		}
		as.Allocations = emptySet.Allocations
		return nil
	}

	// (2) In order to correctly share idle and shared costs, we first compute
	// sharing coefficients, which represent the proportion of each cost to
	// share with each allocation. Idle allocations are shared per-cluster or per-node,
	// per-allocation, and per-resource, while shared resources are shared per-
	// allocation only.
	//
	// For an idleCoefficient example, the entries:
	//   [cluster1][cluster1/namespace1/pod1/container1][cpu] = 0.166667
	//   [cluster1][cluster1/namespace1/pod1/container1][gpu] = 0.166667
	//   [cluster1][cluster1/namespace1/pod1/container1][ram] = 0.687500
	// mean that the allocation "cluster1/namespace1/pod1/container1" will
	// receive 16.67% of cluster1's idle CPU and GPU costs and 68.75% of its
	// RAM costs.
	//
	// For a shareCoefficient example, the entries:
	//   [namespace2] = 0.666667
	//   [__filtered__] = 0.333333
	// mean that the post-aggregation allocation "namespace2" will receive
	// 66.67% of the shared resource costs, while the remaining 33.33% will
	// be filtered out, as they were shared with allocations that did not pass
	// one of the given filters.
	//
	// In order to maintain stable results when multiple operations are being
	// carried out (e.g. sharing idle, sharing resources, and filtering) these
	// coefficients are computed for the full set of allocations prior to
	// adding shared overhead and prior to applying filters.

	var err error

	// (2a) If there are idle costs to be shared, compute the coefficients for
	// sharing them among the non-idle, non-aggregated allocations (including
	// the shared allocations).
	var idleCoefficients map[string]map[string]map[string]float64
	if idleSet.Length() > 0 && options.ShareIdle != ShareNone {
		idleCoefficients, allocatedTotalsMap, err = computeIdleCoeffs(options, as, shareSet)
		if err != nil {
			log.Warnf("AllocationSet.AggregateBy: compute idle coeff: %s", err)
			return fmt.Errorf("error computing idle coefficients: %s", err)
		}
	}

	// (2b) If proportional asset resource costs are to be included, compute them
	// and add them to the allocations.
	if options.IncludeProportionalAssetResourceCosts {
		err := deriveProportionalAssetResourceCosts(options, as, shareSet, parcSet)
		if err != nil {
			log.Debugf("AggregateBy: failed to derive proportional asset resource costs from idle coefficients: %s", err)
			return fmt.Errorf("AggregateBy: failed to derive proportional asset resource costs from idle coefficients: %s", err)
		}
	}

	// (2c) If idle costs are not to be shared, but there are filters, then we
	// need to track the amount of each idle allocation to "filter" in order to
	// maintain parity with the results when idle is shared. That is, we want
	// to return only the idle costs that would have been shared with the given
	// results, even if the filter had not been applied.
	//
	// For example, consider these results from aggregating by namespace with
	// two clusters:
	//
	//  namespace1: 25.00
	//  namespace2: 30.00
	//  namespace3: 15.00
	//  idle:       30.00
	//
	// When we then filter by cluster==cluster1, namespaces 2 and 3 are
	// reduced by the amount that existed on cluster2. Then, idle must also be
	// reduced by the relevant amount:
	//
	//  namespace1: 25.00
	//  namespace2: 15.00
	//  idle:       20.00
	//
	// Note that this can happen for any field, not just cluster, so we again
	// need to track this on a per-cluster or per-node, per-allocation, per-resource basis.
	var idleFiltrationCoefficients map[string]map[string]map[string]float64
	if shouldFilter && options.ShareIdle == ShareNone {
		idleFiltrationCoefficients, _, err = computeIdleCoeffs(options, as, shareSet)
		if err != nil {
			return fmt.Errorf("error computing idle filtration coefficients: %s", err)
		}
	}

	// (2d) Convert SharedHourlyCosts to Allocations in the shareSet. This must
	// come after idle coefficients are computed so that allocations generated
	// by shared overhead do not skew the idle coefficient computation.
	for name, cost := range options.SharedHourlyCosts {
		if cost > 0.0 {
			hours := as.Resolution().Hours()

			// If set ends in the future, adjust hours accordingly
			diff := time.Since(as.End())
			if diff < 0.0 {
				hours += diff.Hours()
			}

			totalSharedCost := cost * hours

			shareSet.Insert(&Allocation{
				Name:       fmt.Sprintf("%s/%s", name, SharedSuffix),
				Start:      as.Start(),
				End:        as.End(),
				SharedCost: totalSharedCost,
				Properties: &AllocationProperties{Cluster: SharedSuffix}, // The allocation needs to belong to a cluster,but it really doesn't matter which one, so just make it clear.
			})
		}
	}

	// (2e) Compute share coefficients for shared resources. These are computed
	// after idle coefficients, and are computed for the aggregated allocations
	// of the main allocation set. See above for details and an example.
	var shareCoefficients map[string]float64
	if shareSet.Length() > 0 {
		shareCoefficients, err = computeShareCoeffs(aggregateBy, options, as)
		if err != nil {
			return fmt.Errorf("error computing share coefficients: %s", err)
		}
	}

	// (3-5) Filter, distribute idle cost, and aggregate (in that order)
	for _, alloc := range as.Allocations {
		idleId, err := alloc.getIdleId(options)
		if err != nil {
			log.DedupedWarningf(3, "AllocationSet.AggregateBy: missing idleId for allocation: %s", alloc.Name)
		}

		// (3) If the allocation does not match the filter, immediately skip the
		// allocation.
		if !filter.Matches(alloc) {
			// If we are tracking idle filtration coefficients, delete the
			// entry corresponding to the filtered allocation. (Deleting the
			// entry will result in that proportional amount being removed
			// from the idle allocation at the end of the process.)
			if idleFiltrationCoefficients != nil {
				if ifcc, ok := idleFiltrationCoefficients[idleId]; ok {
					delete(ifcc, alloc.Name)
				}
			}

			continue
		}

		// (4) Distribute idle allocations according to the idle coefficients
		// NOTE: if idle allocation is off (i.e. ShareIdle == ShareNone) then
		// all idle allocations will be in the aggSet at this point, so idleSet
		// will be empty and we won't enter this block.
		if idleSet.Length() > 0 {
			// Distribute idle allocations by coefficient per-idleId, per-allocation
			for _, idleAlloc := range idleSet.Allocations {
				// Only share idle if the idleId matches; i.e. the allocation
				// is from the same idleId as the idle costs
				iaidleId, err := idleAlloc.getIdleId(options)
				if err != nil {
					log.Errorf("AllocationSet.AggregateBy: Idle allocation is missing idleId %s", idleAlloc.Name)
					return err
				}

				if iaidleId != idleId {
					continue
				}

				// Make sure idle coefficients exist
				if _, ok := idleCoefficients[idleId]; !ok {
					log.Warnf("AllocationSet.AggregateBy: error getting idle coefficient: no idleId '%s' for '%s'", idleId, alloc.Name)
					continue
				}
				if _, ok := idleCoefficients[idleId][alloc.Name]; !ok {
					log.Warnf("AllocationSet.AggregateBy: error getting idle coefficient for '%s'", alloc.Name)
					continue
				}

				alloc.CPUCoreHours += idleAlloc.CPUCoreHours * idleCoefficients[idleId][alloc.Name]["cpu"]
				alloc.GPUHours += idleAlloc.GPUHours * idleCoefficients[idleId][alloc.Name]["gpu"]
				alloc.RAMByteHours += idleAlloc.RAMByteHours * idleCoefficients[idleId][alloc.Name]["ram"]

				idleCPUCost := idleAlloc.CPUCost * idleCoefficients[idleId][alloc.Name]["cpu"]
				idleGPUCost := idleAlloc.GPUCost * idleCoefficients[idleId][alloc.Name]["gpu"]
				idleRAMCost := idleAlloc.RAMCost * idleCoefficients[idleId][alloc.Name]["ram"]
				alloc.CPUCost += idleCPUCost
				alloc.GPUCost += idleGPUCost
				alloc.RAMCost += idleRAMCost
			}
		}

		// (5) generate key to use for aggregation-by-key and allocation name
		key := alloc.generateKey(aggregateBy, options.LabelConfig)

		alloc.Name = key
		if options.MergeUnallocated && alloc.IsUnallocated() {
			alloc.Name = UnallocatedSuffix
		}

		// Inserting the allocation with the generated key for a name will
		// perform the actual basic aggregation step.
		aggSet.Insert(alloc)
	}

	// (6) If idle is shared and resources are shared, it's possible that some
	// amount of idle cost will be shared with a shared resource. Distribute
	// that idle allocation, if it exists, to the respective shared allocations
	// before sharing with the aggregated allocations.
	if idleSet.Length() > 0 && shareSet.Length() > 0 {
		for _, alloc := range shareSet.Allocations {
			idleId, err := alloc.getIdleId(options)
			if err != nil {
				log.DedupedWarningf(3, "AllocationSet.AggregateBy: missing idleId for allocation: %s", alloc.Name)
			}
			// Distribute idle allocations by coefficient per-idleId, per-allocation
			for _, idleAlloc := range idleSet.Allocations {
				// Only share idle if the idleId matches; i.e. the allocation
				// is from the same idleId as the idle costs
				iaidleId, _ := idleAlloc.getIdleId(options)

				if iaidleId != idleId {
					continue
				}

				// Make sure idle coefficients exist
				if _, ok := idleCoefficients[idleId]; !ok {
					log.Warnf("AllocationSet.AggregateBy: error getting idle coefficient: no idleId '%s' for '%s'", idleId, alloc.Name)
					continue
				}
				if _, ok := idleCoefficients[idleId][alloc.Name]; !ok {
					log.Warnf("AllocationSet.AggregateBy: error getting idle coefficient for '%s'", alloc.Name)
					continue
				}

				alloc.CPUCoreHours += idleAlloc.CPUCoreHours * idleCoefficients[idleId][alloc.Name]["cpu"]
				alloc.GPUHours += idleAlloc.GPUHours * idleCoefficients[idleId][alloc.Name]["gpu"]
				alloc.RAMByteHours += idleAlloc.RAMByteHours * idleCoefficients[idleId][alloc.Name]["ram"]

				idleCPUCost := idleAlloc.CPUCost * idleCoefficients[idleId][alloc.Name]["cpu"]
				idleGPUCost := idleAlloc.GPUCost * idleCoefficients[idleId][alloc.Name]["gpu"]
				idleRAMCost := idleAlloc.RAMCost * idleCoefficients[idleId][alloc.Name]["ram"]
				alloc.CPUCost += idleCPUCost
				alloc.GPUCost += idleGPUCost
				alloc.RAMCost += idleRAMCost
			}
		}
	}

	// groupingIdleFiltrationCoeffs is used to track per-resource idle
	// coefficients on a cluster-by-cluster or node-by-node basis depending
	// on the IdleByNode option. It is, essentially, an aggregation of
	// idleFiltrationCoefficients after they have been
	// filtered above (in step 3)
	var groupingIdleFiltrationCoeffs map[string]map[string]float64
	if idleFiltrationCoefficients != nil {
		groupingIdleFiltrationCoeffs = map[string]map[string]float64{}

		for idleId, m := range idleFiltrationCoefficients {
			if _, ok := groupingIdleFiltrationCoeffs[idleId]; !ok {
				groupingIdleFiltrationCoeffs[idleId] = map[string]float64{
					"cpu": 0.0,
					"gpu": 0.0,
					"ram": 0.0,
				}
			}

			for _, n := range m {
				for resource, val := range n {
					groupingIdleFiltrationCoeffs[idleId][resource] += val
				}
			}
		}
	}

	// (7) If we have both un-shared idle allocations and idle filtration
	// coefficients then apply those. See step (2b) for an example.
	if len(aggSet.IdleKeys) > 0 && groupingIdleFiltrationCoeffs != nil {
		for idleKey := range aggSet.IdleKeys {
			idleAlloc := aggSet.Get(idleKey)
			iaidleId, err := idleAlloc.getIdleId(options)
			if err != nil {
				log.Errorf("AllocationSet.AggregateBy: Idle allocation is missing idleId %s", idleAlloc.Name)
				return err
			}

			if resourceCoeffs, ok := groupingIdleFiltrationCoeffs[iaidleId]; ok {
				idleAlloc.CPUCost *= resourceCoeffs["cpu"]
				idleAlloc.CPUCoreHours *= resourceCoeffs["cpu"]
				idleAlloc.RAMCost *= resourceCoeffs["ram"]
				idleAlloc.RAMByteHours *= resourceCoeffs["ram"]
				idleAlloc.GPUCost *= resourceCoeffs["gpu"]
				idleAlloc.GPUHours *= resourceCoeffs["gpu"]
			}
		}
	}

	// (8) Distribute shared allocations according to the share coefficients.
	if shareSet.Length() > 0 {
		for _, alloc := range aggSet.Allocations {
			for _, sharedAlloc := range shareSet.Allocations {
				if _, ok := shareCoefficients[alloc.Name]; !ok {
					if !alloc.IsIdle() && !alloc.IsUnmounted() {
						log.Warnf("AllocationSet.AggregateBy: error getting share coefficient for '%s'", alloc.Name)
					}
					continue
				}

				if options.IncludeSharedCostBreakdown {
					if alloc.SharedCostBreakdown == nil {
						alloc.SharedCostBreakdown = map[string]SharedCostBreakdown{}
					}

					sharedCostName, err := sharedAlloc.determineSharingName(options)
					if err != nil {
						return fmt.Errorf("failed to group shared costs: %s", err)
					}

					// check if current allocation is a shared flat overhead cost
					if strings.Contains(sharedAlloc.Name, SharedSuffix) {
						sharedCostName = "overheadCost"
					}

					scb := SharedCostBreakdown{
						Name:         sharedCostName,
						TotalCost:    sharedAlloc.TotalCost() * shareCoefficients[alloc.Name],
						CPUCost:      sharedAlloc.CPUTotalCost() * shareCoefficients[alloc.Name],
						GPUCost:      sharedAlloc.GPUTotalCost() * shareCoefficients[alloc.Name],
						RAMCost:      sharedAlloc.RAMTotalCost() * shareCoefficients[alloc.Name],
						PVCost:       sharedAlloc.PVCost() * shareCoefficients[alloc.Name],
						NetworkCost:  sharedAlloc.NetworkTotalCost() * shareCoefficients[alloc.Name],
						LBCost:       sharedAlloc.LBTotalCost() * shareCoefficients[alloc.Name],
						ExternalCost: sharedAlloc.ExternalCost * shareCoefficients[alloc.Name],
					}
					// fmt.Printf("shared cost: %+v", scb)
					alloc.SharedCostBreakdown.Insert(scb)
				}

				alloc.SharedCost += sharedAlloc.TotalCost() * shareCoefficients[alloc.Name]
			}
		}
	}

	// (9) Aggregate external allocations into aggregated allocations. This may
	// not be possible for every external allocation, but attempt to find an
	// exact key match, given each external allocation's proerties, and
	// aggregate if an exact match is found.
	for _, alloc := range externalSet.Allocations {
		if filter.Matches(alloc) {
			key := alloc.generateKey(aggregateBy, options.LabelConfig)

			alloc.Name = key
			aggSet.Insert(alloc)
		}
	}

	// (10) In the edge case that some idle has not been distributed because
	// there is no usage of that resource type, add idle back to
	// aggregations with only that cost applied.

	// E.g. in the case where we have a result that looks like this on the
	// frontend:

	// Name		CPU		GPU		RAM
	// __idle__ $10     $12     $6
	// kubecost $2      $0      $1

	// Sharing idle weighted would result in no idle GPU cost being
	// distributed, because the coefficient for the kubecost GPU cost would
	// be zero. Thus, instead we re-add idle to the aggSet with distributed
	// costs zeroed out but the undistributed costs left in.

	// Name		CPU		GPU		RAM
	// __idle__ $0      $12     $0
	// kubecost $12     $0      $7
	if idleSet.Length() > 0 {
		for _, idleAlloc := range idleSet.Allocations {
			// if the idle does not apply to the non-filtered values, skip it
			if !filter.Matches(idleAlloc) {
				continue
			}

			idleId, err := idleAlloc.getIdleId(options)
			if err != nil {
				log.Errorf("AllocationSet.AggregateBy: idle allocation is missing idleId %s", idleAlloc.Name)
				continue
			}

			hasUndistributableCost := false

			if idleAlloc.CPUCost > 0 && allocatedTotalsMap[idleId]["cpu"] == 0 {
				hasUndistributableCost = true
			} else {
				idleAlloc.CPUCost = 0
			}

			if idleAlloc.GPUCost > 0 && allocatedTotalsMap[idleId]["gpu"] == 0 {
				hasUndistributableCost = true
			} else {
				idleAlloc.GPUCost = 0
			}

			if idleAlloc.RAMCost > 0 && allocatedTotalsMap[idleId]["ram"] == 0 {
				hasUndistributableCost = true
			} else {
				idleAlloc.RAMCost = 0
			}

			if hasUndistributableCost {
				idleAlloc.Name = fmt.Sprintf("%s/%s", idleId, IdleSuffix)
				aggSet.Insert(idleAlloc)
			}
		}
	}

	// (11) Combine all idle allocations into a single "__idle__" allocation
	if !options.SplitIdle {
		for _, idleAlloc := range aggSet.IdleAllocations() {
			aggSet.Delete(idleAlloc.Name)
			idleAlloc.Name = IdleSuffix
			aggSet.Insert(idleAlloc)
		}
	}

	// TODO revisit this (ideally we just remove sharing from this function!)
	// If filters and shared resources and shared idle are all enabled then
	// we will over-count idle by exactly the portion that gets shared with the
	// filtered allocations -- and idle filtration will miss this because it
	// only filters the non-idle filtered costs.
	//
	// Consider the following example, from unit tests:
	// - namespace1     28.000
	// - namespace2     36.000
	// - namespace3     18.000
	// - cluster1/idle  20.000
	// - cluster2/idle  10.000
	//
	// Now, we want to share namespace1, filter namespace2, and share idle:
	//
	// 1. Distribute idle
	//                 ns1     ns2     ns3
	//    non-idle  28.000  36.000  18.000
	//        idle  14.688  10.312   5.000
	//
	// 2. Share namespace1
	//
	//                        ns2     ns3
	//           non-idle  36.000  18.000
	//               idle  10.312   5.000
	//    shared non-idle  18.667   9.333
	//    shared     idle   9.792   4.896 (***)
	//
	// 3. Filter out all but namespace2
	//
	//    ns2 = 36.000 + 10.312 + 18.667 + 9.792 = 74.771
	//
	// So, if we had NOT shared idle, we would expect something like this:
	//
	//    ns2 = 36.000 + 18.667 = 54.667
	//   idle = 10.312 + 9.792  = 20.104
	//
	// But we will instead get this:
	//
	//    ns2 = 36.000 + 18.667 = 54.667
	//   idle = 10.312 + 14.688 = 25.000
	//
	// Which over-shoots idle by 4.896 (***), i.e. precisely the amount of idle
	// cost corresponding to namespace1 AND shared with namespace3. Phew.
	//
	// I originally wanted to fix this, but after 2 days, I'm punting with the
	// recommendation that we rewrite this function soon. Too difficult.
	// - Niko

	as.Allocations = aggSet.Allocations

	return nil
}

func computeShareCoeffs(aggregateBy []string, options *AllocationAggregationOptions, as *AllocationSet) (map[string]float64, error) {
	// Compute coeffs by totalling per-allocation, then dividing by the total.
	coeffs := map[string]float64{}

	// Compute totals for all allocations
	total := 0.0

	// ShareEven counts each aggregation with even weight, whereas ShareWeighted
	// counts each aggregation proportionally to its respective costs
	shareType := options.ShareSplit

	var filter AllocationMatcher
	if options.Filter == nil {
		filter = &matcher.AllPass[*Allocation]{}
	} else {
		compiler := NewAllocationMatchCompiler(options.LabelConfig)
		var err error
		filter, err = compiler.Compile(options.Filter)
		if err != nil {
			return nil, fmt.Errorf("compiling filter '%s': %w", ast.ToPreOrderShortString(options.Filter), err)
		}
	}
	if filter == nil {
		return nil, fmt.Errorf("unexpected nil filter")
	}

	// Record allocation values first, then normalize by totals to get percentages
	for _, alloc := range as.Allocations {
		if alloc.IsIdle() {
			// Skip idle allocations in coefficient calculation
			continue
		}
		if alloc.IsUnmounted() {
			// Skip unmounted allocations in coefficient calculation
			continue
		}

		// Determine the post-aggregation key under which the allocation will
		// be shared.
		name := alloc.generateKey(aggregateBy, options.LabelConfig)

		// If the current allocation will be filtered out in step 3, contribute
		// its share of the shared coefficient to a "__filtered__" bin, which
		// will ultimately be dropped. This step ensures that the shared cost
		// of a non-filtered allocation will be conserved even when the filter
		// is removed. (Otherwise, all the shared cost will get redistributed
		// over the unfiltered results, inflating their shared costs.)
		if !filter.Matches(alloc) {
			name = "__filtered__"
		}

		if shareType == ShareEven {
			// Even distribution is not additive - set to 1.0 for everything
			coeffs[name] = 1.0
			// Total for even distribution is always the number of coefficients
			total = float64(len(coeffs))
		} else {
			// Both are additive for weighted distribution, where each
			// cumulative coefficient will be divided by the total.
			coeffs[name] += alloc.TotalCost() - alloc.SharedCost - alloc.UnmountedPVCost
			total += alloc.TotalCost() - alloc.SharedCost - alloc.UnmountedPVCost
		}
	}

	// Normalize coefficients by totals
	for a := range coeffs {
		if coeffs[a] > 0 && total > 0 {
			coeffs[a] /= total
		} else {
			log.Warnf("ETL: invalid values for shared coefficients: %v, %v", coeffs[a], total)
			coeffs[a] = 0.0
		}
	}

	return coeffs, nil
}

func computeIdleCoeffs(options *AllocationAggregationOptions, as *AllocationSet, shareSet *AllocationSet) (map[string]map[string]map[string]float64, map[string]map[string]float64, error) {
	types := []string{"cpu", "gpu", "ram"}

	// Compute idle coefficients, then save them in AllocationAggregationOptions
	// [idle_id][allocation name][resource] = [coeff]
	coeffs := map[string]map[string]map[string]float64{}

	// Compute totals per resource for CPU, GPU, RAM, and PV
	// [idle_id][resource] = [total]
	totals := map[string]map[string]float64{}

	// Record allocation values first, then normalize by totals to get percentages
	for _, alloc := range as.Allocations {
		if alloc.IsIdle() {
			// Skip idle allocations in coefficient calculation
			continue
		}

		idleId, err := alloc.getIdleId(options)
		if err != nil {
			log.DedupedWarningf(3, "Missing Idle Key for %s", alloc.Name)
		}

		// get the name key for the allocation
		name := alloc.Name

		// Create key based tables if they don't exist
		if _, ok := coeffs[idleId]; !ok {
			coeffs[idleId] = map[string]map[string]float64{}
		}
		if _, ok := totals[idleId]; !ok {
			totals[idleId] = map[string]float64{}
		}

		if _, ok := coeffs[idleId][name]; !ok {
			coeffs[idleId][name] = map[string]float64{}
		}

		coeffs[idleId][name]["cpu"] += alloc.CPUTotalCost()
		coeffs[idleId][name]["gpu"] += alloc.GPUTotalCost()
		coeffs[idleId][name]["ram"] += alloc.RAMTotalCost()

		totals[idleId]["cpu"] += alloc.CPUTotalCost()
		totals[idleId]["gpu"] += alloc.GPUTotalCost()
		totals[idleId]["ram"] += alloc.RAMTotalCost()
	}

	// Do the same for shared allocations
	for _, alloc := range shareSet.Allocations {
		if alloc.IsIdle() {
			// Skip idle allocations in coefficient calculation
			continue
		}

		// idleId will be providerId or cluster
		idleId, err := alloc.getIdleId(options)
		if err != nil {
			log.DedupedWarningf(3, "Missing Idle Key in share set for %s", alloc.Name)
		}

		// get the name key for the allocation
		name := alloc.Name

		// Create idleId based tables if they don't exist
		if _, ok := coeffs[idleId]; !ok {
			coeffs[idleId] = map[string]map[string]float64{}
		}
		if _, ok := totals[idleId]; !ok {
			totals[idleId] = map[string]float64{}
		}

		if _, ok := coeffs[idleId][name]; !ok {
			coeffs[idleId][name] = map[string]float64{}
		}

		coeffs[idleId][name]["cpu"] += alloc.CPUTotalCost()
		coeffs[idleId][name]["gpu"] += alloc.GPUTotalCost()
		coeffs[idleId][name]["ram"] += alloc.RAMTotalCost()

		totals[idleId]["cpu"] += alloc.CPUTotalCost()
		totals[idleId]["gpu"] += alloc.GPUTotalCost()
		totals[idleId]["ram"] += alloc.RAMTotalCost()
	}

	// Normalize coefficients by totals
	for id := range coeffs {
		for a := range coeffs[id] {
			for _, r := range types {
				if coeffs[id][a][r] > 0 && totals[id][r] > 0 {
					coeffs[id][a][r] /= totals[id][r]
				}
			}
		}
	}

	return coeffs, totals, nil
}

func deriveProportionalAssetResourceCosts(options *AllocationAggregationOptions, as *AllocationSet, shareSet, parcsSet *AllocationSet) error {

	// Compute idle coefficients, then save them in AllocationAggregationOptions
	// [idle_id][allocation name][resource] = [coeff]
	coeffs := map[string]map[string]map[string]float64{}

	// Compute totals per resource for CPU, GPU, RAM, and PV
	// [idle_id][resource] = [total]
	totals := map[string]map[string]float64{}

	// Record allocation values first, then normalize by totals to get percentages
	for _, alloc := range parcsSet.Allocations {

		idleId, err := alloc.getIdleId(options)
		if err != nil {
			log.DedupedWarningf(3, "Missing Idle Key for %s", alloc.Name)
		}

		// get the name key for the allocation
		name := alloc.Name

		// Create key based tables if they don't exist
		if _, ok := coeffs[idleId]; !ok {
			coeffs[idleId] = map[string]map[string]float64{}
		}
		if _, ok := totals[idleId]; !ok {
			totals[idleId] = map[string]float64{}
		}

		if _, ok := coeffs[idleId][name]; !ok {
			coeffs[idleId][name] = map[string]float64{}
		}
		// idle IDs for load balancers are their services
		for key := range alloc.LoadBalancers {
			if _, ok := totals[key]; !ok {
				totals[key] = map[string]float64{}
			}

			if _, ok := coeffs[key]; !ok {
				coeffs[key] = map[string]map[string]float64{}
			}
			if _, ok := coeffs[key][name]; !ok {
				coeffs[key][name] = map[string]float64{}
			}

			coeffs[key][name]["loadbalancer"] += alloc.LoadBalancerTotalCost()
			totals[key]["loadbalancer"] += alloc.LoadBalancerTotalCost()
		}

		coeffs[idleId][name]["cpu"] += alloc.CPUTotalCost()
		coeffs[idleId][name]["gpu"] += alloc.GPUTotalCost()
		coeffs[idleId][name]["ram"] += alloc.RAMTotalCost()

		totals[idleId]["cpu"] += alloc.CPUTotalCost()
		totals[idleId]["gpu"] += alloc.GPUTotalCost()
		totals[idleId]["ram"] += alloc.RAMTotalCost()
	}

	// Do the same for shared allocations
	for _, alloc := range shareSet.Allocations {
		if alloc.IsIdle() {
			// Skip idle allocations in coefficient calculation
			continue
		}

		// idleId will be providerId or cluster
		idleId, err := alloc.getIdleId(options)
		if err != nil {
			log.DedupedWarningf(3, "Missing Idle Key in share set for %s", alloc.Name)
		}

		// get the name key for the allocation
		name := alloc.Name

		// Create idleId based tables if they don't exist
		if _, ok := coeffs[idleId]; !ok {
			coeffs[idleId] = map[string]map[string]float64{}
		}
		if _, ok := totals[idleId]; !ok {
			totals[idleId] = map[string]float64{}
		}

		if _, ok := coeffs[idleId][name]; !ok {
			coeffs[idleId][name] = map[string]float64{}
		}

		coeffs[idleId][name]["cpu"] += alloc.CPUTotalCost()
		coeffs[idleId][name]["gpu"] += alloc.GPUTotalCost()
		coeffs[idleId][name]["ram"] += alloc.RAMTotalCost()

		totals[idleId]["cpu"] += alloc.CPUTotalCost()
		totals[idleId]["gpu"] += alloc.GPUTotalCost()
		totals[idleId]["ram"] += alloc.RAMTotalCost()

		// idle IDs for load balancers are their services
		for key := range alloc.LoadBalancers {
			if _, ok := totals[key]; !ok {
				totals[key] = map[string]float64{}
			}

			if _, ok := coeffs[key]; !ok {
				coeffs[key] = map[string]map[string]float64{}
			}
			if _, ok := coeffs[key][name]; !ok {
				coeffs[key][name] = map[string]float64{}
			}
			coeffs[key][name]["loadbalancer"] += alloc.LoadBalancerTotalCost()
			totals[key]["loadbalancer"] += alloc.LoadBalancerTotalCost()
		}

	}

	// after totals are computed, loop through and set parcs on allocations
	for _, alloc := range as.Allocations {
		idleId, err := alloc.getIdleId(options)
		if err != nil {
			log.DedupedWarningf(3, "Missing Idle Key in share set for %s", alloc.Name)
		}

		alloc.ProportionalAssetResourceCosts = ProportionalAssetResourceCosts{}
		alloc.ProportionalAssetResourceCosts.Insert(ProportionalAssetResourceCost{
			Cluster:             alloc.Properties.Cluster,
			Name:                alloc.Properties.Node,
			Type:                "Node",
			ProviderID:          alloc.Properties.ProviderID,
			GPUProportionalCost: coeffs[idleId][alloc.Name]["gpu"],
			CPUProportionalCost: coeffs[idleId][alloc.Name]["cpu"],
			RAMProportionalCost: coeffs[idleId][alloc.Name]["ram"],
		}, options.IdleByNode)
		// insert a separate PARC for the load balancer
		if alloc.LoadBalancerCost != 0 {
			for key, svc := range alloc.LoadBalancers {

				alloc.ProportionalAssetResourceCosts.Insert(ProportionalAssetResourceCost{
					Cluster:                      alloc.Properties.Cluster,
					Name:                         svc.Service,
					Type:                         "LoadBalancer",
					LoadBalancerProportionalCost: coeffs[key][alloc.Name]["loadbalancer"],
				}, options.IdleByNode)
			}
		}

		for name, pvAlloc := range alloc.PVs {
			// insert a separate PARC for each PV attached
			alloc.ProportionalAssetResourceCosts.Insert(ProportionalAssetResourceCost{
				Cluster:            name.Cluster,
				Name:               name.Name,
				Type:               "PV",
				ProviderID:         pvAlloc.ProviderID,
				PVProportionalCost: pvAlloc.Cost,
			}, options.IdleByNode)
		}
	}

	return nil
}

func (a *Allocation) determineSharingName(options *AllocationAggregationOptions) (string, error) {
	if a == nil {
		return "", fmt.Errorf("determineSharingName called on nil Allocation")
	} else if options == nil {
		return "unknown", nil
	}

	// grab SharedLabels keys and sort them, to keep this function deterministic
	var labelKeys []string
	for labelKey := range options.SharedLabels {
		labelKeys = append(labelKeys, labelKey)
	}
	slices.Sort(labelKeys)

	var sharedAggregateBy []string
	var sharedLabels [][]string
	for _, labelKey := range labelKeys {
		sharedAgg := fmt.Sprintf("label:%s", labelKey)
		if !slices.Contains(sharedAggregateBy, sharedAgg) {
			sharedAggregateBy = append(sharedAggregateBy, sharedAgg)
		}
		sharedLabels = append(sharedLabels, options.SharedLabels[labelKey])
	}
	if len(options.SharedNamespaces) > 0 {
		sharedAggregateBy = append(sharedAggregateBy, "namespace")
	}
	sharedCostName := a.generateKey(sharedAggregateBy, options.LabelConfig)

	// get each value in the generated key, then reset the name
	sharedCostNameValues := strings.Split(sharedCostName, "/")
	sharedCostName = ""

	// if we don't have as many values as aggregateBys, something went wrong in generateKey
	if len(sharedCostNameValues) != len(sharedAggregateBy) {
		log.Warnf("Unable to determine share cost group for allocation \"%s\"", a.Name)
	} else {
		// try to match to the first label
		for i, sharedLabelValues := range sharedLabels {
			allocLabel := sharedCostNameValues[i]
			if slices.Contains(sharedLabelValues, allocLabel) {
				return allocLabel, nil
			}
		}

		// if we didn't match to a label, try to match to a namespace
		if len(options.SharedNamespaces) > 0 {
			// namespace will always be the last value, if SharedNamespaces is set
			allocNamespace := sharedCostNameValues[len(sharedCostNameValues)-1]
			if slices.Contains(options.SharedNamespaces, allocNamespace) {
				return allocNamespace, nil
			}
		}

		// if neither the labels nor the namespaces matched, we log a warning and mark this allocation
		// as unknown
		if len(sharedCostName) == 0 {
			log.Warnf("Failed to determine shared cost grouping for allocation \"%s\"", a.Name)
		}
	}

	return "unknown", nil
}

// getIdleId returns the providerId or cluster of an Allocation depending on the IdleByNode
// option in the AllocationAggregationOptions and an error if the respective field is missing
func (a *Allocation) getIdleId(options *AllocationAggregationOptions) (string, error) {
	var idleId string
	if options.IdleByNode {
		// Key allocations to ProviderId to match against node
		idleId = fmt.Sprintf("%s/%s", a.Properties.Cluster, a.Properties.Node)
		if idleId == "" {
			return idleId, fmt.Errorf("ProviderId is not set")
		}
	} else {
		// key the allocations by cluster id
		idleId = a.Properties.Cluster
		if idleId == "" {
			return idleId, fmt.Errorf("ClusterProp is not set")
		}
	}
	return idleId, nil
}

func (a *Allocation) generateKey(aggregateBy []string, labelConfig *LabelConfig) string {
	if a == nil {
		return ""
	}

	return a.Properties.GenerateKey(aggregateBy, labelConfig)
}

func (a *Allocation) StringProperty(property string) (string, error) {
	switch property {
	case AllocationClusterProp:
		if a.Properties == nil {
			return "", nil
		}
		return a.Properties.Cluster, nil
	case AllocationNodeProp:
		if a.Properties == nil {
			return "", nil
		}
		return a.Properties.Node, nil
	case AllocationContainerProp:
		if a.Properties == nil {
			return "", nil
		}
		return a.Properties.Container, nil
	case AllocationControllerProp:
		if a.Properties == nil {
			return "", nil
		}
		return a.Properties.Controller, nil
	case AllocationControllerKindProp:
		if a.Properties == nil {
			return "", nil
		}
		return a.Properties.ControllerKind, nil
	case AllocationNamespaceProp:
		if a.Properties == nil {
			return "", nil
		}
		return a.Properties.Namespace, nil
	case AllocationPodProp:
		if a.Properties == nil {
			return "", nil
		}
		return a.Properties.Pod, nil
	case AllocationProviderIDProp:
		if a.Properties == nil {
			return "", nil
		}
		return a.Properties.ProviderID, nil
	default:
		return "", fmt.Errorf("Allocation: StringProperty: invalid property name: %s", property)
	}
}

func (a *Allocation) StringSliceProperty(property string) ([]string, error) {
	switch property {
	case AllocationServiceProp:
		if a.Properties == nil {
			return nil, nil
		}
		return a.Properties.Services, nil
	default:
		return nil, fmt.Errorf("Allocation: StringSliceProperty: invalid property name: %s", property)
	}
}

func (a *Allocation) StringMapProperty(property string) (map[string]string, error) {
	switch property {
	case AllocationLabelProp:
		if a.Properties == nil {
			return nil, nil
		}
		return a.Properties.Labels, nil
	case AllocationAnnotationProp:
		if a.Properties == nil {
			return nil, nil
		}
		return a.Properties.Annotations, nil
	default:
		return nil, fmt.Errorf("Allocation: StringMapProperty: invalid property name: %s", property)
	}
}

func (a *Allocation) SanitizeNaN() {
	if a == nil {
		return
	}
	if math.IsNaN(a.CPUCost) {
		log.DedupedWarningf(5, "Allocation: Unexpected NaN found for CPUCost: name:%s, window:%s, properties:%s", a.Name, a.Window.String(), a.Properties.String())
		a.CPUCost = 0
	}
	if math.IsNaN(a.CPUCostIdle) {
		log.DedupedWarningf(5, "Allocation: Unexpected NaN found for CPUCostIdle: name:%s, window:%s, properties:%s", a.Name, a.Window.String(), a.Properties.String())
		a.CPUCostIdle = 0
	}
	if math.IsNaN(a.CPUCoreRequestAverage) {
		log.DedupedWarningf(5, "Allocation: Unexpected NaN found for CPUCoreRequestAverage: name:%s, window:%s, properties:%s", a.Name, a.Window.String(), a.Properties.String())
		a.CPUCoreRequestAverage = 0
	}
	if math.IsNaN(a.CPUCoreHours) {
		log.DedupedWarningf(5, "Allocation: Unexpected NaN found for CPUCoreHours: name:%s, window:%s, properties:%s", a.Name, a.Window.String(), a.Properties.String())
		a.CPUCoreHours = 0
	}
	if math.IsNaN(a.CPUCoreUsageAverage) {
		log.DedupedWarningf(5, "Allocation: Unexpected NaN found for CPUCoreUsageAverage name:%s, window:%s, properties:%s", a.Name, a.Window.String(), a.Properties.String())
		a.CPUCoreUsageAverage = 0
	}
	if math.IsNaN(a.CPUCostAdjustment) {
		log.DedupedWarningf(5, "Allocation: Unexpected NaN found for CPUCostAdjustment name:%s, window:%s, properties:%s", a.Name, a.Window.String(), a.Properties.String())
		a.CPUCostAdjustment = 0
	}
	if math.IsNaN(a.GPUHours) {
		log.DedupedWarningf(5, "Allocation: Unexpected NaN found for GPUHours name:%s, window:%s, properties:%s", a.Name, a.Window.String(), a.Properties.String())
		a.GPUHours = 0
	}

	if math.IsNaN(a.GPUCost) {
		log.DedupedWarningf(5, "Allocation: Unexpected NaN found for GPUCost name:%s, window:%s, properties:%s", a.Name, a.Window.String(), a.Properties.String())
		a.GPUCost = 0
	}
	if math.IsNaN(a.GPUCostIdle) {
		log.DedupedWarningf(5, "Allocation: Unexpected NaN found for GPUCostIdle name:%s, window:%s, properties:%s", a.Name, a.Window.String(), a.Properties.String())
		a.GPUCostIdle = 0
	}
	if math.IsNaN(a.GPUCostAdjustment) {
		log.DedupedWarningf(5, "Allocation: Unexpected NaN found for GPUCostAdjustment name:%s, window:%s, properties:%s", a.Name, a.Window.String(), a.Properties.String())
		a.GPUCostAdjustment = 0
	}
	if math.IsNaN(a.NetworkTransferBytes) {
		log.DedupedWarningf(5, "Allocation: Unexpected NaN found for NetworkTransferBytes name:%s, window:%s, properties:%s", a.Name, a.Window.String(), a.Properties.String())
		a.NetworkTransferBytes = 0
	}
	if math.IsNaN(a.NetworkReceiveBytes) {
		log.DedupedWarningf(5, "Allocation: Unexpected NaN found for NetworkReceiveBytes name:%s, window:%s, properties:%s", a.Name, a.Window.String(), a.Properties.String())
		a.NetworkReceiveBytes = 0
	}
	if math.IsNaN(a.NetworkCost) {
		log.DedupedWarningf(5, "Allocation: Unexpected NaN found for NetworkCost name:%s, window:%s, properties:%s", a.Name, a.Window.String(), a.Properties.String())
		a.NetworkCost = 0
	}
	if math.IsNaN(a.NetworkCrossZoneCost) {
		log.DedupedWarningf(5, "Allocation: Unexpected NaN found for NetworkCrossZoneCost name:%s, window:%s, properties:%s", a.Name, a.Window.String(), a.Properties.String())
		a.NetworkCrossZoneCost = 0
	}
	if math.IsNaN(a.NetworkCrossRegionCost) {
		log.DedupedWarningf(5, "Allocation: Unexpected NaN found for NetworkCrossRegionCost name:%s, window:%s, properties:%s", a.Name, a.Window.String(), a.Properties.String())
		a.NetworkCrossRegionCost = 0
	}
	if math.IsNaN(a.NetworkInternetCost) {
		log.DedupedWarningf(5, "Allocation: Unexpected NaN found for NetworkInternetCost name:%s, window:%s, properties:%s", a.Name, a.Window.String(), a.Properties.String())
		a.NetworkInternetCost = 0
	}
	if math.IsNaN(a.NetworkCostAdjustment) {
		log.DedupedWarningf(5, "Allocation: Unexpected NaN found for NetworkCostAdjustment name:%s, window:%s, properties:%s", a.Name, a.Window.String(), a.Properties.String())
		a.NetworkCostAdjustment = 0
	}
	if math.IsNaN(a.LoadBalancerCost) {
		log.DedupedWarningf(5, "Allocation: Unexpected NaN found for LoadBalancerCost name:%s, window:%s, properties:%s", a.Name, a.Window.String(), a.Properties.String())
		a.LoadBalancerCost = 0
	}
	if math.IsNaN(a.LoadBalancerCostAdjustment) {
		log.DedupedWarningf(5, "Allocation: Unexpected NaN found for LoadBalancerCostAdjustment name:%s, window:%s, properties:%s", a.Name, a.Window.String(), a.Properties.String())
		a.LoadBalancerCostAdjustment = 0
	}
	if math.IsNaN(a.PVCostAdjustment) {
		log.DedupedWarningf(5, "Allocation: Unexpected NaN found for PVCostAdjustment name:%s, window:%s, properties:%s", a.Name, a.Window.String(), a.Properties.String())
		a.PVCostAdjustment = 0
	}
	if math.IsNaN(a.RAMByteHours) {
		log.DedupedWarningf(5, "Allocation: Unexpected NaN found for RAMByteHours name:%s, window:%s, properties:%s", a.Name, a.Window.String(), a.Properties.String())
		a.RAMByteHours = 0
	}
	if math.IsNaN(a.RAMBytesRequestAverage) {
		log.DedupedWarningf(5, "Allocation: Unexpected NaN found for RAMBytesRequestAverage name:%s, window:%s, properties:%s", a.Name, a.Window.String(), a.Properties.String())
		a.RAMBytesRequestAverage = 0
	}
	if math.IsNaN(a.RAMBytesUsageAverage) {
		log.DedupedWarningf(5, "Allocation: Unexpected NaN found for RAMBytesUsageAverage name:%s, window:%s, properties:%s", a.Name, a.Window.String(), a.Properties.String())
		a.RAMBytesUsageAverage = 0
	}
	if math.IsNaN(a.RAMCost) {
		log.DedupedWarningf(5, "Allocation: Unexpected NaN found for RAMCost name:%s, window:%s, properties:%s", a.Name, a.Window.String(), a.Properties.String())
		a.RAMCost = 0
	}
	if math.IsNaN(a.RAMCostIdle) {
		log.DedupedWarningf(5, "Allocation: Unexpected NaN found for RAMCostIdle name:%s, window:%s, properties:%s", a.Name, a.Window.String(), a.Properties.String())
		a.RAMCostIdle = 0
	}
	if math.IsNaN(a.RAMCostAdjustment) {
		log.DedupedWarningf(5, "Allocation: Unexpected NaN found for RAMCostAdjustment name:%s, window:%s, properties:%s", a.Name, a.Window.String(), a.Properties.String())
		a.RAMCostAdjustment = 0
	}
	if math.IsNaN(a.SharedCost) {
		log.DedupedWarningf(5, "Allocation: Unexpected NaN found for SharedCost name:%s, window:%s, properties:%s", a.Name, a.Window.String(), a.Properties.String())
		a.SharedCost = 0
	}
	if math.IsNaN(a.ExternalCost) {
		log.DedupedWarningf(5, "Allocation: Unexpected NaN found for ExternalCost name:%s, window:%s, properties:%s", a.Name, a.Window.String(), a.Properties.String())
		a.ExternalCost = 0
	}

	a.PVs.SanitizeNaN()
	a.RawAllocationOnly.SanitizeNaN()
	a.GPUAllocation.SanitizeNaN()
	a.ProportionalAssetResourceCosts.SanitizeNaN()
	a.SharedCostBreakdown.SanitizeNaN()
	a.LoadBalancers.SanitizeNaN()
}

// Clone returns a new AllocationSet with a deep copy of the given
// AllocationSet's allocations.
func (as *AllocationSet) Clone() *AllocationSet {
	if as == nil {
		return nil
	}

	allocs := make(map[string]*Allocation, len(as.Allocations))
	for k, v := range as.Allocations {
		allocs[k] = v.Clone()
	}

	externalKeys := make(map[string]bool, len(as.ExternalKeys))
	for k, v := range as.ExternalKeys {
		externalKeys[k] = v
	}

	idleKeys := make(map[string]bool, len(as.IdleKeys))
	for k, v := range as.IdleKeys {
		idleKeys[k] = v
	}

	var errors []string
	var warnings []string

	if as.Errors != nil {
		errors = make([]string, len(as.Errors))
		copy(errors, as.Errors)
	} else {
		errors = nil
	}

	if as.Warnings != nil {
		warnings := make([]string, len(as.Warnings))
		copy(warnings, as.Warnings)
	} else {
		warnings = nil
	}

	return &AllocationSet{
		Allocations:  allocs,
		ExternalKeys: externalKeys,
		IdleKeys:     idleKeys,
		Window:       as.Window.Clone(),
		Errors:       errors,
		Warnings:     warnings,
	}
}

// Delete removes the allocation with the given name from the set
func (as *AllocationSet) Delete(name string) {
	if as == nil {
		return
	}

	delete(as.ExternalKeys, name)
	delete(as.IdleKeys, name)
	delete(as.Allocations, name)
}

// End returns the End time of the AllocationSet window
func (as *AllocationSet) End() time.Time {
	if as == nil {
		log.Warnf("AllocationSet: calling End on nil AllocationSet")
		return time.Unix(0, 0)
	}
	if as.Window.End() == nil {
		log.Warnf("AllocationSet: AllocationSet with illegal window: End is nil; len(as.allocations)=%d", len(as.Allocations))
		return time.Unix(0, 0)
	}
	return *as.Window.End()
}

// Get returns the Allocation at the given key in the AllocationSet
func (as *AllocationSet) Get(key string) *Allocation {
	if alloc, ok := as.Allocations[key]; ok {
		return alloc
	}

	return nil
}

// ExternalAllocations returns a map of the external allocations in the set.
// Returns clones of the actual Allocations, so mutability is not a problem.
func (as *AllocationSet) ExternalAllocations() map[string]*Allocation {
	externals := map[string]*Allocation{}

	if as.IsEmpty() {
		return externals
	}

	for key := range as.ExternalKeys {
		if alloc, ok := as.Allocations[key]; ok {
			externals[key] = alloc.Clone()
		}
	}

	return externals
}

// ExternalCost returns the total aggregated external costs of the set
func (as *AllocationSet) ExternalCost() float64 {
	if as.IsEmpty() {
		return 0.0
	}

	externalCost := 0.0
	for _, alloc := range as.Allocations {
		externalCost += alloc.ExternalCost
	}

	return externalCost
}

// IdleAllocations returns a map of the idle allocations in the AllocationSet.
// Returns clones of the actual Allocations, so mutability is not a problem.
func (as *AllocationSet) IdleAllocations() map[string]*Allocation {
	idles := map[string]*Allocation{}

	if as.IsEmpty() {
		return idles
	}

	for key := range as.IdleKeys {
		if alloc, ok := as.Allocations[key]; ok {
			idles[key] = alloc.Clone()
		}
	}

	return idles
}

// Insert aggregates the current entry in the AllocationSet by the given Allocation,
// but only if the Allocation is valid, i.e. matches the AllocationSet's window. If
// there is no existing entry, one is created. Nil error response indicates success.
func (as *AllocationSet) Insert(that *Allocation) error {
	if as == nil {
		return fmt.Errorf("cannot insert into nil AllocationSet")
	}

	if as.Allocations == nil {
		as.Allocations = map[string]*Allocation{}
	}

	if as.ExternalKeys == nil {
		as.ExternalKeys = map[string]bool{}
	}

	if as.IdleKeys == nil {
		as.IdleKeys = map[string]bool{}
	}

	// Add the given Allocation to the existing entry, if there is one;
	// otherwise just set directly into allocations
	if _, ok := as.Allocations[that.Name]; !ok {
		as.Allocations[that.Name] = that
	} else {
		as.Allocations[that.Name].add(that)
	}

	// If the given Allocation is an external one, record that
	if that.IsExternal() {
		as.ExternalKeys[that.Name] = true
	}

	// If the given Allocation is an idle one, record that
	if that.IsIdle() {
		as.IdleKeys[that.Name] = true
	}

	// Expand the window, just to be safe. It's possible that the Allocation will
	// be set into the map without expanding it to the AllocationSet's window.
	as.Allocations[that.Name].Window = as.Allocations[that.Name].Window.Expand(as.Window)

	return nil
}

// IsEmpty returns true if the AllocationSet is nil, or if it contains
// zero allocations.
func (as *AllocationSet) IsEmpty() bool {
	if as == nil || len(as.Allocations) == 0 {
		return true
	}

	return false
}

// Length returns the number of Allocations in the set
func (as *AllocationSet) Length() int {
	if as == nil {
		return 0
	}

	return len(as.Allocations)
}

// ResetAdjustments sets all cost adjustment fields to zero
func (as *AllocationSet) ResetAdjustments() {
	if as == nil {
		return
	}

	for _, a := range as.Allocations {
		a.ResetAdjustments()
	}
}

// Resolution returns the AllocationSet's window duration
func (as *AllocationSet) Resolution() time.Duration {
	return as.Window.Duration()
}

// GetWindow returns the AllocationSet's window
func (as *AllocationSet) GetWindow() Window {
	return as.Window
}

// Set uses the given Allocation to overwrite the existing entry in the
// AllocationSet under the Allocation's name.
func (as *AllocationSet) Set(alloc *Allocation) error {
	if as.IsEmpty() {
		as.Allocations = map[string]*Allocation{}
		as.ExternalKeys = map[string]bool{}
		as.IdleKeys = map[string]bool{}
	}

	as.Allocations[alloc.Name] = alloc

	// If the given Allocation is an external one, record that
	if alloc.IsExternal() {
		as.ExternalKeys[alloc.Name] = true
	}

	// If the given Allocation is an idle one, record that
	if alloc.IsIdle() {
		as.IdleKeys[alloc.Name] = true
	}

	return nil
}

// GetUnmountedPVCost returns the sum of all UnmountedPVCost fields across all
// allocations in the set.
func (as *AllocationSet) GetUnmountedPVCost() float64 {
	upvc := 0.0

	for _, a := range as.Allocations {
		upvc += a.UnmountedPVCost
	}

	return upvc
}

// SetUnmountedPVCost sets the UnmountedPVCost field for all allocations in the
// set.
func (as *AllocationSet) SetUnmountedPVCost() float64 {
	upvc := 0.0

	for _, a := range as.Allocations {
		upvc += a.SetUnmountedPVCost()
	}

	return upvc
}

// Start returns the Start time of the AllocationSet window
func (as *AllocationSet) Start() time.Time {
	if as == nil {
		log.Warnf("AllocationSet: calling Start on nil AllocationSet")
		return time.Unix(0, 0)
	}
	if as.Window.Start() == nil {
		log.Warnf("AllocationSet: AllocationSet with illegal window: Start is nil; len(as.allocations)=%d", len(as.Allocations))
		return time.Unix(0, 0)
	}
	return *as.Window.Start()
}

// String represents the given Allocation as a string
func (as *AllocationSet) String() string {
	if as == nil {
		return "<nil>"
	}

	return fmt.Sprintf(
		"AllocationSet{length: %d; window: %s; totalCost: %.2f}",
		as.Length(),
		as.Window,
		as.TotalCost())
}

// TotalCost returns the sum of all TotalCosts of the allocations contained
func (as *AllocationSet) TotalCost() float64 {
	if as.IsEmpty() {
		return 0.0
	}

	tc := 0.0
	for _, a := range as.Allocations {
		tc += a.TotalCost()
	}
	return tc
}

// UTCOffset returns the AllocationSet's configured UTCOffset.
func (as *AllocationSet) UTCOffset() time.Duration {
	_, zone := as.Start().Zone()
	return time.Duration(zone) * time.Second
}

func (as *AllocationSet) Accumulate(that *AllocationSet) (*AllocationSet, error) {
	if as.IsEmpty() {
		return that.Clone(), nil
	}

	if that.IsEmpty() {
		return as.Clone(), nil
	}

	// Set start, end to min(start), max(end)
	start := as.Start()
	end := as.End()
	if that.Start().Before(start) {
		start = that.Start()
	}
	if that.End().After(end) {
		end = that.End()
	}

	acc := NewAllocationSet(start, end)

	for _, alloc := range as.Allocations {
		err := acc.Insert(alloc)
		if err != nil {
			return nil, err
		}
	}

	for _, alloc := range that.Allocations {
		err := acc.Insert(alloc)
		if err != nil {
			return nil, err
		}
	}

	return acc, nil
}

func (as *AllocationSet) SanitizeNaN() {
	if as == nil {
		return
	}
	for _, a := range as.Allocations {
		a.SanitizeNaN()
	}
}

// AllocationSetRange is a thread-safe slice of AllocationSets. It is meant to
// be used such that the AllocationSets held are consecutive and coherent with
// respect to using the same aggregation properties, UTC offset, and
// resolution. However these rules are not necessarily enforced, so use wisely.
type AllocationSetRange struct {
	Allocations []*AllocationSet
	FromStore   string // stores the name of the store used to retrieve the data
}

// NewAllocationSetRange instantiates a new range composed of the given
// AllocationSets in the order provided.
func NewAllocationSetRange(allocs ...*AllocationSet) *AllocationSetRange {
	return &AllocationSetRange{
		Allocations: allocs,
	}
}

// Get safely retrieves the AllocationSet at the given index of the range.
func (asr *AllocationSetRange) Get(i int) (*AllocationSet, error) {
	if i < 0 || i >= len(asr.Allocations) {
		return nil, fmt.Errorf("AllocationSetRange: index out of range: %d", i)
	}

	return asr.Allocations[i], nil
}

// Accumulate sums each AllocationSet in the given range, returning a single cumulative
// AllocationSet for the entire range.
func (asr *AllocationSetRange) accumulate() (*AllocationSet, error) {
	var allocSet *AllocationSet
	var err error

	for _, as := range asr.Allocations {
		allocSet, err = allocSet.Accumulate(as)
		if err != nil {
			return nil, err
		}
	}

	return allocSet, nil
}

// newAccumulation clones the first available AllocationSet to use as the data structure to
// Accumulate the remaining data. This leaves the original AllocationSetRange intact.
func (asr *AllocationSetRange) newAccumulation() (*AllocationSet, error) {
	// NOTE: Adding this API for consistency across SummaryAllocation and Assets, but this
	// NOTE: implementation is almost identical to regular Accumulate(). The Accumulate() method
	// NOTE: for Allocation returns Clone() of the input, which is required for Accumulate
	// NOTE: support (unit tests are great for verifying this information).
	var allocSet *AllocationSet
	var err error

	if asr == nil {
		return nil, fmt.Errorf("nil AllocationSetRange in accumulation")
	}

	if len(asr.Allocations) == 0 {
		return nil, fmt.Errorf("AllocationSetRange has empty AssetSet in accumulation")
	}

	for _, as := range asr.Allocations {
		if allocSet == nil {
			allocSet = as.Clone()
			continue
		}

		var allocSetCopy *AllocationSet = nil
		if as != nil {
			allocSetCopy = as.Clone()
		}

		allocSet, err = allocSet.Accumulate(allocSetCopy)
		if err != nil {
			return nil, err
		}
	}

	return allocSet, nil
}

// Accumulate sums AllocationSets based on the AccumulateOption (calendar week or calendar month).
// The accumulated set is determined by the start of the window of the allocation set.
func (asr *AllocationSetRange) Accumulate(accumulateBy AccumulateOption) (*AllocationSetRange, error) {
	switch accumulateBy {
	case AccumulateOptionNone:
		return asr.accumulateByNone()
	case AccumulateOptionAll:
		return asr.accumulateByAll()
	case AccumulateOptionHour:
		return asr.accumulateByHour()
	case AccumulateOptionDay:
		return asr.accumulateByDay()
	case AccumulateOptionWeek:
		return asr.accumulateByWeek()
	case AccumulateOptionMonth:
		return asr.accumulateByMonth()
	default:
		// ideally, this should never happen
		return nil, fmt.Errorf("unexpected error, invalid accumulateByType: %s", accumulateBy)
	}

}

func (asr *AllocationSetRange) accumulateByAll() (*AllocationSetRange, error) {
	var err error
	var as *AllocationSet
	as, err = asr.newAccumulation()
	if err != nil {
		return nil, fmt.Errorf("error accumulating all:%s", err)
	}

	accumulated := NewAllocationSetRange(as)
	return accumulated, nil
}

func (asr *AllocationSetRange) accumulateByNone() (*AllocationSetRange, error) {
	return asr.Clone(), nil
}
func (asr *AllocationSetRange) accumulateByHour() (*AllocationSetRange, error) {
	// ensure that the summary allocation sets have a 1-hour window, if a set exists
	if len(asr.Allocations) > 0 && asr.Allocations[0].Window.Duration() != time.Hour {
		return nil, fmt.Errorf("window duration must equal 1 hour; got:%s", asr.Allocations[0].Window.Duration())
	}

	return asr.Clone(), nil
}

func (asr *AllocationSetRange) accumulateByDay() (*AllocationSetRange, error) {
	// if the allocation set window is 1-day, just return the existing allocation set range
	if len(asr.Allocations) > 0 && asr.Allocations[0].Window.Duration() == time.Hour*24 {
		return asr, nil
	}

	var toAccumulate *AllocationSetRange
	result := NewAllocationSetRange()
	for i, as := range asr.Allocations {

		if as.Window.Duration() != time.Hour {
			return nil, fmt.Errorf("window duration must equal 1 hour; got:%s", as.Window.Duration())
		}

		hour := as.Window.Start().Hour()

		if toAccumulate == nil {
			toAccumulate = NewAllocationSetRange()
			as = as.Clone()
		}

		toAccumulate.Append(as)
		asAccumulated, err := toAccumulate.accumulate()
		if err != nil {
			return nil, fmt.Errorf("error accumulating result: %s", err)
		}
		toAccumulate = NewAllocationSetRange(asAccumulated)

		if hour == 23 || i == len(asr.Allocations)-1 {
			if length := len(toAccumulate.Allocations); length != 1 {
				return nil, fmt.Errorf("failed accumulation, detected %d sets instead of 1", length)
			}
			result.Append(toAccumulate.Allocations[0])
			toAccumulate = nil
		}
	}
	return result, nil
}

func (asr *AllocationSetRange) accumulateByMonth() (*AllocationSetRange, error) {
	var toAccumulate *AllocationSetRange
	result := NewAllocationSetRange()
	for i, as := range asr.Allocations {
		if as.Window.Duration() != time.Hour*24 {
			return nil, fmt.Errorf("window duration must equal 24 hours; got:%s", as.Window.Duration())
		}

		_, month, _ := as.Window.Start().Date()
		_, nextDayMonth, _ := as.Window.Start().Add(time.Hour * 24).Date()

		if toAccumulate == nil {
			toAccumulate = NewAllocationSetRange()
			as = as.Clone()
		}

		toAccumulate.Append(as)
		asAccumulated, err := toAccumulate.accumulate()
		if err != nil {
			return nil, fmt.Errorf("error accumulating result: %s", err)
		}
		toAccumulate = NewAllocationSetRange(asAccumulated)

		// either the month has ended, or there are no more allocation sets
		if month != nextDayMonth || i == len(asr.Allocations)-1 {
			if length := len(toAccumulate.Allocations); length != 1 {
				return nil, fmt.Errorf("failed accumulation, detected %d sets instead of 1", length)
			}
			result.Append(toAccumulate.Allocations[0])
			toAccumulate = nil
		}
	}
	return result, nil
}

func (asr *AllocationSetRange) accumulateByWeek() (*AllocationSetRange, error) {
	if len(asr.Allocations) > 0 && asr.Allocations[0].Window.Duration() == timeutil.Week {
		return asr, nil
	}

	var toAccumulate *AllocationSetRange
	result := NewAllocationSetRange()
	for i, as := range asr.Allocations {
		if as.Window.Duration() != time.Hour*24 {
			return nil, fmt.Errorf("window duration must equal 24 hours; got:%s", as.Window.Duration())
		}

		dayOfWeek := as.Window.Start().Weekday()

		if toAccumulate == nil {
			toAccumulate = NewAllocationSetRange()
			as = as.Clone()
		}

		toAccumulate.Append(as)
		asAccumulated, err := toAccumulate.accumulate()
		if err != nil {
			return nil, fmt.Errorf("error accumulating result: %s", err)
		}
		toAccumulate = NewAllocationSetRange(asAccumulated)

		// current assumption is the week always ends on Saturday, or there are no more allocation sets
		if dayOfWeek == time.Saturday || i == len(asr.Allocations)-1 {
			if length := len(toAccumulate.Allocations); length != 1 {
				return nil, fmt.Errorf("failed accumulation, detected %d sets instead of 1", length)
			}
			result.Append(toAccumulate.Allocations[0])
			toAccumulate = nil
		}
	}
	return result, nil
}

// AggregateBy aggregates each AllocationSet in the range by the given
// properties and options.
func (asr *AllocationSetRange) AggregateBy(aggregateBy []string, options *AllocationAggregationOptions) error {
	aggRange := &AllocationSetRange{Allocations: []*AllocationSet{}}

	for _, as := range asr.Allocations {
		err := as.AggregateBy(aggregateBy, options)
		if err != nil {
			return err
		}
		aggRange.Allocations = append(aggRange.Allocations, as)
	}

	asr.Allocations = aggRange.Allocations

	return nil
}

// Append appends the given AllocationSet to the end of the range. It does not
// validate whether or not that violates window continuity.
func (asr *AllocationSetRange) Append(that *AllocationSet) {
	asr.Allocations = append(asr.Allocations, that)
}

// InsertRange merges the given AllocationSetRange into the receiving one by
// lining up sets with matching windows, then inserting each allocation from
// the given ASR into the respective set in the receiving ASR. If the given
// ASR contains an AllocationSet from a window that does not exist in the
// receiving ASR, then an error is returned. However, the given ASR does not
// need to cover the full range of the receiver.
func (asr *AllocationSetRange) InsertRange(that *AllocationSetRange) error {
	if asr == nil {
		return fmt.Errorf("cannot insert range into nil AllocationSetRange")
	}

	// Providing an empty or nil set range is a no-op
	if that == nil {
		return nil
	}

	// keys maps window to index in asr
	keys := map[string]int{}
	for i, as := range asr.Allocations {
		if as == nil {
			continue
		}
		keys[as.Window.String()] = i
	}

	// Nothing to merge, so simply return
	if len(keys) == 0 {
		return nil
	}

	var err error
	for _, thatAS := range that.Allocations {
		if thatAS == nil || err != nil {
			continue
		}

		// Find matching AllocationSet in asr
		i, ok := keys[thatAS.Window.String()]
		if !ok {
			err = fmt.Errorf("cannot merge AllocationSet into window that does not exist: %s", thatAS.Window.String())
			continue
		}

		as, err := asr.Get(i)
		if err != nil {
			err = fmt.Errorf("AllocationSetRange index does not exist: %d", i)
			continue
		}

		// Insert each Allocation from the given set
		for _, alloc := range thatAS.Allocations {
			err = as.Insert(alloc)
			if err != nil {
				err = fmt.Errorf("error inserting allocation: %s", err)
				continue
			}
		}
	}

	// err might be nil
	return err
}

// Length returns the length of the range, which is zero if nil
func (asr *AllocationSetRange) Length() int {
	if asr == nil || asr.Allocations == nil {
		return 0
	}

	return len(asr.Allocations)
}

// Slice copies the underlying slice of AllocationSets, maintaining order,
// and returns the copied slice.
func (asr *AllocationSetRange) Slice() []*AllocationSet {
	if asr == nil || asr.Allocations == nil {
		return nil
	}

	copy := []*AllocationSet{}
	for _, as := range asr.Allocations {
		copy = append(copy, as.Clone())
	}
	return copy
}

// String represents the given AllocationSetRange as a string
func (asr *AllocationSetRange) String() string {
	if asr == nil {
		return "<nil>"
	}
	return fmt.Sprintf("AllocationSetRange{length: %d}", asr.Length())
}

// UTCOffset returns the detected UTCOffset of the AllocationSets within the
// range. Defaults to 0 if the range is nil or empty. Does not warn if there
// are sets with conflicting UTCOffsets (just returns the first).
func (asr *AllocationSetRange) UTCOffset() time.Duration {
	if asr.Length() == 0 {
		return 0
	}

	as, err := asr.Get(0)
	if err != nil {
		return 0
	}
	return as.UTCOffset()
}

// Window returns the full window that the AllocationSetRange spans, from the
// start of the first AllocationSet to the end of the last one.
func (asr *AllocationSetRange) Window() Window {
	if asr == nil || asr.Length() == 0 {
		return NewWindow(nil, nil)
	}

	start := asr.Allocations[0].Start()
	end := asr.Allocations[asr.Length()-1].End()

	return NewWindow(&start, &end)
}

// Start returns the earliest start of all Allocations in the AllocationSetRange.
// It returns an error if there are no allocations.
func (asr *AllocationSetRange) Start() (time.Time, error) {
	start := time.Time{}
	if asr == nil {
		return start, fmt.Errorf("had no data to compute a start from")
	}

	firstStartNotSet := true
	for _, as := range asr.Allocations {
		for _, a := range as.Allocations {
			if firstStartNotSet {
				start = a.Start
				firstStartNotSet = false
			}
			if a.Start.Before(start) {
				start = a.Start
			}
		}
	}

	if firstStartNotSet {
		return start, fmt.Errorf("had no data to compute a start from")
	}

	return start, nil
}

// End returns the latest end of all Allocations in the AllocationSetRange.
// It returns an error if there are no allocations.
func (asr *AllocationSetRange) End() (time.Time, error) {
	end := time.Time{}
	if asr == nil {
		return end, fmt.Errorf("had no data to compute an end from")
	}

	firstEndNotSet := true
	for _, as := range asr.Allocations {
		for _, a := range as.Allocations {
			if firstEndNotSet {
				end = a.End
				firstEndNotSet = false
			}
			if a.End.After(end) {
				end = a.End
			}
		}
	}

	if firstEndNotSet {
		return end, fmt.Errorf("had no data to compute an end from")
	}

	return end, nil
}

// StartAndEnd iterates over all AssetSets in the AssetSetRange and returns the earliest start and
// latest end over the entire range
func (asr *AllocationSetRange) StartAndEnd() (time.Time, time.Time, error) {
	start := time.Time{}
	end := time.Time{}

	if asr == nil {
		return start, end, fmt.Errorf("had no data to compute a start and end from")
	}

	firstStartNotSet := true
	firstEndNotSet := true

	for _, as := range asr.Allocations {
		for _, a := range as.Allocations {
			if firstStartNotSet {
				start = a.Start
				firstStartNotSet = false
			}
			if a.Start.Before(start) {
				start = a.Start
			}
			if firstEndNotSet {
				end = a.End
				firstEndNotSet = false
			}
			if a.End.After(end) {
				end = a.End
			}
		}
	}

	if firstStartNotSet {
		return start, end, fmt.Errorf("had no data to compute a start from")
	}

	if firstEndNotSet {
		return start, end, fmt.Errorf("had no data to compute an end from")
	}

	return start, end, nil
}

// Minutes returns the duration, in minutes, between the earliest start
// and the latest end of all assets in the AllocationSetRange.
func (asr *AllocationSetRange) Minutes() float64 {
	if asr == nil {
		return 0
	}

	start, end, err := asr.StartAndEnd()
	if err != nil {
		return 0
	}

	duration := end.Sub(start)

	return duration.Minutes()
}

// TotalCost returns the sum of all TotalCosts of the allocations contained
func (asr *AllocationSetRange) TotalCost() float64 {
	if asr == nil || len(asr.Allocations) == 0 {
		return 0.0
	}

	tc := 0.0
	for _, as := range asr.Allocations {
		tc += as.TotalCost()
	}
	return tc
}

// Clone returns a new AllocationSetRange cloned from the existing ASR
func (asr *AllocationSetRange) Clone() *AllocationSetRange {
	sasrClone := NewAllocationSetRange()
	sasrClone.FromStore = asr.FromStore

	for _, as := range asr.Allocations {
		asClone := as.Clone()
		sasrClone.Append(asClone)
	}

	return sasrClone
}

func migrateAllocation(as *Allocation, fromVersion uint8, toVersion uint8) {
	if fromVersion == toVersion {
		return
	}

	if fromVersion == 22 && toVersion >= 23 {
		as.GPUAllocation = &GPUAllocation{}
		as.GPUAllocation.GPUUsageAverage = &as.deprecatedGPUUsageAverage
		as.GPUAllocation.GPURequestAverage = &as.deprecatedGPURequestAverage
	}
}
