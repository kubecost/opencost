package opencost

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/patrickmn/go-cache"
)

type AllocationTotalsResult struct {
	Cluster map[string]*AllocationTotals `json:"cluster"`
	Node    map[string]*AllocationTotals `json:"node"`
}

type AssetTotalsResult struct {
	Cluster map[string]*AssetTotals `json:"cluster"`
	Node    map[string]*AssetTotals `json:"node"`
}

// AllocationTotals represents aggregate costs of all Allocations for
// a given cluster or tuple of (cluster, node) between a given start and end
// time, where the costs are aggregated per-resource. AllocationTotals
// is designed to be used as a pre-computed intermediate data structure when
// contextual knowledge is required to carry out a task, but computing totals
// on-the-fly would be expensive; e.g. idle allocation; sharing coefficients
// for idle or shared resources, etc.
type AllocationTotals struct {
	Start                          time.Time `json:"start"`
	End                            time.Time `json:"end"`
	Cluster                        string    `json:"cluster"`
	Node                           string    `json:"node"`
	Count                          int       `json:"count"`
	CPUCost                        float64   `json:"cpuCost"`
	CPUCostAdjustment              float64   `json:"cpuCostAdjustment"`
	GPUCost                        float64   `json:"gpuCost"`
	GPUCostAdjustment              float64   `json:"gpuCostAdjustment"`
	LoadBalancerCost               float64   `json:"loadBalancerCost"`
	LoadBalancerCostAdjustment     float64   `json:"loadBalancerCostAdjustment"`
	NetworkCost                    float64   `json:"networkCost"`
	NetworkCostAdjustment          float64   `json:"networkCostAdjustment"`
	PersistentVolumeCost           float64   `json:"persistentVolumeCost"`
	PersistentVolumeCostAdjustment float64   `json:"persistentVolumeCostAdjustment"`
	RAMCost                        float64   `json:"ramCost"`
	RAMCostAdjustment              float64   `json:"ramCostAdjustment"`
	// UnmountedPVCost is used to track how much of the cost in
	// PersistentVolumeCost is for an unmounted PV. It is not additive of that
	// field, and need not be sent in API responses.
	UnmountedPVCost float64 `json:"-"`
}

// ClearAdjustments sets all adjustment fields to 0.0
func (art *AllocationTotals) ClearAdjustments() {
	art.CPUCostAdjustment = 0.0
	art.GPUCostAdjustment = 0.0
	art.LoadBalancerCostAdjustment = 0.0
	art.NetworkCostAdjustment = 0.0
	art.PersistentVolumeCostAdjustment = 0.0
	art.RAMCostAdjustment = 0.0
}

// Clone deep copies the AllocationTotals
func (art *AllocationTotals) Clone() *AllocationTotals {
	return &AllocationTotals{
		Start:                          art.Start,
		End:                            art.End,
		Cluster:                        art.Cluster,
		Node:                           art.Node,
		Count:                          art.Count,
		CPUCost:                        art.CPUCost,
		CPUCostAdjustment:              art.CPUCostAdjustment,
		GPUCost:                        art.GPUCost,
		GPUCostAdjustment:              art.GPUCostAdjustment,
		LoadBalancerCost:               art.LoadBalancerCost,
		LoadBalancerCostAdjustment:     art.LoadBalancerCostAdjustment,
		NetworkCost:                    art.NetworkCost,
		NetworkCostAdjustment:          art.NetworkCostAdjustment,
		PersistentVolumeCost:           art.PersistentVolumeCost,
		PersistentVolumeCostAdjustment: art.PersistentVolumeCostAdjustment,
		RAMCost:                        art.RAMCost,
		RAMCostAdjustment:              art.RAMCostAdjustment,
	}
}

// TotalCPUCost returns CPU cost with adjustment.
func (art *AllocationTotals) TotalCPUCost() float64 {
	return art.CPUCost + art.CPUCostAdjustment
}

// TotalGPUCost returns GPU cost with adjustment.
func (art *AllocationTotals) TotalGPUCost() float64 {
	return art.GPUCost + art.GPUCostAdjustment
}

// TotalLoadBalancerCost returns LoadBalancer cost with adjustment.
func (art *AllocationTotals) TotalLoadBalancerCost() float64 {
	return art.LoadBalancerCost + art.LoadBalancerCostAdjustment
}

// TotalNetworkCost returns Network cost with adjustment.
func (art *AllocationTotals) TotalNetworkCost() float64 {
	return art.NetworkCost + art.NetworkCostAdjustment
}

// TotalPersistentVolumeCost returns PersistentVolume cost with adjustment.
func (art *AllocationTotals) TotalPersistentVolumeCost() float64 {
	return art.PersistentVolumeCost + art.PersistentVolumeCostAdjustment
}

// TotalRAMCost returns RAM cost with adjustment.
func (art *AllocationTotals) TotalRAMCost() float64 {
	return art.RAMCost + art.RAMCostAdjustment
}

// TotalCost returns the sum of all costs.
func (art *AllocationTotals) TotalCost() float64 {
	return art.TotalCPUCost() + art.TotalGPUCost() + art.TotalLoadBalancerCost() +
		art.TotalNetworkCost() + art.TotalPersistentVolumeCost() + art.TotalRAMCost()
}

// ComputeAllocationTotals totals the resource costs of the given AllocationSet
// using the given property, i.e. cluster or node, where "node" really means to
// use the fully-qualified (cluster, node) tuple.
func ComputeAllocationTotals(as *AllocationSet, prop string) map[string]*AllocationTotals {
	arts := map[string]*AllocationTotals{}

	for _, alloc := range as.Allocations {
		// Do not count idle or unmounted allocations
		if alloc.IsIdle() || alloc.IsUnmounted() {
			continue
		}

		// Default to computing totals by Cluster, but allow override to use Node.
		key := alloc.Properties.Cluster
		if prop == AllocationNodeProp {
			key = fmt.Sprintf("%s/%s", alloc.Properties.Cluster, alloc.Properties.Node)
		}

		if _, ok := arts[key]; !ok {
			arts[key] = &AllocationTotals{
				Start:   alloc.Start,
				End:     alloc.End,
				Cluster: alloc.Properties.Cluster,
				Node:    alloc.Properties.Node,
			}
		}

		if arts[key].Start.After(alloc.Start) {
			arts[key].Start = alloc.Start
		}
		if arts[key].End.Before(alloc.End) {
			arts[key].End = alloc.End
		}

		if arts[key].Node != alloc.Properties.Node {
			arts[key].Node = ""
		}

		arts[key].Count++

		arts[key].CPUCost += alloc.CPUCost
		arts[key].CPUCostAdjustment += alloc.CPUCostAdjustment

		arts[key].GPUCost += alloc.GPUCost
		arts[key].GPUCostAdjustment += alloc.GPUCostAdjustment

		arts[key].LoadBalancerCost += alloc.LoadBalancerCost
		arts[key].LoadBalancerCostAdjustment += alloc.LoadBalancerCostAdjustment

		arts[key].NetworkCost += alloc.NetworkCost
		arts[key].NetworkCostAdjustment += alloc.NetworkCostAdjustment

		arts[key].PersistentVolumeCost += alloc.PVCost() // NOTE: PVCost() does not include adjustment
		arts[key].PersistentVolumeCostAdjustment += alloc.PVCostAdjustment

		arts[key].RAMCost += alloc.RAMCost
		arts[key].RAMCostAdjustment += alloc.RAMCostAdjustment
	}

	return arts
}

// AllocationTotalsSet represents totals, summed by both "cluster" and "node"
// for a given window of time.
type AllocationTotalsSet struct {
	Cluster map[string]*AllocationTotals `json:"cluster"`
	Node    map[string]*AllocationTotals `json:"node"`
	Window  Window                       `json:"window"`
}

func NewAllocationTotalsSet(window Window, byCluster, byNode map[string]*AllocationTotals) *AllocationTotalsSet {
	return &AllocationTotalsSet{
		Cluster: byCluster,
		Node:    byNode,
		Window:  window.Clone(),
	}
}

// AssetTotals represents aggregate costs of all Assets for a given
// cluster or tuple of (cluster, node) between a given start and end time,
// where the costs are aggregated per-resource. AssetTotals is designed
// to be used as a pre-computed intermediate data structure when contextual
// knowledge is required to carry out a task, but computing totals on-the-fly
// would be expensive; e.g. idle allocation, shared tenancy costs
type AssetTotals struct {
	Start                           time.Time `json:"start"`
	End                             time.Time `json:"end"`
	Cluster                         string    `json:"cluster"`
	Node                            string    `json:"node"`
	Count                           int       `json:"count"`
	AttachedVolumeCost              float64   `json:"attachedVolumeCost"`
	AttachedVolumeCostAdjustment    float64   `json:"attachedVolumeCostAdjustment"`
	ClusterManagementCost           float64   `json:"clusterManagementCost"`
	ClusterManagementCostAdjustment float64   `json:"clusterManagementCostAdjustment"`
	CPUCost                         float64   `json:"cpuCost"`
	CPUCostAdjustment               float64   `json:"cpuCostAdjustment"`
	GPUCost                         float64   `json:"gpuCost"`
	GPUCostAdjustment               float64   `json:"gpuCostAdjustment"`
	LoadBalancerCost                float64   `json:"loadBalancerCost"`
	LoadBalancerCostAdjustment      float64   `json:"loadBalancerCostAdjustment"`
	PersistentVolumeCost            float64   `json:"persistentVolumeCost"`
	PersistentVolumeCostAdjustment  float64   `json:"persistentVolumeCostAdjustment"`
	RAMCost                         float64   `json:"ramCost"`
	RAMCostAdjustment               float64   `json:"ramCostAdjustment"`
	PrivateLoadBalancer             bool      `json:"privateLoadBalancer"`
}

// ClearAdjustments sets all adjustment fields to 0.0
func (art *AssetTotals) ClearAdjustments() {
	art.AttachedVolumeCostAdjustment = 0.0
	art.ClusterManagementCostAdjustment = 0.0
	art.CPUCostAdjustment = 0.0
	art.GPUCostAdjustment = 0.0
	art.LoadBalancerCostAdjustment = 0.0
	art.PersistentVolumeCostAdjustment = 0.0
	art.RAMCostAdjustment = 0.0
}

// Clone deep copies the AssetTotals
func (art *AssetTotals) Clone() *AssetTotals {
	return &AssetTotals{
		Start:                           art.Start,
		End:                             art.End,
		Cluster:                         art.Cluster,
		Node:                            art.Node,
		Count:                           art.Count,
		AttachedVolumeCost:              art.AttachedVolumeCost,
		AttachedVolumeCostAdjustment:    art.AttachedVolumeCostAdjustment,
		ClusterManagementCost:           art.ClusterManagementCost,
		ClusterManagementCostAdjustment: art.ClusterManagementCostAdjustment,
		CPUCost:                         art.CPUCost,
		CPUCostAdjustment:               art.CPUCostAdjustment,
		GPUCost:                         art.GPUCost,
		GPUCostAdjustment:               art.GPUCostAdjustment,
		LoadBalancerCost:                art.LoadBalancerCost,
		LoadBalancerCostAdjustment:      art.LoadBalancerCostAdjustment,
		PersistentVolumeCost:            art.PersistentVolumeCost,
		PersistentVolumeCostAdjustment:  art.PersistentVolumeCostAdjustment,
		RAMCost:                         art.RAMCost,
		RAMCostAdjustment:               art.RAMCostAdjustment,
		PrivateLoadBalancer:             art.PrivateLoadBalancer,
	}
}

// TotalAttachedVolumeCost returns CPU cost with adjustment.
func (art *AssetTotals) TotalAttachedVolumeCost() float64 {
	return art.AttachedVolumeCost + art.AttachedVolumeCostAdjustment
}

// TotalClusterManagementCost returns ClusterManagement cost with adjustment.
func (art *AssetTotals) TotalClusterManagementCost() float64 {
	return art.ClusterManagementCost + art.ClusterManagementCostAdjustment
}

// TotalCPUCost returns CPU cost with adjustment.
func (art *AssetTotals) TotalCPUCost() float64 {
	return art.CPUCost + art.CPUCostAdjustment
}

// TotalGPUCost returns GPU cost with adjustment.
func (art *AssetTotals) TotalGPUCost() float64 {
	return art.GPUCost + art.GPUCostAdjustment
}

// TotalLoadBalancerCost returns LoadBalancer cost with adjustment.
func (art *AssetTotals) TotalLoadBalancerCost() float64 {
	return art.LoadBalancerCost + art.LoadBalancerCostAdjustment
}

// TotalPersistentVolumeCost returns PersistentVolume cost with adjustment.
func (art *AssetTotals) TotalPersistentVolumeCost() float64 {
	return art.PersistentVolumeCost + art.PersistentVolumeCostAdjustment
}

// TotalRAMCost returns RAM cost with adjustment.
func (art *AssetTotals) TotalRAMCost() float64 {
	return art.RAMCost + art.RAMCostAdjustment
}

// TotalCost returns the sum of all costs
func (art *AssetTotals) TotalCost() float64 {
	return art.TotalAttachedVolumeCost() + art.TotalClusterManagementCost() +
		art.TotalCPUCost() + art.TotalGPUCost() + art.TotalLoadBalancerCost() +
		art.TotalPersistentVolumeCost() + art.TotalRAMCost()
}

// ComputeAssetTotals totals the resource costs of the given AssetSet,
// using the given property, i.e. cluster or node, where "node" really means to
// use the fully-qualified (cluster, node) tuple.
// NOTE: we're not capturing LoadBalancers here yet, but only because we don't
// yet need them. They could be added.
func ComputeAssetTotals(as *AssetSet, byAsset bool) map[string]*AssetTotals {
	arts := map[string]*AssetTotals{}

	// Attached disks are tracked by matching their name with the name of the
	// node, as is standard for attached disks.
	nodeNames := map[string]bool{}
	disks := map[string]*Disk{}

	for _, node := range as.Nodes {
		// Default to computing totals by Cluster, but allow override to use Node.
		key := node.Properties.Cluster
		if byAsset {
			key = fmt.Sprintf("%s/%s", node.Properties.Cluster, node.Properties.Name)
		}

		// Add node name to list of node names. (These are to be used later
		// for attached volumes.)
		nodeNames[fmt.Sprintf("%s/%s", node.Properties.Cluster, node.Properties.Name)] = true

		// adjustmentRate is used to scale resource costs proportionally
		// by the adjustment. This is necessary because we only get one
		// adjustment per Node, not one per-resource-per-Node.
		//
		// e.g. total cost =  $90 (cost = $100, adjustment = -$10)  => 0.9000 ( 90 / 100)
		// e.g. total cost = $150 (cost = $450, adjustment = -$300) => 0.3333 (150 / 450)
		// e.g. total cost = $150 (cost = $100, adjustment = $50)   => 1.5000 (150 / 100)
		adjustmentRate := 1.0
		if node.TotalCost()-node.Adjustment == 0 {
			// If (totalCost - adjustment) is 0.0 then adjustment cancels
			// the entire node cost and we should make everything 0
			// without dividing by 0.
			adjustmentRate = 0.0
			log.DedupedWarningf(5, "ComputeTotals: node cost adjusted to $0.00 for %s", node.Properties.Name)
		} else if node.Adjustment != 0.0 {
			// adjustmentRate is the ratio of cost-with-adjustment (i.e. TotalCost)
			// to cost-without-adjustment (i.e. TotalCost - Adjustment).
			adjustmentRate = node.TotalCost() / (node.TotalCost() - node.Adjustment)
		}

		// 1. Start with raw, measured resource cost
		// 2. Apply discount to get discounted resource cost
		// 3. Apply adjustment to get final "adjusted" resource cost
		// 4. Subtract (3 - 2) to get adjustment in doller-terms
		// 5. Use (2 + 4) as total cost, so (2) is "cost" and (4) is "adjustment"

		// Example:
		// - node.CPUCost   = 10.00
		// - node.Discount  =  0.20  // We assume a 20% discount
		// - adjustmentRate =  0.75  // CUR says we need to reduce to 75% of our post-discount node cost
		//
		// 1. See above
		// 2. discountedCPUCost = 10.00 * (1.0 - 0.2) =  8.00
		// 3. adjustedCPUCost   =  8.00 * 0.75        =  6.00  // this is the actual cost according to the CUR
		// 4. adjustment        =  6.00 - 8.00        = -2.00
		// 5. totalCost = 6.00, which is the sum of (2) cost = 8.00 and (4) adjustment = -2.00

		discountedCPUCost := node.CPUCost * (1.0 - node.Discount)
		adjustedCPUCost := discountedCPUCost * adjustmentRate
		cpuCostAdjustment := adjustedCPUCost - discountedCPUCost

		discountedRAMCost := node.RAMCost * (1.0 - node.Discount)
		adjustedRAMCost := discountedRAMCost * adjustmentRate
		ramCostAdjustment := adjustedRAMCost - discountedRAMCost

		adjustedGPUCost := node.GPUCost * adjustmentRate
		gpuCostAdjustment := adjustedGPUCost - node.GPUCost

		if _, ok := arts[key]; !ok {
			arts[key] = &AssetTotals{
				Start:   node.Start,
				End:     node.End,
				Cluster: node.Properties.Cluster,
				Node:    node.Properties.Name,
			}
		}

		if arts[key].Start.After(node.Start) {
			arts[key].Start = node.Start
		}
		if arts[key].End.Before(node.End) {
			arts[key].End = node.End
		}

		if arts[key].Node != node.Properties.Name {
			arts[key].Node = ""
		}

		arts[key].Count++

		// TotalCPUCost will be discounted cost + adjustment
		arts[key].CPUCost += discountedCPUCost
		arts[key].CPUCostAdjustment += cpuCostAdjustment

		// TotalRAMCost will be discounted cost + adjustment
		arts[key].RAMCost += discountedRAMCost
		arts[key].RAMCostAdjustment += ramCostAdjustment

		// TotalGPUCost will be discounted cost + adjustment
		arts[key].GPUCost += node.GPUCost
		arts[key].GPUCostAdjustment += gpuCostAdjustment
	}

	for _, lb := range as.LoadBalancers {
		// Default to computing totals by Cluster, but allow override to use LoadBalancer.
		key := lb.Properties.Cluster
		if byAsset {
			key = fmt.Sprintf("%s/%s", lb.Properties.Cluster, lb.Properties.Name)
		}

		if _, ok := arts[key]; !ok {
			arts[key] = &AssetTotals{
				Start:               lb.Start,
				End:                 lb.End,
				Cluster:             lb.Properties.Cluster,
				Node:                lb.Properties.Name,
				PrivateLoadBalancer: lb.Private,
			}
		}

		arts[key].LoadBalancerCost += lb.Cost
		arts[key].LoadBalancerCostAdjustment += lb.Adjustment
	}

	// Only record ClusterManagement when prop
	// is cluster. We can't breakdown these types by Node.
	if !byAsset {
		for _, cm := range as.ClusterManagement {
			key := cm.Properties.Cluster

			if _, ok := arts[key]; !ok {
				arts[key] = &AssetTotals{
					Start:   cm.GetStart(),
					End:     cm.GetEnd(),
					Cluster: cm.Properties.Cluster,
				}
			}

			arts[key].Count++
			arts[key].ClusterManagementCost += cm.Cost
			arts[key].ClusterManagementCostAdjustment += cm.Adjustment
		}
	}

	// Record disks in an intermediate structure, which will be
	// processed after all assets have been seen.
	for _, disk := range as.Disks {
		key := fmt.Sprintf("%s/%s", disk.Properties.Cluster, disk.Properties.Name)

		disks[key] = disk
	}

	// Record all disks as either attached volumes or persistent volumes.
	for name, disk := range disks {
		// By default, the key will be the name, which is the tuple of
		// cluster/node. But if we're aggregating by cluster only, then
		// reset the key to just the cluster.
		key := name
		if !byAsset {
			key = disk.Properties.Cluster
		}

		if _, ok := arts[key]; !ok {
			arts[key] = &AssetTotals{
				Start:   disk.Start,
				End:     disk.End,
				Cluster: disk.Properties.Cluster,
			}

			if byAsset {
				arts[key].Node = disk.Properties.Name
			}
		}

		_, isAttached := nodeNames[name]
		if isAttached {
			// Record attached volume data at the cluster and node level, using
			// name matching to distinguish from PersistentVolumes.
			// TODO can we make a stronger match at the underlying ETL layer?
			arts[key].Count++
			arts[key].AttachedVolumeCost += disk.Cost
			arts[key].AttachedVolumeCostAdjustment += disk.Adjustment
		} else {
			// Here, we're looking at a PersistentVolume because we're not
			// looking at an AttachedVolume.
			arts[key].Count++
			arts[key].PersistentVolumeCost += disk.Cost
			arts[key].PersistentVolumeCostAdjustment += disk.Adjustment
		}
	}

	return arts
}

// AssetTotalsSet represents totals, summed by both "cluster" and "node"
// for a given window of time.
type AssetTotalsSet struct {
	Cluster map[string]*AssetTotals `json:"cluster"`
	Node    map[string]*AssetTotals `json:"node"`
	Window  Window                  `json:"window"`
}

func NewAssetTotalsSet(window Window, byCluster, byNode map[string]*AssetTotals) *AssetTotalsSet {
	return &AssetTotalsSet{
		Cluster: byCluster,
		Node:    byNode,
		Window:  window.Clone(),
	}
}

// ComputeIdleCoefficients returns the idle coefficients for CPU, GPU, and RAM
// (in that order) for the given resource costs and totals.
func ComputeIdleCoefficients(shareSplit, key string, cpuCost, gpuCost, ramCost float64, allocationTotals map[string]*AllocationTotals) (float64, float64, float64) {
	if shareSplit == ShareNone {
		return 0.0, 0.0, 0.0
	}

	if shareSplit != ShareEven {
		shareSplit = ShareWeighted
	}

	var cpuCoeff, gpuCoeff, ramCoeff float64

	if _, ok := allocationTotals[key]; !ok {
		return 0.0, 0.0, 0.0
	}

	if shareSplit == ShareEven {
		coeff := 1.0 / float64(allocationTotals[key].Count)
		return coeff, coeff, coeff
	}

	if allocationTotals[key].TotalCPUCost() > 0 {
		cpuCoeff = cpuCost / allocationTotals[key].TotalCPUCost()
	}

	if allocationTotals[key].TotalGPUCost() > 0 {
		gpuCoeff = gpuCost / allocationTotals[key].TotalGPUCost()
	}

	if allocationTotals[key].TotalRAMCost() > 0 {
		ramCoeff = ramCost / allocationTotals[key].TotalRAMCost()
	}

	return cpuCoeff, gpuCoeff, ramCoeff
}

// TotalsStore acts as both an AllocationTotalsStore and an
// AssetTotalsStore.
type TotalsStore interface {
	AllocationTotalsStore
	AssetTotalsStore
}

// AllocationTotalsStore allows for storing (i.e. setting and
// getting) AllocationTotals by cluster and by node.
type AllocationTotalsStore interface {
	GetAllocationTotalsByCluster(start, end time.Time) (map[string]*AllocationTotals, bool)
	GetAllocationTotalsByNode(start, end time.Time) (map[string]*AllocationTotals, bool)
	SetAllocationTotalsByCluster(start, end time.Time, rts map[string]*AllocationTotals)
	SetAllocationTotalsByNode(start, end time.Time, rts map[string]*AllocationTotals)
}

// UpdateAllocationTotalsStore updates an AllocationTotalsStore
// by totaling the given AllocationSet and saving the totals.
func UpdateAllocationTotalsStore(arts AllocationTotalsStore, as *AllocationSet) (*AllocationTotalsSet, error) {
	if arts == nil {
		return nil, errors.New("cannot update nil AllocationTotalsStore")
	}

	if as == nil {
		return nil, errors.New("cannot update AllocationTotalsStore from nil AllocationSet")
	}

	if as.Window.IsOpen() {
		return nil, errors.New("cannot update AllocationTotalsStore from AllocationSet with open window")
	}

	start := *as.Window.Start()
	end := *as.Window.End()

	artsByCluster := ComputeAllocationTotals(as, AllocationClusterProp)
	arts.SetAllocationTotalsByCluster(start, end, artsByCluster)

	artsByNode := ComputeAllocationTotals(as, AllocationNodeProp)
	arts.SetAllocationTotalsByNode(start, end, artsByNode)

	log.Debugf("ETL: Allocation: updated resource totals for %s", as.Window)

	win := NewClosedWindow(start, end)

	abc := map[string]*AllocationTotals{}
	for key, val := range artsByCluster {
		abc[key] = val.Clone()
	}

	abn := map[string]*AllocationTotals{}
	for key, val := range artsByNode {
		abn[key] = val.Clone()
	}

	return NewAllocationTotalsSet(win, abc, abn), nil
}

// AssetTotalsStore allows for storing (i.e. setting and getting)
// AssetTotals by cluster and by node.
type AssetTotalsStore interface {
	GetAssetTotalsByCluster(start, end time.Time) (map[string]*AssetTotals, bool)
	GetAssetTotalsByNode(start, end time.Time) (map[string]*AssetTotals, bool)
	SetAssetTotalsByCluster(start, end time.Time, rts map[string]*AssetTotals)
	SetAssetTotalsByNode(start, end time.Time, rts map[string]*AssetTotals)
}

// UpdateAssetTotalsStore updates an AssetTotalsStore
// by totaling the given AssetSet and saving the totals.
func UpdateAssetTotalsStore(arts AssetTotalsStore, as *AssetSet) (*AssetTotalsSet, error) {
	if arts == nil {
		return nil, errors.New("cannot update nil AssetTotalsStore")
	}

	if as == nil {
		return nil, errors.New("cannot update AssetTotalsStore from nil AssetSet")
	}

	if as.Window.IsOpen() {
		return nil, errors.New("cannot update AssetTotalsStore from AssetSet with open window")
	}

	start := *as.Window.Start()
	end := *as.Window.End()

	artsByCluster := ComputeAssetTotals(as, false)
	arts.SetAssetTotalsByCluster(start, end, artsByCluster)

	artsByNode := ComputeAssetTotals(as, true)
	arts.SetAssetTotalsByNode(start, end, artsByNode)

	log.Debugf("ETL: Asset: updated resource totals for %s", as.Window)

	win := NewClosedWindow(start, end)

	abc := map[string]*AssetTotals{}
	for key, val := range artsByCluster {
		abc[key] = val.Clone()
	}

	abn := map[string]*AssetTotals{}
	for key, val := range artsByNode {
		abn[key] = val.Clone()
	}

	return NewAssetTotalsSet(win, abc, abn), nil
}

// MemoryTotalsStore is an in-memory cache TotalsStore
type MemoryTotalsStore struct {
	allocTotalsByCluster *cache.Cache
	allocTotalsByNode    *cache.Cache
	assetTotalsByCluster *cache.Cache
	assetTotalsByNode    *cache.Cache
}

// NewMemoryTotalsStore instantiates a new MemoryTotalsStore,
// which is composed of four in-memory caches.
func NewMemoryTotalsStore() *MemoryTotalsStore {
	return &MemoryTotalsStore{
		allocTotalsByCluster: cache.New(cache.NoExpiration, cache.NoExpiration),
		allocTotalsByNode:    cache.New(cache.NoExpiration, cache.NoExpiration),
		assetTotalsByCluster: cache.New(cache.NoExpiration, cache.NoExpiration),
		assetTotalsByNode:    cache.New(cache.NoExpiration, cache.NoExpiration),
	}
}

// GetAllocationTotalsByCluster retrieves the AllocationTotals
// by cluster for the given start and end times.
func (mts *MemoryTotalsStore) GetAllocationTotalsByCluster(start time.Time, end time.Time) (map[string]*AllocationTotals, bool) {
	k := storeKey(start, end)
	if raw, ok := mts.allocTotalsByCluster.Get(k); !ok {
		return map[string]*AllocationTotals{}, false
	} else {
		original := raw.(map[string]*AllocationTotals)
		totals := make(map[string]*AllocationTotals, len(original))
		for k, v := range original {
			totals[k] = v.Clone()
		}
		return totals, true
	}
}

// GetAllocationTotalsByNode retrieves the AllocationTotals
// by node for the given start and end times.
func (mts *MemoryTotalsStore) GetAllocationTotalsByNode(start time.Time, end time.Time) (map[string]*AllocationTotals, bool) {
	k := storeKey(start, end)
	if raw, ok := mts.allocTotalsByNode.Get(k); !ok {
		return map[string]*AllocationTotals{}, false
	} else {
		original := raw.(map[string]*AllocationTotals)
		totals := make(map[string]*AllocationTotals, len(original))
		for k, v := range original {
			totals[k] = v.Clone()
		}
		return totals, true
	}
}

// SetAllocationTotalsByCluster set the per-cluster AllocationTotals
// to the given values for the given start and end times.
func (mts *MemoryTotalsStore) SetAllocationTotalsByCluster(start time.Time, end time.Time, arts map[string]*AllocationTotals) {
	k := storeKey(start, end)
	mts.allocTotalsByCluster.Set(k, arts, cache.NoExpiration)
}

// SetAllocationTotalsByNode set the per-node AllocationTotals
// to the given values for the given start and end times.
func (mts *MemoryTotalsStore) SetAllocationTotalsByNode(start time.Time, end time.Time, arts map[string]*AllocationTotals) {
	k := storeKey(start, end)
	mts.allocTotalsByNode.Set(k, arts, cache.NoExpiration)
}

// GetAssetTotalsByCluster retrieves the AssetTotals
// by cluster for the given start and end times.
func (mts *MemoryTotalsStore) GetAssetTotalsByCluster(start time.Time, end time.Time) (map[string]*AssetTotals, bool) {
	k := storeKey(start, end)
	if raw, ok := mts.assetTotalsByCluster.Get(k); !ok {
		return map[string]*AssetTotals{}, false
	} else {
		original := raw.(map[string]*AssetTotals)
		totals := make(map[string]*AssetTotals, len(original))
		for k, v := range original {
			totals[k] = v.Clone()
		}
		return totals, true
	}
}

// GetAssetTotalsByNode retrieves the AssetTotals
// by node for the given start and end times.
func (mts *MemoryTotalsStore) GetAssetTotalsByNode(start time.Time, end time.Time) (map[string]*AssetTotals, bool) {
	k := storeKey(start, end)
	if raw, ok := mts.assetTotalsByNode.Get(k); !ok {
		// it's possible that after accumulation, the time chunks stored here
		// are being queried combined
		return map[string]*AssetTotals{}, false
	} else {
		original := raw.(map[string]*AssetTotals)
		totals := make(map[string]*AssetTotals, len(original))
		for k, v := range original {
			totals[k] = v.Clone()
		}
		return totals, true
	}
}

// SetAssetTotalsByCluster set the per-cluster AssetTotals
// to the given values for the given start and end times.
func (mts *MemoryTotalsStore) SetAssetTotalsByCluster(start time.Time, end time.Time, arts map[string]*AssetTotals) {
	k := storeKey(start, end)
	mts.assetTotalsByCluster.Set(k, arts, cache.NoExpiration)
}

// SetAssetTotalsByNode set the per-node AssetTotals
// to the given values for the given start and end times.
func (mts *MemoryTotalsStore) SetAssetTotalsByNode(start time.Time, end time.Time, arts map[string]*AssetTotals) {
	k := storeKey(start, end)
	mts.assetTotalsByNode.Set(k, arts, cache.NoExpiration)
}

// storeKey creates a storage key based on start and end times
func storeKey(start, end time.Time) string {
	startStr := strconv.FormatInt(start.Unix(), 10)
	endStr := strconv.FormatInt(end.Unix(), 10)
	return fmt.Sprintf("%s-%s", startStr, endStr)
}
