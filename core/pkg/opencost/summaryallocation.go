package opencost

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/opencost/opencost/core/pkg/filter/ast"
	"github.com/opencost/opencost/core/pkg/filter/matcher"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
)

// SummaryAllocation summarizes an Allocation, keeping only fields necessary
// for providing a high-level view of identifying the Allocation over a period
// of time (Start, End) over which it ran, and inspecting the associated per-
// resource costs (subtotaled with adjustments), total cost, and efficiency.
//
// SummaryAllocation does not have a concept of Window (i.e. the time period
// within which it is defined, as opposed to the Start and End times). That
// context must be provided by a SummaryAllocationSet.
type SummaryAllocation struct {
	Name                   string                `json:"name"`
	Properties             *AllocationProperties `json:"-"`
	Start                  time.Time             `json:"start"`
	End                    time.Time             `json:"end"`
	CPUCoreRequestAverage  float64               `json:"cpuCoreRequestAverage"`
	CPUCoreUsageAverage    float64               `json:"cpuCoreUsageAverage"`
	CPUCost                float64               `json:"cpuCost"`
	CPUCostIdle            float64               `json:"cpuCostIdle"`
	GPURequestAverage      *float64              `json:"gpuRequestAverage"`
	GPUUsageAverage        *float64              `json:"gpuUsageAverage"`
	GPUCost                float64               `json:"gpuCost"`
	GPUCostIdle            float64               `json:"gpuCostIdle"`
	NetworkCost            float64               `json:"networkCost"`
	LoadBalancerCost       float64               `json:"loadBalancerCost"`
	PVCost                 float64               `json:"pvCost"`
	RAMBytesRequestAverage float64               `json:"ramByteRequestAverage"`
	RAMBytesUsageAverage   float64               `json:"ramByteUsageAverage"`
	RAMCost                float64               `json:"ramCost"`
	RAMCostIdle            float64               `json:"ramCostIdle"`
	SharedCost             float64               `json:"sharedCost"`
	ExternalCost           float64               `json:"externalCost"`
	Share                  bool                  `json:"-"`
	UnmountedPVCost        float64               `json:"-"`
	Efficiency             float64               `json:"efficiency"`
}

// NewSummaryAllocation converts an Allocation to a SummaryAllocation by
// dropping unnecessary fields and consolidating others (e.g. adjustments).
// Reconciliation happens here because that process is synonymous with the
// consolidation of adjustment fields.
func NewSummaryAllocation(alloc *Allocation, reconcile, reconcileNetwork bool) *SummaryAllocation {
	if alloc == nil {
		return nil
	}

	var gpuRequestAvg, gpuUsageAvg *float64
	if alloc.GPUAllocation != nil {
		gpuRequestAvg = alloc.GPUAllocation.GPURequestAverage
		gpuUsageAvg = alloc.GPUAllocation.GPUUsageAverage
	}

	sa := &SummaryAllocation{
		Name:                   alloc.Name,
		Properties:             alloc.Properties,
		Start:                  alloc.Start,
		End:                    alloc.End,
		CPUCoreRequestAverage:  alloc.CPUCoreRequestAverage,
		CPUCoreUsageAverage:    alloc.CPUCoreUsageAverage,
		CPUCost:                alloc.CPUCost + alloc.CPUCostAdjustment,
		CPUCostIdle:            alloc.CPUCostIdle,
		GPURequestAverage:      gpuRequestAvg,
		GPUUsageAverage:        gpuUsageAvg,
		GPUCost:                alloc.GPUCost + alloc.GPUCostAdjustment,
		GPUCostIdle:            alloc.GPUCostIdle,
		NetworkCost:            alloc.NetworkCost + alloc.NetworkCostAdjustment,
		LoadBalancerCost:       alloc.LoadBalancerCost + alloc.LoadBalancerCostAdjustment,
		PVCost:                 alloc.PVCost() + alloc.PVCostAdjustment,
		RAMBytesRequestAverage: alloc.RAMBytesRequestAverage,
		RAMBytesUsageAverage:   alloc.RAMBytesUsageAverage,
		RAMCost:                alloc.RAMCost + alloc.RAMCostAdjustment,
		RAMCostIdle:            alloc.RAMCostIdle,
		SharedCost:             alloc.SharedCost,
		ExternalCost:           alloc.ExternalCost,
		UnmountedPVCost:        alloc.UnmountedPVCost,
	}

	// Revert adjustments if reconciliation is off. If only network
	// reconciliation is off, only revert network adjustment.
	if !reconcile {
		sa.CPUCost -= alloc.CPUCostAdjustment
		sa.GPUCost -= alloc.GPUCostAdjustment
		sa.NetworkCost -= alloc.NetworkCostAdjustment
		sa.LoadBalancerCost -= alloc.LoadBalancerCostAdjustment
		sa.PVCost -= alloc.PVCostAdjustment
		sa.RAMCost -= alloc.RAMCostAdjustment
	} else if !reconcileNetwork {
		sa.NetworkCost -= alloc.NetworkCostAdjustment
	}

	// If the allocation is unmounted, set UnmountedPVCost to the full PVCost.
	if sa.IsUnmounted() {
		sa.UnmountedPVCost = sa.PVCost
	}
	sa.Efficiency = sa.TotalEfficiency()
	return sa
}

// Add sums two SummaryAllocations, adding the given SummaryAllocation to the
// receiving one, thus mutating the receiver. For performance reasons, it
// simply drops Properties, so a SummaryAllocation can only be Added once.
func (sa *SummaryAllocation) Add(that *SummaryAllocation) error {
	if sa == nil || that == nil {
		return errors.New("cannot Add a nil SummaryAllocation")
	}

	// Once Added, a SummaryAllocation has no Properties. This saves us from
	// having to compute the intersection of two sets of Properties, which is
	// expensive.
	sa.Properties = nil

	// Sum non-cumulative fields by turning them into cumulative, adding them,
	// and then converting them back into averages after minutes have been
	// combined (just below).
	cpuReqCoreMins := sa.CPUCoreRequestAverage * sa.Minutes()
	cpuReqCoreMins += that.CPUCoreRequestAverage * that.Minutes()

	cpuUseCoreMins := sa.CPUCoreUsageAverage * sa.Minutes()
	cpuUseCoreMins += that.CPUCoreUsageAverage * that.Minutes()

	ramReqByteMins := sa.RAMBytesRequestAverage * sa.Minutes()
	ramReqByteMins += that.RAMBytesRequestAverage * that.Minutes()

	ramUseByteMins := sa.RAMBytesUsageAverage * sa.Minutes()
	ramUseByteMins += that.RAMBytesUsageAverage * that.Minutes()

	var gpuReqMins *float64 = nil
	if sa.GPURequestAverage != nil {
		result := *sa.GPURequestAverage * sa.Minutes()
		gpuReqMins = &result
	}

	if sa.GPURequestAverage != nil && that.GPURequestAverage != nil {
		if gpuReqMins == nil {
			result := *that.GPURequestAverage * that.Minutes()
			gpuReqMins = &result
		} else {
			result := *gpuReqMins + *that.GPURequestAverage*that.Minutes()
			gpuReqMins = &result
		}
	}

	var gpuUseMins *float64 = nil
	if sa.GPUUsageAverage != nil {
		result := *sa.GPUUsageAverage * sa.Minutes()
		gpuUseMins = &result
	}

	if that.GPUUsageAverage != nil {
		if gpuUseMins == nil {
			result := *that.GPUUsageAverage * that.Minutes()
			gpuUseMins = &result
		} else {
			result := *gpuUseMins + *that.GPUUsageAverage*that.Minutes()
			gpuUseMins = &result
		}
	}

	// Expand Start and End to be the "max" of among the given Allocations
	if that.Start.Before(sa.Start) {
		sa.Start = that.Start
	}
	if that.End.After(sa.End) {
		sa.End = that.End
	}

	// Convert cumulative request and usage back into rates
	if sa.Minutes() > 0 {
		sa.CPUCoreRequestAverage = cpuReqCoreMins / sa.Minutes()
		sa.CPUCoreUsageAverage = cpuUseCoreMins / sa.Minutes()
		sa.RAMBytesRequestAverage = ramReqByteMins / sa.Minutes()
		sa.RAMBytesUsageAverage = ramUseByteMins / sa.Minutes()

		var gpuReqAvgVal, gpuUsageAvgVal *float64
		if gpuReqMins != nil {
			result := *gpuReqMins / sa.Minutes()
			gpuReqAvgVal = &result
		}

		if gpuUseMins != nil {
			result := *gpuUseMins / sa.Minutes()
			gpuUsageAvgVal = &result
		}

		sa.GPURequestAverage = gpuReqAvgVal
		sa.GPUUsageAverage = gpuUsageAvgVal
	} else {
		sa.CPUCoreRequestAverage = 0.0
		sa.CPUCoreUsageAverage = 0.0
		sa.RAMBytesRequestAverage = 0.0
		sa.RAMBytesUsageAverage = 0.0
		sa.GPURequestAverage = nil
		sa.GPUUsageAverage = nil
	}

	// Sum all cumulative cost fields
	sa.CPUCost += that.CPUCost
	sa.ExternalCost += that.ExternalCost
	sa.GPUCost += that.GPUCost
	sa.LoadBalancerCost += that.LoadBalancerCost
	sa.NetworkCost += that.NetworkCost
	sa.PVCost += that.PVCost
	sa.RAMCost += that.RAMCost
	sa.SharedCost += that.SharedCost

	sa.Efficiency = sa.TotalEfficiency()
	return nil
}

// Clone copies the SummaryAllocation and returns the copy
func (sa *SummaryAllocation) Clone() *SummaryAllocation {
	return &SummaryAllocation{
		Name:                   sa.Name,
		Properties:             sa.Properties.Clone(),
		Start:                  sa.Start,
		End:                    sa.End,
		CPUCoreRequestAverage:  sa.CPUCoreRequestAverage,
		CPUCoreUsageAverage:    sa.CPUCoreUsageAverage,
		CPUCost:                sa.CPUCost,
		GPURequestAverage:      sa.GPURequestAverage,
		GPUUsageAverage:        sa.GPUUsageAverage,
		GPUCost:                sa.GPUCost,
		NetworkCost:            sa.NetworkCost,
		LoadBalancerCost:       sa.LoadBalancerCost,
		PVCost:                 sa.PVCost,
		RAMBytesRequestAverage: sa.RAMBytesRequestAverage,
		RAMBytesUsageAverage:   sa.RAMBytesUsageAverage,
		RAMCost:                sa.RAMCost,
		SharedCost:             sa.SharedCost,
		ExternalCost:           sa.ExternalCost,
		Efficiency:             sa.Efficiency,
	}
}

// CPUEfficiency is the ratio of usage to request. If there is no request and
// no usage or cost, then efficiency is zero. If there is no request, but there
// is usage or cost, then efficiency is 100%.
func (sa *SummaryAllocation) CPUEfficiency() float64 {
	if sa == nil || sa.IsIdle() {
		return 0.0
	}

	if sa.CPUCoreRequestAverage > 0 {
		return sa.CPUCoreUsageAverage / sa.CPUCoreRequestAverage
	}

	if sa.CPUCoreUsageAverage == 0.0 || sa.CPUCost == 0.0 {
		return 0.0
	}

	return 1.0
}

func (sa *SummaryAllocation) Equal(that *SummaryAllocation) bool {
	if sa == nil || that == nil {
		return false
	}

	if sa.Name != that.Name {
		return false
	}

	if sa.Start != that.Start {
		return false
	}

	if sa.End != that.End {
		return false
	}

	if sa.CPUCoreRequestAverage != that.CPUCoreRequestAverage {
		return false
	}

	if sa.CPUCoreUsageAverage != that.CPUCoreUsageAverage {
		return false
	}

	if sa.CPUCost != that.CPUCost {
		return false
	}

	if sa.GPURequestAverage != that.GPURequestAverage {
		return false
	}

	if sa.GPUUsageAverage != that.GPUUsageAverage {
		return false
	}

	if sa.GPUCost != that.GPUCost {
		return false
	}

	if sa.NetworkCost != that.NetworkCost {
		return false
	}

	if sa.LoadBalancerCost != that.LoadBalancerCost {
		return false
	}

	if sa.PVCost != that.PVCost {
		return false
	}

	if sa.RAMBytesRequestAverage != that.RAMBytesRequestAverage {
		return false
	}

	if sa.RAMBytesUsageAverage != that.RAMBytesUsageAverage {
		return false
	}

	if sa.RAMCost != that.RAMCost {
		return false
	}

	if sa.SharedCost != that.SharedCost {
		return false
	}

	if sa.ExternalCost != that.ExternalCost {
		return false
	}

	return true
}

func (sa *SummaryAllocation) generateKey(aggregateBy []string, labelConfig *LabelConfig) string {
	if sa == nil {
		return ""
	}

	return sa.Properties.GenerateKey(aggregateBy, labelConfig)
}

// IsExternal is true if the given SummaryAllocation represents external costs.
func (sa *SummaryAllocation) IsExternal() bool {
	if sa == nil {
		return false
	}

	return strings.Contains(sa.Name, ExternalSuffix)
}

// IsIdle is true if the given SummaryAllocation represents idle costs.
func (sa *SummaryAllocation) IsIdle() bool {
	if sa == nil {
		return false
	}

	return strings.Contains(sa.Name, IdleSuffix)
}

// IsUnallocated is true if the given SummaryAllocation represents unallocated
// costs.
func (sa *SummaryAllocation) IsUnallocated() bool {
	if sa == nil {
		return false
	}

	return strings.Contains(sa.Name, UnallocatedSuffix)
}

// IsUnmounted is true if the given SummaryAllocation represents unmounted
// volume costs.
func (sa *SummaryAllocation) IsUnmounted() bool {
	if sa == nil {
		return false
	}

	return strings.Contains(sa.Name, UnmountedSuffix)
}

// Minutes returns the number of minutes the SummaryAllocation represents, as
// defined by the difference between the end and start times.
func (sa *SummaryAllocation) Minutes() float64 {
	if sa == nil {
		return 0.0
	}

	return sa.End.Sub(sa.Start).Minutes()
}

// RAMEfficiency is the ratio of usage to request. If there is no request and
// no usage or cost, then efficiency is zero. If there is no request, but there
// is usage or cost, then efficiency is 100%.
func (sa *SummaryAllocation) RAMEfficiency() float64 {
	if sa == nil || sa.IsIdle() {
		return 0.0
	}

	if sa.RAMBytesRequestAverage > 0 {
		return sa.RAMBytesUsageAverage / sa.RAMBytesRequestAverage
	}

	if sa.RAMBytesUsageAverage == 0.0 || sa.RAMCost == 0.0 {
		return 0.0
	}

	return 1.0
}

// TotalCost is the total cost of the SummaryAllocation
func (sa *SummaryAllocation) TotalCost() float64 {
	if sa == nil {
		return 0.0
	}

	return sa.CPUCost + sa.GPUCost + sa.RAMCost + sa.PVCost + sa.NetworkCost + sa.LoadBalancerCost + sa.SharedCost + sa.ExternalCost
}

// TotalEfficiency is the cost-weighted average of CPU and RAM efficiency. If
// there is no cost at all, then efficiency is zero.
func (sa *SummaryAllocation) TotalEfficiency() float64 {
	if sa == nil || sa.IsIdle() {
		return 0.0
	}

	if sa.RAMCost+sa.CPUCost > 0 {
		ramCostEff := sa.RAMEfficiency() * sa.RAMCost
		cpuCostEff := sa.CPUEfficiency() * sa.CPUCost
		return (ramCostEff + cpuCostEff) / (sa.CPUCost + sa.RAMCost)
	}

	return 0.0
}

// SummaryAllocationSet stores a set of SummaryAllocations, each with a unique
// name, that share a window. An AllocationSet is mutable, so treat it like a
// threadsafe map.
type SummaryAllocationSet struct {
	sync.RWMutex
	externalKeys       map[string]bool
	idleKeys           map[string]bool
	SummaryAllocations map[string]*SummaryAllocation `json:"allocations"`
	Window             Window                        `json:"window"`
}

// NewSummaryAllocationSet converts an AllocationSet to a SummaryAllocationSet.
// Filter functions, keep functions, and reconciliation parameters are
// required for unfortunate reasons to do with performance and legacy order-of-
// operations details, as well as the fact that reconciliation has been
// pushed down to the conversion step between Allocation and SummaryAllocation.
//
// This filter is an AllocationMatcher, not an AST, because at this point we
// already have the data and want to make sure that the filter has already
// gone through a compile step to deal with things like aliases.
func NewSummaryAllocationSet(as *AllocationSet, filter, keep AllocationMatcher, reconcile, reconcileNetwork bool) *SummaryAllocationSet {
	if as == nil {
		return nil
	}

	// If we can know the exact size of the map, use it. If filters or sharing
	// functions are present, we can't know the size, so we make a default map.
	var sasMap map[string]*SummaryAllocation
	if filter == nil {
		// No filters, so make the map of summary allocations exactly the size
		// of the origin allocation set.
		sasMap = make(map[string]*SummaryAllocation, len(as.Allocations))
	} else {
		// There are filters, so start with a standard map
		sasMap = make(map[string]*SummaryAllocation)
	}

	sas := &SummaryAllocationSet{
		SummaryAllocations: sasMap,
		Window:             as.Window.Clone(),
	}

	for _, alloc := range as.Allocations {
		// First, detect if the allocation should be kept. If so, mark it as
		// such, insert it, and continue.
		if keep != nil && keep.Matches(alloc) {
			sa := NewSummaryAllocation(alloc, reconcile, reconcileNetwork)
			sa.Share = true
			sas.Insert(sa)
			continue
		}

		// If the allocation does not pass any of the given filter functions,
		// do not insert it into the set.
		if filter != nil && !filter.Matches(alloc) {
			continue
		}

		err := sas.Insert(NewSummaryAllocation(alloc, reconcile, reconcileNetwork))
		if err != nil {
			log.Errorf("SummaryAllocation: error inserting summary of %s", alloc.Name)
		}
	}

	for key := range as.ExternalKeys {
		sas.externalKeys[key] = true
	}

	for key := range as.IdleKeys {
		sas.idleKeys[key] = true
	}

	return sas
}

// Clone creates a deep copy of the SummaryAllocationSet
func (sas *SummaryAllocationSet) Clone() *SummaryAllocationSet {
	sas.RLock()
	defer sas.RUnlock()

	externalKeys := make(map[string]bool, len(sas.externalKeys))
	for k, v := range sas.externalKeys {
		externalKeys[k] = v
	}
	idleKeys := make(map[string]bool, len(sas.idleKeys))
	for k, v := range sas.idleKeys {
		idleKeys[k] = v
	}
	summaryAllocations := make(map[string]*SummaryAllocation, len(sas.SummaryAllocations))
	for k, v := range sas.SummaryAllocations {
		summaryAllocations[k] = v.Clone()
	}

	return &SummaryAllocationSet{
		externalKeys:       externalKeys,
		idleKeys:           idleKeys,
		SummaryAllocations: summaryAllocations,
		Window:             sas.Window.Clone(),
	}
}

// Add sums two SummaryAllocationSets, which Adds all SummaryAllocations in the
// given SummaryAllocationSet to their counterparts in the receiving set. Add
// also expands the Window to include both constituent Windows, in the case
// that Add is being used from accumulating (as opposed to aggregating). For
// performance reasons, the function may return either a new set, or an
// unmodified original, so it should not be assumed that the original sets are
// safeuly usable after calling Add.
func (sas *SummaryAllocationSet) Add(that *SummaryAllocationSet) (*SummaryAllocationSet, error) {
	if sas == nil || len(sas.SummaryAllocations) == 0 {
		return that, nil
	}

	if that == nil || len(that.SummaryAllocations) == 0 {
		return sas, nil
	}

	if sas.Window.IsOpen() {
		return nil, errors.New("cannot add a SummaryAllocationSet with an open window")
	}

	// Set start, end to min(start), max(end)
	start := *sas.Window.Start()
	end := *sas.Window.End()
	if that.Window.Start().Before(start) {
		start = *that.Window.Start()
	}
	if that.Window.End().After(end) {
		end = *that.Window.End()
	}

	acc := &SummaryAllocationSet{
		SummaryAllocations: make(map[string]*SummaryAllocation, len(sas.SummaryAllocations)),
		Window:             NewClosedWindow(start, end),
	}

	sas.RLock()
	defer sas.RUnlock()

	that.RLock()
	defer that.RUnlock()

	for _, alloc := range sas.SummaryAllocations {
		err := acc.Insert(alloc)
		if err != nil {
			return nil, err
		}
	}

	for _, alloc := range that.SummaryAllocations {
		err := acc.Insert(alloc)
		if err != nil {
			return nil, err
		}
	}

	return acc, nil
}

func (sas *SummaryAllocationSet) GetUnmountedPVCost() float64 {
	upvc := 0.0

	for _, sa := range sas.SummaryAllocations {
		upvc += sa.UnmountedPVCost
	}

	return upvc
}

// AggregateBy aggregates the Allocations in the given AllocationSet by the given
// AllocationProperty. This will only be legal if the AllocationSet is divisible by the
// given AllocationProperty; e.g. Containers can be divided by Namespace, but not vice-a-versa.
func (sas *SummaryAllocationSet) AggregateBy(aggregateBy []string, options *AllocationAggregationOptions) error {
	if sas == nil || len(sas.SummaryAllocations) == 0 {
		return nil
	}

	if sas.Window.IsOpen() {
		return errors.New("cannot aggregate a SummaryAllocationSet with an open window")
	}

	if options == nil {
		options = &AllocationAggregationOptions{}
	}

	if options.LabelConfig == nil {
		options.LabelConfig = NewLabelConfig()
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

	// Check if we have any work to do; if not, then early return. If
	// aggregateBy is nil, we don't aggregate anything. On the other hand,
	// an empty slice implies that we should aggregate everything. (See
	// generateKey for why that makes sense.)
	shouldAggregate := aggregateBy != nil
	shouldKeep := len(options.SharedHourlyCosts) > 0 || options.Share != nil
	if !shouldAggregate && !shouldKeep {
		return nil
	}

	// The order of operations for aggregating a SummaryAllotionSet is as
	// follows:
	//
	//  1. Partition external, idle, and shared allocations into separate sets.
	//     Also, create the resultSet into which the results will be aggregated.
	//
	//  2. Record resource totals for shared costs and unmounted volumes so
	//     that we can account for them in computing idle coefficients.
	//
	//  3. Retrieve pre-computed allocation resource totals, which will be used
	//     to compute idle sharing coefficients.
	//
	//  4. Convert shared hourly cost into a cumulative allocation to share,
	//     and insert it into the share set.
	//
	//  5. Compute sharing coefficients per-aggregation, if sharing resources.
	//
	//  6. Distribute idle allocations according to the idle coefficients.
	//
	//  7. Record allocation resource totals (after filtration) if filters have
	//     been applied. (Used for filtering proportional amount of idle.)
	//
	//  8. Generate aggregation key and insert allocation into the output set
	//
	//  9. If idle is shared and resources are shared, it's probable that some
	//     amount of idle cost will be shared with a shared resource.
	//     Distribute that idle cost, if it exists, among the respective shared
	//     allocations before sharing them with the aggregated allocations.
	//
	// 10. Apply idle filtration, which "filters" the idle cost, or scales it
	//     by the proportion of allocation resources remaining after filters
	//     have been applied.
	//
	// 11. Distribute shared resources according to sharing coefficients.
	//
	// 12. Insert external allocations into the result set.
	//
	// 13. Insert any undistributed idle, in the case that idle
	//     coefficients end up being zero and some idle is not shared.
	//
	// 14. Combine all idle allocations into a single idle allocation, unless
	//     the option to keep idle split by cluster or node is enabled.

	// 1. Partition external, idle, and shared allocations into separate sets.
	// Also, create the resultSet into which the results will be aggregated.

	// resultSet will collect the aggregated allocations
	resultSet := &SummaryAllocationSet{
		Window: sas.Window.Clone(),
	}

	// externalSet will collect external allocations
	externalSet := &SummaryAllocationSet{
		Window: sas.Window.Clone(),
	}

	// idleSet will be shared among resultSet after initial aggregation
	// is complete
	idleSet := &SummaryAllocationSet{
		Window: sas.Window.Clone(),
	}

	// shareSet will be shared among resultSet after initial aggregation
	// is complete
	shareSet := &SummaryAllocationSet{
		Window: sas.Window.Clone(),
	}

	sas.Lock()
	defer sas.Unlock()

	// 2. Record resource totals for shared costs, aggregating by cluster or by
	// node (depending on if idle is partitioned by cluster or node) so that we
	// can account for them in computing idle coefficients. Do the same for
	// unmounted volume costs, which only require a total cost.
	sharedResourceTotals := map[string]*AllocationTotals{}
	totalUnmountedCost := 0.0

	// 1 & 2. Identify set membership and aggregate aforementioned totals.
	for _, sa := range sas.SummaryAllocations {
		if sa.Share {
			var key string
			if options.IdleByNode {
				key = fmt.Sprintf("%s/%s", sa.Properties.Cluster, sa.Properties.Node)
			} else {
				key = sa.Properties.Cluster
			}

			if _, ok := sharedResourceTotals[key]; !ok {
				sharedResourceTotals[key] = &AllocationTotals{}
			}
			sharedResourceTotals[key].CPUCost += sa.CPUCost
			sharedResourceTotals[key].GPUCost += sa.GPUCost
			sharedResourceTotals[key].LoadBalancerCost += sa.LoadBalancerCost
			sharedResourceTotals[key].NetworkCost += sa.NetworkCost
			sharedResourceTotals[key].PersistentVolumeCost += sa.PVCost
			sharedResourceTotals[key].RAMCost += sa.RAMCost
			sharedResourceTotals[key].UnmountedPVCost += sa.UnmountedPVCost

			shareSet.Insert(sa)
			delete(sas.SummaryAllocations, sa.Name)

			continue
		}

		// External allocations get aggregated post-hoc (see step 6) and do
		// not necessarily contain complete sets of properties, so they are
		// moved to a separate AllocationSet.
		if sa.IsExternal() {
			delete(sas.externalKeys, sa.Name)
			delete(sas.SummaryAllocations, sa.Name)
			externalSet.Insert(sa)
			continue
		}

		// Idle allocations should be separated into idleSet if they are to be
		// shared later on. If they are not to be shared, then add them to the
		// resultSet like any other allocation.
		if sa.IsIdle() {
			delete(sas.idleKeys, sa.Name)
			delete(sas.SummaryAllocations, sa.Name)

			if options.ShareIdle == ShareEven || options.ShareIdle == ShareWeighted {
				idleSet.Insert(sa)
			} else {
				resultSet.Insert(sa)
			}

			continue
		}

		// Track total unmounted cost because it must be taken out of total
		// allocated costs for sharing coefficients.
		if sa.IsUnmounted() {
			totalUnmountedCost += sa.TotalCost()
		}
	}

	// It's possible that no more un-shared, non-idle, non-external allocations
	// remain at this point. This always results in an emptySet, so return early.
	if len(sas.SummaryAllocations) == 0 {
		sas.SummaryAllocations = map[string]*SummaryAllocation{}
		return nil
	}

	// 3. Retrieve pre-computed allocation resource totals, which will be used
	// to compute idle coefficients, based on the ratio of an allocation's per-
	// resource cost to the per-resource totals of that allocation's cluster or
	// node. Whether to perform this operation based on cluster or node is an
	// option. (See IdleByNode documentation; defaults to idle-by-cluster.)
	var allocTotals map[string]*AllocationTotals
	var ok bool
	if options.AllocationTotalsStore != nil {
		if options.IdleByNode {
			allocTotals, ok = options.AllocationTotalsStore.GetAllocationTotalsByNode(*sas.Window.Start(), *sas.Window.End())
			if !ok {
				return fmt.Errorf("nil allocation resource totals by node for %s", sas.Window)
			}
		} else {
			allocTotals, ok = options.AllocationTotalsStore.GetAllocationTotalsByCluster(*sas.Window.Start(), *sas.Window.End())
			if !ok {
				return fmt.Errorf("nil allocation resource totals by cluster for %s", sas.Window)
			}
		}
	}

	// If reconciliation has been fully or partially disabled, clear the
	// relevant adjustments from the alloc totals
	if allocTotals != nil && (!options.Reconcile || !options.ReconcileNetwork) {
		if !options.Reconcile {
			for _, tot := range allocTotals {
				tot.ClearAdjustments()
			}
		} else if !options.ReconcileNetwork {
			for _, tot := range allocTotals {
				tot.NetworkCostAdjustment = 0.0
			}
		}
	}

	// If filters have been applied, then we need to record allocation resource
	// totals after filtration (i.e. the allocations that are present) so that
	// we can identify the proportion of idle cost to keep. That is, we should
	// only return the idle cost that would be shared with the remaining
	// allocations, even if we're keeping idle separate. The totals should be
	// recorded by idle-key (cluster or node, depending on the IdleByNode
	// option). Instantiating this map is a signal to record the totals.
	var allocTotalsAfterFilters map[string]*AllocationTotals
	if len(resultSet.idleKeys) > 0 && options.Filter != nil {
		allocTotalsAfterFilters = make(map[string]*AllocationTotals, len(resultSet.idleKeys))
	}

	// If we're recording allocTotalsAfterFilters and there are shared costs,
	// then record those resource totals here so that idle for those shared
	// resources gets included.
	if allocTotalsAfterFilters != nil {
		for key, rt := range sharedResourceTotals {
			if _, ok := allocTotalsAfterFilters[key]; !ok {
				allocTotalsAfterFilters[key] = &AllocationTotals{}
			}

			// Record only those fields required for computing idle
			allocTotalsAfterFilters[key].CPUCost += rt.CPUCost
			allocTotalsAfterFilters[key].GPUCost += rt.GPUCost
			allocTotalsAfterFilters[key].RAMCost += rt.RAMCost
		}
	}

	// 4. Convert shared hourly cost into a cumulative allocation to share,
	// and insert it into the share set.
	for name, cost := range options.SharedHourlyCosts {
		if cost > 0.0 {
			hours := sas.Window.Hours()

			// If set ends in the future, adjust hours accordingly
			diff := time.Since(*sas.Window.End())
			if diff < 0.0 {
				hours += diff.Hours()
			}

			totalSharedCost := cost * hours

			shareSet.Insert(&SummaryAllocation{
				Name:       fmt.Sprintf("%s/%s", name, SharedSuffix),
				Properties: &AllocationProperties{},
				Start:      *sas.Window.Start(),
				End:        *sas.Window.End(),
				SharedCost: totalSharedCost,
			})
		}
	}

	// Sharing coefficients are recorded by post-aggregation-key (e.g. if
	// aggregating by namespace, then the key will be the namespace) and only
	// need to be recorded if there are shared resources. Instantiating this
	// map is the signal to record sharing coefficients.
	var sharingCoeffs map[string]float64
	if len(shareSet.SummaryAllocations) > 0 {
		sharingCoeffs = map[string]float64{}
	}

	// Loop over all remaining SummaryAllocations (after filters, sharing, &c.)
	// doing the following, in this order:
	//  5. Compute sharing coefficients, if there are shared resources
	//  6. Distribute idle cost, if sharing idle
	//  7. Record allocTotalsAfterFiltration, if filters have been applied
	//  8. Aggregate by key
	for _, sa := range sas.SummaryAllocations {
		// Generate key to use for aggregation-by-key and allocation name
		key := sa.generateKey(aggregateBy, options.LabelConfig)

		// 5. Incrementally add to sharing coefficients before adding idle
		// cost, which would skew the coefficients. These coefficients will be
		// later divided by a total, turning them into a coefficient between
		// 0.0 and 1.0.
		// NOTE: SummaryAllocation does not support ShareEven, so only record
		// by cost for cost-weighted distribution.
		// if sharingCoeffs != nil {
		if sharingCoeffs != nil && !sa.IsUnmounted() {
			sharingCoeffs[key] += sa.TotalCost() - sa.SharedCost - sa.UnmountedPVCost
		}

		// 6. Distribute idle allocations according to the idle coefficients.
		// NOTE: if idle allocation is off (i.e. options.ShareIdle: ShareNone)
		// then all idle allocations will be in the resultSet at this point, so
		// idleSet will be empty and we won't enter this block.
		if len(idleSet.SummaryAllocations) > 0 {
			for _, idle := range idleSet.SummaryAllocations {
				// Idle key is either cluster or node, as determined by the
				// IdleByNode option.
				var key string

				// Only share idle allocation with current allocation (sa) if
				// the relevant properties match (i.e. cluster and/or node)
				if idle.Properties.Cluster != sa.Properties.Cluster {
					continue
				}
				key = idle.Properties.Cluster

				if options.IdleByNode {
					if idle.Properties.Node != sa.Properties.Node {
						continue
					}
					key = fmt.Sprintf("%s/%s", idle.Properties.Cluster, idle.Properties.Node)
				}

				cpuCoeff, gpuCoeff, ramCoeff := ComputeIdleCoefficients(options.ShareIdle, key, sa.CPUCost, sa.GPUCost, sa.RAMCost, allocTotals)

				sa.CPUCost += idle.CPUCost * cpuCoeff
				sa.GPUCost += idle.GPUCost * gpuCoeff
				sa.RAMCost += idle.RAMCost * ramCoeff
			}
		}

		// The key becomes the allocation's name, which is used as the key by
		// which the allocation is inserted into the set.
		sa.Name = key

		// If merging unallocated allocations, rename all unallocated
		// allocations as simply __unallocated__
		if options.MergeUnallocated && sa.IsUnallocated() {
			sa.Name = UnallocatedSuffix
		}

		// 7. Record filtered resource totals for idle allocation filtration,
		// only if necessary.
		if allocTotalsAfterFilters != nil {
			key := sa.Properties.Cluster
			if options.IdleByNode {
				key = fmt.Sprintf("%s/%s", sa.Properties.Cluster, sa.Properties.Node)
			}

			if _, ok := allocTotalsAfterFilters[key]; !ok {
				allocTotalsAfterFilters[key] = &AllocationTotals{}
			}
			allocTotalsAfterFilters[key].CPUCost += sa.CPUCost
			allocTotalsAfterFilters[key].GPUCost += sa.GPUCost
			allocTotalsAfterFilters[key].RAMCost += sa.RAMCost
		}

		// 8. Inserting the allocation with the generated key for a name
		// performs the actual aggregation step.
		resultSet.Insert(sa)
	}

	// 9. If idle is shared and resources are shared, it's probable that some
	// amount of idle cost will be shared with a shared resource. Distribute
	// that idle cost, if it exists, among the respective shared allocations
	// before sharing them with the aggregated allocations.
	if len(idleSet.SummaryAllocations) > 0 && len(shareSet.SummaryAllocations) > 0 {
		for _, sa := range shareSet.SummaryAllocations {
			for _, idle := range idleSet.SummaryAllocations {
				var key string

				// Only share idle allocation with current allocation (sa) if
				// the relevant property matches (i.e. Cluster or Node,
				// depending on which idle sharing option is selected)
				if options.IdleByNode {
					if idle.Properties.Cluster != sa.Properties.Cluster || idle.Properties.Node != sa.Properties.Node {
						continue
					}

					key = fmt.Sprintf("%s/%s", idle.Properties.Cluster, idle.Properties.Node)
				} else {
					if idle.Properties.Cluster != sa.Properties.Cluster {
						continue
					}

					key = idle.Properties.Cluster
				}

				cpuCoeff, gpuCoeff, ramCoeff := ComputeIdleCoefficients(options.ShareIdle, key, sa.CPUCost, sa.GPUCost, sa.RAMCost, allocTotals)

				sa.CPUCost += idle.CPUCost * cpuCoeff
				sa.GPUCost += idle.GPUCost * gpuCoeff
				sa.RAMCost += idle.RAMCost * ramCoeff
			}
		}
	}

	// 10. Apply idle filtration, which "filters" the idle cost, i.e. scales
	// idle allocation costs per-resource by the proportion of allocation
	// resources remaining after filtering. In effect, this returns only the
	// idle costs that would have been shared with the remaining allocations,
	// even if idle is kept separated.
	if allocTotalsAfterFilters != nil {
		for idleKey := range resultSet.idleKeys {
			ia := resultSet.SummaryAllocations[idleKey]

			var key string
			if options.IdleByNode {
				key = fmt.Sprintf("%s/%s", ia.Properties.Cluster, ia.Properties.Node)
			} else {
				key = ia.Properties.Cluster
			}

			// Percentage of idle that should remain after filters are applied,
			// which equals the proportion of filtered-to-actual cost.
			cpuFilterCoeff := 0.0
			if allocTotals[key].TotalCPUCost() > 0.0 {
				filteredAlloc, ok := allocTotalsAfterFilters[key]
				if ok {
					cpuFilterCoeff = filteredAlloc.TotalCPUCost() / allocTotals[key].TotalCPUCost()
				} else {
					cpuFilterCoeff = 0.0
				}
			}

			gpuFilterCoeff := 0.0
			if allocTotals[key].TotalGPUCost() > 0.0 {
				filteredAlloc, ok := allocTotalsAfterFilters[key]
				if ok {
					gpuFilterCoeff = filteredAlloc.TotalGPUCost() / allocTotals[key].TotalGPUCost()
				} else {
					gpuFilterCoeff = 0.0
				}
			}

			ramFilterCoeff := 0.0
			if allocTotals[key].TotalRAMCost() > 0.0 {
				filteredAlloc, ok := allocTotalsAfterFilters[key]
				if ok {
					ramFilterCoeff = filteredAlloc.TotalRAMCost() / allocTotals[key].TotalRAMCost()
				} else {
					ramFilterCoeff = 0.0
				}
			}

			ia.CPUCost *= cpuFilterCoeff
			ia.GPUCost *= gpuFilterCoeff
			ia.RAMCost *= ramFilterCoeff
		}
	}

	// 11. Distribute shared resources according to sharing coefficients.
	// NOTE: ShareEven is not supported
	if len(shareSet.SummaryAllocations) > 0 {

		shareCoeffSum := 0.0

		sharingCoeffDenominator := 0.0
		for _, rt := range allocTotals {
			// Here, the allocation totals
			sharingCoeffDenominator += rt.TotalCost() // does NOT include unmounted PVs at all
		}

		// Do not include the shared costs, themselves, when determining
		// sharing coefficients.
		for _, rt := range sharedResourceTotals {
			// Due to the fact that sharingCoeffDenominator already has no
			// unmounted PV costs, we need to be careful not to additionally
			// subtract the unmounted PV cost when we remove shared costs
			// from the denominator.
			sharingCoeffDenominator -= (rt.TotalCost() - rt.UnmountedPVCost)
		}

		if sharingCoeffDenominator <= 0.0 {
			log.Warnf("SummaryAllocation: sharing coefficient denominator is %f", sharingCoeffDenominator)
		} else {

			// Compute sharing coeffs by dividing the thus-far accumulated
			// numerators by the now-finalized denominator.
			for key := range sharingCoeffs {
				// Do not share the value with unmounted suffix since it's not included in the computation.
				if key == UnmountedSuffix {
					continue
				}
				if sharingCoeffs[key] > 0.0 {
					sharingCoeffs[key] /= sharingCoeffDenominator
					shareCoeffSum += sharingCoeffs[key]
				} else {
					log.Warnf("SummaryAllocation: detected illegal sharing coefficient for %s: %v (setting to zero)", key, sharingCoeffs[key])
					sharingCoeffs[key] = 0.0
				}
			}

			for key, sa := range resultSet.SummaryAllocations {
				// Idle and unmounted allocations, by definition, do not
				// receive shared cost
				if sa.IsIdle() || sa.IsUnmounted() {
					continue
				}

				sharingCoeff := sharingCoeffs[key]

				// Distribute each shared cost with the current allocation on the
				// basis of the proportion of the allocation's cost (ShareWeighted)
				// or count (ShareEven) to the total aggregated cost or count. This
				// condition should hold in spite of filters because the sharing
				// coefficient denominator is held constant by pre-computed
				// resource totals and the post-aggregation total cost of the
				// remaining allocations will, by definition, not be affected.
				for _, shared := range shareSet.SummaryAllocations {
					sa.SharedCost += shared.TotalCost() * sharingCoeff
				}
			}
		}
	}

	// 12. Insert external allocations into the result set.
	for _, sa := range externalSet.SummaryAllocations {
		// Make an allocation with the same properties and test that
		// against the FilterFunc to see if the external allocation should
		// be filtered or not.
		// TODO:CLEANUP do something about external cost, this stinks
		ea := &Allocation{Properties: sa.Properties}

		if filter.Matches(ea) {
			key := sa.generateKey(aggregateBy, options.LabelConfig)

			sa.Name = key
			resultSet.Insert(sa)
		}
	}

	// 13. Distribute remaining, undistributed idle. Undistributed idle is any
	// per-resource idle cost for which there can be no idle coefficient
	// computed because there is zero usage across all allocations.
	for _, isa := range idleSet.SummaryAllocations {
		// Make an allocation with the same properties and test that
		// against the FilterFunc to see if the external allocation should
		// be filtered or not.
		// TODO:CLEANUP do something about external cost, this stinks
		ia := &Allocation{Properties: isa.Properties}
		// if the idle does not apply to the non-filtered values, skip it
		if !filter.Matches(ia) {
			continue
		}

		key := isa.Properties.Cluster
		if options.IdleByNode {
			key = fmt.Sprintf("%s/%s", isa.Properties.Cluster, isa.Properties.Node)
		}

		rt, ok := allocTotals[key]
		if !ok {
			log.Warnf("SummaryAllocation: AggregateBy: cannot handle undistributed idle for '%s'", key)
			continue
		}

		hasUndistributableCost := false

		if isa.CPUCost > 0.0 && rt.CPUCost == 0.0 {
			// There is idle CPU cost, but no allocated CPU cost, so that cost
			// is undistributable and must be inserted.
			hasUndistributableCost = true
		} else {
			// Cost was entirely distributed, so zero it out
			isa.CPUCost = 0.0
		}

		if isa.GPUCost > 0.0 && rt.GPUCost == 0.0 {
			// There is idle GPU cost, but no allocated GPU cost, so that cost
			// is undistributable and must be inserted.
			hasUndistributableCost = true
		} else {
			// Cost was entirely distributed, so zero it out
			isa.GPUCost = 0.0
		}

		if isa.RAMCost > 0.0 && rt.RAMCost == 0.0 {
			// There is idle CPU cost, but no allocated CPU cost, so that cost
			// is undistributable and must be inserted.
			hasUndistributableCost = true
		} else {
			// Cost was entirely distributed, so zero it out
			isa.RAMCost = 0.0
		}

		if hasUndistributableCost {
			isa.Name = fmt.Sprintf("%s/%s", key, IdleSuffix)
			resultSet.Insert(isa)
		}
	}

	// 14. Combine all idle allocations into a single idle allocation, unless
	// the option to keep idle split by cluster or node is enabled.
	if !options.SplitIdle {
		for _, ia := range resultSet.idleAllocations() {
			resultSet.Delete(ia.Name)
			ia.Name = IdleSuffix
			resultSet.Insert(ia)
		}
	}

	// Replace the existing set's data with the new, aggregated summary data
	sas.SummaryAllocations = resultSet.SummaryAllocations

	return nil
}

// Delete removes the allocation with the given name from the set
func (sas *SummaryAllocationSet) Delete(name string) {
	if sas == nil {
		return
	}

	sas.Lock()
	defer sas.Unlock()

	delete(sas.externalKeys, name)
	delete(sas.idleKeys, name)
	delete(sas.SummaryAllocations, name)
}

// Each invokes the given function for each SummaryAllocation in the set
func (sas *SummaryAllocationSet) Each(f func(string, *SummaryAllocation)) {
	if sas == nil {
		return
	}

	for k, a := range sas.SummaryAllocations {
		f(k, a)
	}
}

func (sas *SummaryAllocationSet) Equal(that *SummaryAllocationSet) bool {
	if sas == nil || that == nil {
		return false
	}

	sas.RLock()
	defer sas.RUnlock()

	if !sas.Window.Equal(that.Window) {
		return false
	}

	if len(sas.SummaryAllocations) != len(that.SummaryAllocations) {
		return false
	}

	for name, sa := range sas.SummaryAllocations {
		thatSA, ok := that.SummaryAllocations[name]
		if !ok {
			return false
		}

		if !sa.Equal(thatSA) {
			return false
		}
	}

	return true
}

// IdleAllocations returns a map of the idle allocations in the AllocationSet.
func (sas *SummaryAllocationSet) idleAllocations() map[string]*SummaryAllocation {
	idles := map[string]*SummaryAllocation{}

	if sas == nil || len(sas.SummaryAllocations) == 0 {
		return idles
	}

	sas.RLock()
	defer sas.RUnlock()

	for key := range sas.idleKeys {
		if sa, ok := sas.SummaryAllocations[key]; ok {
			idles[key] = sa
		}
	}

	return idles
}

// Insert aggregates the current entry in the SummaryAllocationSet by the given Allocation,
// but only if the Allocation is valid, i.e. matches the SummaryAllocationSet's window. If
// there is no existing entry, one is created. Nil error response indicates success.
func (sas *SummaryAllocationSet) Insert(sa *SummaryAllocation) error {
	if sas == nil {
		return fmt.Errorf("cannot insert into nil SummaryAllocationSet")
	}

	if sa == nil {
		return fmt.Errorf("cannot insert a nil SummaryAllocation")
	}

	sas.Lock()
	defer sas.Unlock()

	if sas.SummaryAllocations == nil {
		sas.SummaryAllocations = map[string]*SummaryAllocation{}
	}

	if sas.externalKeys == nil {
		sas.externalKeys = map[string]bool{}
	}

	if sas.idleKeys == nil {
		sas.idleKeys = map[string]bool{}
	}

	// Add the given Allocation to the existing entry, if there is one;
	// otherwise just set directly into allocations
	if _, ok := sas.SummaryAllocations[sa.Name]; ok {
		err := sas.SummaryAllocations[sa.Name].Add(sa)
		if err != nil {
			return fmt.Errorf("SummaryAllocationSet.Insert: error trying to Add: %s", err)
		}
	} else {
		sas.SummaryAllocations[sa.Name] = sa
	}

	// If the given Allocation is an external one, record that
	if sa.IsExternal() {
		sas.externalKeys[sa.Name] = true
	}

	// If the given Allocation is an idle one, record that
	if sa.IsIdle() {
		sas.idleKeys[sa.Name] = true
	}

	return nil
}

func (sas *SummaryAllocationSet) TotalCost() float64 {
	if sas == nil {
		return 0.0
	}

	sas.RLock()
	defer sas.RUnlock()

	tc := 0.0
	for _, sa := range sas.SummaryAllocations {
		tc += sa.TotalCost()
	}

	return tc
}

// RAMEfficiency func to calculate average RAM efficiency over SummaryAllocationSet
func (sas *SummaryAllocationSet) RAMEfficiency() float64 {
	if sas == nil {
		return 0.0
	}

	sas.RLock()
	defer sas.RUnlock()

	totalRAMBytesMinutesUsage := 0.0
	totalRAMBytesMinutesRequest := 0.0
	totalRAMCost := 0.0
	for _, sa := range sas.SummaryAllocations {
		if sa.IsIdle() {
			continue
		}
		totalRAMBytesMinutesUsage += sa.RAMBytesUsageAverage * sa.Minutes()
		totalRAMBytesMinutesRequest += sa.RAMBytesRequestAverage * sa.Minutes()
		totalRAMCost += sa.RAMCost
	}

	if totalRAMBytesMinutesRequest > 0 {
		return totalRAMBytesMinutesUsage / totalRAMBytesMinutesRequest
	}

	if totalRAMBytesMinutesUsage == 0.0 || totalRAMCost == 0.0 {
		return 0.0
	}

	return 1.0
}

// CPUEfficiency func to calculate average CPU efficiency over SummaryAllocationSet
func (sas *SummaryAllocationSet) CPUEfficiency() float64 {
	if sas == nil {
		return 0.0
	}

	sas.RLock()
	defer sas.RUnlock()

	totalCPUCoreMinutesUsage := 0.0
	totalCPUCoreMinutesRequest := 0.0
	totalCPUCost := 0.0
	for _, sa := range sas.SummaryAllocations {
		if sa.IsIdle() {
			continue
		}
		totalCPUCoreMinutesUsage += sa.CPUCoreUsageAverage * sa.Minutes()
		totalCPUCoreMinutesRequest += sa.CPUCoreRequestAverage * sa.Minutes()
		totalCPUCost += sa.CPUCost
	}

	if totalCPUCoreMinutesRequest > 0 {
		return totalCPUCoreMinutesUsage / totalCPUCoreMinutesRequest
	}

	if totalCPUCoreMinutesUsage == 0.0 || totalCPUCost == 0.0 {
		return 0.0
	}

	return 1.0
}

// TotalEfficiency func to calculate average Total efficiency over SummaryAllocationSet
func (sas *SummaryAllocationSet) TotalEfficiency() float64 {
	if sas == nil {
		return 0.0
	}

	sas.RLock()
	defer sas.RUnlock()

	totalRAMCost := 0.0
	totalCPUCost := 0.0
	for _, sa := range sas.SummaryAllocations {
		if sa.IsIdle() {
			continue
		}
		totalRAMCost += sa.RAMCost
		totalCPUCost += sa.CPUCost
	}

	if totalRAMCost+totalCPUCost > 0 {
		return (totalRAMCost*sas.RAMEfficiency() + totalCPUCost*sas.CPUEfficiency()) / (totalRAMCost + totalCPUCost)
	}

	return 0.0
}

// SummaryAllocationSetRange is a thread-safe slice of SummaryAllocationSets.
type SummaryAllocationSetRange struct {
	sync.RWMutex
	Step                  time.Duration           `json:"step"`
	SummaryAllocationSets []*SummaryAllocationSet `json:"sets"`
	Window                Window                  `json:"window"`
	Message               string                  `json:"-"`
}

// NewSummaryAllocationSetRange instantiates a new range composed of the given
// SummaryAllocationSets in the order provided. The expectations about the
// SummaryAllocationSets are as follows:
// - window durations are all equal
// - sets are consecutive (i.e. chronologically sorted)
// - there are no gaps between sets
// - sets do not have overlapping windows
func NewSummaryAllocationSetRange(sass ...*SummaryAllocationSet) *SummaryAllocationSetRange {
	var step time.Duration
	window := NewWindow(nil, nil)

	for _, sas := range sass {
		if window.Start() == nil || (sas.Window.Start() != nil && sas.Window.Start().Before(*window.Start())) {
			window.start = sas.Window.Start()
		}
		if window.End() == nil || (sas.Window.End() != nil && sas.Window.End().After(*window.End())) {
			window.end = sas.Window.End()
		}
		if step == 0 {
			step = sas.Window.Duration()
		} else if step != sas.Window.Duration() {
			log.Warnf("instantiating range with step %s using set of step %s is illegal", step, sas.Window.Duration())
		}
	}

	return &SummaryAllocationSetRange{
		Step:                  step,
		SummaryAllocationSets: sass,
		Window:                window,
	}
}

// Accumulate sums each AllocationSet in the given range, returning a single cumulative
// AllocationSet for the entire range.
func (sasr *SummaryAllocationSetRange) accumulate() (*SummaryAllocationSet, error) {
	var result *SummaryAllocationSet
	var err error

	sasr.RLock()
	defer sasr.RUnlock()

	for _, sas := range sasr.SummaryAllocationSets {
		result, err = result.Add(sas)
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

// newAccumulation clones the first available SummaryAllocationSet to use as the data structure to
// accumulate the remaining data. This leaves the original SummaryAllocationSetRange intact.
func (sasr *SummaryAllocationSetRange) newAccumulation() (*SummaryAllocationSet, error) {
	var result *SummaryAllocationSet
	var err error

	sasr.RLock()
	defer sasr.RUnlock()

	for _, sas := range sasr.SummaryAllocationSets {
		// we want to clone the first summary allocation set, then just Add the others
		// to the clone. We will eventually use the clone to create the set range.
		if result == nil {
			result = sas.Clone()
			continue
		}

		// Copy if sas is non-nil
		var sasCopy *SummaryAllocationSet = nil
		if sas != nil {
			sasCopy = sas.Clone()
		}

		// nil is ok to pass into Add
		result, err = result.Add(sasCopy)
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

// AggregateBy aggregates each AllocationSet in the range by the given
// properties and options.
func (sasr *SummaryAllocationSetRange) AggregateBy(aggregateBy []string, options *AllocationAggregationOptions) error {
	sasr.Lock()
	defer sasr.Unlock()

	for _, sas := range sasr.SummaryAllocationSets {
		err := sas.AggregateBy(aggregateBy, options)
		if err != nil {
			// Wipe out data so that corrupt data cannot be mistakenly used
			sasr.SummaryAllocationSets = []*SummaryAllocationSet{}
			return err
		}
	}

	return nil
}

// Append appends the given AllocationSet to the end of the range. It does not
// validate whether or not that violates window continuity.
func (sasr *SummaryAllocationSetRange) Append(sas *SummaryAllocationSet) {
	sasr.Lock()
	defer sasr.Unlock()

	// Append to list of sets
	sasr.SummaryAllocationSets = append(sasr.SummaryAllocationSets, sas)

	// Set step, if not set
	if sasr.Step == 0 {
		sasr.Step = sas.Window.Duration()
	}

	// Adjust window
	if sasr.Window.Start() == nil || (sas.Window.Start() != nil && sas.Window.Start().Before(*sasr.Window.Start())) {
		sasr.Window.start = sas.Window.Start()
	}
	if sasr.Window.End() == nil || (sas.Window.End() != nil && sas.Window.End().After(*sasr.Window.End())) {
		sasr.Window.end = sas.Window.End()
	}
}

// Each invokes the given function for each AllocationSet in the range
func (sasr *SummaryAllocationSetRange) Each(f func(int, *SummaryAllocationSet)) {
	if sasr == nil {
		return
	}

	for i, as := range sasr.SummaryAllocationSets {
		f(i, as)
	}
}

// InsertExternalAllocations takes all allocations in the given
// AllocationSetRange (they should all be considered "external") and inserts
// them into the receiving SummaryAllocationSetRange.
// TODO:CLEANUP replace this with a better idea (or get rid of external
// allocations, as such, altogether)
func (sasr *SummaryAllocationSetRange) InsertExternalAllocations(that *AllocationSetRange) error {
	if sasr == nil {
		return fmt.Errorf("cannot insert range into nil AllocationSetRange")
	}

	// keys maps window to index in range
	keys := map[string]int{}
	for i, as := range sasr.SummaryAllocationSets {
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
		sas := sasr.SummaryAllocationSets[i]

		// Insert each Allocation from the given set
		for _, alloc := range thatAS.Allocations {
			externalSA := NewSummaryAllocation(alloc, true, true)
			// This error will be returned below
			// TODO:CLEANUP should Each have early-error-return functionality?
			err = sas.Insert(externalSA)
		}
	}

	// err might be nil
	return err
}

func (sasr *SummaryAllocationSetRange) TotalCost() float64 {
	if sasr == nil {
		return 0.0
	}

	sasr.RLock()
	defer sasr.RUnlock()

	tc := 0.0
	for _, sas := range sasr.SummaryAllocationSets {
		tc += sas.TotalCost()
	}

	return tc
}

// TODO remove after testing
func (sasr *SummaryAllocationSetRange) Print(verbose bool) {
	fmt.Printf("%s (dur=%s, len=%d, cost=%.5f)\n", sasr.Window, sasr.Window.Duration(), len(sasr.SummaryAllocationSets), sasr.TotalCost())
	for _, sas := range sasr.SummaryAllocationSets {
		fmt.Printf(" > %s (dur=%s, len=%d, cost=%.5f) \n", sas.Window, sas.Window.Duration(), len(sas.SummaryAllocations), sas.TotalCost())
		for key, sa := range sas.SummaryAllocations {
			if verbose {
				fmt.Printf("   {\"%s\", cpu: %.5f, gpu: %.5f, lb: %.5f, net: %.5f, pv: %.5f, ram: %.5f, shared: %.5f, external: %.5f}\n",
					key, sa.CPUCost, sa.GPUCost, sa.LoadBalancerCost, sa.NetworkCost, sa.PVCost, sa.RAMCost, sa.SharedCost, sa.ExternalCost)
			} else {
				fmt.Printf("   - \"%s\": %.5f\n", key, sa.TotalCost())
			}
		}
	}
}

func (sasr *SummaryAllocationSetRange) Accumulate(accumulateBy AccumulateOption) (*SummaryAllocationSetRange, error) {
	switch accumulateBy {
	case AccumulateOptionNone:
		return sasr.accumulateByNone()
	case AccumulateOptionAll:
		return sasr.accumulateByAll()
	case AccumulateOptionHour:
		return sasr.accumulateByHour()
	case AccumulateOptionDay:
		return sasr.accumulateByDay()
	case AccumulateOptionWeek:
		return sasr.accumulateByWeek()
	case AccumulateOptionMonth:
		return sasr.accumulateByMonth()
	default:
		// this should never happen
		return nil, fmt.Errorf("unexpected error, invalid accumulateByType: %s", accumulateBy)
	}
}

func (sasr *SummaryAllocationSetRange) accumulateByNone() (*SummaryAllocationSetRange, error) {
	return sasr.clone(), nil
}

func (sasr *SummaryAllocationSetRange) accumulateByAll() (*SummaryAllocationSetRange, error) {
	var err error
	var result *SummaryAllocationSet
	result, err = sasr.newAccumulation()

	if err != nil {
		return nil, fmt.Errorf("error running accumulate: %s", err)
	}
	accumulated := NewSummaryAllocationSetRange(result)
	return accumulated, nil
}

func (sasr *SummaryAllocationSetRange) accumulateByHour() (*SummaryAllocationSetRange, error) {
	// ensure that the summary allocation sets have a 1-hour window, if a set exists
	if len(sasr.SummaryAllocationSets) > 0 && sasr.SummaryAllocationSets[0].Window.Duration() != time.Hour {
		return nil, fmt.Errorf("window duration must equal 1 hour; got:%s", sasr.SummaryAllocationSets[0].Window.Duration())
	}

	result := sasr.clone()

	return result, nil
}

func (sasr *SummaryAllocationSetRange) accumulateByDay() (*SummaryAllocationSetRange, error) {
	// if the summary allocation set window is 1-day, just return the existing summary allocation set range
	if len(sasr.SummaryAllocationSets) > 0 && sasr.SummaryAllocationSets[0].Window.Duration() == time.Hour*24 {
		return sasr, nil
	}

	var toAccumulate *SummaryAllocationSetRange
	result := NewSummaryAllocationSetRange()
	for i, as := range sasr.SummaryAllocationSets {

		if as.Window.Duration() != time.Hour {
			return nil, fmt.Errorf("window duration must equal 1 hour; got:%s", as.Window.Duration())
		}

		hour := as.Window.Start().Hour()

		if toAccumulate == nil {
			toAccumulate = NewSummaryAllocationSetRange()
			as = as.Clone()
		}

		toAccumulate.Append(as)
		sas, err := toAccumulate.accumulate()
		if err != nil {
			return nil, fmt.Errorf("error accumulating result: %s", err)
		}
		toAccumulate = NewSummaryAllocationSetRange(sas)

		if hour == 23 || i == len(sasr.SummaryAllocationSets)-1 {
			if length := len(toAccumulate.SummaryAllocationSets); length != 1 {
				return nil, fmt.Errorf("failed accumulation, detected %d sets instead of 1", length)
			}
			result.Append(toAccumulate.SummaryAllocationSets[0])
			toAccumulate = nil
		}
	}
	return result, nil
}

func (sasr *SummaryAllocationSetRange) accumulateByMonth() (*SummaryAllocationSetRange, error) {
	var toAccumulate *SummaryAllocationSetRange
	result := NewSummaryAllocationSetRange()
	for i, as := range sasr.SummaryAllocationSets {

		if as.Window.Duration() != time.Hour*24 {
			return nil, fmt.Errorf("window duration must equal 24 hours; got:%s", as.Window.Duration())
		}

		_, month, _ := as.Window.Start().Date()
		_, nextDayMonth, _ := as.Window.Start().Add(time.Hour * 24).Date()

		if toAccumulate == nil {
			toAccumulate = NewSummaryAllocationSetRange()
			as = as.Clone()
		}

		toAccumulate.Append(as)

		sas, err := toAccumulate.accumulate()
		if err != nil {
			return nil, fmt.Errorf("error building monthly accumulation: %s", err)
		}

		toAccumulate = NewSummaryAllocationSetRange(sas)

		// either the month has ended, or there are no more summary allocation sets
		if month != nextDayMonth || i == len(sasr.SummaryAllocationSets)-1 {
			if length := len(toAccumulate.SummaryAllocationSets); length != 1 {
				return nil, fmt.Errorf("failed accumulation, detected %d sets instead of 1", length)
			}
			result.Append(toAccumulate.SummaryAllocationSets[0])
			toAccumulate = nil
		}
	}
	return result, nil
}

func (sasr *SummaryAllocationSetRange) accumulateByWeek() (*SummaryAllocationSetRange, error) {
	if len(sasr.SummaryAllocationSets) > 0 && sasr.SummaryAllocationSets[0].Window.Duration() == timeutil.Week {
		return sasr, nil
	}

	var toAccumulate *SummaryAllocationSetRange
	result := NewSummaryAllocationSetRange()
	for i, as := range sasr.SummaryAllocationSets {
		if as.Window.Duration() != time.Hour*24 {
			return nil, fmt.Errorf("window duration must equal 24 hours; got:%s", as.Window.Duration())
		}

		dayOfWeek := as.Window.Start().Weekday()

		if toAccumulate == nil {
			toAccumulate = NewSummaryAllocationSetRange()
			as = as.Clone()
		}

		toAccumulate.Append(as)
		sas, err := toAccumulate.accumulate()
		if err != nil {
			return nil, fmt.Errorf("error accumulating result: %s", err)
		}
		toAccumulate = NewSummaryAllocationSetRange(sas)

		// current assumption is the week always ends on Saturday, or when there are no more summary allocation sets
		if dayOfWeek == time.Saturday || i == len(sasr.SummaryAllocationSets)-1 {
			if length := len(toAccumulate.SummaryAllocationSets); length != 1 {
				return nil, fmt.Errorf("failed accumulation, detected %d sets instead of 1", length)
			}
			result.Append(toAccumulate.SummaryAllocationSets[0])
			toAccumulate = nil
		}
	}
	return result, nil
}

func (sasr *SummaryAllocationSetRange) Clone() *SummaryAllocationSetRange {
	return sasr.clone()
}

// clone returns a new SummaryAllocationSetRange cloned from the existing SASR
func (sasr *SummaryAllocationSetRange) clone() *SummaryAllocationSetRange {
	sasrSource := NewSummaryAllocationSetRange()
	sasrSource.Window = sasr.Window.Clone()
	sasrSource.Step = sasr.Step
	sasrSource.Message = sasr.Message

	for _, sas := range sasr.SummaryAllocationSets {
		var sasClone *SummaryAllocationSet = nil
		if sas != nil {
			sasClone = sas.Clone()
		}

		sasrSource.Append(sasClone)
	}

	return sasrSource
}
