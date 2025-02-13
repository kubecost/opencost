package costmodel

import (
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/util/timeutil"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/pkg/env"
)

// Constants for Network Cost Subtype
const (
	networkCrossZoneCost   = "NetworkCrossZoneCost"
	networkCrossRegionCost = "NetworkCrossRegionCost"
	networkInternetCost    = "NetworkInternetCost"
)

// CanCompute should return true if CostModel can act as a valid source for the
// given time range. In the case of CostModel we want to attempt to compute as
// long as the range starts in the past. If the CostModel ends up not having
// data to match, that's okay, and should be communicated with an error
// response from ComputeAllocation.
func (cm *CostModel) CanCompute(start, end time.Time) bool {
	return start.Before(time.Now())
}

// Name returns the name of the Source
func (cm *CostModel) Name() string {
	return "CostModel"
}

// ComputeAllocation uses the CostModel instance to compute an AllocationSet
// for the window defined by the given start and end times. The Allocations
// returned are unaggregated (i.e. down to the container level).
func (cm *CostModel) ComputeAllocation(start, end time.Time, resolution time.Duration) (*opencost.AllocationSet, error) {

	// If the duration is short enough, compute the AllocationSet directly
	if end.Sub(start) <= cm.BatchDuration {
		as, _, err := cm.computeAllocation(start, end, resolution)
		return as, err
	}

	// If the duration exceeds the configured MaxPrometheusQueryDuration, then
	// query for maximum-sized AllocationSets, collect them, and accumulate.

	// s and e track the coverage of the entire given window over multiple
	// internal queries.
	s, e := start, start

	// Collect AllocationSets in a range, then accumulate
	// TODO optimize by collecting consecutive AllocationSets, accumulating as we go
	asr := opencost.NewAllocationSetRange()

	for e.Before(end) {
		// By default, query for the full remaining duration. But do not let
		// any individual query duration exceed the configured max Prometheus
		// query duration.
		duration := end.Sub(e)
		if duration > cm.BatchDuration {
			duration = cm.BatchDuration
		}

		// Set start and end parameters (s, e) for next individual computation.
		e = s.Add(duration)

		// Compute the individual AllocationSet for just (s, e)
		as, _, err := cm.computeAllocation(s, e, resolution)
		if err != nil {
			return opencost.NewAllocationSet(start, end), fmt.Errorf("error computing allocation for %s: %s", opencost.NewClosedWindow(s, e), err)
		}

		// Append to the range
		asr.Append(as)

		// Set s equal to e to set up the next query, if one exists.
		s = e
	}

	// Populate annotations, labels, and services on each Allocation. This is
	// necessary because Properties.Intersection does not propagate any values
	// stored in maps or slices for performance reasons. In this case, however,
	// it is both acceptable and necessary to do so.
	allocationAnnotations := map[string]map[string]string{}
	allocationLabels := map[string]map[string]string{}
	allocationServices := map[string]map[string]bool{}

	// Also record errors and warnings, then append them to the results later.
	errors := []string{}
	warnings := []string{}

	for _, as := range asr.Allocations {
		for k, a := range as.Allocations {
			if len(a.Properties.Annotations) > 0 {
				if _, ok := allocationAnnotations[k]; !ok {
					allocationAnnotations[k] = map[string]string{}
				}
				for name, val := range a.Properties.Annotations {
					allocationAnnotations[k][name] = val
				}
			}

			if len(a.Properties.Labels) > 0 {
				if _, ok := allocationLabels[k]; !ok {
					allocationLabels[k] = map[string]string{}
				}
				for name, val := range a.Properties.Labels {
					allocationLabels[k][name] = val
				}
			}

			if len(a.Properties.Services) > 0 {
				if _, ok := allocationServices[k]; !ok {
					allocationServices[k] = map[string]bool{}
				}
				for _, val := range a.Properties.Services {
					allocationServices[k][val] = true
				}
			}
		}

		errors = append(errors, as.Errors...)
		warnings = append(warnings, as.Warnings...)
	}

	// Accumulate to yield the result AllocationSet. After this step, we will
	// be nearly complete, but without the raw allocation data, which must be
	// recomputed.
	resultASR, err := asr.Accumulate(opencost.AccumulateOptionAll)
	if err != nil {
		return opencost.NewAllocationSet(start, end), fmt.Errorf("error accumulating data for %s: %s", opencost.NewClosedWindow(s, e), err)
	}
	if resultASR != nil && len(resultASR.Allocations) == 0 {
		return opencost.NewAllocationSet(start, end), nil
	}
	if length := len(resultASR.Allocations); length != 1 {
		return opencost.NewAllocationSet(start, end), fmt.Errorf("expected 1 accumulated allocation set, found %d sets", length)
	}
	result := resultASR.Allocations[0]

	// Apply the annotations, labels, and services to the post-accumulation
	// results. (See above for why this is necessary.)
	for k, a := range result.Allocations {
		if annotations, ok := allocationAnnotations[k]; ok {
			a.Properties.Annotations = annotations
		}

		if labels, ok := allocationLabels[k]; ok {
			a.Properties.Labels = labels
		}

		if services, ok := allocationServices[k]; ok {
			a.Properties.Services = []string{}
			for s := range services {
				a.Properties.Services = append(a.Properties.Services, s)
			}
		}

		// Expand the Window of all Allocations within the AllocationSet
		// to match the Window of the AllocationSet, which gets expanded
		// at the end of this function.
		a.Window = a.Window.ExpandStart(start).ExpandEnd(end)
	}

	// Maintain RAM and CPU max usage values by iterating over the range,
	// computing maximums on a rolling basis, and setting on the result set.
	for _, as := range asr.Allocations {
		for key, alloc := range as.Allocations {
			resultAlloc := result.Get(key)
			if resultAlloc == nil {
				continue
			}

			if resultAlloc.RawAllocationOnly == nil {
				resultAlloc.RawAllocationOnly = &opencost.RawAllocationOnlyData{}
			}

			if alloc.RawAllocationOnly == nil {
				// This will happen inevitably for unmounted disks, but should
				// ideally not happen for any allocation with CPU and RAM data.
				if !alloc.IsUnmounted() {
					log.DedupedWarningf(10, "ComputeAllocation: raw allocation data missing for %s", key)
				}
				continue
			}

			if alloc.RawAllocationOnly.CPUCoreUsageMax > resultAlloc.RawAllocationOnly.CPUCoreUsageMax {
				resultAlloc.RawAllocationOnly.CPUCoreUsageMax = alloc.RawAllocationOnly.CPUCoreUsageMax
			}

			if alloc.RawAllocationOnly.RAMBytesUsageMax > resultAlloc.RawAllocationOnly.RAMBytesUsageMax {
				resultAlloc.RawAllocationOnly.RAMBytesUsageMax = alloc.RawAllocationOnly.RAMBytesUsageMax
			}

			if alloc.RawAllocationOnly.GPUUsageMax != nil {
				if *alloc.RawAllocationOnly.GPUUsageMax > *resultAlloc.RawAllocationOnly.GPUUsageMax {
					resultAlloc.RawAllocationOnly.GPUUsageMax = alloc.RawAllocationOnly.GPUUsageMax
				}
			}

		}
	}

	// Expand the window to match the queried time range.
	result.Window = result.Window.ExpandStart(start).ExpandEnd(end)

	// Append errors and warnings
	result.Errors = errors
	result.Warnings = warnings

	// Convert any NaNs to 0 to avoid JSON marshaling issues and avoid cascading NaN appearances elsewhere
	result.SanitizeNaN()

	return result, nil
}

// DateRange checks the data (up to 90 days in the past), and returns the oldest and newest sample timestamp from opencost scraping metric
// it supposed to be a good indicator of available allocation data
func (cm *CostModel) DateRange(limitDays int) (time.Time, time.Time, error) {
	return cm.DataSource.QueryDataCoverage(limitDays)
}

func (cm *CostModel) computeAllocation(start, end time.Time, resolution time.Duration) (*opencost.AllocationSet, map[nodeKey]*nodePricing, error) {
	// 1. Build out Pod map from resolution-tuned, batched Pod start/end query
	// 2. Run and apply the results of the remaining queries to
	// 3. Build out AllocationSet from completed Pod map

	// Create a window spanning the requested query
	window := opencost.NewWindow(&start, &end)

	// Create an empty AllocationSet. For safety, in the case of an error, we
	// should prefer to return this empty set with the error. (In the case of
	// no error, of course we populate the set and return it.)
	allocSet := opencost.NewAllocationSet(start, end)

	// (1) Build out Pod map

	// Build out a map of Allocations as a mapping from pod-to-container-to-
	// underlying-Allocation instance, starting with (start, end) so that we
	// begin with minutes, from which we compute resource allocation and cost
	// totals from measured rate data.
	podMap := map[podKey]*pod{}

	// clusterStarts and clusterEnds record the earliest start and latest end
	// times, respectively, on a cluster-basis. These are used for unmounted
	// PVs and other "virtual" Allocations so that minutes are maximally
	// accurate during start-up or spin-down of a cluster
	clusterStart := map[string]time.Time{}
	clusterEnd := map[string]time.Time{}

	// If ingesting pod UID, we query kube_pod_container_status_running avg
	// by uid as well as the default values, and all podKeys/pods have their
	// names changed to "<pod_name> <pod_uid>". Because other metrics need
	// to generate keys to match pods but don't have UIDs, podUIDKeyMap
	// stores values of format:

	// default podKey : []{edited podkey 1, edited podkey 2}

	// This is because ingesting UID allows us to catch uncontrolled pods
	// with the same names. However, this will lead to a many-to-one metric
	// to podKey relation, so this map allows us to map the metric's
	// "<pod_name>" key to the edited "<pod_name> <pod_uid>" keys in podMap.
	ingestPodUID := env.IsIngestingPodUID()
	podUIDKeyMap := make(map[podKey][]podKey)

	if ingestPodUID {
		log.Debugf("CostModel.ComputeAllocation: ingesting UID data from KSM metrics...")
	}

	// TODO:CLEANUP remove "max batch" idea and clusterStart/End
	err := cm.buildPodMap(window, cm.BatchDuration, podMap, clusterStart, clusterEnd, ingestPodUID, podUIDKeyMap)
	if err != nil {
		log.Errorf("CostModel.ComputeAllocation: failed to build pod map: %s", err.Error())
	}
	// (2) Run and apply remaining queries

	// Query for the duration between start and end
	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		return allocSet, nil, fmt.Errorf("illegal duration value for %s", opencost.NewClosedWindow(start, end))
	}

	grp := source.NewQueryGroup()
	ds := cm.DataSource

	resChRAMBytesAllocated := grp.With(ds.QueryRAMBytesAllocated(start, end))
	resChRAMRequests := grp.With(ds.QueryRAMRequests(start, end))
	resChRAMUsageAvg := grp.With(ds.QueryRAMUsageAvg(start, end))
	resChRAMUsageMax := grp.With(ds.QueryRAMUsageMax(start, end))

	resChCPUCoresAllocated := grp.With(ds.QueryCPUCoresAllocated(start, end))
	resChCPURequests := grp.With(ds.QueryCPURequests(start, end))
	resChCPUUsageAvg := grp.With(ds.QueryCPUUsageAvg(start, end))
	resChCPUUsageMax := grp.With(ds.QueryCPUUsageMax(start, end))
	resCPUUsageMax, _ := resChCPUUsageMax.Await()
	// This avoids logspam if there is no data for either metric (e.g. if
	// the Prometheus didn't exist in the queried window of time).
	if len(resCPUUsageMax) > 0 {
		log.Debugf("CPU usage recording rule query returned an empty result when queried at %s over %s. Fell back to subquery. Consider setting up Kubecost CPU usage recording role to reduce query load on Prometheus; subqueries are expensive.", end.String(), durStr)
	}

	// GPU Queries
	resChIsGpuShared := grp.With(ds.QueryIsGPUShared(start, end))
	resChGPUsAllocated := grp.With(ds.QueryGPUsAllocated(start, end))
	resChGPUsRequested := grp.With(ds.QueryGPUsRequested(start, end))
	resChGPUsUsageAvg := grp.With(ds.QueryGPUsUsageAvg(start, end))
	resChGPUsUsageMax := grp.With(ds.QueryGPUsUsageMax(start, end))
	resChGetGPUInfo := grp.With(ds.QueryGetGPUInfo(start, end))

	resChNodeCostPerCPUHr := grp.With(ds.QueryNodeCostPerCPUHr(start, end))
	resChNodeCostPerRAMGiBHr := grp.With(ds.QueryNodeCostPerRAMGiBHr(start, end))
	resChNodeCostPerGPUHr := grp.With(ds.QueryNodeCostPerGPUHr(start, end))

	resChNodeIsSpot := grp.With(ds.QueryNodeIsSpot2(start, end))
	resChPVCInfo := grp.With(ds.QueryPVCInfo2(start, end))

	resChPodPVCAllocation := grp.With(ds.QueryPodPVCAllocation(start, end))
	resChPVCBytesRequested := grp.With(ds.QueryPVCBytesRequested(start, end))
	resChPVActiveMins := grp.With(ds.QueryPVActiveMins(start, end))
	resChPVBytes := grp.With(ds.QueryPVBytes(start, end))
	resChPVCostPerGiBHour := grp.With(ds.QueryPVCostPerGiBHour(start, end))
	resChPVMeta := grp.With(ds.QueryPVMeta(start, end))

	resChNetTransferBytes := grp.With(ds.QueryNetTransferBytes(start, end))
	resChNetReceiveBytes := grp.With(ds.QueryNetReceiveBytes(start, end))

	resChNetZoneGiB := grp.With(ds.QueryNetZoneGiB(start, end))
	resChNetZoneCostPerGiB := grp.With(ds.QueryNetZoneCostPerGiB(start, end))

	resChNetRegionGiB := grp.With(ds.QueryNetRegionGiB(start, end))
	resChNetRegionCostPerGiB := grp.With(ds.QueryNetRegionCostPerGiB(start, end))

	resChNetInternetGiB := grp.With(ds.QueryNetInternetGiB(start, end))
	resChNetInternetCostPerGiB := grp.With(ds.QueryNetInternetCostPerGiB(start, end))

	var resChNodeLabels *source.QueryGroupAsyncResult
	if env.GetAllocationNodeLabelsEnabled() {
		resChNodeLabels = grp.With(ds.QueryNodeLabels(start, end))
	}

	resChNamespaceLabels := grp.With(ds.QueryNamespaceLabels(start, end))
	resChNamespaceAnnotations := grp.With(ds.QueryNamespaceAnnotations(start, end))

	resChPodLabels := grp.With(ds.QueryPodLabels(start, end))
	resChPodAnnotations := grp.With(ds.QueryPodAnnotations(start, end))

	resChServiceLabels := grp.With(ds.QueryServiceLabels(start, end))
	resChDeploymentLabels := grp.With(ds.QueryDeploymentLabels(start, end))
	resChStatefulSetLabels := grp.With(ds.QueryStatefulSetLabels(start, end))
	resChDaemonSetLabels := grp.With(ds.QueryDaemonSetLabels(start, end))

	resChPodsWithReplicaSetOwner := grp.With(ds.QueryPodsWithReplicaSetOwner(start, end))
	resChReplicaSetsWithoutOwners := grp.With(ds.QueryReplicaSetsWithoutOwners(start, end))
	resChReplicaSetsWithRolloutOwner := grp.With(ds.QueryReplicaSetsWithRollout(start, end))

	resChJobLabels := grp.With(ds.QueryJobLabels(start, end))

	resChLBCostPerHr := grp.With(ds.QueryLBPricePerHr(start, end))
	resChLBActiveMins := grp.With(ds.QueryLBActiveMinutes(start, end))

	resCPUCoresAllocated, _ := resChCPUCoresAllocated.Await()
	resCPURequests, _ := resChCPURequests.Await()
	resCPUUsageAvg, _ := resChCPUUsageAvg.Await()
	resRAMBytesAllocated, _ := resChRAMBytesAllocated.Await()
	resRAMRequests, _ := resChRAMRequests.Await()
	resRAMUsageAvg, _ := resChRAMUsageAvg.Await()
	resRAMUsageMax, _ := resChRAMUsageMax.Await()
	resGPUsRequested, _ := resChGPUsRequested.Await()
	resGPUsUsageAvg, _ := resChGPUsUsageAvg.Await()
	resGPUsUsageMax, _ := resChGPUsUsageMax.Await()
	resGPUsAllocated, _ := resChGPUsAllocated.Await()

	resIsGpuShared, _ := resChIsGpuShared.Await()
	resGetGPUInfo, _ := resChGetGPUInfo.Await()

	resNodeCostPerCPUHr, _ := resChNodeCostPerCPUHr.Await()
	resNodeCostPerRAMGiBHr, _ := resChNodeCostPerRAMGiBHr.Await()
	resNodeCostPerGPUHr, _ := resChNodeCostPerGPUHr.Await()
	resNodeIsSpot, _ := resChNodeIsSpot.Await()
	nodeExtendedData, _ := queryExtendedNodeData(grp, ds, start, end)

	resPVActiveMins, _ := resChPVActiveMins.Await()
	resPVBytes, _ := resChPVBytes.Await()
	resPVCostPerGiBHour, _ := resChPVCostPerGiBHour.Await()
	resPVMeta, _ := resChPVMeta.Await()

	resPVCInfo, _ := resChPVCInfo.Await()
	resPVCBytesRequested, _ := resChPVCBytesRequested.Await()
	resPodPVCAllocation, _ := resChPodPVCAllocation.Await()

	resNetTransferBytes, _ := resChNetTransferBytes.Await()
	resNetReceiveBytes, _ := resChNetReceiveBytes.Await()
	resNetZoneGiB, _ := resChNetZoneGiB.Await()
	resNetZoneCostPerGiB, _ := resChNetZoneCostPerGiB.Await()
	resNetRegionGiB, _ := resChNetRegionGiB.Await()
	resNetRegionCostPerGiB, _ := resChNetRegionCostPerGiB.Await()
	resNetInternetGiB, _ := resChNetInternetGiB.Await()
	resNetInternetCostPerGiB, _ := resChNetInternetCostPerGiB.Await()

	var resNodeLabels []*source.QueryResult
	if env.GetAllocationNodeLabelsEnabled() {
		resNodeLabels, _ = resChNodeLabels.Await()
	}
	resNamespaceLabels, _ := resChNamespaceLabels.Await()
	resNamespaceAnnotations, _ := resChNamespaceAnnotations.Await()
	resPodLabels, _ := resChPodLabels.Await()
	resPodAnnotations, _ := resChPodAnnotations.Await()
	resServiceLabels, _ := resChServiceLabels.Await()
	resDeploymentLabels, _ := resChDeploymentLabels.Await()
	resStatefulSetLabels, _ := resChStatefulSetLabels.Await()
	resDaemonSetLabels, _ := resChDaemonSetLabels.Await()
	resPodsWithReplicaSetOwner, _ := resChPodsWithReplicaSetOwner.Await()
	resReplicaSetsWithoutOwners, _ := resChReplicaSetsWithoutOwners.Await()
	resReplicaSetsWithRolloutOwner, _ := resChReplicaSetsWithRolloutOwner.Await()
	resJobLabels, _ := resChJobLabels.Await()
	resLBCostPerHr, _ := resChLBCostPerHr.Await()
	resLBActiveMins, _ := resChLBActiveMins.Await()

	if grp.HasErrors() {
		for _, err := range grp.Errors() {
			log.Errorf("CostModel.ComputeAllocation: query context error %s", err)
		}

		return allocSet, nil, grp.Error()
	}

	// We choose to apply allocation before requests in the cases of RAM and
	// CPU so that we can assert that allocation should always be greater than
	// or equal to request.
	applyCPUCoresAllocated(podMap, resCPUCoresAllocated, podUIDKeyMap)
	applyCPUCoresRequested(podMap, resCPURequests, podUIDKeyMap)
	applyCPUCoresUsedAvg(podMap, resCPUUsageAvg, podUIDKeyMap)
	applyCPUCoresUsedMax(podMap, resCPUUsageMax, podUIDKeyMap)
	applyRAMBytesAllocated(podMap, resRAMBytesAllocated, podUIDKeyMap)
	applyRAMBytesRequested(podMap, resRAMRequests, podUIDKeyMap)
	applyRAMBytesUsedAvg(podMap, resRAMUsageAvg, podUIDKeyMap)
	applyRAMBytesUsedMax(podMap, resRAMUsageMax, podUIDKeyMap)
	applyGPUUsage(podMap, resGPUsUsageAvg, podUIDKeyMap, GpuUsageAverageMode)
	applyGPUUsage(podMap, resGPUsUsageMax, podUIDKeyMap, GpuUsageMaxMode)
	applyGPUUsage(podMap, resIsGpuShared, podUIDKeyMap, GpuIsSharedMode)
	applyGPUUsage(podMap, resGetGPUInfo, podUIDKeyMap, GpuInfoMode)
	applyGPUsAllocated(podMap, resGPUsRequested, resGPUsAllocated, podUIDKeyMap)
	applyNetworkTotals(podMap, resNetTransferBytes, resNetReceiveBytes, podUIDKeyMap)
	applyNetworkAllocation(podMap, resNetZoneGiB, resNetZoneCostPerGiB, podUIDKeyMap, networkCrossZoneCost)
	applyNetworkAllocation(podMap, resNetRegionGiB, resNetRegionCostPerGiB, podUIDKeyMap, networkCrossRegionCost)
	applyNetworkAllocation(podMap, resNetInternetGiB, resNetInternetCostPerGiB, podUIDKeyMap, networkInternetCost)

	// In the case that a two pods with the same name had different containers,
	// we will double-count the containers. There is no way to associate each
	// container with the proper pod from the usage metrics above. This will
	// show up as a pod having two Allocations running for the whole pod runtime.

	// Other than that case, Allocations should be associated with pods by the
	// above functions.

	// At this point, we expect "Node" to be set by one of the above functions
	// (e.g. applyCPUCoresAllocated, etc.) -- otherwise, node labels will fail
	// to correctly apply to the pods.
	var nodeLabels map[nodeKey]map[string]string
	if env.GetAllocationNodeLabelsEnabled() {
		nodeLabels = resToNodeLabels(resNodeLabels)
	}
	namespaceLabels := resToNamespaceLabels(resNamespaceLabels)
	podLabels := resToPodLabels(resPodLabels, podUIDKeyMap, ingestPodUID)
	namespaceAnnotations := resToNamespaceAnnotations(resNamespaceAnnotations)
	podAnnotations := resToPodAnnotations(resPodAnnotations, podUIDKeyMap, ingestPodUID)
	applyLabels(podMap, nodeLabels, namespaceLabels, podLabels)
	applyAnnotations(podMap, namespaceAnnotations, podAnnotations)

	podDeploymentMap := labelsToPodControllerMap(podLabels, resToDeploymentLabels(resDeploymentLabels))
	podStatefulSetMap := labelsToPodControllerMap(podLabels, resToStatefulSetLabels(resStatefulSetLabels))
	podDaemonSetMap := resToPodDaemonSetMap(resDaemonSetLabels, podUIDKeyMap, ingestPodUID)
	podJobMap := resToPodJobMap(resJobLabels, podUIDKeyMap, ingestPodUID)
	podReplicaSetMap := resToPodReplicaSetMap(resPodsWithReplicaSetOwner, resReplicaSetsWithoutOwners, resReplicaSetsWithRolloutOwner, podUIDKeyMap, ingestPodUID)
	applyControllersToPods(podMap, podDeploymentMap)
	applyControllersToPods(podMap, podStatefulSetMap)
	applyControllersToPods(podMap, podDaemonSetMap)
	applyControllersToPods(podMap, podJobMap)
	applyControllersToPods(podMap, podReplicaSetMap)

	serviceLabels := getServiceLabels(resServiceLabels)
	allocsByService := map[serviceKey][]*opencost.Allocation{}
	applyServicesToPods(podMap, podLabels, allocsByService, serviceLabels)

	// TODO breakdown network costs?

	// Build out the map of all PVs with class, size and cost-per-hour.
	// Note: this does not record time running, which we may want to
	// include later for increased PV precision. (As long as the PV has
	// a PVC, we get time running there, so this is only inaccurate
	// for short-lived, unmounted PVs.)
	pvMap := map[pvKey]*pv{}
	buildPVMap(resolution, pvMap, resPVCostPerGiBHour, resPVActiveMins, resPVMeta, window)
	applyPVBytes(pvMap, resPVBytes)

	// Build out the map of all PVCs with time running, bytes requested,
	// and connect to the correct PV from pvMap. (If no PV exists, that
	// is noted, but does not result in any allocation/cost.)
	pvcMap := map[pvcKey]*pvc{}
	buildPVCMap(resolution, pvcMap, pvMap, resPVCInfo, window)
	applyPVCBytesRequested(pvcMap, resPVCBytesRequested)

	// Build out the relationships of pods to their PVCs. This step
	// populates the pvc.Count field so that pvc allocation can be
	// split appropriately among each pod's container allocation.
	podPVCMap := map[podKey][]*pvc{}
	buildPodPVCMap(podPVCMap, pvMap, pvcMap, podMap, resPodPVCAllocation, podUIDKeyMap, ingestPodUID)
	applyPVCsToPods(window, podMap, podPVCMap, pvcMap)

	// Identify PVCs without pods and add pv costs to the unmounted Allocation for the pvc's cluster
	applyUnmountedPVCs(window, podMap, pvcMap)

	// Identify PVs without PVCs and add PV costs to the unmounted Allocation for the PV's cluster
	applyUnmountedPVs(window, podMap, pvMap, pvcMap)

	lbMap := make(map[serviceKey]*lbCost)
	getLoadBalancerCosts(lbMap, resLBCostPerHr, resLBActiveMins, resolution, window)
	applyLoadBalancersToPods(window, podMap, lbMap, allocsByService)

	// Build out a map of Nodes with resource costs, discounts, and node types
	// for converting resource allocation data to cumulative costs.
	nodeMap := map[nodeKey]*nodePricing{}

	applyNodeCostPerCPUHr(nodeMap, resNodeCostPerCPUHr)
	applyNodeCostPerRAMGiBHr(nodeMap, resNodeCostPerRAMGiBHr)
	applyNodeCostPerGPUHr(nodeMap, resNodeCostPerGPUHr)
	applyNodeSpot(nodeMap, resNodeIsSpot)
	applyNodeDiscount(nodeMap, cm)
	applyExtendedNodeData(nodeMap, nodeExtendedData)
	cm.applyNodesToPod(podMap, nodeMap)

	// (3) Build out AllocationSet from Pod map
	for _, pod := range podMap {
		for _, alloc := range pod.Allocations {
			cluster := alloc.Properties.Cluster
			nodeName := alloc.Properties.Node
			namespace := alloc.Properties.Namespace
			podName := alloc.Properties.Pod
			container := alloc.Properties.Container

			// Make sure that the name is correct (node may not be present at this
			// point due to it missing from queryMinutes) then insert.
			alloc.Name = fmt.Sprintf("%s/%s/%s/%s/%s", cluster, nodeName, namespace, podName, container)
			allocSet.Set(alloc)
		}
	}

	return allocSet, nodeMap, nil
}
