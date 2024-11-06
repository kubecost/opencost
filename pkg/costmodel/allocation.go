package costmodel

import (
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/timeutil"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/pkg/env"
	"github.com/opencost/opencost/pkg/prom"
)

const (
	// https://kubecost.atlassian.net/browse/BURNDOWN-234
	// upstream KSM has implementation change vs OC internal KSM - it sets metric to 0 when pod goes down
	// VS OC implementation which stops emitting it
	// by adding != 0 filter, we keep just the active times in the prom result
	queryFmtPods    = `avg(kube_pod_container_status_running{%s} != 0) by (pod, namespace, %s)[%s:%s]`
	queryFmtPodsUID = `avg(kube_pod_container_status_running{%s} != 0) by (pod, namespace, uid, %s)[%s:%s]`

	queryFmtRAMBytesAllocated           = `avg(avg_over_time(container_memory_allocation_bytes{container!="", container!="POD", node!="", %s}[%s])) by (container, pod, namespace, node, %s, provider_id)`
	queryFmtRAMRequests                 = `avg(avg_over_time(kube_pod_container_resource_requests{resource="memory", unit="byte", container!="", container!="POD", node!="", %s}[%s])) by (container, pod, namespace, node, %s)`
	queryFmtRAMUsageAvg                 = `avg(avg_over_time(container_memory_working_set_bytes{container!="", container_name!="POD", container!="POD", %s}[%s])) by (container_name, container, pod_name, pod, namespace, instance, %s)`
	queryFmtRAMUsageMax                 = `max(max_over_time(container_memory_working_set_bytes{container!="", container_name!="POD", container!="POD", %s}[%s])) by (container_name, container, pod_name, pod, namespace, instance, %s)`
	queryFmtCPUCoresAllocated           = `avg(avg_over_time(container_cpu_allocation{container!="", container!="POD", node!="", %s}[%s])) by (container, pod, namespace, node, %s)`
	queryFmtCPURequests                 = `avg(avg_over_time(kube_pod_container_resource_requests{resource="cpu", unit="core", container!="", container!="POD", node!="", %s}[%s])) by (container, pod, namespace, node, %s)`
	queryFmtCPUUsageAvg                 = `avg(rate(container_cpu_usage_seconds_total{container!="", container_name!="POD", container!="POD", %s}[%s])) by (container_name, container, pod_name, pod, namespace, instance, %s)`
	queryFmtGPUsRequested               = `avg(avg_over_time(kube_pod_container_resource_requests{resource="nvidia_com_gpu", container!="",container!="POD", node!="", %s}[%s])) by (container, pod, namespace, node, %s)`
	queryFmtGPUsUsageAvg                = `avg(avg_over_time(DCGM_FI_PROF_GR_ENGINE_ACTIVE{container!=""}[%s])) by (container, pod, namespace, %s)`
	queryFmtGPUsUsageMax                = `max(max_over_time(DCGM_FI_PROF_GR_ENGINE_ACTIVE{container!=""}[%s])) by (container, pod, namespace, %s)`
	queryFmtGPUsAllocated               = `avg(avg_over_time(container_gpu_allocation{container!="", container!="POD", node!="", %s}[%s])) by (container, pod, namespace, node, %s)`
	queryFmtNodeCostPerCPUHr            = `avg(avg_over_time(node_cpu_hourly_cost{%s}[%s])) by (node, %s, instance_type, provider_id)`
	queryFmtNodeCostPerRAMGiBHr         = `avg(avg_over_time(node_ram_hourly_cost{%s}[%s])) by (node, %s, instance_type, provider_id)`
	queryFmtNodeCostPerGPUHr            = `avg(avg_over_time(node_gpu_hourly_cost{%s}[%s])) by (node, %s, instance_type, provider_id)`
	queryFmtNodeIsSpot                  = `avg_over_time(kubecost_node_is_spot{%s}[%s])`
	queryFmtPVCInfo                     = `avg(kube_persistentvolumeclaim_info{volumename != "", %s}) by (persistentvolumeclaim, storageclass, volumename, namespace, %s)[%s:%s]`
	queryFmtPodPVCAllocation            = `avg(avg_over_time(pod_pvc_allocation{%s}[%s])) by (persistentvolume, persistentvolumeclaim, pod, namespace, %s)`
	queryFmtPVCBytesRequested           = `avg(avg_over_time(kube_persistentvolumeclaim_resource_requests_storage_bytes{%s}[%s])) by (persistentvolumeclaim, namespace, %s)`
	queryFmtPVActiveMins                = `count(kube_persistentvolume_capacity_bytes{%s}) by (persistentvolume, %s)[%s:%s]`
	queryFmtPVBytes                     = `avg(avg_over_time(kube_persistentvolume_capacity_bytes{%s}[%s])) by (persistentvolume, %s)`
	queryFmtPVCostPerGiBHour            = `avg(avg_over_time(pv_hourly_cost{%s}[%s])) by (volumename, %s)`
	queryFmtPVMeta                      = `avg(avg_over_time(kubecost_pv_info{%s}[%s])) by (%s, persistentvolume, provider_id)`
	queryFmtNetZoneGiB                  = `sum(increase(kubecost_pod_network_egress_bytes_total{internet="false", same_zone="false", same_region="true", %s}[%s])) by (pod_name, namespace, %s) / 1024 / 1024 / 1024`
	queryFmtNetZoneCostPerGiB           = `avg(avg_over_time(kubecost_network_zone_egress_cost{%s}[%s])) by (%s)`
	queryFmtNetRegionGiB                = `sum(increase(kubecost_pod_network_egress_bytes_total{internet="false", same_zone="false", same_region="false", %s}[%s])) by (pod_name, namespace, %s) / 1024 / 1024 / 1024`
	queryFmtNetRegionCostPerGiB         = `avg(avg_over_time(kubecost_network_region_egress_cost{%s}[%s])) by (%s)`
	queryFmtNetInternetGiB              = `sum(increase(kubecost_pod_network_egress_bytes_total{internet="true", %s}[%s])) by (pod_name, namespace, %s) / 1024 / 1024 / 1024`
	queryFmtNetInternetCostPerGiB       = `avg(avg_over_time(kubecost_network_internet_egress_cost{%s}[%s])) by (%s)`
	queryFmtNetReceiveBytes             = `sum(increase(container_network_receive_bytes_total{pod!="", %s}[%s])) by (pod_name, pod, namespace, %s)`
	queryFmtNetTransferBytes            = `sum(increase(container_network_transmit_bytes_total{pod!="", %s}[%s])) by (pod_name, pod, namespace, %s)`
	queryFmtNodeLabels                  = `avg_over_time(kube_node_labels{%s}[%s])`
	queryFmtNamespaceLabels             = `avg_over_time(kube_namespace_labels{%s}[%s])`
	queryFmtNamespaceAnnotations        = `avg_over_time(kube_namespace_annotations{%s}[%s])`
	queryFmtPodLabels                   = `avg_over_time(kube_pod_labels{%s}[%s])`
	queryFmtPodAnnotations              = `avg_over_time(kube_pod_annotations{%s}[%s])`
	queryFmtServiceLabels               = `avg_over_time(service_selector_labels{%s}[%s])`
	queryFmtDeploymentLabels            = `avg_over_time(deployment_match_labels{%s}[%s])`
	queryFmtStatefulSetLabels           = `avg_over_time(statefulSet_match_labels{%s}[%s])`
	queryFmtDaemonSetLabels             = `sum(avg_over_time(kube_pod_owner{owner_kind="DaemonSet", %s}[%s])) by (pod, owner_name, namespace, %s)`
	queryFmtJobLabels                   = `sum(avg_over_time(kube_pod_owner{owner_kind="Job", %s}[%s])) by (pod, owner_name, namespace ,%s)`
	queryFmtPodsWithReplicaSetOwner     = `sum(avg_over_time(kube_pod_owner{owner_kind="ReplicaSet", %s}[%s])) by (pod, owner_name, namespace ,%s)`
	queryFmtReplicaSetsWithoutOwners    = `avg(avg_over_time(kube_replicaset_owner{owner_kind="<none>", owner_name="<none>", %s}[%s])) by (replicaset, namespace, %s)`
	queryFmtReplicaSetsWithRolloutOwner = `avg(avg_over_time(kube_replicaset_owner{owner_kind="Rollout", %s}[%s])) by (replicaset, namespace, owner_kind, owner_name, %s)`
	queryFmtLBCostPerHr                 = `avg(avg_over_time(kubecost_load_balancer_cost{%s}[%s])) by (namespace, service_name, ingress_ip, %s)`
	queryFmtLBActiveMins                = `count(kubecost_load_balancer_cost{%s}) by (namespace, service_name, %s)[%s:%s]`
	queryFmtOldestSample                = `min_over_time(timestamp(group(node_cpu_hourly_cost{%s}))[%s:%s])`
	queryFmtNewestSample                = `max_over_time(timestamp(group(node_cpu_hourly_cost{%s}))[%s:%s])`
	queryFmtIsGPuShared                 = `avg(avg_over_time(kube_pod_container_resource_requests{container!="", node != "", pod != "", container!= "", unit = "integer",  %s}[%s])) by (container, pod, namespace, node, resource)`
	queryFmtGetGPuInfo                  = `avg(avg_over_time(DCGM_FI_DEV_DEC_UTIL{container!="",%s}[%s])) by (container, pod, namespace, device, modelName, UUID)`

	// Because we use container_cpu_usage_seconds_total to calculate CPU usage
	// at any given "instant" of time, we need to use an irate or rate. To then
	// calculate a max (or any aggregation) we have to perform an aggregation
	// query on top of an instant-by-instant maximum. Prometheus supports this
	// type of query with a "subquery" [1], however it is reportedly expensive
	// to make such a query. By default, Kubecost's Prometheus config includes
	// a recording rule that keeps track of the instant-by-instant irate for CPU
	// usage. The metric in this query is created by that recording rule.
	//
	// [1] https://prometheus.io/blog/2019/01/28/subquery-support/
	//
	// If changing the name of the recording rule, make sure to update the
	// corresponding diagnostic query to avoid confusion.
	queryFmtCPUUsageMaxRecordingRule = `max(max_over_time(kubecost_container_cpu_usage_irate{%s}[%s])) by (container_name, container, pod_name, pod, namespace, instance, %s)`
	// This is the subquery equivalent of the above recording rule query. It is
	// more expensive, but does not require the recording rule. It should be
	// used as a fallback query if the recording rule data does not exist.
	//
	// The parameter after the colon [:<thisone>] in the subquery affects the
	// resolution of the subquery.
	// The parameter after the metric ...{}[<thisone>] should be set to 2x
	// the resolution, to make sure the irate always has two points to query
	// in case the Prom scrape duration has been reduced to be equal to the
	// ETL resolution.
	queryFmtCPUUsageMaxSubquery = `max(max_over_time(irate(container_cpu_usage_seconds_total{container!="POD", container!="", %s}[%s])[%s:%s])) by (container, pod_name, pod, namespace, instance, %s)`
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
	if end.Sub(start) <= cm.MaxPrometheusQueryDuration {
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
		if duration > cm.MaxPrometheusQueryDuration {
			duration = cm.MaxPrometheusQueryDuration
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

			if alloc.RawAllocationOnly.CPUCoreUsageMax > resultAlloc.RawAllocationOnly.CPUCoreUsageMax {
				resultAlloc.RawAllocationOnly.GPUUsageMax = alloc.RawAllocationOnly.GPUUsageMax
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
func (cm *CostModel) DateRange() (time.Time, time.Time, error) {
	ctx := prom.NewNamedContext(cm.PrometheusClient, prom.AllocationContextName)
	exportCsvDaysFmt := fmt.Sprintf("%dd", env.GetExportCSVMaxDays())

	resOldest, _, err := ctx.QuerySync(fmt.Sprintf(queryFmtOldestSample, env.GetPromClusterFilter(), exportCsvDaysFmt, "1h"))
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("querying oldest sample: %w", err)
	}
	if len(resOldest) == 0 || len(resOldest[0].Values) == 0 {
		return time.Time{}, time.Time{}, fmt.Errorf("querying oldest sample: no results")
	}
	oldest := time.Unix(int64(resOldest[0].Values[0].Value), 0)

	resNewest, _, err := ctx.QuerySync(fmt.Sprintf(queryFmtNewestSample, env.GetPromClusterFilter(), exportCsvDaysFmt, "1h"))
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("querying newest sample: %w", err)
	}
	if len(resNewest) == 0 || len(resNewest[0].Values) == 0 {
		return time.Time{}, time.Time{}, fmt.Errorf("querying newest sample: no results")
	}
	newest := time.Unix(int64(resNewest[0].Values[0].Value), 0)

	return oldest, newest, nil
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
	err := cm.buildPodMap(window, resolution, env.GetETLMaxPrometheusQueryDuration(), podMap, clusterStart, clusterEnd, ingestPodUID, podUIDKeyMap)
	if err != nil {
		log.Errorf("CostModel.ComputeAllocation: failed to build pod map: %s", err.Error())
	}
	// (2) Run and apply remaining queries

	// Query for the duration between start and end
	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		return allocSet, nil, fmt.Errorf("illegal duration value for %s", opencost.NewClosedWindow(start, end))
	}

	// Convert resolution duration to a query-ready string
	resStr := timeutil.DurationString(resolution)

	ctx := prom.NewNamedContext(cm.PrometheusClient, prom.AllocationContextName)

	queryRAMBytesAllocated := fmt.Sprintf(queryFmtRAMBytesAllocated, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())
	resChRAMBytesAllocated := ctx.QueryAtTime(queryRAMBytesAllocated, end)

	queryRAMRequests := fmt.Sprintf(queryFmtRAMRequests, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())
	resChRAMRequests := ctx.QueryAtTime(queryRAMRequests, end)

	queryRAMUsageAvg := fmt.Sprintf(queryFmtRAMUsageAvg, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())
	resChRAMUsageAvg := ctx.QueryAtTime(queryRAMUsageAvg, end)

	queryRAMUsageMax := fmt.Sprintf(queryFmtRAMUsageMax, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())
	resChRAMUsageMax := ctx.QueryAtTime(queryRAMUsageMax, end)

	queryCPUCoresAllocated := fmt.Sprintf(queryFmtCPUCoresAllocated, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())
	resChCPUCoresAllocated := ctx.QueryAtTime(queryCPUCoresAllocated, end)

	queryCPURequests := fmt.Sprintf(queryFmtCPURequests, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())
	resChCPURequests := ctx.QueryAtTime(queryCPURequests, end)

	queryCPUUsageAvg := fmt.Sprintf(queryFmtCPUUsageAvg, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())
	resChCPUUsageAvg := ctx.QueryAtTime(queryCPUUsageAvg, end)

	queryCPUUsageMax := fmt.Sprintf(queryFmtCPUUsageMaxRecordingRule, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())
	resChCPUUsageMax := ctx.QueryAtTime(queryCPUUsageMax, end)
	resCPUUsageMax, _ := resChCPUUsageMax.Await()
	// If the recording rule has no data, try to fall back to the subquery.
	if len(resCPUUsageMax) == 0 {
		// The parameter after the metric ...{}[<thisone>] should be set to 2x
		// the resolution, to make sure the irate always has two points to query
		// in case the Prom scrape duration has been reduced to be equal to the
		// resolution.
		doubleResStr := timeutil.DurationString(2 * resolution)
		queryCPUUsageMax = fmt.Sprintf(queryFmtCPUUsageMaxSubquery, env.GetPromClusterFilter(), doubleResStr, durStr, resStr, env.GetPromClusterLabel())
		resChCPUUsageMax = ctx.QueryAtTime(queryCPUUsageMax, end)
		resCPUUsageMax, _ = resChCPUUsageMax.Await()

		// This avoids logspam if there is no data for either metric (e.g. if
		// the Prometheus didn't exist in the queried window of time).
		if len(resCPUUsageMax) > 0 {
			log.Debugf("CPU usage recording rule query returned an empty result when queried at %s over %s. Fell back to subquery. Consider setting up Kubecost CPU usage recording role to reduce query load on Prometheus; subqueries are expensive.", end.String(), durStr)
		}
	}

	// GPU Queries
	//queryIsGpuShared := fmt.Sprintf(queryFmtIsGPuShared, durStr)
	queryGPUsRequested := fmt.Sprintf(queryFmtGPUsRequested, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())
	resChGPUsRequested := ctx.QueryAtTime(queryGPUsRequested, end)

	queryGPUsUsageAvg := fmt.Sprintf(queryFmtGPUsUsageAvg, durStr, env.GetPromClusterLabel())
	resChGPUsUsageAvg := ctx.Query(queryGPUsUsageAvg)

	queryGPUsUsageMax := fmt.Sprintf(queryFmtGPUsUsageMax, durStr, env.GetPromClusterLabel())
	resChGPUsUsageMax := ctx.Query(queryGPUsUsageMax)

	queryGPUsAllocated := fmt.Sprintf(queryFmtGPUsAllocated, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())
	resChGPUsAllocated := ctx.QueryAtTime(queryGPUsAllocated, end)

	queryNodeCostPerCPUHr := fmt.Sprintf(queryFmtNodeCostPerCPUHr, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())
	resChNodeCostPerCPUHr := ctx.QueryAtTime(queryNodeCostPerCPUHr, end)

	queryNodeCostPerRAMGiBHr := fmt.Sprintf(queryFmtNodeCostPerRAMGiBHr, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())
	resChNodeCostPerRAMGiBHr := ctx.QueryAtTime(queryNodeCostPerRAMGiBHr, end)

	queryNodeCostPerGPUHr := fmt.Sprintf(queryFmtNodeCostPerGPUHr, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())
	resChNodeCostPerGPUHr := ctx.QueryAtTime(queryNodeCostPerGPUHr, end)

	queryNodeIsSpot := fmt.Sprintf(queryFmtNodeIsSpot, env.GetPromClusterFilter(), durStr)
	resChNodeIsSpot := ctx.QueryAtTime(queryNodeIsSpot, end)

	queryPVCInfo := fmt.Sprintf(queryFmtPVCInfo, env.GetPromClusterFilter(), env.GetPromClusterLabel(), durStr, resStr)
	resChPVCInfo := ctx.QueryAtTime(queryPVCInfo, end)

	queryPodPVCAllocation := fmt.Sprintf(queryFmtPodPVCAllocation, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())
	resChPodPVCAllocation := ctx.QueryAtTime(queryPodPVCAllocation, end)

	queryPVCBytesRequested := fmt.Sprintf(queryFmtPVCBytesRequested, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())
	resChPVCBytesRequested := ctx.QueryAtTime(queryPVCBytesRequested, end)

	queryPVActiveMins := fmt.Sprintf(queryFmtPVActiveMins, env.GetPromClusterFilter(), env.GetPromClusterLabel(), durStr, resStr)
	resChPVActiveMins := ctx.QueryAtTime(queryPVActiveMins, end)

	queryPVBytes := fmt.Sprintf(queryFmtPVBytes, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())
	resChPVBytes := ctx.QueryAtTime(queryPVBytes, end)

	queryPVCostPerGiBHour := fmt.Sprintf(queryFmtPVCostPerGiBHour, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())
	resChPVCostPerGiBHour := ctx.QueryAtTime(queryPVCostPerGiBHour, end)

	queryPVMeta := fmt.Sprintf(queryFmtPVMeta, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())
	resChPVMeta := ctx.QueryAtTime(queryPVMeta, end)

	queryNetTransferBytes := fmt.Sprintf(queryFmtNetTransferBytes, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())
	resChNetTransferBytes := ctx.QueryAtTime(queryNetTransferBytes, end)

	queryNetReceiveBytes := fmt.Sprintf(queryFmtNetReceiveBytes, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())
	resChNetReceiveBytes := ctx.QueryAtTime(queryNetReceiveBytes, end)

	queryNetZoneGiB := fmt.Sprintf(queryFmtNetZoneGiB, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())
	resChNetZoneGiB := ctx.QueryAtTime(queryNetZoneGiB, end)

	queryNetZoneCostPerGiB := fmt.Sprintf(queryFmtNetZoneCostPerGiB, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())
	resChNetZoneCostPerGiB := ctx.QueryAtTime(queryNetZoneCostPerGiB, end)

	queryNetRegionGiB := fmt.Sprintf(queryFmtNetRegionGiB, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())
	resChNetRegionGiB := ctx.QueryAtTime(queryNetRegionGiB, end)

	queryNetRegionCostPerGiB := fmt.Sprintf(queryFmtNetRegionCostPerGiB, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())
	resChNetRegionCostPerGiB := ctx.QueryAtTime(queryNetRegionCostPerGiB, end)

	queryNetInternetGiB := fmt.Sprintf(queryFmtNetInternetGiB, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())
	resChNetInternetGiB := ctx.QueryAtTime(queryNetInternetGiB, end)

	queryNetInternetCostPerGiB := fmt.Sprintf(queryFmtNetInternetCostPerGiB, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())
	resChNetInternetCostPerGiB := ctx.QueryAtTime(queryNetInternetCostPerGiB, end)

	//GPU Queries
	queryIsGpuShared := fmt.Sprintf(queryFmtIsGPuShared, env.GetPromClusterFilter(), durStr)
	resChIsGpuShared := ctx.QueryAtTime(queryIsGpuShared, end)

	queryGetGPUInfo := fmt.Sprintf(queryFmtGetGPuInfo, env.GetPromClusterFilter(), durStr)
	resChGetGPUInfo := ctx.QueryAtTime(queryGetGPUInfo, end)

	var resChNodeLabels prom.QueryResultsChan
	if env.GetAllocationNodeLabelsEnabled() {
		queryNodeLabels := fmt.Sprintf(queryFmtNodeLabels, env.GetPromClusterFilter(), durStr)
		resChNodeLabels = ctx.QueryAtTime(queryNodeLabels, end)
	}

	queryNamespaceLabels := fmt.Sprintf(queryFmtNamespaceLabels, env.GetPromClusterFilter(), durStr)
	resChNamespaceLabels := ctx.QueryAtTime(queryNamespaceLabels, end)

	queryNamespaceAnnotations := fmt.Sprintf(queryFmtNamespaceAnnotations, env.GetPromClusterFilter(), durStr)
	resChNamespaceAnnotations := ctx.QueryAtTime(queryNamespaceAnnotations, end)

	queryPodLabels := fmt.Sprintf(queryFmtPodLabels, env.GetPromClusterFilter(), durStr)
	resChPodLabels := ctx.QueryAtTime(queryPodLabels, end)

	queryPodAnnotations := fmt.Sprintf(queryFmtPodAnnotations, env.GetPromClusterFilter(), durStr)
	resChPodAnnotations := ctx.QueryAtTime(queryPodAnnotations, end)

	queryServiceLabels := fmt.Sprintf(queryFmtServiceLabels, env.GetPromClusterFilter(), durStr)
	resChServiceLabels := ctx.QueryAtTime(queryServiceLabels, end)

	queryDeploymentLabels := fmt.Sprintf(queryFmtDeploymentLabels, env.GetPromClusterFilter(), durStr)
	resChDeploymentLabels := ctx.QueryAtTime(queryDeploymentLabels, end)

	queryStatefulSetLabels := fmt.Sprintf(queryFmtStatefulSetLabels, env.GetPromClusterFilter(), durStr)
	resChStatefulSetLabels := ctx.QueryAtTime(queryStatefulSetLabels, end)

	queryDaemonSetLabels := fmt.Sprintf(queryFmtDaemonSetLabels, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())
	resChDaemonSetLabels := ctx.QueryAtTime(queryDaemonSetLabels, end)

	queryPodsWithReplicaSetOwner := fmt.Sprintf(queryFmtPodsWithReplicaSetOwner, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())
	resChPodsWithReplicaSetOwner := ctx.QueryAtTime(queryPodsWithReplicaSetOwner, end)

	queryReplicaSetsWithoutOwners := fmt.Sprintf(queryFmtReplicaSetsWithoutOwners, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())
	resChReplicaSetsWithoutOwners := ctx.QueryAtTime(queryReplicaSetsWithoutOwners, end)

	queryReplicaSetsWithRolloutOwner := fmt.Sprintf(queryFmtReplicaSetsWithRolloutOwner, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())
	resChReplicaSetsWithRolloutOwner := ctx.QueryAtTime(queryReplicaSetsWithRolloutOwner, end)

	queryJobLabels := fmt.Sprintf(queryFmtJobLabels, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())
	resChJobLabels := ctx.QueryAtTime(queryJobLabels, end)

	queryLBCostPerHr := fmt.Sprintf(queryFmtLBCostPerHr, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())
	resChLBCostPerHr := ctx.QueryAtTime(queryLBCostPerHr, end)

	queryLBActiveMins := fmt.Sprintf(queryFmtLBActiveMins, env.GetPromClusterFilter(), env.GetPromClusterLabel(), durStr, resStr)
	resChLBActiveMins := ctx.QueryAtTime(queryLBActiveMins, end)

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
	nodeExtendedData, _ := queryExtendedNodeData(ctx, start, end, durStr, resStr)

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

	var resNodeLabels []*prom.QueryResult
	if env.GetAllocationNodeLabelsEnabled() {
		if env.GetAllocationNodeLabelsEnabled() {
			resNodeLabels, _ = resChNodeLabels.Await()
		}
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

	if ctx.HasErrors() {
		for _, err := range ctx.Errors() {
			log.Errorf("CostModel.ComputeAllocation: query context error %s", err)
		}

		return allocSet, nil, ctx.ErrorCollection()
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
