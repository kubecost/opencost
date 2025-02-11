package source

import (
	"time"

	"github.com/julienschmidt/httprouter"
)

type InstantMetricsQuerier interface {
	QueryRAMUsage(window string, offset string) QueryResultsChan
	QueryCPUUsage(window string, offset string) QueryResultsChan
	QueryNetworkInZoneRequests(window string, offset string) QueryResultsChan
	QueryNetworkInRegionRequests(window string, offset string) QueryResultsChan
	QueryNetworkInternetRequests(window string, offset string) QueryResultsChan
	QueryNormalization(window string, offset string) QueryResultsChan

	QueryHistoricalCPUCost(window string, offset string) QueryResultsChan
	QueryHistoricalRAMCost(window string, offset string) QueryResultsChan
	QueryHistoricalGPUCost(window string, offset string) QueryResultsChan
	QueryHistoricalPodLabels(window string, offset string) QueryResultsChan
}

type RangeMetricsQuerier interface {
	QueryRAMRequestsOverTime(start, end time.Time, resolution time.Duration) QueryResultsChan
	QueryRAMUsageOverTime(start, end time.Time, resolution time.Duration) QueryResultsChan
	QueryRAMAllocationOverTime(start, end time.Time, resolution time.Duration) QueryResultsChan

	QueryCPURequestsOverTime(start, end time.Time, resolution time.Duration) QueryResultsChan
	QueryCPUUsageOverTime(start, end time.Time, resolution time.Duration) QueryResultsChan
	QueryCPUAllocationOverTime(start, end time.Time, resolution time.Duration) QueryResultsChan

	QueryGPURequestsOverTime(start, end time.Time, resolution time.Duration) QueryResultsChan

	QueryPVRequestsOverTime(start, end time.Time, resolution time.Duration) QueryResultsChan
	QueryPVCAllocationOverTime(start, end time.Time, resolution time.Duration) QueryResultsChan
	QueryPVHourlyCostOverTime(start, end time.Time, resolution time.Duration) QueryResultsChan

	QueryNetworkInZoneOverTime(start, end time.Time, resolution time.Duration) QueryResultsChan
	QueryNetworkInRegionOverTime(start, end time.Time, resolution time.Duration) QueryResultsChan
	QueryNetworkInternetOverTime(start, end time.Time, resolution time.Duration) QueryResultsChan

	QueryNamespaceLabelsOverTime(start, end time.Time, resolution time.Duration) QueryResultsChan
	QueryNamespaceAnnotationsOverTime(start, end time.Time, resolution time.Duration) QueryResultsChan

	QueryPodLabelsOverTime(start, end time.Time, resolution time.Duration) QueryResultsChan
	QueryPodAnnotationsOverTime(start, end time.Time, resolution time.Duration) QueryResultsChan

	QueryServiceLabelsOverTime(start, end time.Time, resolution time.Duration) QueryResultsChan
	QueryDeploymentLabelsOverTime(start, end time.Time, resolution time.Duration) QueryResultsChan
	QueryStatefulsetLabelsOverTime(start, end time.Time, resolution time.Duration) QueryResultsChan

	QueryPodJobsOverTime(start, end time.Time, resolution time.Duration) QueryResultsChan
	QueryPodDaemonsetsOverTime(start, end time.Time, resolution time.Duration) QueryResultsChan

	QueryNormalizationOverTime(start, end time.Time, resolution time.Duration) QueryResultsChan
}

type ClusterMetricsQuerier interface {
	// Cluster Disks
	QueryPVCost(start, end time.Time) QueryResultsChan
	QueryPVSize(start, end time.Time) QueryResultsChan
	QueryPVStorageClass(start, end time.Time) QueryResultsChan
	QueryPVUsedAverage(start, end time.Time) QueryResultsChan
	QueryPVUsedMax(start, end time.Time) QueryResultsChan
	QueryPVCInfo(start, end time.Time) QueryResultsChan
	QueryPVActiveMinutes(start, end time.Time) QueryResultsChan

	// Local Cluster Disks
	QueryLocalStorageCost(start, end time.Time) QueryResultsChan
	QueryLocalStorageUsedCost(start, end time.Time) QueryResultsChan
	QueryLocalStorageUsedAvg(start, end time.Time) QueryResultsChan
	QueryLocalStorageUsedMax(start, end time.Time) QueryResultsChan
	QueryLocalStorageBytes(start, end time.Time) QueryResultsChan
	QueryLocalStorageActiveMinutes(start, end time.Time) QueryResultsChan
	QueryLocalStorageBytesByProvider(provider string, start, end time.Time) QueryResultsChan
	QueryLocalStorageUsedByProvider(provider string, start, end time.Time) QueryResultsChan

	// Nodes
	QueryNodeCPUHourlyCost(start, end time.Time) QueryResultsChan
	QueryNodeCPUCoresCapacity(start, end time.Time) QueryResultsChan
	QueryNodeCPUCoresAllocatable(start, end time.Time) QueryResultsChan
	QueryNodeRAMHourlyCost(start, end time.Time) QueryResultsChan
	QueryNodeRAMBytesCapacity(start, end time.Time) QueryResultsChan
	QueryNodeRAMBytesAllocatable(start, end time.Time) QueryResultsChan
	QueryNodeGPUCount(start, end time.Time) QueryResultsChan
	QueryNodeGPUHourlyCost(start, end time.Time) QueryResultsChan
	QueryNodeLabels(start, end time.Time) QueryResultsChan
	QueryNodeActiveMinutes(start, end time.Time) QueryResultsChan
	QueryNodeIsSpot(start, end time.Time) QueryResultsChan
	QueryNodeCPUModeTotal(start, end time.Time) QueryResultsChan

	QueryNodeCPUModePercent(start, end time.Time) QueryResultsChan
	QueryNodeRAMSystemPercent(start, end time.Time) QueryResultsChan
	QueryNodeRAMUserPercent(start, end time.Time) QueryResultsChan

	QueryNodeTotalLocalStorage(start, end time.Time) QueryResultsChan
	QueryNodeUsedLocalStorage(start, end time.Time) QueryResultsChan

	// Load Balancers
	QueryLBCost(start, end time.Time) QueryResultsChan
	QueryLBActiveMinutes(start, end time.Time) QueryResultsChan

	// Cluster Costs
	QueryDataCount(start, end time.Time) QueryResultsChan
	QueryTotalGPU(start, end time.Time) QueryResultsChan
	QueryTotalCPU(start, end time.Time) QueryResultsChan
	QueryTotalRAM(start, end time.Time) QueryResultsChan
	QueryTotalStorage(start, end time.Time) QueryResultsChan

	// Cluster Costs
	QueryClusterCores(start, end time.Time, step time.Duration) QueryResultsChan
	QueryClusterRAM(start, end time.Time, step time.Duration) QueryResultsChan
	QueryClusterStorage(start, end time.Time, step time.Duration) QueryResultsChan
	QueryClusterStorageByProvider(provider string, start, end time.Time, step time.Duration) QueryResultsChan
	QueryClusterTotal(start, end time.Time, step time.Duration) QueryResultsChan
	QueryClusterTotalByProvider(provider string, start, end time.Time, step time.Duration) QueryResultsChan
	QueryClusterNodes(start, end time.Time, step time.Duration) QueryResultsChan
	QueryClusterNodesByProvider(provider string, start, end time.Time, step time.Duration) QueryResultsChan
}

type AllocationMetricsQuerier interface {
	QueryPods(start, end time.Time) QueryResultsChan
	QueryPodsUID(start, end time.Time) QueryResultsChan
	QueryRAMBytesAllocated(start, end time.Time) QueryResultsChan
	QueryRAMRequests(start, end time.Time) QueryResultsChan
	QueryRAMUsageAvg(start, end time.Time) QueryResultsChan
	QueryRAMUsageMax(start, end time.Time) QueryResultsChan
	QueryCPUCoresAllocated(start, end time.Time) QueryResultsChan
	QueryCPURequests(start, end time.Time) QueryResultsChan
	QueryCPUUsageAvg(start, end time.Time) QueryResultsChan
	QueryCPUUsageMax(start, end time.Time) QueryResultsChan
	QueryGPUsRequested(start, end time.Time) QueryResultsChan
	QueryGPUsUsageAvg(start, end time.Time) QueryResultsChan
	QueryGPUsUsageMax(start, end time.Time) QueryResultsChan
	QueryGPUsAllocated(start, end time.Time) QueryResultsChan
	QueryNodeCostPerCPUHr(start, end time.Time) QueryResultsChan
	QueryNodeCostPerRAMGiBHr(start, end time.Time) QueryResultsChan
	QueryNodeCostPerGPUHr(start, end time.Time) QueryResultsChan
	QueryNodeIsSpot2(start, end time.Time) QueryResultsChan
	QueryPVCInfo2(start, end time.Time) QueryResultsChan
	QueryPodPVCAllocation(start, end time.Time) QueryResultsChan
	QueryPVCBytesRequested(start, end time.Time) QueryResultsChan
	QueryPVActiveMins(start, end time.Time) QueryResultsChan
	QueryPVBytes(start, end time.Time) QueryResultsChan
	QueryPVCostPerGiBHour(start, end time.Time) QueryResultsChan
	QueryPVMeta(start, end time.Time) QueryResultsChan
	QueryNetZoneGiB(start, end time.Time) QueryResultsChan
	QueryNetZoneCostPerGiB(start, end time.Time) QueryResultsChan
	QueryNetRegionGiB(start, end time.Time) QueryResultsChan
	QueryNetRegionCostPerGiB(start, end time.Time) QueryResultsChan
	QueryNetInternetGiB(start, end time.Time) QueryResultsChan
	QueryNetInternetCostPerGiB(start, end time.Time) QueryResultsChan
	QueryNetReceiveBytes(start, end time.Time) QueryResultsChan
	QueryNetTransferBytes(start, end time.Time) QueryResultsChan
	QueryNodeLabels(start, end time.Time) QueryResultsChan
	QueryNamespaceLabels(start, end time.Time) QueryResultsChan
	QueryNamespaceAnnotations(start, end time.Time) QueryResultsChan
	QueryPodLabels(start, end time.Time) QueryResultsChan
	QueryPodAnnotations(start, end time.Time) QueryResultsChan
	QueryServiceLabels(start, end time.Time) QueryResultsChan
	QueryDeploymentLabels(start, end time.Time) QueryResultsChan
	QueryStatefulSetLabels(start, end time.Time) QueryResultsChan
	QueryDaemonSetLabels(start, end time.Time) QueryResultsChan
	QueryJobLabels(start, end time.Time) QueryResultsChan
	QueryPodsWithReplicaSetOwner(start, end time.Time) QueryResultsChan
	QueryReplicaSetsWithoutOwners(start, end time.Time) QueryResultsChan
	QueryReplicaSetsWithRollout(start, end time.Time) QueryResultsChan
	QueryLBCostPerHr(start, end time.Time) QueryResultsChan
	QueryLBActiveMins(start, end time.Time) QueryResultsChan
	QueryDataCoverage(limitDays int) (time.Time, time.Time, error)
	QueryIsGPUShared(start, end time.Time) QueryResultsChan
	QueryGetGPUInfo(start, end time.Time) QueryResultsChan
}

type OpenCostDataSource interface {
	InstantMetricsQuerier
	RangeMetricsQuerier
	ClusterMetricsQuerier
	AllocationMetricsQuerier

	RegisterEndPoints(router *httprouter.Router)

	BatchDuration() time.Duration
	Resolution() time.Duration
}
