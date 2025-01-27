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

type OpenCostDataSource interface {
	InstantMetricsQuerier
	RangeMetricsQuerier
	ClusterMetricsQuerier

	RegisterEndPoints(router *httprouter.Router)

	BatchDuration() time.Duration
}
