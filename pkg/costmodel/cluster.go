package costmodel

import (
	"fmt"
	"time"

	"github.com/kubecost/cost-model/pkg/cloud"
	"github.com/kubecost/cost-model/pkg/env"
	"github.com/kubecost/cost-model/pkg/log"
	"github.com/kubecost/cost-model/pkg/prom"
	"github.com/kubecost/cost-model/pkg/util"

	prometheus "github.com/prometheus/client_golang/api"
	"k8s.io/klog"
)

const (
	queryClusterCores = `sum(
		avg(avg_over_time(kube_node_status_capacity_cpu_cores[%s] %s)) by (node, cluster_id) * avg(avg_over_time(node_cpu_hourly_cost[%s] %s)) by (node, cluster_id) * 730 +
		avg(avg_over_time(node_gpu_hourly_cost[%s] %s)) by (node, cluster_id) * 730
	  ) by (cluster_id)`

	queryClusterRAM = `sum(
		avg(avg_over_time(kube_node_status_capacity_memory_bytes[%s] %s)) by (node, cluster_id) / 1024 / 1024 / 1024 * avg(avg_over_time(node_ram_hourly_cost[%s] %s)) by (node, cluster_id) * 730
	  ) by (cluster_id)`

	queryStorage = `sum(
		avg(avg_over_time(pv_hourly_cost[%s] %s)) by (persistentvolume, cluster_id) * 730
		* avg(avg_over_time(kube_persistentvolume_capacity_bytes[%s] %s)) by (persistentvolume, cluster_id) / 1024 / 1024 / 1024
	  ) by (cluster_id) %s`

	queryTotal = `sum(avg(node_total_hourly_cost) by (node, cluster_id)) * 730 +
	  sum(
		avg(avg_over_time(pv_hourly_cost[1h])) by (persistentvolume, cluster_id) * 730
		* avg(avg_over_time(kube_persistentvolume_capacity_bytes[1h])) by (persistentvolume, cluster_id) / 1024 / 1024 / 1024
	  ) by (cluster_id) %s`

	queryNodes = `sum(avg(node_total_hourly_cost) by (node, cluster_id)) * 730 %s`
)

// Costs represents cumulative and monthly cluster costs over a given duration. Costs
// are broken down by cores, memory, and storage.
type ClusterCosts struct {
	Start             *time.Time             `json:"startTime"`
	End               *time.Time             `json:"endTime"`
	CPUCumulative     float64                `json:"cpuCumulativeCost"`
	CPUMonthly        float64                `json:"cpuMonthlyCost"`
	CPUBreakdown      *ClusterCostsBreakdown `json:"cpuBreakdown"`
	GPUCumulative     float64                `json:"gpuCumulativeCost"`
	GPUMonthly        float64                `json:"gpuMonthlyCost"`
	RAMCumulative     float64                `json:"ramCumulativeCost"`
	RAMMonthly        float64                `json:"ramMonthlyCost"`
	RAMBreakdown      *ClusterCostsBreakdown `json:"ramBreakdown"`
	StorageCumulative float64                `json:"storageCumulativeCost"`
	StorageMonthly    float64                `json:"storageMonthlyCost"`
	StorageBreakdown  *ClusterCostsBreakdown `json:"storageBreakdown"`
	TotalCumulative   float64                `json:"totalCumulativeCost"`
	TotalMonthly      float64                `json:"totalMonthlyCost"`
	DataMinutes       float64
}

// ClusterCostsBreakdown provides percentage-based breakdown of a resource by
// categories: user for user-space (i.e. non-system) usage, system, and idle.
type ClusterCostsBreakdown struct {
	Idle   float64 `json:"idle"`
	Other  float64 `json:"other"`
	System float64 `json:"system"`
	User   float64 `json:"user"`
}

// NewClusterCostsFromCumulative takes cumulative cost data over a given time range, computes
// the associated monthly rate data, and returns the Costs.
func NewClusterCostsFromCumulative(cpu, gpu, ram, storage float64, window, offset string, dataHours float64) (*ClusterCosts, error) {
	start, end, err := util.ParseTimeRange(window, offset)
	if err != nil {
		return nil, err
	}

	// If the number of hours is not given (i.e. is zero) compute one from the window and offset
	if dataHours == 0 {
		dataHours = end.Sub(*start).Hours()
	}

	// Do not allow zero-length windows to prevent divide-by-zero issues
	if dataHours == 0 {
		return nil, fmt.Errorf("illegal time range: window %s, offset %s", window, offset)
	}

	cc := &ClusterCosts{
		Start:             start,
		End:               end,
		CPUCumulative:     cpu,
		GPUCumulative:     gpu,
		RAMCumulative:     ram,
		StorageCumulative: storage,
		TotalCumulative:   cpu + gpu + ram + storage,
		CPUMonthly:        cpu / dataHours * (util.HoursPerMonth),
		GPUMonthly:        gpu / dataHours * (util.HoursPerMonth),
		RAMMonthly:        ram / dataHours * (util.HoursPerMonth),
		StorageMonthly:    storage / dataHours * (util.HoursPerMonth),
	}
	cc.TotalMonthly = cc.CPUMonthly + cc.GPUMonthly + cc.RAMMonthly + cc.StorageMonthly

	return cc, nil
}

type Disk struct {
	Cluster    string
	Name       string
	ProviderID string
	Cost       float64
	Bytes      float64
	Local      bool
	Start      time.Time
	End        time.Time
	Minutes    float64
	Breakdown  *ClusterCostsBreakdown
}

func ClusterDisks(client prometheus.Client, provider cloud.Provider, duration, offset time.Duration) (map[string]*Disk, error) {
	durationStr := fmt.Sprintf("%dm", int64(duration.Minutes()))
	offsetStr := fmt.Sprintf(" offset %dm", int64(offset.Minutes()))
	if offset < time.Minute {
		offsetStr = ""
	}

	// minsPerResolution determines accuracy and resource use for the following
	// queries. Smaller values (higher resolution) result in better accuracy,
	// but more expensive queries, and vice-a-versa.
	minsPerResolution := 1
	resolution := time.Duration(minsPerResolution) * time.Minute

	// hourlyToCumulative is a scaling factor that, when multiplied by an hourly
	// value, converts it to a cumulative value; i.e.
	// [$/hr] * [min/res]*[hr/min] = [$/res]
	hourlyToCumulative := float64(minsPerResolution) * (1.0 / 60.0)

	// TODO niko/assets how do we not hard-code this price?
	costPerGBHr := 0.04 / 730.0

	ctx := prom.NewContext(client)
	queryPVCost := fmt.Sprintf(`sum_over_time((avg(kube_persistentvolume_capacity_bytes) by (cluster_id, persistentvolume)  * on(cluster_id, persistentvolume) group_right avg(pv_hourly_cost) by (cluster_id, persistentvolume,provider_id))[%s:%dm]%s)/1024/1024/1024 * %f`, durationStr, minsPerResolution, offsetStr, hourlyToCumulative)
	queryPVSize := fmt.Sprintf(`avg_over_time(kube_persistentvolume_capacity_bytes[%s:%dm]%s)`, durationStr, minsPerResolution, offsetStr)
	queryActiveMins := fmt.Sprintf(`count(pv_hourly_cost) by (cluster_id, persistentvolume)[%s:%dm]%s`, durationStr, minsPerResolution, offsetStr)

	queryLocalStorageCost := fmt.Sprintf(`sum_over_time(sum(container_fs_limit_bytes{device!="tmpfs", id="/"}) by (instance, cluster_id)[%s:%dm]%s) / 1024 / 1024 / 1024 * %f * %f`, durationStr, minsPerResolution, offsetStr, hourlyToCumulative, costPerGBHr)
	queryLocalStorageUsedCost := fmt.Sprintf(`sum_over_time(sum(container_fs_usage_bytes{device!="tmpfs", id="/"}) by (instance, cluster_id)[%s:%dm]%s) / 1024 / 1024 / 1024 * %f * %f`, durationStr, minsPerResolution, offsetStr, hourlyToCumulative, costPerGBHr)
	queryLocalStorageBytes := fmt.Sprintf(`avg_over_time(sum(container_fs_limit_bytes{device!="tmpfs", id="/"}) by (instance, cluster_id)[%s:%dm]%s)`, durationStr, minsPerResolution, offsetStr)
	queryLocalActiveMins := fmt.Sprintf(`count(node_total_hourly_cost) by (cluster_id, node)[%s:%dm]%s`, durationStr, minsPerResolution, offsetStr)

	resChPVCost := ctx.Query(queryPVCost)
	resChPVSize := ctx.Query(queryPVSize)
	resChActiveMins := ctx.Query(queryActiveMins)
	resChLocalStorageCost := ctx.Query(queryLocalStorageCost)
	resChLocalStorageUsedCost := ctx.Query(queryLocalStorageUsedCost)
	resChLocalStorageBytes := ctx.Query(queryLocalStorageBytes)
	resChLocalActiveMins := ctx.Query(queryLocalActiveMins)

	resPVCost, _ := resChPVCost.Await()
	resPVSize, _ := resChPVSize.Await()
	resActiveMins, _ := resChActiveMins.Await()
	resLocalStorageCost, _ := resChLocalStorageCost.Await()
	resLocalStorageUsedCost, _ := resChLocalStorageUsedCost.Await()
	resLocalStorageBytes, _ := resChLocalStorageBytes.Await()
	resLocalActiveMins, _ := resChLocalActiveMins.Await()
	if ctx.HasErrors() {
		return nil, ctx.ErrorCollection()
	}

	diskMap := map[string]*Disk{}

	for _, result := range resPVCost {
		cluster, err := result.GetString("cluster_id")
		if err != nil {
			cluster = env.GetClusterID()
		}

		name, err := result.GetString("persistentvolume")
		if err != nil {
			log.Warningf("ClusterDisks: PV cost data missing persistentvolume")
			continue
		}

		// TODO niko/assets storage class

		cost := result.Values[0].Value
		key := fmt.Sprintf("%s/%s", cluster, name)
		if _, ok := diskMap[key]; !ok {
			diskMap[key] = &Disk{
				Cluster:   cluster,
				Name:      name,
				Breakdown: &ClusterCostsBreakdown{},
			}
		}
		diskMap[key].Cost += cost
		providerID, _ := result.GetString("provider_id") // just put the providerID set up here, it's the simplest query.
		if providerID != "" {
			diskMap[key].ProviderID = provider.ParsePVID(providerID)
		}
	}

	for _, result := range resPVSize {
		cluster, err := result.GetString("cluster_id")
		if err != nil {
			cluster = env.GetClusterID()
		}

		name, err := result.GetString("persistentvolume")
		if err != nil {
			log.Warningf("ClusterDisks: PV size data missing persistentvolume")
			continue
		}

		// TODO niko/assets storage class

		bytes := result.Values[0].Value
		key := fmt.Sprintf("%s/%s", cluster, name)
		if _, ok := diskMap[key]; !ok {
			diskMap[key] = &Disk{
				Cluster:   cluster,
				Name:      name,
				Breakdown: &ClusterCostsBreakdown{},
			}
		}
		diskMap[key].Bytes = bytes
	}

	for _, result := range resLocalStorageCost {
		cluster, err := result.GetString("cluster_id")
		if err != nil {
			cluster = env.GetClusterID()
		}

		name, err := result.GetString("instance")
		if err != nil {
			log.Warningf("ClusterDisks: local storage data missing instance")
			continue
		}

		cost := result.Values[0].Value
		key := fmt.Sprintf("%s/%s", cluster, name)
		if _, ok := diskMap[key]; !ok {
			diskMap[key] = &Disk{
				Cluster:   cluster,
				Name:      name,
				Breakdown: &ClusterCostsBreakdown{},
				Local:     true,
			}
		}
		diskMap[key].Cost += cost
	}

	for _, result := range resLocalStorageUsedCost {
		cluster, err := result.GetString("cluster_id")
		if err != nil {
			cluster = env.GetClusterID()
		}

		name, err := result.GetString("instance")
		if err != nil {
			log.Warningf("ClusterDisks: local storage usage data missing instance")
			continue
		}

		cost := result.Values[0].Value
		key := fmt.Sprintf("%s/%s", cluster, name)
		if _, ok := diskMap[key]; !ok {
			diskMap[key] = &Disk{
				Cluster:   cluster,
				Name:      name,
				Breakdown: &ClusterCostsBreakdown{},
				Local:     true,
			}
		}
		diskMap[key].Breakdown.System = cost / diskMap[key].Cost
	}

	for _, result := range resLocalStorageBytes {
		cluster, err := result.GetString("cluster_id")
		if err != nil {
			cluster = env.GetClusterID()
		}

		name, err := result.GetString("instance")
		if err != nil {
			log.Warningf("ClusterDisks: local storage data missing instance")
			continue
		}

		bytes := result.Values[0].Value
		key := fmt.Sprintf("%s/%s", cluster, name)
		if _, ok := diskMap[key]; !ok {
			diskMap[key] = &Disk{
				Cluster:   cluster,
				Name:      name,
				Breakdown: &ClusterCostsBreakdown{},
				Local:     true,
			}
		}
		diskMap[key].Bytes = bytes
	}

	for _, result := range resActiveMins {
		cluster, err := result.GetString("cluster_id")
		if err != nil {
			cluster = env.GetClusterID()
		}

		name, err := result.GetString("persistentvolume")
		if err != nil {
			log.Warningf("ClusterDisks: active mins missing instance")
			continue
		}

		key := fmt.Sprintf("%s/%s", cluster, name)
		if _, ok := diskMap[key]; !ok {
			log.Warningf("ClusterDisks: active mins for unidentified disk")
			continue
		}

		if len(result.Values) == 0 {
			continue
		}

		s := time.Unix(int64(result.Values[0].Timestamp), 0)
		e := time.Unix(int64(result.Values[len(result.Values)-1].Timestamp), 0).Add(resolution)
		mins := e.Sub(s).Minutes()

		// TODO niko/assets if mins >= threshold, interpolate for missing data?

		diskMap[key].End = e
		diskMap[key].Start = s
		diskMap[key].Minutes = mins
	}

	for _, result := range resLocalActiveMins {
		cluster, err := result.GetString("cluster_id")
		if err != nil {
			cluster = env.GetClusterID()
		}

		name, err := result.GetString("node")
		if err != nil {
			log.Warningf("ClusterDisks: local active mins data missing instance")
			continue
		}

		key := fmt.Sprintf("%s/%s", cluster, name)
		if _, ok := diskMap[key]; !ok {
			log.Warningf("ClusterDisks: local active mins for unidentified disk")
			continue
		}

		if len(result.Values) == 0 {
			continue
		}

		s := time.Unix(int64(result.Values[0].Timestamp), 0)
		e := time.Unix(int64(result.Values[len(result.Values)-1].Timestamp), 0).Add(resolution)
		mins := e.Sub(s).Minutes()

		// TODO niko/assets if mins >= threshold, interpolate for missing data?

		diskMap[key].End = e
		diskMap[key].Start = s
		diskMap[key].Minutes = mins
	}

	for _, disk := range diskMap {
		// Apply all remaining RAM to Idle
		disk.Breakdown.Idle = 1.0 - (disk.Breakdown.System + disk.Breakdown.Other + disk.Breakdown.User)
	}

	return diskMap, nil
}

type Node struct {
	Cluster      string
	Name         string
	ProviderID   string
	NodeType     string
	CPUCost      float64
	CPUCores     float64
	GPUCost      float64
	RAMCost      float64
	RAMBytes     float64
	Discount     float64
	Preemptible  bool
	CPUBreakdown *ClusterCostsBreakdown
	RAMBreakdown *ClusterCostsBreakdown
	Start        time.Time
	End          time.Time
	Minutes      float64
	Labels       map[string]string
}

// GKE lies about the number of cores e2 nodes have. This table
// contains a mapping from node type -> actual CPU cores
// for those cases.
var partialCPUMap = map[string]float64{
	"e2-micro":  0.25,
	"e2-small":  0.5,
	"e2-medium": 1.0,
}

type NodeIdentifier struct {
	Cluster    string
	Name       string
	ProviderID string
}

type nodeIdentifierNoProviderID struct {
	Cluster string
	Name    string
}

func ClusterNodes(cp cloud.Provider, client prometheus.Client, duration, offset time.Duration) (map[NodeIdentifier]*Node, error) {
	durationStr := fmt.Sprintf("%dm", int64(duration.Minutes()))
	offsetStr := fmt.Sprintf(" offset %dm", int64(offset.Minutes()))
	if offset < time.Minute {
		offsetStr = ""
	}

	// minsPerResolution determines accuracy and resource use for the following
	// queries. Smaller values (higher resolution) result in better accuracy,
	// but more expensive queries, and vice-a-versa.
	minsPerResolution := 1
	resolution := time.Duration(minsPerResolution) * time.Minute

	// hourlyToCumulative is a scaling factor that, when multiplied by an hourly
	// value, converts it to a cumulative value; i.e.
	// [$/hr] * [min/res]*[hr/min] = [$/res]
	hourlyToCumulative := float64(minsPerResolution) * (1.0 / 60.0)

	requiredCtx := prom.NewContext(client)
	optionalCtx := prom.NewContext(client)

	queryNodeCPUCost := fmt.Sprintf(`sum_over_time((avg(kube_node_status_capacity_cpu_cores) by (cluster_id, node) * on(node, cluster_id) group_right avg(node_cpu_hourly_cost) by (cluster_id, node, instance_type, provider_id))[%s:%dm]%s) * %f`, durationStr, minsPerResolution, offsetStr, hourlyToCumulative)
	queryNodeCPUCores := fmt.Sprintf(`avg_over_time(avg(kube_node_status_capacity_cpu_cores) by (cluster_id, node)[%s:%dm]%s)`, durationStr, minsPerResolution, offsetStr)
	queryNodeRAMCost := fmt.Sprintf(`sum_over_time((avg(kube_node_status_capacity_memory_bytes) by (cluster_id, node) * on(cluster_id, node) group_right avg(node_ram_hourly_cost) by (cluster_id, node, instance_type, provider_id))[%s:%dm]%s) / 1024 / 1024 / 1024 * %f`, durationStr, minsPerResolution, offsetStr, hourlyToCumulative)
	queryNodeRAMBytes := fmt.Sprintf(`avg_over_time(avg(kube_node_status_capacity_memory_bytes) by (cluster_id, node)[%s:%dm]%s)`, durationStr, minsPerResolution, offsetStr)
	queryNodeGPUCost := fmt.Sprintf(`sum_over_time((avg(node_gpu_hourly_cost * %d.0 / 60.0) by (cluster_id, node, provider_id))[%s:%dm]%s)`, minsPerResolution, durationStr, minsPerResolution, offsetStr)
	queryNodeCPUModeTotal := fmt.Sprintf(`sum(rate(node_cpu_seconds_total[%s:%dm]%s)) by (kubernetes_node, cluster_id, mode)`, durationStr, minsPerResolution, offsetStr)
	queryNodeRAMSystemPct := fmt.Sprintf(`sum(sum_over_time(container_memory_working_set_bytes{container_name!="POD",container_name!="",namespace="kube-system"}[%s:%dm]%s)) by (instance, cluster_id) / avg(label_replace(sum(sum_over_time(kube_node_status_capacity_memory_bytes[%s:%dm]%s)) by (node, cluster_id), "instance", "$1", "node", "(.*)")) by (instance, cluster_id)`, durationStr, minsPerResolution, offsetStr, durationStr, minsPerResolution, offsetStr)
	queryNodeRAMUserPct := fmt.Sprintf(`sum(sum_over_time(container_memory_working_set_bytes{container_name!="POD",container_name!="",namespace!="kube-system"}[%s:%dm]%s)) by (instance, cluster_id) / avg(label_replace(sum(sum_over_time(kube_node_status_capacity_memory_bytes[%s:%dm]%s)) by (node, cluster_id), "instance", "$1", "node", "(.*)")) by (instance, cluster_id)`, durationStr, minsPerResolution, offsetStr, durationStr, minsPerResolution, offsetStr)
	queryActiveMins := fmt.Sprintf(`avg(node_total_hourly_cost) by (node, cluster_id, provider_id)[%s:%dm]%s`, durationStr, minsPerResolution, offsetStr)
	queryIsSpot := fmt.Sprintf(`avg_over_time(kubecost_node_is_spot[%s:%dm]%s)`, durationStr, minsPerResolution, offsetStr)
	queryLabels := fmt.Sprintf(`count_over_time(kube_node_labels[%s:%dm]%s)`, durationStr, minsPerResolution, offsetStr)

	// Return errors if these fail
	resChNodeCPUCost := requiredCtx.Query(queryNodeCPUCost)
	resChNodeCPUCores := requiredCtx.Query(queryNodeCPUCores)
	resChNodeRAMCost := requiredCtx.Query(queryNodeRAMCost)
	resChNodeRAMBytes := requiredCtx.Query(queryNodeRAMBytes)
	resChNodeGPUCost := requiredCtx.Query(queryNodeGPUCost)
	resChActiveMins := requiredCtx.Query(queryActiveMins)
	resChIsSpot := requiredCtx.Query(queryIsSpot)

	// Do not return errors if these fail, but log warnings
	resChNodeCPUModeTotal := optionalCtx.Query(queryNodeCPUModeTotal)
	resChNodeRAMSystemPct := optionalCtx.Query(queryNodeRAMSystemPct)
	resChNodeRAMUserPct := optionalCtx.Query(queryNodeRAMUserPct)
	resChLabels := optionalCtx.Query(queryLabels)

	resNodeCPUCost, _ := resChNodeCPUCost.Await()
	resNodeCPUCores, _ := resChNodeCPUCores.Await()
	resNodeGPUCost, _ := resChNodeGPUCost.Await()
	resNodeRAMCost, _ := resChNodeRAMCost.Await()
	resNodeRAMBytes, _ := resChNodeRAMBytes.Await()
	resIsSpot, _ := resChIsSpot.Await()
	resNodeCPUModeTotal, _ := resChNodeCPUModeTotal.Await()
	resNodeRAMSystemPct, _ := resChNodeRAMSystemPct.Await()
	resNodeRAMUserPct, _ := resChNodeRAMUserPct.Await()
	resActiveMins, _ := resChActiveMins.Await()
	resLabels, _ := resChLabels.Await()

	if optionalCtx.HasErrors() {
		for _, err := range optionalCtx.Errors() {
			log.Warningf("ClusterNodes: %s", err)
		}
	}
	if requiredCtx.HasErrors() {
		for _, err := range requiredCtx.Errors() {
			log.Errorf("ClusterNodes: %s", err)
		}

		return nil, requiredCtx.ErrorCollection()
	}

	cpuCostMap, clusterAndNameToType1 := buildCPUCostMap(resNodeCPUCost, cp.ParseID)
	ramCostMap, clusterAndNameToType2 := buildRAMCostMap(resNodeRAMCost, cp.ParseID)
	gpuCostMap, clusterAndNameToType3 := buildGPUCostMap(resNodeGPUCost, cp.ParseID)

	clusterAndNameToTypeIntermediate := mergeTypeMaps(clusterAndNameToType1, clusterAndNameToType2)
	clusterAndNameToType := mergeTypeMaps(clusterAndNameToTypeIntermediate, clusterAndNameToType3)

	cpuCoresMap := buildCPUCoresMap(resNodeCPUCores, clusterAndNameToType)

	ramBytesMap := buildRAMBytesMap(resNodeRAMBytes)

	ramUserPctMap := buildRAMUserPctMap(resNodeRAMUserPct)
	ramSystemPctMap := buildRAMSystemPctMap(resNodeRAMSystemPct)

	cpuBreakdownMap := buildCPUBreakdownMap(resNodeCPUModeTotal)
	activeDataMap := buildActiveDataMap(resActiveMins, resolution, cp.ParseID)
	preemptibleMap := buildPreemptibleMap(resIsSpot, cp.ParseID)
	labelsMap := buildLabelsMap(resLabels)

	nodeMap := buildNodeMap(
		cpuCostMap, ramCostMap, gpuCostMap,
		cpuCoresMap, ramBytesMap, ramUserPctMap,
		ramSystemPctMap,
		cpuBreakdownMap,
		activeDataMap,
		preemptibleMap,
		labelsMap,
		clusterAndNameToType,
	)

	c, err := cp.GetConfig()
	if err != nil {
		return nil, err
	}

	discount, err := ParsePercentString(c.Discount)
	if err != nil {
		return nil, err
	}

	negotiatedDiscount, err := ParsePercentString(c.NegotiatedDiscount)
	if err != nil {
		return nil, err
	}

	for _, node := range nodeMap {
		// TODO take RI into account
		node.Discount = cp.CombinedDiscountForNode(node.NodeType, node.Preemptible, discount, negotiatedDiscount)

		// Apply all remaining resources to Idle
		node.CPUBreakdown.Idle = 1.0 - (node.CPUBreakdown.System + node.CPUBreakdown.Other + node.CPUBreakdown.User)
		node.RAMBreakdown.Idle = 1.0 - (node.RAMBreakdown.System + node.RAMBreakdown.Other + node.RAMBreakdown.User)
	}

	return nodeMap, nil
}

type LoadBalancer struct {
	Cluster    string
	Name       string
	ProviderID string
	Cost       float64
	Start      time.Time
	Minutes    float64
}

func ClusterLoadBalancers(cp cloud.Provider, client prometheus.Client, duration, offset time.Duration) (map[string]*LoadBalancer, error) {
	durationStr := fmt.Sprintf("%dm", int64(duration.Minutes()))
	offsetStr := fmt.Sprintf(" offset %dm", int64(offset.Minutes()))
	if offset < time.Minute {
		offsetStr = ""
	}

	// minsPerResolution determines accuracy and resource use for the following
	// queries. Smaller values (higher resolution) result in better accuracy,
	// but more expensive queries, and vice-a-versa.
	minsPerResolution := 5

	// hourlyToCumulative is a scaling factor that, when multiplied by an hourly
	// value, converts it to a cumulative value; i.e.
	// [$/hr] * [min/res]*[hr/min] = [$/res]
	hourlyToCumulative := float64(minsPerResolution) * (1.0 / 60.0)

	ctx := prom.NewContext(client)
	queryLBCost := fmt.Sprintf(`sum_over_time((avg(kubecost_load_balancer_cost) by (namespace, service_name, cluster_id))[%s:%dm]%s) * %f`, durationStr, minsPerResolution, offsetStr, hourlyToCumulative)
	queryActiveMins := fmt.Sprintf(`count(kubecost_load_balancer_cost) by (namespace, service_name, cluster_id)[%s:%dm]%s`, durationStr, minsPerResolution, offsetStr)

	resChLBCost := ctx.Query(queryLBCost)
	resChActiveMins := ctx.Query(queryActiveMins)

	resLBCost, _ := resChLBCost.Await()
	resActiveMins, _ := resChActiveMins.Await()

	if ctx.HasErrors() {
		return nil, ctx.ErrorCollection()
	}

	loadBalancerMap := map[string]*LoadBalancer{}

	for _, result := range resLBCost {
		cluster, err := result.GetString("cluster_id")
		if err != nil {
			cluster = env.GetClusterID()
		}
		namespace, err := result.GetString("namespace")
		if err != nil {
			log.Warningf("ClusterLoadBalancers: LB cost data missing namespace")
			continue
		}
		serviceName, err := result.GetString("service_name")
		if err != nil {
			log.Warningf("ClusterLoadBalancers: LB cost data missing service_name")
			continue
		}
		providerID := ""
		lbCost := result.Values[0].Value

		key := fmt.Sprintf("%s/%s/%s", cluster, namespace, serviceName)
		if _, ok := loadBalancerMap[key]; !ok {
			loadBalancerMap[key] = &LoadBalancer{
				Cluster:    cluster,
				Name:       namespace + "/" + serviceName,
				ProviderID: providerID, // cp.ParseID(providerID) if providerID does get recorded later
			}
		}
		loadBalancerMap[key].Cost += lbCost
	}

	for _, result := range resActiveMins {
		cluster, err := result.GetString("cluster_id")
		if err != nil {
			cluster = env.GetClusterID()
		}
		namespace, err := result.GetString("namespace")
		if err != nil {
			log.Warningf("ClusterLoadBalancers: LB cost data missing namespace")
			continue
		}
		serviceName, err := result.GetString("service_name")
		if err != nil {
			log.Warningf("ClusterLoadBalancers: LB cost data missing service_name")
			continue
		}
		key := fmt.Sprintf("%s/%s/%s", cluster, namespace, serviceName)

		if len(result.Values) == 0 {
			continue
		}

		s := time.Unix(int64(result.Values[0].Timestamp), 0)
		e := time.Unix(int64(result.Values[len(result.Values)-1].Timestamp), 0)
		mins := e.Sub(s).Minutes()

		// TODO niko/assets if mins >= threshold, interpolate for missing data?

		loadBalancerMap[key].Start = s
		loadBalancerMap[key].Minutes = mins
	}
	return loadBalancerMap, nil
}

// ComputeClusterCosts gives the cumulative and monthly-rate cluster costs over a window of time for all clusters.
func (a *Accesses) ComputeClusterCosts(client prometheus.Client, provider cloud.Provider, window, offset string, withBreakdown bool) (map[string]*ClusterCosts, error) {
	// Compute number of minutes in the full interval, for use interpolating missed scrapes or scaling missing data
	start, end, err := util.ParseTimeRange(window, offset)
	if err != nil {
		return nil, err
	}
	mins := end.Sub(*start).Minutes()

	// minsPerResolution determines accuracy and resource use for the following
	// queries. Smaller values (higher resolution) result in better accuracy,
	// but more expensive queries, and vice-a-versa.
	minsPerResolution := 5

	// hourlyToCumulative is a scaling factor that, when multiplied by an hourly
	// value, converts it to a cumulative value; i.e.
	// [$/hr] * [min/res]*[hr/min] = [$/res]
	hourlyToCumulative := float64(minsPerResolution) * (1.0 / 60.0)

	const fmtQueryDataCount = `
		count_over_time(sum(kube_node_status_capacity_cpu_cores) by (cluster_id)[%s:%dm]%s) * %d
	`

	const fmtQueryTotalGPU = `
		sum(
			sum_over_time(node_gpu_hourly_cost[%s:%dm]%s) * %f
		) by (cluster_id)
	`

	const fmtQueryTotalCPU = `
		sum(
			sum_over_time(avg(kube_node_status_capacity_cpu_cores) by (node, cluster_id)[%s:%dm]%s) *
			avg(avg_over_time(node_cpu_hourly_cost[%s:%dm]%s)) by (node, cluster_id) * %f
		) by (cluster_id)
	`

	const fmtQueryTotalRAM = `
		sum(
			sum_over_time(avg(kube_node_status_capacity_memory_bytes) by (node, cluster_id)[%s:%dm]%s) / 1024 / 1024 / 1024 *
			avg(avg_over_time(node_ram_hourly_cost[%s:%dm]%s)) by (node, cluster_id) * %f
		) by (cluster_id)
	`

	const fmtQueryTotalStorage = `
		sum(
			sum_over_time(avg(kube_persistentvolume_capacity_bytes) by (persistentvolume, cluster_id)[%s:%dm]%s) / 1024 / 1024 / 1024 *
			avg(avg_over_time(pv_hourly_cost[%s:%dm]%s)) by (persistentvolume, cluster_id) * %f
		) by (cluster_id)
	`

	const fmtQueryCPUModePct = `
		sum(rate(node_cpu_seconds_total[%s]%s)) by (cluster_id, mode) / ignoring(mode)
		group_left sum(rate(node_cpu_seconds_total[%s]%s)) by (cluster_id)
	`

	const fmtQueryRAMSystemPct = `
		sum(sum_over_time(container_memory_usage_bytes{container_name!="",namespace="kube-system"}[%s:%dm]%s)) by (cluster_id)
		/ sum(sum_over_time(kube_node_status_capacity_memory_bytes[%s:%dm]%s)) by (cluster_id)
	`

	const fmtQueryRAMUserPct = `
		sum(sum_over_time(kubecost_cluster_memory_working_set_bytes[%s:%dm]%s)) by (cluster_id)
		/ sum(sum_over_time(kube_node_status_capacity_memory_bytes[%s:%dm]%s)) by (cluster_id)
	`

	// TODO niko/clustercost metric "kubelet_volume_stats_used_bytes" was deprecated in 1.12, then seems to have come back in 1.17
	// const fmtQueryPVStorageUsePct = `(sum(kube_persistentvolumeclaim_info) by (persistentvolumeclaim, storageclass,namespace) + on (persistentvolumeclaim,namespace)
	// group_right(storageclass) sum(kubelet_volume_stats_used_bytes) by (persistentvolumeclaim,namespace))`

	queryUsedLocalStorage := provider.GetLocalStorageQuery(window, offset, false, true)

	queryTotalLocalStorage := provider.GetLocalStorageQuery(window, offset, false, false)
	if queryTotalLocalStorage != "" {
		queryTotalLocalStorage = fmt.Sprintf(" + %s", queryTotalLocalStorage)
	}

	fmtOffset := ""
	if offset != "" {
		fmtOffset = fmt.Sprintf("offset %s", offset)
	}

	queryDataCount := fmt.Sprintf(fmtQueryDataCount, window, minsPerResolution, fmtOffset, minsPerResolution)
	queryTotalGPU := fmt.Sprintf(fmtQueryTotalGPU, window, minsPerResolution, fmtOffset, hourlyToCumulative)
	queryTotalCPU := fmt.Sprintf(fmtQueryTotalCPU, window, minsPerResolution, fmtOffset, window, minsPerResolution, fmtOffset, hourlyToCumulative)
	queryTotalRAM := fmt.Sprintf(fmtQueryTotalRAM, window, minsPerResolution, fmtOffset, window, minsPerResolution, fmtOffset, hourlyToCumulative)
	queryTotalStorage := fmt.Sprintf(fmtQueryTotalStorage, window, minsPerResolution, fmtOffset, window, minsPerResolution, fmtOffset, hourlyToCumulative)

	ctx := prom.NewContext(client)

	resChs := ctx.QueryAll(
		queryDataCount,
		queryTotalGPU,
		queryTotalCPU,
		queryTotalRAM,
		queryTotalStorage,
	)

	// Only submit the local storage query if it is valid. Otherwise Prometheus
	// will return errors. Always append something to resChs, regardless, to
	// maintain indexing.
	if queryTotalLocalStorage != "" {
		resChs = append(resChs, ctx.Query(queryTotalLocalStorage))
	} else {
		resChs = append(resChs, nil)
	}

	if withBreakdown {
		queryCPUModePct := fmt.Sprintf(fmtQueryCPUModePct, window, fmtOffset, window, fmtOffset)
		queryRAMSystemPct := fmt.Sprintf(fmtQueryRAMSystemPct, window, minsPerResolution, fmtOffset, window, minsPerResolution, fmtOffset)
		queryRAMUserPct := fmt.Sprintf(fmtQueryRAMUserPct, window, minsPerResolution, fmtOffset, window, minsPerResolution, fmtOffset)

		bdResChs := ctx.QueryAll(
			queryCPUModePct,
			queryRAMSystemPct,
			queryRAMUserPct,
		)

		// Only submit the local storage query if it is valid. Otherwise Prometheus
		// will return errors. Always append something to resChs, regardless, to
		// maintain indexing.
		if queryUsedLocalStorage != "" {
			bdResChs = append(bdResChs, ctx.Query(queryUsedLocalStorage))
		} else {
			bdResChs = append(bdResChs, nil)
		}

		resChs = append(resChs, bdResChs...)
	}

	resDataCount, _ := resChs[0].Await()
	resTotalGPU, _ := resChs[1].Await()
	resTotalCPU, _ := resChs[2].Await()
	resTotalRAM, _ := resChs[3].Await()
	resTotalStorage, _ := resChs[4].Await()
	if ctx.HasErrors() {
		return nil, ctx.ErrorCollection()
	}

	defaultClusterID := env.GetClusterID()

	dataMinsByCluster := map[string]float64{}
	for _, result := range resDataCount {
		clusterID, _ := result.GetString("cluster_id")
		if clusterID == "" {
			clusterID = defaultClusterID
		}
		dataMins := mins
		if len(result.Values) > 0 {
			dataMins = result.Values[0].Value
		} else {
			klog.V(3).Infof("[Warning] cluster cost data count returned no results for cluster %s", clusterID)
		}
		dataMinsByCluster[clusterID] = dataMins
	}

	// Determine combined discount
	discount, customDiscount := 0.0, 0.0
	c, err := a.CloudProvider.GetConfig()
	if err == nil {
		discount, err = ParsePercentString(c.Discount)
		if err != nil {
			discount = 0.0
		}
		customDiscount, err = ParsePercentString(c.NegotiatedDiscount)
		if err != nil {
			customDiscount = 0.0
		}
	}

	// Intermediate structure storing mapping of [clusterID][type ∈ {cpu, ram, storage, total}]=cost
	costData := make(map[string]map[string]float64)

	// Helper function to iterate over Prom query results, parsing the raw values into
	// the intermediate costData structure.
	setCostsFromResults := func(costData map[string]map[string]float64, results []*prom.QueryResult, name string, discount float64, customDiscount float64) {
		for _, result := range results {
			clusterID, _ := result.GetString("cluster_id")
			if clusterID == "" {
				clusterID = defaultClusterID
			}
			if _, ok := costData[clusterID]; !ok {
				costData[clusterID] = map[string]float64{}
			}
			if len(result.Values) > 0 {
				costData[clusterID][name] += result.Values[0].Value * (1.0 - discount) * (1.0 - customDiscount)
				costData[clusterID]["total"] += result.Values[0].Value * (1.0 - discount) * (1.0 - customDiscount)
			}
		}
	}
	// Apply both sustained use and custom discounts to RAM and CPU
	setCostsFromResults(costData, resTotalCPU, "cpu", discount, customDiscount)
	setCostsFromResults(costData, resTotalRAM, "ram", discount, customDiscount)
	// Apply only custom discount to GPU and storage
	setCostsFromResults(costData, resTotalGPU, "gpu", 0.0, customDiscount)
	setCostsFromResults(costData, resTotalStorage, "storage", 0.0, customDiscount)
	if queryTotalLocalStorage != "" {
		resTotalLocalStorage, err := resChs[5].Await()
		if err != nil {
			return nil, err
		}
		setCostsFromResults(costData, resTotalLocalStorage, "localstorage", 0.0, customDiscount)
	}

	cpuBreakdownMap := map[string]*ClusterCostsBreakdown{}
	ramBreakdownMap := map[string]*ClusterCostsBreakdown{}
	pvUsedCostMap := map[string]float64{}
	if withBreakdown {
		resCPUModePct, _ := resChs[6].Await()
		resRAMSystemPct, _ := resChs[7].Await()
		resRAMUserPct, _ := resChs[8].Await()
		if ctx.HasErrors() {
			return nil, ctx.ErrorCollection()
		}

		for _, result := range resCPUModePct {
			clusterID, _ := result.GetString("cluster_id")
			if clusterID == "" {
				clusterID = defaultClusterID
			}
			if _, ok := cpuBreakdownMap[clusterID]; !ok {
				cpuBreakdownMap[clusterID] = &ClusterCostsBreakdown{}
			}
			cpuBD := cpuBreakdownMap[clusterID]

			mode, err := result.GetString("mode")
			if err != nil {
				klog.V(3).Infof("[Warning] ComputeClusterCosts: unable to read CPU mode: %s", err)
				mode = "other"
			}

			switch mode {
			case "idle":
				cpuBD.Idle += result.Values[0].Value
			case "system":
				cpuBD.System += result.Values[0].Value
			case "user":
				cpuBD.User += result.Values[0].Value
			default:
				cpuBD.Other += result.Values[0].Value
			}
		}

		for _, result := range resRAMSystemPct {
			clusterID, _ := result.GetString("cluster_id")
			if clusterID == "" {
				clusterID = defaultClusterID
			}
			if _, ok := ramBreakdownMap[clusterID]; !ok {
				ramBreakdownMap[clusterID] = &ClusterCostsBreakdown{}
			}
			ramBD := ramBreakdownMap[clusterID]
			ramBD.System += result.Values[0].Value
		}
		for _, result := range resRAMUserPct {
			clusterID, _ := result.GetString("cluster_id")
			if clusterID == "" {
				clusterID = defaultClusterID
			}
			if _, ok := ramBreakdownMap[clusterID]; !ok {
				ramBreakdownMap[clusterID] = &ClusterCostsBreakdown{}
			}
			ramBD := ramBreakdownMap[clusterID]
			ramBD.User += result.Values[0].Value
		}
		for _, ramBD := range ramBreakdownMap {
			remaining := 1.0
			remaining -= ramBD.Other
			remaining -= ramBD.System
			remaining -= ramBD.User
			ramBD.Idle = remaining
		}

		if queryUsedLocalStorage != "" {
			resUsedLocalStorage, err := resChs[9].Await()
			if err != nil {
				return nil, err
			}
			for _, result := range resUsedLocalStorage {
				clusterID, _ := result.GetString("cluster_id")
				if clusterID == "" {
					clusterID = defaultClusterID
				}
				pvUsedCostMap[clusterID] += result.Values[0].Value
			}
		}
	}

	if ctx.HasErrors() {
		for _, err := range ctx.Errors() {
			log.Errorf("ComputeClusterCosts: %s", err)
		}
		return nil, ctx.ErrorCollection()
	}

	// Convert intermediate structure to Costs instances
	costsByCluster := map[string]*ClusterCosts{}
	for id, cd := range costData {
		dataMins, ok := dataMinsByCluster[id]
		if !ok {
			dataMins = mins
			klog.V(3).Infof("[Warning] cluster cost data count not found for cluster %s", id)
		}
		costs, err := NewClusterCostsFromCumulative(cd["cpu"], cd["gpu"], cd["ram"], cd["storage"]+cd["localstorage"], window, offset, dataMins/util.MinsPerHour)
		if err != nil {
			klog.V(3).Infof("[Warning] Failed to parse cluster costs on %s (%s) from cumulative data: %+v", window, offset, cd)
			return nil, err
		}

		if cpuBD, ok := cpuBreakdownMap[id]; ok {
			costs.CPUBreakdown = cpuBD
		}
		if ramBD, ok := ramBreakdownMap[id]; ok {
			costs.RAMBreakdown = ramBD
		}
		costs.StorageBreakdown = &ClusterCostsBreakdown{}
		if pvUC, ok := pvUsedCostMap[id]; ok {
			costs.StorageBreakdown.Idle = (costs.StorageCumulative - pvUC) / costs.StorageCumulative
			costs.StorageBreakdown.User = pvUC / costs.StorageCumulative
		}
		costs.DataMinutes = dataMins
		costsByCluster[id] = costs
	}

	return costsByCluster, nil
}

type Totals struct {
	TotalCost   [][]string `json:"totalcost"`
	CPUCost     [][]string `json:"cpucost"`
	MemCost     [][]string `json:"memcost"`
	StorageCost [][]string `json:"storageCost"`
}

func resultToTotals(qrs []*prom.QueryResult) ([][]string, error) {
	if len(qrs) == 0 {
		return [][]string{}, fmt.Errorf("Not enough data available in the selected time range")
	}

	result := qrs[0]
	totals := [][]string{}
	for _, value := range result.Values {
		d0 := fmt.Sprintf("%f", value.Timestamp)
		d1 := fmt.Sprintf("%f", value.Value)
		toAppend := []string{
			d0,
			d1,
		}
		totals = append(totals, toAppend)
	}
	return totals, nil
}

// ClusterCostsOverTime gives the full cluster costs over time
func ClusterCostsOverTime(cli prometheus.Client, provider cloud.Provider, startString, endString, windowString, offset string) (*Totals, error) {
	localStorageQuery := provider.GetLocalStorageQuery(windowString, offset, true, false)
	if localStorageQuery != "" {
		localStorageQuery = fmt.Sprintf("+ %s", localStorageQuery)
	}

	layout := "2006-01-02T15:04:05.000Z"

	start, err := time.Parse(layout, startString)
	if err != nil {
		klog.V(1).Infof("Error parsing time " + startString + ". Error: " + err.Error())
		return nil, err
	}
	end, err := time.Parse(layout, endString)
	if err != nil {
		klog.V(1).Infof("Error parsing time " + endString + ". Error: " + err.Error())
		return nil, err
	}
	window, err := time.ParseDuration(windowString)
	if err != nil {
		klog.V(1).Infof("Error parsing time " + windowString + ". Error: " + err.Error())
		return nil, err
	}

	// turn offsets of the format "[0-9+]h" into the format "offset [0-9+]h" for use in query templatess
	if offset != "" {
		offset = fmt.Sprintf("offset %s", offset)
	}

	qCores := fmt.Sprintf(queryClusterCores, windowString, offset, windowString, offset, windowString, offset)
	qRAM := fmt.Sprintf(queryClusterRAM, windowString, offset, windowString, offset)
	qStorage := fmt.Sprintf(queryStorage, windowString, offset, windowString, offset, localStorageQuery)
	qTotal := fmt.Sprintf(queryTotal, localStorageQuery)

	ctx := prom.NewContext(cli)
	resChClusterCores := ctx.QueryRange(qCores, start, end, window)
	resChClusterRAM := ctx.QueryRange(qRAM, start, end, window)
	resChStorage := ctx.QueryRange(qStorage, start, end, window)
	resChTotal := ctx.QueryRange(qTotal, start, end, window)

	resultClusterCores, err := resChClusterCores.Await()
	if err != nil {
		return nil, err
	}

	resultClusterRAM, err := resChClusterRAM.Await()
	if err != nil {
		return nil, err
	}

	resultStorage, err := resChStorage.Await()
	if err != nil {
		return nil, err
	}

	resultTotal, err := resChTotal.Await()
	if err != nil {
		return nil, err
	}

	coreTotal, err := resultToTotals(resultClusterCores)
	if err != nil {
		klog.Infof("[Warning] ClusterCostsOverTime: no cpu data: %s", err)
		return nil, err
	}

	ramTotal, err := resultToTotals(resultClusterRAM)
	if err != nil {
		klog.Infof("[Warning] ClusterCostsOverTime: no ram data: %s", err)
		return nil, err
	}

	storageTotal, err := resultToTotals(resultStorage)
	if err != nil {
		klog.Infof("[Warning] ClusterCostsOverTime: no storage data: %s", err)
	}

	clusterTotal, err := resultToTotals(resultTotal)
	if err != nil {
		// If clusterTotal query failed, it's likely because there are no PVs, which
		// causes the qTotal query to return no data. Instead, query only node costs.
		// If that fails, return an error because something is actually wrong.
		qNodes := fmt.Sprintf(queryNodes, localStorageQuery)

		resultNodes, warnings, err := ctx.QueryRangeSync(qNodes, start, end, window)
		for _, warning := range warnings {
			log.Warningf(warning)
		}
		if err != nil {
			return nil, err
		}

		clusterTotal, err = resultToTotals(resultNodes)
		if err != nil {
			klog.Infof("[Warning] ClusterCostsOverTime: no node data: %s", err)
			return nil, err
		}
	}

	return &Totals{
		TotalCost:   clusterTotal,
		CPUCost:     coreTotal,
		MemCost:     ramTotal,
		StorageCost: storageTotal,
	}, nil
}
