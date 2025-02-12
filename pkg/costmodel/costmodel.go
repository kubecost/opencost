package costmodel

import (
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/clusters"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/util"
	"github.com/opencost/opencost/core/pkg/util/promutil"
	costAnalyzerCloud "github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/clustercache"
	"github.com/opencost/opencost/pkg/env"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	"golang.org/x/sync/singleflight"
)

const (
	profileThreshold = 1000 * 1000 * 1000 // 1s (in ns)

	unmountedPVsContainer = "unmounted-pvs"
)

// isCron matches a CronJob name and captures the non-timestamp name
//
// We support either a 10 character timestamp OR an 8 character timestamp
// because batch/v1beta1 CronJobs creates Jobs with 10 character timestamps
// and batch/v1 CronJobs create Jobs with 8 character timestamps.
var isCron = regexp.MustCompile(`^(.+)-(\d{10}|\d{8})$`)

type CostModel struct {
	Cache           clustercache.ClusterCache
	ClusterMap      clusters.ClusterMap
	BatchDuration   time.Duration
	RequestGroup    *singleflight.Group
	DataSource      source.OpenCostDataSource
	Provider        costAnalyzerCloud.Provider
	pricingMetadata *costAnalyzerCloud.PricingMatchMetadata
}

func NewCostModel(
	dataSource source.OpenCostDataSource,
	provider costAnalyzerCloud.Provider,
	cache clustercache.ClusterCache,
	clusterMap clusters.ClusterMap,
	batchDuration time.Duration,
) *CostModel {
	// request grouping to prevent over-requesting the same data prior to caching
	requestGroup := new(singleflight.Group)

	return &CostModel{
		Cache:         cache,
		ClusterMap:    clusterMap,
		BatchDuration: batchDuration,
		DataSource:    dataSource,
		Provider:      provider,
		RequestGroup:  requestGroup,
	}
}

type CostData struct {
	Name            string                       `json:"name,omitempty"`
	PodName         string                       `json:"podName,omitempty"`
	NodeName        string                       `json:"nodeName,omitempty"`
	NodeData        *costAnalyzerCloud.Node      `json:"node,omitempty"`
	Namespace       string                       `json:"namespace,omitempty"`
	Deployments     []string                     `json:"deployments,omitempty"`
	Services        []string                     `json:"services,omitempty"`
	Daemonsets      []string                     `json:"daemonsets,omitempty"`
	Statefulsets    []string                     `json:"statefulsets,omitempty"`
	Jobs            []string                     `json:"jobs,omitempty"`
	RAMReq          []*util.Vector               `json:"ramreq,omitempty"`
	RAMUsed         []*util.Vector               `json:"ramused,omitempty"`
	RAMAllocation   []*util.Vector               `json:"ramallocated,omitempty"`
	CPUReq          []*util.Vector               `json:"cpureq,omitempty"`
	CPUUsed         []*util.Vector               `json:"cpuused,omitempty"`
	CPUAllocation   []*util.Vector               `json:"cpuallocated,omitempty"`
	GPUReq          []*util.Vector               `json:"gpureq,omitempty"`
	PVCData         []*PersistentVolumeClaimData `json:"pvcData,omitempty"`
	NetworkData     []*util.Vector               `json:"network,omitempty"`
	Annotations     map[string]string            `json:"annotations,omitempty"`
	Labels          map[string]string            `json:"labels,omitempty"`
	NamespaceLabels map[string]string            `json:"namespaceLabels,omitempty"`
	ClusterID       string                       `json:"clusterId"`
	ClusterName     string                       `json:"clusterName"`
}

func (cd *CostData) String() string {
	return fmt.Sprintf("\n\tName: %s; PodName: %s, NodeName: %s\n\tNamespace: %s\n\tDeployments: %s\n\tServices: %s\n\tCPU (req, used, alloc): %d, %d, %d\n\tRAM (req, used, alloc): %d, %d, %d",
		cd.Name, cd.PodName, cd.NodeName, cd.Namespace, strings.Join(cd.Deployments, ", "), strings.Join(cd.Services, ", "),
		len(cd.CPUReq), len(cd.CPUUsed), len(cd.CPUAllocation),
		len(cd.RAMReq), len(cd.RAMUsed), len(cd.RAMAllocation))
}

func (cd *CostData) GetController() (name string, kind string, hasController bool) {
	hasController = false

	if len(cd.Deployments) > 0 {
		name = cd.Deployments[0]
		kind = "deployment"
		hasController = true
	} else if len(cd.Statefulsets) > 0 {
		name = cd.Statefulsets[0]
		kind = "statefulset"
		hasController = true
	} else if len(cd.Daemonsets) > 0 {
		name = cd.Daemonsets[0]
		kind = "daemonset"
		hasController = true
	} else if len(cd.Jobs) > 0 {
		name = cd.Jobs[0]
		kind = "job"
		hasController = true

		match := isCron.FindStringSubmatch(name)
		if match != nil {
			name = match[1]
		}
	}

	return name, kind, hasController
}

func (cm *CostModel) ComputeCostData(start, end time.Time) (map[string]*CostData, error) {
	// Cluster ID is specific to the source cluster
	clusterID := env.GetClusterID()
	cp := cm.Provider
	ds := cm.DataSource

	grp := source.NewQueryGroup()

	resChRAMUsage := grp.With(ds.QueryRAMUsageAvg(start, end))
	resChCPUUsage := grp.With(ds.QueryCPUUsageAvg(start, end))
	resChNetZoneRequests := grp.With(ds.QueryNetZoneGiB(start, end))
	resChNetRegionRequests := grp.With(ds.QueryNetRegionGiB(start, end))
	resChNetInternetRequests := grp.With(ds.QueryNetInternetGiB(start, end))

	// Pull pod information from k8s API
	podlist := cm.Cache.GetAllPods()

	podDeploymentsMapping, err := getPodDeployments(cm.Cache, podlist, clusterID)
	if err != nil {
		return nil, err
	}

	podServicesMapping, err := getPodServices(cm.Cache, podlist, clusterID)
	if err != nil {
		return nil, err
	}

	namespaceLabelsMapping, err := getNamespaceLabels(cm.Cache, clusterID)
	if err != nil {
		return nil, err
	}

	namespaceAnnotationsMapping, err := getNamespaceAnnotations(cm.Cache, clusterID)
	if err != nil {
		return nil, err
	}

	// Process Prometheus query results. Handle errors using ctx.Errors.
	resRAMUsage, _ := resChRAMUsage.Await()
	resCPUUsage, _ := resChCPUUsage.Await()
	resNetZoneRequests, _ := resChNetZoneRequests.Await()
	resNetRegionRequests, _ := resChNetRegionRequests.Await()
	resNetInternetRequests, _ := resChNetInternetRequests.Await()

	// NOTE: The way we currently handle errors and warnings only early returns if there is an error. Warnings
	// NOTE: will not propagate unless coupled with errors.
	if grp.HasErrors() {
		// To keep the context of where the errors are occurring, we log the errors here and pass them the error
		// back to the caller. The caller should handle the specific case where error is an ErrorCollection
		for _, queryErr := range grp.Errors() {
			if queryErr.Error != nil {
				log.Errorf("ComputeCostData: Request Error: %s", queryErr.Error)
			}
			if queryErr.ParseError != nil {
				log.Errorf("ComputeCostData: Parsing Error: %s", queryErr.ParseError)
			}
		}

		// ErrorCollection is an collection of errors wrapped in a single error implementation
		// We opt to not return an error for the sake of running as a pure exporter.
		log.Warnf("ComputeCostData: continuing despite prometheus errors: %s", grp.Error())
	}

	defer measureTime(time.Now(), profileThreshold, "ComputeCostData: Processing Query Data")

	nodes, err := cm.GetNodeCost(cp)
	if err != nil {
		log.Warnf("GetNodeCost: no node cost model available: %s", err)
		return nil, err
	}

	// Unmounted PVs represent the PVs that are not mounted or tied to a volume on a container
	unmountedPVs := make(map[string][]*PersistentVolumeClaimData)
	pvClaimMapping, err := GetPVInfoLocal(cm.Cache, clusterID)
	if err != nil {
		log.Warnf("GetPVInfo: unable to get PV data: %s", err.Error())
	}
	if pvClaimMapping != nil {
		err = addPVData(cm.Cache, pvClaimMapping, cp)
		if err != nil {
			return nil, err
		}
		// copy claim mappings into zombies, then remove as they're discovered
		for k, v := range pvClaimMapping {
			unmountedPVs[k] = []*PersistentVolumeClaimData{v}
		}
	}

	networkUsageMap, err := GetNetworkUsageData(resNetZoneRequests, resNetRegionRequests, resNetInternetRequests, clusterID)
	if err != nil {
		log.Warnf("Unable to get Network Cost Data: %s", err.Error())
		networkUsageMap = make(map[string]*NetworkUsageData)
	}

	containerNameCost := make(map[string]*CostData)
	containers := make(map[string]bool)

	RAMUsedMap, err := GetContainerMetricVector(resRAMUsage, clusterID)
	if err != nil {
		return nil, err
	}
	for key := range RAMUsedMap {
		containers[key] = true
	}
	CPUUsedMap, err := GetContainerMetricVector(resCPUUsage, clusterID) // No need to normalize here, as this comes from a counter
	if err != nil {
		return nil, err
	}
	for key := range CPUUsedMap {
		containers[key] = true
	}
	currentContainers := make(map[string]clustercache.Pod)
	for _, pod := range podlist {
		if pod.Status.Phase != v1.PodRunning {
			continue
		}
		cs, err := NewContainerMetricsFromPod(pod, clusterID)
		if err != nil {
			return nil, err
		}
		for _, c := range cs {
			containers[c.Key()] = true // captures any containers that existed for a time < a prometheus scrape interval. We currently charge 0 for this but should charge something.
			currentContainers[c.Key()] = *pod
		}
	}
	missingNodes := make(map[string]*costAnalyzerCloud.Node)
	missingContainers := make(map[string]*CostData)
	for key := range containers {
		if _, ok := containerNameCost[key]; ok {
			continue // because ordering is important for the allocation model (all PV's applied to the first), just dedupe if it's already been added.
		}
		// The _else_ case for this statement is the case in which the container has been
		// deleted so we have usage information but not request information. In that case,
		// we return partial data for CPU and RAM: only usage and not requests.
		if pod, ok := currentContainers[key]; ok {
			podName := pod.Name
			ns := pod.Namespace

			nsLabels := namespaceLabelsMapping[ns+","+clusterID]
			podLabels := pod.Labels
			if podLabels == nil {
				podLabels = make(map[string]string)
			}

			for k, v := range nsLabels {
				if _, ok := podLabels[k]; !ok {
					podLabels[k] = v
				}
			}

			nsAnnotations := namespaceAnnotationsMapping[ns+","+clusterID]
			podAnnotations := pod.Annotations
			if podAnnotations == nil {
				podAnnotations = make(map[string]string)
			}

			for k, v := range nsAnnotations {
				if _, ok := podAnnotations[k]; !ok {
					podAnnotations[k] = v
				}
			}

			nodeName := pod.Spec.NodeName
			var nodeData *costAnalyzerCloud.Node
			if _, ok := nodes[nodeName]; ok {
				nodeData = nodes[nodeName]
			}

			nsKey := ns + "," + clusterID

			var podDeployments []string
			if _, ok := podDeploymentsMapping[nsKey]; ok {
				if ds, ok := podDeploymentsMapping[nsKey][pod.Name]; ok {
					podDeployments = ds
				} else {
					podDeployments = []string{}
				}
			}

			var podPVs []*PersistentVolumeClaimData
			podClaims := pod.Spec.Volumes
			for _, vol := range podClaims {
				if vol.PersistentVolumeClaim != nil {
					name := vol.PersistentVolumeClaim.ClaimName
					key := ns + "," + name + "," + clusterID
					if pvClaim, ok := pvClaimMapping[key]; ok {
						pvClaim.TimesClaimed++
						podPVs = append(podPVs, pvClaim)

						// Remove entry from potential unmounted pvs
						delete(unmountedPVs, key)
					}
				}
			}

			var podNetCosts []*util.Vector
			if usage, ok := networkUsageMap[ns+","+podName+","+clusterID]; ok {
				netCosts, err := GetNetworkCost(usage, cp)
				if err != nil {
					log.Debugf("Error pulling network costs: %s", err.Error())
				} else {
					podNetCosts = netCosts
				}
			}

			var podServices []string
			if _, ok := podServicesMapping[nsKey]; ok {
				if svcs, ok := podServicesMapping[nsKey][pod.Name]; ok {
					podServices = svcs
				} else {
					podServices = []string{}
				}
			}

			for i, container := range pod.Spec.Containers {
				containerName := container.Name

				// recreate the key and look up data for this container
				newKey := NewContainerMetricFromValues(ns, podName, containerName, pod.Spec.NodeName, clusterID).Key()

				// k8s.io/apimachinery/pkg/api/resource/amount.go and
				// k8s.io/apimachinery/pkg/api/resource/quantity.go for
				// details on the "amount" API. See
				// https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#resource-types
				// for the units of memory and CPU.
				ramRequestBytes := container.Resources.Requests.Memory().Value()

				// Because information on container RAM & CPU requests isn't
				// coming from Prometheus, it won't have a timestamp associated
				// with it. We need to provide a timestamp.
				RAMReqV := []*util.Vector{
					{
						Value:     float64(ramRequestBytes),
						Timestamp: float64(time.Now().UTC().Unix()),
					},
				}

				// use millicores so we can convert to cores in a float64 format
				cpuRequestMilliCores := container.Resources.Requests.Cpu().MilliValue()
				CPUReqV := []*util.Vector{
					{
						Value:     float64(cpuRequestMilliCores) / 1000,
						Timestamp: float64(time.Now().UTC().Unix()),
					},
				}

				gpuReqCount := 0.0
				if g, ok := container.Resources.Requests["nvidia.com/gpu"]; ok {
					gpuReqCount = g.AsApproximateFloat64()
				} else if g, ok := container.Resources.Limits["nvidia.com/gpu"]; ok {
					gpuReqCount = g.AsApproximateFloat64()
				} else if g, ok := container.Resources.Requests["k8s.amazonaws.com/vgpu"]; ok {
					gpuReqCount = g.AsApproximateFloat64()
				} else if g, ok := container.Resources.Limits["k8s.amazonaws.com/vgpu"]; ok {
					gpuReqCount = g.AsApproximateFloat64()
				}
				GPUReqV := []*util.Vector{
					{
						Value:     float64(gpuReqCount),
						Timestamp: float64(time.Now().UTC().Unix()),
					},
				}

				RAMUsedV, ok := RAMUsedMap[newKey]
				if !ok {
					log.Debug("no RAM usage for " + newKey)
					RAMUsedV = []*util.Vector{{}}
				}

				CPUUsedV, ok := CPUUsedMap[newKey]
				if !ok {
					log.Debug("no CPU usage for " + newKey)
					CPUUsedV = []*util.Vector{{}}
				}

				var pvReq []*PersistentVolumeClaimData
				var netReq []*util.Vector
				if i == 0 { // avoid duplicating by just assigning all claims to the first container.
					pvReq = podPVs
					netReq = podNetCosts
				}

				costs := &CostData{
					Name:            containerName,
					PodName:         podName,
					NodeName:        nodeName,
					Namespace:       ns,
					Deployments:     podDeployments,
					Services:        podServices,
					Daemonsets:      getDaemonsetsOfPod(pod),
					Jobs:            getJobsOfPod(pod),
					Statefulsets:    getStatefulSetsOfPod(pod),
					NodeData:        nodeData,
					RAMReq:          RAMReqV,
					RAMUsed:         RAMUsedV,
					CPUReq:          CPUReqV,
					CPUUsed:         CPUUsedV,
					GPUReq:          GPUReqV,
					PVCData:         pvReq,
					NetworkData:     netReq,
					Annotations:     podAnnotations,
					Labels:          podLabels,
					NamespaceLabels: nsLabels,
					ClusterID:       clusterID,
					ClusterName:     cm.ClusterMap.NameFor(clusterID),
				}

				var cpuReq, cpuUse *util.Vector
				if len(costs.CPUReq) > 0 {
					cpuReq = costs.CPUReq[0]
				}
				if len(costs.CPUUsed) > 0 {
					cpuUse = costs.CPUUsed[0]
				}
				costs.CPUAllocation = getContainerAllocation(cpuReq, cpuUse, "CPU")

				var ramReq, ramUse *util.Vector
				if len(costs.RAMReq) > 0 {
					ramReq = costs.RAMReq[0]
				}
				if len(costs.RAMUsed) > 0 {
					ramUse = costs.RAMUsed[0]
				}
				costs.RAMAllocation = getContainerAllocation(ramReq, ramUse, "RAM")

				containerNameCost[newKey] = costs
			}
		} else {
			// The container has been deleted. Not all information is sent to prometheus via ksm, so fill out what we can without k8s api
			log.Debug("The container " + key + " has been deleted. Calculating allocation but resulting object will be missing data.")
			c, err := NewContainerMetricFromKey(key)
			if err != nil {
				return nil, err
			}

			// CPU and RAM requests are obtained from the Kubernetes API.
			// If this case has been reached, the Kubernetes API will not
			// have information about the pod because it no longer exists.
			//
			// The case where this matters is minimal, mainly in environments
			// with very short-lived pods that over-request resources.
			RAMReqV := []*util.Vector{{}}
			CPUReqV := []*util.Vector{{}}
			GPUReqV := []*util.Vector{{}}

			RAMUsedV, ok := RAMUsedMap[key]
			if !ok {
				log.Debug("no RAM usage for " + key)
				RAMUsedV = []*util.Vector{{}}
			}

			CPUUsedV, ok := CPUUsedMap[key]
			if !ok {
				log.Debug("no CPU usage for " + key)
				CPUUsedV = []*util.Vector{{}}
			}

			node, ok := nodes[c.NodeName]
			if !ok {
				log.Debugf("Node \"%s\" has been deleted from Kubernetes. Query historical data to get it.", c.NodeName)
				if n, ok := missingNodes[c.NodeName]; ok {
					node = n
				} else {
					node = &costAnalyzerCloud.Node{}
					missingNodes[c.NodeName] = node
				}
			}
			namespacelabels := namespaceLabelsMapping[c.Namespace+","+c.ClusterID]

			namespaceAnnotations := namespaceAnnotationsMapping[c.Namespace+","+c.ClusterID]

			costs := &CostData{
				Name:            c.ContainerName,
				PodName:         c.PodName,
				NodeName:        c.NodeName,
				NodeData:        node,
				Namespace:       c.Namespace,
				RAMReq:          RAMReqV,
				RAMUsed:         RAMUsedV,
				CPUReq:          CPUReqV,
				CPUUsed:         CPUUsedV,
				GPUReq:          GPUReqV,
				Annotations:     namespaceAnnotations,
				NamespaceLabels: namespacelabels,
				ClusterID:       c.ClusterID,
				ClusterName:     cm.ClusterMap.NameFor(c.ClusterID),
			}

			var cpuReq, cpuUse *util.Vector
			if len(costs.CPUReq) > 0 {
				cpuReq = costs.CPUReq[0]
			}
			if len(costs.CPUUsed) > 0 {
				cpuUse = costs.CPUUsed[0]
			}
			costs.CPUAllocation = getContainerAllocation(cpuReq, cpuUse, "CPU")

			var ramReq, ramUse *util.Vector
			if len(costs.RAMReq) > 0 {
				ramReq = costs.RAMReq[0]
			}
			if len(costs.RAMUsed) > 0 {
				ramUse = costs.RAMUsed[0]
			}
			costs.RAMAllocation = getContainerAllocation(ramReq, ramUse, "RAM")

			containerNameCost[key] = costs
			missingContainers[key] = costs
		}
	}
	// Use unmounted pvs to create a mapping of "Unmounted-<Namespace>" containers
	// to pass along the cost data
	unmounted := findUnmountedPVCostData(cm.ClusterMap, unmountedPVs, namespaceLabelsMapping, namespaceAnnotationsMapping)
	for k, costs := range unmounted {
		log.Debugf("Unmounted PVs in Namespace/ClusterID: %s/%s", costs.Namespace, costs.ClusterID)

		containerNameCost[k] = costs
	}

	err = findDeletedNodeInfo(cm.DataSource, missingNodes, start, end)
	if err != nil {
		log.Errorf("Error fetching historical node data: %s", err.Error())
	}

	err = findDeletedPodInfo(cm.DataSource, missingContainers, start, end)
	if err != nil {
		log.Errorf("Error fetching historical pod data: %s", err.Error())
	}
	return containerNameCost, err
}

func findUnmountedPVCostData(clusterMap clusters.ClusterMap, unmountedPVs map[string][]*PersistentVolumeClaimData, namespaceLabelsMapping map[string]map[string]string, namespaceAnnotationsMapping map[string]map[string]string) map[string]*CostData {
	costs := make(map[string]*CostData)
	if len(unmountedPVs) == 0 {
		return costs
	}

	for k, pv := range unmountedPVs {
		keyParts := strings.Split(k, ",")
		if len(keyParts) != 3 {
			log.Warnf("Unmounted PV used key with incorrect parts: %s", k)
			continue
		}

		ns, _, clusterID := keyParts[0], keyParts[1], keyParts[2]

		namespacelabels := namespaceLabelsMapping[ns+","+clusterID]

		namespaceAnnotations := namespaceAnnotationsMapping[ns+","+clusterID]

		metric := NewContainerMetricFromValues(ns, unmountedPVsContainer, unmountedPVsContainer, "", clusterID)
		key := metric.Key()

		if costData, ok := costs[key]; !ok {
			costs[key] = &CostData{
				Name:            unmountedPVsContainer,
				PodName:         unmountedPVsContainer,
				NodeName:        "",
				Annotations:     namespaceAnnotations,
				Namespace:       ns,
				NamespaceLabels: namespacelabels,
				Labels:          namespacelabels,
				ClusterID:       clusterID,
				ClusterName:     clusterMap.NameFor(clusterID),
				PVCData:         pv,
			}
		} else {
			costData.PVCData = append(costData.PVCData, pv...)
		}
	}

	return costs
}

func findDeletedPodInfo(dataSource source.OpenCostDataSource, missingContainers map[string]*CostData, start, end time.Time) error {
	if len(missingContainers) > 0 {

		podLabelsResCh := dataSource.QueryPodLabels(start, end)
		podLabelsResult, err := podLabelsResCh.Await()
		if err != nil {
			log.Errorf("failed to parse historical pod labels: %s", err.Error())
		}

		podLabels := make(map[string]map[string]string)
		if podLabelsResult != nil {
			podLabels, err = parsePodLabels(podLabelsResult)
			if err != nil {
				log.Errorf("failed to parse historical pod labels: %s", err.Error())
			}
		}
		for key, costData := range missingContainers {
			cm, _ := NewContainerMetricFromKey(key)
			labels, ok := podLabels[cm.PodName]
			if !ok {
				labels = make(map[string]string)
			}
			for k, v := range costData.NamespaceLabels {
				labels[k] = v
			}
			costData.Labels = labels
		}
	}

	return nil
}

func findDeletedNodeInfo(dataSource source.OpenCostDataSource, missingNodes map[string]*costAnalyzerCloud.Node, start, end time.Time) error {
	if len(missingNodes) > 0 {
		defer measureTime(time.Now(), profileThreshold, "Finding Deleted Node Info")

		grp := source.NewQueryGroup()

		cpuCostResCh := grp.With(dataSource.QueryNodeCostPerCPUHr(start, end))
		ramCostResCh := grp.With(dataSource.QueryNodeCostPerRAMGiBHr(start, end))
		gpuCostResCh := grp.With(dataSource.QueryNodeCostPerGPUHr(start, end))

		cpuCostRes, _ := cpuCostResCh.Await()
		ramCostRes, _ := ramCostResCh.Await()
		gpuCostRes, _ := gpuCostResCh.Await()

		if grp.HasErrors() {
			return grp.Error()
		}

		cpuCosts, err := getCost(cpuCostRes)
		if err != nil {
			return err
		}
		ramCosts, err := getCost(ramCostRes)
		if err != nil {
			return err
		}
		gpuCosts, err := getCost(gpuCostRes)
		if err != nil {
			return err
		}

		if len(cpuCosts) == 0 {
			log.Infof("Kubecost prometheus metrics not currently available. Ingest this server's /metrics endpoint to get that data.")
		}

		for node, costv := range cpuCosts {
			if _, ok := missingNodes[node]; ok {
				missingNodes[node].VCPUCost = fmt.Sprintf("%f", costv[0].Value)
			} else {
				log.DedupedWarningf(5, "Node `%s` in prometheus but not k8s api", node)
			}
		}
		for node, costv := range ramCosts {
			if _, ok := missingNodes[node]; ok {
				missingNodes[node].RAMCost = fmt.Sprintf("%f", costv[0].Value)
			}
		}
		for node, costv := range gpuCosts {
			if _, ok := missingNodes[node]; ok {
				missingNodes[node].GPUCost = fmt.Sprintf("%f", costv[0].Value)
			}
		}

	}
	return nil
}

// getContainerAllocation takes the max between request and usage. This function
// returns a slice containing a single element describing the container's
// allocation.
//
// Additionally, the timestamp of the allocation will be the highest value
// timestamp between the two vectors. This mitigates situations where
// Timestamp=0. This should have no effect on the metrics emitted by the
// CostModelMetricsEmitter
func getContainerAllocation(req *util.Vector, used *util.Vector, allocationType string) []*util.Vector {
	var result []*util.Vector

	if req != nil && used != nil {
		x1 := req.Value
		if math.IsNaN(x1) {
			log.Debugf("NaN value found during %s allocation calculation for requests.", allocationType)
			x1 = 0.0
		}
		y1 := used.Value
		if math.IsNaN(y1) {
			log.Debugf("NaN value found during %s allocation calculation for used.", allocationType)
			y1 = 0.0
		}
		result = []*util.Vector{
			{
				Value:     math.Max(x1, y1),
				Timestamp: math.Max(req.Timestamp, used.Timestamp),
			},
		}
		if result[0].Value == 0 && result[0].Timestamp == 0 {
			log.Debugf("No request or usage data found during %s allocation calculation. Setting allocation to 0.", allocationType)
		}
	} else if req != nil {
		result = []*util.Vector{
			{
				Value:     req.Value,
				Timestamp: req.Timestamp,
			},
		}
	} else if used != nil {
		result = []*util.Vector{
			{
				Value:     used.Value,
				Timestamp: used.Timestamp,
			},
		}
	} else {
		log.Debugf("No request or usage data found during %s allocation calculation. Setting allocation to 0.", allocationType)
		result = []*util.Vector{
			{
				Value:     0,
				Timestamp: float64(time.Now().UTC().Unix()),
			},
		}
	}

	return result
}

func addPVData(cache clustercache.ClusterCache, pvClaimMapping map[string]*PersistentVolumeClaimData, cloud costAnalyzerCloud.Provider) error {
	cfg, err := cloud.GetConfig()
	if err != nil {
		return err
	}
	// Pull a region from the first node
	var defaultRegion string
	nodeList := cache.GetAllNodes()
	if len(nodeList) > 0 {
		defaultRegion, _ = util.GetRegion(nodeList[0].Labels)
	}

	storageClasses := cache.GetAllStorageClasses()
	storageClassMap := make(map[string]map[string]string)
	for _, storageClass := range storageClasses {
		params := storageClass.Parameters
		storageClassMap[storageClass.Name] = params
		if storageClass.Annotations["storageclass.kubernetes.io/is-default-class"] == "true" || storageClass.Annotations["storageclass.beta.kubernetes.io/is-default-class"] == "true" {
			storageClassMap["default"] = params
			storageClassMap[""] = params
		}
	}

	pvs := cache.GetAllPersistentVolumes()
	pvMap := make(map[string]*costAnalyzerCloud.PV)
	for _, pv := range pvs {
		parameters, ok := storageClassMap[pv.Spec.StorageClassName]
		if !ok {
			log.Debugf("Unable to find parameters for storage class \"%s\". Does pv \"%s\" have a storageClassName?", pv.Spec.StorageClassName, pv.Name)
		}
		var region string
		if r, ok := util.GetRegion(pv.Labels); ok {
			region = r
		} else {
			region = defaultRegion
		}

		cacPv := &costAnalyzerCloud.PV{
			Class:      pv.Spec.StorageClassName,
			Region:     region,
			Parameters: parameters,
		}
		err := GetPVCost(cacPv, pv, cloud, region)
		if err != nil {
			return err
		}
		pvMap[pv.Name] = cacPv
	}

	for _, pvc := range pvClaimMapping {
		if vol, ok := pvMap[pvc.VolumeName]; ok {
			pvc.Volume = vol
		} else {
			log.Debugf("PV not found, using default")
			pvc.Volume = &costAnalyzerCloud.PV{
				Cost: cfg.Storage,
			}
		}
	}

	return nil
}

func GetPVCost(pv *costAnalyzerCloud.PV, kpv *clustercache.PersistentVolume, cp costAnalyzerCloud.Provider, defaultRegion string) error {
	cfg, err := cp.GetConfig()
	if err != nil {
		return err
	}
	key := cp.GetPVKey(kpv, pv.Parameters, defaultRegion)
	pv.ProviderID = key.ID()
	pvWithCost, err := cp.PVPricing(key)
	if err != nil {
		pv.Cost = cfg.Storage
		return err
	}
	if pvWithCost == nil || pvWithCost.Cost == "" {
		pv.Cost = cfg.Storage
		return nil // set default cost
	}
	pv.Cost = pvWithCost.Cost
	return nil
}

func (cm *CostModel) GetPricingSourceCounts() (*costAnalyzerCloud.PricingMatchMetadata, error) {
	if cm.pricingMetadata != nil {
		return cm.pricingMetadata, nil
	} else {
		return nil, fmt.Errorf("Node costs not yet calculated")
	}
}

func (cm *CostModel) GetNodeCost(cp costAnalyzerCloud.Provider) (map[string]*costAnalyzerCloud.Node, error) {
	cfg, err := cp.GetConfig()
	if err != nil {
		return nil, err
	}

	nodeList := cm.Cache.GetAllNodes()
	nodes := make(map[string]*costAnalyzerCloud.Node)

	pmd := &costAnalyzerCloud.PricingMatchMetadata{
		TotalNodes:        0,
		PricingTypeCounts: make(map[costAnalyzerCloud.PricingType]int),
	}
	for _, n := range nodeList {
		name := n.Name
		nodeLabels := n.Labels
		nodeLabels["providerID"] = n.SpecProviderID

		pmd.TotalNodes++

		cnode, _, err := cp.NodePricing(cp.GetKey(nodeLabels, n))
		if err != nil {
			log.Infof("Error getting node pricing. Error: %s", err.Error())
			if cnode != nil {
				nodes[name] = cnode
				continue
			} else {
				cnode = &costAnalyzerCloud.Node{
					VCPUCost: cfg.CPU,
					RAMCost:  cfg.RAM,
				}
			}
		}

		if _, ok := pmd.PricingTypeCounts[cnode.PricingType]; ok {
			pmd.PricingTypeCounts[cnode.PricingType]++
		} else {
			pmd.PricingTypeCounts[cnode.PricingType] = 1
		}

		// newCnode builds upon cnode but populates/overrides certain fields.
		// cnode was populated leveraging cloud provider public pricing APIs.
		newCnode := *cnode
		if newCnode.InstanceType == "" {
			it, _ := util.GetInstanceType(n.Labels)
			newCnode.InstanceType = it
		}
		if newCnode.Region == "" {
			region, _ := util.GetRegion(n.Labels)
			newCnode.Region = region
		}
		if newCnode.ArchType == "" {
			arch, _ := util.GetArchType(n.Labels)
			newCnode.ArchType = arch
		}
		newCnode.ProviderID = n.SpecProviderID

		var cpu float64
		if newCnode.VCPU == "" {
			cpu = float64(n.Status.Capacity.Cpu().Value())
			newCnode.VCPU = n.Status.Capacity.Cpu().String()
		} else {
			cpu, err = strconv.ParseFloat(newCnode.VCPU, 64)
			if err != nil {
				log.Warnf("parsing VCPU value: \"%s\" as float64", newCnode.VCPU)
			}
		}
		if math.IsNaN(cpu) {
			log.Warnf("cpu parsed as NaN. Setting to 0.")
			cpu = 0
		}

		var ram float64
		if newCnode.RAM == "" {
			newCnode.RAM = n.Status.Capacity.Memory().String()
		}
		ram = float64(n.Status.Capacity.Memory().Value())
		if math.IsNaN(ram) {
			log.Warnf("ram parsed as NaN. Setting to 0.")
			ram = 0
		}

		newCnode.RAMBytes = fmt.Sprintf("%f", ram)

		gpuc, err := strconv.ParseFloat(newCnode.GPU, 64)
		if err != nil {
			gpuc = 0.0
		}

		// The k8s API will often report more accurate results for GPU count
		// than cloud provider public pricing APIs. If found, override the
		// original value.
		gpuOverride, vgpuOverride, err := getGPUCount(cm.Cache, n)
		if err != nil {
			log.Warnf("Unable to get GPUCount for node %s: %s", n.Name, err.Error())
		}
		if gpuOverride > 0 {
			newCnode.GPU = fmt.Sprintf("%f", gpuOverride)
			gpuc = gpuOverride
		}
		if vgpuOverride > 0 {
			newCnode.VGPU = fmt.Sprintf("%f", vgpuOverride)
		}

		// Special case for SUSE rancher, since it won't behave with normal
		// calculations, courtesy of the instance type not being "real" (a
		// recognizable AWS instance type.)
		if newCnode.InstanceType == "rke2" {
			log.Infof(
				"Found a SUSE Rancher node %s, defaulting and skipping math",
				cp.GetKey(nodeLabels, n).Features(),
			)

			defaultCPUCorePrice, err := strconv.ParseFloat(cfg.CPU, 64)
			if err != nil {
				log.Errorf("Could not parse default cpu price")
				defaultCPUCorePrice = 0
			}
			if math.IsNaN(defaultCPUCorePrice) {
				log.Warnf("defaultCPU parsed as NaN. Setting to 0.")
				defaultCPUCorePrice = 0
			}

			defaultRAMPrice, err := strconv.ParseFloat(cfg.RAM, 64)
			if err != nil {
				log.Errorf("Could not parse default ram price")
				defaultRAMPrice = 0
			}
			if math.IsNaN(defaultRAMPrice) {
				log.Warnf("defaultRAM parsed as NaN. Setting to 0.")
				defaultRAMPrice = 0
			}

			defaultGPUPrice, err := strconv.ParseFloat(cfg.GPU, 64)
			if err != nil {
				log.Errorf("Could not parse default gpu price")
				defaultGPUPrice = 0
			}
			if math.IsNaN(defaultGPUPrice) {
				log.Warnf("defaultGPU parsed as NaN. Setting to 0.")
				defaultGPUPrice = 0
			}
			// Just say no to doing the ratios!
			cpuCost := defaultCPUCorePrice * cpu
			gpuCost := defaultGPUPrice * gpuc
			ramCost := defaultRAMPrice * ram
			nodeCost := cpuCost + gpuCost + ramCost

			newCnode.Cost = fmt.Sprintf("%f", nodeCost)
			newCnode.VCPUCost = fmt.Sprintf("%f", defaultCPUCorePrice)
			newCnode.GPUCost = fmt.Sprintf("%f", defaultGPUPrice)
			newCnode.RAMCost = fmt.Sprintf("%f", defaultRAMPrice)
			newCnode.RAMBytes = fmt.Sprintf("%f", ram)

		} else if newCnode.GPU != "" && newCnode.GPUCost == "" {
			// was the big thing to investigate. All the funky ratio math
			// we were doing was messing with their default pricing. for SUSE Rancher.

			// We reach this when a GPU is detected on a node, but no cost for
			// the GPU is defined in the OnDemand pricing. Calculate ratios of
			// CPU to RAM and GPU to RAM costs, then distribute the total node
			// cost among the CPU, RAM, and GPU.
			log.Tracef("GPU without cost found for %s, calculating...", cp.GetKey(nodeLabels, n).Features())

			defaultCPU, err := strconv.ParseFloat(cfg.CPU, 64)
			if err != nil {
				log.Errorf("Could not parse default cpu price")
				defaultCPU = 0
			}
			if math.IsNaN(defaultCPU) {
				log.Warnf("defaultCPU parsed as NaN. Setting to 0.")
				defaultCPU = 0
			}

			defaultRAM, err := strconv.ParseFloat(cfg.RAM, 64)
			if err != nil {
				log.Errorf("Could not parse default ram price")
				defaultRAM = 0
			}
			if math.IsNaN(defaultRAM) {
				log.Warnf("defaultRAM parsed as NaN. Setting to 0.")
				defaultRAM = 0
			}

			defaultGPU, err := strconv.ParseFloat(cfg.GPU, 64)
			if err != nil {
				log.Errorf("Could not parse default gpu price")
				defaultGPU = 0
			}
			if math.IsNaN(defaultGPU) {
				log.Warnf("defaultGPU parsed as NaN. Setting to 0.")
				defaultGPU = 0
			}

			cpuToRAMRatio := defaultCPU / defaultRAM
			if math.IsNaN(cpuToRAMRatio) {
				log.Warnf("cpuToRAMRatio[defaultCPU: %f / defaultRAM: %f] is NaN. Setting to 10.", defaultCPU, defaultRAM)
				cpuToRAMRatio = 10
			}

			gpuToRAMRatio := defaultGPU / defaultRAM
			if math.IsNaN(gpuToRAMRatio) {
				log.Warnf("gpuToRAMRatio is NaN. Setting to 100.")
				gpuToRAMRatio = 100
			}

			ramGB := ram / 1024 / 1024 / 1024
			if math.IsNaN(ramGB) {
				log.Warnf("ramGB is NaN. Setting to 0.")
				ramGB = 0
			}

			ramMultiple := gpuc*gpuToRAMRatio + cpu*cpuToRAMRatio + ramGB
			if math.IsNaN(ramMultiple) {
				log.Warnf("ramMultiple is NaN. Setting to 0.")
				ramMultiple = 0
			}

			var nodePrice float64
			if newCnode.Cost != "" {
				nodePrice, err = strconv.ParseFloat(newCnode.Cost, 64)
				if err != nil {
					log.Errorf("Could not parse total node price")
					return nil, err
				}
			} else if newCnode.VCPUCost != "" {
				nodePrice, err = strconv.ParseFloat(newCnode.VCPUCost, 64) // all the price was allocated to the CPU
				if err != nil {
					log.Errorf("Could not parse node vcpu price")
					return nil, err
				}
			} else { // add case to use default pricing model when API data fails.
				log.Debugf("No node price or CPUprice found, falling back to default")
				nodePrice = defaultCPU*cpu + defaultRAM*ram + gpuc*defaultGPU
			}
			if math.IsNaN(nodePrice) {
				log.Warnf("nodePrice parsed as NaN. Setting to 0.")
				nodePrice = 0
			}

			ramPrice := (nodePrice / ramMultiple)
			if math.IsNaN(ramPrice) {
				log.Warnf("ramPrice[nodePrice: %f / ramMultiple: %f] parsed as NaN. Setting to 0.", nodePrice, ramMultiple)
				ramPrice = 0
			}

			cpuPrice := ramPrice * cpuToRAMRatio
			gpuPrice := ramPrice * gpuToRAMRatio

			newCnode.VCPUCost = fmt.Sprintf("%f", cpuPrice)
			newCnode.RAMCost = fmt.Sprintf("%f", ramPrice)
			newCnode.RAMBytes = fmt.Sprintf("%f", ram)
			newCnode.GPUCost = fmt.Sprintf("%f", gpuPrice)
		} else if newCnode.RAMCost == "" {
			// We reach this when no RAM cost is defined in the OnDemand
			// pricing. It calculates a cpuToRAMRatio and ramMultiple to
			// distrubte the total node cost among CPU and RAM costs.
			log.Tracef("No RAM cost found for %s, calculating...", cp.GetKey(nodeLabels, n).Features())

			defaultCPU, err := strconv.ParseFloat(cfg.CPU, 64)
			if err != nil {
				log.Warnf("Could not parse default cpu price")
				defaultCPU = 0
			}
			if math.IsNaN(defaultCPU) {
				log.Warnf("defaultCPU parsed as NaN. Setting to 0.")
				defaultCPU = 0
			}

			defaultRAM, err := strconv.ParseFloat(cfg.RAM, 64)
			if err != nil {
				log.Warnf("Could not parse default ram price")
				defaultRAM = 0
			}
			if math.IsNaN(defaultRAM) {
				log.Warnf("defaultRAM parsed as NaN. Setting to 0.")
				defaultRAM = 0
			}

			cpuToRAMRatio := defaultCPU / defaultRAM
			if math.IsNaN(cpuToRAMRatio) {
				log.Warnf("cpuToRAMRatio[defaultCPU: %f / defaultRAM: %f] is NaN. Setting to 10.", defaultCPU, defaultRAM)
				cpuToRAMRatio = 10
			}

			ramGB := ram / 1024 / 1024 / 1024
			if math.IsNaN(ramGB) {
				log.Warnf("ramGB is NaN. Setting to 0.")
				ramGB = 0
			}

			ramMultiple := cpu*cpuToRAMRatio + ramGB
			if math.IsNaN(ramMultiple) {
				log.Warnf("ramMultiple is NaN. Setting to 0.")
				ramMultiple = 0
			}

			var nodePrice float64
			if newCnode.Cost != "" {
				nodePrice, err = strconv.ParseFloat(newCnode.Cost, 64)
				if err != nil {
					log.Warnf("Could not parse total node price")
					return nil, err
				}
				if newCnode.GPUCost != "" {
					gpuPrice, err := strconv.ParseFloat(newCnode.GPUCost, 64)
					if err != nil {
						log.Warnf("Could not parse node gpu price")
						return nil, err
					}
					nodePrice = nodePrice - gpuPrice // remove the gpuPrice from the total, we're just costing out RAM and CPU.
				}
			} else if newCnode.VCPUCost != "" {
				nodePrice, err = strconv.ParseFloat(newCnode.VCPUCost, 64) // all the price was allocated to the CPU
				if err != nil {
					log.Warnf("Could not parse node vcpu price")
					return nil, err
				}
			} else { // add case to use default pricing model when API data fails.
				log.Debugf("No node price or CPUprice found, falling back to default")
				nodePrice = defaultCPU*cpu + defaultRAM*ramGB
			}
			if math.IsNaN(nodePrice) {
				log.Warnf("nodePrice parsed as NaN. Setting to 0.")
				nodePrice = 0
			}

			ramPrice := (nodePrice / ramMultiple)
			if math.IsNaN(ramPrice) {
				log.Warnf("ramPrice[nodePrice: %f / ramMultiple: %f] parsed as NaN. Setting to 0.", nodePrice, ramMultiple)
				ramPrice = 0
			}

			cpuPrice := ramPrice * cpuToRAMRatio

			if defaultRAM != 0 {
				newCnode.VCPUCost = fmt.Sprintf("%f", cpuPrice)
				newCnode.RAMCost = fmt.Sprintf("%f", ramPrice)
			} else { // just assign the full price to CPU
				if cpu != 0 {
					newCnode.VCPUCost = fmt.Sprintf("%f", nodePrice/cpu)
				} else {
					newCnode.VCPUCost = fmt.Sprintf("%f", nodePrice)
				}
			}
			newCnode.RAMBytes = fmt.Sprintf("%f", ram)

			log.Tracef("Computed \"%s\" RAM Cost := %v", name, newCnode.RAMCost)
		}

		nodes[name] = &newCnode
	}
	cm.pricingMetadata = pmd
	cp.ApplyReservedInstancePricing(nodes)

	return nodes, nil
}

// TODO: drop some logs
func (cm *CostModel) GetLBCost(cp costAnalyzerCloud.Provider) (map[serviceKey]*costAnalyzerCloud.LoadBalancer, error) {
	// for fetching prices from cloud provider
	// cfg, err := cp.GetConfig()
	// if err != nil {
	// 	return nil, err
	// }

	servicesList := cm.Cache.GetAllServices()
	loadBalancerMap := make(map[serviceKey]*costAnalyzerCloud.LoadBalancer)

	for _, service := range servicesList {
		namespace := service.Namespace
		name := service.Name
		key := serviceKey{
			Cluster:   env.GetClusterID(),
			Namespace: namespace,
			Service:   name,
		}

		if service.Type == "LoadBalancer" {
			loadBalancer, err := cp.LoadBalancerPricing()
			if err != nil {
				return nil, err
			}
			newLoadBalancer := *loadBalancer
			for _, loadBalancerIngress := range service.Status.LoadBalancer.Ingress {
				address := loadBalancerIngress.IP
				// Some cloud providers use hostname rather than IP
				if address == "" {
					address = loadBalancerIngress.Hostname
				}
				newLoadBalancer.IngressIPAddresses = append(newLoadBalancer.IngressIPAddresses, address)

			}
			loadBalancerMap[key] = &newLoadBalancer
		}
	}
	return loadBalancerMap, nil
}

func getPodServices(cache clustercache.ClusterCache, podList []*clustercache.Pod, clusterID string) (map[string]map[string][]string, error) {
	servicesList := cache.GetAllServices()
	podServicesMapping := make(map[string]map[string][]string)
	for _, service := range servicesList {
		namespace := service.Namespace
		name := service.Name
		key := namespace + "," + clusterID
		if _, ok := podServicesMapping[key]; !ok {
			podServicesMapping[key] = make(map[string][]string)
		}
		s := labels.Nothing()
		if len(service.SpecSelector) > 0 {
			s = labels.Set(service.SpecSelector).AsSelectorPreValidated()
		}
		for _, pod := range podList {
			labelSet := labels.Set(pod.Labels)
			if s.Matches(labelSet) && pod.Namespace == namespace {
				services, ok := podServicesMapping[key][pod.Name]
				if ok {
					podServicesMapping[key][pod.Name] = append(services, name)
				} else {
					podServicesMapping[key][pod.Name] = []string{name}
				}
			}
		}
	}
	return podServicesMapping, nil
}

func getPodDeployments(cache clustercache.ClusterCache, podList []*clustercache.Pod, clusterID string) (map[string]map[string][]string, error) {
	deploymentsList := cache.GetAllDeployments()
	podDeploymentsMapping := make(map[string]map[string][]string) // namespace: podName: [deploymentNames]
	for _, deployment := range deploymentsList {
		namespace := deployment.Namespace
		name := deployment.Name

		key := namespace + "," + clusterID
		if _, ok := podDeploymentsMapping[key]; !ok {
			podDeploymentsMapping[key] = make(map[string][]string)
		}
		s, err := metav1.LabelSelectorAsSelector(deployment.SpecSelector)
		if err != nil {
			log.Errorf("Error doing deployment label conversion: %s", err)
		}
		for _, pod := range podList {
			labelSet := labels.Set(pod.Labels)
			if s.Matches(labelSet) && pod.Namespace == namespace {
				deployments, ok := podDeploymentsMapping[key][pod.Name]
				if ok {
					podDeploymentsMapping[key][pod.Name] = append(deployments, name)
				} else {
					podDeploymentsMapping[key][pod.Name] = []string{name}
				}
			}
		}
	}
	return podDeploymentsMapping, nil
}

func getNamespaceLabels(cache clustercache.ClusterCache, clusterID string) (map[string]map[string]string, error) {
	nsToLabels := make(map[string]map[string]string)
	nss := cache.GetAllNamespaces()
	for _, ns := range nss {
		labels := make(map[string]string)
		for k, v := range ns.Labels {
			labels[promutil.SanitizeLabelName(k)] = v
		}
		nsToLabels[ns.Name+","+clusterID] = labels
	}
	return nsToLabels, nil
}

func getNamespaceAnnotations(cache clustercache.ClusterCache, clusterID string) (map[string]map[string]string, error) {
	nsToAnnotations := make(map[string]map[string]string)
	nss := cache.GetAllNamespaces()
	for _, ns := range nss {
		annotations := make(map[string]string)
		for k, v := range ns.Annotations {
			annotations[promutil.SanitizeLabelName(k)] = v
		}
		nsToAnnotations[ns.Name+","+clusterID] = annotations
	}
	return nsToAnnotations, nil
}

func getDaemonsetsOfPod(pod clustercache.Pod) []string {
	for _, ownerReference := range pod.OwnerReferences {
		if ownerReference.Kind == "DaemonSet" {
			return []string{ownerReference.Name}
		}
	}
	return []string{}
}

func getJobsOfPod(pod clustercache.Pod) []string {
	for _, ownerReference := range pod.OwnerReferences {
		if ownerReference.Kind == "Job" {
			return []string{ownerReference.Name}
		}
	}
	return []string{}
}

func getStatefulSetsOfPod(pod clustercache.Pod) []string {
	for _, ownerReference := range pod.OwnerReferences {
		if ownerReference.Kind == "StatefulSet" {
			return []string{ownerReference.Name}
		}
	}
	return []string{}
}

// getGPUCount reads the node's Status and Labels (via the k8s API) to identify
// the number of GPUs and vGPUs are equipped on the node. If unable to identify
// a GPU count, it will return -1.
func getGPUCount(cache clustercache.ClusterCache, n *clustercache.Node) (float64, float64, error) {
	g, hasGpu := n.Status.Capacity["nvidia.com/gpu"]
	_, hasReplicas := n.Labels["nvidia.com/gpu.replicas"]

	// Case 1: Standard NVIDIA GPU
	if hasGpu && g.Value() != 0 && !hasReplicas {
		return float64(g.Value()), float64(g.Value()), nil
	}

	// Case 2: NVIDIA GPU with GPU Feature Discovery (GFD) Pod enabled.
	// Ref: https://docs.nvidia.com/datacenter/cloud-native/gpu-operator/latest/gpu-sharing.html#verifying-the-gpu-time-slicing-configuration
	// Ref: https://github.com/NVIDIA/k8s-device-plugin/blob/d899752a424818428f744a946d32b132ea2c0cf1/internal/lm/resource_test.go#L44-L45
	// Ref: https://github.com/NVIDIA/k8s-device-plugin/blob/d899752a424818428f744a946d32b132ea2c0cf1/internal/lm/resource_test.go#L103-L118
	if hasReplicas {
		resultGPU := 0.0
		resultVGPU := 0.0

		if c, ok := n.Labels["nvidia.com/gpu.count"]; ok {
			var err error
			resultGPU, err = strconv.ParseFloat(c, 64)
			if err != nil {
				return -1, -1, fmt.Errorf("could not parse label \"nvidia.com/gpu.count\": %v", err)
			}
		}

		if s, ok := n.Status.Capacity["nvidia.com/gpu.shared"]; ok { // GFD configured `renameByDefault=true`
			resultVGPU = float64(s.Value())
		} else if g, ok := n.Status.Capacity["nvidia.com/gpu"]; ok { // GFD configured `renameByDefault=false`
			resultVGPU = float64(g.Value())
		} else {
			resultVGPU = resultGPU
		}

		return resultGPU, resultVGPU, nil
	}

	// Case 3: AWS vGPU
	if vgpu, ok := n.Status.Capacity["k8s.amazonaws.com/vgpu"]; ok {
		vgpuCount, err := getAllocatableVGPUs(cache)
		if err != nil {
			return -1, -1, err
		}

		vgpuCoeff := 10.0
		if vgpuCount > 0.0 {
			vgpuCoeff = vgpuCount
		}

		if vgpu.Value() != 0 {
			resultGPU := float64(vgpu.Value()) / vgpuCoeff
			resultVGPU := float64(vgpu.Value())
			return resultGPU, resultVGPU, nil
		}
	}

	// No GPU found
	return -1, -1, nil
}

func getAllocatableVGPUs(cache clustercache.ClusterCache) (float64, error) {
	daemonsets := cache.GetAllDaemonSets()
	vgpuCount := 0.0
	for _, ds := range daemonsets {
		dsContainerList := &ds.SpecContainers
		for _, ctnr := range *dsContainerList {
			if ctnr.Args != nil {
				for _, arg := range ctnr.Args {
					if strings.Contains(arg, "--vgpu=") {
						vgpus, err := strconv.ParseFloat(arg[strings.IndexByte(arg, '=')+1:], 64)
						if err != nil {
							log.Errorf("failed to parse vgpu allocation string %s: %v", arg, err)
							continue
						}
						vgpuCount = vgpus
						return vgpuCount, nil
					}

				}
			}
		}
	}
	return vgpuCount, nil
}

type PersistentVolumeClaimData struct {
	Class        string                `json:"class"`
	Claim        string                `json:"claim"`
	Namespace    string                `json:"namespace"`
	ClusterID    string                `json:"clusterId"`
	TimesClaimed int                   `json:"timesClaimed"`
	VolumeName   string                `json:"volumeName"`
	Volume       *costAnalyzerCloud.PV `json:"persistentVolume"`
	Values       []*util.Vector        `json:"values"`
}

func measureTime(start time.Time, threshold time.Duration, name string) {
	elapsed := time.Since(start)
	if elapsed > threshold {
		log.Infof("[Profiler] %s: %s", elapsed, name)
	}
}

func (cm *CostModel) QueryAllocation(window opencost.Window, resolution, step time.Duration, aggregate []string, includeIdle, idleByNode, includeProportionalAssetResourceCosts, includeAggregatedMetadata, sharedLoadBalancer bool, accumulateBy opencost.AccumulateOption, shareIdle bool) (*opencost.AllocationSetRange, error) {
	// Validate window is legal
	if window.IsOpen() || window.IsNegative() {
		return nil, fmt.Errorf("illegal window: %s", window)
	}

	var totalsStore opencost.TotalsStore
	// Idle is required for proportional asset costs
	if includeProportionalAssetResourceCosts {
		if !includeIdle {
			return nil, errors.New("bad request - includeIdle must be set true if includeProportionalAssetResourceCosts is true")
		}
		totalsStore = opencost.NewMemoryTotalsStore()
	}

	// Begin with empty response
	asr := opencost.NewAllocationSetRange()

	// Query for AllocationSets in increments of the given step duration,
	// appending each to the response.
	stepStart := *window.Start()
	stepEnd := stepStart.Add(step)
	var isAKS bool
	for window.End().After(stepStart) {
		allocSet, err := cm.ComputeAllocation(stepStart, stepEnd, resolution)
		if err != nil {
			return nil, fmt.Errorf("error computing allocations for %s: %w", opencost.NewClosedWindow(stepStart, stepEnd), err)
		}

		if includeIdle {
			assetSet, err := cm.ComputeAssets(stepStart, stepEnd)
			if err != nil {
				return nil, fmt.Errorf("error computing assets for %s: %w", opencost.NewClosedWindow(stepStart, stepEnd), err)
			}

			if includeProportionalAssetResourceCosts {

				// AKS is a special case - there can be a maximum of 2
				// load balancers (1 public and 1 private) in an AKS cluster
				// therefore, when calculating PARCs for load balancers,
				// we must know if this is an AKS cluster
				for _, node := range assetSet.Nodes {
					if _, found := node.Labels["label_kubernetes_azure_com_cluster"]; found {
						isAKS = true
						break
					}
				}

				_, err := opencost.UpdateAssetTotalsStore(totalsStore, assetSet)
				if err != nil {
					log.Errorf("ETL: error updating asset resource totals for %s: %s", assetSet.Window, err)
				}
			}

			idleSet, err := computeIdleAllocations(allocSet, assetSet, true)
			if err != nil {
				return nil, fmt.Errorf("error computing idle allocations for %s: %w", opencost.NewClosedWindow(stepStart, stepEnd), err)
			}

			for _, idleAlloc := range idleSet.Allocations {
				allocSet.Insert(idleAlloc)
			}
		}

		asr.Append(allocSet)

		stepStart = stepEnd
		stepEnd = stepStart.Add(step)
	}

	// Set aggregation options and aggregate
	var shareIdleOpt string
	if shareIdle {
		shareIdleOpt = opencost.ShareWeighted
	} else {
		shareIdleOpt = opencost.ShareNone
	}

	opts := &opencost.AllocationAggregationOptions{
		IncludeProportionalAssetResourceCosts: includeProportionalAssetResourceCosts,
		IdleByNode:                            idleByNode,
		IncludeAggregatedMetadata:             includeAggregatedMetadata,
		ShareIdle:                             shareIdleOpt,
	}

	// Aggregate
	err := asr.AggregateBy(aggregate, opts)
	if err != nil {
		return nil, fmt.Errorf("error aggregating for %s: %w", window, err)
	}

	// Accumulate, if requested
	if accumulateBy != opencost.AccumulateOptionNone {
		asr, err = asr.Accumulate(accumulateBy)
		if err != nil {
			log.Errorf("error accumulating by %v: %s", accumulateBy, err)
			return nil, fmt.Errorf("error accumulating by %v: %s", accumulateBy, err)
		}

		// when accumulating and returning PARCs, we need the totals for the
		// accumulated windows to accurately compute a fraction
		if includeProportionalAssetResourceCosts {
			assetSet, err := cm.ComputeAssets(*asr.Window().Start(), *asr.Window().End())
			if err != nil {
				return nil, fmt.Errorf("error computing assets for %s: %w", opencost.NewClosedWindow(*asr.Window().Start(), *asr.Window().End()), err)
			}

			_, err = opencost.UpdateAssetTotalsStore(totalsStore, assetSet)
			if err != nil {
				log.Errorf("ETL: error updating asset resource totals for %s: %s", opencost.NewClosedWindow(*asr.Window().Start(), *asr.Window().End()), err)
			}

		}
	}

	if includeProportionalAssetResourceCosts {

		for _, as := range asr.Allocations {
			totalStoreByNode, ok := totalsStore.GetAssetTotalsByNode(as.Start(), as.End())
			if !ok {
				log.Errorf("unable to locate allocation totals for node for window %v - %v", as.Start(), as.End())
				return nil, fmt.Errorf("unable to locate allocation totals for node for window %v - %v", as.Start(), as.End())
			}

			totalStoreByCluster, ok := totalsStore.GetAssetTotalsByCluster(as.Start(), as.End())
			if !ok {
				log.Errorf("unable to locate allocation totals for cluster for window %v - %v", as.Start(), as.End())
				return nil, fmt.Errorf("unable to locate allocation totals for cluster for window %v - %v", as.Start(), as.End())
			}

			var totalPublicLbCost, totalPrivateLbCost float64
			if isAKS && sharedLoadBalancer {
				// loop through all assetTotals, adding all load balancer costs by public and private
				for _, tot := range totalStoreByNode {
					if tot.PrivateLoadBalancer {
						totalPrivateLbCost += tot.LoadBalancerCost
					} else {
						totalPublicLbCost += tot.LoadBalancerCost
					}
				}
			}

			// loop through each allocation set, using total cost from totals store
			for _, alloc := range as.Allocations {
				for rawKey, parc := range alloc.ProportionalAssetResourceCosts {

					key := strings.TrimSuffix(strings.ReplaceAll(rawKey, ",", "/"), "/")
					// for each parc , check the totals store for each
					// on a totals hit, set the corresponding total and calculate percentage
					var totals *opencost.AssetTotals
					if totalsLoc, found := totalStoreByCluster[key]; found {
						totals = totalsLoc
					}

					if totalsLoc, found := totalStoreByNode[key]; found {
						totals = totalsLoc
					}

					if totals == nil {
						log.Errorf("unable to locate asset totals for allocation %s, corresponding PARC is being skipped", key)
						continue
					}

					parc.CPUTotalCost = totals.CPUCost
					parc.GPUTotalCost = totals.GPUCost
					parc.RAMTotalCost = totals.RAMCost
					parc.PVTotalCost = totals.PersistentVolumeCost
					if isAKS && sharedLoadBalancer && len(alloc.LoadBalancers) > 0 {
						// Azure is a special case - use computed totals above
						// use the lbAllocations in the object to determine if
						// this PARC is a public or private load balancer
						// then set the total accordingly
						// AKS only has 1 public and 1 private load balancer

						lbAlloc, found := alloc.LoadBalancers[key]
						if found {
							if lbAlloc.Private {
								parc.LoadBalancerTotalCost = totalPrivateLbCost
							} else {
								parc.LoadBalancerTotalCost = totalPublicLbCost
							}
						}
					} else {
						parc.LoadBalancerTotalCost = totals.LoadBalancerCost
					}

					opencost.ComputePercentages(&parc)
					alloc.ProportionalAssetResourceCosts[rawKey] = parc
				}
			}

		}
	}

	return asr, nil
}

func computeIdleAllocations(allocSet *opencost.AllocationSet, assetSet *opencost.AssetSet, idleByNode bool) (*opencost.AllocationSet, error) {
	if !allocSet.Window.Equal(assetSet.Window) {
		return nil, fmt.Errorf("cannot compute idle allocations for mismatched sets: %s does not equal %s", allocSet.Window, assetSet.Window)
	}

	var allocTotals map[string]*opencost.AllocationTotals
	var assetTotals map[string]*opencost.AssetTotals

	if idleByNode {
		allocTotals = opencost.ComputeAllocationTotals(allocSet, opencost.AllocationNodeProp)
		assetTotals = opencost.ComputeAssetTotals(assetSet, true)
	} else {
		allocTotals = opencost.ComputeAllocationTotals(allocSet, opencost.AllocationClusterProp)
		assetTotals = opencost.ComputeAssetTotals(assetSet, false)
	}

	start, end := *allocSet.Window.Start(), *allocSet.Window.End()
	idleSet := opencost.NewAllocationSet(start, end)

	for key, assetTotal := range assetTotals {
		allocTotal, ok := allocTotals[key]
		if !ok {
			log.Warnf("ETL: did not find allocations for asset key: %s", key)

			// Use a zero-value set of totals. This indicates either (1) an
			// error computing totals, or (2) that no allocations ran on the
			// given node for the given window.
			allocTotal = &opencost.AllocationTotals{
				Cluster: assetTotal.Cluster,
				Node:    assetTotal.Node,
				Start:   assetTotal.Start,
				End:     assetTotal.End,
			}
		}

		// Insert one idle allocation for each key (whether by node or
		// by cluster), defined as the difference between the total
		// asset cost and the allocated cost per-resource.
		name := fmt.Sprintf("%s/%s", key, opencost.IdleSuffix)
		err := idleSet.Insert(&opencost.Allocation{
			Name:   name,
			Window: idleSet.Window.Clone(),
			Properties: &opencost.AllocationProperties{
				Cluster:    assetTotal.Cluster,
				Node:       assetTotal.Node,
				ProviderID: assetTotal.Node,
			},
			Start:   assetTotal.Start,
			End:     assetTotal.End,
			CPUCost: assetTotal.TotalCPUCost() - allocTotal.TotalCPUCost(),
			GPUCost: assetTotal.TotalGPUCost() - allocTotal.TotalGPUCost(),
			RAMCost: assetTotal.TotalRAMCost() - allocTotal.TotalRAMCost(),
		})
		if err != nil {
			return nil, fmt.Errorf("failed to insert idle allocation %s: %w", name, err)
		}
	}

	return idleSet, nil
}
