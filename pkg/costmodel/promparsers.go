package costmodel

import (
	"errors"
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util"
	costAnalyzerCloud "github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/clustercache"
	"github.com/opencost/opencost/pkg/env"
	"github.com/opencost/opencost/pkg/prom"
)

func GetPVInfoLocal(cache clustercache.ClusterCache, defaultClusterID string) (map[string]*PersistentVolumeClaimData, error) {
	toReturn := make(map[string]*PersistentVolumeClaimData)

	pvcs := cache.GetAllPersistentVolumeClaims()
	for _, pvc := range pvcs {
		var vals []*util.Vector
		vals = append(vals, &util.Vector{
			Timestamp: float64(time.Now().Unix()),
			Value:     float64(pvc.Spec.Resources.Requests.Storage().Value()),
		})
		ns := pvc.Namespace
		pvcName := pvc.Name
		volumeName := pvc.Spec.VolumeName
		pvClass := ""
		if pvc.Spec.StorageClassName != nil {
			pvClass = *pvc.Spec.StorageClassName
		}
		clusterID := defaultClusterID
		key := fmt.Sprintf("%s,%s,%s", ns, pvcName, clusterID)
		toReturn[key] = &PersistentVolumeClaimData{
			Class:      pvClass,
			Claim:      pvcName,
			Namespace:  ns,
			ClusterID:  clusterID,
			VolumeName: volumeName,
			Values:     vals,
		}
	}
	return toReturn, nil
}

// TODO niko/prom move parsing functions from costmodel.go

func GetPVInfo(qrs []*prom.QueryResult, defaultClusterID string) (map[string]*PersistentVolumeClaimData, error) {
	toReturn := make(map[string]*PersistentVolumeClaimData)

	for _, val := range qrs {
		clusterID, _ := val.GetString(env.GetPromClusterLabel())
		if clusterID == "" {
			clusterID = defaultClusterID
		}

		ns, err := val.GetString("namespace")
		if err != nil {
			return toReturn, err
		}

		pvcName, err := val.GetString("persistentvolumeclaim")
		if err != nil {
			return toReturn, err
		}

		volumeName, err := val.GetString("volumename")
		if err != nil {
			log.Debugf("Unfulfilled claim %s: volumename field does not exist in data result vector", pvcName)
			volumeName = ""
		}

		pvClass, err := val.GetString("storageclass")
		if err != nil {
			// TODO: We need to look up the actual PV and PV capacity. For now just proceed with "".
			log.DedupedWarningf(5, "Storage Class not found for claim \"%s/%s\".", ns, pvcName)
			pvClass = ""
		}

		key := fmt.Sprintf("%s,%s,%s", ns, pvcName, clusterID)
		toReturn[key] = &PersistentVolumeClaimData{
			Class:      pvClass,
			Claim:      pvcName,
			Namespace:  ns,
			ClusterID:  clusterID,
			VolumeName: volumeName,
			Values:     val.Values,
		}
	}

	return toReturn, nil
}

func GetPVAllocationMetrics(qrs []*prom.QueryResult, defaultClusterID string) (map[string][]*PersistentVolumeClaimData, error) {
	toReturn := make(map[string][]*PersistentVolumeClaimData)

	for _, val := range qrs {
		clusterID, _ := val.GetString(env.GetPromClusterLabel())
		if clusterID == "" {
			clusterID = defaultClusterID
		}

		ns, err := val.GetString("namespace")
		if err != nil {
			return toReturn, err
		}

		pod, err := val.GetString("pod")
		if err != nil {
			return toReturn, err
		}

		pvcName, err := val.GetString("persistentvolumeclaim")
		if err != nil {
			return toReturn, err
		}

		pvName, err := val.GetString("persistentvolume")
		if err != nil {
			log.Warnf("persistentvolume field does not exist for pv %s", pvcName) // This is possible for an unfulfilled claim
			continue
		}

		key := fmt.Sprintf("%s,%s,%s", ns, pod, clusterID)
		pvcData := &PersistentVolumeClaimData{
			Class:      "",
			Claim:      pvcName,
			Namespace:  ns,
			ClusterID:  clusterID,
			VolumeName: pvName,
			Values:     val.Values,
		}

		toReturn[key] = append(toReturn[key], pvcData)
	}

	return toReturn, nil
}

func GetPVCostMetrics(qrs []*prom.QueryResult, defaultClusterID string) (map[string]*costAnalyzerCloud.PV, error) {
	toReturn := make(map[string]*costAnalyzerCloud.PV)

	for _, val := range qrs {
		clusterID, _ := val.GetString(env.GetPromClusterLabel())
		if clusterID == "" {
			clusterID = defaultClusterID
		}

		volumeName, err := val.GetString("volumename")
		if err != nil {
			return toReturn, err
		}

		key := fmt.Sprintf("%s,%s", volumeName, clusterID)
		toReturn[key] = &costAnalyzerCloud.PV{
			Cost: fmt.Sprintf("%f", val.Values[0].Value),
		}
	}

	return toReturn, nil
}

func GetNamespaceLabelsMetrics(qrs []*prom.QueryResult, defaultClusterID string) (map[string]map[string]string, error) {
	toReturn := make(map[string]map[string]string)

	for _, val := range qrs {
		// We want Namespace and ClusterID for key generation purposes
		ns, err := val.GetString("namespace")
		if err != nil {
			return toReturn, err
		}

		clusterID, _ := val.GetString(env.GetPromClusterLabel())
		if clusterID == "" {
			clusterID = defaultClusterID
		}

		nsKey := ns + "," + clusterID
		if nsLabels, ok := toReturn[nsKey]; ok {
			for k, v := range val.GetLabels() {
				nsLabels[k] = v // override with more recently assigned if we changed labels within the window.
			}
		} else {
			toReturn[nsKey] = val.GetLabels()
		}
	}
	return toReturn, nil
}

func GetPodLabelsMetrics(qrs []*prom.QueryResult, defaultClusterID string) (map[string]map[string]string, error) {
	toReturn := make(map[string]map[string]string)

	for _, val := range qrs {
		// We want Pod, Namespace and ClusterID for key generation purposes
		pod, err := val.GetString("pod")
		if err != nil {
			return toReturn, err
		}

		ns, err := val.GetString("namespace")
		if err != nil {
			return toReturn, err
		}

		clusterID, _ := val.GetString(env.GetPromClusterLabel())
		if clusterID == "" {
			clusterID = defaultClusterID
		}

		nsKey := ns + "," + pod + "," + clusterID
		if labels, ok := toReturn[nsKey]; ok {
			newlabels := val.GetLabels()
			for k, v := range newlabels {
				labels[k] = v
			}
		} else {
			toReturn[nsKey] = val.GetLabels()
		}
	}

	return toReturn, nil
}

func GetNamespaceAnnotationsMetrics(qrs []*prom.QueryResult, defaultClusterID string) (map[string]map[string]string, error) {
	toReturn := make(map[string]map[string]string)

	for _, val := range qrs {
		// We want Namespace and ClusterID for key generation purposes
		ns, err := val.GetString("namespace")
		if err != nil {
			return toReturn, err
		}

		clusterID, _ := val.GetString(env.GetPromClusterLabel())
		if clusterID == "" {
			clusterID = defaultClusterID
		}

		nsKey := ns + "," + clusterID
		if nsAnnotations, ok := toReturn[nsKey]; ok {
			for k, v := range val.GetAnnotations() {
				nsAnnotations[k] = v // override with more recently assigned if we changed labels within the window.
			}
		} else {
			toReturn[nsKey] = val.GetAnnotations()
		}
	}
	return toReturn, nil
}

func GetPodAnnotationsMetrics(qrs []*prom.QueryResult, defaultClusterID string) (map[string]map[string]string, error) {
	toReturn := make(map[string]map[string]string)

	for _, val := range qrs {
		// We want Pod, Namespace and ClusterID for key generation purposes
		pod, err := val.GetString("pod")
		if err != nil {
			return toReturn, err
		}

		ns, err := val.GetString("namespace")
		if err != nil {
			return toReturn, err
		}

		clusterID, _ := val.GetString(env.GetPromClusterLabel())

		if clusterID == "" {
			clusterID = defaultClusterID
		}

		nsKey := ns + "," + pod + "," + clusterID
		if labels, ok := toReturn[nsKey]; ok {
			for k, v := range val.GetAnnotations() {
				labels[k] = v
			}
		} else {
			toReturn[nsKey] = val.GetAnnotations()
		}
	}

	return toReturn, nil
}

func GetStatefulsetMatchLabelsMetrics(qrs []*prom.QueryResult, defaultClusterID string) (map[string]map[string]string, error) {
	toReturn := make(map[string]map[string]string)

	for _, val := range qrs {
		// We want Statefulset, Namespace and ClusterID for key generation purposes
		ss, err := val.GetString("statefulSet")
		if err != nil {
			return toReturn, err
		}

		ns, err := val.GetString("namespace")
		if err != nil {
			return toReturn, err
		}

		clusterID, _ := val.GetString(env.GetPromClusterLabel())
		if clusterID == "" {
			clusterID = defaultClusterID
		}

		nsKey := ns + "," + ss + "," + clusterID
		toReturn[nsKey] = val.GetLabels()
	}

	return toReturn, nil
}

func GetPodDaemonsetsWithMetrics(qrs []*prom.QueryResult, defaultClusterID string) (map[string]string, error) {
	toReturn := make(map[string]string)

	for _, val := range qrs {
		ds, err := val.GetString("owner_name")
		if err != nil {
			return toReturn, err
		}

		ns, err := val.GetString("namespace")
		if err != nil {
			return toReturn, err
		}

		clusterID, _ := val.GetString(env.GetPromClusterLabel())
		if clusterID == "" {
			clusterID = defaultClusterID
		}

		pod, err := val.GetString("pod")
		if err != nil {
			return toReturn, err
		}

		nsKey := ns + "," + pod + "," + clusterID
		toReturn[nsKey] = ds
	}

	return toReturn, nil
}

func GetPodJobsWithMetrics(qrs []*prom.QueryResult, defaultClusterID string) (map[string]string, error) {
	toReturn := make(map[string]string)

	for _, val := range qrs {
		ds, err := val.GetString("owner_name")
		if err != nil {
			return toReturn, err
		}

		ns, err := val.GetString("namespace")
		if err != nil {
			return toReturn, err
		}

		clusterID, _ := val.GetString(env.GetPromClusterLabel())
		if clusterID == "" {
			clusterID = defaultClusterID
		}

		pod, err := val.GetString("pod")
		if err != nil {
			return toReturn, err
		}

		nsKey := ns + "," + pod + "," + clusterID
		toReturn[nsKey] = ds
	}

	return toReturn, nil
}

func GetDeploymentMatchLabelsMetrics(qrs []*prom.QueryResult, defaultClusterID string) (map[string]map[string]string, error) {
	toReturn := make(map[string]map[string]string)

	for _, val := range qrs {
		// We want Deployment, Namespace and ClusterID for key generation purposes
		deployment, err := val.GetString("deployment")
		if err != nil {
			return toReturn, err
		}

		ns, err := val.GetString("namespace")
		if err != nil {
			return toReturn, err
		}

		clusterID, _ := val.GetString(env.GetPromClusterLabel())
		if clusterID == "" {
			clusterID = defaultClusterID
		}

		nsKey := ns + "," + deployment + "," + clusterID
		toReturn[nsKey] = val.GetLabels()
	}

	return toReturn, nil
}

func GetServiceSelectorLabelsMetrics(qrs []*prom.QueryResult, defaultClusterID string) (map[string]map[string]string, error) {
	toReturn := make(map[string]map[string]string)

	for _, val := range qrs {
		// We want Service, Namespace and ClusterID for key generation purposes
		service, err := val.GetString("service")
		if err != nil {
			return toReturn, err
		}

		ns, err := val.GetString("namespace")
		if err != nil {
			return toReturn, err
		}

		clusterID, _ := val.GetString(env.GetPromClusterLabel())
		if clusterID == "" {
			clusterID = defaultClusterID
		}

		nsKey := ns + "," + service + "," + clusterID
		toReturn[nsKey] = val.GetLabels()
	}

	return toReturn, nil
}

func GetContainerMetricVector(qrs []*prom.QueryResult, normalize bool, normalizationValue float64, defaultClusterID string) (map[string][]*util.Vector, error) {
	containerData := make(map[string][]*util.Vector)
	for _, val := range qrs {
		containerMetric, err := NewContainerMetricFromPrometheus(val.Metric, defaultClusterID)
		if err != nil {
			return nil, err
		}

		if normalize && normalizationValue != 0 {
			for _, v := range val.Values {
				v.Value = v.Value / normalizationValue
			}
		}
		containerData[containerMetric.Key()] = val.Values
	}
	return containerData, nil
}

func GetContainerMetricVectors(qrs []*prom.QueryResult, defaultClusterID string) (map[string][]*util.Vector, error) {
	containerData := make(map[string][]*util.Vector)
	for _, val := range qrs {
		containerMetric, err := NewContainerMetricFromPrometheus(val.Metric, defaultClusterID)
		if err != nil {
			return nil, err
		}
		containerData[containerMetric.Key()] = val.Values
	}
	return containerData, nil
}

func GetNormalizedContainerMetricVectors(qrs []*prom.QueryResult, normalizationValues []*util.Vector, defaultClusterID string) (map[string][]*util.Vector, error) {
	containerData := make(map[string][]*util.Vector)
	for _, val := range qrs {
		containerMetric, err := NewContainerMetricFromPrometheus(val.Metric, defaultClusterID)
		if err != nil {
			return nil, err
		}
		containerData[containerMetric.Key()] = util.NormalizeVectorByVector(val.Values, normalizationValues)
	}
	return containerData, nil
}

func getCost(qrs []*prom.QueryResult) (map[string][]*util.Vector, error) {
	toReturn := make(map[string][]*util.Vector)

	for _, val := range qrs {
		instance, err := val.GetString("node")
		if err != nil {
			return toReturn, err
		}

		toReturn[instance] = val.Values
	}

	return toReturn, nil
}

// TODO niko/prom retain message:
// normalization data is empty: time window may be invalid or kube-state-metrics or node-exporter may not be running
func getNormalization(qrs []*prom.QueryResult) (float64, error) {
	if len(qrs) == 0 {
		return 0.0, prom.NoDataErr("getNormalization")
	}
	if len(qrs[0].Values) == 0 {
		return 0.0, prom.NoDataErr("getNormalization")
	}
	return qrs[0].Values[0].Value, nil
}

// TODO niko/prom retain message:
// normalization data is empty: time window may be invalid or kube-state-metrics or node-exporter may not be running
func getNormalizations(qrs []*prom.QueryResult) ([]*util.Vector, error) {
	if len(qrs) == 0 {
		return nil, prom.NoDataErr("getNormalizations")
	}

	return qrs[0].Values, nil
}

func parsePodLabels(qrs []*prom.QueryResult) (map[string]map[string]string, error) {
	podLabels := map[string]map[string]string{}

	for _, result := range qrs {
		pod, err := result.GetString("pod")
		if err != nil {
			return podLabels, errors.New("missing pod field")
		}

		if _, ok := podLabels[pod]; ok {
			podLabels[pod] = result.GetLabels()
		} else {
			podLabels[pod] = map[string]string{}
			podLabels[pod] = result.GetLabels()
		}
	}

	return podLabels, nil
}
