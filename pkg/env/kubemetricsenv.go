package env

import "github.com/opencost/opencost/core/pkg/env"

const (
	KubecostMetricsPodEnabledEnvVar = "KUBECOST_METRICS_POD_ENABLED"
	KubecostMetricsPodPortEnvVar    = "KUBECOST_METRICS_PORT"
	ExportClusterCacheEnabledEnvVar = "EXPORT_CLUSTER_CACHE_ENABLED"
	ExportClusterInfoEnabledEnvVar  = "EXPORT_CLUSTER_INFO_ENABLED"
)

func GetKubecostMetricsPort() int {
	return env.GetInt(KubecostMetricsPodPortEnvVar, 9005)
}

// IsKubecostMetricsPodEnabled returns true if the kubecost metrics pod is deployed
func IsKubecostMetricsPodEnabled() bool {
	return env.GetBool(KubecostMetricsPodEnabledEnvVar, false)
}

// IsExportClusterCacheEnabled is set to true if the metrics pod should export the cluster cache
// data to a target file location
func IsExportClusterCacheEnabled() bool {
	return env.GetBool(ExportClusterCacheEnabledEnvVar, false)
}

// IsExportClusterInfoEnabled is set to true if the metrics pod should export its own cluster info
// data to a target file location
func IsExportClusterInfoEnabled() bool {
	return env.GetBool(ExportClusterInfoEnabledEnvVar, false)
}
