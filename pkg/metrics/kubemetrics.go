package metrics

import (
	"fmt"
	"strings"
	"sync"

	"github.com/opencost/opencost/core/pkg/util/promutil"
	"github.com/opencost/opencost/pkg/clustercache"

	"github.com/prometheus/client_golang/prometheus"
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/validation"
)

//--------------------------------------------------------------------------
//  Kube Metric Registration
//--------------------------------------------------------------------------

// initializer
var kubeMetricInit sync.Once

// KubeMetricsOpts represents our Kubernetes metrics emission options.
type KubeMetricsOpts struct {
	EmitKubecostControllerMetrics bool
	EmitNamespaceAnnotations      bool
	EmitPodAnnotations            bool
	EmitKubeStateMetrics          bool
	EmitKubeStateMetricsV1Only    bool
	EmitDeprecatedMetrics         bool
}

// DefaultKubeMetricsOpts returns KubeMetricsOpts with default values set
func DefaultKubeMetricsOpts() *KubeMetricsOpts {
	return &KubeMetricsOpts{
		EmitKubecostControllerMetrics: true,
		EmitNamespaceAnnotations:      false,
		EmitPodAnnotations:            false,
		EmitKubeStateMetrics:          true,
		EmitKubeStateMetricsV1Only:    false,
		EmitDeprecatedMetrics:         false,
	}
}

// InitKubeMetrics initializes kubernetes metric emission using the provided options.
func InitKubeMetrics(clusterCache clustercache.ClusterCache, metricsConfig *MetricsConfig, opts *KubeMetricsOpts) {
	if opts == nil {
		opts = DefaultKubeMetricsOpts()
	}

	kubeMetricInit.Do(func() {
		if !opts.EmitDeprecatedMetrics {
			metricsConfig.DisabledMetrics = append(metricsConfig.DisabledMetrics,
				"kube_pod_container_resource_limits",
				"kube_pod_container_resource_limits_memory_bytes",
				"kube_pod_container_resource_limits_cpu_cores",
				"kube_pod_container_status_restarts_total",
				"kube_node_status_condition",
				"kube_deployment_status_replicas_available",
				"kube_deployment_spec_replicas",
				"kube_persistentvolume_status_phase",
				"kube_pod_status_phase",
			)
		}

		if opts.EmitKubecostControllerMetrics {
			prometheus.MustRegister(KubecostServiceCollector{
				KubeClusterCache: clusterCache,
				metricsConfig:    *metricsConfig,
			})
			prometheus.MustRegister(KubecostDeploymentCollector{
				KubeClusterCache: clusterCache,
				metricsConfig:    *metricsConfig,
			})
			prometheus.MustRegister(KubecostStatefulsetCollector{
				KubeClusterCache: clusterCache,
				metricsConfig:    *metricsConfig,
			})
		}

		if opts.EmitPodAnnotations {
			prometheus.MustRegister(KubecostPodCollector{
				KubeClusterCache: clusterCache,
				metricsConfig:    *metricsConfig,
			})
		}

		if opts.EmitNamespaceAnnotations {
			prometheus.MustRegister(KubecostNamespaceCollector{
				KubeClusterCache: clusterCache,
				metricsConfig:    *metricsConfig,
			})
		}

		if opts.EmitKubeStateMetrics {
			prometheus.MustRegister(KubeNodeCollector{
				KubeClusterCache: clusterCache,
				metricsConfig:    *metricsConfig,
			})
			prometheus.MustRegister(KubeNamespaceCollector{
				KubeClusterCache: clusterCache,
				metricsConfig:    *metricsConfig,
			})
			prometheus.MustRegister(KubeDeploymentCollector{
				KubeClusterCache: clusterCache,
				metricsConfig:    *metricsConfig,
			})
			prometheus.MustRegister(KubePodCollector{
				KubeClusterCache: clusterCache,
				metricsConfig:    *metricsConfig,
			})
			prometheus.MustRegister(KubePVCollector{
				KubeClusterCache: clusterCache,
				metricsConfig:    *metricsConfig,
			})
			prometheus.MustRegister(KubePVCCollector{
				KubeClusterCache: clusterCache,
				metricsConfig:    *metricsConfig,
			})
			prometheus.MustRegister(KubeJobCollector{
				KubeClusterCache: clusterCache,
				metricsConfig:    *metricsConfig,
			})
		} else if opts.EmitKubeStateMetricsV1Only {
			// We still need the kubecost_pv_info metric to look up storageclass on legacy clusters.
			forceDisabled := []string{"kube_persistentvolume_capacity_bytes", "kube_persistentvolume_status_phase"}
			metricsConfig.DisabledMetrics = append(metricsConfig.DisabledMetrics, forceDisabled...)
			prometheus.MustRegister(KubeNodeCollector{
				KubeClusterCache: clusterCache,
				metricsConfig:    *metricsConfig,
			})
			prometheus.MustRegister(KubeNamespaceCollector{
				KubeClusterCache: clusterCache,
				metricsConfig:    *metricsConfig,
			})
			prometheus.MustRegister(KubePodLabelsCollector{
				KubeClusterCache: clusterCache,
				metricsConfig:    *metricsConfig,
			})
			prometheus.MustRegister(KubePVCollector{
				KubeClusterCache: clusterCache,
				metricsConfig:    *metricsConfig,
			})
		} else {
			// We still need the kubecost_pv_info metric to look up storageclass on legacy clusters.
			forceDisabled := []string{"kube_persistentvolume_capacity_bytes", "kube_persistentvolume_status_phase"}
			metricsConfig.DisabledMetrics = append(metricsConfig.DisabledMetrics, forceDisabled...)
			prometheus.MustRegister(KubePVCollector{
				KubeClusterCache: clusterCache,
				metricsConfig:    *metricsConfig,
			})
		}
	})
}

//--------------------------------------------------------------------------
//  Kube Metric Helpers
//--------------------------------------------------------------------------

// getPersistentVolumeClaimClass returns StorageClassName. If no storage class was
// requested, it returns "".
func getPersistentVolumeClaimClass(claim *clustercache.PersistentVolumeClaim) string {
	// Use beta annotation first
	if class, found := claim.Annotations[v1.BetaStorageClassAnnotation]; found {
		return class
	}

	if claim.Spec.StorageClassName != nil {
		return *claim.Spec.StorageClassName
	}

	// Special non-empty string to indicate absence of storage class.
	return ""
}

// toResourceUnitValue accepts a resource name and quantity and returns the sanitized resource, the unit, and the value in the units.
// Returns an empty string for resource and unit if there was a failure.
func toResourceUnitValue(resourceName v1.ResourceName, quantity resource.Quantity) (resource string, unit string, value float64) {
	resource = promutil.SanitizeLabelName(string(resourceName))

	switch resourceName {
	case v1.ResourceCPU:
		unit = "core"
		value = float64(quantity.MilliValue()) / 1000
		return

	case v1.ResourceStorage:
		fallthrough
	case v1.ResourceEphemeralStorage:
		fallthrough
	case v1.ResourceMemory:
		unit = "byte"
		value = float64(quantity.Value())
		return
	case v1.ResourcePods:
		unit = "integer"
		value = float64(quantity.Value())
		return
	default:
		if isHugePageResourceName(resourceName) || isAttachableVolumeResourceName(resourceName) {
			unit = "byte"
			value = float64(quantity.Value())
			return
		}

		if isExtendedResourceName(resourceName) {
			unit = "integer"
			value = float64(quantity.Value())
			return
		}
	}

	resource = ""
	unit = ""
	value = 0.0
	return
}

// isHugePageResourceName checks for a huge page container resource name
func isHugePageResourceName(name v1.ResourceName) bool {
	return strings.HasPrefix(string(name), v1.ResourceHugePagesPrefix)
}

// isAttachableVolumeResourceName checks for attached volume container resource name
func isAttachableVolumeResourceName(name v1.ResourceName) bool {
	return strings.HasPrefix(string(name), v1.ResourceAttachableVolumesPrefix)
}

// isExtendedResourceName checks for extended container resource name
func isExtendedResourceName(name v1.ResourceName) bool {
	if isNativeResource(name) || strings.HasPrefix(string(name), v1.DefaultResourceRequestsPrefix) {
		return false
	}
	// Ensure it satisfies the rules in IsQualifiedName() after converted into quota resource name
	nameForQuota := fmt.Sprintf("%s%s", v1.DefaultResourceRequestsPrefix, string(name))
	if errs := validation.IsQualifiedName(nameForQuota); len(errs) != 0 {
		return false
	}
	return true
}

// isNativeResource checks for a kubernetes.io/ prefixed resource name
func isNativeResource(name v1.ResourceName) bool {
	return !strings.Contains(string(name), "/") || isPrefixedNativeResource(name)
}

func isPrefixedNativeResource(name v1.ResourceName) bool {
	return strings.Contains(string(name), v1.ResourceDefaultNamespacePrefix)
}

func failureReason(jc *batchv1.JobCondition, reason string) bool {
	if jc == nil {
		return false
	}
	return jc.Reason == reason
}

// boolFloat64 converts a boolean input into a 1 or 0
func boolFloat64(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// toStringPtr is used to create a new string pointer from iteration vars
func toStringPtr(s string) *string { return &s }
