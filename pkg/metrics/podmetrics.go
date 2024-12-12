package metrics

import (
	"fmt"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util/promutil"
	"github.com/opencost/opencost/pkg/clustercache"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	v1 "k8s.io/api/core/v1"
)

//--------------------------------------------------------------------------
//  KubecostPodCollector
//--------------------------------------------------------------------------

// KubecostPodCollector is a prometheus collector that emits pod metrics
type KubecostPodCollector struct {
	KubeClusterCache clustercache.ClusterCache
	metricsConfig    MetricsConfig
}

// Describe sends the super-set of all possible descriptors of metrics
// collected by this Collector.
func (kpmc KubecostPodCollector) Describe(ch chan<- *prometheus.Desc) {
	disabledMetrics := kpmc.metricsConfig.GetDisabledMetricsMap()
	if _, disabled := disabledMetrics["kube_pod_annotations"]; disabled {
		return
	}

	ch <- prometheus.NewDesc("kube_pod_annotations", "All annotations for each pod prefix with annotation_", []string{}, nil)

}

// Collect is called by the Prometheus registry when collecting metrics.
func (kpmc KubecostPodCollector) Collect(ch chan<- prometheus.Metric) {
	disabledMetrics := kpmc.metricsConfig.GetDisabledMetricsMap()
	if _, disabled := disabledMetrics["kube_pod_annotations"]; disabled {
		return
	}

	pods := kpmc.KubeClusterCache.GetAllPods()
	for _, pod := range pods {
		podName := pod.Name
		podNS := pod.Namespace

		// Pod Annotations
		labels, values := promutil.KubeAnnotationsToLabels(pod.Annotations)
		if len(labels) > 0 {
			ch <- newPodAnnotationMetric("kube_pod_annotations", podNS, podName, labels, values)
		}
	}

}

//--------------------------------------------------------------------------
//  KubePodCollector
//--------------------------------------------------------------------------

// KubePodMetricCollector is a prometheus collector that emits pod metrics
type KubePodCollector struct {
	KubeClusterCache clustercache.ClusterCache
	metricsConfig    MetricsConfig
}

// Describe sends the super-set of all possible descriptors of metrics
// collected by this Collector.
func (kpmc KubePodCollector) Describe(ch chan<- *prometheus.Desc) {
	disabledMetrics := kpmc.metricsConfig.GetDisabledMetricsMap()

	if _, disabled := disabledMetrics["kube_pod_labels"]; !disabled {
		ch <- prometheus.NewDesc("kube_pod_labels", "All labels for each pod prefixed with label_", []string{}, nil)
	}
	if _, disabled := disabledMetrics["kube_pod_owner"]; !disabled {
		ch <- prometheus.NewDesc("kube_pod_owner", "Information about the Pod's owner", []string{}, nil)
	}
	if _, disabled := disabledMetrics["kube_pod_container_status_running"]; !disabled {
		ch <- prometheus.NewDesc("kube_pod_container_status_running", "Describes whether the container is currently in running state", []string{}, nil)
	}
	if _, disabled := disabledMetrics["kube_pod_container_status_terminated_reason"]; !disabled {
		ch <- prometheus.NewDesc("kube_pod_container_status_terminated_reason", "Describes the reason the container is currently in terminated state.", []string{}, nil)
	}
	if _, disabled := disabledMetrics["kube_pod_container_status_restarts_total"]; !disabled {
		ch <- prometheus.NewDesc("kube_pod_container_status_restarts_total", "The number of container restarts per container.", []string{}, nil)
	}
	if _, disabled := disabledMetrics["kube_pod_container_resource_requests"]; !disabled {
		ch <- prometheus.NewDesc("kube_pod_container_resource_requests", "The number of requested resource by a container", []string{}, nil)
	}
	if _, disabled := disabledMetrics["kube_pod_container_resource_limits"]; !disabled {
		ch <- prometheus.NewDesc("kube_pod_container_resource_limits", "The number of requested limit resource by a container.", []string{}, nil)
	}
	if _, disabled := disabledMetrics["kube_pod_container_resource_limits_cpu_cores"]; !disabled {
		ch <- prometheus.NewDesc("kube_pod_container_resource_limits_cpu_cores", "The number of requested limit cpu core resource by a container.", []string{}, nil)
	}
	if _, disabled := disabledMetrics["kube_pod_container_resource_limits_memory_bytes"]; !disabled {
		ch <- prometheus.NewDesc("kube_pod_container_resource_limits_memory_bytes", "The number of requested limit memory resource by a container.", []string{}, nil)
	}
	if _, disabled := disabledMetrics["kube_pod_status_phase"]; !disabled {
		ch <- prometheus.NewDesc("kube_pod_status_phase", "The pods current phase.", []string{}, nil)
	}
}

// Collect is called by the Prometheus registry when collecting metrics.
func (kpmc KubePodCollector) Collect(ch chan<- prometheus.Metric) {
	pods := kpmc.KubeClusterCache.GetAllPods()
	disabledMetrics := kpmc.metricsConfig.GetDisabledMetricsMap()

	for _, pod := range pods {
		podName := pod.Name
		podNS := pod.Namespace
		podUID := string(pod.UID)
		node := pod.Spec.NodeName
		phase := pod.Status.Phase

		// Pod Status Phase
		if _, disabled := disabledMetrics["kube_pod_status_phase"]; !disabled {
			if phase != "" {
				phases := []struct {
					v bool
					n string
				}{
					{phase == v1.PodPending, string(v1.PodPending)},
					{phase == v1.PodSucceeded, string(v1.PodSucceeded)},
					{phase == v1.PodFailed, string(v1.PodFailed)},
					{phase == v1.PodUnknown, string(v1.PodUnknown)},
					{phase == v1.PodRunning, string(v1.PodRunning)},
				}

				for _, p := range phases {
					ch <- newKubePodStatusPhaseMetric("kube_pod_status_phase", podNS, podName, podUID, p.n, boolFloat64(p.v))
				}
			}
		}

		// Pod Labels
		if _, disabled := disabledMetrics["kube_pod_labels"]; !disabled {
			labelNames, labelValues := promutil.KubePrependQualifierToLabels(promutil.SanitizeLabels(pod.Labels), "label_")
			ch <- newKubePodLabelsMetric("kube_pod_labels", podNS, podName, podUID, labelNames, labelValues)
		}

		// Owner References
		if _, disabled := disabledMetrics["kube_pod_owner"]; !disabled {
			for _, owner := range pod.OwnerReferences {
				ch <- newKubePodOwnerMetric("kube_pod_owner", podNS, podName, owner.Name, owner.Kind, owner.Controller != nil)
			}
		}

		// Container Status
		for _, status := range pod.Status.ContainerStatuses {
			if _, disabled := disabledMetrics["kube_pod_container_status_restarts_total"]; !disabled {
				ch <- newKubePodContainerStatusRestartsTotalMetric("kube_pod_container_status_restarts_total", podNS, podName, podUID, status.Name, float64(status.RestartCount))
			}
			if status.State.Running != nil {
				if _, disabled := disabledMetrics["kube_pod_container_status_running"]; !disabled {
					ch <- newKubePodContainerStatusRunningMetric("kube_pod_container_status_running", podNS, podName, podUID, status.Name)
				}
			}

			if status.State.Terminated != nil {
				if _, disabled := disabledMetrics["kube_pod_container_status_terminated_reason"]; !disabled {
					ch <- newKubePodContainerStatusTerminatedReasonMetric(
						"kube_pod_container_status_terminated_reason",
						podNS,
						podName,
						podUID,
						status.Name,
						status.State.Terminated.Reason)
				}
			}
		}

		for _, container := range pod.Spec.Containers {

			// Requests
			if _, disabled := disabledMetrics["kube_pod_container_resource_requests"]; !disabled {
				for resourceName, quantity := range container.Resources.Requests {
					resource, unit, value := toResourceUnitValue(resourceName, quantity)

					// failed to parse the resource type
					if resource == "" {
						log.DedupedWarningf(5, "Failed to parse resource units and quantity for resource: %s", resourceName)
						continue
					}

					ch <- newKubePodContainerResourceRequestsMetric(
						"kube_pod_container_resource_requests",
						podNS,
						podName,
						podUID,
						container.Name,
						node,
						resource,
						unit,
						value)
				}
			}

			// Limits
			for resourceName, quantity := range container.Resources.Limits {
				resource, unit, value := toResourceUnitValue(resourceName, quantity)

				// failed to parse the resource type
				if resource == "" {
					log.DedupedWarningf(5, "Failed to parse resource units and quantity for resource: %s", resourceName)
					continue
				}

				// KSM v1 Emission
				if _, disabled := disabledMetrics["kube_pod_container_resource_limits_cpu_cores"]; !disabled {
					if resource == "cpu" {
						ch <- newKubePodContainerResourceLimitsCPUCoresMetric(
							"kube_pod_container_resource_limits_cpu_cores",
							podNS,
							podName,
							podUID,
							container.Name,
							node,
							value)
					}
				}
				if _, disabled := disabledMetrics["kube_pod_container_resource_limits_memory_bytes"]; !disabled {
					if resource == "memory" {
						ch <- newKubePodContainerResourceLimitsMemoryBytesMetric(
							"kube_pod_container_resource_limits_memory_bytes",
							podNS,
							podName,
							podUID,
							container.Name,
							node,
							value)
					}
				}
				if _, disabled := disabledMetrics["kube_pod_container_resource_limits"]; !disabled {
					ch <- newKubePodContainerResourceLimitsMetric(
						"kube_pod_container_resource_limits",
						podNS,
						podName,
						podUID,
						container.Name,
						node,
						resource,
						unit,
						value)
				}
			}
		}
	}
}

//--------------------------------------------------------------------------
//  PodAnnotationsMetric
//--------------------------------------------------------------------------

// PodAnnotationsMetric is a prometheus.Metric used to encode namespace annotations
type PodAnnotationsMetric struct {
	fqName      string
	help        string
	namespace   string
	pod         string
	labelNames  []string
	labelValues []string
}

// Creates a new PodAnnotationsMetric, implementation of prometheus.Metric
func newPodAnnotationMetric(fqname, namespace, pod string, labelNames, labelValues []string) PodAnnotationsMetric {
	return PodAnnotationsMetric{
		fqName:      fqname,
		help:        "kube_pod_annotations Pod Annotations",
		namespace:   namespace,
		pod:         pod,
		labelNames:  labelNames,
		labelValues: labelValues,
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (pam PodAnnotationsMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{
		"namespace": pam.namespace,
		"pod":       pam.pod,
	}
	return prometheus.NewDesc(pam.fqName, pam.help, []string{}, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data
// transmission object.
func (pam PodAnnotationsMetric) Write(m *dto.Metric) error {
	h := float64(1)
	m.Gauge = &dto.Gauge{
		Value: &h,
	}

	var labels []*dto.LabelPair
	for i := range pam.labelNames {
		labels = append(labels, &dto.LabelPair{
			Name:  &pam.labelNames[i],
			Value: &pam.labelValues[i],
		})
	}
	labels = append(labels,
		&dto.LabelPair{
			Name:  toStringPtr("namespace"),
			Value: &pam.namespace,
		},
		&dto.LabelPair{
			Name:  toStringPtr("pod"),
			Value: &pam.pod,
		})
	m.Label = labels
	return nil
}

//--------------------------------------------------------------------------
//  KubePodLabelsMetric
//--------------------------------------------------------------------------

// KubePodLabelsMetric is a prometheus.Metric used to encode
// a duplicate of the deprecated kube-state-metrics metric
// kube_pod_labels
type KubePodLabelsMetric struct {
	fqName      string
	help        string
	pod         string
	namespace   string
	uid         string
	labelNames  []string
	labelValues []string
}

// Creates a new KubePodLabelsMetric, implementation of prometheus.Metric
func newKubePodLabelsMetric(fqname, namespace, pod, uid string, labelNames []string, labelValues []string) KubePodLabelsMetric {
	return KubePodLabelsMetric{
		fqName:      fqname,
		help:        "kube_pod_labels all labels for each pod prefixed with label_",
		pod:         pod,
		namespace:   namespace,
		uid:         uid,
		labelNames:  labelNames,
		labelValues: labelValues,
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (nam KubePodLabelsMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{
		"namespace": nam.namespace,
		"pod":       nam.pod,
		"uid":       nam.uid,
	}
	return prometheus.NewDesc(nam.fqName, nam.help, nam.labelNames, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data
// transmission object.
func (nam KubePodLabelsMetric) Write(m *dto.Metric) error {
	h := float64(1)
	m.Gauge = &dto.Gauge{
		Value: &h,
	}

	var labels []*dto.LabelPair
	for i := range nam.labelNames {
		labels = append(labels, &dto.LabelPair{
			Name:  &nam.labelNames[i],
			Value: &nam.labelValues[i],
		})
	}

	labels = append(labels,
		&dto.LabelPair{
			Name:  toStringPtr("pod"),
			Value: &nam.pod,
		},
		&dto.LabelPair{
			Name:  toStringPtr("namespace"),
			Value: &nam.namespace,
		},
		&dto.LabelPair{
			Name:  toStringPtr("uid"),
			Value: &nam.uid,
		},
	)
	m.Label = labels
	return nil
}

//--------------------------------------------------------------------------
//  KubePodContainerStatusRestartsTotalMetric
//--------------------------------------------------------------------------

// KubePodContainerStatusRestartsTotalMetric is a prometheus.Metric emitting container restarts metrics.
type KubePodContainerStatusRestartsTotalMetric struct {
	fqName    string
	help      string
	pod       string
	namespace string
	container string
	uid       string
	value     float64
}

// Creates a new KubePodContainerStatusRestartsTotalMetric, implementation of prometheus.Metric
func newKubePodContainerStatusRestartsTotalMetric(fqname, namespace, pod, uid, container string, value float64) KubePodContainerStatusRestartsTotalMetric {
	return KubePodContainerStatusRestartsTotalMetric{
		fqName:    fqname,
		help:      "kube_pod_container_status_restarts_total total container restarts",
		pod:       pod,
		namespace: namespace,
		uid:       uid,
		container: container,
		value:     value,
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (kpcs KubePodContainerStatusRestartsTotalMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{
		"namespace": kpcs.namespace,
		"pod":       kpcs.pod,
		"uid":       kpcs.uid,
		"container": kpcs.container,
	}
	return prometheus.NewDesc(kpcs.fqName, kpcs.help, []string{}, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data transmission object.
func (kpcs KubePodContainerStatusRestartsTotalMetric) Write(m *dto.Metric) error {
	m.Counter = &dto.Counter{
		Value: &kpcs.value,
	}

	var labels []*dto.LabelPair
	labels = append(labels,
		&dto.LabelPair{
			Name:  toStringPtr("namespace"),
			Value: &kpcs.namespace,
		},
		&dto.LabelPair{
			Name:  toStringPtr("pod"),
			Value: &kpcs.pod,
		},
		&dto.LabelPair{
			Name:  toStringPtr("container"),
			Value: &kpcs.container,
		},
		&dto.LabelPair{
			Name:  toStringPtr("uid"),
			Value: &kpcs.uid,
		},
	)
	m.Label = labels
	return nil
}

//--------------------------------------------------------------------------
//  KubePodContainerStatusTerminatedReasonMetric
//--------------------------------------------------------------------------

// KubePodContainerStatusTerminatedReasonMetric is a prometheus.Metric emitting container termination reasons.
type KubePodContainerStatusTerminatedReasonMetric struct {
	fqName    string
	help      string
	pod       string
	namespace string
	container string
	uid       string
	reason    string
}

// Creates a new KubePodContainerStatusRestartsTotalMetric, implementation of prometheus.Metric
func newKubePodContainerStatusTerminatedReasonMetric(fqname, namespace, pod, uid, container, reason string) KubePodContainerStatusTerminatedReasonMetric {
	return KubePodContainerStatusTerminatedReasonMetric{
		fqName:    fqname,
		help:      "kube_pod_container_status_terminated_reason Describes the reason the container is currently in terminated state.",
		pod:       pod,
		namespace: namespace,
		uid:       uid,
		container: container,
		reason:    reason,
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (kpcs KubePodContainerStatusTerminatedReasonMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{
		"namespace": kpcs.namespace,
		"pod":       kpcs.pod,
		"uid":       kpcs.uid,
		"container": kpcs.container,
		"reason":    kpcs.reason,
	}
	return prometheus.NewDesc(kpcs.fqName, kpcs.help, []string{}, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data transmission object.
func (kpcs KubePodContainerStatusTerminatedReasonMetric) Write(m *dto.Metric) error {
	h := float64(1)
	m.Gauge = &dto.Gauge{
		Value: &h,
	}

	var labels []*dto.LabelPair
	labels = append(labels,
		&dto.LabelPair{
			Name:  toStringPtr("namespace"),
			Value: &kpcs.namespace,
		},
		&dto.LabelPair{
			Name:  toStringPtr("pod"),
			Value: &kpcs.pod,
		},
		&dto.LabelPair{
			Name:  toStringPtr("container"),
			Value: &kpcs.container,
		},
		&dto.LabelPair{
			Name:  toStringPtr("uid"),
			Value: &kpcs.uid,
		},
		&dto.LabelPair{
			Name:  toStringPtr("reason"),
			Value: &kpcs.reason,
		},
	)
	m.Label = labels
	return nil
}

//--------------------------------------------------------------------------
//  KubePodStatusPhaseMetric
//--------------------------------------------------------------------------

// KubePodStatusPhaseMetric is a prometheus.Metric emitting all phases for a pod
type KubePodStatusPhaseMetric struct {
	fqName    string
	help      string
	pod       string
	namespace string
	uid       string
	phase     string
	value     float64
}

// Creates a new KubePodContainerStatusRestartsTotalMetric, implementation of prometheus.Metric
func newKubePodStatusPhaseMetric(fqname, namespace, pod, uid, phase string, value float64) KubePodStatusPhaseMetric {
	return KubePodStatusPhaseMetric{
		fqName:    fqname,
		help:      "kube_pod_container_status_terminated_reason Describes the reason the container is currently in terminated state.",
		pod:       pod,
		namespace: namespace,
		uid:       uid,
		phase:     phase,
		value:     value,
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (kpcs KubePodStatusPhaseMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{
		"namespace": kpcs.namespace,
		"pod":       kpcs.pod,
		"uid":       kpcs.uid,
		"phase":     kpcs.phase,
	}
	return prometheus.NewDesc(kpcs.fqName, kpcs.help, []string{}, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data transmission object.
func (kpcs KubePodStatusPhaseMetric) Write(m *dto.Metric) error {
	m.Gauge = &dto.Gauge{
		Value: &kpcs.value,
	}

	var labels []*dto.LabelPair
	labels = append(labels,
		&dto.LabelPair{
			Name:  toStringPtr("namespace"),
			Value: &kpcs.namespace,
		},
		&dto.LabelPair{
			Name:  toStringPtr("pod"),
			Value: &kpcs.pod,
		},
		&dto.LabelPair{
			Name:  toStringPtr("uid"),
			Value: &kpcs.uid,
		},
		&dto.LabelPair{
			Name:  toStringPtr("phase"),
			Value: &kpcs.phase,
		},
	)
	m.Label = labels
	return nil
}

//--------------------------------------------------------------------------
//  KubePodContainerStatusRunningMetric
//--------------------------------------------------------------------------

// KubePodLabelsMetric is a prometheus.Metric used to encode
// a duplicate of the deprecated kube-state-metrics metric
// kube_pod_labels
type KubePodContainerStatusRunningMetric struct {
	fqName    string
	help      string
	pod       string
	namespace string
	container string
	uid       string
}

// Creates a new KubePodContainerStatusRunningMetric, implementation of prometheus.Metric
func newKubePodContainerStatusRunningMetric(fqname, namespace, pod, uid, container string) KubePodContainerStatusRunningMetric {
	return KubePodContainerStatusRunningMetric{
		fqName:    fqname,
		help:      "kube_pod_container_status_running pods container status",
		pod:       pod,
		namespace: namespace,
		uid:       uid,
		container: container,
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (kpcs KubePodContainerStatusRunningMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{
		"namespace": kpcs.namespace,
		"pod":       kpcs.pod,
		"uid":       kpcs.uid,
		"container": kpcs.container,
	}
	return prometheus.NewDesc(kpcs.fqName, kpcs.help, []string{}, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data
// transmission object.
func (kpcs KubePodContainerStatusRunningMetric) Write(m *dto.Metric) error {
	h := float64(1)
	m.Gauge = &dto.Gauge{
		Value: &h,
	}

	var labels []*dto.LabelPair
	labels = append(labels,
		&dto.LabelPair{
			Name:  toStringPtr("namespace"),
			Value: &kpcs.namespace,
		},
		&dto.LabelPair{
			Name:  toStringPtr("pod"),
			Value: &kpcs.pod,
		},
		&dto.LabelPair{
			Name:  toStringPtr("container"),
			Value: &kpcs.container,
		},
		&dto.LabelPair{
			Name:  toStringPtr("uid"),
			Value: &kpcs.uid,
		},
	)
	m.Label = labels
	return nil
}

//--------------------------------------------------------------------------
//  KubePodContainerResourceRequestMetric
//--------------------------------------------------------------------------

// KubePodContainerResourceRequestsMetric is a prometheus.Metric
type KubePodContainerResourceRequestsMetric struct {
	fqName    string
	help      string
	pod       string
	namespace string
	container string
	uid       string
	resource  string
	unit      string
	node      string
	value     float64
}

// Creates a new newKubePodContainerResourceRequestsMetric, implementation of prometheus.Metric
func newKubePodContainerResourceRequestsMetric(fqname, namespace, pod, uid, container, node, resource, unit string, value float64) KubePodContainerResourceRequestsMetric {
	return KubePodContainerResourceRequestsMetric{
		fqName:    fqname,
		help:      "kube_pod_container_resource_requests pods container resource requests",
		pod:       pod,
		namespace: namespace,
		uid:       uid,
		container: container,
		node:      node,
		resource:  resource,
		unit:      unit,
		value:     value,
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (kpcrr KubePodContainerResourceRequestsMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{
		"namespace": kpcrr.namespace,
		"pod":       kpcrr.pod,
		"uid":       kpcrr.uid,
		"container": kpcrr.container,
		"node":      kpcrr.node,
		"resource":  kpcrr.resource,
		"unit":      kpcrr.unit,
	}
	return prometheus.NewDesc(kpcrr.fqName, kpcrr.help, []string{}, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data
// transmission object.
func (kpcrr KubePodContainerResourceRequestsMetric) Write(m *dto.Metric) error {
	m.Gauge = &dto.Gauge{
		Value: &kpcrr.value,
	}

	m.Label = []*dto.LabelPair{
		{
			Name:  toStringPtr("namespace"),
			Value: &kpcrr.namespace,
		},
		{
			Name:  toStringPtr("pod"),
			Value: &kpcrr.pod,
		},
		{
			Name:  toStringPtr("container"),
			Value: &kpcrr.container,
		},
		{
			Name:  toStringPtr("uid"),
			Value: &kpcrr.uid,
		},
		{
			Name:  toStringPtr("node"),
			Value: &kpcrr.node,
		},
		{
			Name:  toStringPtr("resource"),
			Value: &kpcrr.resource,
		},
		{
			Name:  toStringPtr("unit"),
			Value: &kpcrr.unit,
		},
	}
	return nil
}

//--------------------------------------------------------------------------
//  KubePodContainerResourceLimitsMetric
//--------------------------------------------------------------------------

// KubePodContainerResourceLimitsMetric is a prometheus.Metric
type KubePodContainerResourceLimitsMetric struct {
	fqName    string
	help      string
	pod       string
	namespace string
	container string
	uid       string
	resource  string
	unit      string
	node      string
	value     float64
}

// Creates a new KubePodContainerResourceLimitsMetric, implementation of prometheus.Metric
func newKubePodContainerResourceLimitsMetric(fqname, namespace, pod, uid, container, node, resource, unit string, value float64) KubePodContainerResourceLimitsMetric {
	return KubePodContainerResourceLimitsMetric{
		fqName:    fqname,
		help:      "kube_pod_container_resource_limits pods container resource limits",
		pod:       pod,
		namespace: namespace,
		uid:       uid,
		container: container,
		node:      node,
		resource:  resource,
		unit:      unit,
		value:     value,
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (kpcrr KubePodContainerResourceLimitsMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{
		"namespace": kpcrr.namespace,
		"pod":       kpcrr.pod,
		"uid":       kpcrr.uid,
		"container": kpcrr.container,
		"node":      kpcrr.node,
		"resource":  kpcrr.resource,
		"unit":      kpcrr.unit,
	}
	return prometheus.NewDesc(kpcrr.fqName, kpcrr.help, []string{}, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data
// transmission object.
func (kpcrr KubePodContainerResourceLimitsMetric) Write(m *dto.Metric) error {
	m.Gauge = &dto.Gauge{
		Value: &kpcrr.value,
	}

	m.Label = []*dto.LabelPair{
		{
			Name:  toStringPtr("namespace"),
			Value: &kpcrr.namespace,
		},
		{
			Name:  toStringPtr("pod"),
			Value: &kpcrr.pod,
		},
		{
			Name:  toStringPtr("container"),
			Value: &kpcrr.container,
		},
		{
			Name:  toStringPtr("uid"),
			Value: &kpcrr.uid,
		},
		{
			Name:  toStringPtr("node"),
			Value: &kpcrr.node,
		},
		{
			Name:  toStringPtr("resource"),
			Value: &kpcrr.resource,
		},
		{
			Name:  toStringPtr("unit"),
			Value: &kpcrr.unit,
		},
	}
	return nil
}

//--------------------------------------------------------------------------
//  KubePodContainerResourceLimitsCPUCoresMetric (KSM v1)
//--------------------------------------------------------------------------

// KubePodContainerResourceLimitsCPUCoresMetric is a prometheus.Metric
type KubePodContainerResourceLimitsCPUCoresMetric struct {
	fqName    string
	help      string
	pod       string
	namespace string
	container string
	uid       string
	node      string
	value     float64
}

// Creates a new KubePodContainerResourceLimitsMetric, implementation of prometheus.Metric
func newKubePodContainerResourceLimitsCPUCoresMetric(fqname, namespace, pod, uid, container, node string, value float64) KubePodContainerResourceLimitsCPUCoresMetric {
	return KubePodContainerResourceLimitsCPUCoresMetric{
		fqName:    fqname,
		help:      "kube_pod_container_resource_limits_cpu_cores pods container cpu cores resource limits",
		pod:       pod,
		namespace: namespace,
		uid:       uid,
		container: container,
		node:      node,
		value:     value,
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (kpcrr KubePodContainerResourceLimitsCPUCoresMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{
		"namespace": kpcrr.namespace,
		"pod":       kpcrr.pod,
		"uid":       kpcrr.uid,
		"container": kpcrr.container,
		"node":      kpcrr.node,
	}
	return prometheus.NewDesc(kpcrr.fqName, kpcrr.help, []string{}, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data
// transmission object.
func (kpcrr KubePodContainerResourceLimitsCPUCoresMetric) Write(m *dto.Metric) error {
	m.Gauge = &dto.Gauge{
		Value: &kpcrr.value,
	}

	m.Label = []*dto.LabelPair{
		{
			Name:  toStringPtr("namespace"),
			Value: &kpcrr.namespace,
		},
		{
			Name:  toStringPtr("pod"),
			Value: &kpcrr.pod,
		},
		{
			Name:  toStringPtr("container"),
			Value: &kpcrr.container,
		},
		{
			Name:  toStringPtr("uid"),
			Value: &kpcrr.uid,
		},
		{
			Name:  toStringPtr("node"),
			Value: &kpcrr.node,
		},
	}
	return nil
}

//--------------------------------------------------------------------------
//  KubePodContainerResourceLimitsMemoryBytesMetric (KSM v1)
//--------------------------------------------------------------------------

// KubePodContainerResourceLimitsMemoryBytesMetric is a prometheus.Metric
type KubePodContainerResourceLimitsMemoryBytesMetric struct {
	fqName    string
	help      string
	pod       string
	namespace string
	container string
	uid       string
	node      string
	value     float64
}

// Creates a new KubePodContainerResourceLimitsMemoryBytesMetric, implementation of prometheus.Metric
func newKubePodContainerResourceLimitsMemoryBytesMetric(fqname, namespace, pod, uid, container, node string, value float64) KubePodContainerResourceLimitsMemoryBytesMetric {
	return KubePodContainerResourceLimitsMemoryBytesMetric{
		fqName:    fqname,
		help:      "kube_pod_container_resource_limits_memory_bytes pods container memory bytes resource limits",
		pod:       pod,
		namespace: namespace,
		uid:       uid,
		container: container,
		node:      node,
		value:     value,
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (kpcrr KubePodContainerResourceLimitsMemoryBytesMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{
		"namespace": kpcrr.namespace,
		"pod":       kpcrr.pod,
		"uid":       kpcrr.uid,
		"container": kpcrr.container,
		"node":      kpcrr.node,
	}
	return prometheus.NewDesc(kpcrr.fqName, kpcrr.help, []string{}, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data
// transmission object.
func (kpcrr KubePodContainerResourceLimitsMemoryBytesMetric) Write(m *dto.Metric) error {
	m.Gauge = &dto.Gauge{
		Value: &kpcrr.value,
	}

	m.Label = []*dto.LabelPair{
		{
			Name:  toStringPtr("namespace"),
			Value: &kpcrr.namespace,
		},
		{
			Name:  toStringPtr("pod"),
			Value: &kpcrr.pod,
		},
		{
			Name:  toStringPtr("container"),
			Value: &kpcrr.container,
		},
		{
			Name:  toStringPtr("uid"),
			Value: &kpcrr.uid,
		},
		{
			Name:  toStringPtr("node"),
			Value: &kpcrr.node,
		},
	}
	return nil
}

//--------------------------------------------------------------------------
//  KubePodOwnerMetric
//--------------------------------------------------------------------------

// KubePodOwnerMetric is a prometheus.Metric
type KubePodOwnerMetric struct {
	fqName            string
	help              string
	namespace         string
	pod               string
	ownerIsController bool
	ownerName         string
	ownerKind         string
}

// Creates a new KubePodOwnerMetric, implementation of prometheus.Metric
func newKubePodOwnerMetric(fqname, namespace, pod, ownerName, ownerKind string, ownerIsController bool) KubePodOwnerMetric {
	return KubePodOwnerMetric{
		fqName:            fqname,
		help:              "kube_pod_owner Information about the Pod's owner",
		namespace:         namespace,
		pod:               pod,
		ownerName:         ownerName,
		ownerKind:         ownerKind,
		ownerIsController: ownerIsController,
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (kpo KubePodOwnerMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{
		"namespace":           kpo.namespace,
		"pod":                 kpo.pod,
		"owner_name":          kpo.ownerName,
		"owner_kind":          kpo.ownerKind,
		"owner_is_controller": fmt.Sprintf("%t", kpo.ownerIsController),
	}
	return prometheus.NewDesc(kpo.fqName, kpo.help, []string{}, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data
// transmission object.
func (kpo KubePodOwnerMetric) Write(m *dto.Metric) error {
	v := float64(1.0)
	m.Gauge = &dto.Gauge{
		Value: &v,
	}

	m.Label = []*dto.LabelPair{
		{
			Name:  toStringPtr("namespace"),
			Value: &kpo.namespace,
		},
		{
			Name:  toStringPtr("pod"),
			Value: &kpo.pod,
		},
		{
			Name:  toStringPtr("owner_name"),
			Value: &kpo.ownerName,
		},
		{
			Name:  toStringPtr("owner_kind"),
			Value: &kpo.ownerKind,
		},
		{
			Name:  toStringPtr("owner_is_controller"),
			Value: toStringPtr(fmt.Sprintf("%t", kpo.ownerIsController)),
		},
	}
	return nil
}
