package metrics

import (
	"github.com/opencost/opencost/core/pkg/util/promutil"
	"github.com/opencost/opencost/pkg/clustercache"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

//--------------------------------------------------------------------------
//  KubecostNamespaceCollector
//--------------------------------------------------------------------------

// KubecostNamespaceCollector is a prometheus collector that generates namespace sourced metrics
type KubecostNamespaceCollector struct {
	KubeClusterCache clustercache.ClusterCache
	metricsConfig    MetricsConfig
}

// Describe sends the super-set of all possible descriptors of metrics
// collected by this Collector.
func (nsac KubecostNamespaceCollector) Describe(ch chan<- *prometheus.Desc) {
	disabledMetrics := nsac.metricsConfig.GetDisabledMetricsMap()
	if _, disabled := disabledMetrics["kube_namespace_annotations"]; disabled {
		return
	}

	ch <- prometheus.NewDesc("kube_namespace_annotations", "namespace annotations", []string{}, nil)

}

// Collect is called by the Prometheus registry when collecting metrics.
func (nsac KubecostNamespaceCollector) Collect(ch chan<- prometheus.Metric) {
	disabledMetrics := nsac.metricsConfig.GetDisabledMetricsMap()
	if _, disabled := disabledMetrics["kube_namespace_annotations"]; disabled {
		return
	}

	namespaces := nsac.KubeClusterCache.GetAllNamespaces()
	for _, namespace := range namespaces {
		nsName := namespace.Name

		labels, values := promutil.KubeAnnotationsToLabels(namespace.Annotations)
		if len(labels) > 0 {
			m := newNamespaceAnnotationsMetric("kube_namespace_annotations", nsName, labels, values)
			ch <- m
		}
	}

}

//--------------------------------------------------------------------------
//  NamespaceAnnotationsMetric
//--------------------------------------------------------------------------

// NamespaceAnnotationsMetric is a prometheus.Metric used to encode namespace annotations
type NamespaceAnnotationsMetric struct {
	fqName      string
	help        string
	namespace   string
	labelNames  []string
	labelValues []string
}

// Creates a new NamespaceAnnotationsMetric, implementation of prometheus.Metric
func newNamespaceAnnotationsMetric(fqname, namespace string, labelNames []string, labelValues []string) NamespaceAnnotationsMetric {
	return NamespaceAnnotationsMetric{
		fqName:      fqname,
		help:        "kube_namespace_annotations Namespace Annotations",
		namespace:   namespace,
		labelNames:  labelNames,
		labelValues: labelValues,
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (nam NamespaceAnnotationsMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{
		"namespace": nam.namespace,
	}
	return prometheus.NewDesc(nam.fqName, nam.help, []string{}, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data
// transmission object.
func (nam NamespaceAnnotationsMetric) Write(m *dto.Metric) error {
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
	labels = append(labels, &dto.LabelPair{
		Name:  toStringPtr("namespace"),
		Value: &nam.namespace,
	})
	m.Label = labels
	return nil
}

//--------------------------------------------------------------------------
//  KubeNamespaceCollector
//--------------------------------------------------------------------------

// KubeNamespaceCollector is a prometheus collector that generates namespace sourced metrics
type KubeNamespaceCollector struct {
	KubeClusterCache clustercache.ClusterCache
	metricsConfig    MetricsConfig
}

// Describe sends the super-set of all possible descriptors of metrics
// collected by this Collector.
func (nsac KubeNamespaceCollector) Describe(ch chan<- *prometheus.Desc) {
	disabledMetrics := nsac.metricsConfig.GetDisabledMetricsMap()
	if _, disabled := disabledMetrics["kube_namespace_labels"]; disabled {
		return
	}

	ch <- prometheus.NewDesc("kube_namespace_labels", "namespace labels", []string{}, nil)

}

// Collect is called by the Prometheus registry when collecting metrics.
func (nsac KubeNamespaceCollector) Collect(ch chan<- prometheus.Metric) {
	disabledMetrics := nsac.metricsConfig.GetDisabledMetricsMap()
	if _, disabled := disabledMetrics["kube_namespace_labels"]; disabled {
		return
	}

	namespaces := nsac.KubeClusterCache.GetAllNamespaces()
	for _, namespace := range namespaces {
		nsName := namespace.Name

		labels, values := promutil.KubeLabelsToLabels(promutil.SanitizeLabels(namespace.Labels))
		if len(labels) > 0 {
			m := newNamespaceAnnotationsMetric("kube_namespace_labels", nsName, labels, values)
			ch <- m
		}
	}

}

//--------------------------------------------------------------------------
//  NamespaceAnnotationsMetric
//--------------------------------------------------------------------------

// NamespaceAnnotationsMetric is a prometheus.Metric used to encode namespace annotations
type KubeNamespaceLabelsMetric struct {
	fqName      string
	help        string
	namespace   string
	labelNames  []string
	labelValues []string
}

// Creates a new KubeNamespaceLabelsMetric, implementation of prometheus.Metric
func newKubeNamespaceLabelsMetric(fqname, namespace string, labelNames []string, labelValues []string) KubeNamespaceLabelsMetric {
	return KubeNamespaceLabelsMetric{
		namespace:   namespace,
		fqName:      fqname,
		labelNames:  labelNames,
		labelValues: labelValues,
		help:        "kube_namespace_labels Namespace Labels",
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (nam KubeNamespaceLabelsMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{
		"namespace": nam.namespace,
	}
	return prometheus.NewDesc(nam.fqName, nam.help, []string{}, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data transmission object.
func (nam KubeNamespaceLabelsMetric) Write(m *dto.Metric) error {
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
	labels = append(labels, &dto.LabelPair{
		Name:  toStringPtr("namespace"),
		Value: &nam.namespace,
	})
	m.Label = labels
	return nil
}
