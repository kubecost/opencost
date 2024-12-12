package metrics

import (
	"github.com/opencost/opencost/core/pkg/util/promutil"
	"github.com/opencost/opencost/pkg/clustercache"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

//--------------------------------------------------------------------------
//  KubecostServiceCollector
//--------------------------------------------------------------------------

// KubecostServiceCollector is a prometheus collector that generates service sourced metrics.
type KubecostServiceCollector struct {
	KubeClusterCache clustercache.ClusterCache
	metricsConfig    MetricsConfig
}

// Describe sends the super-set of all possible descriptors of metrics
// collected by this Collector.
func (sc KubecostServiceCollector) Describe(ch chan<- *prometheus.Desc) {
	disabledMetrics := sc.metricsConfig.GetDisabledMetricsMap()
	if _, disabled := disabledMetrics["service_selector_labels"]; disabled {
		return
	}

	ch <- prometheus.NewDesc("service_selector_labels", "service selector labels", []string{}, nil)

}

// Collect is called by the Prometheus registry when collecting metrics.
func (sc KubecostServiceCollector) Collect(ch chan<- prometheus.Metric) {
	disabledMetrics := sc.metricsConfig.GetDisabledMetricsMap()
	if _, disabled := disabledMetrics["service_selector_labels"]; disabled {
		return
	}

	svcs := sc.KubeClusterCache.GetAllServices()
	for _, svc := range svcs {
		serviceName := svc.Name
		serviceNS := svc.Namespace

		labels, values := promutil.KubeLabelsToLabels(promutil.SanitizeLabels(svc.SpecSelector))
		if len(labels) > 0 {
			m := newServiceSelectorLabelsMetric(serviceName, serviceNS, "service_selector_labels", labels, values)
			ch <- m
		}
	}

}

//--------------------------------------------------------------------------
//  ServiceSelectorLabelsMetric
//--------------------------------------------------------------------------

// ServiceSelectorLabelsMetric is a prometheus.Metric used to encode service selector labels
type ServiceSelectorLabelsMetric struct {
	fqName      string
	help        string
	labelNames  []string
	labelValues []string
	serviceName string
	namespace   string
}

// Creates a new ServiceMetric, implementation of prometheus.Metric
func newServiceSelectorLabelsMetric(name, namespace, fqname string, labelNames, labelvalues []string) ServiceSelectorLabelsMetric {
	return ServiceSelectorLabelsMetric{
		fqName:      fqname,
		labelNames:  labelNames,
		labelValues: labelvalues,
		help:        "service_selector_labels Service Selector Labels",
		serviceName: name,
		namespace:   namespace,
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (s ServiceSelectorLabelsMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{
		"service":   s.serviceName,
		"namespace": s.namespace,
	}
	return prometheus.NewDesc(s.fqName, s.help, s.labelNames, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data
// transmission object.
func (s ServiceSelectorLabelsMetric) Write(m *dto.Metric) error {
	h := float64(1)
	m.Gauge = &dto.Gauge{
		Value: &h,
	}
	var labels []*dto.LabelPair
	for i := range s.labelNames {
		labels = append(labels, &dto.LabelPair{
			Name:  &s.labelNames[i],
			Value: &s.labelValues[i],
		})
	}
	labels = append(labels, &dto.LabelPair{
		Name:  toStringPtr("namespace"),
		Value: &s.namespace,
	})
	labels = append(labels, &dto.LabelPair{
		Name:  toStringPtr("service"),
		Value: &s.serviceName,
	})
	m.Label = labels
	return nil
}
