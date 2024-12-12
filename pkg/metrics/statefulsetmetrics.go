package metrics

import (
	"github.com/opencost/opencost/core/pkg/util/promutil"
	"github.com/opencost/opencost/pkg/clustercache"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

//--------------------------------------------------------------------------
//  KubecostStatefulsetCollector
//--------------------------------------------------------------------------

// StatefulsetCollector is a prometheus collector that generates StatefulsetMetrics
type KubecostStatefulsetCollector struct {
	KubeClusterCache clustercache.ClusterCache
	metricsConfig    MetricsConfig
}

// Describe sends the super-set of all possible descriptors of metrics
// collected by this Collector.
func (sc KubecostStatefulsetCollector) Describe(ch chan<- *prometheus.Desc) {
	disabledMetrics := sc.metricsConfig.GetDisabledMetricsMap()
	if _, disabled := disabledMetrics["statefulSet_match_labels"]; disabled {
		return
	}

	ch <- prometheus.NewDesc("statefulSet_match_labels", "statfulSet match labels", []string{}, nil)
}

// Collect is called by the Prometheus registry when collecting metrics.
func (sc KubecostStatefulsetCollector) Collect(ch chan<- prometheus.Metric) {
	disabledMetrics := sc.metricsConfig.GetDisabledMetricsMap()
	if _, disabled := disabledMetrics["statefulSet_match_labels"]; disabled {
		return
	}

	ds := sc.KubeClusterCache.GetAllStatefulSets()
	for _, statefulset := range ds {
		statefulsetName := statefulset.Name
		statefulsetNS := statefulset.Namespace

		labels, values := promutil.KubeLabelsToLabels(promutil.SanitizeLabels(statefulset.SpecSelector.MatchLabels))
		if len(labels) > 0 {
			m := newStatefulsetMatchLabelsMetric(statefulsetName, statefulsetNS, "statefulSet_match_labels", labels, values)
			ch <- m
		}
	}

}

//--------------------------------------------------------------------------
//  StatefulsetMatchLabelsMetric
//--------------------------------------------------------------------------

// StatefulsetMetric is a prometheus.Metric used to encode statefulset match labels
type StatefulsetMatchLabelsMetric struct {
	fqName          string
	help            string
	labelNames      []string
	labelValues     []string
	statefulsetName string
	namespace       string
}

// Creates a new StatefulsetMetric, implementation of prometheus.Metric
func newStatefulsetMatchLabelsMetric(name, namespace, fqname string, labelNames, labelvalues []string) StatefulsetMatchLabelsMetric {
	return StatefulsetMatchLabelsMetric{
		fqName:          fqname,
		labelNames:      labelNames,
		labelValues:     labelvalues,
		help:            "statefulSet_match_labels StatefulSet Match Labels",
		statefulsetName: name,
		namespace:       namespace,
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (s StatefulsetMatchLabelsMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{
		"statefulSet": s.statefulsetName,
		"namespace":   s.namespace,
	}
	return prometheus.NewDesc(s.fqName, s.help, s.labelNames, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data
// transmission object.
func (s StatefulsetMatchLabelsMetric) Write(m *dto.Metric) error {
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
		Name:  toStringPtr("statefulSet"),
		Value: &s.statefulsetName,
	})
	m.Label = labels
	return nil
}
