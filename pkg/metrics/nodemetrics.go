package metrics

import (
	"strings"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util/promutil"
	"github.com/opencost/opencost/pkg/clustercache"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	v1 "k8s.io/api/core/v1"
)

var (
	conditionStatuses = []v1.ConditionStatus{v1.ConditionTrue, v1.ConditionFalse, v1.ConditionUnknown}
)

//--------------------------------------------------------------------------
//  KubeNodeCollector
//--------------------------------------------------------------------------

// KubeNodeCollector is a prometheus collector that generates node sourced metrics.
type KubeNodeCollector struct {
	KubeClusterCache clustercache.ClusterCache
	metricsConfig    MetricsConfig
}

// Describe sends the super-set of all possible descriptors of metrics
// collected by this Collector.
func (nsac KubeNodeCollector) Describe(ch chan<- *prometheus.Desc) {
	disabledMetrics := nsac.metricsConfig.GetDisabledMetricsMap()

	if _, disabled := disabledMetrics["kube_node_status_capacity"]; !disabled {
		ch <- prometheus.NewDesc("kube_node_status_capacity", "Node resource capacity.", []string{}, nil)
	}
	if _, disabled := disabledMetrics["kube_node_status_capacity_memory_bytes"]; !disabled {
		ch <- prometheus.NewDesc("kube_node_status_capacity_memory_bytes", "node capacity memory bytes", []string{}, nil)
	}
	if _, disabled := disabledMetrics["kube_node_status_capacity_cpu_cores"]; !disabled {
		ch <- prometheus.NewDesc("kube_node_status_capacity_cpu_cores", "node capacity cpu cores", []string{}, nil)
	}
	if _, disabled := disabledMetrics["kube_node_status_allocatable"]; !disabled {
		ch <- prometheus.NewDesc("kube_node_status_allocatable", "The allocatable for different resources of a node that are available for scheduling.", []string{}, nil)
	}
	if _, disabled := disabledMetrics["kube_node_status_allocatable_cpu_cores"]; !disabled {
		ch <- prometheus.NewDesc("kube_node_status_allocatable_cpu_cores", "The allocatable cpu cores.", []string{}, nil)
	}
	if _, disabled := disabledMetrics["kube_node_status_allocatable_memory_bytes"]; !disabled {
		ch <- prometheus.NewDesc("kube_node_status_allocatable_memory_bytes", "The allocatable memory in bytes.", []string{}, nil)
	}
	if _, disabled := disabledMetrics["kube_node_labels"]; !disabled {
		ch <- prometheus.NewDesc("kube_node_labels", "all labels for each node prefixed with label_", []string{}, nil)
	}
	if _, disabled := disabledMetrics["kube_node_status_condition"]; !disabled {
		ch <- prometheus.NewDesc("kube_node_status_condition", "The condition of a cluster node.", []string{}, nil)
	}
}

// Collect is called by the Prometheus registry when collecting metrics.
func (nsac KubeNodeCollector) Collect(ch chan<- prometheus.Metric) {
	nodes := nsac.KubeClusterCache.GetAllNodes()
	disabledMetrics := nsac.metricsConfig.GetDisabledMetricsMap()

	for _, node := range nodes {
		nodeName := node.Name

		// Node Capacity
		for resourceName, quantity := range node.Status.Capacity {
			resource, unit, value := toResourceUnitValue(resourceName, quantity)

			// failed to parse the resource type
			if resource == "" {
				log.DedupedWarningf(5, "Failed to parse resource units and quantity for resource: %s", resourceName)
				continue
			}

			// KSM v1 Emission
			if _, disabled := disabledMetrics["kube_node_status_capacity_cpu_cores"]; !disabled {
				if resource == "cpu" {
					ch <- newKubeNodeStatusCapacityCPUCoresMetric("kube_node_status_capacity_cpu_cores", nodeName, value)

				}
			}
			if _, disabled := disabledMetrics["kube_node_status_capacity_memory_bytes"]; !disabled {
				if resource == "memory" {
					ch <- newKubeNodeStatusCapacityMemoryBytesMetric("kube_node_status_capacity_memory_bytes", nodeName, value)
				}
			}

			if _, disabled := disabledMetrics["kube_node_status_capacity"]; !disabled {
				ch <- newKubeNodeStatusCapacityMetric("kube_node_status_capacity", nodeName, resource, unit, value)
			}
		}

		// Node Allocatable Resources
		for resourceName, quantity := range node.Status.Allocatable {
			resource, unit, value := toResourceUnitValue(resourceName, quantity)

			// failed to parse the resource type
			if resource == "" {
				log.DedupedWarningf(5, "Failed to parse resource units and quantity for resource: %s", resourceName)
				continue
			}

			// KSM v1 Emission
			if _, disabled := disabledMetrics["kube_node_status_allocatable_cpu_cores"]; !disabled {
				if resource == "cpu" {
					ch <- newKubeNodeStatusAllocatableCPUCoresMetric("kube_node_status_allocatable_cpu_cores", nodeName, value)
				}
			}
			if _, disabled := disabledMetrics["kube_node_status_allocatable_memory_bytes"]; !disabled {
				if resource == "memory" {
					ch <- newKubeNodeStatusAllocatableMemoryBytesMetric("kube_node_status_allocatable_memory_bytes", nodeName, value)
				}
			}
			if _, disabled := disabledMetrics["kube_node_status_allocatable"]; !disabled {
				ch <- newKubeNodeStatusAllocatableMetric("kube_node_status_allocatable", nodeName, resource, unit, value)
			}
		}

		// node labels
		if _, disabled := disabledMetrics["kube_node_labels"]; !disabled {
			labelNames, labelValues := promutil.KubePrependQualifierToLabels(promutil.SanitizeLabels(node.Labels), "label_")
			ch <- newKubeNodeLabelsMetric(nodeName, "kube_node_labels", labelNames, labelValues)
		}

		// kube_node_status_condition
		// Collect node conditions and while default to false.
		if _, disabled := disabledMetrics["kube_node_status_condition"]; !disabled {
			for _, c := range node.Status.Conditions {
				conditions := getConditions(c.Status)

				for _, cond := range conditions {
					ch <- newKubeNodeStatusConditionMetric(nodeName, "kube_node_status_condition", string(c.Type), cond.status, cond.value)
				}
			}
		}
	}
}

//--------------------------------------------------------------------------
//  KubeNodeStatusCapacityMetric
//--------------------------------------------------------------------------

// KubeNodeStatusCapacityMetric is a prometheus.Metric
type KubeNodeStatusCapacityMetric struct {
	fqName   string
	help     string
	resource string
	unit     string
	node     string
	value    float64
}

// Creates a new KubeNodeStatusCapacityMetric, implementation of prometheus.Metric
func newKubeNodeStatusCapacityMetric(fqname, node, resource, unit string, value float64) KubeNodeStatusCapacityMetric {
	return KubeNodeStatusCapacityMetric{
		fqName:   fqname,
		help:     "kube_node_status_capacity node capacity",
		node:     node,
		resource: resource,
		unit:     unit,
		value:    value,
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (kpcrr KubeNodeStatusCapacityMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{
		"node":     kpcrr.node,
		"resource": kpcrr.resource,
		"unit":     kpcrr.unit,
	}
	return prometheus.NewDesc(kpcrr.fqName, kpcrr.help, []string{}, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data transmission object.
func (kpcrr KubeNodeStatusCapacityMetric) Write(m *dto.Metric) error {
	m.Gauge = &dto.Gauge{
		Value: &kpcrr.value,
	}

	m.Label = []*dto.LabelPair{
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
//  KubeNodeStatusCapacityMemoryBytesMetric
//--------------------------------------------------------------------------

// KubeNodeStatusCapacityMemoryBytesMetric is a prometheus.Metric used to encode
// a duplicate of the deprecated kube-state-metrics metric
// kube_node_status_capacity_memory_bytes
type KubeNodeStatusCapacityMemoryBytesMetric struct {
	fqName string
	help   string
	bytes  float64
	node   string
}

// Creates a new KubeNodeStatusCapacityMemoryBytesMetric, implementation of prometheus.Metric
func newKubeNodeStatusCapacityMemoryBytesMetric(fqname string, node string, bytes float64) KubeNodeStatusCapacityMemoryBytesMetric {
	return KubeNodeStatusCapacityMemoryBytesMetric{
		fqName: fqname,
		help:   "kube_node_status_capacity_memory_bytes Node Capacity Memory Bytes",
		node:   node,
		bytes:  bytes,
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (nam KubeNodeStatusCapacityMemoryBytesMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{"node": nam.node}
	return prometheus.NewDesc(nam.fqName, nam.help, []string{}, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data
// transmission object.
func (nam KubeNodeStatusCapacityMemoryBytesMetric) Write(m *dto.Metric) error {
	m.Gauge = &dto.Gauge{
		Value: &nam.bytes,
	}
	m.Label = []*dto.LabelPair{
		{
			Name:  toStringPtr("node"),
			Value: &nam.node,
		},
	}
	return nil
}

//--------------------------------------------------------------------------
//  KubeNodeStatusCapacityCPUCoresMetric
//--------------------------------------------------------------------------

// KubeNodeStatusCapacityCPUCoresMetric is a prometheus.Metric used to encode
// a duplicate of the deprecated kube-state-metrics metric
// kube_node_status_capacity_memory_bytes
type KubeNodeStatusCapacityCPUCoresMetric struct {
	fqName string
	help   string
	cores  float64
	node   string
}

// Creates a new KubeNodeStatusCapacityCPUCoresMetric, implementation of prometheus.Metric
func newKubeNodeStatusCapacityCPUCoresMetric(fqname string, node string, cores float64) KubeNodeStatusCapacityCPUCoresMetric {
	return KubeNodeStatusCapacityCPUCoresMetric{
		fqName: fqname,
		help:   "kube_node_status_capacity_cpu_cores Node Capacity CPU Cores",
		cores:  cores,
		node:   node,
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (nam KubeNodeStatusCapacityCPUCoresMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{"node": nam.node}
	return prometheus.NewDesc(nam.fqName, nam.help, []string{}, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data
// transmission object.
func (nam KubeNodeStatusCapacityCPUCoresMetric) Write(m *dto.Metric) error {
	m.Gauge = &dto.Gauge{
		Value: &nam.cores,
	}
	m.Label = []*dto.LabelPair{
		{
			Name:  toStringPtr("node"),
			Value: &nam.node,
		},
	}
	return nil
}

//--------------------------------------------------------------------------
//  KubeNodeLabelsMetric
//--------------------------------------------------------------------------

// KubeNodeLabelsMetric is a prometheus.Metric used to encode
// a duplicate of the deprecated kube-state-metrics metric
// kube_node_labels
type KubeNodeLabelsMetric struct {
	fqName      string
	help        string
	labelNames  []string
	labelValues []string
	node        string
}

// Creates a new KubeNodeLabelsMetric, implementation of prometheus.Metric
func newKubeNodeLabelsMetric(node string, fqname string, labelNames []string, labelValues []string) KubeNodeLabelsMetric {
	return KubeNodeLabelsMetric{
		fqName:      fqname,
		labelNames:  labelNames,
		labelValues: labelValues,
		help:        "kube_node_labels all labels for each node prefixed with label_",
		node:        node,
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (nam KubeNodeLabelsMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{
		"node": nam.node,
	}
	return prometheus.NewDesc(nam.fqName, nam.help, nam.labelNames, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data
// transmission object.
func (nam KubeNodeLabelsMetric) Write(m *dto.Metric) error {
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

	nodeString := "node"
	labels = append(labels, &dto.LabelPair{Name: &nodeString, Value: &nam.node})
	m.Label = labels
	return nil
}

//--------------------------------------------------------------------------
//  KubeNodeStatusConditionMetric
//--------------------------------------------------------------------------

// KubeNodeStatusConditionMetric
type KubeNodeStatusConditionMetric struct {
	fqName    string
	help      string
	node      string
	condition string
	status    string
	value     float64
}

// Creates a new KubeNodeStatusConditionMetric, implementation of prometheus.Metric
func newKubeNodeStatusConditionMetric(node, fqname, condition, status string, value float64) KubeNodeStatusConditionMetric {
	return KubeNodeStatusConditionMetric{
		fqName:    fqname,
		help:      "kube_node_status_condition condition status for nodes",
		node:      node,
		condition: condition,
		status:    status,
		value:     value,
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (nam KubeNodeStatusConditionMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{
		"node":      nam.node,
		"condition": nam.condition,
		"status":    nam.status,
	}
	return prometheus.NewDesc(nam.fqName, nam.help, []string{}, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data
// transmission object.
func (nam KubeNodeStatusConditionMetric) Write(m *dto.Metric) error {
	m.Gauge = &dto.Gauge{
		Value: &nam.value,
	}
	m.Label = []*dto.LabelPair{
		{
			Name:  toStringPtr("node"),
			Value: &nam.node,
		},
		{
			Name:  toStringPtr("condition"),
			Value: &nam.condition,
		},
		{
			Name:  toStringPtr("status"),
			Value: &nam.status,
		},
	}
	return nil
}

// helper type for status condition reporting and metric rollup
type statusCondition struct {
	status string
	value  float64
}

// retrieves the total status conditions and the comparison to the provided condition
func getConditions(cs v1.ConditionStatus) []*statusCondition {
	ms := make([]*statusCondition, len(conditionStatuses))

	for i, status := range conditionStatuses {
		ms[i] = &statusCondition{
			status: strings.ToLower(string(status)),
			value:  boolFloat64(cs == status),
		}
	}

	return ms
}

//--------------------------------------------------------------------------
//  KubeNodeStatusAllocatableMetric
//--------------------------------------------------------------------------

// KubeNodeStatusAllocatableMetric is a prometheus.Metric
type KubeNodeStatusAllocatableMetric struct {
	fqName   string
	help     string
	resource string
	unit     string
	node     string
	value    float64
}

// Creates a new KubeNodeStatusAllocatableMetric, implementation of prometheus.Metric
func newKubeNodeStatusAllocatableMetric(fqname, node, resource, unit string, value float64) KubeNodeStatusAllocatableMetric {
	return KubeNodeStatusAllocatableMetric{
		fqName:   fqname,
		help:     "kube_node_status_allocatable node allocatable",
		node:     node,
		resource: resource,
		unit:     unit,
		value:    value,
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (kpcrr KubeNodeStatusAllocatableMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{
		"node":     kpcrr.node,
		"resource": kpcrr.resource,
		"unit":     kpcrr.unit,
	}
	return prometheus.NewDesc(kpcrr.fqName, kpcrr.help, []string{}, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data transmission object.
func (kpcrr KubeNodeStatusAllocatableMetric) Write(m *dto.Metric) error {
	m.Gauge = &dto.Gauge{
		Value: &kpcrr.value,
	}

	m.Label = []*dto.LabelPair{
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
//  KubeNodeStatusAllocatableCPUCoresMetric
//--------------------------------------------------------------------------

// KubeNodeStatusAllocatableCPUCoresMetric is a prometheus.Metric
type KubeNodeStatusAllocatableCPUCoresMetric struct {
	fqName   string
	help     string
	resource string
	unit     string
	node     string
	value    float64
}

// Creates a new KubeNodeStatusAllocatableCPUCoresMetric, implementation of prometheus.Metric
func newKubeNodeStatusAllocatableCPUCoresMetric(fqname, node string, value float64) KubeNodeStatusAllocatableCPUCoresMetric {
	return KubeNodeStatusAllocatableCPUCoresMetric{
		fqName: fqname,
		help:   "kube_node_status_allocatable_cpu_cores node allocatable cpu cores",
		node:   node,
		value:  value,
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (kpcrr KubeNodeStatusAllocatableCPUCoresMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{
		"node": kpcrr.node,
	}
	return prometheus.NewDesc(kpcrr.fqName, kpcrr.help, []string{}, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data transmission object.
func (kpcrr KubeNodeStatusAllocatableCPUCoresMetric) Write(m *dto.Metric) error {
	m.Gauge = &dto.Gauge{
		Value: &kpcrr.value,
	}

	m.Label = []*dto.LabelPair{
		{
			Name:  toStringPtr("node"),
			Value: &kpcrr.node,
		},
	}
	return nil
}

//--------------------------------------------------------------------------
//  KubeNodeStatusAllocatableMemoryBytesMetric
//--------------------------------------------------------------------------

// KubeNodeStatusAllocatableMemoryBytesMetric is a prometheus.Metric
type KubeNodeStatusAllocatableMemoryBytesMetric struct {
	fqName   string
	help     string
	resource string
	unit     string
	node     string
	value    float64
}

// Creates a new KubeNodeStatusAllocatableMemoryBytesMetric, implementation of prometheus.Metric
func newKubeNodeStatusAllocatableMemoryBytesMetric(fqname, node string, value float64) KubeNodeStatusAllocatableMemoryBytesMetric {
	return KubeNodeStatusAllocatableMemoryBytesMetric{
		fqName: fqname,
		help:   "kube_node_status_allocatable_memory_bytes node allocatable memory in bytes",
		node:   node,
		value:  value,
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (kpcrr KubeNodeStatusAllocatableMemoryBytesMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{
		"node": kpcrr.node,
	}
	return prometheus.NewDesc(kpcrr.fqName, kpcrr.help, []string{}, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data transmission object.
func (kpcrr KubeNodeStatusAllocatableMemoryBytesMetric) Write(m *dto.Metric) error {
	m.Gauge = &dto.Gauge{
		Value: &kpcrr.value,
	}

	m.Label = []*dto.LabelPair{
		{
			Name:  toStringPtr("node"),
			Value: &kpcrr.node,
		},
	}
	return nil
}
