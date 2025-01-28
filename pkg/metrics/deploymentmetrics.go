package metrics

import (
	"github.com/opencost/opencost/core/pkg/util/promutil"
	"github.com/opencost/opencost/pkg/clustercache"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

//--------------------------------------------------------------------------
//  KubecostDeploymentCollector
//--------------------------------------------------------------------------

// KubecostDeploymentCollector is a prometheus collector that generates kubecost
// specific deployment metrics.
type KubecostDeploymentCollector struct {
	KubeClusterCache clustercache.ClusterCache
	metricsConfig    MetricsConfig
}

// Describe sends the super-set of all possible descriptors of metrics
// collected by this Collector.
func (kdc KubecostDeploymentCollector) Describe(ch chan<- *prometheus.Desc) {
	disabledMetrics := kdc.metricsConfig.GetDisabledMetricsMap()
	if _, disabled := disabledMetrics["deployment_match_labels"]; disabled {
		return
	}

	ch <- prometheus.NewDesc("deployment_match_labels", "deployment match labels", []string{}, nil)
}

// Collect is called by the Prometheus registry when collecting metrics.
func (kdc KubecostDeploymentCollector) Collect(ch chan<- prometheus.Metric) {
	disabledMetrics := kdc.metricsConfig.GetDisabledMetricsMap()
	if _, disabled := disabledMetrics["deployment_match_labels"]; disabled {
		return
	}

	ds := kdc.KubeClusterCache.GetAllDeployments()
	for _, deployment := range ds {
		deploymentName := deployment.Name
		deploymentNS := deployment.Namespace

		labels, values := promutil.KubeLabelsToLabels(promutil.SanitizeLabels(deployment.MatchLabels))
		if len(labels) > 0 {
			m := newDeploymentMatchLabelsMetric(deploymentName, deploymentNS, "deployment_match_labels", labels, values)
			ch <- m
		}
	}

}

//--------------------------------------------------------------------------
//  DeploymentMatchLabelsMetric
//--------------------------------------------------------------------------

// DeploymentMatchLabelsMetric is a prometheus.Metric used to encode deployment match labels
type DeploymentMatchLabelsMetric struct {
	fqName         string
	help           string
	labelNames     []string
	labelValues    []string
	deploymentName string
	namespace      string
}

// Creates a new DeploymentMatchLabelsMetric, implementation of prometheus.Metric
func newDeploymentMatchLabelsMetric(name, namespace, fqname string, labelNames, labelvalues []string) DeploymentMatchLabelsMetric {
	return DeploymentMatchLabelsMetric{
		fqName:         fqname,
		labelNames:     labelNames,
		labelValues:    labelvalues,
		help:           "deployment_match_labels Deployment Match Labels",
		deploymentName: name,
		namespace:      namespace,
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (dmlm DeploymentMatchLabelsMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{
		"deployment": dmlm.deploymentName,
		"namespace":  dmlm.namespace,
	}
	return prometheus.NewDesc(dmlm.fqName, dmlm.help, dmlm.labelNames, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data
// transmission object.
func (dmlm DeploymentMatchLabelsMetric) Write(m *dto.Metric) error {
	h := float64(1)
	m.Gauge = &dto.Gauge{
		Value: &h,
	}
	var labels []*dto.LabelPair
	for i := range dmlm.labelNames {
		labels = append(labels, &dto.LabelPair{
			Name:  &dmlm.labelNames[i],
			Value: &dmlm.labelValues[i],
		})
	}
	labels = append(labels, &dto.LabelPair{
		Name:  toStringPtr("namespace"),
		Value: &dmlm.namespace,
	})
	labels = append(labels, &dto.LabelPair{
		Name:  toStringPtr("deployment"),
		Value: &dmlm.deploymentName,
	})
	m.Label = labels
	return nil
}

//--------------------------------------------------------------------------
//  KubeDeploymentCollector
//--------------------------------------------------------------------------

// KubeDeploymentCollector is a prometheus collector that generates
type KubeDeploymentCollector struct {
	KubeClusterCache clustercache.ClusterCache
	metricsConfig    MetricsConfig
}

// Describe sends the super-set of all possible descriptors of metrics
// collected by this Collector.
func (kdc KubeDeploymentCollector) Describe(ch chan<- *prometheus.Desc) {
	disabledMetrics := kdc.metricsConfig.GetDisabledMetricsMap()

	if _, disabled := disabledMetrics["kube_deployment_spec_replicas"]; !disabled {
		ch <- prometheus.NewDesc("kube_deployment_spec_replicas", "Number of desired pods for a deployment.", []string{}, nil)
	}
	if _, disabled := disabledMetrics["kube_deployment_status_replicas_available"]; !disabled {
		ch <- prometheus.NewDesc("kube_deployment_status_replicas_available", "The number of available replicas per deployment.", []string{}, nil)
	}

}

// Collect is called by the Prometheus registry when collecting metrics.
func (kdc KubeDeploymentCollector) Collect(ch chan<- prometheus.Metric) {
	deployments := kdc.KubeClusterCache.GetAllDeployments()
	disabledMetrics := kdc.metricsConfig.GetDisabledMetricsMap()

	for _, deployment := range deployments {
		deploymentName := deployment.Name
		deploymentNS := deployment.Namespace

		// Replicas Defined
		var replicas int32
		if deployment.SpecReplicas == nil {
			replicas = 1 // defaults to 1, documented on the 'Replicas' field
		} else {
			replicas = *deployment.SpecReplicas
		}

		if _, disabled := disabledMetrics["kube_deployment_spec_replicas"]; !disabled {
			ch <- newKubeDeploymentReplicasMetric("kube_deployment_spec_replicas", deploymentName, deploymentNS, replicas)
		}
		if _, disabled := disabledMetrics["kube_deployment_status_replicas_available"]; !disabled {
			// Replicas Available
			ch <- newKubeDeploymentStatusAvailableReplicasMetric(
				"kube_deployment_status_replicas_available",
				deploymentName,
				deploymentNS,
				deployment.StatusAvailableReplicas)
		}
	}
}

//--------------------------------------------------------------------------
//  KubeDeploymentReplicasMetric
//--------------------------------------------------------------------------

// KubeDeploymentReplicasMetric is a prometheus.Metric used to encode deployment match labels
type KubeDeploymentReplicasMetric struct {
	fqName     string
	help       string
	deployment string
	namespace  string
	replicas   float64
}

// Creates a new DeploymentMatchLabelsMetric, implementation of prometheus.Metric
func newKubeDeploymentReplicasMetric(fqname, deployment, namespace string, replicas int32) KubeDeploymentReplicasMetric {
	return KubeDeploymentReplicasMetric{
		fqName:     fqname,
		help:       "kube_deployment_spec_replicas Number of desired pods for a deployment.",
		deployment: deployment,
		namespace:  namespace,
		replicas:   float64(replicas),
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (kdr KubeDeploymentReplicasMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{
		"deployment": kdr.deployment,
		"namespace":  kdr.namespace,
	}
	return prometheus.NewDesc(kdr.fqName, kdr.help, []string{}, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data
// transmission object.
func (kdr KubeDeploymentReplicasMetric) Write(m *dto.Metric) error {
	m.Gauge = &dto.Gauge{
		Value: &kdr.replicas,
	}
	m.Label = []*dto.LabelPair{
		{
			Name:  toStringPtr("namespace"),
			Value: &kdr.namespace,
		},
		{
			Name:  toStringPtr("deployment"),
			Value: &kdr.deployment,
		},
	}

	return nil
}

//--------------------------------------------------------------------------
//  KubeDeploymentStatusAvailableReplicasMetric
//--------------------------------------------------------------------------

// KubeDeploymentStatusAvailableReplicasMetric is a prometheus.Metric used to encode deployment match labels
type KubeDeploymentStatusAvailableReplicasMetric struct {
	fqName            string
	help              string
	deployment        string
	namespace         string
	replicasAvailable float64
}

// Creates a new DeploymentMatchLabelsMetric, implementation of prometheus.Metric
func newKubeDeploymentStatusAvailableReplicasMetric(fqname, deployment, namespace string, replicasAvailable int32) KubeDeploymentStatusAvailableReplicasMetric {
	return KubeDeploymentStatusAvailableReplicasMetric{
		fqName:            fqname,
		help:              "kube_deployment_status_replicas_available The number of available replicas per deployment.",
		deployment:        deployment,
		namespace:         namespace,
		replicasAvailable: float64(replicasAvailable),
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (kdr KubeDeploymentStatusAvailableReplicasMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{
		"deployment": kdr.deployment,
		"namespace":  kdr.namespace,
	}
	return prometheus.NewDesc(kdr.fqName, kdr.help, []string{}, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data
// transmission object.
func (kdr KubeDeploymentStatusAvailableReplicasMetric) Write(m *dto.Metric) error {
	m.Gauge = &dto.Gauge{
		Value: &kdr.replicasAvailable,
	}
	m.Label = []*dto.LabelPair{
		{
			Name:  toStringPtr("namespace"),
			Value: &kdr.namespace,
		},
		{
			Name:  toStringPtr("deployment"),
			Value: &kdr.deployment,
		},
	}

	return nil
}
