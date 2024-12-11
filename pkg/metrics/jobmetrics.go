package metrics

import (
	"github.com/opencost/opencost/pkg/clustercache"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	batchv1 "k8s.io/api/batch/v1"
)

var (
	jobFailureReasons = []string{"BackoffLimitExceeded", "DeadLineExceeded", "Evicted"}
)

//--------------------------------------------------------------------------
//  KubeJobCollector
//--------------------------------------------------------------------------

// KubeJobCollector is a prometheus collector that generates job sourced metrics.
type KubeJobCollector struct {
	KubeClusterCache clustercache.ClusterCache
	metricsConfig    MetricsConfig
}

// Describe sends the super-set of all possible descriptors of metrics
// collected by this Collector.
func (kjc KubeJobCollector) Describe(ch chan<- *prometheus.Desc) {
	disabledMetrics := kjc.metricsConfig.GetDisabledMetricsMap()
	if _, disabled := disabledMetrics["kube_job_status_failed"]; disabled {
		return
	}

	ch <- prometheus.NewDesc("kube_job_status_failed", "The number of pods which reached Phase Failed and the reason for failure.", []string{}, nil)
}

// Collect is called by the Prometheus registry when collecting metrics.
func (kjc KubeJobCollector) Collect(ch chan<- prometheus.Metric) {
	disabledMetrics := kjc.metricsConfig.GetDisabledMetricsMap()
	if _, disabled := disabledMetrics["kube_job_status_failed"]; disabled {
		return
	}

	jobs := kjc.KubeClusterCache.GetAllJobs()
	for _, job := range jobs {
		jobName := job.Name
		jobNS := job.Namespace

		if job.Status.Failed == 0 {
			ch <- newKubeJobStatusFailedMetric(jobName, jobNS, "kube_job_status_failed", "", 0)
		} else {
			for _, condition := range job.Status.Conditions {
				if condition.Type == batchv1.JobFailed {
					reasonKnown := false
					for _, reason := range jobFailureReasons {
						reasonKnown = reasonKnown || failureReason(&condition, reason)

						ch <- newKubeJobStatusFailedMetric(jobName, jobNS, "kube_job_status_failed", reason, boolFloat64(failureReason(&condition, reason)))
					}

					// for unknown reasons
					if !reasonKnown {
						ch <- newKubeJobStatusFailedMetric(jobName, jobNS, "kube_job_status_failed", "", float64(job.Status.Failed))
					}
				}
			}
		}
	}

}

//--------------------------------------------------------------------------
//  KubeJobStatusFailedMetric
//--------------------------------------------------------------------------

// KubeJobStatusFailedMetric
type KubeJobStatusFailedMetric struct {
	fqName    string
	help      string
	job       string
	namespace string
	reason    string
	value     float64
}

// Creates a new KubeJobStatusFailedMetric, implementation of prometheus.Metric
func newKubeJobStatusFailedMetric(job, namespace, fqName, reason string, value float64) KubeJobStatusFailedMetric {
	return KubeJobStatusFailedMetric{
		fqName:    fqName,
		help:      "kube_job_status_failed Failed job",
		job:       job,
		namespace: namespace,
		reason:    reason,
		value:     value,
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (kjsf KubeJobStatusFailedMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{
		"job_name":  kjsf.job,
		"namespace": kjsf.namespace,
		"reason":    kjsf.reason,
	}
	return prometheus.NewDesc(kjsf.fqName, kjsf.help, []string{}, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data
// transmission object.
func (kjsf KubeJobStatusFailedMetric) Write(m *dto.Metric) error {
	m.Gauge = &dto.Gauge{
		Value: &kjsf.value,
	}
	m.Label = []*dto.LabelPair{
		{
			Name:  toStringPtr("job_name"),
			Value: &kjsf.job,
		},
		{
			Name:  toStringPtr("namespace"),
			Value: &kjsf.namespace,
		},
		{
			Name:  toStringPtr("reason"),
			Value: &kjsf.reason,
		},
	}
	return nil
}
