package prom

import (
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/pkg/env"
	prometheus "github.com/prometheus/client_golang/api"
)

// Prometheus Metric Diagnostic IDs
const (
	// CAdvisorDiagnosticMetricID is the identifier of the metric used to determine if cAdvisor is being scraped.
	CAdvisorDiagnosticMetricID = "cadvisorMetric"

	// CAdvisorLabelDiagnosticMetricID is the identifier of the metric used to determine if cAdvisor labels are correct.
	CAdvisorLabelDiagnosticMetricID = "cadvisorLabel"

	// KSMDiagnosticMetricID is the identifier for the metric used to determine if KSM metrics are being scraped.
	KSMDiagnosticMetricID = "ksmMetric"

	// KSMVersionDiagnosticMetricID is the identifier for the metric used to determine if KSM version is correct.
	KSMVersionDiagnosticMetricID = "ksmVersion"

	// KubecostDiagnosticMetricID is the identifier for the metric used to determine if Kubecost metrics are being scraped.
	KubecostDiagnosticMetricID = "kubecostMetric"

	// ScrapeIntervalDiagnosticMetricID is the identifier for the metric used to determine if prometheus has its own self-scraped
	// metrics.
	ScrapeIntervalDiagnosticMetricID = "scrapeInterval"

	// CPUThrottlingDiagnosticMetricID is the identifier for the metric used to determine if CPU throttling is being applied to the
	// cost-model container.
	CPUThrottlingDiagnosticMetricID = "cpuThrottling"

	// KubecostRecordingRuleCPUUsageID is the identifier for the query used to
	// determine of the CPU usage recording rule is set up correctly.
	KubecostRecordingRuleCPUUsageID = "kubecostRecordingRuleCPUUsage"

	// CAdvisorWorkingSetBytesMetricID is the identifier for the query used to determine
	// if cAdvisor working set bytes data is being scraped
	CAdvisorWorkingSetBytesMetricID = "cadvisorWorkingSetBytesMetric"

	// KSMCPUCapacityMetricID is the identifier for the query used to determine if
	// KSM CPU capacity data is being scraped
	KSMCPUCapacityMetricID = "ksmCpuCapacityMetric"

	// KSMAllocatableCPUCoresMetricID is the identifier for the query used to determine
	// if KSM allocatable CPU core data is being scraped
	KSMAllocatableCPUCoresMetricID = "ksmAllocatableCpuCoresMetric"
)

const DocumentationBaseURL = "https://www.opencost.io/docs/"

// diagnostic definitions mapping holds all of the diagnostic definitions that can be used for prometheus metrics diagnostics
var diagnosticDefinitions map[string]*diagnosticDefinition = map[string]*diagnosticDefinition{
	CAdvisorDiagnosticMetricID: {
		ID:          CAdvisorDiagnosticMetricID,
		QueryFmt:    `absent_over_time(container_cpu_usage_seconds_total{%s}[5m] %s)`,
		Label:       "cAdvisor metrics available",
		Description: "Determine if cAdvisor metrics are available during last 5 minutes.",
		DocLink:     fmt.Sprintf("%s#cadvisor-metrics-available", DocumentationBaseURL),
	},
	KSMDiagnosticMetricID: {
		ID:          KSMDiagnosticMetricID,
		QueryFmt:    `absent_over_time(kube_pod_container_resource_requests{resource="memory", unit="byte", %s}[5m] %s)`,
		Label:       "Kube-state-metrics available",
		Description: "Determine if metrics from kube-state-metrics are available during last 5 minutes.",
		DocLink:     fmt.Sprintf("%s#kube-state-metrics-metrics-available", DocumentationBaseURL),
	},
	KubecostDiagnosticMetricID: {
		ID:          KubecostDiagnosticMetricID,
		QueryFmt:    `absent_over_time(node_cpu_hourly_cost{%s}[5m] %s)`,
		Label:       "Kubecost metrics available",
		Description: "Determine if metrics from Kubecost are available during last 5 minutes.",
	},
	CAdvisorLabelDiagnosticMetricID: {
		ID:          CAdvisorLabelDiagnosticMetricID,
		QueryFmt:    `absent_over_time(container_cpu_usage_seconds_total{container!="",pod!="", %s}[5m] %s)`,
		Label:       "Expected cAdvisor labels available",
		Description: "Determine if expected cAdvisor labels are present during last 5 minutes.",
		DocLink:     fmt.Sprintf("%s#cadvisor-metrics-available", DocumentationBaseURL),
	},
	KSMVersionDiagnosticMetricID: {
		ID:          KSMVersionDiagnosticMetricID,
		QueryFmt:    `absent_over_time(kube_persistentvolume_capacity_bytes{%s}[5m] %s)`,
		Label:       "Expected kube-state-metrics version found",
		Description: "Determine if metric in required kube-state-metrics version are present during last 5 minutes.",
		DocLink:     fmt.Sprintf("%s#expected-kube-state-metrics-version-found", DocumentationBaseURL),
	},
	ScrapeIntervalDiagnosticMetricID: {
		ID:          ScrapeIntervalDiagnosticMetricID,
		QueryFmt:    `absent_over_time(prometheus_target_interval_length_seconds{%s}[5m]  %s)`,
		Label:       "Expected Prometheus self-scrape metrics available",
		Description: "Determine if prometheus has its own self-scraped metrics during the last 5 minutes.",
	},
	CPUThrottlingDiagnosticMetricID: {
		ID: CPUThrottlingDiagnosticMetricID,
		QueryFmt: `avg(increase(container_cpu_cfs_throttled_periods_total{container="cost-model", %s}[10m] %s)) by (container_name, pod_name, namespace)
	/ avg(increase(container_cpu_cfs_periods_total{container="cost-model",%s}[10m] %s)) by (container_name, pod_name, namespace) > 0.2`,
		Label:       "Kubecost is not CPU throttled",
		Description: "Kubecost loading slowly? A kubecost component might be CPU throttled",
	},
	KubecostRecordingRuleCPUUsageID: {
		ID:          KubecostRecordingRuleCPUUsageID,
		QueryFmt:    `absent_over_time(kubecost_container_cpu_usage_irate{%s}[5m] %s)`,
		Label:       "Kubecost's CPU usage recording rule is set up",
		Description: "If the 'kubecost_container_cpu_usage_irate' recording rule is not set up, Allocation pipeline build may put pressure on your Prometheus due to the use of a subquery.",
		DocLink:     "https://www.opencost.io/docs/installation/prometheus",
	},
	CAdvisorWorkingSetBytesMetricID: {
		ID:          CAdvisorWorkingSetBytesMetricID,
		QueryFmt:    `absent_over_time(container_memory_working_set_bytes{container="cost-model", container!="POD", instance!="", %s}[5m] %s)`,
		Label:       "cAdvisor working set bytes metrics available",
		Description: "Determine if cAdvisor working set bytes metrics are available during last 5 minutes.",
	},
	KSMCPUCapacityMetricID: {
		ID:          KSMCPUCapacityMetricID,
		QueryFmt:    `absent_over_time(kube_node_status_capacity_cpu_cores{%s}[5m] %s)`,
		Label:       "KSM had CPU capacity during the last 5 minutes",
		Description: "Determine if KSM had CPU capacity during the last 5 minutes",
	},
	KSMAllocatableCPUCoresMetricID: {
		ID:          KSMAllocatableCPUCoresMetricID,
		QueryFmt:    `absent_over_time(kube_node_status_allocatable_cpu_cores{%s}[5m] %s)`,
		Label:       "KSM had allocatable CPU cores during the last 5 minutes",
		Description: "Determine if KSM had allocatable CPU cores during the last 5 minutes",
	},
}

// QueuedPromRequest is a representation of a request waiting to be sent by the prometheus
// client.
type QueuedPromRequest struct {
	Context   string `json:"context"`
	Query     string `json:"query"`
	QueueTime int64  `json:"queueTime"`
}

// PrometheusQueueState contains diagnostic information concerning the state of the prometheus request
// queue
type PrometheusQueueState struct {
	QueuedRequests      []*QueuedPromRequest `json:"queuedRequests"`
	OutboundRequests    int                  `json:"outboundRequests"`
	TotalRequests       int                  `json:"totalRequests"`
	MaxQueryConcurrency int                  `json:"maxQueryConcurrency"`
}

// GetPrometheusQueueState is a diagnostic function that probes the prometheus request queue and gathers
// query, context, and queue statistics.
func GetPrometheusQueueState(client prometheus.Client) (*PrometheusQueueState, error) {
	rlpc, ok := client.(*RateLimitedPrometheusClient)
	if !ok {
		return nil, fmt.Errorf("Failed to get prometheus queue state for the provided client. Must be of type RateLimitedPrometheusClient.")
	}

	outbound := rlpc.TotalOutboundRequests()

	requests := []*QueuedPromRequest{}
	rlpc.queue.Each(func(_ int, req *workRequest) {
		requests = append(requests, &QueuedPromRequest{
			Context:   req.contextName,
			Query:     req.query,
			QueueTime: time.Since(req.start).Milliseconds(),
		})
	})

	return &PrometheusQueueState{
		QueuedRequests:      requests,
		OutboundRequests:    outbound,
		TotalRequests:       outbound + len(requests),
		MaxQueryConcurrency: env.GetMaxQueryConcurrency(),
	}, nil
}

// LogPrometheusClientState logs the current state, with respect to outbound requests, if that
// information is available.
func LogPrometheusClientState(client prometheus.Client) {
	if rc, ok := client.(requestCounter); ok {
		queued := rc.TotalQueuedRequests()
		outbound := rc.TotalOutboundRequests()
		total := queued + outbound

		log.Infof("Outbound Requests: %d, Queued Requests: %d, Total Requests: %d", outbound, queued, total)
	}
}

// GetPrometheusMetrics returns a list of the state of Prometheus metric used by kubecost using the provided client
func GetPrometheusMetrics(client prometheus.Client, offset string) PrometheusDiagnostics {
	ctx := NewNamedContext(client, DiagnosticContextName)

	var result []*PrometheusDiagnostic
	for _, definition := range diagnosticDefinitions {
		pd := definition.NewDiagnostic(offset)
		err := pd.executePrometheusDiagnosticQuery(ctx)

		// log the errror, append to results anyways, and continue
		if err != nil {
			log.Errorf(err.Error())
		}
		result = append(result, pd)
	}

	return result
}

// GetPrometheusMetricsByID returns a list of the state of specific Prometheus metrics by identifier.
func GetPrometheusMetricsByID(ids []string, client prometheus.Client, offset string) PrometheusDiagnostics {
	ctx := NewNamedContext(client, DiagnosticContextName)

	var result []*PrometheusDiagnostic
	for _, id := range ids {
		if definition, ok := diagnosticDefinitions[id]; ok {
			pd := definition.NewDiagnostic(offset)
			err := pd.executePrometheusDiagnosticQuery(ctx)

			// log the errror, append to results anyways, and continue
			if err != nil {
				log.Errorf(err.Error())
			}
			result = append(result, pd)
		} else {
			log.Warnf("Failed to find diagnostic definition for id: %s", id)
		}
	}

	return result
}

// PrometheusDiagnostics is a PrometheusDiagnostic container with helper methods.
type PrometheusDiagnostics []*PrometheusDiagnostic

// HasFailure returns true if any of the diagnostic tests didn't pass.
func (pd PrometheusDiagnostics) HasFailure() bool {
	for _, p := range pd {
		if !p.Passed {
			return true
		}
	}

	return false
}

// diagnosticDefinition is a definition of a diagnostic that can be used to create new
// PrometheusDiagnostic instances using the definition's fields.
type diagnosticDefinition struct {
	ID          string
	QueryFmt    string
	Label       string
	Description string
	DocLink     string
}

// NewDiagnostic creates a new PrometheusDiagnostic instance using the provided definition data.
func (pdd *diagnosticDefinition) NewDiagnostic(offset string) *PrometheusDiagnostic {
	// FIXME: Any reasonable way to get the total number of replacements required in the query?
	// FIXME: All of the other queries require a single offset replace, but CPUThrottle requires two.
	var query string
	filter := env.GetPromClusterFilter()
	if pdd.ID == CPUThrottlingDiagnosticMetricID {
		query = fmt.Sprintf(pdd.QueryFmt, filter, offset, filter, offset)
	} else {
		query = fmt.Sprintf(pdd.QueryFmt, filter, offset)
	}

	return &PrometheusDiagnostic{
		ID:          pdd.ID,
		Query:       query,
		Label:       pdd.Label,
		Description: pdd.Description,
		DocLink:     pdd.DocLink,
	}
}

// PrometheusDiagnostic holds information about a metric and the query to ensure it is functional
type PrometheusDiagnostic struct {
	ID          string         `json:"id"`
	Query       string         `json:"query"`
	Label       string         `json:"label"`
	Description string         `json:"description"`
	DocLink     string         `json:"docLink"`
	Result      []*QueryResult `json:"result"`
	Passed      bool           `json:"passed"`
}

// executePrometheusDiagnosticQuery executes a PrometheusDiagnostic query using the given context
func (pd *PrometheusDiagnostic) executePrometheusDiagnosticQuery(ctx *Context) error {
	resultCh := ctx.Query(pd.Query)
	result, err := resultCh.Await()
	if err != nil {
		return fmt.Errorf("prometheus diagnostic %s failed with error: %s", pd.ID, err)
	}
	if result == nil {
		result = []*QueryResult{}
	}
	pd.Result = result
	pd.Passed = len(result) == 0
	return nil
}
