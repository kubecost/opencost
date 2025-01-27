package env

import (
	"fmt"
	"runtime"
	"time"

	"github.com/opencost/opencost/core/pkg/env"
)

const (
	PrometheusServerEndpointEnvVar = "PROMETHEUS_SERVER_ENDPOINT"

	PrometheusRetryOnRateLimitResponseEnvVar    = "PROMETHEUS_RETRY_ON_RATE_LIMIT"
	PrometheusRetryOnRateLimitMaxRetriesEnvVar  = "PROMETHEUS_RETRY_ON_RATE_LIMIT_MAX_RETRIES"
	PrometheusRetryOnRateLimitDefaultWaitEnvVar = "PROMETHEUS_RETRY_ON_RATE_LIMIT_DEFAULT_WAIT"

	PrometheusQueryTimeoutEnvVar        = "PROMETHEUS_QUERY_TIMEOUT"
	PrometheusKeepAliveEnvVar           = "PROMETHEUS_KEEP_ALIVE"
	PrometheusTLSHandshakeTimeoutEnvVar = "PROMETHEUS_TLS_HANDSHAKE_TIMEOUT"
	ScrapeIntervalEnvVar                = "KUBECOST_SCRAPE_INTERVAL"

	ETLMaxPrometheusQueryDurationMinutes = "ETL_MAX_PROMETHEUS_QUERY_DURATION_MINUTES"

	MaxQueryConcurrencyEnvVar = "MAX_QUERY_CONCURRENCY"
	QueryLoggingFileEnvVar    = "QUERY_LOGGING_FILE"
	PromClusterIDLabelEnvVar  = "PROM_CLUSTER_ID_LABEL"

	PrometheusHeaderXScopeOrgIdEnvVar = "PROMETHEUS_HEADER_X_SCOPE_ORGID"
	InsecureSkipVerifyEnvVar          = "INSECURE_SKIP_VERIFY"
	KubeRbacProxyEnabledEnvVar        = "KUBE_RBAC_PROXY_ENABLED"

	ThanosEnabledEnvVar      = "THANOS_ENABLED"
	ThanosQueryUrlEnvVar     = "THANOS_QUERY_URL"
	ThanosOffsetEnvVar       = "THANOS_QUERY_OFFSET"
	ThanosMaxSourceResEnvVar = "THANOS_MAX_SOURCE_RESOLUTION"

	DBBasicAuthUsername = "DB_BASIC_AUTH_USERNAME"
	DBBasicAuthPassword = "DB_BASIC_AUTH_PW"
	DBBearerToken       = "DB_BEARER_TOKEN"

	MultiClusterBasicAuthUsername = "MC_BASIC_AUTH_USERNAME"
	MultiClusterBasicAuthPassword = "MC_BASIC_AUTH_PW"
	MultiClusterBearerToken       = "MC_BEARER_TOKEN"

	CurrentClusterIdFilterEnabledVar = "CURRENT_CLUSTER_ID_FILTER_ENABLED"
	ClusterIDEnvVar                  = "CLUSTER_ID"

	KubecostJobNameEnvVar      = "KUBECOST_JOB_NAME"
	ETLResolutionSecondsEnvVar = "ETL_RESOLUTION_SECONDS"
)

// IsPrometheusRetryOnRateLimitResponse will attempt to retry if a 429 response is received OR a 400 with a body containing
// ThrottleException (common in AWS services like AMP)
func IsPrometheusRetryOnRateLimitResponse() bool {
	return env.GetBool(PrometheusRetryOnRateLimitResponseEnvVar, true)
}

// GetPrometheusRetryOnRateLimitMaxRetries returns the maximum number of retries that should be attempted prior to failing.
// Only used if IsPrometheusRetryOnRateLimitResponse() is true.
func GetPrometheusRetryOnRateLimitMaxRetries() int {
	return env.GetInt(PrometheusRetryOnRateLimitMaxRetriesEnvVar, 5)
}

// GetPrometheusRetryOnRateLimitDefaultWait returns the default wait time for a retriable rate limit response without a
// Retry-After header.
func GetPrometheusRetryOnRateLimitDefaultWait() time.Duration {
	return env.GetDuration(PrometheusRetryOnRateLimitDefaultWaitEnvVar, 100*time.Millisecond)
}

// GetPrometheusHeaderXScopeOrgId returns the default value for X-Scope-OrgID header used for requests in Mimir/Cortex-Tenant API.
// To use Mimir(or Cortex-Tenant) instead of Prometheus add variable from cluster settings:
// "PROMETHEUS_HEADER_X_SCOPE_ORGID": "my-cluster-name"
// Then set Prometheus URL to prometheus API endpoint:
// "PROMETHEUS_SERVER_ENDPOINT": "http://mimir-url/prometheus/"
func GetPrometheusHeaderXScopeOrgId() string {
	return env.Get(PrometheusHeaderXScopeOrgIdEnvVar, "")
}

// GetPrometheusServerEndpoint returns the environment variable value for PrometheusServerEndpointEnvVar which
// represents the prometheus server endpoint used to execute prometheus queries.
func GetPrometheusServerEndpoint() string {
	return env.Get(PrometheusServerEndpointEnvVar, "")
}

func GetScrapeInterval() time.Duration {
	return env.GetDuration(ScrapeIntervalEnvVar, 0)
}

func GetPrometheusQueryTimeout() time.Duration {
	return env.GetDuration(PrometheusQueryTimeoutEnvVar, 120*time.Second)
}

func GetPrometheusKeepAlive() time.Duration {
	return env.GetDuration(PrometheusKeepAliveEnvVar, 120*time.Second)
}

func GetPrometheusTLSHandshakeTimeout() time.Duration {
	return env.GetDuration(PrometheusTLSHandshakeTimeoutEnvVar, 10*time.Second)
}

// GetJobName returns the environment variable value for JobNameEnvVar, specifying which job name
// is used for prometheus to scrape the provided metrics.
func GetJobName() string {
	return env.Get(KubecostJobNameEnvVar, "kubecost")
}

func IsInsecureSkipVerify() bool {
	return env.GetBool(InsecureSkipVerifyEnvVar, false)
}

func IsKubeRbacProxyEnabled() bool {
	return env.GetBool(KubeRbacProxyEnabledEnvVar, false)
}

// GetETLResolution determines the resolution of ETL queries. The smaller the
// duration, the higher the resolution; the higher the resolution, the more
// accurate the query results, but the more computationally expensive.
func GetETLResolution() time.Duration {
	// Use the configured ETL resolution, or default to
	// 5m (i.e. 300s)
	secs := time.Duration(env.GetInt64(ETLResolutionSecondsEnvVar, 300))
	return secs * time.Second
}

// IsThanosEnabled returns the environment variable value for ThanosEnabledEnvVar which represents whether
// or not thanos is enabled.
func IsThanosEnabled() bool {
	return env.GetBool(ThanosEnabledEnvVar, false)
}

// GetThanosQueryUrl returns the environment variable value for ThanosQueryUrlEnvVar which represents the
// target query endpoint for hitting thanos.
func GetThanosQueryUrl() string {
	return env.Get(ThanosQueryUrlEnvVar, "")
}

// GetThanosOffset returns the environment variable value for ThanosOffsetEnvVar which represents the total
// amount of time to offset all queries made to thanos.
func GetThanosOffset() string {
	return env.Get(ThanosOffsetEnvVar, "3h")
}

// GetThanosMaxSourceResolution returns the environment variable value for ThanosMaxSourceResEnvVar which represents
// the max source resolution to use when querying thanos.
func GetThanosMaxSourceResolution() string {
	res := env.Get(ThanosMaxSourceResEnvVar, "raw")

	switch res {
	case "raw":
		return "0s"
	case "0s":
		fallthrough
	case "5m":
		fallthrough
	case "1h":
		return res
	default:
		return "0s"
	}
}

// GetMaxQueryConcurrency returns the environment variable value for MaxQueryConcurrencyEnvVar
func GetMaxQueryConcurrency() int {
	maxQueryConcurrency := env.GetInt(MaxQueryConcurrencyEnvVar, 5)
	if maxQueryConcurrency <= 0 {
		return runtime.GOMAXPROCS(0)
	}
	return maxQueryConcurrency
}

// GetQueryLoggingFile returns a file location if query logging is enabled. Otherwise, empty string
func GetQueryLoggingFile() string {
	return env.Get(QueryLoggingFileEnvVar, "")
}

func GetDBBasicAuthUsername() string {
	return env.Get(DBBasicAuthUsername, "")
}

func GetDBBasicAuthUserPassword() string {
	return env.Get(DBBasicAuthPassword, "")

}

func GetDBBearerToken() string {
	return env.Get(DBBearerToken, "")
}

// GetMultiClusterBasicAuthUsername returns the environment variable value for MultiClusterBasicAuthUsername
func GetMultiClusterBasicAuthUsername() string {
	return env.Get(MultiClusterBasicAuthUsername, "")
}

// GetMultiClusterBasicAuthPassword returns the environment variable value for MultiClusterBasicAuthPassword
func GetMultiClusterBasicAuthPassword() string {
	return env.Get(MultiClusterBasicAuthPassword, "")
}

func GetMultiClusterBearerToken() string {
	return env.Get(MultiClusterBearerToken, "")
}

func GetETLMaxPrometheusQueryDuration() time.Duration {
	dayMins := 60 * 24
	mins := time.Duration(env.GetInt64(ETLMaxPrometheusQueryDurationMinutes, int64(dayMins)))
	return mins * time.Minute
}

// GetPromClusterLabel returns the environment variable value for PromClusterIDLabel
func GetPromClusterLabel() string {
	return env.Get(PromClusterIDLabelEnvVar, "cluster_id")
}

// GetClusterID returns the environment variable value for ClusterIDEnvVar which represents the
// configurable identifier used for multi-cluster metric emission.
func GetClusterID() string {
	return env.Get(ClusterIDEnvVar, "")
}

// GetPromClusterFilter returns environment variable value CurrentClusterIdFilterEnabledVar which
// represents additional prometheus filter for all metrics for current cluster id
func GetPromClusterFilter() string {
	if env.GetBool(CurrentClusterIdFilterEnabledVar, false) {
		return fmt.Sprintf("%s=\"%s\"", GetPromClusterLabel(), GetClusterID())
	}
	return ""
}
