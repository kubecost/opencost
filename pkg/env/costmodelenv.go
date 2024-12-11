package env

import (
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/env"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
)

const (
	APIPortEnvVar = "API_PORT"

	AWSAccessKeyIDEnvVar     = "AWS_ACCESS_KEY_ID"
	AWSAccessKeySecretEnvVar = "AWS_SECRET_ACCESS_KEY"
	AWSClusterIDEnvVar       = "AWS_CLUSTER_ID"
	AWSPricingURL            = "AWS_PRICING_URL"

	AlibabaAccessKeyIDEnvVar     = "ALIBABA_ACCESS_KEY_ID"
	AlibabaAccessKeySecretEnvVar = "ALIBABA_SECRET_ACCESS_KEY"

	AzureOfferIDEnvVar                   = "AZURE_OFFER_ID"
	AzureBillingAccountEnvVar            = "AZURE_BILLING_ACCOUNT"
	AzureDownloadBillingDataToDiskEnvVar = "AZURE_DOWNLOAD_BILLING_DATA_TO_DISK"

	KubecostNamespaceEnvVar        = "KUBECOST_NAMESPACE"
	KubecostScrapeIntervalEnvVar   = "KUBECOST_SCRAPE_INTERVAL"
	PodNameEnvVar                  = "POD_NAME"
	ClusterIDEnvVar                = "CLUSTER_ID"
	ClusterProfileEnvVar           = "CLUSTER_PROFILE"
	PrometheusServerEndpointEnvVar = "PROMETHEUS_SERVER_ENDPOINT"
	MaxQueryConcurrencyEnvVar      = "MAX_QUERY_CONCURRENCY"
	QueryLoggingFileEnvVar         = "QUERY_LOGGING_FILE"
	RemoteEnabledEnvVar            = "REMOTE_WRITE_ENABLED"
	RemotePWEnvVar                 = "REMOTE_WRITE_PASSWORD"
	SQLAddressEnvVar               = "SQL_ADDRESS"
	UseCSVProviderEnvVar           = "USE_CSV_PROVIDER"
	UseCustomProviderEnvVar        = "USE_CUSTOM_PROVIDER"
	CSVRegionEnvVar                = "CSV_REGION"
	CSVEndpointEnvVar              = "CSV_ENDPOINT"
	CSVPathEnvVar                  = "CSV_PATH"
	ConfigPathEnvVar               = "CONFIG_PATH"
	CloudProviderAPIKeyEnvVar      = "CLOUD_PROVIDER_API_KEY"
	DisableAggregateCostModelCache = "DISABLE_AGGREGATE_COST_MODEL_CACHE"

	EmitPodAnnotationsMetricEnvVar       = "EMIT_POD_ANNOTATIONS_METRIC"
	EmitNamespaceAnnotationsMetricEnvVar = "EMIT_NAMESPACE_ANNOTATIONS_METRIC"
	EmitDeprecatedMetrics                = "EMIT_DEPRECATED_METRICS"

	EmitKsmV1MetricsEnvVar = "EMIT_KSM_V1_METRICS"
	EmitKsmV1MetricsOnly   = "EMIT_KSM_V1_METRICS_ONLY"

	ThanosEnabledEnvVar      = "THANOS_ENABLED"
	ThanosQueryUrlEnvVar     = "THANOS_QUERY_URL"
	ThanosOffsetEnvVar       = "THANOS_QUERY_OFFSET"
	ThanosMaxSourceResEnvVar = "THANOS_MAX_SOURCE_RESOLUTION"

	PProfEnabledEnvVar = "PPROF_ENABLED"

	LogCollectionEnabledEnvVar    = "LOG_COLLECTION_ENABLED"
	ProductAnalyticsEnabledEnvVar = "PRODUCT_ANALYTICS_ENABLED"
	ErrorReportingEnabledEnvVar   = "ERROR_REPORTING_ENABLED"
	ValuesReportingEnabledEnvVar  = "VALUES_REPORTING_ENABLED"

	DBBasicAuthUsername = "DB_BASIC_AUTH_USERNAME"
	DBBasicAuthPassword = "DB_BASIC_AUTH_PW"
	DBBearerToken       = "DB_BEARER_TOKEN"

	MultiClusterBasicAuthUsername = "MC_BASIC_AUTH_USERNAME"
	MultiClusterBasicAuthPassword = "MC_BASIC_AUTH_PW"
	MultiClusterBearerToken       = "MC_BEARER_TOKEN"

	InsecureSkipVerify = "INSECURE_SKIP_VERIFY"
	KubeRbacProxyEnabled = "KUBE_RBAC_PROXY_ENABLED"

	KubeConfigPathEnvVar = "KUBECONFIG_PATH"

	UTCOffsetEnvVar                  = "UTC_OFFSET"
	CurrentClusterIdFilterEnabledVar = "CURRENT_CLUSTER_ID_FILTER_ENABLED"

	CacheWarmingEnabledEnvVar            = "CACHE_WARMING_ENABLED"
	ETLEnabledEnvVar                     = "ETL_ENABLED"
	ETLMaxPrometheusQueryDurationMinutes = "ETL_MAX_PROMETHEUS_QUERY_DURATION_MINUTES"
	ETLResolutionSeconds                 = "ETL_RESOLUTION_SECONDS"
	LegacyExternalAPIDisabledVar         = "LEGACY_EXTERNAL_API_DISABLED"

	PromClusterIDLabelEnvVar = "PROM_CLUSTER_ID_LABEL"

	PricingConfigmapName  = "PRICING_CONFIGMAP_NAME"
	MetricsConfigmapName  = "METRICS_CONFIGMAP_NAME"
	KubecostJobNameEnvVar = "KUBECOST_JOB_NAME"

	KubecostConfigBucketEnvVar    = "KUBECOST_CONFIG_BUCKET"
	ClusterInfoFileEnabledEnvVar  = "CLUSTER_INFO_FILE_ENABLED"
	ClusterCacheFileEnabledEnvVar = "CLUSTER_CACHE_FILE_ENABLED"

	PrometheusQueryOffsetEnvVar                 = "PROMETHEUS_QUERY_OFFSET"
	PrometheusRetryOnRateLimitResponseEnvVar    = "PROMETHEUS_RETRY_ON_RATE_LIMIT"
	PrometheusRetryOnRateLimitMaxRetriesEnvVar  = "PROMETHEUS_RETRY_ON_RATE_LIMIT_MAX_RETRIES"
	PrometheusRetryOnRateLimitDefaultWaitEnvVar = "PROMETHEUS_RETRY_ON_RATE_LIMIT_DEFAULT_WAIT"

	PrometheusHeaderXScopeOrgIdEnvVar = "PROMETHEUS_HEADER_X_SCOPE_ORGID"

	IngestPodUIDEnvVar = "INGEST_POD_UID"

	ETLReadOnlyMode = "ETL_READ_ONLY"

	AllocationNodeLabelsEnabled     = "ALLOCATION_NODE_LABELS_ENABLED"
	AllocationNodeLabelsIncludeList = "ALLOCATION_NODE_LABELS_INCLUDE_LIST"

	AssetIncludeLocalDiskCostEnvVar = "ASSET_INCLUDE_LOCAL_DISK_COST"

	regionOverrideList = "REGION_OVERRIDE_LIST"

	ExportCSVFile       = "EXPORT_CSV_FILE"
	ExportCSVLabelsList = "EXPORT_CSV_LABELS_LIST"
	ExportCSVLabelsAll  = "EXPORT_CSV_LABELS_ALL"
	ExportCSVMaxDays    = "EXPORT_CSV_MAX_DAYS"

	DataRetentionDailyResolutionDaysEnvVar   = "DATA_RETENTION_DAILY_RESOLUTION_DAYS"
	DataRetentionHourlyResolutionHoursEnvVar = "DATA_RETENTION_HOURLY_RESOLUTION_HOURS"

	// We assume that Kubernetes is enabled if there is a KUBERNETES_PORT environment variable present
	KubernetesEnabledEnvVar         = "KUBERNETES_PORT"
	CloudCostEnabledEnvVar          = "CLOUD_COST_ENABLED"
	CloudCostConfigPath             = "CLOUD_COST_CONFIG_PATH"
	CloudCostMonthToDateIntervalVar = "CLOUD_COST_MONTH_TO_DATE_INTERVAL"
	CloudCostRefreshRateHoursEnvVar = "CLOUD_COST_REFRESH_RATE_HOURS"
	CloudCostQueryWindowDaysEnvVar  = "CLOUD_COST_QUERY_WINDOW_DAYS"
	CloudCostRunWindowDaysEnvVar    = "CLOUD_COST_RUN_WINDOW_DAYS"

	CustomCostEnabledEnvVar          = "CUSTOM_COST_ENABLED"
	CustomCostQueryWindowDaysEnvVar  = "CUSTOM_COST_QUERY_WINDOW_DAYS"
	CustomCostRefreshRateHoursEnvVar = "CUSTOM_COST_REFRESH_RATE_HOURS"

	PluginConfigDirEnvVar     = "PLUGIN_CONFIG_DIR"
	PluginExecutableDirEnvVar = "PLUGIN_EXECUTABLE_DIR"

	OCIPricingURL = "OCI_PRICING_URL"

	CarbonEstimatesEnabledEnvVar = "CARBON_ESTIMATES_ENABLED"

	UseCacheV1 = "USE_CACHE_V1"
)

const DefaultConfigMountPath = "/var/configs"

func IsETLReadOnlyMode() bool {
	return env.GetBool(ETLReadOnlyMode, false)
}

func GetExportCSVFile() string {
	return env.Get(ExportCSVFile, "")
}

func GetExportCSVLabelsAll() bool {
	return env.GetBool(ExportCSVLabelsAll, false)
}

func GetKubecostScrapeInterval() time.Duration {
	return env.GetDuration(KubecostScrapeIntervalEnvVar, 0)
}

func GetExportCSVLabelsList() []string {
	return env.GetList(ExportCSVLabelsList, ",")
}

func IsPProfEnabled() bool {
	return env.GetBool(PProfEnabledEnvVar, false)
}

func GetExportCSVMaxDays() int {
	return env.GetInt(ExportCSVMaxDays, 90)
}

// GetAPIPort returns the environment variable value for APIPortEnvVar which
// is the port number the API is available on.
func GetAPIPort() int {
	return env.GetInt(APIPortEnvVar, 9003)
}

// GetKubecostConfigBucket returns a file location for a mounted bucket configuration which is used to store
// a subset of kubecost configurations that require sharing via remote storage.
func GetKubecostConfigBucket() string {
	return env.Get(KubecostConfigBucketEnvVar, "")
}

// IsClusterInfoFileEnabled returns true if the cluster info is read from a file or pulled from the local
// cloud provider and kubernetes.
func IsClusterInfoFileEnabled() bool {
	return env.GetBool(ClusterInfoFileEnabledEnvVar, false)
}

// IsClusterCacheFileEnabled returns true if the kubernetes cluster data is read from a file or pulled from the local
// kubernetes API.
func IsClusterCacheFileEnabled() bool {
	return env.GetBool(ClusterCacheFileEnabledEnvVar, false)
}

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

// GetPrometheusQueryOffset returns the time.Duration to offset all prometheus queries by. NOTE: This env var is applied
// to all non-range queries made via our query context. This should only be applied when there is a significant delay in
// data arriving in the target prom db. For example, if supplying a thanos or cortex querier for the prometheus server, using
// a 3h offset will ensure that current time = current time - 3h.
//
// This offset is NOT the same as the GetThanosOffset() option, as that is only applied to queries made specifically targeting
// thanos. This offset is applied globally.
func GetPrometheusQueryOffset() time.Duration {
	offset := env.Get(PrometheusQueryOffsetEnvVar, "")
	if offset == "" {
		return 0
	}

	dur, err := timeutil.ParseDuration(offset)
	if err != nil {
		return 0
	}
	return dur
}

func GetPricingConfigmapName() string {
	return env.Get(PricingConfigmapName, "pricing-configs")
}

func GetMetricsConfigmapName() string {
	return env.Get(MetricsConfigmapName, "metrics-config")
}

// IsEmitNamespaceAnnotationsMetric returns true if cost-model is configured to emit the kube_namespace_annotations metric
// containing the namespace annotations
func IsEmitNamespaceAnnotationsMetric() bool {
	return env.GetBool(EmitNamespaceAnnotationsMetricEnvVar, false)
}

// IsEmitPodAnnotationsMetric returns true if cost-model is configured to emit the kube_pod_annotations metric containing
// pod annotations.
func IsEmitPodAnnotationsMetric() bool {
	return env.GetBool(EmitPodAnnotationsMetricEnvVar, false)
}

// IsEmitKsmV1Metrics returns true if cost-model is configured to emit all necessary KSM v1
// metrics that were removed in KSM v2
func IsEmitKsmV1Metrics() bool {
	return env.GetBool(EmitKsmV1MetricsEnvVar, true)
}

func IsEmitKsmV1MetricsOnly() bool {
	return env.GetBool(EmitKsmV1MetricsOnly, false)
}

func IsEmitDeprecatedMetrics() bool {
	return env.GetBool(EmitDeprecatedMetrics, false)
}

// GetAWSAccessKeyID returns the environment variable value for AWSAccessKeyIDEnvVar which represents
// the AWS access key for authentication
func GetAWSAccessKeyID() string {
	awsAccessKeyID := env.Get(AWSAccessKeyIDEnvVar, "")
	// If the sample nil service key name is set, zero it out so that it is not
	// misinterpreted as a real service key.
	if awsAccessKeyID == "AKIXXX" {
		awsAccessKeyID = ""
	}
	return awsAccessKeyID
}

// GetAWSAccessKeySecret returns the environment variable value for AWSAccessKeySecretEnvVar which represents
// the AWS access key secret for authentication
func GetAWSAccessKeySecret() string {
	return env.Get(AWSAccessKeySecretEnvVar, "")
}

// GetAWSClusterID returns the environment variable value for AWSClusterIDEnvVar which represents
// an AWS specific cluster identifier.
func GetAWSClusterID() string {
	return env.Get(AWSClusterIDEnvVar, "")
}

// GetAWSPricingURL returns an optional alternative URL to fetch AWS pricing data from; for use in airgapped environments
func GetAWSPricingURL() string {
	return env.Get(AWSPricingURL, "")
}

// GetAlibabaAccessKeyID returns the environment variable value for AlibabaAccessKeyIDEnvVar which represents
// the Alibaba access key for authentication
func GetAlibabaAccessKeyID() string {
	return env.Get(AlibabaAccessKeyIDEnvVar, "")
}

// GetAlibabaAccessKeySecret returns the environment variable value for AlibabaAccessKeySecretEnvVar which represents
// the Alibaba access key secret for authentication
func GetAlibabaAccessKeySecret() string {
	return env.Get(AlibabaAccessKeySecretEnvVar, "")
}

// GetAzureOfferID returns the environment variable value for AzureOfferIDEnvVar which represents
// the Azure offer ID for determining prices.
func GetAzureOfferID() string {
	return env.Get(AzureOfferIDEnvVar, "")
}

// GetAzureBillingAccount returns the environment variable value for
// AzureBillingAccountEnvVar which represents the Azure billing
// account for determining prices. If this is specified
// customer-specific prices will be downloaded from the consumption
// price sheet API.
func GetAzureBillingAccount() string {
	return env.Get(AzureBillingAccountEnvVar, "")
}

// IsAzureDownloadBillingDataToDisk returns the environment variable value for
// AzureDownloadBillingDataToDiskEnvVar which indicates whether the Azure
// Billing Data should be held in memory or written to disk.
func IsAzureDownloadBillingDataToDisk() bool {
	return env.GetBool(AzureDownloadBillingDataToDiskEnvVar, true)
}

// GetKubecostNamespace returns the environment variable value for KubecostNamespaceEnvVar which
// represents the namespace the cost model exists in.
func GetKubecostNamespace() string {
	return env.Get(KubecostNamespaceEnvVar, "kubecost")
}

// GetPodName returns the name of the current running pod. If this environment variable is not set,
// empty string is returned.
func GetPodName() string {
	return env.Get(PodNameEnvVar, "")
}

// GetClusterProfile returns the environment variable value for ClusterProfileEnvVar which
// represents the cluster profile configured for
func GetClusterProfile() string {
	return env.Get(ClusterProfileEnvVar, "development")
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

// GetPrometheusServerEndpoint returns the environment variable value for PrometheusServerEndpointEnvVar which
// represents the prometheus server endpoint used to execute prometheus queries.
func GetPrometheusServerEndpoint() string {
	return env.Get(PrometheusServerEndpointEnvVar, "")
}

func GetInsecureSkipVerify() bool {
	return env.GetBool(InsecureSkipVerify, false)
}

func IsKubeRbacProxyEnabled() bool {
	return env.GetBool(KubeRbacProxyEnabled, false)
}

// IsAggregateCostModelCacheDisabled returns the environment variable value for DisableAggregateCostModelCache which
// will inform the aggregator on whether to load cached data. Defaults to false
func IsAggregateCostModelCacheDisabled() bool {
	return env.GetBool(DisableAggregateCostModelCache, false)
}

// IsRemoteEnabled returns the environment variable value for RemoteEnabledEnvVar which represents whether
// or not remote write is enabled for prometheus for use with SQL backed persistent storage.
func IsRemoteEnabled() bool {
	return env.GetBool(RemoteEnabledEnvVar, false)
}

// GetRemotePW returns the environment variable value for RemotePWEnvVar which represents the remote
// persistent storage password.
func GetRemotePW() string {
	return env.Get(RemotePWEnvVar, "")
}

// GetSQLAddress returns the environment variable value for SQLAddressEnvVar which represents the SQL
// database address used with remote persistent storage.
func GetSQLAddress() string {
	return env.Get(SQLAddressEnvVar, "")
}

// IsUseCSVProvider returns the environment variable value for UseCSVProviderEnvVar which represents
// whether or not the use of a CSV cost provider is enabled.
func IsUseCSVProvider() bool {
	return env.GetBool(UseCSVProviderEnvVar, false)
}

// IsUseCustomProvider returns the environment variable value for UseCustomProviderEnvVar which represents
// whether or not the use of a custom cost provider is enabled.
func IsUseCustomProvider() bool {
	return env.GetBool(UseCustomProviderEnvVar, false)
}

// GetCSVRegion returns the environment variable value for CSVRegionEnvVar which represents the
// region configured for a CSV provider.
func GetCSVRegion() string {
	return env.Get(CSVRegionEnvVar, "")
}

// GetCSVEndpoint returns the environment variable value for CSVEndpointEnvVar which represents the
// endpoint configured for a S3 CSV provider another than AWS S3.
func GetCSVEndpoint() string {
	return env.Get(CSVEndpointEnvVar, "")
}

// GetCSVPath returns the environment variable value for CSVPathEnvVar which represents the key path
// configured for a CSV provider.
func GetCSVPath() string {
	return env.Get(CSVPathEnvVar, "")
}

// GetCostAnalyzerVolumeMountPath is an alias of GetConfigPath, which returns the mount path for the
// Cost Analyzer volume, which stores configs, persistent data, etc.
func GetCostAnalyzerVolumeMountPath() string {
	return GetConfigPathWithDefault(DefaultConfigMountPath)
}

// GetConfigPath returns the environment variable value for ConfigPathEnvVar which represents the cost
// model configuration path
func GetConfigPathWithDefault(defaultValue string) string {
	return env.Get(ConfigPathEnvVar, defaultValue)
}

// GetCloudProviderAPI returns the environment variable value for CloudProviderAPIEnvVar which represents
// the API key provided for the cloud provider.
func GetCloudProviderAPIKey() string {
	return env.Get(CloudProviderAPIKeyEnvVar, "")
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

// IsLogCollectionEnabled returns the environment variable value for LogCollectionEnabledEnvVar which represents
// whether or not log collection has been enabled for kubecost deployments.
func IsLogCollectionEnabled() bool {
	return env.GetBool(LogCollectionEnabledEnvVar, true)
}

// IsProductAnalyticsEnabled returns the environment variable value for ProductAnalyticsEnabledEnvVar
func IsProductAnalyticsEnabled() bool {
	return env.GetBool(ProductAnalyticsEnabledEnvVar, true)
}

// IsErrorReportingEnabled returns the environment variable value for ErrorReportingEnabledEnvVar
func IsErrorReportingEnabled() bool {
	return env.GetBool(ErrorReportingEnabledEnvVar, true)
}

// IsValuesReportingEnabled returns the environment variable value for ValuesReportingEnabledEnvVar
func IsValuesReportingEnabled() bool {
	return env.GetBool(ValuesReportingEnabledEnvVar, true)
}

// GetMaxQueryConcurrency returns the environment variable value for MaxQueryConcurrencyEnvVar
func GetMaxQueryConcurrency() int {
	return env.GetInt(MaxQueryConcurrencyEnvVar, 5)
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

// GetKubeConfigPath returns the environment variable value for KubeConfigPathEnvVar
func GetKubeConfigPath() string {
	return env.Get(KubeConfigPathEnvVar, "")
}

// GetUTCOffset returns the environment variable value for UTCOffset
func GetUTCOffset() string {
	return env.Get(UTCOffsetEnvVar, "")
}

// GetParsedUTCOffset returns the duration of the configured UTC offset
func GetParsedUTCOffset() time.Duration {
	offset, err := timeutil.ParseUTCOffset(GetUTCOffset())
	if err != nil {
		log.Warnf("Failed to parse UTC offset: %s", err)
		return time.Duration(0)
	}
	return offset
}

// GetKubecostJobName returns the environment variable value for KubecostJobNameEnvVar
func GetKubecostJobName() string {
	return env.Get(KubecostJobNameEnvVar, "kubecost")
}

func IsCacheWarmingEnabled() bool {
	return env.GetBool(CacheWarmingEnabledEnvVar, true)
}

func IsETLEnabled() bool {
	return env.GetBool(ETLEnabledEnvVar, true)
}

func GetETLMaxPrometheusQueryDuration() time.Duration {
	dayMins := 60 * 24
	mins := time.Duration(env.GetInt64(ETLMaxPrometheusQueryDurationMinutes, int64(dayMins)))
	return mins * time.Minute
}

// GetETLResolution determines the resolution of ETL queries. The smaller the
// duration, the higher the resolution; the higher the resolution, the more
// accurate the query results, but the more computationally expensive.
func GetETLResolution() time.Duration {
	// Use the configured ETL resolution, or default to
	// 5m (i.e. 300s)
	secs := time.Duration(env.GetInt64(ETLResolutionSeconds, 300))
	return secs * time.Second
}

func LegacyExternalCostsAPIDisabled() bool {
	return env.GetBool(LegacyExternalAPIDisabledVar, false)
}

// GetPromClusterLabel returns the environment variable value for PromClusterIDLabel
func GetPromClusterLabel() string {
	return env.Get(PromClusterIDLabelEnvVar, "cluster_id")
}

// IsIngestingPodUID returns the env variable from ingestPodUID, which alters the
// contents of podKeys in Allocation
func IsIngestingPodUID() bool {
	return env.GetBool(IngestPodUIDEnvVar, false)
}

func GetAllocationNodeLabelsEnabled() bool {
	return env.GetBool(AllocationNodeLabelsEnabled, true)
}

var defaultAllocationNodeLabelsIncludeList []string = []string{
	"cloud.google.com/gke-nodepool",
	"eks.amazonaws.com/nodegroup",
	"kubernetes.azure.com/agentpool",
	"node.kubernetes.io/instance-type",
	"topology.kubernetes.io/region",
	"topology.kubernetes.io/zone",
}

func GetAllocationNodeLabelsIncludeList() []string {
	// If node labels are not enabled, return an empty list.
	if !GetAllocationNodeLabelsEnabled() {
		return []string{}
	}

	list := env.GetList(AllocationNodeLabelsIncludeList, ",")

	// If node labels are enabled, but the white list is empty, use defaults.
	if len(list) == 0 {
		return defaultAllocationNodeLabelsIncludeList
	}

	return list
}

func GetAssetIncludeLocalDiskCost() bool {
	return env.GetBool(AssetIncludeLocalDiskCostEnvVar, true)
}

func GetRegionOverrideList() []string {
	regionList := env.GetList(regionOverrideList, ",")

	if regionList == nil {
		return []string{}
	}

	return regionList
}

func GetDataRetentionDailyResolutionDays() int64 {
	return env.GetInt64(DataRetentionDailyResolutionDaysEnvVar, 30)
}

func GetDataRetentionHourlyResolutionHours() int64 {
	return env.GetInt64(DataRetentionHourlyResolutionHoursEnvVar, 49)
}

func IsKubernetesEnabled() bool {
	return env.Get(KubernetesEnabledEnvVar, "") != ""
}

func IsCloudCostEnabled() bool {
	return env.GetBool(CloudCostEnabledEnvVar, false)
}

func IsCustomCostEnabled() bool {
	return env.GetBool(CustomCostEnabledEnvVar, false)
}

func GetCloudCostConfigPath() string {
	return env.Get(CloudCostConfigPath, "cloud-integration.json")
}

func GetCloudCostMonthToDateInterval() int {
	return env.GetInt(CloudCostMonthToDateIntervalVar, 6)
}

func GetCloudCostRefreshRateHours() int64 {
	return env.GetInt64(CloudCostRefreshRateHoursEnvVar, 6)
}

func GetCloudCostQueryWindowDays() int64 {
	return env.GetInt64(CloudCostQueryWindowDaysEnvVar, 7)
}

func GetCustomCostQueryWindowHours() int64 {
	return env.GetInt64(CustomCostQueryWindowDaysEnvVar, 1)
}

func GetCustomCostQueryWindowDays() int64 {
	return env.GetInt64(CustomCostQueryWindowDaysEnvVar, 7)
}

func GetCloudCostRunWindowDays() int64 {
	return env.GetInt64(CloudCostRunWindowDaysEnvVar, 3)
}

func GetOCIPricingURL() string {
	return env.Get(OCIPricingURL, "https://apexapps.oracle.com/pls/apex/cetools/api/v1/products")
}

func GetPluginConfigDir() string {
	return env.Get(PluginConfigDirEnvVar, "/opt/opencost/plugin/config")
}

func GetPluginExecutableDir() string {
	return env.Get(PluginExecutableDirEnvVar, "/opt/opencost/plugin/bin")
}

func GetCustomCostRefreshRateHours() string {
	return env.Get(CustomCostRefreshRateHoursEnvVar, "12h")
}

func IsCarbonEstimatesEnabled() bool {
	return env.GetBool(CarbonEstimatesEnabledEnvVar, false)
}

// GetUseCacheV1 is a temporary flag to allow users to opt-in to using the old cache
// Mainly for comparison purposes
func GetUseCacheV1() bool {
	return env.GetBool(UseCacheV1, false)
}
