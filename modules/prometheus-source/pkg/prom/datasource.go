package prom

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/opencost/opencost/modules/prometheus-source/pkg/env"

	"github.com/opencost/opencost/core/pkg/clusters"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/protocol"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/util/httputil"
	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/core/pkg/util/timeutil"

	prometheus "github.com/prometheus/client_golang/api"
	prometheusAPI "github.com/prometheus/client_golang/api/prometheus/v1"
)

const (
	apiPrefix         = "/api/v1"
	epAlertManagers   = apiPrefix + "/alertmanagers"
	epLabelValues     = apiPrefix + "/label/:name/values"
	epSeries          = apiPrefix + "/series"
	epTargets         = apiPrefix + "/targets"
	epSnapshot        = apiPrefix + "/admin/tsdb/snapshot"
	epDeleteSeries    = apiPrefix + "/admin/tsdb/delete_series"
	epCleanTombstones = apiPrefix + "/admin/tsdb/clean_tombstones"
	epConfig          = apiPrefix + "/status/config"
	epFlags           = apiPrefix + "/status/flags"
	epRules           = apiPrefix + "/rules"
)

// helper for query range proxy requests
func toStartEndStep(qp httputil.QueryParams) (start, end time.Time, step time.Duration, err error) {
	var e error

	ss := qp.Get("start", "")
	es := qp.Get("end", "")
	ds := qp.Get("duration", "")
	layout := "2006-01-02T15:04:05.000Z"

	start, e = time.Parse(layout, ss)
	if e != nil {
		err = fmt.Errorf("Error parsing time %s. Error: %s", ss, err)
		return
	}
	end, e = time.Parse(layout, es)
	if e != nil {
		err = fmt.Errorf("Error parsing time %s. Error: %s", es, err)
		return
	}
	step, e = time.ParseDuration(ds)
	if e != nil {
		err = fmt.Errorf("Error parsing duration %s. Error: %s", ds, err)
		return
	}
	err = nil

	return
}

// FIXME: Before merge, implement a more robust design. This is brittle and bug-prone,
// FIXME: but decouples the prom requirements from the Provider implementations.
var providerStorageQueries = map[string]func(config *OpenCostPrometheusConfig, start, end time.Time, rate bool, used bool) string{
	"aws": func(config *OpenCostPrometheusConfig, start, end time.Time, rate bool, used bool) string {
		return ""
	},
	"gcp": func(config *OpenCostPrometheusConfig, start, end time.Time, rate bool, used bool) string {
		// TODO Set to the price for the appropriate storage class. It's not trivial to determine the local storage disk type
		// See https://cloud.google.com/compute/disks-image-pricing#persistentdisk
		localStorageCost := 0.04

		baseMetric := "container_fs_limit_bytes"
		if used {
			baseMetric = "container_fs_usage_bytes"
		}

		fmtCumulativeQuery := `sum(
			sum_over_time(%s{device!="tmpfs", id="/", %s}[%s:1m])
		) by (%s) / 60 / 730 / 1024 / 1024 / 1024 * %f`

		fmtMonthlyQuery := `sum(
			avg_over_time(%s{device!="tmpfs", id="/", %s}[%s:1m])
		) by (%s) / 1024 / 1024 / 1024 * %f`

		fmtQuery := fmtCumulativeQuery
		if rate {
			fmtQuery = fmtMonthlyQuery
		}
		fmtWindow := timeutil.DurationString(end.Sub(start))

		return fmt.Sprintf(fmtQuery, baseMetric, config.ClusterFilter, fmtWindow, config.ClusterLabel, localStorageCost)
	},
	"azure": func(config *OpenCostPrometheusConfig, start, end time.Time, rate bool, used bool) string {
		return ""
	},
	"alibaba": func(config *OpenCostPrometheusConfig, start, end time.Time, rate bool, used bool) string {
		return ""
	},
	"scaleway": func(config *OpenCostPrometheusConfig, start, end time.Time, rate bool, used bool) string {
		return ""
	},
	"otc": func(config *OpenCostPrometheusConfig, start, end time.Time, rate bool, used bool) string {
		return ""
	},
	"oracle": func(config *OpenCostPrometheusConfig, start, end time.Time, rate bool, used bool) string {
		return ""
	},
	"csv": func(config *OpenCostPrometheusConfig, start, end time.Time, rate bool, used bool) string {
		return ""
	},
	"custom": func(config *OpenCostPrometheusConfig, start, end time.Time, rate bool, used bool) string {
		return ""
	},
}

// creates a new help error which indicates the caller can retry and is non-fatal.
func newHelpRetryError(format string, args ...any) error {
	formatWithHelp := format + "\nTroubleshooting help available at: %s"
	args = append(args, PrometheusTroubleshootingURL)

	cause := fmt.Errorf(formatWithHelp, args...)
	return source.NewHelpRetryError(cause)
}

// PrometheusDataSource is the OpenCost data source implementation leveraging Prometheus. Prometheus provides longer retention periods and
// more detailed metrics than the OpenCost Collector, which is useful for historical analysis and cost forecasting.
type PrometheusDataSource struct {
	promConfig   *OpenCostPrometheusConfig
	promClient   prometheus.Client
	promContexts *ContextFactory

	thanosConfig   *OpenCostThanosConfig
	thanosClient   prometheus.Client
	thanosContexts *ContextFactory
}

// NewDefaultPrometheusDataSource creates and initializes a new `PrometheusDataSource` with configuration
// parsed from environment variables. This function will block until a connection to prometheus is established,
// or fails. It is recommended to run this function in a goroutine on a retry cycle.
func NewDefaultPrometheusDataSource() (*PrometheusDataSource, error) {
	config, err := NewOpenCostPrometheusConfigFromEnv()
	if err != nil {
		return nil, fmt.Errorf("failed to create prometheus config from env: %w", err)
	}

	var thanosConfig *OpenCostThanosConfig
	if env.IsThanosEnabled() {
		// thanos initialization is not fatal, so we log the error and continue
		thanosConfig, err = NewOpenCostThanosConfigFromEnv()
		if err != nil {
			log.Warnf("Thanos was enabled, but failed to create thanos config from env: %s. Continuing...", err.Error())
		}
	}

	return NewPrometheusDataSource(config, thanosConfig)
}

// NewPrometheusDataSource initializes clients for Prometheus and Thanos, and returns a new PrometheusDataSource.
func NewPrometheusDataSource(promConfig *OpenCostPrometheusConfig, thanosConfig *OpenCostThanosConfig) (*PrometheusDataSource, error) {
	promClient, err := NewPrometheusClient(promConfig.ServerEndpoint, promConfig.ClientConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to build prometheus client: %w", err)
	}

	// validation of the prometheus client

	m, err := Validate(promClient, promConfig)
	if err != nil || !m.Running {
		if err != nil {
			return nil, newHelpRetryError("failed to query prometheus at %s: %w", promConfig.ServerEndpoint, err)
		} else if !m.Running {
			return nil, newHelpRetryError("prometheus at %s is not running", promConfig.ServerEndpoint)
		}
	} else {
		log.Infof("Success: retrieved the 'up' query against prometheus at: %s", promConfig.ServerEndpoint)
	}

	// we don't consider this a fatal error, but we log for visibility
	api := prometheusAPI.NewAPI(promClient)
	_, err = api.Buildinfo(context.Background())
	if err != nil {
		log.Infof("No valid prometheus config file at %s. Error: %s.\nTroubleshooting help available at: %s.\n**Ignore if using cortex/mimir/thanos here**", promConfig.ServerEndpoint, err.Error(), PrometheusTroubleshootingURL)
	} else {
		log.Infof("Retrieved a prometheus config file from: %s", promConfig.ServerEndpoint)
	}

	// Fix scrape interval if zero by attempting to lookup the interval for the configured job
	if promConfig.ScrapeInterval == 0 {
		promConfig.ScrapeInterval = time.Minute

		// Lookup scrape interval for kubecost job, update if found
		si, err := ScrapeIntervalFor(promClient, promConfig.JobName)
		if err == nil {
			promConfig.ScrapeInterval = si
		}
	}

	log.Infof("Using scrape interval of %f", promConfig.ScrapeInterval.Seconds())

	promContexts := NewContextFactory(promClient, promConfig)

	var thanosClient prometheus.Client
	var thanosContexts *ContextFactory

	// if the thanos configuration is non-nil, we assume intent to use thanos. However, failure to
	// initialize the thanos client is not fatal, and we will log the error and continue.
	if thanosConfig != nil {
		thanosHost := thanosConfig.ServerEndpoint
		if thanosHost != "" {
			thanosCli, _ := NewThanosClient(thanosHost, thanosConfig)

			_, err = Validate(thanosCli, thanosConfig.OpenCostPrometheusConfig)
			if err != nil {
				log.Warnf("Failed to query Thanos at %s. Error: %s.", thanosHost, err.Error())
				thanosClient = thanosCli
			} else {
				log.Infof("Success: retrieved the 'up' query against Thanos at: %s", thanosHost)

				thanosClient = thanosCli
			}

			thanosContexts = NewContextFactory(thanosClient, thanosConfig.OpenCostPrometheusConfig)
		} else {
			log.Infof("Error resolving environment variable: $%s", env.ThanosQueryUrlEnvVar)
		}
	}

	return &PrometheusDataSource{
		promConfig:     promConfig,
		promClient:     promClient,
		promContexts:   promContexts,
		thanosConfig:   thanosConfig,
		thanosClient:   thanosClient,
		thanosContexts: thanosContexts,
	}, nil
}

var proto = protocol.HTTP()

// prometheusMetadata returns the metadata for the prometheus server
func (pds *PrometheusDataSource) prometheusMetadata(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	resp := proto.ToResponse(Validate(pds.promClient, pds.promConfig))
	proto.WriteResponse(w, resp)
}

// prometheusRecordingRules is a proxy for /rules against prometheus
func (pds *PrometheusDataSource) prometheusRecordingRules(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	u := pds.promClient.URL(epRules, nil)

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		fmt.Fprintf(w, "error creating Prometheus rule request: %s", err)
		return
	}

	_, body, err := pds.promClient.Do(r.Context(), req)
	if err != nil {
		fmt.Fprintf(w, "error making Prometheus rule request: %s", err)
		return
	}

	w.Write(body)
}

// prometheusConfig returns the current configuration of the prometheus server
func (pds *PrometheusDataSource) prometheusConfig(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	pConfig := map[string]string{
		"address": pds.promConfig.ServerEndpoint,
	}

	body, err := json.Marshal(pConfig)
	if err != nil {
		fmt.Fprintf(w, "Error marshalling prometheus config")
	} else {
		w.Write(body)
	}
}

// prometheusTargets is a proxy for /targets against prometheus
func (pds *PrometheusDataSource) prometheusTargets(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	u := pds.promClient.URL(epTargets, nil)

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		fmt.Fprintf(w, "error creating Prometheus rule request: %s", err)
		return
	}

	_, body, err := pds.promClient.Do(r.Context(), req)
	if err != nil {
		fmt.Fprintf(w, "error making Prometheus rule request: %s", err)
		return
	}

	w.Write(body)
}

// status returns the status of the prometheus client
func (pds *PrometheusDataSource) status(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	promServer := pds.promConfig.ServerEndpoint

	api := prometheusAPI.NewAPI(pds.promClient)
	result, err := api.Buildinfo(r.Context())
	if err != nil {
		fmt.Fprintf(w, "Using Prometheus at %s, Error: %s", promServer, err)
	} else {
		fmt.Fprintf(w, "Using Prometheus at %s, version: %s", promServer, result.Version)
	}
}

// prometheusQuery is a proxy for /query against prometheus
func (pds *PrometheusDataSource) prometheusQuery(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	qp := httputil.NewQueryParams(r.URL.Query())
	query := qp.Get("query", "")
	if query == "" {
		proto.WriteResponse(w, proto.ToResponse(nil, fmt.Errorf("Query Parameter 'query' is unset'")))
		return
	}

	// Attempt to parse time as either a unix timestamp or as an RFC3339 value
	var timeVal time.Time
	timeStr := qp.Get("time", "")
	if len(timeStr) > 0 {
		if t, err := strconv.ParseInt(timeStr, 10, 64); err == nil {
			timeVal = time.Unix(t, 0)
		} else if t, err := time.Parse(time.RFC3339, timeStr); err == nil {
			timeVal = t
		}

		// If time is given, but not parse-able, return an error
		if timeVal.IsZero() {
			http.Error(w, fmt.Sprintf("time must be a unix timestamp or RFC3339 value; illegal value given: %s", timeStr), http.StatusBadRequest)
		}
	}

	ctx := pds.promContexts.NewNamedContext(FrontendContextName)
	body, err := ctx.RawQuery(query, timeVal)
	if err != nil {
		proto.WriteResponse(w, proto.ToResponse(nil, fmt.Errorf("Error running query %s. Error: %s", query, err)))
		return
	}

	w.Write(body) // prometheusQueryRange is a proxy for /query_range against prometheus
}

func (pds *PrometheusDataSource) prometheusQueryRange(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	qp := httputil.NewQueryParams(r.URL.Query())
	query := qp.Get("query", "")
	if query == "" {
		fmt.Fprintf(w, "Error parsing query from request parameters.")
		return
	}

	start, end, duration, err := toStartEndStep(qp)
	if err != nil {
		fmt.Fprintf(w, "error: %s", err)
		return
	}

	ctx := pds.promContexts.NewNamedContext(FrontendContextName)
	body, err := ctx.RawQueryRange(query, start, end, duration)
	if err != nil {
		fmt.Fprintf(w, "Error running query %s. Error: %s", query, err)
		return
	}

	w.Write(body)
}

// thanosQuery is a proxy for /query against thanos
func (pds *PrometheusDataSource) thanosQuery(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	if pds.thanosClient == nil {
		proto.WriteResponse(w, proto.ToResponse(nil, fmt.Errorf("ThanosDisabled")))
		return
	}

	qp := httputil.NewQueryParams(r.URL.Query())
	query := qp.Get("query", "")
	if query == "" {
		proto.WriteResponse(w, proto.ToResponse(nil, fmt.Errorf("Query Parameter 'query' is unset'")))
		return
	}

	// Attempt to parse time as either a unix timestamp or as an RFC3339 value
	var timeVal time.Time
	timeStr := qp.Get("time", "")
	if len(timeStr) > 0 {
		if t, err := strconv.ParseInt(timeStr, 10, 64); err == nil {
			timeVal = time.Unix(t, 0)
		} else if t, err := time.Parse(time.RFC3339, timeStr); err == nil {
			timeVal = t
		}

		// If time is given, but not parse-able, return an error
		if timeVal.IsZero() {
			http.Error(w, fmt.Sprintf("time must be a unix timestamp or RFC3339 value; illegal value given: %s", timeStr), http.StatusBadRequest)
		}
	}

	ctx := pds.thanosContexts.NewNamedContext(FrontendContextName)
	body, err := ctx.RawQuery(query, timeVal)
	if err != nil {
		proto.WriteResponse(w, proto.ToResponse(nil, fmt.Errorf("Error running query %s. Error: %s", query, err)))
		return
	}

	w.Write(body)
}

// thanosQueryRange is a proxy for /query_range against thanos
func (pds *PrometheusDataSource) thanosQueryRange(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	if pds.thanosClient == nil {
		proto.WriteResponse(w, proto.ToResponse(nil, fmt.Errorf("ThanosDisabled")))
		return
	}

	qp := httputil.NewQueryParams(r.URL.Query())
	query := qp.Get("query", "")
	if query == "" {
		fmt.Fprintf(w, "Error parsing query from request parameters.")
		return
	}

	start, end, duration, err := toStartEndStep(qp)
	if err != nil {
		fmt.Fprintf(w, "error: %s", err)
		return
	}

	ctx := pds.thanosContexts.NewNamedContext(FrontendContextName)
	body, err := ctx.RawQueryRange(query, start, end, duration)
	if err != nil {
		fmt.Fprintf(w, "Error running query %s. Error: %s", query, err)
		return
	}

	w.Write(body)
}

// promtheusQueueState returns the current state of the prometheus and thanos request queues
func (pds *PrometheusDataSource) prometheusQueueState(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	promQueueState, err := GetPrometheusQueueState(pds.promClient, pds.promConfig)
	if err != nil {
		proto.WriteResponse(w, proto.ToResponse(nil, err))
		return
	}

	result := map[string]*PrometheusQueueState{
		"prometheus": promQueueState,
	}

	if pds.thanosClient != nil {
		thanosQueueState, err := GetPrometheusQueueState(pds.thanosClient, pds.thanosConfig.OpenCostPrometheusConfig)
		if err != nil {
			log.Warnf("Error getting Thanos queue state: %s", err)
		} else {
			result["thanos"] = thanosQueueState
		}
	}

	proto.WriteResponse(w, proto.ToResponse(result, nil))
}

// prometheusMetrics retrieves availability of Prometheus and Thanos metrics
func (pds *PrometheusDataSource) prometheusMetrics(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	promMetrics := GetPrometheusMetrics(pds.promClient, pds.promConfig, "")

	result := map[string][]*PrometheusDiagnostic{
		"prometheus": promMetrics,
	}

	if pds.thanosClient != nil {
		thanosMetrics := GetPrometheusMetrics(pds.thanosClient, pds.thanosConfig.OpenCostPrometheusConfig, pds.thanosConfig.Offset)
		result["thanos"] = thanosMetrics
	}

	proto.WriteResponse(w, proto.ToResponse(result, nil))
}

func (pds *PrometheusDataSource) PrometheusClient() prometheus.Client {
	return pds.promClient
}

func (pds *PrometheusDataSource) PrometheusConfig() *OpenCostPrometheusConfig {
	return pds.promConfig
}

func (pds *PrometheusDataSource) PrometheusContexts() *ContextFactory {
	return pds.promContexts
}

func (pds *PrometheusDataSource) ThanosClient() prometheus.Client {
	return pds.thanosClient
}

func (pds *PrometheusDataSource) ThanosConfig() *OpenCostThanosConfig {
	return pds.thanosConfig
}

func (pds *PrometheusDataSource) ThanosContexts() *ContextFactory {
	return pds.thanosContexts
}

func (pds *PrometheusDataSource) NewClusterMap(clusterInfoProvider clusters.ClusterInfoProvider) clusters.ClusterMap {
	if pds.thanosClient != nil {
		return newPrometheusClusterMap(pds.thanosContexts, clusterInfoProvider, 10*time.Minute)
	}

	return newPrometheusClusterMap(pds.promContexts, clusterInfoProvider, 5*time.Minute)
}

func (pds *PrometheusDataSource) RegisterEndPoints(router *httprouter.Router) {
	// endpoints migrated from server
	router.GET("/validatePrometheus", pds.prometheusMetadata)
	router.GET("/prometheusRecordingRules", pds.prometheusRecordingRules)
	router.GET("/prometheusConfig", pds.prometheusConfig)
	router.GET("/prometheusTargets", pds.prometheusTargets)
	router.GET("/status", pds.status)

	// prom query proxies
	router.GET("/prometheusQuery", pds.prometheusQuery)
	router.GET("/prometheusQueryRange", pds.prometheusQueryRange)
	router.GET("/thanosQuery", pds.thanosQuery)
	router.GET("/thanosQueryRange", pds.thanosQueryRange)

	// diagnostics
	router.GET("/diagnostics/requestQueue", pds.prometheusQueueState)
	router.GET("/diagnostics/prometheusMetrics", pds.prometheusMetrics)
}

func (pds *PrometheusDataSource) RefreshInterval() time.Duration {
	return pds.promConfig.ScrapeInterval
}

func (pds *PrometheusDataSource) BatchDuration() time.Duration {
	return pds.promConfig.MaxQueryDuration
}

func (pds *PrometheusDataSource) Resolution() time.Duration {
	return pds.promConfig.DataResolution
}

func (pds *PrometheusDataSource) MetaData() map[string]string {
	thanosEnabled := pds.thanosClient != nil

	metadata := map[string]string{
		clusters.ClusterInfoThanosEnabledKey: fmt.Sprintf("%t", thanosEnabled),
	}

	if thanosEnabled {
		metadata[clusters.ClusterInfoThanosOffsetKey] = pds.thanosConfig.Offset
	}

	return metadata
}

//--------------------------------------------------------------------------
//  InstantMetricsQuerier
//--------------------------------------------------------------------------

func (pds *PrometheusDataSource) QueryPVCost(start, end time.Time) source.QueryResultsChan {
	const pvCostQuery = `avg(avg_over_time(pv_hourly_cost{%s}[%s])) by (%s, persistentvolume,provider_id)`

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryPVCost")
	}

	queryPVCost := fmt.Sprintf(pvCostQuery, pds.promConfig.ClusterFilter, durStr, pds.promConfig.ClusterLabel)

	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return ctx.QueryAtTime(queryPVCost, end)
}

func (pds *PrometheusDataSource) QueryPVSize(start, end time.Time) source.QueryResultsChan {
	const pvSizeQuery = `avg(avg_over_time(kube_persistentvolume_capacity_bytes{%s}[%s])) by (%s, persistentvolume)`

	cfg := pds.promConfig
	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryPVCost")
	}

	queryPVSize := fmt.Sprintf(pvSizeQuery, cfg.ClusterFilter, durStr, cfg.ClusterLabel)

	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return ctx.QueryAtTime(queryPVSize, end)
}

func (pds *PrometheusDataSource) QueryPVStorageClass(start, end time.Time) source.QueryResultsChan {
	// `avg(avg_over_time(kubecost_pv_info{%s}[%s])) by (%s, persistentvolume, storageclass)`
	// , env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())

	const pvStorageSizeQuery = `avg(avg_over_time(kubecost_pv_info{%s}[%s])) by (%s, persistentvolume, storageclass)`
	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryPVStorageClass")
	}

	queryPVStorageClass := fmt.Sprintf(pvStorageSizeQuery, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return ctx.QueryAtTime(queryPVStorageClass, end)
}

func (pds *PrometheusDataSource) QueryPVUsedAverage(start, end time.Time) source.QueryResultsChan {
	// `avg(avg_over_time(kubelet_volume_stats_used_bytes{%s}[%s])) by (%s, persistentvolumeclaim, namespace)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())

	const pvUsedAverageQuery = `avg(avg_over_time(kubelet_volume_stats_used_bytes{%s}[%s])) by (%s, persistentvolumeclaim, namespace)`
	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryPVUsedAverage")
	}

	queryPVUsedAvg := fmt.Sprintf(pvUsedAverageQuery, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return ctx.QueryAtTime(queryPVUsedAvg, end)
}

func (pds *PrometheusDataSource) QueryPVUsedMax(start, end time.Time) source.QueryResultsChan {
	// `max(max_over_time(kubelet_volume_stats_used_bytes{%s}[%s])) by (%s, persistentvolumeclaim, namespace)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())

	const pvUsedMaxQuery = `max(max_over_time(kubelet_volume_stats_used_bytes{%s}[%s])) by (%s, persistentvolumeclaim, namespace)`
	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryPVUsedMax")
	}

	queryPVUsedMax := fmt.Sprintf(pvUsedMaxQuery, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return ctx.QueryAtTime(queryPVUsedMax, end)
}

func (pds *PrometheusDataSource) QueryPVCInfo(start, end time.Time) source.QueryResultsChan {
	// `avg(avg_over_time(kube_persistentvolumeclaim_info{%s}[%s])) by (%s, volumename, persistentvolumeclaim, namespace)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())

	const pvcInfoQuery = `avg(avg_over_time(kube_persistentvolumeclaim_info{%s}[%s])) by (%s, volumename, persistentvolumeclaim, namespace)`
	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryPVCInfo")
	}

	queryPVCInfo := fmt.Sprintf(pvcInfoQuery, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return ctx.QueryAtTime(queryPVCInfo, end)
}

func (pds *PrometheusDataSource) QueryPVActiveMinutes(start, end time.Time) source.QueryResultsChan {
	// `avg(kube_persistentvolume_capacity_bytes{%s}) by (%s, persistentvolume)[%s:%dm]`
	// env.GetPromClusterFilter(), env.GetPromClusterLabel(), durStr, minsPerResolution)
	const pvActiveMinsQuery = `avg(kube_persistentvolume_capacity_bytes{%s}) by (%s, persistentvolume)[%s:%dm]`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryPVActiveMinutes")
	}

	queryPVActiveMins := fmt.Sprintf(pvActiveMinsQuery, cfg.ClusterFilter, cfg.ClusterLabel, durStr, minsPerResolution)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return ctx.QueryAtTime(queryPVActiveMins, end)
}

func (pds *PrometheusDataSource) QueryLocalStorageCost(start, end time.Time) source.QueryResultsChan {
	// `sum_over_time(sum(container_fs_limit_bytes{device=~"/dev/(nvme|sda).*", id="/", %s}) by (instance, device, %s)[%s:%dm]) / 1024 / 1024 / 1024 * %f * %f`
	// env.GetPromClusterFilter(), env.GetPromClusterLabel(), durStr, minsPerResolution, hourlyToCumulative, costPerGBHr)

	const localStorageCostQuery = `sum_over_time(sum(container_fs_limit_bytes{device=~"/dev/(nvme|sda).*", id="/", %s}) by (instance, device, %s)[%s:%dm]) / 1024 / 1024 / 1024 * %f * %f`

	cfg := pds.promConfig
	resolution := cfg.DataResolution

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryLocalStorageCost")
	}

	//Ensuring if data resolution is less than 60s default it to 1m
	var minsPerResolution int
	if minsPerResolution = int(resolution.Minutes()); int(resolution.Minutes()) == 0 {
		minsPerResolution = 1
		log.DedupedWarningf(3, "QueryLocalStorageCost: Configured resolution (%d seconds) is below the 60 seconds threshold. Overriding with 1 minute.", int(resolution.Seconds()))
	}

	// hourlyToCumulative is a scaling factor that, when multiplied by an
	// hourly value, converts it to a cumulative value; i.e. [$/hr] *
	// [min/res]*[hr/min] = [$/res]
	hourlyToCumulative := float64(minsPerResolution) * (1.0 / 60.0)
	costPerGBHr := 0.04 / 730.0

	queryLocalStorageCost := fmt.Sprintf(localStorageCostQuery, cfg.ClusterFilter, cfg.ClusterLabel, durStr, minsPerResolution, hourlyToCumulative, costPerGBHr)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return ctx.QueryAtTime(queryLocalStorageCost, end)
}

func (pds *PrometheusDataSource) QueryLocalStorageUsedCost(start, end time.Time) source.QueryResultsChan {
	// `sum_over_time(sum(container_fs_usage_bytes{device=~"/dev/(nvme|sda).*", id="/", %s}) by (instance, device, %s)[%s:%dm]) / 1024 / 1024 / 1024 * %f * %f`
	// env.GetPromClusterFilter(), env.GetPromClusterLabel(), durStr, minsPerResolution, hourlyToCumulative, costPerGBHr)

	const localStorageUsedCostQuery = `sum_over_time(sum(container_fs_usage_bytes{device=~"/dev/(nvme|sda).*", id="/", %s}) by (instance, device, %s)[%s:%dm]) / 1024 / 1024 / 1024 * %f * %f`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryLocalStorageUsedCost")
	}

	// hourlyToCumulative is a scaling factor that, when multiplied by an
	// hourly value, converts it to a cumulative value; i.e. [$/hr] *
	// [min/res]*[hr/min] = [$/res]
	hourlyToCumulative := float64(minsPerResolution) * (1.0 / 60.0)
	costPerGBHr := 0.04 / 730.0

	queryLocalStorageUsedCost := fmt.Sprintf(localStorageUsedCostQuery, cfg.ClusterFilter, cfg.ClusterLabel, durStr, minsPerResolution, hourlyToCumulative, costPerGBHr)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return ctx.QueryAtTime(queryLocalStorageUsedCost, end)
}

func (pds *PrometheusDataSource) QueryLocalStorageUsedAvg(start, end time.Time) source.QueryResultsChan {
	// `avg(sum(avg_over_time(container_fs_usage_bytes{device=~"/dev/(nvme|sda).*", id="/", %s}[%s])) by (instance, device, %s, job)) by (instance, device, %s)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel(), env.GetPromClusterLabel())

	const localStorageUsedAvgQuery = `avg(sum(avg_over_time(container_fs_usage_bytes{device=~"/dev/(nvme|sda).*", id="/", %s}[%s])) by (instance, device, %s, job)) by (instance, device, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryLocalStorageUsedAvg")
	}

	queryLocalStorageUsedAvg := fmt.Sprintf(localStorageUsedAvgQuery, cfg.ClusterFilter, durStr, cfg.ClusterLabel, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return ctx.QueryAtTime(queryLocalStorageUsedAvg, end)
}

func (pds *PrometheusDataSource) QueryLocalStorageUsedMax(start, end time.Time) source.QueryResultsChan {
	// `max(sum(max_over_time(container_fs_usage_bytes{device=~"/dev/(nvme|sda).*", id="/", %s}[%s])) by (instance, device, %s, job)) by (instance, device, %s)`
	//  env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel(), env.GetPromClusterLabel())
	const localStorageUsedMaxQuery = `max(sum(max_over_time(container_fs_usage_bytes{device=~"/dev/(nvme|sda).*", id="/", %s}[%s])) by (instance, device, %s, job)) by (instance, device, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryLocalStorageUsedMax")
	}

	queryLocalStorageUsedMax := fmt.Sprintf(localStorageUsedMaxQuery, cfg.ClusterFilter, durStr, cfg.ClusterLabel, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return ctx.QueryAtTime(queryLocalStorageUsedMax, end)
}

func (pds *PrometheusDataSource) QueryLocalStorageBytes(start, end time.Time) source.QueryResultsChan {
	// `avg_over_time(sum(container_fs_limit_bytes{device=~"/dev/(nvme|sda).*", id="/", %s}) by (instance, device, %s)[%s:%dm])`
	// env.GetPromClusterFilter(), env.GetPromClusterLabel(), durStr, minsPerResolution)

	const localStorageBytesQuery = `avg_over_time(sum(container_fs_limit_bytes{device=~"/dev/(nvme|sda).*", id="/", %s}) by (instance, device, %s)[%s:%dm])`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryLocalStorageBytes")
	}

	queryLocalStorageBytes := fmt.Sprintf(localStorageBytesQuery, cfg.ClusterFilter, cfg.ClusterLabel, durStr, minsPerResolution)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return ctx.QueryAtTime(queryLocalStorageBytes, end)
}

func (pds *PrometheusDataSource) QueryLocalStorageActiveMinutes(start, end time.Time) source.QueryResultsChan {
	// `count(node_total_hourly_cost{%s}) by (%s, node)[%s:%dm]`
	// env.GetPromClusterFilter(), env.GetPromClusterLabel(), durStr, minsPerResolution)

	const localStorageActiveMinutesQuery = `count(node_total_hourly_cost{%s}) by (%s, node)[%s:%dm]`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryLocalStorageActiveMinutes")
	}

	queryLocalStorageActiveMins := fmt.Sprintf(localStorageActiveMinutesQuery, cfg.ClusterFilter, cfg.ClusterLabel, durStr, minsPerResolution)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return ctx.QueryAtTime(queryLocalStorageActiveMins, end)
}

func (pds *PrometheusDataSource) QueryLocalStorageBytesByProvider(provider string, start, end time.Time) source.QueryResultsChan {
	var localStorageBytesQuery string

	key := strings.ToLower(provider)
	if f, ok := providerStorageQueries[key]; ok {
		localStorageBytesQuery = f(pds.promConfig, start, end, false, false)
	} else {
		localStorageBytesQuery = ""
	}

	if localStorageBytesQuery == "" {
		return newEmptyResult()
	}

	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return ctx.QueryAtTime(localStorageBytesQuery, end)
}

func (pds *PrometheusDataSource) QueryLocalStorageUsedByProvider(provider string, start, end time.Time) source.QueryResultsChan {
	var localStorageUsedQuery string

	key := strings.ToLower(provider)
	if f, ok := providerStorageQueries[key]; ok {
		localStorageUsedQuery = f(pds.promConfig, start, end, false, true)
	} else {
		localStorageUsedQuery = ""
	}

	if localStorageUsedQuery == "" {
		return newEmptyResult()
	}

	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return ctx.QueryAtTime(localStorageUsedQuery, end)
}

func (pds *PrometheusDataSource) QueryNodeCPUHourlyCost(start, end time.Time) source.QueryResultsChan {
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())

	const nodeCPUHourlyCostQuery = `avg(avg_over_time(node_cpu_hourly_cost{%s}[%s])) by (%s, node, instance_type, provider_id)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNodeCPUHourlyCost")
	}

	queryNodeCPUHourlyCost := fmt.Sprintf(nodeCPUHourlyCostQuery, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return ctx.QueryAtTime(queryNodeCPUHourlyCost, end)
}

func (pds *PrometheusDataSource) QueryNodeCPUCoresCapacity(start, end time.Time) source.QueryResultsChan {
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())

	const nodeCPUCoresCapacityQuery = `avg(avg_over_time(kube_node_status_capacity_cpu_cores{%s}[%s])) by (%s, node)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNodeCPUCoresCapacity")
	}

	queryNodeCPUCoresCapacity := fmt.Sprintf(nodeCPUCoresCapacityQuery, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return ctx.QueryAtTime(queryNodeCPUCoresCapacity, end)
}

func (pds *PrometheusDataSource) QueryNodeCPUCoresAllocatable(start, end time.Time) source.QueryResultsChan {
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())

	const nodeCPUCoresAllocatableQuery = `avg(avg_over_time(kube_node_status_allocatable_cpu_cores{%s}[%s])) by (%s, node)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNodeCPUCoresAllocatable")
	}

	queryNodeCPUCoresAllocatable := fmt.Sprintf(nodeCPUCoresAllocatableQuery, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return ctx.QueryAtTime(queryNodeCPUCoresAllocatable, end)
}

func (pds *PrometheusDataSource) QueryNodeRAMHourlyCost(start, end time.Time) source.QueryResultsChan {
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())

	const nodeRAMHourlyCostQuery = `avg(avg_over_time(node_ram_hourly_cost{%s}[%s])) by (%s, node, instance_type, provider_id) / 1024 / 1024 / 1024`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNodeRAMHourlyCost")
	}

	queryNodeRAMHourlyCost := fmt.Sprintf(nodeRAMHourlyCostQuery, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return ctx.QueryAtTime(queryNodeRAMHourlyCost, end)
}

func (pds *PrometheusDataSource) QueryNodeRAMBytesCapacity(start, end time.Time) source.QueryResultsChan {
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())

	const nodeRAMBytesCapacityQuery = `avg(avg_over_time(kube_node_status_capacity_memory_bytes{%s}[%s])) by (%s, node)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNodeRAMBytesCapacity")
	}

	queryNodeRAMBytesCapacity := fmt.Sprintf(nodeRAMBytesCapacityQuery, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return ctx.QueryAtTime(queryNodeRAMBytesCapacity, end)
}

func (pds *PrometheusDataSource) QueryNodeRAMBytesAllocatable(start, end time.Time) source.QueryResultsChan {
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())

	const nodeRAMBytesAllocatableQuery = `avg(avg_over_time(kube_node_status_allocatable_memory_bytes{%s}[%s])) by (%s, node)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNodeRAMBytesAllocatable")
	}

	queryNodeRAMBytesAllocatable := fmt.Sprintf(nodeRAMBytesAllocatableQuery, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return ctx.QueryAtTime(queryNodeRAMBytesAllocatable, end)
}

func (pds *PrometheusDataSource) QueryNodeGPUCount(start, end time.Time) source.QueryResultsChan {
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())

	const nodeGPUCountQuery = `avg(avg_over_time(node_gpu_count{%s}[%s])) by (%s, node, provider_id)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNodeGPUCount")
	}

	queryNodeGPUCount := fmt.Sprintf(nodeGPUCountQuery, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return ctx.QueryAtTime(queryNodeGPUCount, end)
}

func (pds *PrometheusDataSource) QueryNodeGPUHourlyCost(start, end time.Time) source.QueryResultsChan {
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())

	const nodeGPUHourlyCostQuery = `avg(avg_over_time(node_gpu_hourly_cost{%s}[%s])) by (%s, node, instance_type, provider_id)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNodeGPUHourlyCost")
	}

	queryNodeGPUHourlyCost := fmt.Sprintf(nodeGPUHourlyCostQuery, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return ctx.QueryAtTime(queryNodeGPUHourlyCost, end)
}

func (pds *PrometheusDataSource) QueryNodeLabels(start, end time.Time) source.QueryResultsChan {
	// env.GetPromClusterFilter(), durStr, minsPerResolution)

	const labelsQuery = `count_over_time(kube_node_labels{%s}[%s:%dm])`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNodeLabels")
	}

	queryLabels := fmt.Sprintf(labelsQuery, cfg.ClusterFilter, durStr, minsPerResolution)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return ctx.QueryAtTime(queryLabels, end)
}

func (pds *PrometheusDataSource) QueryNodeActiveMinutes(start, end time.Time) source.QueryResultsChan {
	// env.GetPromClusterFilter(), env.GetPromClusterLabel(), durStr, minsPerResolution)

	const activeMinsQuery = `avg(node_total_hourly_cost{%s}) by (node, %s, provider_id)[%s:%dm]`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNodeActiveMinutes")
	}

	queryActiveMins := fmt.Sprintf(activeMinsQuery, cfg.ClusterFilter, cfg.ClusterLabel, durStr, minsPerResolution)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return ctx.QueryAtTime(queryActiveMins, end)
}

func (pds *PrometheusDataSource) QueryNodeIsSpot(start, end time.Time) source.QueryResultsChan {
	// env.GetPromClusterFilter(), durStr, minsPerResolution)

	const isSpotQuery = `avg_over_time(kubecost_node_is_spot{%s}[%s:%dm])`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNodeIsSpot")
	}

	queryIsSpot := fmt.Sprintf(isSpotQuery, cfg.ClusterFilter, durStr, minsPerResolution)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return ctx.QueryAtTime(queryIsSpot, end)
}

func (pds *PrometheusDataSource) QueryNodeCPUModeTotal(start, end time.Time) source.QueryResultsChan {
	// env.GetPromClusterFilter(), durStr, minsPerResolution, env.GetPromClusterLabel())

	const nodeCPUModeTotalQuery = `sum(rate(node_cpu_seconds_total{%s}[%s:%dm])) by (kubernetes_node, %s, mode)`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNodeCPUModeTotal")
	}

	queryCPUModeTotal := fmt.Sprintf(nodeCPUModeTotalQuery, cfg.ClusterFilter, durStr, minsPerResolution, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return ctx.QueryAtTime(queryCPUModeTotal, end)
}

func (pds *PrometheusDataSource) QueryNodeCPUModePercent(start, end time.Time) source.QueryResultsChan {
	const fmtQueryCPUModePct = `
		sum(rate(node_cpu_seconds_total{%s}[%s])) by (%s, mode) / ignoring(mode)
		group_left sum(rate(node_cpu_seconds_total{%s}[%s])) by (%s)
	`
	// env.GetPromClusterFilter(), windowStr, env.GetPromClusterLabel(), env.GetPromClusterFilter(), windowStr, fmtOffset, env.GetPromClusterLabel()

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNodeCPUModePercent")
	}

	queryCPUModePct := fmt.Sprintf(fmtQueryCPUModePct, cfg.ClusterFilter, durStr, cfg.ClusterLabel, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return ctx.QueryAtTime(queryCPUModePct, end)
}

func (pds *PrometheusDataSource) QueryNodeRAMSystemPercent(start, end time.Time) source.QueryResultsChan {
	// env.GetPromClusterFilter(), durStr, minsPerResolution, env.GetPromClusterLabel(), env.GetPromClusterFilter(), durStr, minsPerResolution, env.GetPromClusterLabel(), env.GetPromClusterLabel())

	const nodeRAMSystemPctQuery = `sum(sum_over_time(container_memory_working_set_bytes{container_name!="POD",container_name!="",namespace="kube-system", %s}[%s:%dm])) by (instance, %s) / avg(label_replace(sum(sum_over_time(kube_node_status_capacity_memory_bytes{%s}[%s:%dm])) by (node, %s), "instance", "$1", "node", "(.*)")) by (instance, %s)`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNodeRAMSystemPercent")
	}

	queryRAMSystemPct := fmt.Sprintf(nodeRAMSystemPctQuery, cfg.ClusterFilter, durStr, minsPerResolution, cfg.ClusterLabel, cfg.ClusterFilter, durStr, minsPerResolution, cfg.ClusterLabel, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return ctx.QueryAtTime(queryRAMSystemPct, end)
}

func (pds *PrometheusDataSource) QueryNodeRAMUserPercent(start, end time.Time) source.QueryResultsChan {
	// env.GetPromClusterFilter(), durStr, minsPerResolution, env.GetPromClusterLabel(), env.GetPromClusterFilter(), durStr, minsPerResolution, env.GetPromClusterLabel(), env.GetPromClusterLabel())

	const nodeRAMUserPctQuery = `sum(sum_over_time(container_memory_working_set_bytes{container_name!="POD",container_name!="",namespace!="kube-system", %s}[%s:%dm])) by (instance, %s) / avg(label_replace(sum(sum_over_time(kube_node_status_capacity_memory_bytes{%s}[%s:%dm])) by (node, %s), "instance", "$1", "node", "(.*)")) by (instance, %s)`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNodeRAMUserPercent")
	}

	queryRAMUserPct := fmt.Sprintf(nodeRAMUserPctQuery, cfg.ClusterFilter, durStr, minsPerResolution, cfg.ClusterLabel, cfg.ClusterFilter, durStr, minsPerResolution, cfg.ClusterLabel, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return ctx.QueryAtTime(queryRAMUserPct, end)
}

func (pds *PrometheusDataSource) QueryLBPricePerHr(start, end time.Time) source.QueryResultsChan {
	const queryFmtLBCostPerHr = `avg(avg_over_time(kubecost_load_balancer_cost{%s}[%s])) by (namespace, service_name, ingress_ip, %s)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryLBPricePerHr")
	}

	queryLBCostPerHr := fmt.Sprintf(queryFmtLBCostPerHr, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return ctx.QueryAtTime(queryLBCostPerHr, end)
}

func (pds *PrometheusDataSource) QueryLBActiveMinutes(start, end time.Time) source.QueryResultsChan {
	const lbActiveMinutesQuery = `avg(kubecost_load_balancer_cost{%s}) by (namespace, service_name, %s, ingress_ip)[%s:%dm]`
	// env.GetPromClusterFilter(), env.GetPromClusterLabel(), durStr, minsPerResolution)

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryLBActiveMinutes")
	}

	queryLBActiveMins := fmt.Sprintf(lbActiveMinutesQuery, cfg.ClusterFilter, cfg.ClusterLabel, durStr, minsPerResolution)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return ctx.QueryAtTime(queryLBActiveMins, end)
}

func (pds *PrometheusDataSource) QueryClusterManagementDuration(start, end time.Time) source.QueryResultsChan {
	const clusterManagementDurationQuery = `avg(kubecost_cluster_management_cost{%s}) by (%s, provisioner_name)[%s:%dm]`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryClusterManagementDuration")
	}

	queryClusterManagementDuration := fmt.Sprintf(clusterManagementDurationQuery, cfg.ClusterFilter, cfg.ClusterLabel, durStr, minsPerResolution)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return ctx.QueryAtTime(queryClusterManagementDuration, end)
}

func (pds *PrometheusDataSource) QueryClusterManagementPricePerHr(start, end time.Time) source.QueryResultsChan {
	const clusterManagementCostQuery = `avg(avg_over_time(kubecost_cluster_management_cost{%s}[%s])) by (%s, provisioner_name)`
	// env.GetPromClusterFilter(), durationStr, env.GetPromClusterLabel()

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryClusterManagementCost")
	}

	queryClusterManagementCost := fmt.Sprintf(clusterManagementCostQuery, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return ctx.QueryAtTime(queryClusterManagementCost, end)
}

func (pds *PrometheusDataSource) QueryDataCount(start, end time.Time) source.QueryResultsChan {
	const fmtQueryDataCount = `
		count_over_time(sum(kube_node_status_capacity_cpu_cores{%s}) by (%s)[%s:%dm]) * %d
	`
	// env.GetPromClusterFilter(), env.GetPromClusterLabel(), windowStr, minsPerResolution, minsPerResolution)

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryDataCount")
	}

	queryDataCount := fmt.Sprintf(fmtQueryDataCount, cfg.ClusterFilter, cfg.ClusterLabel, durStr, minsPerResolution, minsPerResolution)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return ctx.QueryAtTime(queryDataCount, end)
}

func (pds *PrometheusDataSource) QueryTotalGPU(start, end time.Time) source.QueryResultsChan {
	const fmtQueryTotalGPU = `
		sum(
			sum_over_time(node_gpu_hourly_cost{%s}[%s:%dm]) * %f
		) by (%s)
	`
	// env.GetPromClusterFilter(), windowStr, minsPerResolution, fmtOffset, hourlyToCumulative, env.GetPromClusterLabel())

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryTotalGPU")
	}

	// hourlyToCumulative is a scaling factor that, when multiplied by an hourly
	// value, converts it to a cumulative value; i.e.
	// [$/hr] * [min/res]*[hr/min] = [$/res]
	hourlyToCumulative := float64(minsPerResolution) * (1.0 / 60.0)

	queryTotalGPU := fmt.Sprintf(fmtQueryTotalGPU, cfg.ClusterFilter, durStr, minsPerResolution, hourlyToCumulative, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return ctx.QueryAtTime(queryTotalGPU, end)
}

func (pds *PrometheusDataSource) QueryTotalCPU(start, end time.Time) source.QueryResultsChan {
	const fmtQueryTotalCPU = `
		sum(
			sum_over_time(avg(kube_node_status_capacity_cpu_cores{%s}) by (node, %s)[%s:%dm]) *
			avg(avg_over_time(node_cpu_hourly_cost{%s}[%s:%dm])) by (node, %s) * %f
		) by (%s)
	`
	// env.GetPromClusterFilter(), env.GetPromClusterLabel(), windowStr, minsPerResolution, fmtOffset, env.GetPromClusterFilter(), windowStr, minsPerResolution, fmtOffset, env.GetPromClusterLabel(), hourlyToCumulative, env.GetPromClusterLabel()

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryTotalCPU")
	}

	// hourlyToCumulative is a scaling factor that, when multiplied by an hourly
	// value, converts it to a cumulative value; i.e.
	// [$/hr] * [min/res]*[hr/min] = [$/res]
	hourlyToCumulative := float64(minsPerResolution) * (1.0 / 60.0)

	queryTotalCPU := fmt.Sprintf(fmtQueryTotalCPU, cfg.ClusterFilter, cfg.ClusterLabel, durStr, minsPerResolution, cfg.ClusterFilter, durStr, minsPerResolution, cfg.ClusterLabel, hourlyToCumulative, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return ctx.QueryAtTime(queryTotalCPU, end)
}

func (pds *PrometheusDataSource) QueryTotalRAM(start, end time.Time) source.QueryResultsChan {
	const fmtQueryTotalRAM = `
		sum(
			sum_over_time(avg(kube_node_status_capacity_memory_bytes{%s}) by (node, %s)[%s:%dm]) / 1024 / 1024 / 1024 *
			avg(avg_over_time(node_ram_hourly_cost{%s}[%s:%dm])) by (node, %s) * %f
		) by (%s)
	`
	// env.GetPromClusterFilter(), env.GetPromClusterLabel(), windowStr, minsPerResolution, env.GetPromClusterFilter(), windowStr, minsPerResolution, env.GetPromClusterLabel(), hourlyToCumulative, env.GetPromClusterLabel())

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryTotalRAM")
	}

	// hourlyToCumulative is a scaling factor that, when multiplied by an hourly
	// value, converts it to a cumulative value; i.e.
	// [$/hr] * [min/res]*[hr/min] = [$/res]
	hourlyToCumulative := float64(minsPerResolution) * (1.0 / 60.0)

	queryTotalRAM := fmt.Sprintf(fmtQueryTotalRAM, cfg.ClusterFilter, cfg.ClusterLabel, durStr, minsPerResolution, cfg.ClusterFilter, durStr, minsPerResolution, cfg.ClusterLabel, hourlyToCumulative, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return ctx.QueryAtTime(queryTotalRAM, end)
}

func (pds *PrometheusDataSource) QueryTotalStorage(start, end time.Time) source.QueryResultsChan {
	const fmtQueryTotalStorage = `
		sum(
			sum_over_time(avg(kube_persistentvolume_capacity_bytes{%s}) by (persistentvolume, %s)[%s:%dm]) / 1024 / 1024 / 1024 *
			avg(avg_over_time(pv_hourly_cost{%s}[%s:%dm])) by (persistentvolume, %s) * %f
		) by (%s)
	`
	// env.GetPromClusterFilter(), env.GetPromClusterLabel(), windowStr, minsPerResolution, env.GetPromClusterFilter(), windowStr, minsPerResolution, env.GetPromClusterLabel(), hourlyToCumulative, env.GetPromClusterLabel())

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryTotalStorage")
	}

	// hourlyToCumulative is a scaling factor that, when multiplied by an hourly
	// value, converts it to a cumulative value; i.e.
	// [$/hr] * [min/res]*[hr/min] = [$/res]
	hourlyToCumulative := float64(minsPerResolution) * (1.0 / 60.0)

	queryTotalStorage := fmt.Sprintf(fmtQueryTotalStorage, cfg.ClusterFilter, cfg.ClusterLabel, durStr, minsPerResolution, cfg.ClusterFilter, durStr, minsPerResolution, cfg.ClusterLabel, hourlyToCumulative, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return ctx.QueryAtTime(queryTotalStorage, end)
}

func (pds *PrometheusDataSource) QueryClusterCores(start, end time.Time, step time.Duration) source.QueryResultsChan {
	const queryClusterCores = `sum(
		avg(avg_over_time(kube_node_status_capacity_cpu_cores{%s}[%s])) by (node, %s) * avg(avg_over_time(node_cpu_hourly_cost{%s}[%s])) by (node, %s) * 730 +
		avg(avg_over_time(node_gpu_hourly_cost{%s}[%s])) by (node, %s) * 730
	  ) by (%s)`
	// env.GetPromClusterFilter(), fmtWindow, env.GetPromClusterLabel(), env.GetPromClusterFilter(), fmtWindow, env.GetPromClusterLabel(), env.GetPromClusterFilter(), fmtWindow,  env.GetPromClusterLabel(), env.GetPromClusterLabel())

	cfg := pds.promConfig
	durStr := timeutil.DurationString(step)

	if durStr == "" {
		panic("failed to parse duration string passed to QueryClusterCores")
	}

	clusterCoresQuery := fmt.Sprintf(queryClusterCores, cfg.ClusterFilter, durStr, cfg.ClusterLabel, cfg.ClusterFilter, durStr, cfg.ClusterLabel, cfg.ClusterFilter, durStr, cfg.ClusterLabel, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return ctx.QueryRange(clusterCoresQuery, start, end, step)
}

func (pds *PrometheusDataSource) QueryClusterRAM(start, end time.Time, step time.Duration) source.QueryResultsChan {
	const queryClusterRAM = `sum(
		avg(avg_over_time(kube_node_status_capacity_memory_bytes{%s}[%s])) by (node, %s) / 1024 / 1024 / 1024 * avg(avg_over_time(node_ram_hourly_cost{%s}[%s])) by (node, %s) * 730
	  ) by (%s)`
	//  env.GetPromClusterFilter(), fmtWindow, env.GetPromClusterLabel(), env.GetPromClusterFilter(), fmtWindow, env.GetPromClusterLabel(), env.GetPromClusterLabel())

	cfg := pds.promConfig
	durStr := timeutil.DurationString(step)

	if durStr == "" {
		panic("failed to parse duration string passed to QueryClusterRAM")
	}

	clusterRAMQuery := fmt.Sprintf(queryClusterRAM, cfg.ClusterFilter, durStr, cfg.ClusterLabel, cfg.ClusterFilter, durStr, cfg.ClusterLabel, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return ctx.QueryRange(clusterRAMQuery, start, end, step)
}

func (pds *PrometheusDataSource) QueryClusterStorage(start, end time.Time, step time.Duration) source.QueryResultsChan {
	return pds.QueryClusterStorageByProvider("", start, end, step)
}

func (pds *PrometheusDataSource) QueryClusterStorageByProvider(provider string, start, end time.Time, step time.Duration) source.QueryResultsChan {
	const queryStorage = `sum(
		avg(avg_over_time(pv_hourly_cost{%s}[%s])) by (persistentvolume, %s) * 730
		* avg(avg_over_time(kube_persistentvolume_capacity_bytes{%s}[%s])) by (persistentvolume, %s) / 1024 / 1024 / 1024
	  ) by (%s) %s`
	// env.GetPromClusterFilter(), fmtWindow, env.GetPromClusterLabel(), env.GetPromClusterFilter(), fmtWindow, env.GetPromClusterLabel(), env.GetPromClusterLabel(), localStorageQuery)

	var localStorageQuery string
	if provider != "" {
		key := strings.ToLower(provider)
		if f, ok := providerStorageQueries[key]; ok {
			localStorageQuery = f(pds.promConfig, start, end, true, false)
		} else {
			localStorageQuery = ""
		}
	}

	if localStorageQuery != "" {
		localStorageQuery = fmt.Sprintf(" + %s", localStorageQuery)
	}

	cfg := pds.promConfig
	durStr := timeutil.DurationString(step)

	if durStr == "" {
		panic("failed to parse duration string passed to QueryClusterCores")
	}

	clusterStorageQuery := fmt.Sprintf(queryStorage, cfg.ClusterFilter, durStr, cfg.ClusterLabel, cfg.ClusterFilter, durStr, cfg.ClusterLabel, cfg.ClusterLabel, localStorageQuery)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return ctx.QueryRange(clusterStorageQuery, start, end, step)
}

func (pds *PrometheusDataSource) QueryClusterTotal(start, end time.Time, step time.Duration) source.QueryResultsChan {
	return pds.QueryClusterTotalByProvider("", start, end, step)
}

func (pds *PrometheusDataSource) QueryClusterTotalByProvider(provider string, start, end time.Time, step time.Duration) source.QueryResultsChan {
	const queryTotal = `sum(avg(node_total_hourly_cost{%s}) by (node, %s)) * 730 +
	  sum(
		avg(avg_over_time(pv_hourly_cost{%s}[1h])) by (persistentvolume, %s) * 730
		* avg(avg_over_time(kube_persistentvolume_capacity_bytes{%s}[1h])) by (persistentvolume, %s) / 1024 / 1024 / 1024
	  ) by (%s) %s`

	var localStorageQuery string
	if provider != "" {
		key := strings.ToLower(provider)
		if f, ok := providerStorageQueries[key]; ok {
			localStorageQuery = f(pds.promConfig, start, end, true, false)
		} else {
			localStorageQuery = ""
		}
	}

	if localStorageQuery != "" {
		localStorageQuery = fmt.Sprintf(" + %s", localStorageQuery)
	}

	cfg := pds.promConfig

	durStr := timeutil.DurationString(step)
	if durStr == "" {
		panic("failed to parse duration string passed to QueryClusterTotalByProvider")
	}

	clusterTotalQuery := fmt.Sprintf(queryTotal, cfg.ClusterFilter, cfg.ClusterLabel, cfg.ClusterFilter, cfg.ClusterLabel, cfg.ClusterFilter, cfg.ClusterLabel, cfg.ClusterLabel, localStorageQuery)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return ctx.QueryRange(clusterTotalQuery, start, end, step)
}

func (pds *PrometheusDataSource) QueryClusterNodes(start, end time.Time, step time.Duration) source.QueryResultsChan {
	return pds.QueryClusterNodesByProvider("", start, end, step)
}

func (pds *PrometheusDataSource) QueryClusterNodesByProvider(provider string, start, end time.Time, step time.Duration) source.QueryResultsChan {
	const queryNodes = `sum(avg(node_total_hourly_cost{%s}) by (node, %s)) * 730 %s`
	// env.GetPromClusterFilter(), env.GetPromClusterLabel(), localStorageQuery)

	var localStorageQuery string
	if provider != "" {
		key := strings.ToLower(provider)
		if f, ok := providerStorageQueries[key]; ok {
			localStorageQuery = f(pds.promConfig, start, end, true, false)
		} else {
			localStorageQuery = ""
		}
	}

	if localStorageQuery != "" {
		localStorageQuery = fmt.Sprintf(" + %s", localStorageQuery)
	}

	cfg := pds.promConfig

	durStr := timeutil.DurationString(step)
	if durStr == "" {
		panic("failed to parse duration string passed to QueryClusterNodesByProvider")
	}

	clusterNodesCostQuery := fmt.Sprintf(queryNodes, cfg.ClusterFilter, cfg.ClusterLabel, localStorageQuery)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return ctx.QueryRange(clusterNodesCostQuery, start, end, step)
}

// AllocationMetricQuerier

func (pds *PrometheusDataSource) QueryPods(start, end time.Time) source.QueryResultsChan {
	const queryFmtPods = `avg(kube_pod_container_status_running{%s} != 0) by (pod, namespace, %s)[%s:%s]`
	// env.GetPromClusterFilter(), env.GetPromClusterLabel(), durStr, resStr)

	cfg := pds.promConfig
	resolution := cfg.DataResolution
	resStr := timeutil.DurationString(resolution)

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryPods")
	}

	queryPods := fmt.Sprintf(queryFmtPods, cfg.ClusterFilter, cfg.ClusterLabel, durStr, resStr)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return ctx.QueryAtTime(queryPods, end)
}

func (pds *PrometheusDataSource) QueryPodsUID(start, end time.Time) source.QueryResultsChan {
	const queryFmtPodsUID = `avg(kube_pod_container_status_running{%s} != 0) by (pod, namespace, uid, %s)[%s:%s]`
	// env.GetPromClusterFilter(), env.GetPromClusterLabel(), durStr, resStr)

	cfg := pds.promConfig
	resolution := cfg.DataResolution
	resStr := timeutil.DurationString(resolution)

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryPodsUID")
	}

	queryPodsUID := fmt.Sprintf(queryFmtPodsUID, cfg.ClusterFilter, cfg.ClusterLabel, durStr, resStr)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return ctx.QueryAtTime(queryPodsUID, end)
}

func (pds *PrometheusDataSource) QueryRAMBytesAllocated(start, end time.Time) source.QueryResultsChan {
	const queryFmtRAMBytesAllocated = `avg(avg_over_time(container_memory_allocation_bytes{container!="", container!="POD", node!="", %s}[%s])) by (container, pod, namespace, node, %s, provider_id)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryRAMBytesAllocated")
	}

	queryRAMBytesAllocated := fmt.Sprintf(queryFmtRAMBytesAllocated, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return ctx.QueryAtTime(queryRAMBytesAllocated, end)
}

func (pds *PrometheusDataSource) QueryRAMRequests(start, end time.Time) source.QueryResultsChan {
	const queryFmtRAMRequests = `avg(avg_over_time(kube_pod_container_resource_requests{resource="memory", unit="byte", container!="", container!="POD", node!="", %s}[%s])) by (container, pod, namespace, node, %s)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel()

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryRAMRequests")
	}

	queryRAMRequests := fmt.Sprintf(queryFmtRAMRequests, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return ctx.QueryAtTime(queryRAMRequests, end)
}

func (pds *PrometheusDataSource) QueryRAMUsageAvg(start, end time.Time) source.QueryResultsChan {
	const queryFmtRAMUsageAvg = `avg(avg_over_time(container_memory_working_set_bytes{container!="", container_name!="POD", container!="POD", %s}[%s])) by (container_name, container, pod_name, pod, namespace, instance, %s)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel()

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryRAMUsageAvg")
	}

	queryRAMUsageAvg := fmt.Sprintf(queryFmtRAMUsageAvg, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return ctx.QueryAtTime(queryRAMUsageAvg, end)
}

func (pds *PrometheusDataSource) QueryRAMUsageMax(start, end time.Time) source.QueryResultsChan {
	const queryFmtRAMUsageMax = `max(max_over_time(container_memory_working_set_bytes{container!="", container_name!="POD", container!="POD", %s}[%s])) by (container_name, container, pod_name, pod, namespace, instance, %s)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel()

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryRAMUsageMax")
	}

	queryRAMUsageMax := fmt.Sprintf(queryFmtRAMUsageMax, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return ctx.QueryAtTime(queryRAMUsageMax, end)
}

func (pds *PrometheusDataSource) QueryCPUCoresAllocated(start, end time.Time) source.QueryResultsChan {
	const queryFmtCPUCoresAllocated = `avg(avg_over_time(container_cpu_allocation{container!="", container!="POD", node!="", %s}[%s])) by (container, pod, namespace, node, %s)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel()

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryCPUCoresAllocated")
	}

	queryCPUCoresAllocated := fmt.Sprintf(queryFmtCPUCoresAllocated, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return ctx.QueryAtTime(queryCPUCoresAllocated, end)
}

func (pds *PrometheusDataSource) QueryCPURequests(start, end time.Time) source.QueryResultsChan {
	const queryFmtCPURequests = `avg(avg_over_time(kube_pod_container_resource_requests{resource="cpu", unit="core", container!="", container!="POD", node!="", %s}[%s])) by (container, pod, namespace, node, %s)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel()

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryCPURequests")
	}

	queryCPURequests := fmt.Sprintf(queryFmtCPURequests, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return ctx.QueryAtTime(queryCPURequests, end)
}

func (pds *PrometheusDataSource) QueryCPUUsageAvg(start, end time.Time) source.QueryResultsChan {
	const queryFmtCPUUsageAvg = `avg(rate(container_cpu_usage_seconds_total{container!="", container_name!="POD", container!="POD", %s}[%s])) by (container_name, container, pod_name, pod, namespace, instance, %s)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel()

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryCPUUsageAvg")
	}

	queryCPUUsageAvg := fmt.Sprintf(queryFmtCPUUsageAvg, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return ctx.QueryAtTime(queryCPUUsageAvg, end)
}

func (pds *PrometheusDataSource) QueryCPUUsageMax(start, end time.Time) source.QueryResultsChan {
	// Because we use container_cpu_usage_seconds_total to calculate CPU usage
	// at any given "instant" of time, we need to use an irate or rate. To then
	// calculate a max (or any aggregation) we have to perform an aggregation
	// query on top of an instant-by-instant maximum. Prometheus supports this
	// type of query with a "subquery" [1], however it is reportedly expensive
	// to make such a query. By default, Kubecost's Prometheus config includes
	// a recording rule that keeps track of the instant-by-instant irate for CPU
	// usage. The metric in this query is created by that recording rule.
	//
	// [1] https://prometheus.io/blog/2019/01/28/subquery-support/
	//
	// If changing the name of the recording rule, make sure to update the
	// corresponding diagnostic query to avoid confusion.
	const queryFmtCPUUsageMaxRecordingRule = `max(max_over_time(kubecost_container_cpu_usage_irate{%s}[%s])) by (container_name, container, pod_name, pod, namespace, instance, %s)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel()

	// This is the subquery equivalent of the above recording rule query. It is
	// more expensive, but does not require the recording rule. It should be
	// used as a fallback query if the recording rule data does not exist.
	//
	// The parameter after the colon [:<thisone>] in the subquery affects the
	// resolution of the subquery.
	// The parameter after the metric ...{}[<thisone>] should be set to 2x
	// the resolution, to make sure the irate always has two points to query
	// in case the Prom scrape duration has been reduced to be equal to the
	// ETL resolution.
	const queryFmtCPUUsageMaxSubquery = `max(max_over_time(irate(container_cpu_usage_seconds_total{container!="POD", container!="", %s}[%s])[%s:%s])) by (container, pod_name, pod, namespace, instance, %s)`
	// env.GetPromClusterFilter(), doubleResStr, durStr, resStr, env.GetPromClusterLabel()

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryCPUUsageMax")
	}

	queryCPUUsageMaxRecordingRule := fmt.Sprintf(queryFmtCPUUsageMaxRecordingRule, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	resCPUUsageMaxRR := ctx.QueryAtTime(queryCPUUsageMaxRecordingRule, end)
	resCPUUsageMax, _ := resCPUUsageMaxRR.Await()

	if len(resCPUUsageMax) > 0 {
		return wrapResults(queryCPUUsageMaxRecordingRule, resCPUUsageMax)
	}

	resolution := cfg.DataResolution
	resStr := timeutil.DurationString(resolution)
	doubleResStr := timeutil.DurationString(2 * resolution)

	queryCPUUsageMaxSubquery := fmt.Sprintf(queryFmtCPUUsageMaxSubquery, cfg.ClusterFilter, doubleResStr, durStr, resStr, cfg.ClusterLabel)
	return ctx.QueryAtTime(queryCPUUsageMaxSubquery, end)
}

func (pds *PrometheusDataSource) QueryGPUsRequested(start, end time.Time) source.QueryResultsChan {
	const queryFmtGPUsRequested = `avg(avg_over_time(kube_pod_container_resource_requests{resource="nvidia_com_gpu", container!="",container!="POD", node!="", %s}[%s])) by (container, pod, namespace, node, %s)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel()

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryGPUsRequested")
	}

	queryGPUsRequested := fmt.Sprintf(queryFmtGPUsRequested, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return ctx.QueryAtTime(queryGPUsRequested, end)
}

func (pds *PrometheusDataSource) QueryGPUsUsageAvg(start, end time.Time) source.QueryResultsChan {
	const queryFmtGPUsUsageAvg = `avg(avg_over_time(DCGM_FI_PROF_GR_ENGINE_ACTIVE{container!=""}[%s])) by (container, pod, namespace, %s)`
	// durStr, env.GetPromClusterLabel()

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryGPUsUsageAvg")
	}

	queryGPUsUsageAvg := fmt.Sprintf(queryFmtGPUsUsageAvg, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return ctx.QueryAtTime(queryGPUsUsageAvg, end)
}

func (pds *PrometheusDataSource) QueryGPUsUsageMax(start, end time.Time) source.QueryResultsChan {
	const queryFmtGPUsUsageMax = `max(max_over_time(DCGM_FI_PROF_GR_ENGINE_ACTIVE{container!=""}[%s])) by (container, pod, namespace, %s)`
	// durStr, env.GetPromClusterLabel()

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryGPUsUsageMax")
	}

	queryGPUsUsageMax := fmt.Sprintf(queryFmtGPUsUsageMax, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return ctx.QueryAtTime(queryGPUsUsageMax, end)
}

func (pds *PrometheusDataSource) QueryGPUsAllocated(start, end time.Time) source.QueryResultsChan {
	const queryFmtGPUsAllocated = `avg(avg_over_time(container_gpu_allocation{container!="", container!="POD", node!="", %s}[%s])) by (container, pod, namespace, node, %s)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel()

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryGPUsAllocated")
	}

	queryGPUsAllocated := fmt.Sprintf(queryFmtGPUsAllocated, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return ctx.QueryAtTime(queryGPUsAllocated, end)
}

func (pds *PrometheusDataSource) QueryNodeCostPerCPUHr(start, end time.Time) source.QueryResultsChan {
	const queryFmtNodeCostPerCPUHr = `avg(avg_over_time(node_cpu_hourly_cost{%s}[%s])) by (node, %s, instance_type, provider_id)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNodeCostPerCPUHr")
	}

	queryNodeCostPerCPUHr := fmt.Sprintf(queryFmtNodeCostPerCPUHr, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return ctx.QueryAtTime(queryNodeCostPerCPUHr, end)
}

func (pds *PrometheusDataSource) QueryNodeCostPerRAMGiBHr(start, end time.Time) source.QueryResultsChan {
	const queryFmtNodeCostPerRAMGiBHr = `avg(avg_over_time(node_ram_hourly_cost{%s}[%s])) by (node, %s, instance_type, provider_id)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNodeCostPerRAMGiBHr")
	}

	queryNodeCostPerRAMGiBHr := fmt.Sprintf(queryFmtNodeCostPerRAMGiBHr, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return ctx.QueryAtTime(queryNodeCostPerRAMGiBHr, end)
}

func (pds *PrometheusDataSource) QueryNodeCostPerGPUHr(start, end time.Time) source.QueryResultsChan {
	const queryFmtNodeCostPerGPUHr = `avg(avg_over_time(node_gpu_hourly_cost{%s}[%s])) by (node, %s, instance_type, provider_id)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNodeCostPerGPUHr")
	}

	queryNodeCostPerGPUHr := fmt.Sprintf(queryFmtNodeCostPerGPUHr, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return ctx.QueryAtTime(queryNodeCostPerGPUHr, end)
}

func (pds *PrometheusDataSource) QueryNodeIsSpot2(start, end time.Time) source.QueryResultsChan {
	const queryFmtNodeIsSpot = `avg_over_time(kubecost_node_is_spot{%s}[%s])`
	// env.GetPromClusterFilter(), durStr)

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNodeIsSpot2")
	}

	queryNodeIsSpot := fmt.Sprintf(queryFmtNodeIsSpot, cfg.ClusterFilter, durStr)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return ctx.QueryAtTime(queryNodeIsSpot, end)
}

func (pds *PrometheusDataSource) QueryPVCInfo2(start, end time.Time) source.QueryResultsChan {
	const queryFmtPVCInfo = `avg(kube_persistentvolumeclaim_info{volumename != "", %s}) by (persistentvolumeclaim, storageclass, volumename, namespace, %s)[%s:%s]`
	// env.GetPromClusterFilter(), env.GetPromClusterLabel(), durStr, resStr)

	cfg := pds.promConfig
	resolution := cfg.DataResolution
	resStr := timeutil.DurationString(resolution)

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryPVCInfo2")
	}

	queryPVCInfo := fmt.Sprintf(queryFmtPVCInfo, cfg.ClusterFilter, cfg.ClusterLabel, durStr, resStr)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return ctx.QueryAtTime(queryPVCInfo, end)
}

func (pds *PrometheusDataSource) QueryPodPVCAllocation(start, end time.Time) source.QueryResultsChan {
	const queryFmtPodPVCAllocation = `avg(avg_over_time(pod_pvc_allocation{%s}[%s])) by (persistentvolume, persistentvolumeclaim, pod, namespace, %s)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryPodPVCAllocation")
	}

	queryPodPVCAllocation := fmt.Sprintf(queryFmtPodPVCAllocation, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return ctx.QueryAtTime(queryPodPVCAllocation, end)
}

func (pds *PrometheusDataSource) QueryPVCBytesRequested(start, end time.Time) source.QueryResultsChan {
	const queryFmtPVCBytesRequested = `avg(avg_over_time(kube_persistentvolumeclaim_resource_requests_storage_bytes{%s}[%s])) by (persistentvolumeclaim, namespace, %s)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryPVCBytesRequested")
	}

	queryPVCBytesRequested := fmt.Sprintf(queryFmtPVCBytesRequested, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return ctx.QueryAtTime(queryPVCBytesRequested, end)
}

func (pds *PrometheusDataSource) QueryPVActiveMins(start, end time.Time) source.QueryResultsChan {
	const queryFmtPVActiveMins = `count(kube_persistentvolume_capacity_bytes{%s}) by (persistentvolume, %s)[%s:%s]`
	// env.GetPromClusterFilter(), env.GetPromClusterLabel(), durStr, resStr)

	cfg := pds.promConfig
	resolution := cfg.DataResolution
	resStr := timeutil.DurationString(resolution)

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryPVActiveMins")
	}

	queryPVActiveMins := fmt.Sprintf(queryFmtPVActiveMins, cfg.ClusterFilter, cfg.ClusterLabel, durStr, resStr)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return ctx.QueryAtTime(queryPVActiveMins, end)
}

func (pds *PrometheusDataSource) QueryPVBytes(start, end time.Time) source.QueryResultsChan {
	const queryFmtPVBytes = `avg(avg_over_time(kube_persistentvolume_capacity_bytes{%s}[%s])) by (persistentvolume, %s)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryPVBytes")
	}

	queryPVBytes := fmt.Sprintf(queryFmtPVBytes, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return ctx.QueryAtTime(queryPVBytes, end)
}

func (pds *PrometheusDataSource) QueryPVCostPerGiBHour(start, end time.Time) source.QueryResultsChan {
	const queryFmtPVCostPerGiBHour = `avg(avg_over_time(pv_hourly_cost{%s}[%s])) by (volumename, %s)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryPVCostPerGiBHour")
	}

	queryPVCostPerGiBHour := fmt.Sprintf(queryFmtPVCostPerGiBHour, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return ctx.QueryAtTime(queryPVCostPerGiBHour, end)
}

func (pds *PrometheusDataSource) QueryPVMeta(start, end time.Time) source.QueryResultsChan {
	const queryFmtPVMeta = `avg(avg_over_time(kubecost_pv_info{%s}[%s])) by (%s, persistentvolume, provider_id)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryPVMeta")
	}

	queryPVMeta := fmt.Sprintf(queryFmtPVMeta, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return ctx.QueryAtTime(queryPVMeta, end)
}

func (pds *PrometheusDataSource) QueryNetZoneGiB(start, end time.Time) source.QueryResultsChan {
	const queryFmtNetZoneGiB = `sum(increase(kubecost_pod_network_egress_bytes_total{internet="false", same_zone="false", same_region="true", %s}[%s])) by (pod_name, namespace, %s) / 1024 / 1024 / 1024`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel()

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNetZoneGiB")
	}

	queryNetZoneGiB := fmt.Sprintf(queryFmtNetZoneGiB, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return ctx.QueryAtTime(queryNetZoneGiB, end)
}

func (pds *PrometheusDataSource) QueryNetZoneCostPerGiB(start, end time.Time) source.QueryResultsChan {
	const queryFmtNetZoneCostPerGiB = `avg(avg_over_time(kubecost_network_zone_egress_cost{%s}[%s])) by (%s)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel()

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNetZoneCostPerGiB")
	}

	queryNetZoneCostPerGiB := fmt.Sprintf(queryFmtNetZoneCostPerGiB, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return ctx.QueryAtTime(queryNetZoneCostPerGiB, end)
}

func (pds *PrometheusDataSource) QueryNetRegionGiB(start, end time.Time) source.QueryResultsChan {
	const queryFmtNetRegionGiB = `sum(increase(kubecost_pod_network_egress_bytes_total{internet="false", same_zone="false", same_region="false", %s}[%s])) by (pod_name, namespace, %s) / 1024 / 1024 / 1024`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel()

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNetRegionGiB")
	}

	queryNetRegionGiB := fmt.Sprintf(queryFmtNetRegionGiB, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return ctx.QueryAtTime(queryNetRegionGiB, end)
}

func (pds *PrometheusDataSource) QueryNetRegionCostPerGiB(start, end time.Time) source.QueryResultsChan {
	const queryFmtNetRegionCostPerGiB = `avg(avg_over_time(kubecost_network_region_egress_cost{%s}[%s])) by (%s)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel()

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNetRegionCostPerGiB")
	}

	queryNetRegionCostPerGiB := fmt.Sprintf(queryFmtNetRegionCostPerGiB, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return ctx.QueryAtTime(queryNetRegionCostPerGiB, end)
}

func (pds *PrometheusDataSource) QueryNetInternetGiB(start, end time.Time) source.QueryResultsChan {
	const queryFmtNetInternetGiB = `sum(increase(kubecost_pod_network_egress_bytes_total{internet="true", %s}[%s])) by (pod_name, namespace, %s) / 1024 / 1024 / 1024`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel()

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNetInternetGiB")
	}

	queryNetInternetGiB := fmt.Sprintf(queryFmtNetInternetGiB, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return ctx.QueryAtTime(queryNetInternetGiB, end)
}

func (pds *PrometheusDataSource) QueryNetInternetCostPerGiB(start, end time.Time) source.QueryResultsChan {
	const queryFmtNetInternetCostPerGiB = `avg(avg_over_time(kubecost_network_internet_egress_cost{%s}[%s])) by (%s)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel()

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNetInternetCostPerGiB")
	}

	queryNetInternetCostPerGiB := fmt.Sprintf(queryFmtNetInternetCostPerGiB, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return ctx.QueryAtTime(queryNetInternetCostPerGiB, end)
}

func (pds *PrometheusDataSource) QueryNetReceiveBytes(start, end time.Time) source.QueryResultsChan {
	const queryFmtNetReceiveBytes = `sum(increase(container_network_receive_bytes_total{pod!="", %s}[%s])) by (pod_name, pod, namespace, %s)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNetReceiveBytes")
	}

	queryNetReceiveBytes := fmt.Sprintf(queryFmtNetReceiveBytes, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return ctx.QueryAtTime(queryNetReceiveBytes, end)
}

func (pds *PrometheusDataSource) QueryNetTransferBytes(start, end time.Time) source.QueryResultsChan {
	const queryFmtNetTransferBytes = `sum(increase(container_network_transmit_bytes_total{pod!="", %s}[%s])) by (pod_name, pod, namespace, %s)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNetTransferBytes")
	}

	queryNetTransferBytes := fmt.Sprintf(queryFmtNetTransferBytes, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return ctx.QueryAtTime(queryNetTransferBytes, end)
}

func (pds *PrometheusDataSource) QueryNodeLabels2(start, end time.Time) source.QueryResultsChan {
	const queryFmtNodeLabels = `avg_over_time(kube_node_labels{%s}[%s])`
	// env.GetPromClusterFilter(), durStr

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNodeLabels2")
	}

	queryNodeLabels := fmt.Sprintf(queryFmtNodeLabels, cfg.ClusterFilter, durStr)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return ctx.QueryAtTime(queryNodeLabels, end)
}

func (pds *PrometheusDataSource) QueryNamespaceLabels(start, end time.Time) source.QueryResultsChan {
	const queryFmtNamespaceLabels = `avg_over_time(kube_namespace_labels{%s}[%s])`
	// env.GetPromClusterFilter(), durStr

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNamespaceLabels")
	}

	queryNamespaceLabels := fmt.Sprintf(queryFmtNamespaceLabels, cfg.ClusterFilter, durStr)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return ctx.QueryAtTime(queryNamespaceLabels, end)
}

func (pds *PrometheusDataSource) QueryNamespaceAnnotations(start, end time.Time) source.QueryResultsChan {
	const queryFmtNamespaceAnnotations = `avg_over_time(kube_namespace_annotations{%s}[%s])`
	// env.GetPromClusterFilter(), durStr

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNamespaceAnnotations")
	}

	queryNamespaceAnnotations := fmt.Sprintf(queryFmtNamespaceAnnotations, cfg.ClusterFilter, durStr)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return ctx.QueryAtTime(queryNamespaceAnnotations, end)
}

func (pds *PrometheusDataSource) QueryPodLabels(start, end time.Time) source.QueryResultsChan {
	const queryFmtPodLabels = `avg_over_time(kube_pod_labels{%s}[%s])`
	// env.GetPromClusterFilter(), durStr

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryPodLabels")
	}

	queryPodLabels := fmt.Sprintf(queryFmtPodLabels, cfg.ClusterFilter, durStr)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return ctx.QueryAtTime(queryPodLabels, end)
}

func (pds *PrometheusDataSource) QueryPodAnnotations(start, end time.Time) source.QueryResultsChan {
	const queryFmtPodAnnotations = `avg_over_time(kube_pod_annotations{%s}[%s])`
	// env.GetPromClusterFilter(), durStr

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryPodAnnotations")
	}

	queryPodAnnotations := fmt.Sprintf(queryFmtPodAnnotations, cfg.ClusterFilter, durStr)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return ctx.QueryAtTime(queryPodAnnotations, end)
}

func (pds *PrometheusDataSource) QueryServiceLabels(start, end time.Time) source.QueryResultsChan {
	const queryFmtServiceLabels = `avg_over_time(service_selector_labels{%s}[%s])`
	// env.GetPromClusterFilter(), durStr

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryServiceLabels")
	}

	queryServiceLabels := fmt.Sprintf(queryFmtServiceLabels, cfg.ClusterFilter, durStr)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return ctx.QueryAtTime(queryServiceLabels, end)
}

func (pds *PrometheusDataSource) QueryDeploymentLabels(start, end time.Time) source.QueryResultsChan {
	const queryFmtDeploymentLabels = `avg_over_time(deployment_match_labels{%s}[%s])`
	// env.GetPromClusterFilter(), durStr

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNamespaceAnnotations")
	}

	queryDeploymentLabels := fmt.Sprintf(queryFmtDeploymentLabels, cfg.ClusterFilter, durStr)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return ctx.QueryAtTime(queryDeploymentLabels, end)
}

func (pds *PrometheusDataSource) QueryStatefulSetLabels(start, end time.Time) source.QueryResultsChan {
	const queryFmtStatefulSetLabels = `avg_over_time(statefulSet_match_labels{%s}[%s])`
	// env.GetPromClusterFilter(), durStr

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryStatefulSetLabels")
	}

	queryStatefulSetLabels := fmt.Sprintf(queryFmtStatefulSetLabels, cfg.ClusterFilter, durStr)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return ctx.QueryAtTime(queryStatefulSetLabels, end)
}

func (pds *PrometheusDataSource) QueryDaemonSetLabels(start, end time.Time) source.QueryResultsChan {
	const queryFmtDaemonSetLabels = `sum(avg_over_time(kube_pod_owner{owner_kind="DaemonSet", %s}[%s])) by (pod, owner_name, namespace, %s)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryDaemonSetLabels")
	}

	queryDaemonSetLabels := fmt.Sprintf(queryFmtDaemonSetLabels, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return ctx.QueryAtTime(queryDaemonSetLabels, end)
}

func (pds *PrometheusDataSource) QueryJobLabels(start, end time.Time) source.QueryResultsChan {
	const queryFmtJobLabels = `sum(avg_over_time(kube_pod_owner{owner_kind="Job", %s}[%s])) by (pod, owner_name, namespace ,%s)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryJobLabels")
	}

	queryJobLabels := fmt.Sprintf(queryFmtJobLabels, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return ctx.QueryAtTime(queryJobLabels, end)
}

func (pds *PrometheusDataSource) QueryPodsWithReplicaSetOwner(start, end time.Time) source.QueryResultsChan {
	const queryFmtPodsWithReplicaSetOwner = `sum(avg_over_time(kube_pod_owner{owner_kind="ReplicaSet", %s}[%s])) by (pod, owner_name, namespace ,%s)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryPodsWithReplicaSetOwner")
	}

	queryPodsWithReplicaSetOwner := fmt.Sprintf(queryFmtPodsWithReplicaSetOwner, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return ctx.QueryAtTime(queryPodsWithReplicaSetOwner, end)
}

func (pds *PrometheusDataSource) QueryReplicaSetsWithoutOwners(start, end time.Time) source.QueryResultsChan {
	const queryFmtReplicaSetsWithoutOwners = `avg(avg_over_time(kube_replicaset_owner{owner_kind="<none>", owner_name="<none>", %s}[%s])) by (replicaset, namespace, %s)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryReplicaSetsWithoutOwners")
	}

	queryReplicaSetsWithoutOwners := fmt.Sprintf(queryFmtReplicaSetsWithoutOwners, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return ctx.QueryAtTime(queryReplicaSetsWithoutOwners, end)
}

func (pds *PrometheusDataSource) QueryReplicaSetsWithRollout(start, end time.Time) source.QueryResultsChan {
	const queryFmtReplicaSetsWithRolloutOwner = `avg(avg_over_time(kube_replicaset_owner{owner_kind="Rollout", %s}[%s])) by (replicaset, namespace, owner_kind, owner_name, %s)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryReplicaSetsWithRollout")
	}

	queryReplicaSetsWithRolloutOwner := fmt.Sprintf(queryFmtReplicaSetsWithRolloutOwner, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return ctx.QueryAtTime(queryReplicaSetsWithRolloutOwner, end)
}

func (pds *PrometheusDataSource) QueryDataCoverage(limitDays int) (time.Time, time.Time, error) {
	const (
		queryFmtOldestSample = `min_over_time(timestamp(group(node_cpu_hourly_cost{%s}))[%s:%s])`
		queryFmtNewestSample = `max_over_time(timestamp(group(node_cpu_hourly_cost{%s}))[%s:%s])`
	)

	cfg := pds.promConfig
	now := time.Now()
	durStr := fmt.Sprintf("%dd", limitDays)

	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	queryOldest := fmt.Sprintf(queryFmtOldestSample, cfg.ClusterFilter, durStr, "1h")
	resOldestFut := ctx.QueryAtTime(queryOldest, now)

	resOldest, err := resOldestFut.Await()
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("querying oldest sample: %w", err)
	}
	if len(resOldest) == 0 || len(resOldest[0].Values) == 0 {
		return time.Time{}, time.Time{}, fmt.Errorf("querying oldest sample: %w", err)
	}

	oldest := time.Unix(int64(resOldest[0].Values[0].Value), 0)

	queryNewest := fmt.Sprintf(queryFmtNewestSample, cfg.ClusterFilter, durStr, "1h")
	resNewestFut := ctx.QueryAtTime(queryNewest, now)

	resNewest, err := resNewestFut.Await()
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("querying newest sample: %w", err)
	}
	if len(resNewest) == 0 || len(resNewest[0].Values) == 0 {
		return time.Time{}, time.Time{}, fmt.Errorf("querying newest sample: %w", err)
	}

	newest := time.Unix(int64(resNewest[0].Values[0].Value), 0)

	return oldest, newest, nil
}

func (pds *PrometheusDataSource) QueryIsGPUShared(start, end time.Time) source.QueryResultsChan {
	const queryFmtIsGPUShared = `avg(avg_over_time(kube_pod_container_resource_requests{container!="", node != "", pod != "", container!= "", unit = "integer",  %s}[%s])) by (container, pod, namespace, node, resource)`
	// env.GetPromClusterFilter(), durStr

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryIsGPUShared")
	}

	queryIsGPUShared := fmt.Sprintf(queryFmtIsGPUShared, cfg.ClusterFilter, durStr)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return ctx.QueryAtTime(queryIsGPUShared, end)
}

func (pds *PrometheusDataSource) QueryGetGPUInfo(start, end time.Time) source.QueryResultsChan {
	const queryFmtGetGPUInfo = `avg(avg_over_time(DCGM_FI_DEV_DEC_UTIL{container!="",%s}[%s])) by (container, pod, namespace, device, modelName, UUID)`
	// env.GetPromClusterFilter(), durStr

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryGetGPUInfo")
	}

	queryGetGPUInfo := fmt.Sprintf(queryFmtGetGPUInfo, cfg.ClusterFilter, durStr)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return ctx.QueryAtTime(queryGetGPUInfo, end)
}

func newEmptyResult() source.QueryResultsChan {
	ch := make(source.QueryResultsChan)
	go func() {
		results := source.NewQueryResults("")
		ch <- results
	}()
	return ch
}

func wrapResults(query string, results []*source.QueryResult) source.QueryResultsChan {
	ch := make(source.QueryResultsChan)

	go func() {
		r := source.NewQueryResults(query)
		r.Results = results
		ch <- r
	}()

	return ch
}

func snapResolutionMinute(res time.Duration) time.Duration {
	resMins := int64(math.Trunc(res.Minutes()))
	if resMins <= 0 {
		resMins = 1
	}
	return time.Duration(resMins) * time.Minute
}

func formatResolutionMinutes(resMins int64) string {
	if resMins%60 == 0 {
		return fmt.Sprintf("%dh", resMins/60)
	}

	return fmt.Sprintf("%dm", resMins)
}
