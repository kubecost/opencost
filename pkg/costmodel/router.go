package costmodel

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"os"
	"path"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/microcosm-cc/bluemonday"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/httputil"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
	"github.com/opencost/opencost/core/pkg/version"
	"github.com/opencost/opencost/pkg/cloud/aws"
	cloudconfig "github.com/opencost/opencost/pkg/cloud/config"
	"github.com/opencost/opencost/pkg/cloud/gcp"
	"github.com/opencost/opencost/pkg/cloud/provider"
	"github.com/opencost/opencost/pkg/cloudcost"
	"github.com/opencost/opencost/pkg/config"
	clustermap "github.com/opencost/opencost/pkg/costmodel/clusters"
	"github.com/opencost/opencost/pkg/customcost"
	"github.com/opencost/opencost/pkg/kubeconfig"
	"github.com/opencost/opencost/pkg/metrics"
	"github.com/opencost/opencost/pkg/services"
	"github.com/opencost/opencost/pkg/util/watcher"

	"github.com/julienschmidt/httprouter"

	"github.com/getsentry/sentry-go"

	"github.com/opencost/opencost/core/pkg/clusters"
	sysenv "github.com/opencost/opencost/core/pkg/env"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/pkg/cloud/azure"
	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/cloud/utils"
	"github.com/opencost/opencost/pkg/clustercache"
	"github.com/opencost/opencost/pkg/env"
	"github.com/opencost/opencost/pkg/errors"
	"github.com/opencost/opencost/pkg/prom"
	"github.com/opencost/opencost/pkg/thanos"
	prometheus "github.com/prometheus/client_golang/api"
	prometheusAPI "github.com/prometheus/client_golang/api/prometheus/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/patrickmn/go-cache"

	"k8s.io/client-go/kubernetes"
)

var sanitizePolicy = bluemonday.UGCPolicy()

const (
	RFC3339Milli         = "2006-01-02T15:04:05.000Z"
	maxCacheMinutes1d    = 11
	maxCacheMinutes2d    = 17
	maxCacheMinutes7d    = 37
	maxCacheMinutes30d   = 137
	CustomPricingSetting = "CustomPricing"
	DiscountSetting      = "Discount"
	epRules              = apiPrefix + "/rules"
)

var (
	// gitCommit is set by the build system
	gitCommit string
)

// Accesses defines a singleton application instance, providing access to
// Prometheus, Kubernetes, the cloud provider, and caches.
type Accesses struct {
	PrometheusClient    prometheus.Client
	ThanosClient        prometheus.Client
	KubeClientSet       kubernetes.Interface
	ClusterCache        clustercache.ClusterCache
	ClusterMap          clusters.ClusterMap
	CloudProvider       models.Provider
	ConfigFileManager   *config.ConfigFileManager
	ClusterInfoProvider clusters.ClusterInfoProvider
	Model               *CostModel
	MetricsEmitter      *CostModelMetricsEmitter
	OutOfClusterCache   *cache.Cache
	AggregateCache      *cache.Cache
	CostDataCache       *cache.Cache
	ClusterCostsCache   *cache.Cache
	CacheExpiration     map[time.Duration]time.Duration
	AggAPI              Aggregator
	// SettingsCache stores current state of app settings
	SettingsCache *cache.Cache
	// settingsSubscribers tracks channels through which changes to different
	// settings will be published in a pub/sub model
	settingsSubscribers map[string][]chan string
	settingsMutex       sync.Mutex
	// registered http service instances
	httpServices services.HTTPServices
}

// GetPrometheusClient decides whether the default Prometheus client or the Thanos client
// should be used.
func (a *Accesses) GetPrometheusClient(remote bool) prometheus.Client {
	// Use Thanos Client if it exists (enabled) and remote flag set
	var pc prometheus.Client

	if remote && a.ThanosClient != nil {
		pc = a.ThanosClient
	} else {
		pc = a.PrometheusClient
	}

	return pc
}

// GetCacheExpiration looks up and returns custom cache expiration for the given duration.
// If one does not exists, it returns the default cache expiration, which is defined by
// the particular cache.
func (a *Accesses) GetCacheExpiration(dur time.Duration) time.Duration {
	if expiration, ok := a.CacheExpiration[dur]; ok {
		return expiration
	}
	return cache.DefaultExpiration
}

// GetCacheRefresh determines how long to wait before refreshing the cache for the given duration,
// which is done 1 minute before we expect the cache to expire, or 1 minute if expiration is
// not found or is less than 2 minutes.
func (a *Accesses) GetCacheRefresh(dur time.Duration) time.Duration {
	expiry := a.GetCacheExpiration(dur).Minutes()
	if expiry <= 2.0 {
		return time.Minute
	}
	mins := time.Duration(expiry/2.0) * time.Minute
	return mins
}

func (a *Accesses) ClusterCostsFromCacheHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")

	duration := 24 * time.Hour
	offset := time.Minute
	durationHrs := "24h"
	fmtOffset := "1m"
	pClient := a.GetPrometheusClient(true)

	key := fmt.Sprintf("%s:%s", durationHrs, fmtOffset)
	if data, valid := a.ClusterCostsCache.Get(key); valid {
		clusterCosts := data.(map[string]*ClusterCosts)
		w.Write(WrapDataWithMessage(clusterCosts, nil, "clusterCosts cache hit"))
	} else {
		data, err := a.ComputeClusterCosts(pClient, a.CloudProvider, duration, offset, true)
		w.Write(WrapDataWithMessage(data, err, fmt.Sprintf("clusterCosts cache miss: %s", key)))
	}
}

type Response struct {
	Code    int         `json:"code"`
	Status  string      `json:"status"`
	Data    interface{} `json:"data"`
	Message string      `json:"message,omitempty"`
	Warning string      `json:"warning,omitempty"`
}

// FilterFunc is a filter that returns true iff the given CostData should be filtered out, and the environment that was used as the filter criteria, if it was an aggregate
type FilterFunc func(*CostData) (bool, string)

// FilterCostData allows through only CostData that matches all the given filter functions
func FilterCostData(data map[string]*CostData, retains []FilterFunc, filters []FilterFunc) (map[string]*CostData, int, map[string]int) {
	result := make(map[string]*CostData)
	filteredEnvironments := make(map[string]int)
	filteredContainers := 0
DataLoop:
	for key, datum := range data {
		for _, rf := range retains {
			if ok, _ := rf(datum); ok {
				result[key] = datum
				// if any retain function passes, the data is retained and move on
				continue DataLoop
			}
		}
		for _, ff := range filters {
			if ok, environment := ff(datum); !ok {
				if environment != "" {
					filteredEnvironments[environment]++
				}
				filteredContainers++
				// if any filter function check fails, move on to the next datum
				continue DataLoop
			}
		}
		result[key] = datum
	}

	return result, filteredContainers, filteredEnvironments
}

func filterFields(fields string, data map[string]*CostData) map[string]CostData {
	fs := strings.Split(fields, ",")
	fmap := make(map[string]bool)
	for _, f := range fs {
		fieldNameLower := strings.ToLower(f) // convert to go struct name by uppercasing first letter
		log.Debugf("to delete: %s", fieldNameLower)
		fmap[fieldNameLower] = true
	}
	filteredData := make(map[string]CostData)
	for cname, costdata := range data {
		s := reflect.TypeOf(*costdata)
		val := reflect.ValueOf(*costdata)
		costdata2 := CostData{}
		cd2 := reflect.New(reflect.Indirect(reflect.ValueOf(costdata2)).Type()).Elem()
		n := s.NumField()
		for i := 0; i < n; i++ {
			field := s.Field(i)
			value := val.Field(i)
			value2 := cd2.Field(i)
			if _, ok := fmap[strings.ToLower(field.Name)]; !ok {
				value2.Set(reflect.Value(value))
			}
		}
		filteredData[cname] = cd2.Interface().(CostData)
	}
	return filteredData
}

func normalizeTimeParam(param string) (string, error) {
	if param == "" {
		return "", fmt.Errorf("invalid time param")
	}
	// convert days to hours
	if param[len(param)-1:] == "d" {
		count := param[:len(param)-1]
		val, err := strconv.ParseInt(count, 10, 64)
		if err != nil {
			return "", err
		}
		val = val * 24
		param = fmt.Sprintf("%dh", val)
	}

	return param, nil
}

// ParsePercentString takes a string of expected format "N%" and returns a floating point 0.0N.
// If the "%" symbol is missing, it just returns 0.0N. Empty string is interpreted as "0%" and
// return 0.0.
func ParsePercentString(percentStr string) (float64, error) {
	if len(percentStr) == 0 {
		return 0.0, nil
	}
	if percentStr[len(percentStr)-1:] == "%" {
		percentStr = percentStr[:len(percentStr)-1]
	}
	discount, err := strconv.ParseFloat(percentStr, 64)
	if err != nil {
		return 0.0, err
	}
	discount *= 0.01

	return discount, nil
}

func WrapData(data interface{}, err error) []byte {
	var resp []byte

	if err != nil {
		log.Errorf("Error returned to client: %s", err.Error())
		resp, _ = json.Marshal(&Response{
			Code:    http.StatusInternalServerError,
			Status:  "error",
			Message: err.Error(),
			Data:    data,
		})
	} else {
		resp, err = json.Marshal(&Response{
			Code:   http.StatusOK,
			Status: "success",
			Data:   data,
		})
		if err != nil {
			log.Errorf("error marshaling response json: %s", err.Error())
		}
	}

	return resp
}

func WrapDataWithMessage(data interface{}, err error, message string) []byte {
	var resp []byte

	if err != nil {
		log.Errorf("Error returned to client: %s", err.Error())
		resp, _ = json.Marshal(&Response{
			Code:    http.StatusInternalServerError,
			Status:  "error",
			Message: err.Error(),
			Data:    data,
		})
	} else {
		resp, _ = json.Marshal(&Response{
			Code:    http.StatusOK,
			Status:  "success",
			Data:    data,
			Message: message,
		})
	}

	return resp
}

func WrapDataWithWarning(data interface{}, err error, warning string) []byte {
	var resp []byte

	if err != nil {
		log.Errorf("Error returned to client: %s", err.Error())
		resp, _ = json.Marshal(&Response{
			Code:    http.StatusInternalServerError,
			Status:  "error",
			Message: err.Error(),
			Warning: warning,
			Data:    data,
		})
	} else {
		resp, _ = json.Marshal(&Response{
			Code:    http.StatusOK,
			Status:  "success",
			Data:    data,
			Warning: warning,
		})
	}

	return resp
}

func WrapDataWithMessageAndWarning(data interface{}, err error, message, warning string) []byte {
	var resp []byte

	if err != nil {
		log.Errorf("Error returned to client: %s", err.Error())
		resp, _ = json.Marshal(&Response{
			Code:    http.StatusInternalServerError,
			Status:  "error",
			Message: err.Error(),
			Warning: warning,
			Data:    data,
		})
	} else {
		resp, _ = json.Marshal(&Response{
			Code:    http.StatusOK,
			Status:  "success",
			Data:    data,
			Message: message,
			Warning: warning,
		})
	}

	return resp
}

// wrapAsObjectItems wraps a slice of items into an object containing a single items list
// allows our k8s proxy methods to emulate a List() request to k8s API
func wrapAsObjectItems(items interface{}) map[string]interface{} {
	return map[string]interface{}{
		"items": items,
	}
}

// RefreshPricingData needs to be called when a new node joins the fleet, since we cache the relevant subsets of pricing data to avoid storing the whole thing.
func (a *Accesses) RefreshPricingData(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	err := a.CloudProvider.DownloadPricingData()
	if err != nil {
		log.Errorf("Error refreshing pricing data: %s", err.Error())
	}

	w.Write(WrapData(nil, err))
}

func (a *Accesses) CostDataModel(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	window := r.URL.Query().Get("timeWindow")
	offset := r.URL.Query().Get("offset")
	fields := r.URL.Query().Get("filterFields")
	namespace := r.URL.Query().Get("namespace")

	if offset != "" {
		offset = "offset " + offset
	}

	data, err := a.Model.ComputeCostData(a.PrometheusClient, a.CloudProvider, window, offset, namespace)

	if fields != "" {
		filteredData := filterFields(fields, data)
		w.Write(WrapData(filteredData, err))
	} else {
		w.Write(WrapData(data, err))
	}

}

func (a *Accesses) ClusterCosts(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	window := r.URL.Query().Get("window")
	offset := r.URL.Query().Get("offset")

	if window == "" {
		w.Write(WrapData(nil, fmt.Errorf("missing window argument")))
		return
	}
	windowDur, err := timeutil.ParseDuration(window)
	if err != nil {
		w.Write(WrapData(nil, fmt.Errorf("error parsing window (%s): %s", window, err)))
		return
	}

	// offset is not a required parameter
	var offsetDur time.Duration
	if offset != "" {
		offsetDur, err = timeutil.ParseDuration(offset)
		if err != nil {
			w.Write(WrapData(nil, fmt.Errorf("error parsing offset (%s): %s", offset, err)))
			return
		}
	}

	useThanos, _ := strconv.ParseBool(r.URL.Query().Get("multi"))

	if useThanos && !thanos.IsEnabled() {
		w.Write(WrapData(nil, fmt.Errorf("Multi=true while Thanos is not enabled.")))
		return
	}

	var client prometheus.Client
	if useThanos {
		client = a.ThanosClient
		offsetDur = thanos.OffsetDuration()

	} else {
		client = a.PrometheusClient
	}

	data, err := a.ComputeClusterCosts(client, a.CloudProvider, windowDur, offsetDur, true)
	w.Write(WrapData(data, err))
}

func (a *Accesses) ClusterCostsOverTime(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	start := r.URL.Query().Get("start")
	end := r.URL.Query().Get("end")
	window := r.URL.Query().Get("window")
	offset := r.URL.Query().Get("offset")

	if window == "" {
		w.Write(WrapData(nil, fmt.Errorf("missing window argument")))
		return
	}
	windowDur, err := timeutil.ParseDuration(window)
	if err != nil {
		w.Write(WrapData(nil, fmt.Errorf("error parsing window (%s): %s", window, err)))
		return
	}

	// offset is not a required parameter
	var offsetDur time.Duration
	if offset != "" {
		offsetDur, err = timeutil.ParseDuration(offset)
		if err != nil {
			w.Write(WrapData(nil, fmt.Errorf("error parsing offset (%s): %s", offset, err)))
			return
		}
	}

	data, err := ClusterCostsOverTime(a.PrometheusClient, a.CloudProvider, start, end, windowDur, offsetDur)
	w.Write(WrapData(data, err))
}

func (a *Accesses) CostDataModelRange(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	startStr := r.URL.Query().Get("start")
	endStr := r.URL.Query().Get("end")
	windowStr := r.URL.Query().Get("window")
	fields := r.URL.Query().Get("filterFields")
	namespace := r.URL.Query().Get("namespace")
	cluster := r.URL.Query().Get("cluster")
	remote := r.URL.Query().Get("remote")
	remoteEnabled := env.IsRemoteEnabled() && remote != "false"

	layout := "2006-01-02T15:04:05.000Z"
	start, err := time.Parse(layout, startStr)
	if err != nil {
		w.Write(WrapDataWithMessage(nil, fmt.Errorf("invalid start date: %s", startStr), fmt.Sprintf("invalid start date: %s", startStr)))
		return
	}
	end, err := time.Parse(layout, endStr)
	if err != nil {
		w.Write(WrapDataWithMessage(nil, fmt.Errorf("invalid end date: %s", endStr), fmt.Sprintf("invalid end date: %s", endStr)))
		return
	}

	window := opencost.NewWindow(&start, &end)
	if window.IsOpen() || !window.HasDuration() || window.IsNegative() {
		w.Write(WrapDataWithMessage(nil, fmt.Errorf("invalid date range: %s", window), fmt.Sprintf("invalid date range: %s", window)))
		return
	}

	resolution := time.Hour
	if resDur, err := time.ParseDuration(windowStr); err == nil {
		resolution = resDur
	}

	// Use Thanos Client if it exists (enabled) and remote flag set
	var pClient prometheus.Client
	if remote != "false" && a.ThanosClient != nil {
		pClient = a.ThanosClient
	} else {
		pClient = a.PrometheusClient
	}

	data, err := a.Model.ComputeCostDataRange(pClient, a.CloudProvider, window, resolution, namespace, cluster, remoteEnabled)
	if err != nil {
		w.Write(WrapData(nil, err))
	}
	if fields != "" {
		filteredData := filterFields(fields, data)
		w.Write(WrapData(filteredData, err))
	} else {
		w.Write(WrapData(data, err))
	}
}

func parseAggregations(customAggregation, aggregator, filterType string) (string, []string, string) {
	var key string
	var filter string
	var val []string
	if customAggregation != "" {
		key = customAggregation
		filter = filterType
		val = strings.Split(customAggregation, ",")
	} else {
		aggregations := strings.Split(aggregator, ",")
		for i, agg := range aggregations {
			aggregations[i] = "kubernetes_" + agg
		}
		key = strings.Join(aggregations, ",")
		filter = "kubernetes_" + filterType
		val = aggregations
	}
	return key, val, filter
}

func (a *Accesses) GetAllNodePricing(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	data, err := a.CloudProvider.AllNodePricing()
	w.Write(WrapData(data, err))
}

func (a *Accesses) GetConfigs(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	data, err := a.CloudProvider.GetConfig()
	w.Write(WrapData(data, err))
}

func (a *Accesses) UpdateSpotInfoConfigs(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	data, err := a.CloudProvider.UpdateConfig(r.Body, aws.SpotInfoUpdateType)
	if err != nil {
		w.Write(WrapData(data, err))
		return
	}
	w.Write(WrapData(data, err))
	err = a.CloudProvider.DownloadPricingData()
	if err != nil {
		log.Errorf("Error redownloading data on config update: %s", err.Error())
	}
	return
}

func (a *Accesses) UpdateAthenaInfoConfigs(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	data, err := a.CloudProvider.UpdateConfig(r.Body, aws.AthenaInfoUpdateType)
	if err != nil {
		w.Write(WrapData(data, err))
		return
	}
	w.Write(WrapData(data, err))
	return
}

func (a *Accesses) UpdateBigQueryInfoConfigs(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	data, err := a.CloudProvider.UpdateConfig(r.Body, gcp.BigqueryUpdateType)
	if err != nil {
		w.Write(WrapData(data, err))
		return
	}
	w.Write(WrapData(data, err))
	return
}

func (a *Accesses) UpdateAzureStorageConfigs(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	data, err := a.CloudProvider.UpdateConfig(r.Body, azure.AzureStorageUpdateType)
	if err != nil {
		w.Write(WrapData(data, err))
		return
	}
	w.Write(WrapData(data, err))
	return
}

func (a *Accesses) UpdateConfigByKey(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	data, err := a.CloudProvider.UpdateConfig(r.Body, "")
	if err != nil {
		w.Write(WrapData(data, err))
		return
	}
	w.Write(WrapData(data, err))
	return
}

func (a *Accesses) ManagementPlatform(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	data, err := a.CloudProvider.GetManagementPlatform()
	if err != nil {
		w.Write(WrapData(data, err))
		return
	}
	w.Write(WrapData(data, err))
	return
}

func (a *Accesses) ClusterInfo(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	data := a.ClusterInfoProvider.GetClusterInfo()

	w.Write(WrapData(data, nil))
}

func (a *Accesses) GetClusterInfoMap(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	data := a.ClusterMap.AsMap()

	w.Write(WrapData(data, nil))
}

func (a *Accesses) GetServiceAccountStatus(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	w.Write(WrapData(a.CloudProvider.ServiceAccountStatus(), nil))
}

func (a *Accesses) GetPricingSourceStatus(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	w.Write(WrapData(a.CloudProvider.PricingSourceStatus(), nil))
}

func (a *Accesses) GetPricingSourceCounts(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	w.Write(WrapData(a.Model.GetPricingSourceCounts()))
}

func (a *Accesses) GetPricingSourceSummary(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	data := a.CloudProvider.PricingSourceSummary()
	w.Write(WrapData(data, nil))
}

func (a *Accesses) GetPrometheusMetadata(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	w.Write(WrapData(prom.Validate(a.PrometheusClient)))
}

func (a *Accesses) PrometheusQuery(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	qp := httputil.NewQueryParams(r.URL.Query())
	query := qp.Get("query", "")
	if query == "" {
		w.Write(WrapData(nil, fmt.Errorf("Query Parameter 'query' is unset'")))
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

	ctx := prom.NewNamedContext(a.PrometheusClient, prom.FrontendContextName)
	body, err := ctx.RawQuery(query, timeVal)
	if err != nil {
		w.Write(WrapData(nil, fmt.Errorf("Error running query %s. Error: %s", query, err)))
		return
	}

	w.Write(body)
}

func (a *Accesses) PrometheusQueryRange(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
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
		fmt.Fprintf(w, err.Error())
		return
	}

	ctx := prom.NewNamedContext(a.PrometheusClient, prom.FrontendContextName)
	body, err := ctx.RawQueryRange(query, start, end, duration)
	if err != nil {
		fmt.Fprintf(w, "Error running query %s. Error: %s", query, err)
		return
	}

	w.Write(body)
}

func (a *Accesses) ThanosQuery(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	if !thanos.IsEnabled() {
		w.Write(WrapData(nil, fmt.Errorf("ThanosDisabled")))
		return
	}

	qp := httputil.NewQueryParams(r.URL.Query())
	query := qp.Get("query", "")
	if query == "" {
		w.Write(WrapData(nil, fmt.Errorf("Query Parameter 'query' is unset'")))
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

	ctx := prom.NewNamedContext(a.ThanosClient, prom.FrontendContextName)
	body, err := ctx.RawQuery(query, timeVal)
	if err != nil {
		w.Write(WrapData(nil, fmt.Errorf("Error running query %s. Error: %s", query, err)))
		return
	}

	w.Write(body)
}

func (a *Accesses) ThanosQueryRange(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	if !thanos.IsEnabled() {
		w.Write(WrapData(nil, fmt.Errorf("ThanosDisabled")))
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
		fmt.Fprintf(w, err.Error())
		return
	}

	ctx := prom.NewNamedContext(a.ThanosClient, prom.FrontendContextName)
	body, err := ctx.RawQueryRange(query, start, end, duration)
	if err != nil {
		fmt.Fprintf(w, "Error running query %s. Error: %s", query, err)
		return
	}

	w.Write(body)
}

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

func (a *Accesses) GetPrometheusQueueState(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	promQueueState, err := prom.GetPrometheusQueueState(a.PrometheusClient)
	if err != nil {
		w.Write(WrapData(nil, err))
		return
	}

	result := map[string]*prom.PrometheusQueueState{
		"prometheus": promQueueState,
	}

	if thanos.IsEnabled() {
		thanosQueueState, err := prom.GetPrometheusQueueState(a.ThanosClient)
		if err != nil {
			log.Warnf("Error getting Thanos queue state: %s", err)
		} else {
			result["thanos"] = thanosQueueState
		}
	}

	w.Write(WrapData(result, nil))
}

// GetPrometheusMetrics retrieves availability of Prometheus and Thanos metrics
func (a *Accesses) GetPrometheusMetrics(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	promMetrics := prom.GetPrometheusMetrics(a.PrometheusClient, "")

	result := map[string][]*prom.PrometheusDiagnostic{
		"prometheus": promMetrics,
	}

	if thanos.IsEnabled() {
		thanosMetrics := prom.GetPrometheusMetrics(a.ThanosClient, thanos.QueryOffset())
		result["thanos"] = thanosMetrics
	}

	w.Write(WrapData(result, nil))
}

func (a *Accesses) PrometheusRecordingRules(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	u := a.PrometheusClient.URL(epRules, nil)

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		fmt.Fprintf(w, "Error creating Prometheus rule request: "+err.Error())
	}

	_, body, err := a.PrometheusClient.Do(r.Context(), req)
	if err != nil {
		fmt.Fprintf(w, "Error making Prometheus rule request: "+err.Error())
	} else {
		w.Write(body)
	}
}

func (a *Accesses) PrometheusConfig(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	pConfig := map[string]string{
		"address": env.GetPrometheusServerEndpoint(),
	}

	body, err := json.Marshal(pConfig)
	if err != nil {
		fmt.Fprintf(w, "Error marshalling prometheus config")
	} else {
		w.Write(body)
	}
}

func (a *Accesses) PrometheusTargets(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	u := a.PrometheusClient.URL(epTargets, nil)

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		fmt.Fprintf(w, "Error creating Prometheus rule request: "+err.Error())
	}

	_, body, err := a.PrometheusClient.Do(r.Context(), req)
	if err != nil {
		fmt.Fprintf(w, "Error making Prometheus rule request: "+err.Error())
	} else {
		w.Write(body)
	}
}

func (a *Accesses) GetOrphanedPods(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	podlist := a.ClusterCache.GetAllPods()

	var lonePods []*clustercache.Pod
	for _, pod := range podlist {
		if len(pod.OwnerReferences) == 0 {
			lonePods = append(lonePods, pod)
		}
	}

	body, err := json.Marshal(lonePods)
	if err != nil {
		fmt.Fprintf(w, "Error decoding pod: "+err.Error())
	} else {
		w.Write(body)
	}
}

func (a *Accesses) GetInstallNamespace(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	ns := env.GetKubecostNamespace()
	w.Write([]byte(ns))
}

type InstallInfo struct {
	Containers  []ContainerInfo   `json:"containers"`
	ClusterInfo map[string]string `json:"clusterInfo"`
	Version     string            `json:"version"`
}

type ContainerInfo struct {
	ContainerName string `json:"containerName"`
	Image         string `json:"image"`
	StartTime     string `json:"startTime"`
}

func (a *Accesses) GetInstallInfo(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	containers, err := GetKubecostContainers(a.KubeClientSet)
	if err != nil {
		writeErrorResponse(w, 500, fmt.Sprintf("Unable to list pods: %s", err.Error()))
		return
	}

	info := InstallInfo{
		Containers:  containers,
		ClusterInfo: make(map[string]string),
		Version:     version.FriendlyVersion(),
	}

	nodes := a.ClusterCache.GetAllNodes()
	cachePods := a.ClusterCache.GetAllPods()

	info.ClusterInfo["nodeCount"] = strconv.Itoa(len(nodes))
	info.ClusterInfo["podCount"] = strconv.Itoa(len(cachePods))

	body, err := json.Marshal(info)
	if err != nil {
		writeErrorResponse(w, 500, fmt.Sprintf("Error decoding pod: %s", err.Error()))
		return
	}

	w.Write(body)
}

func GetKubecostContainers(kubeClientSet kubernetes.Interface) ([]ContainerInfo, error) {
	pods, err := kubeClientSet.CoreV1().Pods(env.GetKubecostNamespace()).List(context.Background(), metav1.ListOptions{
		LabelSelector: "app=cost-analyzer",
		FieldSelector: "status.phase=Running",
		Limit:         1,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to query kubernetes client for kubecost pods: %s", err)
	}

	// If we have zero pods either something is weird with the install since the app selector is not exposed in the helm
	// chart or more likely we are running locally - in either case Images field will return as null
	var containers []ContainerInfo
	if len(pods.Items) > 0 {
		for _, pod := range pods.Items {
			for _, container := range pod.Spec.Containers {
				c := ContainerInfo{
					ContainerName: container.Name,
					Image:         container.Image,
					StartTime:     pod.Status.StartTime.String(),
				}
				containers = append(containers, c)
			}
		}
	}

	return containers, nil
}

func (a *Accesses) AddServiceKey(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	r.ParseForm()

	key := r.PostForm.Get("key")
	k := []byte(key)
	err := os.WriteFile(path.Join(env.GetConfigPathWithDefault(env.DefaultConfigMountPath), "key.json"), k, 0644)
	if err != nil {
		fmt.Fprintf(w, "Error writing service key: "+err.Error())
	}

	w.WriteHeader(http.StatusOK)
}

func (a *Accesses) GetHelmValues(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	encodedValues := sysenv.Get("HELM_VALUES", "")
	if encodedValues == "" {
		fmt.Fprintf(w, "Values reporting disabled")
		return
	}

	result, err := base64.StdEncoding.DecodeString(encodedValues)
	if err != nil {
		fmt.Fprintf(w, "Failed to decode encoded values: %s", err)
		return
	}

	w.Write(result)
}

func (a *Accesses) Status(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	promServer := env.GetPrometheusServerEndpoint()

	api := prometheusAPI.NewAPI(a.PrometheusClient)
	result, err := api.Buildinfo(r.Context())
	if err != nil {
		fmt.Fprintf(w, "Using Prometheus at "+promServer+". Error: "+err.Error())
	} else {

		fmt.Fprintf(w, "Using Prometheus at "+promServer+". Version: "+result.Version)
	}
}

// captures the panic event in sentry
func capturePanicEvent(err string, stack string) {
	msg := fmt.Sprintf("Panic: %s\nStackTrace: %s\n", err, stack)
	log.Infof(msg)
	sentry.CurrentHub().CaptureEvent(&sentry.Event{
		Level:   sentry.LevelError,
		Message: msg,
	})
	sentry.Flush(5 * time.Second)
}

// handle any panics reported by the errors package
func handlePanic(p errors.Panic) bool {
	err := p.Error

	if err != nil {
		if err, ok := err.(error); ok {
			capturePanicEvent(err.Error(), p.Stack)
		}

		if err, ok := err.(string); ok {
			capturePanicEvent(err, p.Stack)
		}
	}

	// Return true to recover iff the type is http, otherwise allow kubernetes
	// to recover.
	return p.Type == errors.PanicTypeHTTP
}

func Initialize(router *httprouter.Router, additionalConfigWatchers ...*watcher.ConfigMapWatcher) *Accesses {
	var err error
	if errorReportingEnabled {
		err = sentry.Init(sentry.ClientOptions{Release: version.FriendlyVersion()})
		if err != nil {
			log.Infof("Failed to initialize sentry for error reporting")
		} else {
			err = errors.SetPanicHandler(handlePanic)
			if err != nil {
				log.Infof("Failed to set panic handler: %s", err)
			}
		}
	}

	address := env.GetPrometheusServerEndpoint()
	if address == "" {
		log.Fatalf("No address for prometheus set in $%s. Aborting.", env.PrometheusServerEndpointEnvVar)
	}

	queryConcurrency := env.GetMaxQueryConcurrency()
	log.Infof("Prometheus/Thanos Client Max Concurrency set to %d", queryConcurrency)

	timeout := 120 * time.Second
	keepAlive := 120 * time.Second
	tlsHandshakeTimeout := 10 * time.Second
	scrapeInterval := env.GetKubecostScrapeInterval()

	var rateLimitRetryOpts *prom.RateLimitRetryOpts = nil
	if env.IsPrometheusRetryOnRateLimitResponse() {
		rateLimitRetryOpts = &prom.RateLimitRetryOpts{
			MaxRetries:       env.GetPrometheusRetryOnRateLimitMaxRetries(),
			DefaultRetryWait: env.GetPrometheusRetryOnRateLimitDefaultWait(),
		}
	}

	promCli, err := prom.NewPrometheusClient(address, &prom.PrometheusClientConfig{
		Timeout:               timeout,
		KeepAlive:             keepAlive,
		TLSHandshakeTimeout:   tlsHandshakeTimeout,
		TLSInsecureSkipVerify: env.GetInsecureSkipVerify(),
		RateLimitRetryOpts:    rateLimitRetryOpts,
		Auth: &prom.ClientAuth{
			Username:    env.GetDBBasicAuthUsername(),
			Password:    env.GetDBBasicAuthUserPassword(),
			BearerToken: env.GetDBBearerToken(),
		},
		QueryConcurrency:  queryConcurrency,
		QueryLogFile:      "",
		HeaderXScopeOrgId: env.GetPrometheusHeaderXScopeOrgId(),
	})
	if err != nil {
		log.Fatalf("Failed to create prometheus client, Error: %v", err)
	}

	m, err := prom.Validate(promCli)
	if err != nil || !m.Running {
		if err != nil {
			log.Errorf("Failed to query prometheus at %s. Error: %s . Troubleshooting help available at: %s", address, err.Error(), prom.PrometheusTroubleshootingURL)
		} else if !m.Running {
			log.Errorf("Prometheus at %s is not running. Troubleshooting help available at: %s", address, prom.PrometheusTroubleshootingURL)
		}
	} else {
		log.Infof("Success: retrieved the 'up' query against prometheus at: " + address)
	}

	api := prometheusAPI.NewAPI(promCli)
	_, err = api.Buildinfo(context.Background())
	if err != nil {
		log.Infof("No valid prometheus config file at %s. Error: %s . Troubleshooting help available at: %s. Ignore if using cortex/mimir/thanos here.", address, err.Error(), prom.PrometheusTroubleshootingURL)
	} else {
		log.Infof("Retrieved a prometheus config file from: %s", address)
	}

	if scrapeInterval == 0 {
		scrapeInterval = time.Minute
		// Lookup scrape interval for kubecost job, update if found
		si, err := prom.ScrapeIntervalFor(promCli, env.GetKubecostJobName())
		if err == nil {
			scrapeInterval = si
		}
	}

	log.Infof("Using scrape interval of %f", scrapeInterval.Seconds())

	// Kubernetes API setup
	kubeClientset, err := kubeconfig.LoadKubeClient("")
	if err != nil {
		log.Fatalf("Failed to build Kubernetes client: %s", err.Error())
	}

	// Create ConfigFileManager for synchronization of shared configuration
	confManager := config.NewConfigFileManager(&config.ConfigFileManagerOpts{
		BucketStoreConfig: env.GetKubecostConfigBucket(),
		LocalConfigPath:   "/",
	})

	configPrefix := env.GetConfigPathWithDefault("/var/configs/")

	// Create Kubernetes Cluster Cache + Watchers
	k8sCache := clustercache.NewKubernetesClusterCache(kubeClientset)
	k8sCache.Run()

	cloudProviderKey := env.GetCloudProviderAPIKey()
	cloudProvider, err := provider.NewProvider(k8sCache, cloudProviderKey, confManager)
	if err != nil {
		panic(err.Error())
	}

	// Append the pricing config watcher
	kubecostNamespace := env.GetKubecostNamespace()

	configWatchers := watcher.NewConfigMapWatchers(kubeClientset, kubecostNamespace, additionalConfigWatchers...)
	configWatchers.AddWatcher(provider.ConfigWatcherFor(cloudProvider))
	configWatchers.AddWatcher(metrics.GetMetricsConfigWatcher())
	configWatchers.Watch()

	remoteEnabled := env.IsRemoteEnabled()
	if remoteEnabled {
		info, err := cloudProvider.ClusterInfo()
		log.Infof("Saving cluster  with id:'%s', and name:'%s' to durable storage", info["id"], info["name"])
		if err != nil {
			log.Infof("Error saving cluster id %s", err.Error())
		}
		_, _, err = utils.GetOrCreateClusterMeta(info["id"], info["name"])
		if err != nil {
			log.Infof("Unable to set cluster id '%s' for cluster '%s', %s", info["id"], info["name"], err.Error())
		}
	}

	// Thanos Client
	var thanosClient prometheus.Client
	if thanos.IsEnabled() {
		thanosAddress := thanos.QueryURL()

		if thanosAddress != "" {
			thanosCli, _ := thanos.NewThanosClient(thanosAddress, &prom.PrometheusClientConfig{
				Timeout:               timeout,
				KeepAlive:             keepAlive,
				TLSHandshakeTimeout:   tlsHandshakeTimeout,
				TLSInsecureSkipVerify: env.GetInsecureSkipVerify(),
				RateLimitRetryOpts:    rateLimitRetryOpts,
				Auth: &prom.ClientAuth{
					Username:    env.GetMultiClusterBasicAuthUsername(),
					Password:    env.GetMultiClusterBasicAuthPassword(),
					BearerToken: env.GetMultiClusterBearerToken(),
				},
				QueryConcurrency: queryConcurrency,
				QueryLogFile:     env.GetQueryLoggingFile(),
			})

			_, err = prom.Validate(thanosCli)
			if err != nil {
				log.Warnf("Failed to query Thanos at %s. Error: %s.", thanosAddress, err.Error())
				thanosClient = thanosCli
			} else {
				log.Infof("Success: retrieved the 'up' query against Thanos at: " + thanosAddress)

				thanosClient = thanosCli
			}

		} else {
			log.Infof("Error resolving environment variable: $%s", env.ThanosQueryUrlEnvVar)
		}
	}

	// ClusterInfo Provider to provide the cluster map with local and remote cluster data
	var clusterInfoProvider clusters.ClusterInfoProvider
	if env.IsClusterInfoFileEnabled() {
		clusterInfoFile := confManager.ConfigFileAt(path.Join(configPrefix, "cluster-info.json"))
		clusterInfoProvider = NewConfiguredClusterInfoProvider(clusterInfoFile)
	} else {
		clusterInfoProvider = NewLocalClusterInfoProvider(kubeClientset, cloudProvider)
	}

	// Initialize ClusterMap for maintaining ClusterInfo by ClusterID
	var clusterMap clusters.ClusterMap
	if thanosClient != nil {
		clusterMap = clustermap.NewClusterMap(thanosClient, clusterInfoProvider, 10*time.Minute)
	} else {
		clusterMap = clustermap.NewClusterMap(promCli, clusterInfoProvider, 5*time.Minute)
	}

	// cache responses from model and aggregation for a default of 10 minutes;
	// clear expired responses every 20 minutes
	aggregateCache := cache.New(time.Minute*10, time.Minute*20)
	costDataCache := cache.New(time.Minute*10, time.Minute*20)
	clusterCostsCache := cache.New(cache.NoExpiration, cache.NoExpiration)
	outOfClusterCache := cache.New(time.Minute*5, time.Minute*10)
	settingsCache := cache.New(cache.NoExpiration, cache.NoExpiration)

	// query durations that should be cached longer should be registered here
	// use relatively prime numbers to minimize likelihood of synchronized
	// attempts at cache warming
	day := 24 * time.Hour
	cacheExpiration := map[time.Duration]time.Duration{
		day:      maxCacheMinutes1d * time.Minute,
		2 * day:  maxCacheMinutes2d * time.Minute,
		7 * day:  maxCacheMinutes7d * time.Minute,
		30 * day: maxCacheMinutes30d * time.Minute,
	}

	var pc prometheus.Client
	if thanosClient != nil {
		pc = thanosClient
	} else {
		pc = promCli
	}
	costModel := NewCostModel(pc, cloudProvider, k8sCache, clusterMap, scrapeInterval)
	metricsEmitter := NewCostModelMetricsEmitter(promCli, k8sCache, cloudProvider, clusterInfoProvider, costModel)

	a := &Accesses{
		httpServices:        services.NewCostModelServices(),
		PrometheusClient:    promCli,
		ThanosClient:        thanosClient,
		KubeClientSet:       kubeClientset,
		ClusterCache:        k8sCache,
		ClusterMap:          clusterMap,
		CloudProvider:       cloudProvider,
		ConfigFileManager:   confManager,
		ClusterInfoProvider: clusterInfoProvider,
		Model:               costModel,
		MetricsEmitter:      metricsEmitter,
		AggregateCache:      aggregateCache,
		CostDataCache:       costDataCache,
		ClusterCostsCache:   clusterCostsCache,
		OutOfClusterCache:   outOfClusterCache,
		SettingsCache:       settingsCache,
		CacheExpiration:     cacheExpiration,
	}

	// Use the Accesses instance, itself, as the CostModelAggregator. This is
	// confusing and unconventional, but necessary so that we can swap it
	// out for the ETL-adapted version elsewhere.
	// TODO clean this up once ETL is open-sourced.
	a.AggAPI = a

	// Initialize mechanism for subscribing to settings changes
	a.InitializeSettingsPubSub()
	err = a.CloudProvider.DownloadPricingData()
	if err != nil {
		log.Infof("Failed to download pricing data: " + err.Error())
	}

	// Warm the aggregate cache unless explicitly set to false
	if env.IsCacheWarmingEnabled() {
		log.Infof("Init: AggregateCostModel cache warming enabled")
		a.warmAggregateCostModelCache()
	} else {
		log.Infof("Init: AggregateCostModel cache warming disabled")
	}

	if !env.IsKubecostMetricsPodEnabled() {
		a.MetricsEmitter.Start()
	}

	a.httpServices.RegisterAll(router)

	router.GET("/costDataModel", a.CostDataModel)
	router.GET("/costDataModelRange", a.CostDataModelRange)
	router.GET("/aggregatedCostModel", a.AggregateCostModelHandler)
	router.GET("/allocation/compute", a.ComputeAllocationHandler)
	router.GET("/allocation/compute/summary", a.ComputeAllocationHandlerSummary)
	router.GET("/allNodePricing", a.GetAllNodePricing)
	router.POST("/refreshPricing", a.RefreshPricingData)
	router.GET("/clusterCostsOverTime", a.ClusterCostsOverTime)
	router.GET("/clusterCosts", a.ClusterCosts)
	router.GET("/clusterCostsFromCache", a.ClusterCostsFromCacheHandler)
	router.GET("/validatePrometheus", a.GetPrometheusMetadata)
	router.GET("/managementPlatform", a.ManagementPlatform)
	router.GET("/clusterInfo", a.ClusterInfo)
	router.GET("/clusterInfoMap", a.GetClusterInfoMap)
	router.GET("/serviceAccountStatus", a.GetServiceAccountStatus)
	router.GET("/pricingSourceStatus", a.GetPricingSourceStatus)
	router.GET("/pricingSourceSummary", a.GetPricingSourceSummary)
	router.GET("/pricingSourceCounts", a.GetPricingSourceCounts)

	// endpoints migrated from server
	router.GET("/prometheusRecordingRules", a.PrometheusRecordingRules)
	router.GET("/prometheusConfig", a.PrometheusConfig)
	router.GET("/prometheusTargets", a.PrometheusTargets)
	router.GET("/orphanedPods", a.GetOrphanedPods)
	router.GET("/installNamespace", a.GetInstallNamespace)
	router.GET("/installInfo", a.GetInstallInfo)
	router.POST("/serviceKey", a.AddServiceKey)
	router.GET("/helmValues", a.GetHelmValues)
	router.GET("/status", a.Status)

	// prom query proxies
	router.GET("/prometheusQuery", a.PrometheusQuery)
	router.GET("/prometheusQueryRange", a.PrometheusQueryRange)
	router.GET("/thanosQuery", a.ThanosQuery)
	router.GET("/thanosQueryRange", a.ThanosQueryRange)

	// diagnostics
	router.GET("/diagnostics/requestQueue", a.GetPrometheusQueueState)
	router.GET("/diagnostics/prometheusMetrics", a.GetPrometheusMetrics)

	return a
}

// InitializeCloudCost Initializes Cloud Cost pipeline and querier and registers endpoints
func InitializeCloudCost(router *httprouter.Router, providerConfig models.ProviderConfig) {
	log.Debugf("Cloud Cost config path: %s", env.GetCloudCostConfigPath())
	cloudConfigController := cloudconfig.NewMemoryController(providerConfig)

	repo := cloudcost.NewMemoryRepository()
	cloudCostPipelineService := cloudcost.NewPipelineService(repo, cloudConfigController, cloudcost.DefaultIngestorConfiguration())
	repoQuerier := cloudcost.NewRepositoryQuerier(repo)
	cloudCostQueryService := cloudcost.NewQueryService(repoQuerier, repoQuerier)

	router.GET("/cloud/config/export", cloudConfigController.GetExportConfigHandler())
	router.GET("/cloud/config/enable", cloudConfigController.GetEnableConfigHandler())
	router.GET("/cloud/config/disable", cloudConfigController.GetDisableConfigHandler())
	router.GET("/cloud/config/delete", cloudConfigController.GetDeleteConfigHandler())

	router.GET("/cloudCost", cloudCostQueryService.GetCloudCostHandler())
	router.GET("/cloudCost/view/graph", cloudCostQueryService.GetCloudCostViewGraphHandler())
	router.GET("/cloudCost/view/totals", cloudCostQueryService.GetCloudCostViewTotalsHandler())
	router.GET("/cloudCost/view/table", cloudCostQueryService.GetCloudCostViewTableHandler())

	router.GET("/cloudCost/status", cloudCostPipelineService.GetCloudCostStatusHandler())
	router.GET("/cloudCost/rebuild", cloudCostPipelineService.GetCloudCostRebuildHandler())
	router.GET("/cloudCost/repair", cloudCostPipelineService.GetCloudCostRepairHandler())
}

func InitializeCustomCost(router *httprouter.Router) *customcost.PipelineService {
	hourlyRepo := customcost.NewMemoryRepository()
	dailyRepo := customcost.NewMemoryRepository()
	ingConfig := customcost.DefaultIngestorConfiguration()
	var err error
	customCostPipelineService, err := customcost.NewPipelineService(hourlyRepo, dailyRepo, ingConfig)
	if err != nil {
		log.Errorf("error instantiating custom cost pipeline service: %v", err)
		return nil
	}

	customCostQuerier := customcost.NewRepositoryQuerier(hourlyRepo, dailyRepo, ingConfig.HourlyDuration, ingConfig.DailyDuration)
	customCostQueryService := customcost.NewQueryService(customCostQuerier)

	router.GET("/customCost/total", customCostQueryService.GetCustomCostTotalHandler())
	router.GET("/customCost/timeseries", customCostQueryService.GetCustomCostTimeseriesHandler())

	return customCostPipelineService
}

func writeErrorResponse(w http.ResponseWriter, code int, message string) {
	out := map[string]string{
		"message": message,
	}
	bytes, err := json.Marshal(out)
	if err != nil {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(500)
		fmt.Fprint(w, "unable to marshall json for error")
		log.Warnf("Failed to marshall JSON for error response: %s", err.Error())
		return
	}
	w.WriteHeader(code)
	fmt.Fprint(w, string(bytes))
}
