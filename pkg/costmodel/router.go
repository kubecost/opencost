package costmodel

import (
	"context"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kubecost/cost-model/pkg/config"
	"github.com/kubecost/cost-model/pkg/services"
	"github.com/kubecost/cost-model/pkg/util/httputil"
	"github.com/kubecost/cost-model/pkg/util/timeutil"
	"github.com/kubecost/cost-model/pkg/util/watcher"
	"github.com/microcosm-cc/bluemonday"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog"

	"github.com/julienschmidt/httprouter"

	sentry "github.com/getsentry/sentry-go"

	"github.com/kubecost/cost-model/pkg/cloud"
	"github.com/kubecost/cost-model/pkg/clustercache"
	"github.com/kubecost/cost-model/pkg/costmodel/clusters"
	"github.com/kubecost/cost-model/pkg/env"
	"github.com/kubecost/cost-model/pkg/errors"
	"github.com/kubecost/cost-model/pkg/kubecost"
	"github.com/kubecost/cost-model/pkg/log"
	"github.com/kubecost/cost-model/pkg/prom"
	"github.com/kubecost/cost-model/pkg/thanos"
	"github.com/kubecost/cost-model/pkg/util/json"
	prometheus "github.com/prometheus/client_golang/api"
	prometheusAPI "github.com/prometheus/client_golang/api/prometheus/v1"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/patrickmn/go-cache"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
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
	LogSeparator         = "+-------------------------------------------------------------------------------------"
)

var (
	// gitCommit is set by the build system
	gitCommit string
)

// Accesses defines a singleton application instance, providing access to
// Prometheus, Kubernetes, the cloud provider, and caches.
type Accesses struct {
	Router              *httprouter.Router
	PrometheusClient    prometheus.Client
	ThanosClient        prometheus.Client
	KubeClientSet       kubernetes.Interface
	ClusterCache        clustercache.ClusterCache
	ClusterMap          clusters.ClusterMap
	CloudProvider       cloud.Provider
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
		klog.V(1).Infof("to delete: %s", fieldNameLower)
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
		klog.V(1).Infof("Error returned to client: %s", err.Error())
		resp, _ = json.Marshal(&Response{
			Code:    http.StatusInternalServerError,
			Status:  "error",
			Message: err.Error(),
			Data:    data,
		})
	} else {
		resp, _ = json.Marshal(&Response{
			Code:   http.StatusOK,
			Status: "success",
			Data:   data,
		})
	}

	return resp
}

func WrapDataWithMessage(data interface{}, err error, message string) []byte {
	var resp []byte

	if err != nil {
		klog.V(1).Infof("Error returned to client: %s", err.Error())
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
		klog.V(1).Infof("Error returned to client: %s", err.Error())
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
		klog.V(1).Infof("Error returned to client: %s", err.Error())
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
		klog.V(1).Infof("Error refreshing pricing data: %s", err.Error())
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
		w.Write(WrapData(nil, fmt.Errorf("missing window arguement")))
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
		w.Write(WrapData(nil, fmt.Errorf("missing window arguement")))
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

	window := kubecost.NewWindow(&start, &end)
	if window.IsOpen() || window.IsEmpty() || window.IsNegative() {
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
	data, err := a.CloudProvider.UpdateConfig(r.Body, cloud.SpotInfoUpdateType)
	if err != nil {
		w.Write(WrapData(data, err))
		return
	}
	w.Write(WrapData(data, err))
	err = a.CloudProvider.DownloadPricingData()
	if err != nil {
		klog.V(1).Infof("Error redownloading data on config update: %s", err.Error())
	}
	return
}

func (a *Accesses) UpdateAthenaInfoConfigs(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	data, err := a.CloudProvider.UpdateConfig(r.Body, cloud.AthenaInfoUpdateType)
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
	data, err := a.CloudProvider.UpdateConfig(r.Body, cloud.BigqueryUpdateType)
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

	ctx := prom.NewNamedContext(a.PrometheusClient, prom.FrontendContextName)
	body, err := ctx.RawQuery(query)
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

	ctx := prom.NewNamedContext(a.ThanosClient, prom.FrontendContextName)
	body, err := ctx.RawQuery(query)
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
			log.Warningf("Error getting Thanos queue state: %s", err)
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

	promMetrics, err := prom.GetPrometheusMetrics(a.PrometheusClient, "")
	if err != nil {
		w.Write(WrapData(nil, err))
		return
	}

	result := map[string][]*prom.PrometheusDiagnostic{
		"prometheus": promMetrics,
	}

	if thanos.IsEnabled() {
		thanosMetrics, err := prom.GetPrometheusMetrics(a.ThanosClient, thanos.QueryOffset())
		if err != nil {
			log.Warningf("Error getting Thanos queue state: %s", err)
		} else {
			result["thanos"] = thanosMetrics
		}
	}

	w.Write(WrapData(result, nil))
}

func (a *Accesses) GetAllPersistentVolumes(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	pvList := a.ClusterCache.GetAllPersistentVolumes()

	body, err := json.Marshal(wrapAsObjectItems(pvList))
	if err != nil {
		fmt.Fprintf(w, "Error decoding persistent volumes: "+err.Error())
	} else {
		w.Write(body)
	}

}

func (a *Accesses) GetAllDeployments(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	qp := httputil.NewQueryParams(r.URL.Query())

	namespace := qp.Get("namespace", "")

	deploymentsList := a.ClusterCache.GetAllDeployments()

	// filter for provided namespace
	var deployments []*appsv1.Deployment
	if namespace == "" {
		deployments = deploymentsList
	} else {
		deployments = []*appsv1.Deployment{}

		for _, d := range deploymentsList {
			if d.Namespace == namespace {
				deployments = append(deployments, d)
			}
		}
	}

	body, err := json.Marshal(wrapAsObjectItems(deployments))
	if err != nil {
		fmt.Fprintf(w, "Error decoding deployment: "+err.Error())
	} else {
		w.Write(body)
	}
}

func (a *Accesses) GetAllStorageClasses(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	scList := a.ClusterCache.GetAllStorageClasses()

	body, err := json.Marshal(wrapAsObjectItems(scList))
	if err != nil {
		fmt.Fprintf(w, "Error decoding storageclasses: "+err.Error())
	} else {
		w.Write(body)
	}
}

func (a *Accesses) GetAllStatefulSets(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	qp := httputil.NewQueryParams(r.URL.Query())

	namespace := qp.Get("namespace", "")

	statefulSetsList := a.ClusterCache.GetAllStatefulSets()

	// filter for provided namespace
	var statefulSets []*appsv1.StatefulSet
	if namespace == "" {
		statefulSets = statefulSetsList
	} else {
		statefulSets = []*appsv1.StatefulSet{}

		for _, ss := range statefulSetsList {
			if ss.Namespace == namespace {
				statefulSets = append(statefulSets, ss)
			}
		}
	}

	body, err := json.Marshal(wrapAsObjectItems(statefulSets))
	if err != nil {
		fmt.Fprintf(w, "Error decoding deployment: "+err.Error())
	} else {
		w.Write(body)
	}
}

func (a *Accesses) GetAllNodes(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	nodeList := a.ClusterCache.GetAllNodes()

	body, err := json.Marshal(wrapAsObjectItems(nodeList))
	if err != nil {
		fmt.Fprintf(w, "Error decoding nodes: "+err.Error())
	} else {
		w.Write(body)
	}
}

func (a *Accesses) GetAllPods(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	podlist := a.ClusterCache.GetAllPods()

	body, err := json.Marshal(wrapAsObjectItems(podlist))
	if err != nil {
		fmt.Fprintf(w, "Error decoding pods: "+err.Error())
	} else {
		w.Write(body)
	}
}

func (a *Accesses) GetAllNamespaces(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	namespaces := a.ClusterCache.GetAllNamespaces()

	body, err := json.Marshal(wrapAsObjectItems(namespaces))
	if err != nil {
		fmt.Fprintf(w, "Error decoding deployment: "+err.Error())
	} else {
		w.Write(body)
	}
}

func (a *Accesses) GetAllDaemonSets(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	daemonSets := a.ClusterCache.GetAllDaemonSets()

	body, err := json.Marshal(wrapAsObjectItems(daemonSets))
	if err != nil {
		fmt.Fprintf(w, "Error decoding daemon set: "+err.Error())
	} else {
		w.Write(body)
	}
}

func (a *Accesses) GetPod(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	podName := ps.ByName("name")
	podNamespace := ps.ByName("namespace")

	// TODO: ClusterCache API could probably afford to have some better filtering
	allPods := a.ClusterCache.GetAllPods()
	for _, pod := range allPods {
		for _, container := range pod.Spec.Containers {
			container.Env = make([]v1.EnvVar, 0)
		}
		if pod.Namespace == podNamespace && pod.Name == podName {
			body, err := json.Marshal(pod)
			if err != nil {
				fmt.Fprintf(w, "Error decoding pod: "+err.Error())
			} else {
				w.Write(body)
			}
			return
		}
	}

	fmt.Fprintf(w, "Pod not found\n")
}

func (a *Accesses) PrometheusRecordingRules(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	u := a.PrometheusClient.URL(epRules, nil)

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		fmt.Fprintf(w, "Error creating Prometheus rule request: "+err.Error())
	}

	_, body, _, err := a.PrometheusClient.Do(r.Context(), req)
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

	_, body, _, err := a.PrometheusClient.Do(r.Context(), req)
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

	var lonePods []*v1.Pod
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

// logsFor pulls the logs for a specific pod, namespace, and container
func logsFor(c kubernetes.Interface, namespace string, pod string, container string, dur time.Duration, ctx context.Context) (string, error) {
	since := time.Now().UTC().Add(-dur)

	logOpts := v1.PodLogOptions{
		SinceTime: &metav1.Time{Time: since},
	}
	if container != "" {
		logOpts.Container = container
	}

	req := c.CoreV1().Pods(namespace).GetLogs(pod, &logOpts)
	reader, err := req.Stream(ctx)
	if err != nil {
		return "", err
	}

	podLogs, err := ioutil.ReadAll(reader)
	if err != nil {
		return "", err
	}

	return string(podLogs), nil
}

func (a *Accesses) GetPodLogs(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	qp := httputil.NewQueryParams(r.URL.Query())

	ns := qp.Get("namespace", env.GetKubecostNamespace())
	pod := qp.Get("pod", "")
	selector := qp.Get("selector", "")
	container := qp.Get("container", "")
	since := qp.Get("since", "24h")

	sinceDuration, err := time.ParseDuration(since)
	if err != nil {
		fmt.Fprintf(w, "Invalid Duration String: "+err.Error())
		return
	}

	var logResult string
	appendLog := func(ns string, pod string, container string, l string) {
		if l == "" {
			return
		}

		logResult += fmt.Sprintf("%s\n| %s:%s:%s\n%s\n%s\n\n", LogSeparator, ns, pod, container, LogSeparator, l)
	}

	if pod != "" {
		pd, err := a.KubeClientSet.CoreV1().Pods(ns).Get(r.Context(), pod, metav1.GetOptions{})
		if err != nil {
			fmt.Fprintf(w, "Error Finding Pod: "+err.Error())
			return
		}

		if container != "" {
			var foundContainer bool
			for _, cont := range pd.Spec.Containers {
				if strings.EqualFold(cont.Name, container) {
					foundContainer = true
					break
				}
			}
			if !foundContainer {
				fmt.Fprintf(w, "Could not find container: "+container)
				return
			}
		}

		logs, err := logsFor(a.KubeClientSet, ns, pod, container, sinceDuration, r.Context())
		if err != nil {
			fmt.Fprintf(w, "Error Getting Logs: "+err.Error())
			return
		}

		appendLog(ns, pod, container, logs)

		w.Write([]byte(logResult))
		return
	}

	if selector != "" {
		pods, err := a.KubeClientSet.CoreV1().Pods(ns).List(r.Context(), metav1.ListOptions{LabelSelector: selector})
		if err != nil {
			fmt.Fprintf(w, "Error Finding Pod: "+err.Error())
			return
		}

		for _, pd := range pods.Items {
			for _, cont := range pd.Spec.Containers {
				logs, err := logsFor(a.KubeClientSet, ns, pd.Name, cont.Name, sinceDuration, r.Context())
				if err != nil {
					continue
				}
				appendLog(ns, pd.Name, cont.Name, logs)
			}
		}
	}

	w.Write([]byte(logResult))
}

func (a *Accesses) AddServiceKey(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	r.ParseForm()

	key := r.PostForm.Get("key")
	k := []byte(key)
	err := ioutil.WriteFile("/var/configs/key.json", k, 0644)
	if err != nil {
		fmt.Fprintf(w, "Error writing service key: "+err.Error())
	}

	w.WriteHeader(http.StatusOK)
}

func (a *Accesses) GetHelmValues(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	encodedValues := env.Get("HELM_VALUES", "")
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
	result, err := api.Config(r.Context())
	if err != nil {
		fmt.Fprintf(w, "Using Prometheus at "+promServer+". Error: "+err.Error())
	} else {

		fmt.Fprintf(w, "Using Prometheus at "+promServer+". PrometheusConfig: "+result.YAML)
	}
}

// captures the panic event in sentry
func capturePanicEvent(err string, stack string) {
	msg := fmt.Sprintf("Panic: %s\nStackTrace: %s\n", err, stack)
	klog.V(1).Infoln(msg)
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

func Initialize(additionalConfigWatchers ...*watcher.ConfigMapWatcher) *Accesses {
	klog.V(1).Infof("Starting cost-model (git commit \"%s\")", env.GetAppVersion())

	configWatchers := watcher.NewConfigMapWatchers(additionalConfigWatchers...)

	var err error
	if errorReportingEnabled {
		err = sentry.Init(sentry.ClientOptions{Release: env.GetAppVersion()})
		if err != nil {
			klog.Infof("Failed to initialize sentry for error reporting")
		} else {
			err = errors.SetPanicHandler(handlePanic)
			if err != nil {
				klog.Infof("Failed to set panic handler: %s", err)
			}
		}
	}

	address := env.GetPrometheusServerEndpoint()
	if address == "" {
		klog.Fatalf("No address for prometheus set in $%s. Aborting.", env.PrometheusServerEndpointEnvVar)
	}

	queryConcurrency := env.GetMaxQueryConcurrency()
	klog.Infof("Prometheus/Thanos Client Max Concurrency set to %d", queryConcurrency)

	timeout := 120 * time.Second
	keepAlive := 120 * time.Second
	scrapeInterval := time.Minute

	promCli, err := prom.NewPrometheusClient(address, timeout, keepAlive, queryConcurrency, "")
	if err != nil {
		klog.Fatalf("Failed to create prometheus client, Error: %v", err)
	}

	m, err := prom.Validate(promCli)
	if err != nil || !m.Running {
		if err != nil {
			klog.Errorf("Failed to query prometheus at %s. Error: %s . Troubleshooting help available at: %s", address, err.Error(), prom.PrometheusTroubleshootingURL)
		} else if !m.Running {
			klog.Errorf("Prometheus at %s is not running. Troubleshooting help available at: %s", address, prom.PrometheusTroubleshootingURL)
		}
	} else {
		klog.V(1).Info("Success: retrieved the 'up' query against prometheus at: " + address)
	}

	api := prometheusAPI.NewAPI(promCli)
	_, err = api.Config(context.Background())
	if err != nil {
		klog.Infof("No valid prometheus config file at %s. Error: %s . Troubleshooting help available at: %s. Ignore if using cortex/thanos here.", address, err.Error(), prom.PrometheusTroubleshootingURL)
	} else {
		klog.Infof("Retrieved a prometheus config file from: %s", address)
	}

	// Lookup scrape interval for kubecost job, update if found
	si, err := prom.ScrapeIntervalFor(promCli, env.GetKubecostJobName())
	if err == nil {
		scrapeInterval = si
	}

	klog.Infof("Using scrape interval of %f", scrapeInterval.Seconds())

	// Kubernetes API setup
	var kc *rest.Config
	if kubeconfig := env.GetKubeConfigPath(); kubeconfig != "" {
		kc, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		kc, err = rest.InClusterConfig()
	}

	if err != nil {
		panic(err.Error())
	}
	kubeClientset, err := kubernetes.NewForConfig(kc)
	if err != nil {
		panic(err.Error())
	}

	// Create ConfigFileManager for synchronization of shared configuration
	confManager := config.NewConfigFileManager(&config.ConfigFileManagerOpts{
		BucketStoreConfig: env.GetKubecostConfigBucket(),
		LocalConfigPath:   "/",
	})

	// Create Kubernetes Cluster Cache + Watchers
	var k8sCache clustercache.ClusterCache
	if env.IsClusterCacheFileEnabled() {
		importLocation := confManager.ConfigFileAt("/var/configs/cluster-cache.json")
		k8sCache = clustercache.NewClusterImporter(importLocation)
	} else {
		k8sCache = clustercache.NewKubernetesClusterCache(kubeClientset)
	}
	k8sCache.Run()

	cloudProviderKey := env.GetCloudProviderAPIKey()
	cloudProvider, err := cloud.NewProvider(k8sCache, cloudProviderKey, confManager)
	if err != nil {
		panic(err.Error())
	}

	// Append the pricing config watcher
	configWatchers.AddWatcher(cloud.ConfigWatcherFor(cloudProvider))
	watchConfigFunc := configWatchers.ToWatchFunc()
	watchedConfigs := configWatchers.GetWatchedConfigs()

	kubecostNamespace := env.GetKubecostNamespace()
	// We need an initial invocation because the init of the cache has happened before we had access to the provider.
	for _, cw := range watchedConfigs {
		configs, err := kubeClientset.CoreV1().ConfigMaps(kubecostNamespace).Get(context.Background(), cw, metav1.GetOptions{})
		if err != nil {
			klog.Infof("No %s configmap found at install time, using existing configs: %s", cw, err.Error())
		} else {
			klog.Infof("Found configmap %s, watching...", configs.Name)
			watchConfigFunc(configs)
		}
	}

	k8sCache.SetConfigMapUpdateFunc(watchConfigFunc)

	remoteEnabled := env.IsRemoteEnabled()
	if remoteEnabled {
		info, err := cloudProvider.ClusterInfo()
		klog.Infof("Saving cluster  with id:'%s', and name:'%s' to durable storage", info["id"], info["name"])
		if err != nil {
			klog.Infof("Error saving cluster id %s", err.Error())
		}
		_, _, err = cloud.GetOrCreateClusterMeta(info["id"], info["name"])
		if err != nil {
			klog.Infof("Unable to set cluster id '%s' for cluster '%s', %s", info["id"], info["name"], err.Error())
		}
	}

	// Thanos Client
	var thanosClient prometheus.Client
	if thanos.IsEnabled() {
		thanosAddress := thanos.QueryURL()

		if thanosAddress != "" {
			thanosCli, _ := thanos.NewThanosClient(thanosAddress, timeout, keepAlive, queryConcurrency, env.GetQueryLoggingFile())

			_, err = prom.Validate(thanosCli)
			if err != nil {
				klog.V(1).Infof("[Warning] Failed to query Thanos at %s. Error: %s.", thanosAddress, err.Error())
				thanosClient = thanosCli
			} else {
				klog.V(1).Info("Success: retrieved the 'up' query against Thanos at: " + thanosAddress)

				thanosClient = thanosCli
			}

		} else {
			klog.Infof("Error resolving environment variable: $%s", env.ThanosQueryUrlEnvVar)
		}
	}

	// ClusterInfo Provider to provide the cluster map with local and remote cluster data
	var clusterInfoProvider clusters.ClusterInfoProvider
	if env.IsClusterInfoFileEnabled() {
		clusterInfoFile := confManager.ConfigFileAt("/var/configs/cluster-info.json")
		clusterInfoProvider = NewConfiguredClusterInfoProvider(clusterInfoFile)
	} else {
		clusterInfoProvider = NewLocalClusterInfoProvider(kubeClientset, cloudProvider)
	}

	// Initialize ClusterMap for maintaining ClusterInfo by ClusterID
	var clusterMap clusters.ClusterMap
	if thanosClient != nil {
		clusterMap = clusters.NewClusterMap(thanosClient, clusterInfoProvider, 10*time.Minute)
	} else {
		clusterMap = clusters.NewClusterMap(promCli, clusterInfoProvider, 5*time.Minute)
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
		Router:              httprouter.New(),
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
		httpServices:        services.NewCostModelServices(),
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
		klog.V(1).Info("Failed to download pricing data: " + err.Error())
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

	a.Router.GET("/costDataModel", a.CostDataModel)
	a.Router.GET("/costDataModelRange", a.CostDataModelRange)
	a.Router.GET("/aggregatedCostModel", a.AggregateCostModelHandler)
	a.Router.GET("/allocation/compute", a.ComputeAllocationHandler)
	a.Router.GET("/allNodePricing", a.GetAllNodePricing)
	a.Router.POST("/refreshPricing", a.RefreshPricingData)
	a.Router.GET("/clusterCostsOverTime", a.ClusterCostsOverTime)
	a.Router.GET("/clusterCosts", a.ClusterCosts)
	a.Router.GET("/clusterCostsFromCache", a.ClusterCostsFromCacheHandler)
	a.Router.GET("/validatePrometheus", a.GetPrometheusMetadata)
	a.Router.GET("/managementPlatform", a.ManagementPlatform)
	a.Router.GET("/clusterInfo", a.ClusterInfo)
	a.Router.GET("/clusterInfoMap", a.GetClusterInfoMap)
	a.Router.GET("/serviceAccountStatus", a.GetServiceAccountStatus)
	a.Router.GET("/pricingSourceStatus", a.GetPricingSourceStatus)
	a.Router.GET("/pricingSourceCounts", a.GetPricingSourceCounts)

	// endpoints migrated from server
	a.Router.GET("/allPersistentVolumes", a.GetAllPersistentVolumes)
	a.Router.GET("/allDeployments", a.GetAllDeployments)
	a.Router.GET("/allStorageClasses", a.GetAllStorageClasses)
	a.Router.GET("/allStatefulSets", a.GetAllStatefulSets)
	a.Router.GET("/allNodes", a.GetAllNodes)
	a.Router.GET("/allPods", a.GetAllPods)
	a.Router.GET("/allNamespaces", a.GetAllNamespaces)
	a.Router.GET("/allDaemonSets", a.GetAllDaemonSets)
	a.Router.GET("/pod/:namespace/:name", a.GetPod)
	a.Router.GET("/prometheusRecordingRules", a.PrometheusRecordingRules)
	a.Router.GET("/prometheusConfig", a.PrometheusConfig)
	a.Router.GET("/prometheusTargets", a.PrometheusTargets)
	a.Router.GET("/orphanedPods", a.GetOrphanedPods)
	a.Router.GET("/installNamespace", a.GetInstallNamespace)
	a.Router.GET("/podLogs", a.GetPodLogs)
	a.Router.POST("/serviceKey", a.AddServiceKey)
	a.Router.GET("/helmValues", a.GetHelmValues)
	a.Router.GET("/status", a.Status)

	// prom query proxies
	a.Router.GET("/prometheusQuery", a.PrometheusQuery)
	a.Router.GET("/prometheusQueryRange", a.PrometheusQueryRange)
	a.Router.GET("/thanosQuery", a.ThanosQuery)
	a.Router.GET("/thanosQueryRange", a.ThanosQueryRange)

	// diagnostics
	a.Router.GET("/diagnostics/requestQueue", a.GetPrometheusQueueState)
	a.Router.GET("/diagnostics/prometheusMetrics", a.GetPrometheusMetrics)

	a.httpServices.RegisterAll(a.Router)

	return a
}
