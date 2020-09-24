package costmodel

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	"k8s.io/klog"

	"github.com/julienschmidt/httprouter"

	sentry "github.com/getsentry/sentry-go"

	costAnalyzerCloud "github.com/kubecost/cost-model/pkg/cloud"
	"github.com/kubecost/cost-model/pkg/clustercache"
	cm "github.com/kubecost/cost-model/pkg/clustermanager"
	"github.com/kubecost/cost-model/pkg/env"
	"github.com/kubecost/cost-model/pkg/errors"
	"github.com/kubecost/cost-model/pkg/prom"
	"github.com/kubecost/cost-model/pkg/thanos"
	prometheusClient "github.com/prometheus/client_golang/api"
	prometheusAPI "github.com/prometheus/client_golang/api/prometheus/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/patrickmn/go-cache"
	"github.com/prometheus/client_golang/prometheus"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	prometheusTroubleshootingEp = "http://docs.kubecost.com/custom-prom#troubleshoot"
	RFC3339Milli                = "2006-01-02T15:04:05.000Z"
)

var (
	// gitCommit is set by the build system
	gitCommit                       string
	dbBasicAuthUsername             string = env.GetDBBasicAuthUsername()
	dbBasicAuthPW                   string = env.GetDBBasicAuthUserPassword()
	dbBearerToken                   string = env.GetDBBearerToken()
	multiclusterDBBasicAuthUsername string = env.GetMultiClusterBasicAuthUsername()
	multiclusterDBBasicAuthPW       string = env.GetMultiClusterBasicAuthPassword()
	multiClusterBearerToken         string = env.GetMultiClusterBearerToken()
)

var Router = httprouter.New()
var A Accesses

type Accesses struct {
	PrometheusClient              prometheusClient.Client
	ThanosClient                  prometheusClient.Client
	KubeClientSet                 kubernetes.Interface
	ClusterManager                *cm.ClusterManager
	Cloud                         costAnalyzerCloud.Provider
	CPUPriceRecorder              *prometheus.GaugeVec
	RAMPriceRecorder              *prometheus.GaugeVec
	PersistentVolumePriceRecorder *prometheus.GaugeVec
	GPUPriceRecorder              *prometheus.GaugeVec
	NodeTotalPriceRecorder        *prometheus.GaugeVec
	NodeSpotRecorder              *prometheus.GaugeVec
	RAMAllocationRecorder         *prometheus.GaugeVec
	CPUAllocationRecorder         *prometheus.GaugeVec
	GPUAllocationRecorder         *prometheus.GaugeVec
	PVAllocationRecorder          *prometheus.GaugeVec
	ClusterManagementCostRecorder *prometheus.GaugeVec
	NetworkZoneEgressRecorder     prometheus.Gauge
	NetworkRegionEgressRecorder   prometheus.Gauge
	NetworkInternetEgressRecorder prometheus.Gauge
	ServiceSelectorRecorder       *prometheus.GaugeVec
	DeploymentSelectorRecorder    *prometheus.GaugeVec
	Model                         *CostModel
	OutOfClusterCache             *cache.Cache
}

type DataEnvelope struct {
	Code    int         `json:"code"`
	Status  string      `json:"status"`
	Data    interface{} `json:"data"`
	Message string      `json:"message,omitempty"`
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

// parsePercentString takes a string of expected format "N%" and returns a floating point 0.0N.
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

// parseDuration converts a Prometheus-style duration string into a Duration
func ParseDuration(duration string) (*time.Duration, error) {
	unitStr := duration[len(duration)-1:]
	var unit time.Duration
	switch unitStr {
	case "s":
		unit = time.Second
	case "m":
		unit = time.Minute
	case "h":
		unit = time.Hour
	case "d":
		unit = 24.0 * time.Hour
	default:
		return nil, fmt.Errorf("error parsing duration: %s did not match expected format [0-9+](s|m|d|h)", duration)
	}

	amountStr := duration[:len(duration)-1]
	amount, err := strconv.ParseInt(amountStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("error parsing duration: %s did not match expected format [0-9+](s|m|d|h)", duration)
	}

	dur := time.Duration(amount) * unit
	return &dur, nil
}

// ParseTimeRange returns a start and end time, respectively, which are converted from
// a duration and offset, defined as strings with Prometheus-style syntax.
func ParseTimeRange(duration, offset string) (*time.Time, *time.Time, error) {
	// endTime defaults to the current time, unless an offset is explicity declared,
	// in which case it shifts endTime back by given duration
	endTime := time.Now()
	if offset != "" {
		o, err := ParseDuration(offset)
		if err != nil {
			return nil, nil, fmt.Errorf("error parsing offset (%s): %s", offset, err)
		}
		endTime = endTime.Add(-1 * *o)
	}

	// if duration is defined in terms of days, convert to hours
	// e.g. convert "2d" to "48h"
	durationNorm, err := normalizeTimeParam(duration)
	if err != nil {
		return nil, nil, fmt.Errorf("error parsing duration (%s): %s", duration, err)
	}

	// convert time duration into start and end times, formatted
	// as ISO datetime strings
	dur, err := time.ParseDuration(durationNorm)
	if err != nil {
		return nil, nil, fmt.Errorf("errorf parsing duration (%s): %s", durationNorm, err)
	}
	startTime := endTime.Add(-1 * dur)

	return &startTime, &endTime, nil
}

func WrapDataWithMessage(data interface{}, err error, message string) []byte {
	var resp []byte

	if err != nil {
		klog.V(1).Infof("Error returned to client: %s", err.Error())
		resp, _ = json.Marshal(&DataEnvelope{
			Code:    http.StatusInternalServerError,
			Status:  "error",
			Message: err.Error(),
			Data:    data,
		})
	} else {
		resp, _ = json.Marshal(&DataEnvelope{
			Code:    http.StatusOK,
			Status:  "success",
			Data:    data,
			Message: message,
		})

	}

	return resp
}

func WrapData(data interface{}, err error) []byte {
	var resp []byte

	if err != nil {
		klog.V(1).Infof("Error returned to client: %s", err.Error())
		resp, _ = json.Marshal(&DataEnvelope{
			Code:    http.StatusInternalServerError,
			Status:  "error",
			Message: err.Error(),
			Data:    data,
		})
	} else {
		resp, _ = json.Marshal(&DataEnvelope{
			Code:   http.StatusOK,
			Status: "success",
			Data:   data,
		})

	}

	return resp
}

// RefreshPricingData needs to be called when a new node joins the fleet, since we cache the relevant subsets of pricing data to avoid storing the whole thing.
func (a *Accesses) RefreshPricingData(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	err := a.Cloud.DownloadPricingData()

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

	data, err := a.Model.ComputeCostData(a.PrometheusClient, a.KubeClientSet, a.Cloud, window, offset, namespace)

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

	useThanos, _ := strconv.ParseBool(r.URL.Query().Get("multi"))

	if useThanos && !thanos.IsEnabled() {
		w.Write(WrapData(nil, fmt.Errorf("Multi=true while Thanos is not enabled.")))
		return
	}

	var client prometheusClient.Client
	if useThanos {
		client = a.ThanosClient
		offset = thanos.Offset()
	} else {
		client = a.PrometheusClient
	}

	data, err := ComputeClusterCosts(client, a.Cloud, window, offset, true)
	w.Write(WrapData(data, err))
}

func (a *Accesses) ClusterCostsOverTime(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	start := r.URL.Query().Get("start")
	end := r.URL.Query().Get("end")
	window := r.URL.Query().Get("window")
	offset := r.URL.Query().Get("offset")

	data, err := ClusterCostsOverTime(a.PrometheusClient, a.Cloud, start, end, window, offset)
	w.Write(WrapData(data, err))
}

func (a *Accesses) CostDataModelRange(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	start := r.URL.Query().Get("start")
	end := r.URL.Query().Get("end")
	window := r.URL.Query().Get("window")
	fields := r.URL.Query().Get("filterFields")
	namespace := r.URL.Query().Get("namespace")
	cluster := r.URL.Query().Get("cluster")
	remote := r.URL.Query().Get("remote")

	remoteEnabled := env.IsRemoteEnabled() && remote != "false"

	// Use Thanos Client if it exists (enabled) and remote flag set
	var pClient prometheusClient.Client
	if remote != "false" && a.ThanosClient != nil {
		pClient = a.ThanosClient
	} else {
		pClient = a.PrometheusClient
	}

	resolutionHours := 1.0
	data, err := a.Model.ComputeCostDataRange(pClient, a.KubeClientSet, a.Cloud, start, end, window, resolutionHours, namespace, cluster, remoteEnabled)
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

// CostDataModelRangeLarge is experimental multi-cluster and long-term data storage in SQL support.
func (a *Accesses) CostDataModelRangeLarge(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	startString := r.URL.Query().Get("start")
	endString := r.URL.Query().Get("end")
	windowString := r.URL.Query().Get("window")

	var start time.Time
	var end time.Time
	var err error

	if windowString == "" {
		windowString = "1h"
	}
	if startString != "" {
		start, err = time.Parse(RFC3339Milli, startString)
		if err != nil {
			klog.V(1).Infof("Error parsing time " + startString + ". Error: " + err.Error())
			w.Write(WrapData(nil, err))
		}
	} else {
		window, err := time.ParseDuration(windowString)
		if err != nil {
			w.Write(WrapData(nil, fmt.Errorf("Invalid duration '%s'", windowString)))

		}
		start = time.Now().Add(-2 * window)
	}
	if endString != "" {
		end, err = time.Parse(RFC3339Milli, endString)
		if err != nil {
			klog.V(1).Infof("Error parsing time " + endString + ". Error: " + err.Error())
			w.Write(WrapData(nil, err))
		}
	} else {
		end = time.Now()
	}

	remoteLayout := "2006-01-02T15:04:05Z"
	remoteStartStr := start.Format(remoteLayout)
	remoteEndStr := end.Format(remoteLayout)
	klog.V(1).Infof("Using remote database for query from %s to %s with window %s", startString, endString, windowString)

	data, err := CostDataRangeFromSQL("", "", windowString, remoteStartStr, remoteEndStr)
	w.Write(WrapData(data, err))
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

func (a *Accesses) OutofClusterCosts(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	start := r.URL.Query().Get("start")
	end := r.URL.Query().Get("end")
	aggregator := r.URL.Query().Get("aggregator")
	customAggregation := r.URL.Query().Get("customAggregation")
	filterType := r.URL.Query().Get("filterType")
	filterValue := r.URL.Query().Get("filterValue")
	var data []*costAnalyzerCloud.OutOfClusterAllocation
	var err error
	_, aggregations, filter := parseAggregations(customAggregation, aggregator, filterType)
	data, err = a.Cloud.ExternalAllocations(start, end, aggregations, filter, filterValue, false)
	w.Write(WrapData(data, err))
}

func (a *Accesses) OutOfClusterCostsWithCache(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	// start date for which to query costs, inclusive; format YYYY-MM-DD
	start := r.URL.Query().Get("start")
	// end date for which to query costs, inclusive; format YYYY-MM-DD
	end := r.URL.Query().Get("end")
	// aggregator sets the field by which to aggregate; default, prepended by "kubernetes_"
	kubernetesAggregation := r.URL.Query().Get("aggregator")
	// customAggregation allows full customization of aggregator w/o prepending
	customAggregation := r.URL.Query().Get("customAggregation")
	// disableCache, if set to "true", tells this function to recompute and
	// cache the requested data
	disableCache := r.URL.Query().Get("disableCache") == "true"
	// clearCache, if set to "true", tells this function to flush the cache,
	// then recompute and cache the requested data
	clearCache := r.URL.Query().Get("clearCache") == "true"

	filterType := r.URL.Query().Get("filterType")
	filterValue := r.URL.Query().Get("filterValue")

	aggregationkey, aggregation, filter := parseAggregations(customAggregation, kubernetesAggregation, filterType)

	// clear cache prior to checking the cache so that a clearCache=true
	// request always returns a freshly computed value
	if clearCache {
		a.OutOfClusterCache.Flush()
	}

	// attempt to retrieve cost data from cache
	key := fmt.Sprintf(`%s:%s:%s:%s:%s`, start, end, aggregationkey, filter, filterValue)
	if value, found := a.OutOfClusterCache.Get(key); found && !disableCache {
		if data, ok := value.([]*costAnalyzerCloud.OutOfClusterAllocation); ok {
			w.Write(WrapDataWithMessage(data, nil, fmt.Sprintf("out of cluster cache hit: %s", key)))
			return
		}
		klog.Errorf("caching error: failed to type cast data: %s", key)
	}

	data, err := a.Cloud.ExternalAllocations(start, end, aggregation, filter, filterValue, false)
	if err == nil {
		a.OutOfClusterCache.Set(key, data, cache.DefaultExpiration)
	}

	w.Write(WrapDataWithMessage(data, err, fmt.Sprintf("out of cluser cache miss: %s", key)))
}

func (p *Accesses) GetAllNodePricing(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	data, err := p.Cloud.AllNodePricing()
	w.Write(WrapData(data, err))
}

func (p *Accesses) GetConfigs(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	data, err := p.Cloud.GetConfig()
	w.Write(WrapData(data, err))
}

func (p *Accesses) UpdateSpotInfoConfigs(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	data, err := p.Cloud.UpdateConfig(r.Body, costAnalyzerCloud.SpotInfoUpdateType)
	if err != nil {
		w.Write(WrapData(data, err))
		return
	}
	w.Write(WrapData(data, err))
	err = p.Cloud.DownloadPricingData()
	if err != nil {
		klog.V(1).Infof("Error redownloading data on config update: %s", err.Error())
	}
	return
}

func (p *Accesses) UpdateAthenaInfoConfigs(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	data, err := p.Cloud.UpdateConfig(r.Body, costAnalyzerCloud.AthenaInfoUpdateType)
	if err != nil {
		w.Write(WrapData(data, err))
		return
	}
	w.Write(WrapData(data, err))
	return
}

func (p *Accesses) UpdateBigQueryInfoConfigs(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	data, err := p.Cloud.UpdateConfig(r.Body, costAnalyzerCloud.BigqueryUpdateType)
	if err != nil {
		w.Write(WrapData(data, err))
		return
	}
	w.Write(WrapData(data, err))
	return
}

func (p *Accesses) UpdateConfigByKey(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	data, err := p.Cloud.UpdateConfig(r.Body, "")
	if err != nil {
		w.Write(WrapData(data, err))
		return
	}
	w.Write(WrapData(data, err))
	return
}

func (p *Accesses) ManagementPlatform(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	data, err := p.Cloud.GetManagementPlatform()
	if err != nil {
		w.Write(WrapData(data, err))
		return
	}
	w.Write(WrapData(data, err))
	return
}

func (p *Accesses) ClusterInfo(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	data := GetClusterInfo(p.KubeClientSet, p.Cloud)

	w.Write(WrapData(data, nil))
}

func (p *Accesses) GetServiceAccountStatus(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	w.Write(WrapData(A.Cloud.ServiceAccountStatus(), nil))
}

func (p *Accesses) GetPrometheusMetadata(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Write(WrapData(ValidatePrometheus(p.PrometheusClient, false)))
}

// Creates a new ClusterManager instance using a boltdb storage. If that fails,
// then we fall back to a memory-only storage.
func newClusterManager() *cm.ClusterManager {
	clustersConfigFile := "/var/configs/clusters/default-clusters.yaml"

	// Return a memory-backed cluster manager populated by configmap
	return cm.NewConfiguredClusterManager(cm.NewMapDBClusterStorage(), clustersConfigFile)

	// NOTE: The following should be used with a persistent disk store. Since the
	// NOTE: configmap approach is currently the "persistent" source (entries are read-only
	// NOTE: on the backend), we don't currently need to store on disk.
	/*
		path := env.GetConfigPath()
		db, err := bolt.Open(path+"costmodel.db", 0600, nil)
		if err != nil {
			klog.V(1).Infof("[Error] Failed to create costmodel.db: %s", err.Error())
			return cm.NewConfiguredClusterManager(cm.NewMapDBClusterStorage(), clustersConfigFile)
		}

		store, err := cm.NewBoltDBClusterStorage("clusters", db)
		if err != nil {
			klog.V(1).Infof("[Error] Failed to Create Cluster Storage: %s", err.Error())
			return cm.NewConfiguredClusterManager(cm.NewMapDBClusterStorage(), clustersConfigFile)
		}

		return cm.NewConfiguredClusterManager(store, clustersConfigFile)
	*/
}

type ConfigWatchers struct {
	ConfigmapName string
	WatchFunc     func(string, map[string]string) error
}

// captures the panic event in sentry
func capturePanicEvent(err string, stack string) {
	msg := fmt.Sprintf("Panic: %s\nStackTrace: %s\n", err, stack)
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

func Initialize(additionalConfigWatchers ...ConfigWatchers) {
	klog.InitFlags(nil)
	flag.Set("v", "3")
	flag.Parse()
	klog.V(1).Infof("Starting cost-model (git commit \"%s\")", gitCommit)

	var err error
	if errorReportingEnabled {
		err = sentry.Init(sentry.ClientOptions{Release: gitCommit})
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

	tlsConfig := &tls.Config{InsecureSkipVerify: env.GetInsecureSkipVerify()}
	var LongTimeoutRoundTripper http.RoundTripper = &http.Transport{ // may be necessary for long prometheus queries. TODO: make this configurable
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   120 * time.Second,
			KeepAlive: 120 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout: 10 * time.Second,
		TLSClientConfig:     tlsConfig,
	}

	pc := prometheusClient.Config{
		Address:      address,
		RoundTripper: LongTimeoutRoundTripper,
	}
	promCli, _ := prom.NewRateLimitedClient(pc, queryConcurrency, dbBasicAuthUsername, dbBasicAuthPW, dbBearerToken, env.GetQueryLoggingFile())

	m, err := ValidatePrometheus(promCli, false)
	if err != nil || m.Running == false {
		if err != nil {
			klog.Errorf("Failed to query prometheus at %s. Error: %s . Troubleshooting help available at: %s", address, err.Error(), prometheusTroubleshootingEp)
		} else if m.Running == false {
			klog.Errorf("Prometheus at %s is not running. Troubleshooting help available at: %s", address, prometheusTroubleshootingEp)
		}
		api := prometheusAPI.NewAPI(promCli)
		_, err = api.Config(context.Background())
		if err != nil {
			klog.Infof("No valid prometheus config file at %s. Error: %s . Troubleshooting help available at: %s. Ignore if using cortex/thanos here.", address, err.Error(), prometheusTroubleshootingEp)
		} else {
			klog.V(1).Info("Retrieved a prometheus config file from: " + address)
		}
	} else {
		klog.V(1).Info("Success: retrieved the 'up' query against prometheus at: " + address)
	}

	// Kubernetes API setup
	kc, err := rest.InClusterConfig()
	if err != nil {
		panic(err.Error())
	}
	kubeClientset, err := kubernetes.NewForConfig(kc)
	if err != nil {
		panic(err.Error())
	}

	// Create Kubernetes Cluster Cache + Watchers
	k8sCache := clustercache.NewKubernetesClusterCache(kubeClientset)
	k8sCache.Run()

	cloudProviderKey := env.GetCloudProviderAPIKey()
	cloudProvider, err := costAnalyzerCloud.NewProvider(k8sCache, cloudProviderKey)
	if err != nil {
		panic(err.Error())
	}

	watchConfigFunc := func(c interface{}) {
		conf := c.(*v1.ConfigMap)
		if conf.GetName() == "pricing-configs" {
			_, err := cloudProvider.UpdateConfigFromConfigMap(conf.Data)
			if err != nil {
				klog.Infof("ERROR UPDATING %s CONFIG: %s", "pricing-configs", err.Error())
			}
		}
		for _, cw := range additionalConfigWatchers {
			if conf.GetName() == cw.ConfigmapName {
				err := cw.WatchFunc(conf.GetName(), conf.Data)
				if err != nil {
					klog.Infof("ERROR UPDATING %s CONFIG: %s", cw.ConfigmapName, err.Error())
				}
			}
		}
	}
	kubecostNamespace := env.GetKubecostNamespace()
	// We need an initial invocation because the init of the cache has happened before we had access to the provider.
	configs, err := kubeClientset.CoreV1().ConfigMaps(kubecostNamespace).Get("pricing-configs", metav1.GetOptions{})
	if err != nil {
		klog.Infof("No %s configmap found at installtime, using existing configs: %s", "pricing-configs", err.Error())
	} else {
		watchConfigFunc(configs)
	}

	for _, cw := range additionalConfigWatchers {
		configs, err := kubeClientset.CoreV1().ConfigMaps(kubecostNamespace).Get(cw.ConfigmapName, metav1.GetOptions{})
		if err != nil {
			klog.Infof("No %s configmap found at installtime, using existing configs: %s", cw.ConfigmapName, err.Error())
		} else {
			watchConfigFunc(configs)
		}
	}

	k8sCache.SetConfigMapUpdateFunc(watchConfigFunc)

	// TODO: General Architecture Note: Several passes have been made to modularize a lot of
	// TODO: our code, but the router still continues to be the obvious entry point for new \
	// TODO: features. We should look to spliting out the actual "router" functionality and
	// TODO: implement a builder -> controller for stitching new features and other dependencies.
	clusterManager := newClusterManager()

	cpuGv := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "node_cpu_hourly_cost",
		Help: "node_cpu_hourly_cost hourly cost for each cpu on this node",
	}, []string{"instance", "node", "instance_type", "region", "provider_id"})

	ramGv := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "node_ram_hourly_cost",
		Help: "node_ram_hourly_cost hourly cost for each gb of ram on this node",
	}, []string{"instance", "node", "instance_type", "region", "provider_id"})

	gpuGv := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "node_gpu_hourly_cost",
		Help: "node_gpu_hourly_cost hourly cost for each gpu on this node",
	}, []string{"instance", "node", "instance_type", "region", "provider_id"})

	totalGv := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "node_total_hourly_cost",
		Help: "node_total_hourly_cost Total node cost per hour",
	}, []string{"instance", "node", "instance_type", "region", "provider_id"})

	spotGv := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kubecost_node_is_spot",
		Help: "kubecost_node_is_spot Cloud provider info about node preemptibility",
	}, []string{"instance", "node", "instance_type", "region", "provider_id"})

	pvGv := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pv_hourly_cost",
		Help: "pv_hourly_cost Cost per GB per hour on a persistent disk",
	}, []string{"volumename", "persistentvolume"})

	RAMAllocation := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "container_memory_allocation_bytes",
		Help: "container_memory_allocation_bytes Bytes of RAM used",
	}, []string{"namespace", "pod", "container", "instance", "node"})

	CPUAllocation := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "container_cpu_allocation",
		Help: "container_cpu_allocation Percent of a single CPU used in a minute",
	}, []string{"namespace", "pod", "container", "instance", "node"})

	GPUAllocation := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "container_gpu_allocation",
		Help: "container_gpu_allocation GPU used",
	}, []string{"namespace", "pod", "container", "instance", "node"})
	PVAllocation := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pod_pvc_allocation",
		Help: "pod_pvc_allocation Bytes used by a PVC attached to a pod",
	}, []string{"namespace", "pod", "persistentvolumeclaim", "persistentvolume"})

	NetworkZoneEgressRecorder := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kubecost_network_zone_egress_cost",
		Help: "kubecost_network_zone_egress_cost Total cost per GB egress across zones",
	})
	NetworkRegionEgressRecorder := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kubecost_network_region_egress_cost",
		Help: "kubecost_network_region_egress_cost Total cost per GB egress across regions",
	})
	NetworkInternetEgressRecorder := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kubecost_network_internet_egress_cost",
		Help: "kubecost_network_internet_egress_cost Total cost per GB of internet egress.",
	})
	ClusterManagementCostRecorder := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kubecost_cluster_management_cost",
		Help: "kubecost_cluster_management_cost Hourly cost paid as a cluster management fee.",
	}, []string{"provisioner_name"})

	prometheus.MustRegister(cpuGv)
	prometheus.MustRegister(ramGv)
	prometheus.MustRegister(gpuGv)
	prometheus.MustRegister(totalGv)
	prometheus.MustRegister(pvGv)
	prometheus.MustRegister(spotGv)
	prometheus.MustRegister(RAMAllocation)
	prometheus.MustRegister(CPUAllocation)
	prometheus.MustRegister(PVAllocation)
	prometheus.MustRegister(GPUAllocation)
	prometheus.MustRegister(NetworkZoneEgressRecorder, NetworkRegionEgressRecorder, NetworkInternetEgressRecorder)
	prometheus.MustRegister(ClusterManagementCostRecorder)
	prometheus.MustRegister(ServiceCollector{
		KubeClientSet: kubeClientset,
	})
	prometheus.MustRegister(DeploymentCollector{
		KubeClientSet: kubeClientset,
	})
	prometheus.MustRegister(StatefulsetCollector{
		KubeClientSet: kubeClientset,
	})
	prometheus.MustRegister(ClusterInfoCollector{
		KubeClientSet: kubeClientset,
		Cloud:         cloudProvider,
	})

	// cache responses from model for a default of 5 minutes; clear expired responses every 10 minutes
	outOfClusterCache := cache.New(time.Minute*5, time.Minute*10)

	A = Accesses{
		PrometheusClient:              promCli,
		KubeClientSet:                 kubeClientset,
		ClusterManager:                clusterManager,
		Cloud:                         cloudProvider,
		CPUPriceRecorder:              cpuGv,
		RAMPriceRecorder:              ramGv,
		GPUPriceRecorder:              gpuGv,
		NodeTotalPriceRecorder:        totalGv,
		NodeSpotRecorder:              spotGv,
		RAMAllocationRecorder:         RAMAllocation,
		CPUAllocationRecorder:         CPUAllocation,
		GPUAllocationRecorder:         GPUAllocation,
		PVAllocationRecorder:          PVAllocation,
		NetworkZoneEgressRecorder:     NetworkZoneEgressRecorder,
		NetworkRegionEgressRecorder:   NetworkRegionEgressRecorder,
		NetworkInternetEgressRecorder: NetworkInternetEgressRecorder,
		PersistentVolumePriceRecorder: pvGv,
		ClusterManagementCostRecorder: ClusterManagementCostRecorder,
		Model:                         NewCostModel(k8sCache),
		OutOfClusterCache:             outOfClusterCache,
	}

	remoteEnabled := env.IsRemoteEnabled()
	if remoteEnabled {
		info, err := cloudProvider.ClusterInfo()
		klog.Infof("Saving cluster  with id:'%s', and name:'%s' to durable storage", info["id"], info["name"])
		if err != nil {
			klog.Infof("Error saving cluster id %s", err.Error())
		}
		_, _, err = costAnalyzerCloud.GetOrCreateClusterMeta(info["id"], info["name"])
		if err != nil {
			klog.Infof("Unable to set cluster id '%s' for cluster '%s', %s", info["id"], info["name"], err.Error())
		}
	}

	// Thanos Client
	if thanos.IsEnabled() {
		thanosUrl := thanos.QueryURL()

		if thanosUrl != "" {
			var thanosRT http.RoundTripper = &http.Transport{
				Proxy: http.ProxyFromEnvironment,
				DialContext: (&net.Dialer{
					Timeout:   120 * time.Second,
					KeepAlive: 120 * time.Second,
				}).DialContext,
				TLSHandshakeTimeout: 10 * time.Second,
				TLSClientConfig:     tlsConfig,
			}

			thanosConfig := prometheusClient.Config{
				Address:      thanosUrl,
				RoundTripper: thanosRT,
			}

			thanosCli, _ := prom.NewRateLimitedClient(thanosConfig, queryConcurrency, multiclusterDBBasicAuthUsername, multiclusterDBBasicAuthPW, multiClusterBearerToken, env.GetQueryLoggingFile())

			_, err = ValidatePrometheus(thanosCli, true)
			if err != nil {
				klog.V(1).Infof("[Warning] Failed to query Thanos at %s. Error: %s.", thanosUrl, err.Error())
				A.ThanosClient = thanosCli
			} else {
				klog.V(1).Info("Success: retrieved the 'up' query against Thanos at: " + thanosUrl)

				A.ThanosClient = thanosCli
			}

		} else {
			klog.Infof("Error resolving environment variable: $%s", env.ThanosQueryUrlEnvVar)
		}
	}

	err = A.Cloud.DownloadPricingData()
	if err != nil {
		klog.V(1).Info("Failed to download pricing data: " + err.Error())
	}

	StartCostModelMetricRecording(&A)

	managerEndpoints := cm.NewClusterManagerEndpoints(A.ClusterManager)

	Router.GET("/costDataModel", A.CostDataModel)
	Router.GET("/costDataModelRange", A.CostDataModelRange)
	Router.GET("/costDataModelRangeLarge", A.CostDataModelRangeLarge)
	Router.GET("/outOfClusterCosts", A.OutOfClusterCostsWithCache)
	Router.GET("/allNodePricing", A.GetAllNodePricing)
	Router.POST("/refreshPricing", A.RefreshPricingData)
	Router.GET("/clusterCostsOverTime", A.ClusterCostsOverTime)
	Router.GET("/clusterCosts", A.ClusterCosts)
	Router.GET("/validatePrometheus", A.GetPrometheusMetadata)
	Router.GET("/managementPlatform", A.ManagementPlatform)
	Router.GET("/clusterInfo", A.ClusterInfo)
	Router.GET("/clusters", managerEndpoints.GetAllClusters)
	Router.GET("/serviceAccountStatus", A.GetServiceAccountStatus)
	Router.PUT("/clusters", managerEndpoints.PutCluster)
	Router.DELETE("/clusters/:id", managerEndpoints.DeleteCluster)
}
