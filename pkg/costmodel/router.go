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

	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/util/retry"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
	"github.com/opencost/opencost/core/pkg/version"
	"github.com/opencost/opencost/pkg/cloud/aws"
	cloudconfig "github.com/opencost/opencost/pkg/cloud/config"
	"github.com/opencost/opencost/pkg/cloud/gcp"
	"github.com/opencost/opencost/pkg/cloud/provider"
	"github.com/opencost/opencost/pkg/cloudcost"
	"github.com/opencost/opencost/pkg/config"
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
	"github.com/opencost/opencost/modules/prometheus-source/pkg/prom"
	"github.com/opencost/opencost/pkg/cloud/azure"
	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/clustercache"
	"github.com/opencost/opencost/pkg/env"
	"github.com/opencost/opencost/pkg/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/patrickmn/go-cache"

	"k8s.io/client-go/kubernetes"
)

const (
	RFC3339Milli         = "2006-01-02T15:04:05.000Z"
	maxCacheMinutes1d    = 11
	maxCacheMinutes2d    = 17
	maxCacheMinutes7d    = 37
	maxCacheMinutes30d   = 137
	CustomPricingSetting = "CustomPricing"
	DiscountSetting      = "Discount"
)

var (
	// gitCommit is set by the build system
	gitCommit string
)

// Accesses defines a singleton application instance, providing access to
// Prometheus, Kubernetes, the cloud provider, and caches.
type Accesses struct {
	DataSource          source.OpenCostDataSource
	KubeClientSet       kubernetes.Interface
	ClusterCache        clustercache.ClusterCache
	ClusterMap          clusters.ClusterMap
	CloudProvider       models.Provider
	ConfigFileManager   *config.ConfigFileManager
	ClusterInfoProvider clusters.ClusterInfoProvider
	Model               *CostModel
	MetricsEmitter      *CostModelMetricsEmitter
	OutOfClusterCache   *cache.Cache
	CostDataCache       *cache.Cache
	ClusterCostsCache   *cache.Cache
	CacheExpiration     map[time.Duration]time.Duration
	// SettingsCache stores current state of app settings
	SettingsCache *cache.Cache
	// settingsSubscribers tracks channels through which changes to different
	// settings will be published in a pub/sub model
	settingsSubscribers map[string][]chan string
	settingsMutex       sync.Mutex
	// registered http service instances
	httpServices services.HTTPServices
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
	dataSource := a.DataSource

	key := fmt.Sprintf("%s:%s", durationHrs, fmtOffset)
	if data, valid := a.ClusterCostsCache.Get(key); valid {
		clusterCosts := data.(map[string]*ClusterCosts)
		w.Write(WrapDataWithMessage(clusterCosts, nil, "clusterCosts cache hit"))
	} else {
		data, err := a.ComputeClusterCosts(dataSource, a.CloudProvider, duration, offset, true)
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

	duration, err := timeutil.ParseDuration(window)
	if err != nil {
		w.Write(WrapData(nil, fmt.Errorf("error parsing window (%s): %s", window, err)))
		return
	}

	end := time.Now()
	if offset != "" {
		offsetDur, err := timeutil.ParseDuration(offset)
		if err != nil {
			w.Write(WrapData(nil, fmt.Errorf("error parsing offset (%s): %s", offset, err)))
			return
		}

		end = end.Add(-offsetDur)
	}

	start := end.Add(-duration)

	data, err := a.Model.ComputeCostData(start, end)

	// apply filter by removing if != namespace
	if namespace != "" {
		for key, costData := range data {
			if costData.Namespace != namespace {
				delete(data, key)
			}
		}
	}

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
	/*
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
	*/

	data, err := a.ComputeClusterCosts(a.DataSource, a.CloudProvider, windowDur, offsetDur, true)
	w.Write(WrapData(data, err))
}

func (a *Accesses) ClusterCostsOverTime(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	startString := r.URL.Query().Get("start")
	endString := r.URL.Query().Get("end")
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

	const layout = "2006-01-02T15:04:05.000Z"

	start, err := time.Parse(layout, startString)
	if err != nil {
		log.Errorf("Error parsing time %s. Error: %s", startString, err.Error())
		w.Write(WrapData(nil, fmt.Errorf("error parsing 'start': %s: %w", startString, err)))
		return
	}

	end, err := time.Parse(layout, endString)
	if err != nil {
		log.Errorf("Error parsing time %s. Error: %s", endString, err.Error())
		w.Write(WrapData(nil, fmt.Errorf("error parsing 'end': %s: %w", endString, err)))
		return
	}

	data, err := ClusterCostsOverTime(a.DataSource, a.CloudProvider, start, end, windowDur, offsetDur)
	w.Write(WrapData(data, err))
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
		fmt.Fprintf(w, "Error decoding pod: %s", err)
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
		fmt.Fprintf(w, "Error writing service key: %s", err)
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

// captures the panic event in sentry
func capturePanicEvent(err string, stack string) {
	msg := fmt.Sprintf("Panic: %s\nStackTrace: %s\n", err, stack)
	log.Infof("%s", msg)
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

	const maxRetries = 10
	const retryInterval = 10 * time.Second

	var fatalErr error

	ctx, cancel := context.WithCancel(context.Background())
	dataSource, _ := retry.Retry(
		ctx,
		func() (source.OpenCostDataSource, error) {
			ds, e := prom.NewDefaultPrometheusDataSource()
			if e != nil {
				if source.IsRetryable(e) {
					return nil, e
				}
				fatalErr = e
				cancel()
			}

			return ds, e
		},
		maxRetries,
		retryInterval,
	)

	if fatalErr != nil {
		log.Fatalf("Failed to create Prometheus data source: %s", fatalErr)
		panic(fatalErr)
	}

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

	// ClusterInfo Provider to provide the cluster map with local and remote cluster data
	var clusterInfoProvider clusters.ClusterInfoProvider
	if env.IsClusterInfoFileEnabled() {
		clusterInfoFile := confManager.ConfigFileAt(path.Join(configPrefix, "cluster-info.json"))
		clusterInfoProvider = NewConfiguredClusterInfoProvider(clusterInfoFile)
	} else {
		clusterInfoProvider = NewLocalClusterInfoProvider(kubeClientset, dataSource, cloudProvider)
	}

	clusterMap := dataSource.NewClusterMap(clusterInfoProvider)

	// cache responses from model and aggregation for a default of 10 minutes;
	// clear expired responses every 20 minutes
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

	costModel := NewCostModel(dataSource, cloudProvider, k8sCache, clusterMap, dataSource.BatchDuration())
	metricsEmitter := NewCostModelMetricsEmitter(k8sCache, cloudProvider, clusterInfoProvider, costModel)

	a := &Accesses{
		httpServices:        services.NewCostModelServices(),
		DataSource:          dataSource,
		KubeClientSet:       kubeClientset,
		ClusterCache:        k8sCache,
		ClusterMap:          clusterMap,
		CloudProvider:       cloudProvider,
		ConfigFileManager:   confManager,
		ClusterInfoProvider: clusterInfoProvider,
		Model:               costModel,
		MetricsEmitter:      metricsEmitter,
		CostDataCache:       costDataCache,
		ClusterCostsCache:   clusterCostsCache,
		OutOfClusterCache:   outOfClusterCache,
		SettingsCache:       settingsCache,
		CacheExpiration:     cacheExpiration,
	}

	// Initialize mechanism for subscribing to settings changes
	a.InitializeSettingsPubSub()
	err = a.CloudProvider.DownloadPricingData()
	if err != nil {
		log.Infof("Failed to download pricing data: %s", err)
	}

	// NOTE: (bolt) this only warms the cache for cluster costs.
	if env.IsCacheWarmingEnabled() {
		log.Infof("Init: ClusterCosts cache warming enabled")
		a.warmAggregateCostModelCache()
	} else {
		log.Infof("Init: ClusterCosts cache warming disabled")
	}

	// router.GET("/aggregatedCostModel", a.AggregateCostModelHandler)

	if !env.IsKubecostMetricsPodEnabled() {
		a.MetricsEmitter.Start()
	}

	a.httpServices.RegisterAll(router)
	a.DataSource.RegisterEndPoints(router)

	router.GET("/costDataModel", a.CostDataModel)
	router.GET("/allocation/compute", a.ComputeAllocationHandler)
	router.GET("/allocation/compute/summary", a.ComputeAllocationHandlerSummary)

	router.GET("/allNodePricing", a.GetAllNodePricing)
	router.POST("/refreshPricing", a.RefreshPricingData)
	router.GET("/clusterCostsOverTime", a.ClusterCostsOverTime)
	router.GET("/clusterCosts", a.ClusterCosts)
	router.GET("/clusterCostsFromCache", a.ClusterCostsFromCacheHandler)
	router.GET("/managementPlatform", a.ManagementPlatform)
	router.GET("/clusterInfo", a.ClusterInfo)
	router.GET("/clusterInfoMap", a.GetClusterInfoMap)
	router.GET("/serviceAccountStatus", a.GetServiceAccountStatus)
	router.GET("/pricingSourceStatus", a.GetPricingSourceStatus)
	router.GET("/pricingSourceSummary", a.GetPricingSourceSummary)
	router.GET("/pricingSourceCounts", a.GetPricingSourceCounts)
	router.GET("/orphanedPods", a.GetOrphanedPods)
	router.GET("/installNamespace", a.GetInstallNamespace)
	router.GET("/installInfo", a.GetInstallInfo)
	router.POST("/serviceKey", a.AddServiceKey)
	router.GET("/helmValues", a.GetHelmValues)

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
