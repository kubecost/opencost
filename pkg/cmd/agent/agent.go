package agent

import (
	"context"
	"fmt"
	"net/http"
	"path"
	"time"

	"github.com/opencost/opencost/core/pkg/clusters"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/pkg/util/watcher"

	"github.com/opencost/opencost/core/pkg/version"
	"github.com/opencost/opencost/pkg/cloud/provider"
	"github.com/opencost/opencost/pkg/clustercache"
	"github.com/opencost/opencost/pkg/config"
	"github.com/opencost/opencost/pkg/costmodel"
	clustermap "github.com/opencost/opencost/pkg/costmodel/clusters"
	"github.com/opencost/opencost/pkg/env"
	"github.com/opencost/opencost/pkg/kubeconfig"
	"github.com/opencost/opencost/pkg/metrics"
	"github.com/opencost/opencost/pkg/prom"

	prometheus "github.com/prometheus/client_golang/api"
	prometheusAPI "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/rs/cors"
	"k8s.io/client-go/kubernetes"
)

// AgentOpts contain configuration options that can be passed to the Execute() method
type AgentOpts struct {
	// Stubbed for future configuration
}

// ClusterExportInterval is the interval used to export the cluster if env.IsExportClusterCacheEnabled() is true
const ClusterExportInterval = 5 * time.Minute

// clusterExporter is used if env.IsExportClusterCacheEnabled() is set to true
// it will export the kubernetes cluster data to a file on a specific interval
var clusterExporter *clustercache.ClusterExporter

func Healthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(200)
	w.Header().Set("Content-Length", "0")
	w.Header().Set("Content-Type", "text/plain")
}

// initializes the kubernetes client cache
func newKubernetesClusterCache() (kubernetes.Interface, clustercache.ClusterCache, error) {
	var err error

	// Kubernetes API setup
	kubeClientset, err := kubeconfig.LoadKubeClient("")
	if err != nil {
		return nil, nil, err
	}

	// Create Kubernetes Cluster Cache + Watchers
	k8sCache := clustercache.NewKubernetesClusterCache(kubeClientset)
	k8sCache.Run()

	return kubeClientset, k8sCache, nil
}

func newPrometheusClient() (prometheus.Client, error) {
	address := env.GetPrometheusServerEndpoint()
	if address == "" {
		return nil, fmt.Errorf("No address for prometheus set in $%s. Aborting.", env.PrometheusServerEndpointEnvVar)
	}

	queryConcurrency := env.GetMaxQueryConcurrency()
	log.Infof("Prometheus Client Max Concurrency set to %d", queryConcurrency)

	timeout := 120 * time.Second
	keepAlive := 120 * time.Second
	tlsHandshakeTimeout := 10 * time.Second

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
		QueryConcurrency: queryConcurrency,
		QueryLogFile:     "",
	})
	if err != nil {
		return nil, fmt.Errorf("Failed to create prometheus client, Error: %v", err)
	}

	m, err := prom.Validate(promCli)
	if err != nil || !m.Running {
		if err != nil {
			log.Errorf("Failed to query prometheus at %s. Error: %s . Troubleshooting help available at: %s", address, err.Error(), prom.PrometheusTroubleshootingURL)
		} else if !m.Running {
			log.Errorf("Prometheus at %s is not running. Troubleshooting help available at: %s", address, prom.PrometheusTroubleshootingURL)
		}
	} else {
		log.Infof("Success: retrieved the 'up' query against prometheus at: %s", address)
	}

	api := prometheusAPI.NewAPI(promCli)
	_, err = api.Buildinfo(context.Background())
	if err != nil {
		log.Infof("No valid prometheus config file at %s. Error: %s . Troubleshooting help available at: %s. Ignore if using cortex/mimir/thanos here.", address, err.Error(), prom.PrometheusTroubleshootingURL)
	} else {
		log.Infof("Retrieved a prometheus config file from: %s", address)
	}

	return promCli, nil
}

func Execute(opts *AgentOpts) error {
	log.Infof("Starting Kubecost Agent version %s", version.FriendlyVersion())
	scrapeInterval := env.GetKubecostScrapeInterval()
	promCli, err := newPrometheusClient()
	if err != nil {
		panic(err.Error())
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

	// initialize kubernetes client and cluster cache
	k8sClient, clusterCache, err := newKubernetesClusterCache()
	if err != nil {
		panic(err.Error())
	}

	// Create ConfigFileManager for synchronization of shared configuration
	confManager := config.NewConfigFileManager(&config.ConfigFileManagerOpts{
		BucketStoreConfig: env.GetKubecostConfigBucket(),
		LocalConfigPath:   "/",
	})

	cloudProviderKey := env.GetCloudProviderAPIKey()
	cloudProvider, err := provider.NewProvider(clusterCache, cloudProviderKey, confManager)
	if err != nil {
		panic(err.Error())
	}

	// Append the pricing config watcher
	kubecostNamespace := env.GetKubecostNamespace()
	configWatchers := watcher.NewConfigMapWatchers(k8sClient, kubecostNamespace)
	configWatchers.AddWatcher(provider.ConfigWatcherFor(cloudProvider))
	configWatchers.Watch()

	configPrefix := env.GetConfigPathWithDefault(env.DefaultConfigMountPath)

	// Initialize cluster exporting if it's enabled
	if env.IsExportClusterCacheEnabled() {
		cacheLocation := confManager.ConfigFileAt(path.Join(configPrefix, "cluster-cache.json"))
		clusterExporter = clustercache.NewClusterExporter(clusterCache, cacheLocation, ClusterExportInterval)
		clusterExporter.Run()
	}

	// ClusterInfo Provider to provide the cluster map with local and remote cluster data
	localClusterInfo := costmodel.NewLocalClusterInfoProvider(k8sClient, cloudProvider)

	var clusterInfoProvider clusters.ClusterInfoProvider
	if env.IsExportClusterInfoEnabled() {
		clusterInfoConf := confManager.ConfigFileAt(path.Join(configPrefix, "cluster-info.json"))
		clusterInfoProvider = costmodel.NewClusterInfoWriteOnRequest(localClusterInfo, clusterInfoConf)
	} else {
		clusterInfoProvider = localClusterInfo
	}

	// Initialize ClusterMap for maintaining ClusterInfo by ClusterID
	clusterMap := clustermap.NewClusterMap(promCli, clusterInfoProvider, 5*time.Minute)

	costModel := costmodel.NewCostModel(promCli, cloudProvider, clusterCache, clusterMap, scrapeInterval)

	// initialize Kubernetes Metrics Emitter
	metricsEmitter := costmodel.NewCostModelMetricsEmitter(promCli, clusterCache, cloudProvider, clusterInfoProvider, costModel)

	// download pricing data
	err = cloudProvider.DownloadPricingData()
	if err != nil {
		log.Errorf("Error downloading pricing data: %s", err)
	}

	// start emitting metrics
	metricsEmitter.Start()

	rootMux := http.NewServeMux()
	rootMux.HandleFunc("/healthz", Healthz)
	rootMux.Handle("/metrics", promhttp.Handler())
	telemetryHandler := metrics.ResponseMetricMiddleware(rootMux)
	handler := cors.AllowAll().Handler(telemetryHandler)

	return http.ListenAndServe(fmt.Sprintf(":%d", env.GetKubecostMetricsPort()), handler)
}
