package agent

import (
	"context"
	"fmt"
	"net/http"
	"path"
	"time"

	"github.com/opencost/opencost/core/pkg/clusters"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/util/retry"
	"github.com/opencost/opencost/pkg/util/watcher"

	"github.com/opencost/opencost/core/pkg/version"
	"github.com/opencost/opencost/modules/prometheus-source/pkg/prom"
	"github.com/opencost/opencost/pkg/cloud/provider"
	"github.com/opencost/opencost/pkg/clustercache"
	"github.com/opencost/opencost/pkg/config"
	"github.com/opencost/opencost/pkg/costmodel"
	"github.com/opencost/opencost/pkg/env"
	"github.com/opencost/opencost/pkg/kubeconfig"
	"github.com/opencost/opencost/pkg/metrics"

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

func Execute(opts *AgentOpts) error {
	log.Infof("Starting Kubecost Agent version %s", version.FriendlyVersion())

	const maxRetries = 10
	const retryInterval = 10 * time.Second

	var fatalErr error

	ctx, cancel := context.WithCancel(context.Background())
	dataSource, err := retry.Retry(
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
	localClusterInfo := costmodel.NewLocalClusterInfoProvider(k8sClient, dataSource, cloudProvider)

	var clusterInfoProvider clusters.ClusterInfoProvider
	if env.IsExportClusterInfoEnabled() {
		clusterInfoConf := confManager.ConfigFileAt(path.Join(configPrefix, "cluster-info.json"))
		clusterInfoProvider = costmodel.NewClusterInfoWriteOnRequest(localClusterInfo, clusterInfoConf)
	} else {
		clusterInfoProvider = localClusterInfo
	}

	// Initialize ClusterMap for maintaining ClusterInfo by ClusterID
	clusterMap := dataSource.NewClusterMap(clusterInfoProvider)

	costModel := costmodel.NewCostModel(dataSource, cloudProvider, clusterCache, clusterMap, dataSource.BatchDuration())

	// initialize Kubernetes Metrics Emitter
	metricsEmitter := costmodel.NewCostModelMetricsEmitter(clusterCache, cloudProvider, clusterInfoProvider, costModel)

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
