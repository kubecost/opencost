package prom

import (
	"crypto/x509"
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
	"github.com/opencost/opencost/modules/prometheus-source/pkg/env"

	restclient "k8s.io/client-go/rest"
	certutil "k8s.io/client-go/util/cert"
)

const (
	ServiceCA = `/var/run/secrets/kubernetes.io/serviceaccount/service-ca.crt`
)

type OpenCostPrometheusConfig struct {
	ServerEndpoint        string
	ClientConfig          *PrometheusClientConfig
	ScrapeInterval        time.Duration
	JobName               string
	Offset                string
	QueryOffset           time.Duration
	MaxQueryDuration      time.Duration
	ClusterLabel          string
	ClusterID             string
	ClusterFilter         string
	DataResolution        time.Duration
	DataResolutionMinutes int
}

type OpenCostThanosConfig struct {
	*OpenCostPrometheusConfig

	MaxSourceResulution string
}

func (ocpc *OpenCostPrometheusConfig) IsRateLimitRetryEnabled() bool {
	return ocpc.ClientConfig.RateLimitRetryOpts != nil
}

// NewOpenCostPrometheusConfigFromEnv creates a new OpenCostPrometheusConfig from environment variables.
func NewOpenCostPrometheusConfigFromEnv() (*OpenCostPrometheusConfig, error) {
	serverEndpoint := env.GetPrometheusServerEndpoint()
	if serverEndpoint == "" {
		return nil, fmt.Errorf("no address for prometheus set in $%s", env.PrometheusServerEndpointEnvVar)
	}

	queryConcurrency := env.GetMaxQueryConcurrency()
	log.Infof("Prometheus Client Max Concurrency set to %d", queryConcurrency)

	timeout := env.GetPrometheusQueryTimeout()
	keepAlive := env.GetPrometheusKeepAlive()
	tlsHandshakeTimeout := env.GetPrometheusTLSHandshakeTimeout()

	jobName := env.GetJobName()
	scrapeInterval := env.GetScrapeInterval()

	maxQueryDuration := env.GetETLMaxPrometheusQueryDuration()

	clusterId := env.GetClusterID()
	clusterLabel := env.GetPromClusterLabel()
	clusterFilter := env.GetPromClusterFilter()

	var rateLimitRetryOpts *RateLimitRetryOpts = nil
	if env.IsPrometheusRetryOnRateLimitResponse() {
		rateLimitRetryOpts = &RateLimitRetryOpts{
			MaxRetries:       env.GetPrometheusRetryOnRateLimitMaxRetries(),
			DefaultRetryWait: env.GetPrometheusRetryOnRateLimitDefaultWait(),
		}
	}

	auth := &ClientAuth{
		Username:    env.GetDBBasicAuthUsername(),
		Password:    env.GetDBBasicAuthUserPassword(),
		BearerToken: env.GetDBBearerToken(),
	}

	// We will use the service account token and service-ca.crt to authenticate with the Prometheus server via kube-rbac-proxy.
	// We need to ensure that the service account has the necessary permissions to access the Prometheus server by binding it to the appropriate role.
	var tlsCaCert *x509.CertPool
	if env.IsKubeRbacProxyEnabled() {
		restConfig, err := restclient.InClusterConfig()
		if err != nil {
			log.Errorf("%s was set to true but failed to get in-cluster config: %s", env.KubeRbacProxyEnabledEnvVar, err)
		}
		auth.BearerToken = restConfig.BearerToken
		tlsCaCert, err = certutil.NewPool(ServiceCA)
		if err != nil {
			log.Errorf("%s was set to true but failed to load service-ca.crt: %s", env.KubeRbacProxyEnabledEnvVar, err)
		}
	}

	dataResolution := env.GetETLResolution()

	// Ensuring if data resolution is less than 60s default it to 1m
	resolutionMinutes := int(dataResolution.Minutes())
	if resolutionMinutes == 0 {
		resolutionMinutes = 1
	}

	clientConfig := &PrometheusClientConfig{
		Timeout:               timeout,
		KeepAlive:             keepAlive,
		TLSHandshakeTimeout:   tlsHandshakeTimeout,
		TLSInsecureSkipVerify: env.IsInsecureSkipVerify(),
		RootCAs:               tlsCaCert,
		RateLimitRetryOpts:    rateLimitRetryOpts,
		Auth:                  auth,
		QueryConcurrency:      queryConcurrency,
		QueryLogFile:          "",
		HeaderXScopeOrgId:     env.GetPrometheusHeaderXScopeOrgId(),
	}

	return &OpenCostPrometheusConfig{
		ServerEndpoint:        serverEndpoint,
		ClientConfig:          clientConfig,
		ScrapeInterval:        scrapeInterval,
		JobName:               jobName,
		Offset:                "",
		QueryOffset:           time.Duration(0),
		MaxQueryDuration:      maxQueryDuration,
		ClusterLabel:          clusterLabel,
		ClusterID:             clusterId,
		ClusterFilter:         clusterFilter,
		DataResolution:        dataResolution,
		DataResolutionMinutes: resolutionMinutes,
	}, nil
}

// NewOpenCostPrometheusConfigFromEnv creates a new OpenCostPrometheusConfig from environment variables.
func NewOpenCostThanosConfigFromEnv() (*OpenCostThanosConfig, error) {
	serverEndpoint := env.GetThanosQueryUrl()
	if serverEndpoint == "" {
		return nil, fmt.Errorf("no address for thanos set in $%s", env.ThanosQueryUrlEnvVar)
	}

	queryConcurrency := env.GetMaxQueryConcurrency()
	log.Infof("Thanos Client Max Concurrency set to %d", queryConcurrency)

	timeout := env.GetPrometheusQueryTimeout()
	keepAlive := env.GetPrometheusKeepAlive()
	tlsHandshakeTimeout := env.GetPrometheusTLSHandshakeTimeout()

	jobName := env.GetJobName()
	scrapeInterval := env.GetScrapeInterval()

	maxQueryDuration := env.GetETLMaxPrometheusQueryDuration()
	clusterLabel := env.GetPromClusterLabel()

	var rateLimitRetryOpts *RateLimitRetryOpts = nil
	if env.IsPrometheusRetryOnRateLimitResponse() {
		rateLimitRetryOpts = &RateLimitRetryOpts{
			MaxRetries:       env.GetPrometheusRetryOnRateLimitMaxRetries(),
			DefaultRetryWait: env.GetPrometheusRetryOnRateLimitDefaultWait(),
		}
	}

	auth := &ClientAuth{
		Username:    env.GetMultiClusterBasicAuthUsername(),
		Password:    env.GetMultiClusterBasicAuthPassword(),
		BearerToken: env.GetMultiClusterBearerToken(),
	}

	clientConfig := &PrometheusClientConfig{
		Timeout:               timeout,
		KeepAlive:             keepAlive,
		TLSHandshakeTimeout:   tlsHandshakeTimeout,
		TLSInsecureSkipVerify: env.IsInsecureSkipVerify(),
		RateLimitRetryOpts:    rateLimitRetryOpts,
		Auth:                  auth,
		QueryConcurrency:      queryConcurrency,
		QueryLogFile:          env.GetQueryLoggingFile(),
		HeaderXScopeOrgId:     "",
		RootCAs:               nil,
	}

	thanosQueryOffset := env.GetThanosOffset()
	d, err := timeutil.ParseDuration(thanosQueryOffset)
	if err != nil {
		return nil, fmt.Errorf("failed to parse thanos query offset: %w", err)
	}

	dataResolution := env.GetETLResolution()

	return &OpenCostThanosConfig{
		OpenCostPrometheusConfig: &OpenCostPrometheusConfig{
			ServerEndpoint:   serverEndpoint,
			ClientConfig:     clientConfig,
			ScrapeInterval:   scrapeInterval,
			JobName:          jobName,
			Offset:           thanosQueryOffset,
			QueryOffset:      d,
			MaxQueryDuration: maxQueryDuration,
			ClusterID:        "", // thanos is multi-cluster
			ClusterFilter:    "", // thanos is multi-cluster
			ClusterLabel:     clusterLabel,
			DataResolution:   dataResolution,
		},
		MaxSourceResulution: env.GetThanosMaxSourceResolution(),
	}, nil
}
