package prom

import (
	"crypto/tls"
	"net"
	"net/http"
	"net/url"
	"strings"

	prometheus "github.com/prometheus/client_golang/api"
)

// MaxSourceResulution is the query parameter key used to designate the resolution
// to use when executing a query.
const MaxSourceResulution = "max_source_resolution"

// NewThanosClient creates a new `prometheus.Client` with the specific thanos configuration, with a
// thanos client identifier.
func NewThanosClient(address string, thanosConfig *OpenCostThanosConfig) (prometheus.Client, error) {
	config := thanosConfig.ClientConfig

	tc := prometheus.Config{
		Address: address,
		RoundTripper: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   config.Timeout,
				KeepAlive: config.KeepAlive,
			}).DialContext,
			TLSHandshakeTimeout: config.TLSHandshakeTimeout,
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: config.TLSInsecureSkipVerify,
			},
		},
	}

	client, err := prometheus.NewClient(tc)
	if err != nil {
		return nil, err
	}

	// max source resolution decorator
	maxSourceDecorator := func(path string, queryParams url.Values) url.Values {
		if strings.Contains(path, "query") {
			queryParams.Set(MaxSourceResulution, thanosConfig.MaxSourceResulution)
		}
		return queryParams
	}

	return NewRateLimitedClient(
		ThanosClientID,
		client,
		config.QueryConcurrency,
		config.Auth,
		maxSourceDecorator,
		config.RateLimitRetryOpts,
		config.QueryLogFile,
		"",
	)
}
