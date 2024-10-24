package prom

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/opencost/opencost/core/pkg/collections"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util/fileutil"
	"github.com/opencost/opencost/core/pkg/util/httputil"
	"github.com/opencost/opencost/core/pkg/version"
	"github.com/opencost/opencost/pkg/env"

	golog "log"

	prometheus "github.com/prometheus/client_golang/api"
	restclient "k8s.io/client-go/rest"
	certutil "k8s.io/client-go/util/cert"
)

var UserAgent = fmt.Sprintf("Opencost/%s", version.Version)

//--------------------------------------------------------------------------
//  QueryParamsDecorator
//--------------------------------------------------------------------------

// QueryParamsDecorator is used to decorate and return query parameters for
// outgoing requests
type QueryParamsDecorator = func(path string, values url.Values) url.Values

//--------------------------------------------------------------------------
//  ClientAuth
//--------------------------------------------------------------------------

// ClientAuth is used to authenticate outgoing client requests.
type ClientAuth struct {
	Username    string
	Password    string
	BearerToken string
}

// Apply Applies the authentication data to the request headers
func (auth *ClientAuth) Apply(req *http.Request) {
	if auth == nil {
		return
	}

	if auth.Username != "" {
		req.SetBasicAuth(auth.Username, auth.Password)
	}

	if auth.BearerToken != "" {
		token := "Bearer " + auth.BearerToken
		req.Header.Add("Authorization", token)
	}
}

//--------------------------------------------------------------------------
//  Rate Limit Options
//--------------------------------------------------------------------------

// MaxRetryAfterDuration is the maximum amount of time we should ever wait
// during a retry. This is to prevent starvation on the request threads
const MaxRetryAfterDuration = 10 * time.Second

// Default header key for Mimir/Cortex-Tenant API requests
const HeaderXScopeOrgId = "X-Scope-OrgID"

// RateLimitRetryOpts contains retry options
type RateLimitRetryOpts struct {
	MaxRetries       int
	DefaultRetryWait time.Duration
}

// RateLimitResponseStatus contains the status of the rate limited retries
type RateLimitResponseStatus struct {
	RetriesRemaining int
	WaitTime         time.Duration
}

// String creates a string representation of the rate limit status
func (rtrs *RateLimitResponseStatus) String() string {
	return fmt.Sprintf("Wait Time: %.2f seconds, Retries Remaining: %d", rtrs.WaitTime.Seconds(), rtrs.RetriesRemaining)
}

// RateLimitedError contains a list of retry statuses that occurred during
// retries on a rate limited response
type RateLimitedResponseError struct {
	RateLimitStatus []*RateLimitResponseStatus
}

// Error returns a string representation of the error, including the rate limit
// status reports
func (rlre *RateLimitedResponseError) Error() string {
	var sb strings.Builder

	sb.WriteString("Request was Rate Limited and Retries Exhausted:\n")

	for _, rls := range rlre.RateLimitStatus {
		sb.WriteString(" * ")
		sb.WriteString(rls.String())
		sb.WriteString("\n")
	}

	return sb.String()
}

//--------------------------------------------------------------------------
//  RateLimitedPrometheusClient
//--------------------------------------------------------------------------

// RateLimitedPrometheusClient is a prometheus client which limits the total number of
// concurrent outbound requests allowed at a given moment.
type RateLimitedPrometheusClient struct {
	id                string
	client            prometheus.Client
	auth              *ClientAuth
	queue             collections.BlockingQueue[*workRequest]
	decorator         QueryParamsDecorator
	rateLimitRetry    *RateLimitRetryOpts
	outbound          atomic.Int32
	fileLogger        *golog.Logger
	headerXScopeOrgId string
}

// requestCounter is used to determine if the prometheus client keeps track of
// the concurrent outbound requests
type requestCounter interface {
	TotalQueuedRequests() int
	TotalOutboundRequests() int
}

// NewRateLimitedClient creates a prometheus client which limits the number of concurrent outbound
// prometheus requests.
func NewRateLimitedClient(
	id string,
	client prometheus.Client,
	maxConcurrency int,
	auth *ClientAuth,
	decorator QueryParamsDecorator,
	rateLimitRetryOpts *RateLimitRetryOpts,
	queryLogFile string,
	headerXScopeOrgId string) (prometheus.Client, error) {

	queue := collections.NewBlockingQueue[*workRequest]()

	var logger *golog.Logger
	if queryLogFile != "" {
		exists, err := fileutil.FileExists(queryLogFile)
		if err != nil {
			log.Infof("Failed to check for existence of queryLogFile: %s: %s", queryLogFile, err)
		}
		if exists {
			err = os.Remove(queryLogFile)
			if err != nil {
				log.Infof("Failed to remove queryLogFile: %s: %s", queryLogFile, err)
			}
		}

		f, err := os.OpenFile(queryLogFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Infof("Failed to open queryLogFile: %s for query logging: %s", queryLogFile, err)
		} else {
			logger = golog.New(f, "query-log", golog.LstdFlags)
		}
	}

	// default authentication
	if auth == nil {
		auth = &ClientAuth{
			Username:    "",
			Password:    "",
			BearerToken: "",
		}
	}

	rlpc := &RateLimitedPrometheusClient{
		id:                id,
		client:            client,
		queue:             queue,
		decorator:         decorator,
		rateLimitRetry:    rateLimitRetryOpts,
		auth:              auth,
		fileLogger:        logger,
		headerXScopeOrgId: headerXScopeOrgId,
	}

	// Start concurrent request processing
	for i := 0; i < maxConcurrency; i++ {
		go rlpc.worker()
	}

	return rlpc, nil
}

// ID is used to identify the type of client
func (rlpc *RateLimitedPrometheusClient) ID() string {
	return rlpc.id
}

// TotalRequests returns the total number of requests that are either waiting to be sent and/or
// are currently outbound.
func (rlpc *RateLimitedPrometheusClient) TotalQueuedRequests() int {
	return rlpc.queue.Length()
}

// TotalOutboundRequests returns the total number of concurrent outbound requests, which have been
// sent to the server and are awaiting response.
func (rlpc *RateLimitedPrometheusClient) TotalOutboundRequests() int {
	return int(rlpc.outbound.Load())
}

// Passthrough to the prometheus client API
func (rlpc *RateLimitedPrometheusClient) URL(ep string, args map[string]string) *url.URL {
	return rlpc.client.URL(ep, args)
}

// workRequest is used to queue requests
type workRequest struct {
	ctx      context.Context
	req      *http.Request
	start    time.Time
	respChan chan *workResponse
	// used as a sentinel value to close the worker goroutine
	closer bool
	// request metadata for diagnostics
	contextName string
	query       string
}

// workResponse is the response payload returned to the Do method
type workResponse struct {
	res  *http.Response
	body []byte
	err  error
}

// worker is used as a consumer goroutine to pull workRequest from the blocking queue and execute them
func (rlpc *RateLimitedPrometheusClient) worker() {
	retryOpts := rlpc.rateLimitRetry
	retryRateLimit := retryOpts != nil

	for {
		// blocks until there is an item available
		we := rlpc.queue.Dequeue()

		// if we need to shut down all workers, we'll need to submit sentinel values
		// that will force the worker to return
		if we.closer {
			return
		}

		ctx := we.ctx
		req := we.req

		// decorate the raw query parameters
		if rlpc.decorator != nil {
			req.URL.RawQuery = rlpc.decorator(req.URL.Path, req.URL.Query()).Encode()
		}

		// measure time in queue
		timeInQueue := time.Since(we.start)

		// Increment outbound counter
		rlpc.outbound.Add(1)

		// Execute Request
		roundTripStart := time.Now()
		res, body, err := rlpc.client.Do(ctx, req)

		// If retries on rate limited response is enabled:
		// * Check for a 429 StatusCode OR 400 StatusCode and message containing "ThrottlingException"
		// * Attempt to parse a Retry-After from response headers (common on 429)
		// * If we couldn't determine how long to wait for a retry, use 1 second by default
		if res != nil && retryRateLimit {
			var status []*RateLimitResponseStatus
			var retries int = retryOpts.MaxRetries
			var defaultWait time.Duration = retryOpts.DefaultRetryWait

			for httputil.IsRateLimited(res, body) && retries > 0 {
				// calculate amount of time to wait before retry, in the event the default wait is used,
				// an exponential backoff is applied based on the number of times we've retried.
				retryAfter := httputil.RateLimitedRetryFor(res, defaultWait, retryOpts.MaxRetries-retries)
				retries--

				status = append(status, &RateLimitResponseStatus{RetriesRemaining: retries, WaitTime: retryAfter})
				log.DedupedInfof(50, "Rate Limited Prometheus Request. Waiting for: %d ms. Retries Remaining: %d", retryAfter.Milliseconds(), retries)

				// To prevent total starvation of request threads, hard limit wait time to 10s. We also want quota limits/throttles
				// to eventually pass through as an error. For example, if some quota is reached with 10 days left, we clearly
				// don't want to block for 10 days.
				if retryAfter > MaxRetryAfterDuration {
					retryAfter = MaxRetryAfterDuration
				}

				// execute wait and retry
				time.Sleep(retryAfter)
				res, body, err = rlpc.client.Do(ctx, req)
			}

			// if we've broken out of our retry loop and the resp is still rate limited,
			// then let's generate a meaningful error to pass back
			if retries == 0 && httputil.IsRateLimited(res, body) {
				err = &RateLimitedResponseError{RateLimitStatus: status}
			}
		}

		// Decrement outbound counter
		rlpc.outbound.Add(-1)
		LogQueryRequest(rlpc.fileLogger, req, timeInQueue, time.Since(roundTripStart))

		// Pass back response data over channel to caller
		we.respChan <- &workResponse{
			res:  res,
			body: body,
			err:  err,
		}
	}
}

// Rate limit and passthrough to prometheus client API
func (rlpc *RateLimitedPrometheusClient) Do(ctx context.Context, req *http.Request) (*http.Response, []byte, error) {
	if rlpc.headerXScopeOrgId != "" {
		req.Header.Set(HeaderXScopeOrgId, rlpc.headerXScopeOrgId)
	}

	rlpc.auth.Apply(req)

	respChan := make(chan *workResponse)
	defer close(respChan)

	// request names are used as a debug utility to identify requests in queue
	contextName := "<none>"
	if n, ok := httputil.GetName(req); ok {
		contextName = n
	}
	query, _ := httputil.GetQuery(req)

	rlpc.queue.Enqueue(&workRequest{
		ctx:         ctx,
		req:         req,
		start:       time.Now(),
		respChan:    respChan,
		closer:      false,
		contextName: contextName,
		query:       query,
	})

	workRes := <-respChan
	return workRes.res, workRes.body, workRes.err
}

//--------------------------------------------------------------------------
//  Client Helpers
//--------------------------------------------------------------------------

// PrometheusClientConfig contains all configurable options for creating a new prometheus client
type PrometheusClientConfig struct {
	Timeout               time.Duration
	KeepAlive             time.Duration
	TLSHandshakeTimeout   time.Duration
	TLSInsecureSkipVerify bool
	RateLimitRetryOpts    *RateLimitRetryOpts
	Auth                  *ClientAuth
	QueryConcurrency      int
	QueryLogFile          string
	HeaderXScopeOrgId     string
}

// NewPrometheusClient creates a new rate limited client which limits by outbound concurrent requests.
func NewPrometheusClient(address string, config *PrometheusClientConfig) (prometheus.Client, error) {

	var tlsCaCert *x509.CertPool
	// We will use the service account token and service-ca.crt to authenticate with the Prometheus server via kube-rbac-proxy.
	// We need to ensure that the service account has the necessary permissions to access the Prometheus server by binding it to the appropriate role.
	if env.IsKubeRbacProxyEnabled() {
		restConfig, err := restclient.InClusterConfig()
		if err != nil {
			log.Errorf("KUBE_RBAC_PROXY_ENABLED was set to true but failed to get in-cluster config: %s", err)
		}
		config.Auth.BearerToken = restConfig.BearerToken
		tlsCaCert, err = certutil.NewPool(`/var/run/secrets/kubernetes.io/serviceaccount/service-ca.crt`)
		if err != nil {
			log.Errorf("KUBE_RBAC_PROXY_ENABLED was set to true but failed to load service-ca.crt: %s", err)
		}
	}

	// may be necessary for long prometheus queries
	rt := httputil.NewUserAgentTransport(UserAgent, &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   config.Timeout,
			KeepAlive: config.KeepAlive,
		}).DialContext,
		TLSHandshakeTimeout: config.TLSHandshakeTimeout,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: config.TLSInsecureSkipVerify,
			RootCAs:            tlsCaCert,
		},
	})
	pc := prometheus.Config{
		Address:      address,
		RoundTripper: rt,
	}

	client, err := prometheus.NewClient(pc)
	if err != nil {
		return nil, err
	}

	return NewRateLimitedClient(
		PrometheusClientID,
		client,
		config.QueryConcurrency,
		config.Auth,
		nil,
		config.RateLimitRetryOpts,
		config.QueryLogFile,
		config.HeaderXScopeOrgId,
	)
}

// LogQueryRequest logs the query that was send to prom/thanos with the time in queue and total time after being sent
func LogQueryRequest(l *golog.Logger, req *http.Request, queueTime time.Duration, sendTime time.Duration) {
	if l == nil {
		return
	}
	qp := httputil.NewQueryParams(req.URL.Query())
	query := qp.Get("query", "<Unknown>")

	l.Printf("[Queue: %fs, Outbound: %fs][Query: %s]\n", queueTime.Seconds(), sendTime.Seconds(), query)
}
