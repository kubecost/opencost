package prom

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util/httputil"
	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/pkg/env"
	"github.com/opencost/opencost/pkg/errors"
	prometheus "github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
)

const (
	apiPrefix    = "/api/v1"
	epQuery      = apiPrefix + "/query"
	epQueryRange = apiPrefix + "/query_range"
)

// prometheus query offset to apply to each non-range query
// package scope to prevent calling duration parse each use
var promQueryOffset time.Duration = env.GetPrometheusQueryOffset()

// Context wraps a Prometheus client and provides methods for querying and
// parsing query responses and errors.
type Context struct {
	Client         prometheus.Client
	name           string
	errorCollector *QueryErrorCollector
}

// NewContext creates a new Prometheus querying context from the given client
func NewContext(client prometheus.Client) *Context {
	var ec QueryErrorCollector

	return &Context{
		Client:         client,
		name:           "",
		errorCollector: &ec,
	}
}

// NewNamedContext creates a new named Prometheus querying context from the given client
func NewNamedContext(client prometheus.Client, name string) *Context {
	ctx := NewContext(client)
	ctx.name = name
	return ctx
}

// Warnings returns the warnings collected from the Context's ErrorCollector
func (ctx *Context) Warnings() []*QueryWarning {
	return ctx.errorCollector.Warnings()
}

// HasWarnings returns true if the ErrorCollector has warnings.
func (ctx *Context) HasWarnings() bool {
	return ctx.errorCollector.IsWarning()
}

// Errors returns the errors collected from the Context's ErrorCollector.
func (ctx *Context) Errors() []*QueryError {
	return ctx.errorCollector.Errors()
}

// HasErrors returns true if the ErrorCollector has errors
func (ctx *Context) HasErrors() bool {
	return ctx.errorCollector.IsError()
}

// ErrorCollection returns the aggregation of errors if there exists errors. Otherwise,
// nil is returned
func (ctx *Context) ErrorCollection() error {
	if ctx.errorCollector.IsError() {
		// errorCollector implements the error interface
		return ctx.errorCollector
	}

	return nil
}

// Query returns a QueryResultsChan, then runs the given query and sends the
// results on the provided channel. Receiver is responsible for closing the
// channel, preferably using the Read method.
func (ctx *Context) Query(query string) QueryResultsChan {
	resCh := make(QueryResultsChan)

	go runQuery(query, ctx, resCh, time.Now(), "")

	return resCh
}

// QueryAtTime returns a QueryResultsChan, then runs the given query at the
// given time (see time parameter here: https://prometheus.io/docs/prometheus/latest/querying/api/#instant-queries)
// and sends the results on the provided channel. Receiver is responsible for
// closing the channel, preferably using the Read method.
func (ctx *Context) QueryAtTime(query string, t time.Time) QueryResultsChan {
	resCh := make(QueryResultsChan)

	go runQuery(query, ctx, resCh, t, "")

	return resCh
}

// ProfileQuery returns a QueryResultsChan, then runs the given query with a profile
// label and sends the results on the provided channel. Receiver is responsible for closing the
// channel, preferably using the Read method.
func (ctx *Context) ProfileQuery(query string, profileLabel string) QueryResultsChan {
	resCh := make(QueryResultsChan)

	go runQuery(query, ctx, resCh, time.Now(), profileLabel)

	return resCh
}

// QueryAll returns one QueryResultsChan for each query provided, then runs
// each query concurrently and returns results on each channel, respectively,
// in the order they were provided; i.e. the response to queries[1] will be
// sent on channel resChs[1].
func (ctx *Context) QueryAll(queries ...string) []QueryResultsChan {
	resChs := []QueryResultsChan{}

	for _, q := range queries {
		resChs = append(resChs, ctx.Query(q))
	}

	return resChs
}

// ProfileQueryAll returns one QueryResultsChan for each query provided, then runs
// each ProfileQuery concurrently and returns results on each channel, respectively,
// in the order they were provided; i.e. the response to queries[1] will be
// sent on channel resChs[1].
func (ctx *Context) ProfileQueryAll(queries ...string) []QueryResultsChan {
	resChs := []QueryResultsChan{}

	for _, q := range queries {
		resChs = append(resChs, ctx.ProfileQuery(q, fmt.Sprintf("Query #%d", len(resChs)+1)))
	}

	return resChs
}

func (ctx *Context) QuerySync(query string) ([]*QueryResult, v1.Warnings, error) {
	raw, warnings, err := ctx.query(query, time.Now())
	if err != nil {
		return nil, warnings, err
	}

	results := NewQueryResults(query, raw)
	if results.Error != nil {
		return nil, warnings, results.Error
	}

	return results.Results, warnings, nil
}

// QueryURL returns the URL used to query Prometheus
func (ctx *Context) QueryURL() *url.URL {
	return ctx.Client.URL(epQuery, nil)
}

// runQuery executes the prometheus query asynchronously, collects results and
// errors, and passes them through the results channel.
func runQuery(query string, ctx *Context, resCh QueryResultsChan, t time.Time, profileLabel string) {
	defer errors.HandlePanic()
	startQuery := time.Now()

	raw, warnings, requestError := ctx.query(query, t)
	results := NewQueryResults(query, raw)

	// report all warnings, request, and parse errors (nils will be ignored)
	ctx.errorCollector.Report(query, warnings, requestError, results.Error)

	if profileLabel != "" {
		log.Profile(startQuery, profileLabel)
	}

	resCh <- results
}

// RawQuery is a direct query to the prometheus client and returns the body of the response
func (ctx *Context) RawQuery(query string, t time.Time) ([]byte, error) {
	u := ctx.Client.URL(epQuery, nil)
	q := u.Query()
	q.Set("query", query)

	if t.IsZero() {
		t = time.Now()
	}

	q.Set("time", strconv.FormatInt(t.Unix(), 10))

	u.RawQuery = q.Encode()

	req, err := http.NewRequest(http.MethodPost, u.String(), nil)
	if err != nil {
		return nil, err
	}

	// Set QueryContext name if non empty
	if ctx.name != "" {
		req = httputil.SetName(req, ctx.name)
	}
	req = httputil.SetQuery(req, query)

	// Note that the warnings return value from client.Do() is always nil using this
	// version of the prometheus client library. We parse the warnings out of the response
	// body after json decodidng completes.
	resp, body, err := ctx.Client.Do(context.Background(), req)
	if err != nil {
		if resp == nil {
			return nil, fmt.Errorf("query error: '%s' fetching query '%s'", err.Error(), query)
		}

		return nil, fmt.Errorf("query error %d: '%s' fetching query '%s'", resp.StatusCode, err.Error(), query)
	}

	// Unsuccessful Status Code, log body and status
	statusCode := resp.StatusCode
	statusText := http.StatusText(statusCode)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, CommErrorf("%d (%s) URL: '%s', Body: '%s' Query: '%s'", statusCode, statusText, req.URL, body, query)
	}

	return body, err
}

func (ctx *Context) query(query string, t time.Time) (interface{}, v1.Warnings, error) {
	body, err := ctx.RawQuery(query, t)
	if err != nil {
		return nil, nil, err
	}

	var toReturn interface{}
	err = json.Unmarshal(body, &toReturn)
	if err != nil {
		return nil, nil, fmt.Errorf("query '%s' caused unmarshal error: %s", query, err)
	}

	warnings := warningsFrom(toReturn)
	for _, w := range warnings {
		// NoStoreAPIWarning is a warning that we would consider an error. It returns partial data relating only to the
		// store apis which were reachable. In order to ensure integrity of data across all clusters, we'll need to identify
		// this warning and convert it to an error.
		if IsNoStoreAPIWarning(w) {
			return nil, warnings, CommErrorf("Error: %s, Body: %s, Query: %s", w, body, query)
		}

		log.Warnf("fetching query '%s': %s", query, w)
	}

	return toReturn, warnings, nil
}

// isRequestStepAligned will check if the start and end times are aligned with the step
func (ctx *Context) isRequestStepAligned(start, end time.Time, step time.Duration) bool {
	startInUnix := start.Unix()
	endInUnix := end.Unix()
	stepInSeconds := step.Milliseconds() / 1e3
	return startInUnix%stepInSeconds == 0 && endInUnix%stepInSeconds == 0
}

func (ctx *Context) QueryRange(query string, start, end time.Time, step time.Duration) QueryResultsChan {
	resCh := make(QueryResultsChan)

	if !ctx.isRequestStepAligned(start, end, step) {
		start, end = ctx.alignWindow(start, end, step)
	}

	go runQueryRange(query, start, end, step, ctx, resCh, "")

	return resCh
}

func (ctx *Context) ProfileQueryRange(query string, start, end time.Time, step time.Duration, profileLabel string) QueryResultsChan {
	resCh := make(QueryResultsChan)

	go runQueryRange(query, start, end, step, ctx, resCh, profileLabel)

	return resCh
}

func (ctx *Context) QueryRangeSync(query string, start, end time.Time, step time.Duration) ([]*QueryResult, v1.Warnings, error) {
	raw, warnings, err := ctx.queryRange(query, start, end, step)
	if err != nil {
		return nil, warnings, err
	}

	results := NewQueryResults(query, raw)
	if results.Error != nil {
		return nil, warnings, results.Error
	}

	return results.Results, warnings, nil
}

// QueryRangeURL returns the URL used to query_range Prometheus
func (ctx *Context) QueryRangeURL() *url.URL {
	return ctx.Client.URL(epQueryRange, nil)
}

// runQueryRange executes the prometheus queryRange asynchronously, collects results and
// errors, and passes them through the results channel.
func runQueryRange(query string, start, end time.Time, step time.Duration, ctx *Context, resCh QueryResultsChan, profileLabel string) {
	defer errors.HandlePanic()
	startQuery := time.Now()

	raw, warnings, requestError := ctx.queryRange(query, start, end, step)
	results := NewQueryResults(query, raw)

	// report all warnings, request, and parse errors (nils will be ignored)
	ctx.errorCollector.Report(query, warnings, requestError, results.Error)

	if profileLabel != "" {
		log.Profile(startQuery, profileLabel)
	}

	resCh <- results
}

// RawQuery is a direct query to the prometheus client and returns the body of the response
func (ctx *Context) RawQueryRange(query string, start, end time.Time, step time.Duration) ([]byte, error) {
	u := ctx.Client.URL(epQueryRange, nil)
	q := u.Query()
	q.Set("query", query)
	q.Set("start", start.Format(time.RFC3339Nano))
	q.Set("end", end.Format(time.RFC3339Nano))
	q.Set("step", strconv.FormatFloat(step.Seconds(), 'f', 3, 64))
	u.RawQuery = q.Encode()

	req, err := http.NewRequest(http.MethodPost, u.String(), nil)
	if err != nil {
		return nil, err
	}

	// Set QueryContext name if non empty
	if ctx.name != "" {
		req = httputil.SetName(req, ctx.name)
	}
	req = httputil.SetQuery(req, query)

	// Note that the warnings return value from client.Do() is always nil using this
	// version of the prometheus client library. We parse the warnings out of the response
	// body after json decodidng completes.
	resp, body, err := ctx.Client.Do(context.Background(), req)
	if err != nil {
		if resp == nil {
			return nil, fmt.Errorf("Error: %s, Body: %s Query: %s", err.Error(), body, query)
		}

		return nil, fmt.Errorf("%d (%s) Error: %s Body: %s Query: %s", resp.StatusCode, http.StatusText(resp.StatusCode), body, err.Error(), query)
	}

	// Unsuccessful Status Code, log body and status
	statusCode := resp.StatusCode
	statusText := http.StatusText(statusCode)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, CommErrorf("%d (%s) Body: %s Query: %s", statusCode, statusText, body, query)
	}

	return body, err
}

func (ctx *Context) queryRange(query string, start, end time.Time, step time.Duration) (interface{}, v1.Warnings, error) {
	body, err := ctx.RawQueryRange(query, start, end, step)

	if err != nil {
		return nil, nil, err
	}

	var toReturn interface{}
	err = json.Unmarshal(body, &toReturn)
	if err != nil {
		return nil, nil, fmt.Errorf("query '%s' caused unmarshal error: %s", query, err)
	}

	warnings := warningsFrom(toReturn)
	for _, w := range warnings {
		// NoStoreAPIWarning is a warning that we would consider an error. It returns partial data relating only to the
		// store apis which were reachable. In order to ensure integrity of data across all clusters, we'll need to identify
		// this warning and convert it to an error.
		if IsNoStoreAPIWarning(w) {
			return nil, warnings, CommErrorf("Error: %s, Body: %s, Query: %s", w, body, query)
		}

		log.Warnf("fetching query '%s': %s", query, w)
	}

	return toReturn, warnings, nil
}

// alignWindow will update the start and end times to be aligned with the step duration.
// Current implementation will always floor the start/end times
func (ctx *Context) alignWindow(start time.Time, end time.Time, step time.Duration) (time.Time, time.Time) {
	// Convert the step duration from Milliseconds to Seconds to match the Unix timestamp, which is in seconds
	stepInSeconds := step.Milliseconds() / 1e3
	alignedStart := (start.Unix() / stepInSeconds) * stepInSeconds
	alignedEnd := (end.Unix() / stepInSeconds) * stepInSeconds
	return time.Unix(alignedStart, 0).UTC(), time.Unix(alignedEnd, 0).UTC()
}

// Extracts the warnings from the resulting json if they exist (part of the prometheus response api).
func warningsFrom(result interface{}) v1.Warnings {
	var warnings v1.Warnings

	if resultMap, ok := result.(map[string]interface{}); ok {
		if warningProp, ok := resultMap["warnings"]; ok {
			if w, ok := warningProp.([]string); ok {
				warnings = w
			}
		}
	}

	return warnings
}
