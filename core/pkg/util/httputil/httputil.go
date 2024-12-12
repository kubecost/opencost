package httputil

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/util/mapper"
)

//--------------------------------------------------------------------------
//  QueryParams
//--------------------------------------------------------------------------

// valuesPrimitiveMap implements mapper.PrimitiveMap so we can build extra
// functionality into the QueryParams interface.
type valuesPrimitiveMap struct {
	url.Values
}

func (values valuesPrimitiveMap) Has(key string) bool {
	return values.Values.Has(key)
}
func (values valuesPrimitiveMap) Get(key string) string {
	return values.Values.Get(key)
}
func (values valuesPrimitiveMap) Set(key, value string) error {
	values.Values.Set(key, value)
	return nil
}

// QueryParams provides basic map access to URL values as well as providing
// helpful additional functionality for validation.
type QueryParams interface {
	mapper.PrimitiveMap

	// InvalidKeys returns the set of param keys which are not present in the
	// possible valid set. It is a set subtraction: present - valid = invalid
	//
	// Example usage to catch a typo:
	// qp.InvalidKeys([]string{"window", "aggregate", "filterClusters"}) ->
	//   "filterClsuters"
	//
	// If qp contains no keys, then this should always return an empty slice/nil
	InvalidKeys(possibleValidKeys []string) (invalidKeys []string)
}

// queryParamsMap implements the QueryParams interface on top of
// valuesPrimitiveMap.
type queryParamsMap struct {
	values url.Values
	mapper.PrimitiveMap
}

// NewQueryParams creates a primitive map using the request query parameters
func NewQueryParams(values url.Values) QueryParams {
	vpm := valuesPrimitiveMap{values}

	return &queryParamsMap{
		values:       values,
		PrimitiveMap: mapper.NewMapper(vpm),
	}
}

// InvalidKeys performs a set difference: Params keys - possible valid keys.
//
// For now, dealing with cache busting parameters should be the handler's
// responsibility.
func (qpm *queryParamsMap) InvalidKeys(possibleValidKeys []string) []string {
	validMap := map[string]struct{}{}
	for _, validKey := range possibleValidKeys {
		validMap[validKey] = struct{}{}
	}

	var invalidKeys []string

	for key := range qpm.values {
		if _, ok := validMap[key]; !ok {
			invalidKeys = append(invalidKeys, key)
		}
	}

	return invalidKeys
}

//--------------------------------------------------------------------------
//  HTTP Context Utilities
//--------------------------------------------------------------------------

const (
	ContextWarning string = "Warning"
	ContextName    string = "Name"
	ContextQuery   string = "Query"
)

// GetWarning Extracts a warning message from the request context if it exists
func GetWarning(r *http.Request) (warning string, ok bool) {
	warning, ok = r.Context().Value(ContextWarning).(string)
	return
}

// SetWarning Sets the warning context on the provided request and returns a new instance of the request
// with the new context.
func SetWarning(r *http.Request, warning string) *http.Request {
	ctx := context.WithValue(r.Context(), ContextWarning, warning)
	return r.WithContext(ctx)
}

// GetName Extracts a name value from the request context if it exists
func GetName(r *http.Request) (name string, ok bool) {
	name, ok = r.Context().Value(ContextName).(string)
	return
}

// SetName Sets the name value on the provided request and returns a new instance of the request
// with the new context.
func SetName(r *http.Request, name string) *http.Request {
	ctx := context.WithValue(r.Context(), ContextName, name)
	return r.WithContext(ctx)
}

// GetQuery Extracts a query value from the request context if it exists
func GetQuery(r *http.Request) (name string, ok bool) {
	name, ok = r.Context().Value(ContextQuery).(string)
	return
}

// SetQuery Sets the query value on the provided request and returns a new instance of the request
// with the new context.
func SetQuery(r *http.Request, query string) *http.Request {
	ctx := context.WithValue(r.Context(), ContextQuery, query)
	return r.WithContext(ctx)
}

//--------------------------------------------------------------------------
//  Package Funcs
//--------------------------------------------------------------------------

// IsRateLimited accepts a response and body to determine if either indicate
// a rate limited return
func IsRateLimited(resp *http.Response, body []byte) bool {
	return IsRateLimitedResponse(resp) || IsRateLimitedBody(resp, body)
}

// RateLimitedRetryFor returns the parsed Retry-After header relative to the
// current time. If the Retry-After header does not exist, the defaultWait parameter
// is returned.
func RateLimitedRetryFor(resp *http.Response, defaultWait time.Duration, retry int) time.Duration {
	if resp.Header == nil {
		return ExponentialBackoffWaitFor(defaultWait, retry)
	}

	// Retry-After is either the number of seconds to wait or a target datetime (RFC1123)
	value := resp.Header.Get("Retry-After")
	if value == "" {
		return defaultWait
	}

	seconds, err := strconv.ParseInt(value, 10, 64)
	if err == nil {
		return time.Duration(seconds) * time.Second
	}

	// failed to parse an integer, try datetime RFC1123
	t, err := time.Parse(time.RFC1123, value)
	if err == nil {
		// return 0 if the datetime has already elapsed
		result := t.Sub(time.Now())
		if result < 0 {
			return 0
		}
		return result
	}

	// failed to parse datetime, return default
	return defaultWait
}

// ExpontentialBackoffWatiFor accepts a default wait duration and the current retry count
// and returns a new duration
func ExponentialBackoffWaitFor(defaultWait time.Duration, retry int) time.Duration {
	return time.Duration(math.Pow(2, float64(retry))*float64(defaultWait.Milliseconds())) * time.Millisecond
}

// IsRateLimitedResponse returns true if the status code is a 429 (TooManyRequests)
func IsRateLimitedResponse(resp *http.Response) bool {
	return resp.StatusCode == http.StatusTooManyRequests
}

// IsRateLimitedBody attempts to determine if a response body indicates throttling
// has occurred. This function is a result of some API providers (AWS) returning
// a 400 status code instead of 429 for rate limit exceptions.
func IsRateLimitedBody(resp *http.Response, body []byte) bool {
	// ignore non-400 status
	if resp.StatusCode < http.StatusBadRequest || resp.StatusCode >= http.StatusInternalServerError {
		return false
	}
	return strings.Contains(string(body), "ThrottlingException")
}

// HeaderString writes the request/response http.Header to a string.
func HeaderString(h http.Header) string {
	var sb strings.Builder
	var first bool = true
	sb.WriteString("{ ")

	for k, vs := range h {
		if first {
			first = false
		} else {
			sb.WriteString(", ")
		}
		fmt.Fprintf(&sb, "%s: [ ", k)
		for idx, v := range vs {
			sb.WriteString(v)
			if idx != len(vs)-1 {
				sb.WriteString(", ")
			}
		}
		sb.WriteString(" ]")
	}
	sb.WriteString(" }")

	return sb.String()
}
