package prom

import (
	"bytes"
	"context"
	"io"
	"math"
	"net/http"

	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/util"
	"github.com/opencost/opencost/core/pkg/util/httputil"
	prometheus "github.com/prometheus/client_golang/api"
)

// ResponseAndBody is just a test objet used to hold predefined responses
// and response bodies
type ResponseAndBody struct {
	Response *http.Response
	Body     []byte
}

// MockPromClient accepts a slice of responses and bodies to return on requests made.
// It will cycle these responses linearly, then reset back to the first.
// Also works with concurrent requests.
type MockPromClient struct {
	sync.Mutex
	responses []*ResponseAndBody
	current   int
}

// prometheus.Client URL()
func (mpc *MockPromClient) URL(ep string, args map[string]string) *url.URL {
	return nil
}

// prometheus.Client Do
func (mpc *MockPromClient) Do(context.Context, *http.Request) (*http.Response, []byte, error) {
	// fake latency
	time.Sleep(250 * time.Millisecond)

	mpc.Lock()
	defer mpc.Unlock()
	rnb := mpc.responses[mpc.current]
	mpc.current++
	if mpc.current >= len(mpc.responses) {
		mpc.current = 0
	}

	return rnb.Response, rnb.Body, nil
}

// Creates a new mock prometheus client
func newMockPromClientWith(responses []*ResponseAndBody) prometheus.Client {
	return &MockPromClient{
		responses: responses,
		current:   0,
	}
}

// creates a ResponseAndBody representing a 200 status code
func newSuccessfulResponse() *ResponseAndBody {
	body := []byte("Success")

	return &ResponseAndBody{
		Response: &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(bytes.NewReader(body)),
		},
		Body: body,
	}
}

// creates a ResponseAndBody representing a 400 status code
func newFailureResponse() *ResponseAndBody {
	body := []byte("Fail")

	return &ResponseAndBody{
		Response: &http.Response{
			StatusCode: 400,
			Body:       io.NopCloser(bytes.NewReader(body)),
		},
		Body: body,
	}
}

// creates a ResponseAndBody representing a 429 status code and 'Retry-After' header
func newNormalRateLimitedResponse(retryAfter string) *ResponseAndBody {
	body := []byte("Rate Limitted")

	return &ResponseAndBody{
		Response: &http.Response{
			StatusCode: 429,
			Header: http.Header{
				"Retry-After": []string{retryAfter},
			},
			Body: io.NopCloser(bytes.NewReader(body)),
		},
		Body: body,
	}
}

// creates a ResponseAndBody representing some amazon services ThrottlingException 400 status
func newHackyAmazonRateLimitedResponse() *ResponseAndBody {
	body := []byte("<ThrottlingException>\n  <Message>Rate exceeded</Message>\n</ThrottlingException>\n")

	return &ResponseAndBody{
		Response: &http.Response{
			StatusCode: 400,
			Body:       io.NopCloser(bytes.NewReader(body)),
		},
		Body: body,
	}
}

func newTestRetryOpts() *RateLimitRetryOpts {
	return &RateLimitRetryOpts{
		MaxRetries:       5,
		DefaultRetryWait: 100 * time.Millisecond,
	}
}

func TestRateLimitedOnceAndSuccess(t *testing.T) {
	t.Parallel()

	// creates a prom client with hard coded responses for any requests that
	// are issued
	promClient := newMockPromClientWith([]*ResponseAndBody{
		newNormalRateLimitedResponse("2"),
		newSuccessfulResponse(),
	})

	client, err := NewRateLimitedClient(
		"TestClient",
		promClient,
		1,
		nil,
		nil,
		newTestRetryOpts(),
		"",
		"",
	)

	if err != nil {
		t.Fatal(err)
	}

	req, err := http.NewRequest(http.MethodPost, "", nil)
	if err != nil {
		t.Fatal(err)
	}

	// we just need to execute this  once to see retries in effect
	res, body, err := client.Do(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}

	if res.StatusCode != 200 {
		t.Fatalf("200 StatusCode expected. Got: %d", res.StatusCode)
	}

	if string(body) != "Success" {
		t.Fatalf("Expected 'Success' message body. Got: %s", string(body))
	}
}

func TestRateLimitedOnceAndFail(t *testing.T) {
	t.Parallel()

	// creates a prom client with hard coded responses for any requests that
	// are issued
	promClient := newMockPromClientWith([]*ResponseAndBody{
		newNormalRateLimitedResponse("2"),
		newFailureResponse(),
	})

	client, err := NewRateLimitedClient(
		"TestClient",
		promClient,
		1,
		nil,
		nil,
		newTestRetryOpts(),
		"",
		"",
	)

	if err != nil {
		t.Fatal(err)
	}

	req, err := http.NewRequest(http.MethodPost, "", nil)
	if err != nil {
		t.Fatal(err)
	}

	// we just need to execute this  once to see retries in effect
	res, body, err := client.Do(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}

	if res.StatusCode != 400 {
		t.Fatalf("400 StatusCode expected. Got: %d", res.StatusCode)
	}

	if string(body) != "Fail" {
		t.Fatalf("Expected 'fail' message body. Got: %s", string(body))
	}
}

func TestRateLimitedResponses(t *testing.T) {
	t.Parallel()

	dateRetry := time.Now().Add(5 * time.Second).Format(time.RFC1123)

	// creates a prom client with hard coded responses for any requests that
	// are issued
	promClient := newMockPromClientWith([]*ResponseAndBody{
		newNormalRateLimitedResponse("2"),
		newNormalRateLimitedResponse(dateRetry),
		newHackyAmazonRateLimitedResponse(),
		newHackyAmazonRateLimitedResponse(),
		newNormalRateLimitedResponse("3"),
	})

	client, err := NewRateLimitedClient(
		"TestClient",
		promClient,
		1,
		nil,
		nil,
		newTestRetryOpts(),
		"",
		"",
	)

	if err != nil {
		t.Fatal(err)
	}

	req, err := http.NewRequest(http.MethodPost, "", nil)
	if err != nil {
		t.Fatal(err)
	}

	// we just need to execute this  once to see retries in effect
	_, _, err = client.Do(context.Background(), req)

	if err == nil {
		t.Fatal("Expected a RateLimitedResponseError. Err was nil.")
	}

	rateLimitErr, ok := err.(*RateLimitedResponseError)
	if !ok {
		t.Fatal("Expected a RateLimitedResponseError. Got unexpected type.")
	}

	t.Logf("%s\n", rateLimitErr.Error())

	// RateLimitedResponseStatus checks just ensure that wait times were close configuration
	rateLimitRetries := rateLimitErr.RateLimitStatus

	if len(rateLimitRetries) != 5 {
		t.Fatalf("Expected 5 retries. Got: %d", len(rateLimitRetries))
	}

	// check 2s wait after
	seconds := rateLimitRetries[0].WaitTime.Seconds()
	if !util.IsApproximately(seconds, 2.0) {
		t.Fatalf("Expected 2.0 seconds. Got %.2f", seconds)
	}

	// check to see if fuzzed wait time for datetime parsing
	seconds = rateLimitRetries[1].WaitTime.Seconds()
	if math.Abs(seconds-2.0) > 3.0 {
		t.Fatalf("Expected delta between 2s and resulting wait time to be within 3s. Seconds: %.2f, Delta: %.2f", seconds, math.Abs(seconds-2.0))
	}

	// check 1s wait
	seconds = rateLimitRetries[2].WaitTime.Seconds()
	if !util.IsApproximately(seconds, 0.4) {
		t.Fatalf("Expected 0.4 seconds. Got %.2f", seconds)
	}

	// check 1s wait
	seconds = rateLimitRetries[3].WaitTime.Seconds()
	if !util.IsApproximately(seconds, 0.8) {
		t.Fatalf("Expected 0.8 seconds. Got %.2f", seconds)
	}

	// check 3s wait
	seconds = rateLimitRetries[4].WaitTime.Seconds()
	if !util.IsApproximately(seconds, 3.0) {
		t.Fatalf("Expected 3.0 seconds. Got %.2f", seconds)
	}

}

func AssertDurationEqual(t *testing.T, expected, actual time.Duration) {
	if actual != expected {
		t.Fatalf("Expected: %dms, Got: %dms", expected.Milliseconds(), actual.Milliseconds())
	}
}

func TestExponentialBackOff(t *testing.T) {
	var ExpectedResults = []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
		800 * time.Millisecond,
		1600 * time.Millisecond,
	}

	w := 100 * time.Millisecond

	for retry := 0; retry < 5; retry++ {
		AssertDurationEqual(t, ExpectedResults[retry], httputil.ExponentialBackoffWaitFor(w, retry))
	}
}

func TestConcurrentRateLimiting(t *testing.T) {
	t.Parallel()

	// Set QueryConcurrency to 3 here, then add a few for total requests
	const QueryConcurrency = 3
	const TotalRequests = QueryConcurrency + 2

	dateRetry := time.Now().Add(5 * time.Second).Format(time.RFC1123)

	// creates a prom client with hard coded responses for any requests that
	// are issued
	promClient := newMockPromClientWith([]*ResponseAndBody{
		newNormalRateLimitedResponse("2"),
		newNormalRateLimitedResponse(dateRetry),
		newHackyAmazonRateLimitedResponse(),
		newHackyAmazonRateLimitedResponse(),
		newNormalRateLimitedResponse("3"),
	})

	client, err := NewRateLimitedClient(
		"TestClient",
		promClient,
		QueryConcurrency,
		nil,
		nil,
		newTestRetryOpts(),
		"",
		"",
	)

	if err != nil {
		t.Fatal(err)
	}

	errs := make(chan error, TotalRequests)

	for i := 0; i < TotalRequests; i++ {
		go func() {
			req, err := http.NewRequest(http.MethodPost, "", nil)
			if err != nil {
				errs <- err
				return
			}

			// we just need to execute this  once to see retries in effect
			_, _, err = client.Do(context.Background(), req)

			errs <- err
		}()
	}

	for i := 0; i < TotalRequests; i++ {
		err := <-errs
		if err == nil {
			t.Fatal("Expected a RateLimitedResponseError. Err was nil.")
		}

		rateLimitErr, ok := err.(*RateLimitedResponseError)
		if !ok {
			t.Fatal("Expected a RateLimitedResponseError. Got unexpected type.")
		}

		t.Logf("%s\n", rateLimitErr.Error())
	}
}
