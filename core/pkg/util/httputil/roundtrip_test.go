package httputil

import (
	"fmt"
	"net/http"
	"reflect"
	"testing"
)

type reqValidateRoundTripper struct {
	expectedReq *http.Request
}

func (rt *reqValidateRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	if !reflect.DeepEqual(r, rt.expectedReq) {
		return nil, fmt.Errorf("expected req %v, got %v", rt.expectedReq, r)
	}
	return nil, nil
}

func TestUserAgentTransport(t *testing.T) {
	for _, tc := range []struct {
		name   string
		ua     string
		req    *http.Request
		expReq *http.Request
	}{
		{
			name:   "opencost",
			ua:     "opencost",
			req:    &http.Request{},
			expReq: &http.Request{Header: http.Header{"User-Agent": []string{"opencost"}}},
		},
		{
			name:   "foo",
			ua:     "foo",
			req:    &http.Request{},
			expReq: &http.Request{Header: http.Header{"User-Agent": []string{"foo"}}},
		},
		{
			name:   "overwrite user agent if exists",
			ua:     "opencost",
			req:    &http.Request{Header: http.Header{"User-Agent": []string{"foo"}}},
			expReq: &http.Request{Header: http.Header{"User-Agent": []string{"opencost"}}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rt := NewUserAgentTransport(tc.ua, &reqValidateRoundTripper{
				expectedReq: tc.expReq,
			})
			_, err := rt.RoundTrip(tc.req)
			if err != nil {
				t.Error(err)
			}
		})
	}
}
