package httputil

import "net/http"

type userAgentTransport struct {
	userAgent string
	base      http.RoundTripper
}

// NewUserAgentTransport creates a RoundTripper that attaches the configured user agent.
func NewUserAgentTransport(userAgent string, base http.RoundTripper) http.RoundTripper {
	return &userAgentTransport{
		userAgent: userAgent,
		base:      base,
	}
}

func (t userAgentTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	// The specification of http.RoundTripper says that it shouldn't mutate
	// the request so make a copy of req.Header since this is all that is
	// modified.
	r2 := new(http.Request)
	*r2 = *r
	r2.Header = make(http.Header)
	for k, s := range r.Header {
		r2.Header[k] = s
	}
	r2.Header.Set("User-Agent", t.userAgent)
	r = r2
	return t.base.RoundTrip(r)
}
