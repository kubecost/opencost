package httputil

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestInvalidKeys(t *testing.T) {
	vals := url.Values{}
	vals.Set("window", "7d")
	vals.Set("aggregate", "namespace")
	vals.Set("filterClsuters", "cluster-two") // Intentional typo

	qp := NewQueryParams(vals)

	result := qp.InvalidKeys([]string{"window", "aggregate", "filterClusters", "filterNamespaces"})
	expected := []string{"filterClsuters"}
	if diff := cmp.Diff(result, expected); len(diff) > 0 {
		t.Errorf("Expected: %+v. Got: %+v", expected, result)
	}
}

func TestHeaderString(t *testing.T) {
	h := make(http.Header)
	h.Add("foo", "abc")
	h.Add("foo", "123")
	h.Add("bar", "foo")
	h.Add("Content-Type", "application/octet-stream")

	s := HeaderString(h)
	if len(s) == 0 {
		t.Errorf("Header String failed to produce a valid output")
		return
	}

	t.Logf("Result: %s\n", s)
}

func TestEmptyHeader(t *testing.T) {
	h := make(http.Header)

	s := HeaderString(h)
	if len(s) == 0 {
		t.Errorf("Header String failed to produce a valid output")
		return
	}

	t.Logf("Result: %s\n", s)
}

func TestNilHeader(t *testing.T) {
	var h http.Header

	s := HeaderString(h)
	if len(s) == 0 {
		t.Errorf("Header String failed to produce a valid output")
		return
	}

	t.Logf("Result: %s\n", s)
}
