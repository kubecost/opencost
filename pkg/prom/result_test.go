package prom

import (
	"fmt"
	"strings"
	"testing"
)

func TestErrorFunctions(t *testing.T) {
	testCases := []struct {
		name     string
		fn       func(string, interface{}) error
		query    string
		response interface{}
	}{
		{
			name:     "DataFieldFormatErr",
			fn:       DataFieldFormatErr,
			query:    "avg(node_total_hourly_cost{}) by (node, cluster, provider_id)[24h:5m]",
			response: map[string]string{"foo": "bar"},
		},
		{
			name:     "DataPointFormatErr",
			fn:       DataPointFormatErr,
			query:    "avg(node_total_hourly_cost{}) by (node, cluster, provider_id)[24h:5m]",
			response: []string{"invalid"},
		},
		{
			name:     "MetricFieldDoesNotExistErr",
			fn:       MetricFieldDoesNotExistErr,
			query:    "avg(node_total_hourly_cost{}) by (node, cluster, provider_id)[24h:5m]",
			response: map[string]interface{}{"values": []interface{}{}},
		},
		{
			name:     "MetricFieldFormatErr",
			fn:       MetricFieldFormatErr,
			query:    "avg(node_total_hourly_cost{}) by (node, cluster, provider_id)[24h:5m]",
			response: "invalid",
		},
		{
			name:     "PromUnexpectedResponseErr",
			fn:       PromUnexpectedResponseErr,
			query:    "avg(node_total_hourly_cost{}) by (node, cluster, provider_id)[24h:5m]",
			response: nil,
		},
		{
			name:     "ResultFieldDoesNotExistErr",
			fn:       ResultFieldDoesNotExistErr,
			query:    "avg(node_total_hourly_cost{}) by (node, cluster, provider_id)[24h:5m]",
			response: map[string]interface{}{"resultType": "matrix"},
		},
		{
			name:     "ResultFieldFormatErr",
			fn:       ResultFieldFormatErr,
			query:    "avg(node_total_hourly_cost{}) by (node, cluster, provider_id)[24h:5m]",
			response: "invalid",
		},
		{
			name:     "ResultFormatErr",
			fn:       ResultFormatErr,
			query:    "avg(node_total_hourly_cost{}) by (node, cluster, provider_id)[24h:5m]",
			response: 123,
		},
		{
			name:     "ValueFieldDoesNotExistErr",
			fn:       ValueFieldDoesNotExistErr,
			query:    "avg(node_total_hourly_cost{}) by (node, cluster, provider_id)[24h:5m]",
			response: map[string]interface{}{"metric": map[string]interface{}{}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.fn(tc.query, tc.response)
			if err == nil {
				t.Errorf("Expected error, got nil")
				return
			}

			// Verify error contains key components without being overly strict about exact wording
			if !strings.Contains(err.Error(), tc.query) {
				t.Errorf("Error message missing query string '%s': %s", tc.query, err.Error())
			}
			if !strings.Contains(err.Error(), fmt.Sprintf("%+v", tc.response)) {
				t.Errorf("Error message missing response value '%+v': %s", tc.response, err.Error())
			}
		})
	}
}
