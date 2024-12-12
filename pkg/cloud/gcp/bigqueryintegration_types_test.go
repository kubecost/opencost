package gcp

import (
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/opencost/opencost/core/pkg/opencost"
)

func Test_Load_ResourceFallback(t *testing.T) {
	schema := bigquery.Schema{
		&bigquery.FieldSchema{
			Name: UsageDateColumnName,
		},
		&bigquery.FieldSchema{
			Name: ResourceNameColumnName,
		},
		&bigquery.FieldSchema{
			Name: ResourceGlobalNameColumnName,
		},
	}

	testCases := map[string]struct {
		values             []bigquery.Value
		expectedProviderID string
	}{
		"no data": {
			values: []bigquery.Value{
				bigquery.Value(time.Now()),
				bigquery.Value(nil),
				bigquery.Value(nil),
			},
			expectedProviderID: "",
		},
		"resource name only": {
			values: []bigquery.Value{
				bigquery.Value(time.Now()),
				bigquery.Value("resource_name"),
				bigquery.Value(nil),
			},
			expectedProviderID: "resource_name",
		},
		"resource global name only": {
			values: []bigquery.Value{
				bigquery.Value(time.Now()),
				bigquery.Value(nil),
				bigquery.Value("resource_global_name"),
			},
			expectedProviderID: "resource_global_name",
		},
		"resource name and global name": {
			values: []bigquery.Value{
				bigquery.Value(time.Now()),
				bigquery.Value("resource_name"),
				bigquery.Value("resource_global_name"),
			},
			expectedProviderID: "resource_name",
		},
	}
	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			ccl := CloudCostLoader{
				CloudCost: &opencost.CloudCost{},
			}

			err := ccl.Load(testCase.values, schema)
			if err != nil {
				t.Errorf("Other error during testing %s", err)
			} else if ccl.CloudCost.Properties.ProviderID != testCase.expectedProviderID {
				t.Errorf("Incorrect result, actual ProviderID: %s, expected: %s", ccl.CloudCost.Properties.ProviderID, testCase.expectedProviderID)
			}
		})
	}
}
