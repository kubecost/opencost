package gcp

import (
	"fmt"
	"testing"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/pkg/cloud"
)

func TestBigQueryConfiguration_Validate(t *testing.T) {
	testCases := map[string]struct {
		config   BigQueryConfiguration
		expected error
	}{
		"valid config GCP Key": {
			config: BigQueryConfiguration{
				ProjectID: "projectID",
				Dataset:   "dataset",
				Table:     "table",
				Authorizer: &ServiceAccountKey{
					Key: map[string]string{
						"Key":  "Key",
						"key1": "key2",
					},
				},
			},
			expected: nil,
		},
		"valid config WorkloadIdentity": {
			config: BigQueryConfiguration{
				ProjectID:  "projectID",
				Dataset:    "dataset",
				Table:      "table",
				Authorizer: &WorkloadIdentity{},
			},
			expected: nil,
		},
		"access Key invalid": {
			config: BigQueryConfiguration{
				ProjectID: "projectID",
				Dataset:   "dataset",
				Table:     "table",
				Authorizer: &ServiceAccountKey{
					Key: nil,
				},
			},
			expected: fmt.Errorf("BigQueryConfig: issue with GCP Authorizer: ServiceAccountKey: missing Key"),
		},
		"missing configurer": {
			config: BigQueryConfiguration{
				ProjectID:  "projectID",
				Dataset:    "dataset",
				Table:      "table",
				Authorizer: nil,
			},
			expected: fmt.Errorf("BigQueryConfig: missing configurer"),
		},
		"missing projectID": {
			config: BigQueryConfiguration{
				ProjectID: "",
				Dataset:   "dataset",
				Table:     "table",
				Authorizer: &ServiceAccountKey{
					Key: map[string]string{
						"Key":  "Key",
						"key1": "key2",
					},
				},
			},
			expected: fmt.Errorf("BigQueryConfig: missing ProjectID"),
		},
		"missing dataset": {
			config: BigQueryConfiguration{
				ProjectID: "projectID",
				Dataset:   "",
				Table:     "table",
				Authorizer: &ServiceAccountKey{
					Key: map[string]string{
						"Key":  "Key",
						"key1": "key2",
					},
				},
			},
			expected: fmt.Errorf("BigQueryConfig: missing Dataset"),
		},
		"missing table": {
			config: BigQueryConfiguration{
				ProjectID: "projectID",
				Dataset:   "dataset",
				Table:     "",
				Authorizer: &ServiceAccountKey{
					Key: map[string]string{
						"Key":  "Key",
						"key1": "key2",
					},
				},
			},
			expected: fmt.Errorf("BigQueryConfig: missing Table"),
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			actual := testCase.config.Validate()
			actualString := "nil"
			if actual != nil {
				actualString = actual.Error()
			}
			expectedString := "nil"
			if testCase.expected != nil {
				expectedString = testCase.expected.Error()
			}
			if actualString != expectedString {
				t.Errorf("errors do not match: Actual: '%s', Expected: '%s", actualString, expectedString)
			}
		})
	}
}

func TestBigQueryConfiguration_Equals(t *testing.T) {
	testCases := map[string]struct {
		left     BigQueryConfiguration
		right    cloud.Config
		expected bool
	}{
		"matching config": {
			left: BigQueryConfiguration{
				ProjectID: "projectID",
				Dataset:   "dataset",
				Table:     "table",
				Authorizer: &ServiceAccountKey{
					Key: map[string]string{
						"Key":  "Key",
						"key1": "key2",
					},
				},
			},
			right: &BigQueryConfiguration{
				ProjectID: "projectID",
				Dataset:   "dataset",
				Table:     "table",
				Authorizer: &ServiceAccountKey{
					Key: map[string]string{
						"Key":  "Key",
						"key1": "key2",
					},
				},
			},
			expected: true,
		},
		"different configurer": {
			left: BigQueryConfiguration{
				ProjectID: "projectID",
				Dataset:   "dataset",
				Table:     "table",
				Authorizer: &ServiceAccountKey{
					Key: map[string]string{
						"Key":  "Key",
						"key1": "key2",
					},
				},
			},
			right: &BigQueryConfiguration{
				ProjectID:  "projectID",
				Dataset:    "dataset",
				Table:      "table",
				Authorizer: &WorkloadIdentity{},
			},
			expected: false,
		},
		"missing both configurer": {
			left: BigQueryConfiguration{
				ProjectID:  "projectID",
				Dataset:    "dataset",
				Table:      "table",
				Authorizer: nil,
			},
			right: &BigQueryConfiguration{
				ProjectID:  "projectID",
				Dataset:    "dataset",
				Table:      "table",
				Authorizer: nil,
			},
			expected: true,
		},
		"missing left configurer": {
			left: BigQueryConfiguration{
				ProjectID:  "projectID",
				Dataset:    "dataset",
				Table:      "table",
				Authorizer: nil,
			},
			right: &BigQueryConfiguration{
				ProjectID:  "projectID",
				Dataset:    "dataset",
				Table:      "table",
				Authorizer: &WorkloadIdentity{},
			},
			expected: false,
		},
		"missing right configurer": {
			left: BigQueryConfiguration{
				ProjectID: "projectID",
				Dataset:   "dataset",
				Table:     "table",
				Authorizer: &ServiceAccountKey{
					Key: map[string]string{
						"Key":  "Key",
						"key1": "key2",
					},
				},
			},
			right: &BigQueryConfiguration{
				ProjectID:  "projectID",
				Dataset:    "dataset",
				Table:      "table",
				Authorizer: nil,
			},
			expected: false,
		},
		"different projectID": {
			left: BigQueryConfiguration{
				ProjectID: "projectID",
				Dataset:   "dataset",
				Table:     "table",
				Authorizer: &ServiceAccountKey{
					Key: map[string]string{
						"Key":  "Key",
						"key1": "key2",
					},
				},
			},
			right: &BigQueryConfiguration{
				ProjectID: "projectID2",
				Dataset:   "dataset",
				Table:     "table",
				Authorizer: &ServiceAccountKey{
					Key: map[string]string{
						"Key":  "Key",
						"key1": "key2",
					},
				},
			},
			expected: false,
		},
		"different dataset": {
			left: BigQueryConfiguration{
				ProjectID: "projectID",
				Dataset:   "dataset",
				Table:     "table",
				Authorizer: &ServiceAccountKey{
					Key: map[string]string{
						"Key":  "Key",
						"key1": "key2",
					},
				},
			},
			right: &BigQueryConfiguration{
				ProjectID: "projectID",
				Dataset:   "dataset2",
				Table:     "table",
				Authorizer: &ServiceAccountKey{
					Key: map[string]string{
						"Key":  "Key",
						"key1": "key2",
					},
				},
			},
			expected: false,
		},
		"different table": {
			left: BigQueryConfiguration{
				ProjectID: "projectID",
				Dataset:   "dataset",
				Table:     "table",
				Authorizer: &ServiceAccountKey{
					Key: map[string]string{
						"Key":  "Key",
						"key1": "key2",
					},
				},
			},
			right: &BigQueryConfiguration{
				ProjectID: "projectID",
				Dataset:   "dataset",
				Table:     "table2",
				Authorizer: &ServiceAccountKey{
					Key: map[string]string{
						"Key":  "Key",
						"key1": "key2",
					},
				},
			},
			expected: false,
		},
		"different config": {
			left: BigQueryConfiguration{
				ProjectID: "projectID",
				Dataset:   "dataset",
				Table:     "table",
				Authorizer: &ServiceAccountKey{
					Key: map[string]string{
						"Key":  "Key",
						"key1": "key2",
					},
				},
			},
			right: &ServiceAccountKey{

				Key: map[string]string{
					"Key":  "Key",
					"key1": "key2",
				},
			},
			expected: false,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			actual := testCase.left.Equals(testCase.right)
			if actual != testCase.expected {
				t.Errorf("incorrect result: Actual: '%t', Expected: '%t", actual, testCase.expected)
			}
		})
	}
}

func TestBigQueryConfiguration_JSON(t *testing.T) {
	testCases := map[string]struct {
		config BigQueryConfiguration
	}{
		"Empty Config": {
			config: BigQueryConfiguration{},
		},
		"Nil Authorizer": {
			config: BigQueryConfiguration{
				ProjectID:  "projectID",
				Dataset:    "dataset",
				Table:      "table",
				Authorizer: nil,
			},
		},
		"ServiceAccountKeyConfigurer": {
			config: BigQueryConfiguration{
				ProjectID: "projectID",
				Dataset:   "dataset",
				Table:     "table",
				Authorizer: &ServiceAccountKey{
					Key: map[string]string{
						"Key":  "Key",
						"key1": "key2",
					},
				},
			},
		},
		"WorkLoadIdentityConfigurer": {
			config: BigQueryConfiguration{
				ProjectID:  "projectID",
				Dataset:    "dataset",
				Table:      "table",
				Authorizer: &WorkloadIdentity{},
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {

			// test JSON Marshalling
			configJSON, err := json.Marshal(testCase.config)
			if err != nil {
				t.Errorf("failed to marshal configuration: %s", err.Error())
			}
			log.Info(string(configJSON))
			unmarshalledConfig := &BigQueryConfiguration{}
			err = json.Unmarshal(configJSON, unmarshalledConfig)
			if err != nil {
				t.Errorf("failed to unmarshal configuration: %s", err.Error())
			}
			if !testCase.config.Equals(unmarshalledConfig) {
				t.Error("config does not equal unmarshalled config")
			}
		})
	}
}
