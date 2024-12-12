package aws

import (
	"fmt"
	"testing"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/pkg/cloud"
)

func TestAthenaConfiguration_Validate(t *testing.T) {
	testCases := map[string]struct {
		config   AthenaConfiguration
		expected error
	}{
		"valid config access key": {
			config: AthenaConfiguration{
				Bucket:    "bucket",
				Region:    "region",
				Database:  "database",
				Catalog:   "catalog",
				Table:     "table",
				Workgroup: "workgroup",
				Account:   "account",
				Authorizer: &AccessKey{
					ID:     "id",
					Secret: "secret",
				},
			},
			expected: nil,
		},
		"valid config service account": {
			config: AthenaConfiguration{
				Bucket:     "bucket",
				Region:     "region",
				Database:   "database",
				Catalog:    "catalog",
				Table:      "table",
				Workgroup:  "workgroup",
				Account:    "account",
				Authorizer: &ServiceAccount{},
			},
			expected: nil,
		},
		"valid missing catalog": {
			config: AthenaConfiguration{
				Bucket:     "bucket",
				Region:     "region",
				Database:   "database",
				Table:      "table",
				Workgroup:  "workgroup",
				Account:    "account",
				Authorizer: &ServiceAccount{},
			},
			expected: nil,
		},
		"access key invalid": {
			config: AthenaConfiguration{
				Bucket:    "bucket",
				Region:    "region",
				Database:  "database",
				Catalog:   "catalog",
				Table:     "table",
				Workgroup: "workgroup",
				Account:   "account",
				Authorizer: &AccessKey{
					ID: "id",
				},
			},
			expected: fmt.Errorf("AthenaConfiguration: AccessKey: missing Secret"),
		},
		"missing Authorizer": {
			config: AthenaConfiguration{
				Bucket:     "bucket",
				Region:     "region",
				Database:   "database",
				Catalog:    "catalog",
				Table:      "table",
				Workgroup:  "workgroup",
				Account:    "account",
				Authorizer: nil,
			},
			expected: fmt.Errorf("AthenaConfiguration: missing Authorizer"),
		},
		"missing bucket": {
			config: AthenaConfiguration{
				Bucket:     "",
				Region:     "region",
				Database:   "database",
				Catalog:    "catalog",
				Table:      "table",
				Workgroup:  "workgroup",
				Account:    "account",
				Authorizer: &ServiceAccount{},
			},
			expected: fmt.Errorf("AthenaConfiguration: missing bucket"),
		},
		"missing region": {
			config: AthenaConfiguration{
				Bucket:     "bucket",
				Region:     "",
				Database:   "database",
				Catalog:    "catalog",
				Table:      "table",
				Workgroup:  "workgroup",
				Account:    "account",
				Authorizer: &ServiceAccount{},
			},
			expected: fmt.Errorf("AthenaConfiguration: missing region"),
		},
		"missing database": {
			config: AthenaConfiguration{
				Bucket:     "bucket",
				Region:     "region",
				Database:   "",
				Catalog:    "catalog",
				Table:      "table",
				Workgroup:  "workgroup",
				Account:    "account",
				Authorizer: &ServiceAccount{},
			},
			expected: fmt.Errorf("AthenaConfiguration: missing database"),
		},
		"missing table": {
			config: AthenaConfiguration{
				Bucket:     "bucket",
				Region:     "region",
				Database:   "database",
				Table:      "",
				Catalog:    "catalog",
				Workgroup:  "workgroup",
				Account:    "account",
				Authorizer: &ServiceAccount{},
			},
			expected: fmt.Errorf("AthenaConfiguration: missing table"),
		},
		"missing workgroup": {
			config: AthenaConfiguration{
				Bucket:     "bucket",
				Region:     "region",
				Database:   "database",
				Catalog:    "catalog",
				Table:      "table",
				Workgroup:  "",
				Account:    "account",
				Authorizer: &ServiceAccount{},
			},
			expected: nil,
		},
		"missing account": {
			config: AthenaConfiguration{
				Bucket:     "bucket",
				Region:     "region",
				Database:   "database",
				Catalog:    "catalog",
				Table:      "table",
				Workgroup:  "workgroup",
				Account:    "",
				Authorizer: &ServiceAccount{},
			},
			expected: fmt.Errorf("AthenaConfiguration: missing account"),
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

func TestAthenaConfiguration_Equals(t *testing.T) {
	testCases := map[string]struct {
		left     AthenaConfiguration
		right    cloud.Config
		expected bool
	}{
		"matching config": {
			left: AthenaConfiguration{
				Bucket:    "bucket",
				Region:    "region",
				Database:  "database",
				Catalog:   "catalog",
				Table:     "table",
				Workgroup: "workgroup",
				Account:   "account",
				Authorizer: &AccessKey{
					ID:     "id",
					Secret: "secret",
				},
			},
			right: &AthenaConfiguration{
				Bucket:    "bucket",
				Region:    "region",
				Database:  "database",
				Catalog:   "catalog",
				Table:     "table",
				Workgroup: "workgroup",
				Account:   "account",
				Authorizer: &AccessKey{
					ID:     "id",
					Secret: "secret",
				},
			},
			expected: true,
		},
		"different Authorizer": {
			left: AthenaConfiguration{
				Bucket:    "bucket",
				Region:    "region",
				Database:  "database",
				Catalog:   "catalog",
				Table:     "table",
				Workgroup: "workgroup",
				Account:   "account",
				Authorizer: &AccessKey{
					ID:     "id",
					Secret: "secret",
				},
			},
			right: &AthenaConfiguration{
				Bucket:     "bucket",
				Region:     "region",
				Database:   "database",
				Catalog:    "catalog",
				Table:      "table",
				Workgroup:  "workgroup",
				Account:    "account",
				Authorizer: &ServiceAccount{},
			},
			expected: false,
		},
		"missing both Authorizer": {
			left: AthenaConfiguration{
				Bucket:     "bucket",
				Region:     "region",
				Database:   "database",
				Catalog:    "catalog",
				Table:      "table",
				Workgroup:  "workgroup",
				Account:    "account",
				Authorizer: nil,
			},
			right: &AthenaConfiguration{
				Bucket:     "bucket",
				Region:     "region",
				Database:   "database",
				Catalog:    "catalog",
				Table:      "table",
				Workgroup:  "workgroup",
				Account:    "account",
				Authorizer: nil,
			},
			expected: true,
		},
		"missing left Authorizer": {
			left: AthenaConfiguration{
				Bucket:     "bucket",
				Region:     "region",
				Database:   "database",
				Catalog:    "catalog",
				Table:      "table",
				Workgroup:  "workgroup",
				Account:    "account",
				Authorizer: nil,
			},
			right: &AthenaConfiguration{
				Bucket:     "bucket",
				Region:     "region",
				Database:   "database",
				Catalog:    "catalog",
				Table:      "table",
				Workgroup:  "workgroup",
				Account:    "account",
				Authorizer: &ServiceAccount{},
			},
			expected: false,
		},
		"missing right Authorizer": {
			left: AthenaConfiguration{
				Bucket:    "bucket",
				Region:    "region",
				Database:  "database",
				Catalog:   "catalog",
				Table:     "table",
				Workgroup: "workgroup",
				Account:   "account",
				Authorizer: &AccessKey{
					ID:     "id",
					Secret: "secret",
				},
			},
			right: &AthenaConfiguration{
				Bucket:     "bucket",
				Region:     "region",
				Database:   "database",
				Catalog:    "catalog",
				Table:      "table",
				Workgroup:  "workgroup",
				Account:    "account",
				Authorizer: nil,
			},
			expected: false,
		},
		"different bucket": {
			left: AthenaConfiguration{
				Bucket:    "bucket",
				Region:    "region",
				Database:  "database",
				Catalog:   "catalog",
				Table:     "table",
				Workgroup: "workgroup",
				Account:   "account",
				Authorizer: &AccessKey{
					ID:     "id",
					Secret: "secret",
				},
			},
			right: &AthenaConfiguration{
				Bucket:    "bucket2",
				Region:    "region",
				Database:  "database",
				Catalog:   "catalog",
				Table:     "table",
				Workgroup: "workgroup",
				Account:   "account",
				Authorizer: &AccessKey{
					ID:     "id",
					Secret: "secret",
				},
			},
			expected: false,
		},
		"different region": {
			left: AthenaConfiguration{
				Bucket:    "bucket",
				Region:    "region",
				Database:  "database",
				Catalog:   "catalog",
				Table:     "table",
				Workgroup: "workgroup",
				Account:   "account",
				Authorizer: &AccessKey{
					ID:     "id",
					Secret: "secret",
				},
			},
			right: &AthenaConfiguration{
				Bucket:    "bucket",
				Region:    "region2",
				Database:  "database",
				Catalog:   "catalog",
				Table:     "table",
				Workgroup: "workgroup",
				Account:   "account",
				Authorizer: &AccessKey{
					ID:     "id",
					Secret: "secret",
				},
			},
			expected: false,
		},
		"different database": {
			left: AthenaConfiguration{
				Bucket:    "bucket",
				Region:    "region",
				Database:  "database",
				Catalog:   "catalog",
				Table:     "table",
				Workgroup: "workgroup",
				Account:   "account",
				Authorizer: &AccessKey{
					ID:     "id",
					Secret: "secret",
				},
			},
			right: &AthenaConfiguration{
				Bucket:    "bucket",
				Region:    "region",
				Database:  "database2",
				Catalog:   "catalog",
				Table:     "table",
				Workgroup: "workgroup",
				Account:   "account",
				Authorizer: &AccessKey{
					ID:     "id",
					Secret: "secret",
				},
			},
			expected: false,
		},
		"different table": {
			left: AthenaConfiguration{
				Bucket:    "bucket",
				Region:    "region",
				Database:  "database",
				Catalog:   "catalog",
				Table:     "table",
				Workgroup: "workgroup",
				Account:   "account",
				Authorizer: &AccessKey{
					ID:     "id",
					Secret: "secret",
				},
			},
			right: &AthenaConfiguration{
				Bucket:    "bucket",
				Region:    "region",
				Database:  "database",
				Catalog:   "catalog",
				Table:     "table2",
				Workgroup: "workgroup",
				Account:   "account",
				Authorizer: &AccessKey{
					ID:     "id",
					Secret: "secret",
				},
			},
			expected: false,
		},
		"different catalog": {
			left: AthenaConfiguration{
				Bucket:    "bucket",
				Region:    "region",
				Database:  "database",
				Catalog:   "catalog",
				Table:     "table",
				Workgroup: "workgroup",
				Account:   "account",
				Authorizer: &AccessKey{
					ID:     "id",
					Secret: "secret",
				},
			},
			right: &AthenaConfiguration{
				Bucket:    "bucket",
				Region:    "region",
				Database:  "database",
				Catalog:   "catalog2",
				Table:     "table",
				Workgroup: "workgroup",
				Account:   "account",
				Authorizer: &AccessKey{
					ID:     "id",
					Secret: "secret",
				},
			},
			expected: false,
		},
		"different workgroup": {
			left: AthenaConfiguration{
				Bucket:    "bucket",
				Region:    "region",
				Database:  "database",
				Catalog:   "catalog",
				Table:     "table",
				Workgroup: "workgroup",
				Account:   "account",
				Authorizer: &AccessKey{
					ID:     "id",
					Secret: "secret",
				},
			},
			right: &AthenaConfiguration{
				Bucket:    "bucket",
				Region:    "region",
				Database:  "database",
				Catalog:   "catalog",
				Table:     "table",
				Workgroup: "workgroup2",
				Account:   "account",
				Authorizer: &AccessKey{
					ID:     "id",
					Secret: "secret",
				},
			},
			expected: false,
		},
		"different account": {
			left: AthenaConfiguration{
				Bucket:    "bucket",
				Region:    "region",
				Database:  "database",
				Table:     "table",
				Workgroup: "workgroup",
				Account:   "account",
				Authorizer: &AccessKey{
					ID:     "id",
					Secret: "secret",
				},
			},
			right: &AthenaConfiguration{
				Bucket:    "bucket",
				Region:    "region",
				Database:  "database",
				Table:     "table",
				Workgroup: "workgroup",
				Account:   "account2",
				Authorizer: &AccessKey{
					ID:     "id",
					Secret: "secret",
				},
			},
			expected: false,
		},
		"different config": {
			left: AthenaConfiguration{
				Bucket:    "bucket",
				Region:    "region",
				Database:  "database",
				Table:     "table",
				Workgroup: "workgroup",
				Account:   "account",
				Authorizer: &AccessKey{
					ID:     "id",
					Secret: "secret",
				},
			},
			right: &AccessKey{
				ID:     "id",
				Secret: "secret",
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

func TestAthenaConfiguration_JSON(t *testing.T) {
	testCases := map[string]struct {
		config AthenaConfiguration
	}{
		"Empty Config": {
			config: AthenaConfiguration{},
		},
		"AccessKey": {
			config: AthenaConfiguration{
				Bucket:    "bucket",
				Region:    "region",
				Database:  "database",
				Catalog:   "catalog",
				Table:     "table",
				Workgroup: "workgroup",
				Account:   "account",
				Authorizer: &AccessKey{
					ID:     "id",
					Secret: "secret",
				},
			},
		},

		"ServiceAccount": {
			config: AthenaConfiguration{
				Bucket:     "bucket",
				Region:     "region",
				Database:   "database",
				Catalog:    "catalog",
				Table:      "table",
				Workgroup:  "workgroup",
				Account:    "account",
				Authorizer: &ServiceAccount{},
			},
		},
		"AssumeRole with AccessKey": {
			config: AthenaConfiguration{
				Bucket:    "bucket",
				Region:    "region",
				Database:  "database",
				Catalog:   "catalog",
				Table:     "table",
				Workgroup: "workgroup",
				Account:   "account",
				Authorizer: &AssumeRole{
					Authorizer: &AccessKey{
						ID:     "id",
						Secret: "secret",
					},
					RoleARN: "12345",
				},
			},
		},
		"AssumeRole with ServiceAccount": {
			config: AthenaConfiguration{
				Bucket:    "bucket",
				Region:    "region",
				Database:  "database",
				Catalog:   "catalog",
				Table:     "table",
				Workgroup: "workgroup",
				Account:   "account",
				Authorizer: &AssumeRole{
					Authorizer: &ServiceAccount{},
					RoleARN:    "12345",
				},
			},
		},
		"RoleArnNil": {
			config: AthenaConfiguration{
				Bucket:    "bucket",
				Region:    "region",
				Database:  "database",
				Catalog:   "catalog",
				Table:     "table",
				Workgroup: "workgroup",
				Account:   "account",
				Authorizer: &AssumeRole{
					Authorizer: nil,
					RoleARN:    "12345",
				},
			},
		},
		"AssumeRole with AssumeRole with ServiceAccount": {
			config: AthenaConfiguration{
				Bucket:    "bucket",
				Region:    "region",
				Database:  "database",
				Catalog:   "catalog",
				Table:     "table",
				Workgroup: "workgroup",
				Account:   "account",
				Authorizer: &AssumeRole{
					Authorizer: &AssumeRole{
						RoleARN:    "12345",
						Authorizer: &ServiceAccount{},
					},
					RoleARN: "12345",
				},
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
			unmarshalledConfig := &AthenaConfiguration{}
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
