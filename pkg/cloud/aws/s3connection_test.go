package aws

import (
	"fmt"
	"testing"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/pkg/cloud"
)

func TestS3Configuration_Validate(t *testing.T) {
	testCases := map[string]struct {
		config   S3Configuration
		expected error
	}{
		"valid config access key": {
			config: S3Configuration{
				Bucket:  "bucket",
				Region:  "region",
				Account: "account",
				Authorizer: &AccessKey{
					ID:     "id",
					Secret: "secret",
				},
			},
			expected: nil,
		},
		"valid config service account": {
			config: S3Configuration{
				Bucket:     "bucket",
				Region:     "region",
				Account:    "account",
				Authorizer: &ServiceAccount{},
			},
			expected: nil,
		},
		"access key invalid": {
			config: S3Configuration{
				Bucket:  "bucket",
				Region:  "region",
				Account: "account",
				Authorizer: &AccessKey{
					ID: "id",
				},
			},
			expected: fmt.Errorf("S3Configuration: AccessKey: missing Secret"),
		},
		"missing Authorizer": {
			config: S3Configuration{
				Bucket:     "bucket",
				Region:     "region",
				Account:    "account",
				Authorizer: nil,
			},
			expected: fmt.Errorf("S3Configuration: missing Authorizer"),
		},
		"missing bucket": {
			config: S3Configuration{
				Bucket:     "",
				Region:     "region",
				Account:    "account",
				Authorizer: &ServiceAccount{},
			},
			expected: fmt.Errorf("S3Configuration: missing bucket"),
		},
		"missing region": {
			config: S3Configuration{
				Bucket:     "bucket",
				Region:     "",
				Account:    "account",
				Authorizer: &ServiceAccount{},
			},
			expected: fmt.Errorf("S3Configuration: missing region"),
		},
		"missing account": {
			config: S3Configuration{
				Bucket:     "bucket",
				Region:     "region",
				Account:    "",
				Authorizer: &ServiceAccount{},
			},
			expected: fmt.Errorf("S3Configuration: missing account"),
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

func TestS3Configuration_Equals(t *testing.T) {
	testCases := map[string]struct {
		left     S3Configuration
		right    cloud.Config
		expected bool
	}{
		"matching config": {
			left: S3Configuration{
				Bucket:  "bucket",
				Region:  "region",
				Account: "account",
				Authorizer: &AccessKey{
					ID:     "id",
					Secret: "secret",
				},
			},
			right: &S3Configuration{
				Bucket:  "bucket",
				Region:  "region",
				Account: "account",
				Authorizer: &AccessKey{
					ID:     "id",
					Secret: "secret",
				},
			},
			expected: true,
		},
		"different Authorizer": {
			left: S3Configuration{
				Bucket:  "bucket",
				Region:  "region",
				Account: "account",
				Authorizer: &AccessKey{
					ID:     "id",
					Secret: "secret",
				},
			},
			right: &S3Configuration{
				Bucket:     "bucket",
				Region:     "region",
				Account:    "account",
				Authorizer: &ServiceAccount{},
			},
			expected: false,
		},
		"missing both Authorizer": {
			left: S3Configuration{
				Bucket:     "bucket",
				Region:     "region",
				Account:    "account",
				Authorizer: nil,
			},
			right: &S3Configuration{
				Bucket:     "bucket",
				Region:     "region",
				Account:    "account",
				Authorizer: nil,
			},
			expected: true,
		},
		"missing left Authorizer": {
			left: S3Configuration{
				Bucket:     "bucket",
				Region:     "region",
				Account:    "account",
				Authorizer: nil,
			},
			right: &S3Configuration{
				Bucket:     "bucket",
				Region:     "region",
				Account:    "account",
				Authorizer: &ServiceAccount{},
			},
			expected: false,
		},
		"missing right Authorizer": {
			left: S3Configuration{
				Bucket:  "bucket",
				Region:  "region",
				Account: "account",
				Authorizer: &AccessKey{
					ID:     "id",
					Secret: "secret",
				},
			},
			right: &S3Configuration{
				Bucket:     "bucket",
				Region:     "region",
				Account:    "account",
				Authorizer: nil,
			},
			expected: false,
		},
		"different bucket": {
			left: S3Configuration{
				Bucket:  "bucket",
				Region:  "region",
				Account: "account",
				Authorizer: &AccessKey{
					ID:     "id",
					Secret: "secret",
				},
			},
			right: &S3Configuration{
				Bucket:  "bucket2",
				Region:  "region",
				Account: "account",
				Authorizer: &AccessKey{
					ID:     "id",
					Secret: "secret",
				},
			},
			expected: false,
		},
		"different region": {
			left: S3Configuration{
				Bucket:  "bucket",
				Region:  "region",
				Account: "account",
				Authorizer: &AccessKey{
					ID:     "id",
					Secret: "secret",
				},
			},
			right: &S3Configuration{
				Bucket:  "bucket",
				Region:  "region2",
				Account: "account",
				Authorizer: &AccessKey{
					ID:     "id",
					Secret: "secret",
				},
			},
			expected: false,
		},
		"different account": {
			left: S3Configuration{
				Bucket:  "bucket",
				Region:  "region",
				Account: "account",
				Authorizer: &AccessKey{
					ID:     "id",
					Secret: "secret",
				},
			},
			right: &S3Configuration{
				Bucket:  "bucket",
				Region:  "region",
				Account: "account2",
				Authorizer: &AccessKey{
					ID:     "id",
					Secret: "secret",
				},
			},
			expected: false,
		},
		"different config": {
			left: S3Configuration{
				Bucket:  "bucket",
				Region:  "region",
				Account: "account",
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

func TestS3Configuration_JSON(t *testing.T) {
	testCases := map[string]struct {
		config S3Configuration
	}{
		"Empty Config": {
			config: S3Configuration{},
		},
		"AccessKey": {
			config: S3Configuration{
				Bucket:  "bucket",
				Region:  "region",
				Account: "account",
				Authorizer: &AccessKey{
					ID:     "id",
					Secret: "secret",
				},
			},
		},

		"ServiceAccount": {
			config: S3Configuration{
				Bucket:     "bucket",
				Region:     "region",
				Account:    "account",
				Authorizer: &ServiceAccount{},
			},
		},
		"AssumeRole with AccessKey": {
			config: S3Configuration{
				Bucket:  "bucket",
				Region:  "region",
				Account: "account",
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
			config: S3Configuration{
				Bucket:  "bucket",
				Region:  "region",
				Account: "account",
				Authorizer: &AssumeRole{
					Authorizer: &ServiceAccount{},
					RoleARN:    "12345",
				},
			},
		},
		"RoleArnNil": {
			config: S3Configuration{
				Bucket:  "bucket",
				Region:  "region",
				Account: "account",
				Authorizer: &AssumeRole{
					Authorizer: nil,
					RoleARN:    "12345",
				},
			},
		},
		"AssumeRole with AssumeRole with ServiceAccount": {
			config: S3Configuration{
				Bucket:  "bucket",
				Region:  "region",
				Account: "account",
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
			unmarshalledConfig := &S3Configuration{}
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
