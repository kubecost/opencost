package alibaba

import (
	"fmt"
	"testing"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/pkg/cloud"
)

func TestBoaConfiguration_Validate(t *testing.T) {
	testCases := map[string]struct {
		config   BOAConfiguration
		expected error
	}{
		"valid config Azure AccessKey": {
			config: BOAConfiguration{
				Account: "Account Number",
				Region:  "Region",
				Authorizer: &AccessKey{
					AccessKeyID:     "accessKeyID",
					AccessKeySecret: "accessKeySecret",
				},
			},
			expected: nil,
		},
		"access key invalid": {
			config: BOAConfiguration{
				Account: "Account Number",
				Region:  "Region",
				Authorizer: &AccessKey{
					AccessKeySecret: "accessKeySecret",
				},
			},
			expected: fmt.Errorf("AccessKey: missing Access key ID"),
		},
		"access secret invalid": {
			config: BOAConfiguration{
				Account: "Account Number",
				Region:  "Region",
				Authorizer: &AccessKey{
					AccessKeyID: "accessKeyId",
				},
			},
			expected: fmt.Errorf("AccessKey: missing Access Key secret"),
		},
		"missing authorizer": {
			config: BOAConfiguration{
				Account:    "Account Number",
				Region:     "Region",
				Authorizer: nil,
			},
			expected: fmt.Errorf("BOAConfiguration: missing authorizer"),
		},
		"missing Account": {
			config: BOAConfiguration{
				Account: "",
				Region:  "Region",
				Authorizer: &AccessKey{
					AccessKeyID:     "accessKeyID",
					AccessKeySecret: "accessKeySecret",
				},
			},
			expected: fmt.Errorf("BOAConfiguration: missing account"),
		},
		"missing Region": {
			config: BOAConfiguration{
				Account: "Account",
				Authorizer: &AccessKey{
					AccessKeyID:     "accessKeyID",
					AccessKeySecret: "accessKeySecret",
				},
			},
			expected: fmt.Errorf("BOAConfiguration: missing region"),
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

func TestBOAConfiguration_Equals(t *testing.T) {
	testCases := map[string]struct {
		left     BOAConfiguration
		right    cloud.Config
		expected bool
	}{
		"matching config": {
			left: BOAConfiguration{
				Region:  "region",
				Account: "account",
				Authorizer: &AccessKey{
					AccessKeyID:     "id",
					AccessKeySecret: "secret",
				},
			},
			right: &BOAConfiguration{
				Region:  "region",
				Account: "account",
				Authorizer: &AccessKey{
					AccessKeyID:     "id",
					AccessKeySecret: "secret",
				},
			},
			expected: true,
		},
		"different Authorizer": {
			left: BOAConfiguration{
				Region:  "region",
				Account: "account",
				Authorizer: &AccessKey{
					AccessKeyID:     "id",
					AccessKeySecret: "secret",
				},
			},
			right: &BOAConfiguration{
				Region:  "region",
				Account: "account",
				Authorizer: &AccessKey{
					AccessKeyID:     "id2",
					AccessKeySecret: "secret2",
				},
			},
			expected: false,
		},
		"missing both Authorizer": {
			left: BOAConfiguration{
				Region:     "region",
				Account:    "account",
				Authorizer: nil,
			},
			right: &BOAConfiguration{
				Region:     "region",
				Account:    "account",
				Authorizer: nil,
			},
			expected: true,
		},
		"missing left Authorizer": {
			left: BOAConfiguration{
				Region:     "region",
				Account:    "account",
				Authorizer: nil,
			},
			right: &BOAConfiguration{
				Region:  "region",
				Account: "account",
				Authorizer: &AccessKey{
					AccessKeyID:     "id",
					AccessKeySecret: "secret",
				},
			},
			expected: false,
		},
		"missing right Authorizer": {
			left: BOAConfiguration{
				Region:  "region",
				Account: "account",
				Authorizer: &AccessKey{
					AccessKeyID:     "id",
					AccessKeySecret: "secret",
				},
			},
			right: &BOAConfiguration{
				Region:     "region",
				Account:    "account",
				Authorizer: nil,
			},
			expected: false,
		},
		"different region": {
			left: BOAConfiguration{
				Region:  "region",
				Account: "account",
				Authorizer: &AccessKey{
					AccessKeyID:     "id",
					AccessKeySecret: "secret",
				},
			},
			right: &BOAConfiguration{
				Region:  "region2",
				Account: "account",
				Authorizer: &AccessKey{
					AccessKeyID:     "id",
					AccessKeySecret: "secret",
				},
			},
			expected: false,
		},
		"different account": {
			left: BOAConfiguration{
				Region:  "region",
				Account: "account",
				Authorizer: &AccessKey{
					AccessKeyID:     "id",
					AccessKeySecret: "secret",
				},
			},
			right: &BOAConfiguration{
				Region:  "region",
				Account: "account2",
				Authorizer: &AccessKey{
					AccessKeyID:     "id",
					AccessKeySecret: "secret",
				},
			},
			expected: false,
		},
		"different config": {
			left: BOAConfiguration{
				Region:  "region",
				Account: "account",
				Authorizer: &AccessKey{
					AccessKeyID:     "id",
					AccessKeySecret: "secret",
				},
			},
			right: &AccessKey{
				AccessKeyID:     "id",
				AccessKeySecret: "secret",
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

func TestBOAConfiguration_JSON(t *testing.T) {
	testCases := map[string]struct {
		config BOAConfiguration
	}{
		"Empty Config": {
			config: BOAConfiguration{},
		},
		"AccessKey": {
			config: BOAConfiguration{
				Region:  "region",
				Account: "account",
				Authorizer: &AccessKey{
					AccessKeyID:     "id",
					AccessKeySecret: "secret",
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
			unmarshalledConfig := &BOAConfiguration{}
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
