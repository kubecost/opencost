package oracle

import (
	"fmt"
	"testing"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/pkg/cloud"
)

func TestUsageApiConfiguration_Validate(t *testing.T) {
	testCases := map[string]struct {
		config   UsageApiConfiguration
		expected error
	}{
		"valid config OCI Key": {
			config: UsageApiConfiguration{
				TenancyID: "tenancyID",
				Region:    "region",
				Authorizer: &RawConfigProvider{
					TenancyID:   "tenancyID",
					UserID:      "userID",
					Region:      "region",
					Fingerprint: "fingerprint",
					PrivateKey:  "key",
				},
			},
			expected: nil,
		},
		"invalid authorizer": {
			config: UsageApiConfiguration{
				TenancyID: "tenancyID",
				Region:    "region",
				Authorizer: &RawConfigProvider{
					TenancyID:   "tenancyID",
					UserID:      "userID",
					Region:      "region",
					Fingerprint: "",
					PrivateKey:  "",
				},
			},
			expected: fmt.Errorf("UsageApiConfiguration: RawConfigProvider: missing key fingerprint"),
		},
		"missing authorizer": {
			config: UsageApiConfiguration{
				TenancyID:  "tenancyID",
				Region:     "region",
				Authorizer: nil,
			},
			expected: fmt.Errorf("UsageApiConfiguration: missing Authorizer"),
		},
		"missing tenancyID": {
			config: UsageApiConfiguration{
				TenancyID: "",
				Region:    "region",
				Authorizer: &RawConfigProvider{
					TenancyID:   "tenancyID",
					UserID:      "userID",
					Region:      "region",
					Fingerprint: "fingerprint",
					PrivateKey:  "key",
				},
			},
			expected: fmt.Errorf("UsageApiConfiguration: missing tenancyID"),
		},
		"missing region": {
			config: UsageApiConfiguration{
				TenancyID: "tenancyID",
				Region:    "",
				Authorizer: &RawConfigProvider{
					TenancyID:   "tenancyID",
					UserID:      "userID",
					Region:      "region",
					Fingerprint: "fingerprint",
					PrivateKey:  "key",
				},
			},
			expected: fmt.Errorf("UsageApiConfiguration: missing region"),
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

func TestUsageApiConfiguration_Equals(t *testing.T) {
	testCases := map[string]struct {
		left     UsageApiConfiguration
		right    cloud.Config
		expected bool
	}{
		"matching config": {
			left: UsageApiConfiguration{
				TenancyID: "tenancyID",
				Region:    "region",
				Authorizer: &RawConfigProvider{
					TenancyID:   "tenancyID",
					UserID:      "userID",
					Region:      "region",
					Fingerprint: "fingerprint",
					PrivateKey:  "key",
				},
			},
			right: &UsageApiConfiguration{
				TenancyID: "tenancyID",
				Region:    "region",
				Authorizer: &RawConfigProvider{
					TenancyID:   "tenancyID",
					UserID:      "userID",
					Region:      "region",
					Fingerprint: "fingerprint",
					PrivateKey:  "key",
				},
			},
			expected: true,
		},
		"different configurer": {
			left: UsageApiConfiguration{
				TenancyID: "tenancyID",
				Region:    "region",
				Authorizer: &RawConfigProvider{
					TenancyID:   "tenancyID",
					UserID:      "userID",
					Region:      "region",
					Fingerprint: "fingerprint2",
					PrivateKey:  "key",
				},
			},
			right: &UsageApiConfiguration{
				TenancyID: "tenancyID",
				Region:    "region",
				Authorizer: &RawConfigProvider{
					TenancyID:   "tenancyID",
					UserID:      "userID",
					Region:      "region",
					Fingerprint: "fingerprint",
					PrivateKey:  "key",
				},
			},
			expected: false,
		},
		"missing both configurer": {
			left: UsageApiConfiguration{
				TenancyID:  "tenancyID",
				Region:     "region",
				Authorizer: nil,
			},
			right: &UsageApiConfiguration{
				TenancyID:  "tenancyID",
				Region:     "region",
				Authorizer: nil,
			},
			expected: true,
		},
		"missing left configurer": {
			left: UsageApiConfiguration{
				TenancyID:  "tenancyID",
				Region:     "region",
				Authorizer: nil,
			},
			right: &UsageApiConfiguration{
				TenancyID: "tenancyID",
				Region:    "region",
				Authorizer: &RawConfigProvider{
					TenancyID:   "tenancyID",
					UserID:      "userID",
					Region:      "region",
					Fingerprint: "fingerprint",
					PrivateKey:  "key",
				},
			},
			expected: false,
		},
		"missing right configurer": {
			left: UsageApiConfiguration{
				TenancyID: "tenancyID",
				Region:    "region",
				Authorizer: &RawConfigProvider{
					TenancyID:   "tenancyID",
					UserID:      "userID",
					Region:      "region",
					Fingerprint: "fingerprint",
					PrivateKey:  "key",
				},
			},
			right: &UsageApiConfiguration{
				TenancyID:  "tenancyID",
				Region:     "region",
				Authorizer: nil,
			},
			expected: false,
		},
		"different tenancyID": {
			left: UsageApiConfiguration{
				TenancyID: "tenancyID",
				Region:    "region",
				Authorizer: &RawConfigProvider{
					TenancyID:   "tenancyID",
					UserID:      "userID",
					Region:      "region",
					Fingerprint: "fingerprint",
					PrivateKey:  "key",
				},
			},
			right: &UsageApiConfiguration{
				TenancyID: "tenancyID2",
				Region:    "region",
				Authorizer: &RawConfigProvider{
					TenancyID:   "tenancyID2",
					UserID:      "userID",
					Region:      "region",
					Fingerprint: "fingerprint",
					PrivateKey:  "key",
				},
			},
			expected: false,
		},
		"different region": {
			left: UsageApiConfiguration{
				TenancyID: "tenancyID",
				Region:    "region",
				Authorizer: &RawConfigProvider{
					TenancyID:   "tenancyID",
					UserID:      "userID",
					Region:      "region",
					Fingerprint: "fingerprint",
					PrivateKey:  "key",
				},
			},
			right: &UsageApiConfiguration{
				TenancyID: "tenancyID",
				Region:    "region2",
				Authorizer: &RawConfigProvider{
					TenancyID:   "tenancyID",
					UserID:      "userID",
					Region:      "region2",
					Fingerprint: "fingerprint",
					PrivateKey:  "key",
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

func TestUsageApiConfiguration_JSON(t *testing.T) {
	testCases := map[string]struct {
		config UsageApiConfiguration
	}{
		"Empty Config": {
			config: UsageApiConfiguration{},
		},
		"Nil Authorizer": {
			config: UsageApiConfiguration{
				TenancyID:  "tenancyID",
				Region:     "region",
				Authorizer: nil,
			},
		},
		"RawConfigProviderAuthorizer": {
			config: UsageApiConfiguration{
				TenancyID: "tenancyID",
				Region:    "region",
				Authorizer: &RawConfigProvider{
					TenancyID:   "tenancyID",
					UserID:      "userID",
					Region:      "region2",
					Fingerprint: "fingerprint",
					PrivateKey:  "key",
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
			unmarshalledConfig := &UsageApiConfiguration{}
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
