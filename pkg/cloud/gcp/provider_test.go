package gcp

import (
	"bytes"
	"encoding/json"
	"os"
	"reflect"
	"testing"

	"github.com/google/martian/log"
	"github.com/opencost/opencost/pkg/cloud/models"
)

func TestParseGCPInstanceTypeLabel(t *testing.T) {
	cases := []struct {
		input    string
		expected string
	}{
		{
			input:    "n1-standard-2",
			expected: "n1standard",
		},
		{
			input:    "e2-medium",
			expected: "e2medium",
		},
		{
			input:    "k3s",
			expected: "unknown",
		},
		{
			input:    "custom-n1-standard-2",
			expected: "custom",
		},
		{
			input:    "n2d-highmem-8",
			expected: "n2dstandard",
		},
	}

	for _, test := range cases {
		result := parseGCPInstanceTypeLabel(test.input)
		if result != test.expected {
			t.Errorf("Input: %s, Expected: %s, Actual: %s", test.input, test.expected, result)
		}
	}
}

func TestParseGCPProjectID(t *testing.T) {
	cases := []struct {
		input    string
		expected string
	}{
		{
			input:    "gce://guestbook-12345/...",
			expected: "guestbook-12345",
		},
		{
			input:    "gce:/guestbook-12345/...",
			expected: "",
		},
		{
			input:    "asdfa",
			expected: "",
		},
		{
			input:    "",
			expected: "",
		},
	}

	for _, test := range cases {
		result := ParseGCPProjectID(test.input)
		if result != test.expected {
			t.Errorf("Input: %s, Expected: %s, Actual: %s", test.input, test.expected, result)
		}
	}
}

func TestGetUsageType(t *testing.T) {
	cases := []struct {
		input    map[string]string
		expected string
	}{
		{
			input: map[string]string{
				GKEPreemptibleLabel: "true",
			},
			expected: "preemptible",
		},
		{
			input: map[string]string{
				GKESpotLabel: "true",
			},
			expected: "preemptible",
		},
		{
			input: map[string]string{
				models.KarpenterCapacityTypeLabel: models.KarpenterCapacitySpotTypeValue,
			},
			expected: "preemptible",
		},
		{
			input: map[string]string{
				"someotherlabel": "true",
			},
			expected: "ondemand",
		},
		{
			input:    map[string]string{},
			expected: "ondemand",
		},
	}

	for _, test := range cases {
		result := getUsageType(test.input)
		if result != test.expected {
			t.Errorf("Input: %v, Expected: %s, Actual: %s", test.input, test.expected, result)
		}
	}
}

func TestKeyFeatures(t *testing.T) {
	type testCase struct {
		key *gcpKey
		exp string
	}

	testCases := []testCase{
		{
			key: &gcpKey{
				Labels: map[string]string{
					"node.kubernetes.io/instance-type": "n2-standard-4",
					"topology.kubernetes.io/region":    "us-east1",
				},
			},
			exp: "us-east1,n2standard,ondemand",
		},
		{
			key: &gcpKey{
				Labels: map[string]string{
					"node.kubernetes.io/instance-type": "e2-standard-8",
					"topology.kubernetes.io/region":    "us-west1",
					"cloud.google.com/gke-preemptible": "true",
				},
			},
			exp: "us-west1,e2standard,preemptible",
		},
		{
			key: &gcpKey{
				Labels: map[string]string{
					"node.kubernetes.io/instance-type": "a2-highgpu-1g",
					"cloud.google.com/gke-gpu":         "true",
					"cloud.google.com/gke-accelerator": "nvidia-tesla-a100",
					"topology.kubernetes.io/region":    "us-central1",
				},
			},
			exp: "us-central1,a2highgpu,ondemand,gpu",
		},
		{
			key: &gcpKey{
				Labels: map[string]string{
					"node.kubernetes.io/instance-type": "t2d-standard-1",
					"topology.kubernetes.io/region":    "asia-southeast1",
				},
			},
			exp: "asia-southeast1,t2dstandard,ondemand",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.exp, func(t *testing.T) {
			act := tc.key.Features()
			if act != tc.exp {
				t.Errorf("expected '%s'; got '%s'", tc.exp, act)
			}
		})
	}
}

// tests basic parsing of GCP pricing API responses
// Load a reader object on a portion of a GCP api response
// Confirm that the resting *GCP object contains the correctly parsed pricing info
func TestParsePage(t *testing.T) {

	testCases := map[string]struct {
		inputFile      string
		inputKeys      map[string]models.Key
		pvKeys         map[string]models.PVKey
		expectedPrices map[string]*GCPPricing
		expectedToken  string
		expectError    bool
	}{
		"Error Response": {
			inputFile:      "./test/error.json",
			inputKeys:      nil,
			pvKeys:         nil,
			expectedPrices: nil,
			expectError:    true,
		},
		"SKU file": {
			// NOTE: SKUs here are copied directly from GCP Billing API. Some of them
			// are in currency IDR, which relates directly to ticket GTM-52, for which
			// some of this work was done. So if the prices look huge... don't panic.
			// The only thing we're testing here is that, given these instance types
			// and regions and prices, those same prices get set appropriately into
			// the returned pricing map.
			inputFile: "./test/skus.json",
			inputKeys: map[string]models.Key{
				"us-central1,a2highgpu,ondemand,gpu": &gcpKey{
					Labels: map[string]string{
						"node.kubernetes.io/instance-type": "a2-highgpu-1g",
						"cloud.google.com/gke-gpu":         "true",
						"cloud.google.com/gke-accelerator": "nvidia-tesla-a100",
						"topology.kubernetes.io/region":    "us-central1",
					},
				},
				"us-central1,e2medium,ondemand": &gcpKey{
					Labels: map[string]string{
						"node.kubernetes.io/instance-type": "e2-medium",
						"topology.kubernetes.io/region":    "us-central1",
					},
				},
				"us-central1,e2standard,ondemand": &gcpKey{
					Labels: map[string]string{
						"node.kubernetes.io/instance-type": "e2-standard",
						"topology.kubernetes.io/region":    "us-central1",
					},
				},
				"asia-southeast1,t2dstandard,ondemand": &gcpKey{
					Labels: map[string]string{
						"node.kubernetes.io/instance-type": "t2d-standard-1",
						"topology.kubernetes.io/region":    "asia-southeast1",
					},
				},
			},
			pvKeys: map[string]models.PVKey{},
			expectedPrices: map[string]*GCPPricing{
				"us-central1,a2highgpu,ondemand,gpu": {
					Name:        "services/6F81-5844-456A/skus/039F-D0DA-4055",
					SKUID:       "039F-D0DA-4055",
					Description: "Nvidia Tesla A100 GPU running in Americas",
					Category: &GCPResourceInfo{
						ServiceDisplayName: "Compute Engine",
						ResourceFamily:     "Compute",
						ResourceGroup:      "GPU",
						UsageType:          "OnDemand",
					},
					ServiceRegions: []string{"us-central1", "us-east1", "us-west1"},
					PricingInfo: []*PricingInfo{
						{
							Summary: "",
							PricingExpression: &PricingExpression{
								UsageUnit:                "h",
								UsageUnitDescription:     "hour",
								BaseUnit:                 "s",
								BaseUnitConversionFactor: 0,
								DisplayQuantity:          1,
								TieredRates: []*TieredRates{
									{
										StartUsageAmount: 0,
										UnitPrice: &UnitPriceInfo{
											CurrencyCode: "USD",
											Units:        "2",
											Nanos:        933908000,
										},
									},
								},
							},
							CurrencyConversionRate: 1,
							EffectiveTime:          "2023-03-24T10:52:50.681Z",
						},
					},
					ServiceProviderName: "Google",
					Node: &models.Node{
						VCPUCost:         "0.031611",
						RAMCost:          "0.004237",
						UsesBaseCPUPrice: false,
						GPU:              "1",
						GPUName:          "nvidia-tesla-a100",
						GPUCost:          "2.933908",
					},
				},
				"us-central1,a2highgpu,ondemand": {
					Node: &models.Node{
						VCPUCost:         "0.031611",
						RAMCost:          "0.004237",
						UsesBaseCPUPrice: false,
						UsageType:        "ondemand",
					},
				},
				"us-central1,e2medium,ondemand": {
					Node: &models.Node{
						VCPU:             "1.000000",
						VCPUCost:         "327.173848364",
						RAMCost:          "43.85294978",
						UsesBaseCPUPrice: false,
						UsageType:        "ondemand",
					},
				},
				"us-central1,e2medium,ondemand,gpu": {
					Node: &models.Node{
						VCPU:             "1.000000",
						VCPUCost:         "327.173848364",
						RAMCost:          "43.85294978",
						UsesBaseCPUPrice: false,
						UsageType:        "ondemand",
					},
				},
				"us-central1,e2standard,ondemand": {
					Node: &models.Node{
						VCPUCost:         "327.173848364",
						RAMCost:          "43.85294978",
						UsesBaseCPUPrice: false,
						UsageType:        "ondemand",
					},
				},
				"us-central1,e2standard,ondemand,gpu": {
					Node: &models.Node{
						VCPUCost:         "327.173848364",
						RAMCost:          "43.85294978",
						UsesBaseCPUPrice: false,
						UsageType:        "ondemand",
					},
				},
				"asia-southeast1,t2dstandard,ondemand": {
					Node: &models.Node{
						VCPUCost:         "508.934997455",
						RAMCost:          "68.204999658",
						UsesBaseCPUPrice: false,
						UsageType:        "ondemand",
					},
				},
				"asia-southeast1,t2dstandard,ondemand,gpu": {
					Node: &models.Node{
						VCPUCost:         "508.934997455",
						RAMCost:          "68.204999658",
						UsesBaseCPUPrice: false,
						UsageType:        "ondemand",
					},
				},
			},
			expectedToken: "APKCS1HVa0YpwgyTFbqbJ1eGwzKZmsPwLqzMZPTSNia5ck1Hc54Tx_Kz3oBxwSnRIdGVxXoSPdf-XlDpyNBf4QuxKcIEgtrQ1LDLWAgZowI0ns7HjrGta2s=",
			expectError:   false,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			fileBytes, err := os.ReadFile(tc.inputFile)
			if err != nil {
				t.Fatalf("failed to open file '%s': %s", tc.inputFile, err)
			}
			reader := bytes.NewReader(fileBytes)

			testGcp := &GCP{}
			actualPrices, token, err := testGcp.parsePage(reader, tc.inputKeys, tc.pvKeys)
			if err != nil {
				log.Errorf("got error parsing page: %v", err)
			}
			if tc.expectError != (err != nil) {
				t.Fatalf("Error from result was not as expected. Expected: %v, Actual: %v", tc.expectError, err != nil)
			}

			if token != tc.expectedToken {
				t.Fatalf("error parsing GCP next page token, parsed %s but expected %s", token, tc.expectedToken)
			}

			if !reflect.DeepEqual(actualPrices, tc.expectedPrices) {
				act, _ := json.Marshal(actualPrices)
				exp, _ := json.Marshal(tc.expectedPrices)
				t.Errorf("error parsing GCP prices: parsed \n%s\n expected \n%s\n", string(act), string(exp))
			}
		})
	}

}
