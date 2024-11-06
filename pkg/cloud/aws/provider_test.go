package aws

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"testing"

	"github.com/opencost/opencost/pkg/cloud/models"
	v1 "k8s.io/api/core/v1"
)

func Test_awsKey_getUsageType(t *testing.T) {
	type fields struct {
		Labels     map[string]string
		ProviderID string
	}
	type args struct {
		labels map[string]string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
	}{
		{
			// test with no labels should return false
			name: "Label does not have the capacityType label associated with it",
			args: args{
				labels: map[string]string{},
			},
			want: "",
		},
		{
			name: "EKS label with a capacityType set to empty string should return empty string",
			args: args{
				labels: map[string]string{
					EKSCapacityTypeLabel: "",
				},
			},
			want: "",
		},
		{
			name: "EKS label with capacityType set to a random value should return empty string",
			args: args{
				labels: map[string]string{
					EKSCapacityTypeLabel: "TEST_ME",
				},
			},
			want: "",
		},
		{
			name: "EKS label with capacityType set to spot should return spot",
			args: args{
				labels: map[string]string{
					EKSCapacityTypeLabel: EKSCapacitySpotTypeValue,
				},
			},
			want: PreemptibleType,
		},
		{
			name: "Karpenter label with a capacityType set to empty string should return empty string",
			args: args{
				labels: map[string]string{
					models.KarpenterCapacityTypeLabel: "",
				},
			},
			want: "",
		},
		{
			name: "Karpenter label with capacityType set to a random value should return empty string",
			args: args{
				labels: map[string]string{
					models.KarpenterCapacityTypeLabel: "TEST_ME",
				},
			},
			want: "",
		},
		{
			name: "Karpenter label with capacityType set to spot should return spot",
			args: args{
				labels: map[string]string{
					models.KarpenterCapacityTypeLabel: models.KarpenterCapacitySpotTypeValue,
				},
			},
			want: PreemptibleType,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k := &awsKey{
				Labels:     tt.fields.Labels,
				ProviderID: tt.fields.ProviderID,
			}
			if got := k.getUsageType(tt.args.labels); got != tt.want {
				t.Errorf("getUsageType() = %v, want %v", got, tt.want)
			}
		})
	}
}

// Test_PricingData_Regression
//
// Objective: To test the pricing data download and validate the schema is still
// as expected
//
// These tests may take a long time to complete. It is downloading AWS Pricing
// data files (~500MB) for each region.
func Test_PricingData_Regression(t *testing.T) {
	if os.Getenv("INTEGRATION") == "" {
		t.Skip("skipping integration tests, set environment variable INTEGRATION")
	}

	awsRegions := []string{"us-east-1", "eu-west-1"}

	// Check pricing data produced for each region
	for _, region := range awsRegions {
		node := v1.Node{}
		node.SetLabels(map[string]string{"topology.kubernetes.io/region": region})

		awsTest := AWS{}
		res, _, err := awsTest.getRegionPricing([]*v1.Node{&node})
		if err != nil {
			t.Errorf("Failed to download pricing data for region %s: %v", region, err)
		}

		// Unmarshal pricing data into AWSPricing
		var pricingData AWSPricing
		body, err := io.ReadAll(res.Body)
		if err != nil {
			t.Errorf("Failed to read pricing data for region %s: %v", region, err)
		}
		err = json.Unmarshal(body, &pricingData)
		if err != nil {
			t.Errorf("Failed to unmarshal pricing data for region %s: %v", region, err)
		}

		// ASSERTION. We only anticipate "OnDemand" or "CapacityBlock" in the
		// pricing data.
		//
		// Failing this test does not necessarily mean we have regressed. Just
		// that we need to revisit this code to ensure OnDemand pricing is still
		// functioning as expected.
		for _, product := range pricingData.Products {
			if product.Attributes.MarketOption != "OnDemand" && product.Attributes.MarketOption != "CapacityBlock" && product.Attributes.MarketOption != "" {
				t.Errorf("Invalid marketOption for product %s: %s", product.Sku, product.Attributes.MarketOption)
			}
		}
	}
}

// Test_populate_pricing
//
// Objective: To test core pricing population logic for AWS
//
// Case 0: US endpoints
// Take a portion of json returned from ondemand terms in us endpoints load the
// request into the http response and give it to the function inspect the
// resulting aws object after the function returns and validate fields
//
// Case 1: Ensure marketOption=OnDemand
// AWS introduced the field marketOption. We need to further filter for
// marketOption=OnDemand to ensure we are not getting pricing from a line item
// such as marketOption=CapacityBlock
//
// Case 2: Chinese endpoints
// Same as above US test case, except using CN PV offer codes. Validate
// populated fields in AWS object
func Test_populate_pricing(t *testing.T) {
	awsTest := AWS{
		ValidPricingKeys: map[string]bool{},
		ClusterRegion:    "us-east-2",
	}
	inputkeys := map[string]bool{
		"us-east-2,m5.large,linux": true,
	}

	fixture, err := os.Open("testdata/pricing-us-east-2.json")
	if err != nil {
		t.Fatalf("failed to load pricing fixture: %s", err)
	}

	testResponse := http.Response{
		Body: io.NopCloser(fixture),
		Request: &http.Request{
			URL: &url.URL{
				Scheme: "https",
				Host:   "test-aws-http-endpoint:443",
			},
		},
	}

	awsTest.populatePricing(&testResponse, inputkeys)

	expectedProdTermsDisk := &AWSProductTerms{
		Sku:     "M6UGCCQ3CDJQAA37",
		Memory:  "",
		Storage: "",
		VCpu:    "",
		GPU:     "",
		OnDemand: &AWSOfferTerm{
			Sku:           "M6UGCCQ3CDJQAA37",
			OfferTermCode: "JRTCKXETXF",
			PriceDimensions: map[string]*AWSRateCode{
				"M6UGCCQ3CDJQAA37.JRTCKXETXF.6YS6EN2CT7": {
					Unit: "GB-Mo",
					PricePerUnit: AWSCurrencyCode{
						USD: "0.0800000000",
						CNY: "",
					},
				},
			},
		},
		PV: &models.PV{
			Cost:       "0.00010958904109589041",
			CostPerIO:  "",
			Class:      "gp3",
			Size:       "",
			Region:     "us-east-2",
			ProviderID: "",
		},
	}

	expectedProdTermsInstanceOndemand := &AWSProductTerms{
		Sku:     "8D49XP354UEYTHGM",
		Memory:  "8 GiB",
		Storage: "EBS only",
		VCpu:    "2",
		GPU:     "",
		OnDemand: &AWSOfferTerm{
			Sku:           "8D49XP354UEYTHGM",
			OfferTermCode: "MZU6U2429S",
			PriceDimensions: map[string]*AWSRateCode{
				"8D49XP354UEYTHGM.MZU6U2429S.2TG2D8R56U": {
					Unit: "Quantity",
					PricePerUnit: AWSCurrencyCode{
						USD: "1161",
						CNY: "",
					},
				},
			},
		},
	}

	expectedProdTermsInstanceSpot := &AWSProductTerms{
		Sku:     "8D49XP354UEYTHGM",
		Memory:  "8 GiB",
		Storage: "EBS only",
		VCpu:    "2",
		GPU:     "",
		OnDemand: &AWSOfferTerm{
			Sku:           "8D49XP354UEYTHGM",
			OfferTermCode: "MZU6U2429S",
			PriceDimensions: map[string]*AWSRateCode{
				"8D49XP354UEYTHGM.MZU6U2429S.2TG2D8R56U": {
					Unit: "Quantity",
					PricePerUnit: AWSCurrencyCode{
						USD: "1161",
						CNY: "",
					},
				},
			},
		},
	}

	expectedProdTermsLoadbalancer := &AWSProductTerms{
		Sku: "Y9RYMSE644KDSV4S",
		OnDemand: &AWSOfferTerm{
			Sku:           "Y9RYMSE644KDSV4S",
			OfferTermCode: "JRTCKXETXF",
			PriceDimensions: map[string]*AWSRateCode{
				"Y9RYMSE644KDSV4S.JRTCKXETXF.6YS6EN2CT7": {
					Unit: "Hrs",
					PricePerUnit: AWSCurrencyCode{
						USD: "0.0225000000",
						CNY: "",
					},
				},
			},
		},
		LoadBalancer: &models.LoadBalancer{
			Cost: 0.0225,
		},
	}

	expectedPricing := map[string]*AWSProductTerms{
		"us-east-2,EBS:VolumeUsage.gp3":             expectedProdTermsDisk,
		"us-east-2,EBS:VolumeUsage.gp3,preemptible": expectedProdTermsDisk,
		"us-east-2,m5.large,linux":                  expectedProdTermsInstanceOndemand,
		"us-east-2,m5.large,linux,preemptible":      expectedProdTermsInstanceSpot,
		"us-east-2,LoadBalancerUsage":               expectedProdTermsLoadbalancer,
	}

	if !reflect.DeepEqual(expectedPricing, awsTest.Pricing) {
		t.Fatalf("expected parsed pricing did not match actual parsed result (us-east-2)")
	}

	lbPricing, _ := awsTest.LoadBalancerPricing()
	if lbPricing.Cost != 0.0225 {
		t.Fatalf("expected loadbalancer pricing of 0.0225 but got %f (us-east-2)", lbPricing.Cost)
	}

	// Case 1 - Only accept `"marketoption":"OnDemand"`
	inputkeysCase1 := map[string]bool{
		"us-east-1,p4d.24xlarge,linux": true,
	}

	fixture, err = os.Open("testdata/pricing-us-east-1.json")
	if err != nil {
		t.Fatalf("failed to load pricing fixture: %s", err)
	}

	testResponseCase1 := http.Response{
		Body: io.NopCloser(fixture),
		Request: &http.Request{
			URL: &url.URL{
				Scheme: "https",
				Host:   "test-aws-http-endpoint:443",
			},
		},
	}

	awsTest.populatePricing(&testResponseCase1, inputkeysCase1)

	expectedProdTermsInstanceOndemandCase1 := &AWSProductTerms{
		Sku:     "H7NGEAC6UEHNTKSJ",
		Memory:  "1152 GiB",
		Storage: "8 x 1000 SSD",
		VCpu:    "96",
		GPU:     "8",
		OnDemand: &AWSOfferTerm{
			Sku:           "H7NGEAC6UEHNTKSJ",
			OfferTermCode: "JRTCKXETXF",
			PriceDimensions: map[string]*AWSRateCode{
				"H7NGEAC6UEHNTKSJ.JRTCKXETXF.6YS6EN2CT7": {
					Unit: "Hrs",
					PricePerUnit: AWSCurrencyCode{
						USD: "32.7726000000",
					},
				},
			},
		},
	}

	expectedPricingCase1 := map[string]*AWSProductTerms{
		"us-east-1,p4d.24xlarge,linux":             expectedProdTermsInstanceOndemandCase1,
		"us-east-1,p4d.24xlarge,linux,preemptible": expectedProdTermsInstanceOndemandCase1,
	}

	if !reflect.DeepEqual(expectedPricingCase1, awsTest.Pricing) {
		expectedJsonString, _ := json.MarshalIndent(expectedPricingCase1, "", "  ")
		resultJsonString, _ := json.MarshalIndent(awsTest.Pricing, "", "  ")
		t.Logf("Expected: %s", string(expectedJsonString))
		t.Logf("Result: %s", string(resultJsonString))
		t.Fatalf("expected parsed pricing did not match actual parsed result (us-east-1)")
	}

	// Case 2
	awsTest = AWS{
		ValidPricingKeys: map[string]bool{},
	}

	fixture, err = os.Open("testdata/pricing-cn-northwest-1.json")
	if err != nil {
		t.Fatalf("failed to load pricing fixture: %s", err)
	}

	testResponse = http.Response{
		Body: io.NopCloser(fixture),
		Request: &http.Request{
			URL: &url.URL{
				Scheme: "https",
				Host:   "test-aws-http-endpoint:443",
			},
		},
	}

	awsTest.populatePricing(&testResponse, inputkeys)

	expectedProdTermsDisk = &AWSProductTerms{
		Sku:     "R83VXG9NAPDASEGN",
		Memory:  "",
		Storage: "",
		VCpu:    "",
		GPU:     "",
		OnDemand: &AWSOfferTerm{
			Sku:           "R83VXG9NAPDASEGN",
			OfferTermCode: "5Y9WH78GDR",
			PriceDimensions: map[string]*AWSRateCode{
				"R83VXG9NAPDASEGN.5Y9WH78GDR.Q7UJUT2CE6": {
					Unit: "GB-Mo",
					PricePerUnit: AWSCurrencyCode{
						USD: "",
						CNY: "0.5312000000",
					},
				},
			},
		},
		PV: &models.PV{
			Cost:       "0.0007276712328767123",
			CostPerIO:  "",
			Class:      "gp3",
			Size:       "",
			Region:     "cn-northwest-1",
			ProviderID: "",
		},
	}

	expectedPricing = map[string]*AWSProductTerms{
		"cn-northwest-1,EBS:VolumeUsage.gp3":             expectedProdTermsDisk,
		"cn-northwest-1,EBS:VolumeUsage.gp3,preemptible": expectedProdTermsDisk,
	}

	if !reflect.DeepEqual(expectedPricing, awsTest.Pricing) {
		t.Fatalf("expected parsed pricing did not match actual parsed result (cn)")
	}
}

func TestFeatures(t *testing.T) {
	testCases := map[string]struct {
		aws      awsKey
		expected string
	}{
		"Spot from custom labels": {
			aws: awsKey{
				SpotLabelName:  "node-type",
				SpotLabelValue: "node-spot",
				Labels: map[string]string{
					"node-type":                "node-spot",
					v1.LabelOSStable:           "linux",
					v1.LabelHostname:           "my-hostname",
					v1.LabelTopologyRegion:     "us-west-2",
					v1.LabelTopologyZone:       "us-west-2b",
					v1.LabelInstanceTypeStable: "m5.large",
				},
			},
			expected: "us-west-2,m5.large,linux,preemptible",
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			features := tc.aws.Features()
			if features != tc.expected {
				t.Errorf("expected %s, got %s", tc.expected, features)
			}
		})
	}
}

func Test_getStorageClassTypeFrom(t *testing.T) {
	tests := []struct {
		name        string
		provisioner string
		want        string
	}{
		{
			name:        "empty-provisioner",
			provisioner: "",
			want:        "",
		},
		{
			name:        "ebs-default-provisioner",
			provisioner: "kubernetes.io/aws-ebs",
			want:        "gp2",
		},
		{
			name:        "ebs-csi-provisioner",
			provisioner: "ebs.csi.aws.com",
			want:        "gp3",
		},
		{
			name:        "unknown-provisioner",
			provisioner: "unknown",
			want:        "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getStorageClassTypeFrom(tt.provisioner); got != tt.want {
				t.Errorf("getStorageClassTypeFrom() = %v, want %v", got, tt.want)
			}
		})
	}
}
