package cloudcost

import (
	"testing"
)

func TestNewCloudCostFilterParserParse(t *testing.T) {
	parser := NewCloudCostFilterParser()
	testCases := map[string]struct {
		input       string
		expectError bool
	}{
		"Empty": {
			input:       ``,
			expectError: false,
		},
		"InvoiceEntityID": {
			input:       `invoiceEntityID: "123"`,
			expectError: false,
		},
		"InvoiceEntityName": {
			input:       `invoiceEntityName: "foo"`,
			expectError: false,
		},
		"AccountID": {
			input:       `accountID: "123"`,
			expectError: false,
		},
		"AccountName": {
			input:       `accountName: "foo"`,
			expectError: false,
		},
		"RegionID": {
			input:       `regionID: "us-west-1"`,
			expectError: false,
		},
		"AvailabilityZone": {
			input:       `availabilityZone: "us-west-1a"`,
			expectError: false,
		},
		"Provider": {
			input:       `provider: "aws"`,
			expectError: false,
		},
		"ProviderID": {
			input:       `providerID: "i-123"`,
			expectError: false,
		},
		"Category": {
			input:       `category: "compute"`,
			expectError: false,
		},
		"Service": {
			input:       `service: "ec2"`,
			expectError: false,
		},
		"Label": {
			input:       `label[foo]:"bar"`,
			expectError: false,
		},
		"InvalidField": {
			input:       `foo: "bar"`,
			expectError: true,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			_, err := parser.Parse(tc.input)
			if (err != nil) != tc.expectError {
				t.Errorf("expected error: %v, got: %v", tc.expectError, err)
			}
		})
	}

}
