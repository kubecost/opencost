package cloudcost

import (
	"testing"
)

type CloudCostFilterEqualsTestcase struct {
	name     string
	this     CloudCostFilter
	that     CloudCostFilter
	expected bool
}

func TestCloudCostFilter_Equals(t *testing.T) {
	testCases := []CloudCostFilterEqualsTestcase{
		{
			name: "both filters nil",
			this: CloudCostFilter{
				AccountIDs:       nil,
				Categories:       nil,
				InvoiceEntityIDs: nil,
				Labels:           nil,
				Providers:        nil,
				ProviderIDs:      nil,
				Services:         nil,
			},
			that: CloudCostFilter{
				AccountIDs:       nil,
				Categories:       nil,
				InvoiceEntityIDs: nil,
				Labels:           nil,
				Providers:        nil,
				ProviderIDs:      nil,
				Services:         nil,
			},
			expected: true,
		},
		{
			name: "both filters not nil and matching",
			this: CloudCostFilter{
				AccountIDs:       []string{"account1", "account2", "account3"},
				Categories:       []string{"category1", "category2"},
				InvoiceEntityIDs: []string{"invoice1", "invoice2"},
				Labels:           []string{"label1", "label2"},
				Providers:        []string{"provider1", "provider2"},
				ProviderIDs:      []string{"provider1", "provider2"},
				Services:         []string{"s1"},
			},
			that: CloudCostFilter{
				AccountIDs:       []string{"account1", "account2", "account3"},
				Categories:       []string{"category1", "category2"},
				InvoiceEntityIDs: []string{"invoice1", "invoice2"},
				Labels:           []string{"label1", "label2"},
				Providers:        []string{"provider1", "provider2"},
				ProviderIDs:      []string{"provider1", "provider2"},
				Services:         []string{"s1"},
			},
			expected: true,
		},
		{
			name: "both filters diff count",
			this: CloudCostFilter{
				AccountIDs:       []string{"account1", "account2", "account3"},
				Categories:       []string{"category1", "category2"},
				InvoiceEntityIDs: []string{"invoice1", "invoice2"},
				Labels:           []string{"label1", "label2"},
				Providers:        []string{"provider1", "provider2"},
				ProviderIDs:      []string{"provider1", "provider2"},
				Services:         []string{"s1"},
			},
			that: CloudCostFilter{
				AccountIDs:       []string{"account1", "account2"},
				Categories:       []string{"category1", "category2"},
				InvoiceEntityIDs: []string{"invoice1", "invoice2"},
				Labels:           []string{"label1", "label2"},
				Providers:        []string{"provider1", "provider2"},
				ProviderIDs:      []string{"provider1", "provider2"},
				Services:         []string{"s1"},
			},
			expected: false,
		},
		{
			name: "slight mismatch",
			this: CloudCostFilter{
				AccountIDs:       []string{"account1", "account2", "account3"},
				Categories:       []string{"category1", "category2"},
				InvoiceEntityIDs: []string{"invoice1", "invoice2"},
				Labels:           []string{"label1", "label2"},
				Providers:        []string{"provider1", "provider2"},
				ProviderIDs:      []string{"provider1", "provider2"},
				Services:         []string{"s1"},
			},
			that: CloudCostFilter{
				AccountIDs:       []string{"account10", "account2", "account3"},
				Categories:       []string{"category1", "category2"},
				InvoiceEntityIDs: []string{"invoice1", "invoice2"},
				Labels:           []string{"label1", "label2"},
				Providers:        []string{"provider1", "provider2"},
				ProviderIDs:      []string{"provider1", "provider2"},
				Services:         []string{"s1"},
			},
			expected: false,
		},
		{
			name: "one nil, one not",
			this: CloudCostFilter{
				AccountIDs:       []string{"account1", "account2", "account3"},
				Categories:       []string{"category1", "category2"},
				InvoiceEntityIDs: []string{"invoice1", "invoice2"},
				Labels:           []string{"label1", "label2"},
				Providers:        []string{"provider1", "provider2"},
				ProviderIDs:      []string{"provider1", "provider2"},
				Services:         []string{"s1"},
			},
			that: CloudCostFilter{
				AccountIDs:       nil,
				Categories:       nil,
				InvoiceEntityIDs: nil,
				Labels:           nil,
				Providers:        nil,
				ProviderIDs:      nil,
				Services:         nil,
			},
			expected: false,
		},
	}

	for _, tc := range testCases {
		got := tc.this.Equals(tc.that)
		if got != tc.expected {
			t.Fatalf("expected %t, got: %t for test case: %s", tc.expected, got, tc.name)
		}
	}
}
