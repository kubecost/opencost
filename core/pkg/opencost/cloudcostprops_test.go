package opencost

import "testing"

func TestCloudCostPropertiesIntersection(t *testing.T) {
	testCases := map[string]struct {
		baseCCP     *CloudCostProperties
		intCCP      *CloudCostProperties
		expectedCCP *CloudCostProperties
	}{
		"When properties match between both CloudCostProperties": {
			baseCCP: &CloudCostProperties{
				Provider:          "CustomProvider",
				ProviderID:        "ProviderID1",
				AccountID:         "WorkGroupID1",
				AccountName:       "AccountName1",
				InvoiceEntityID:   "InvoiceEntityID1",
				InvoiceEntityName: "InvoiceEntityName1",
				RegionID:          "RegionID1",
				AvailabilityZone:  "AvailabilityZone1",
				Service:           "Service1",
				Category:          "Category1",
				Labels: map[string]string{
					"key1": "value1",
				},
			},
			intCCP: &CloudCostProperties{
				Provider:          "CustomProvider",
				ProviderID:        "ProviderID1",
				AccountID:         "WorkGroupID1",
				AccountName:       "AccountName1",
				InvoiceEntityID:   "InvoiceEntityID1",
				InvoiceEntityName: "InvoiceEntityName1",
				RegionID:          "RegionID1",
				AvailabilityZone:  "AvailabilityZone1",
				Service:           "Service1",
				Category:          "Category1",
				Labels: map[string]string{
					"key1": "value1",
				},
			},
			expectedCCP: &CloudCostProperties{
				Provider:          "CustomProvider",
				ProviderID:        "ProviderID1",
				AccountID:         "WorkGroupID1",
				AccountName:       "AccountName1",
				InvoiceEntityID:   "InvoiceEntityID1",
				InvoiceEntityName: "InvoiceEntityName1",
				RegionID:          "RegionID1",
				AvailabilityZone:  "AvailabilityZone1",
				Service:           "Service1",
				Category:          "Category1",
				Labels: map[string]string{
					"key1": "value1",
				},
			},
		},
		"When one of the properties differ in the two CloudCostProperties": {
			baseCCP: &CloudCostProperties{
				Provider:          "CustomProvider",
				ProviderID:        "ProviderID1",
				AccountID:         "WorkGroupID1",
				AccountName:       "AccountName1",
				InvoiceEntityID:   "InvoiceEntityID1",
				InvoiceEntityName: "InvoiceEntityName1",
				RegionID:          "RegionID1",
				AvailabilityZone:  "AvailabilityZone1",
				Service:           "Service1",
				Category:          "Category1",
				Labels: map[string]string{
					"key1": "value1",
				},
			},
			intCCP: &CloudCostProperties{
				Provider:          "CustomProvider",
				ProviderID:        "ProviderID1",
				AccountID:         "WorkGroupID1",
				AccountName:       "AccountName1",
				InvoiceEntityID:   "InvoiceEntityID1",
				InvoiceEntityName: "InvoiceEntityName1",
				RegionID:          "RegionID1",
				AvailabilityZone:  "AvailabilityZone1",
				Service:           "Service2",
				Category:          "Category1",
				Labels: map[string]string{
					"key1": "value1",
				},
			},
			expectedCCP: &CloudCostProperties{
				Provider:          "CustomProvider",
				ProviderID:        "ProviderID1",
				AccountID:         "WorkGroupID1",
				AccountName:       "AccountName1",
				InvoiceEntityID:   "InvoiceEntityID1",
				InvoiceEntityName: "InvoiceEntityName1",
				RegionID:          "RegionID1",
				AvailabilityZone:  "AvailabilityZone1",
				Service:           "",
				Category:          "Category1",
				Labels: map[string]string{
					"key1": "value1",
				},
			},
		},
		"When two of the properties differ in the two CloudCostProperties": {
			baseCCP: &CloudCostProperties{
				Provider:          "CustomProvider",
				ProviderID:        "ProviderID1",
				AccountID:         "WorkGroupID1",
				AccountName:       "AccountName1",
				InvoiceEntityID:   "InvoiceEntityID1",
				InvoiceEntityName: "InvoiceEntityName1",
				RegionID:          "RegionID1",
				AvailabilityZone:  "AvailabilityZone1",
				Service:           "Service1",
				Category:          "Category1",
				Labels: map[string]string{
					"key1": "value1",
				},
			},
			intCCP: &CloudCostProperties{
				Provider:          "CustomProvider",
				ProviderID:        "ProviderID1",
				AccountID:         "WorkGroupID2",
				AccountName:       "AccountName1",
				InvoiceEntityID:   "InvoiceEntityID1",
				InvoiceEntityName: "InvoiceEntityName1",
				RegionID:          "RegionID1",
				AvailabilityZone:  "AvailabilityZone1",
				Service:           "Service2",
				Category:          "Category1",
				Labels: map[string]string{
					"key1": "value1",
				},
			},
			expectedCCP: &CloudCostProperties{
				Provider:          "CustomProvider",
				ProviderID:        "ProviderID1",
				AccountID:         "",
				AccountName:       "AccountName1",
				InvoiceEntityID:   "InvoiceEntityID1",
				InvoiceEntityName: "InvoiceEntityName1",
				RegionID:          "RegionID1",
				AvailabilityZone:  "AvailabilityZone1",
				Service:           "",
				Category:          "Category1",
				Labels: map[string]string{
					"key1": "value1",
				},
			},
		},
		"When all properties differ in the two CloudCostProperties": {
			baseCCP: &CloudCostProperties{
				Provider:          "CustomProvider",
				ProviderID:        "ProviderID1",
				AccountID:         "WorkGroupID1",
				AccountName:       "AccountName1",
				InvoiceEntityID:   "InvoiceEntityID1",
				InvoiceEntityName: "InvoiceEntityName1",
				RegionID:          "RegionID1",
				AvailabilityZone:  "AvailabilityZone1",
				Service:           "Service1",
				Category:          "Category1",
				Labels: map[string]string{
					"key1": "value1",
				},
			},
			intCCP: &CloudCostProperties{
				Provider:          "CustomProvider2",
				ProviderID:        "ProviderID2",
				AccountID:         "WorkGroupID2",
				AccountName:       "AccountName2",
				InvoiceEntityID:   "InvoiceEntityID2",
				InvoiceEntityName: "InvoiceEntityName2",
				RegionID:          "RegionID2",
				AvailabilityZone:  "AvailabilityZone2",
				Service:           "Service2",
				Category:          "Category2",
				Labels: map[string]string{
					"key2": "value2",
				},
			},
			expectedCCP: &CloudCostProperties{
				Provider:          "",
				ProviderID:        "",
				AccountID:         "",
				AccountName:       "",
				InvoiceEntityID:   "",
				InvoiceEntityName: "",
				RegionID:          "",
				AvailabilityZone:  "",
				Service:           "",
				Category:          "",
				Labels:            map[string]string{},
			},
		},
		"When labels differ": {
			baseCCP: &CloudCostProperties{
				Provider:          "CustomProvider",
				ProviderID:        "ProviderID1",
				AccountID:         "WorkGroupID1",
				AccountName:       "AccountName1",
				InvoiceEntityID:   "InvoiceEntityID1",
				InvoiceEntityName: "InvoiceEntityName1",
				RegionID:          "RegionID1",
				AvailabilityZone:  "AvailabilityZone1",
				Service:           "Service1",
				Category:          "Category1",
				Labels: map[string]string{
					"key1": "value1",
					"key2": "value2",
					"key3": "value3",
				},
			},
			intCCP: &CloudCostProperties{
				Provider:          "CustomProvider",
				ProviderID:        "ProviderID1",
				AccountID:         "WorkGroupID1",
				AccountName:       "AccountName1",
				InvoiceEntityID:   "InvoiceEntityID1",
				InvoiceEntityName: "InvoiceEntityName1",
				RegionID:          "RegionID1",
				AvailabilityZone:  "AvailabilityZone1",
				Service:           "Service1",
				Category:          "Category1",
				Labels: map[string]string{
					"key1": "value2",
					"key2": "value2",
					"key4": "value4",
				},
			},
			expectedCCP: &CloudCostProperties{
				Provider:          "CustomProvider",
				ProviderID:        "ProviderID1",
				AccountID:         "WorkGroupID1",
				AccountName:       "AccountName1",
				InvoiceEntityID:   "InvoiceEntityID1",
				InvoiceEntityName: "InvoiceEntityName1",
				RegionID:          "RegionID1",
				AvailabilityZone:  "AvailabilityZone1",
				Service:           "Service1",
				Category:          "Category1",
				Labels: map[string]string{
					"key2": "value2",
				},
			},
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			actualCCP := tc.baseCCP.Intersection(tc.intCCP)

			if !actualCCP.Equal(tc.expectedCCP) {
				t.Errorf("Case %s: properties dont match with expected CloudCostProperties: %v actual %v", name, tc.expectedCCP, actualCCP)
			}
		})
	}
}

func TestCloudCostProperties_hashKey(t *testing.T) {

	tests := map[string]struct {
		props *CloudCostProperties
		want  string
	}{
		"enpty props": {
			props: &CloudCostProperties{},
			want:  "cbf29ce484222325",
		},
		"All props no labels": {
			props: &CloudCostProperties{
				ProviderID:        "providerid1",
				Provider:          "provider1",
				AccountID:         "workgroup1",
				AccountName:       "AccountName1",
				InvoiceEntityID:   "billing1",
				InvoiceEntityName: "InvoiceEntityName1",
				RegionID:          "RegionID1",
				AvailabilityZone:  "AvailabilityZone1",
				Service:           "service1",
				Category:          "category1",
				Labels:            map[string]string{},
			},
			want: "d07ffd0bd6d5eaf1",
		},
		"All props": {
			props: &CloudCostProperties{
				ProviderID:        "providerid1",
				Provider:          "provider1",
				AccountID:         "workgroup1",
				AccountName:       "AccountName1",
				InvoiceEntityID:   "billing1",
				InvoiceEntityName: "InvoiceEntityName1",
				RegionID:          "RegionID1",
				AvailabilityZone:  "AvailabilityZone1",
				Service:           "service1",
				Category:          "category1",
				Labels: map[string]string{
					"label1": "value1",
					"label2": "value2",
				},
			},
			want: "318cb6294bf9e2d5",
		},
		"All props swap labels": {
			props: &CloudCostProperties{
				ProviderID:        "providerid1",
				Provider:          "provider1",
				AccountID:         "workgroup1",
				AccountName:       "AccountName1",
				InvoiceEntityID:   "billing1",
				InvoiceEntityName: "InvoiceEntityName1",
				RegionID:          "RegionID1",
				AvailabilityZone:  "AvailabilityZone1",
				Service:           "service1",
				Category:          "category1",
				Labels: map[string]string{
					"label2": "value2",
					"label1": "value1",
				},
			},
			want: "318cb6294bf9e2d5",
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if got := tt.props.hashKey(); got != tt.want {
				t.Errorf("hashKey() = %v, want %v", got, tt.want)
			}
		})
	}
}
