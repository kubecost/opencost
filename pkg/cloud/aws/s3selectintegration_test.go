package aws

import (
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
)

func TestS3Integration_GetCloudCost(t *testing.T) {
	s3ConfigPath := os.Getenv("S3_CONFIGURATION")
	if s3ConfigPath == "" {
		t.Skip("skipping integration test, set environment variable S3_CONFIGURATION")
	}
	s3ConfigBin, err := os.ReadFile(s3ConfigPath)
	if err != nil {
		t.Fatalf("failed to read config file: %s", err.Error())
	}
	var s3Config S3Configuration
	err = json.Unmarshal(s3ConfigBin, &s3Config)
	if err != nil {
		t.Fatalf("failed to unmarshal config from JSON: %s", err.Error())
	}
	testCases := map[string]struct {
		integration *S3SelectIntegration
		start       time.Time
		end         time.Time
		expected    bool
	}{
		// No CUR data is expected within 2 days of now
		"too_recent_window": {
			integration: &S3SelectIntegration{
				S3SelectQuerier: S3SelectQuerier{
					S3Connection: S3Connection{
						S3Configuration: s3Config,
					},
				},
			},
			end:      time.Now(),
			start:    time.Now().Add(-timeutil.Day),
			expected: true,
		},
		// CUR data should be available
		"last week window": {
			integration: &S3SelectIntegration{
				S3SelectQuerier: S3SelectQuerier{
					S3Connection: S3Connection{
						S3Configuration: s3Config,
					},
				},
			},
			end:      time.Now().Add(-7 * timeutil.Day),
			start:    time.Now().Add(-8 * timeutil.Day),
			expected: false,
		},
	}
	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			actual, err := testCase.integration.GetCloudCost(testCase.start, testCase.end)
			if err != nil {
				t.Errorf("Other error during testing %s", err)
			} else if actual.IsEmpty() != testCase.expected {
				t.Errorf("Incorrect result, actual emptiness: %t, expected: %t", actual.IsEmpty(), testCase.expected)
			}
		})
	}
}

func Test_s3RowToCloudCost(t *testing.T) {
	columnIndexes := map[string]int{
		S3SelectListCost:                         0,
		S3SelectNetCost:                          1,
		S3SelectRICost:                           2,
		S3SelectNetRICost:                        3,
		S3SelectSPCost:                           4,
		S3SelectNetSPCost:                        5,
		S3SelectStartDate:                        6,
		S3SelectBillPayerAccountID:               7,
		S3SelectAccountID:                        8,
		S3SelectResourceID:                       9,
		S3SelectItemType:                         10,
		S3SelectProductCode:                      11,
		S3SelectUsageType:                        12,
		S3SelectRegionCode:                       13,
		S3SelectAvailabilityZone:                 14,
		`s."resourceTags/user:test"`:             15,
		`s."resourceTags/aws:test"`:              16,
		`s."resourceTags/user:eks:cluster-name"`: 17,
	}

	userTagColumns := []string{`s."resourceTags/user:test"`, `s."resourceTags/user:eks:cluster-name"`}
	awsTagColumns := []string{`s."resourceTags/aws:test"`}

	tests := []struct {
		name           string
		row            []string
		columnIndexes  map[string]int
		userTagColumns []string
		awsTagColumns  []string
		want           *opencost.CloudCost
		wantErr        bool
	}{
		{
			name:           "invalid list cost",
			row:            []string{"invalid", "2", "3", "4", "5", "6", "2024-09-01T00:00:00Z", "payerAccountID", "usageAccountID", "resourceID", "itemType", "productCode", "usageType", "regionCode", "availabilityZone", "", "", ""},
			columnIndexes:  columnIndexes,
			userTagColumns: userTagColumns,
			awsTagColumns:  awsTagColumns,
			want:           nil,
			wantErr:        true,
		},
		{
			name:           "invalid net cost",
			row:            []string{"1", "invalid", "3", "4", "5", "6", "2024-09-01T00:00:00Z", "payerAccountID", "usageAccountID", "resourceID", "itemType", "productCode", "usageType", "regionCode", "availabilityZone", "", "", ""},
			columnIndexes:  columnIndexes,
			userTagColumns: userTagColumns,
			awsTagColumns:  awsTagColumns,
			want:           nil,
			wantErr:        true,
		},
		{
			name:           "invalid RI cost",
			row:            []string{"1", "2", "invalid", "4", "5", "6", "2024-09-01T00:00:00Z", "payerAccountID", "usageAccountID", "resourceID", TypeDiscountedUsage, "productCode", "usageType", "regionCode", "availabilityZone", "", "", ""},
			columnIndexes:  columnIndexes,
			userTagColumns: userTagColumns,
			awsTagColumns:  awsTagColumns,
			want:           nil,
			wantErr:        true,
		},
		{
			name:           "invalid net RI cost",
			row:            []string{"1", "2", "3", "invalid", "5", "6", "2024-09-01T00:00:00Z", "payerAccountID", "usageAccountID", "resourceID", TypeDiscountedUsage, "productCode", "usageType", "regionCode", "availabilityZone", "", "", ""},
			columnIndexes:  columnIndexes,
			userTagColumns: userTagColumns,
			awsTagColumns:  awsTagColumns,
			want:           nil,
			wantErr:        true,
		},
		{
			name:           "invalid SP cost",
			row:            []string{"1", "2", "3", "4", "invalid", "6", "2024-09-01T00:00:00Z", "payerAccountID", "usageAccountID", "resourceID", TypeSavingsPlanCoveredUsage, "productCode", "usageType", "regionCode", "availabilityZone", "", "", ""},
			columnIndexes:  columnIndexes,
			userTagColumns: userTagColumns,
			awsTagColumns:  awsTagColumns,
			want:           nil,
			wantErr:        true,
		},
		{
			name:           "invalid net SP cost",
			row:            []string{"1", "2", "3", "4", "5", "invalid", "2024-09-01T00:00:00Z", "payerAccountID", "usageAccountID", "resourceID", TypeSavingsPlanCoveredUsage, "productCode", "usageType", "regionCode", "availabilityZone", "", "", ""},
			columnIndexes:  columnIndexes,
			userTagColumns: userTagColumns,
			awsTagColumns:  awsTagColumns,
			want:           nil,
			wantErr:        true,
		},
		{
			name:           "invalid date",
			row:            []string{"1", "2", "3", "4", "5", "6", "invalid", "payerAccountID", "usageAccountID", "resourceID", "itemType", "productCode", "usageType", "regionCode", "availabilityZone", "", "", ""},
			columnIndexes:  columnIndexes,
			userTagColumns: userTagColumns,
			awsTagColumns:  awsTagColumns,
			want:           nil,
			wantErr:        true,
		},
		{
			name:           "valid empty labels",
			row:            []string{"1", "2", "3", "4", "5", "6", "2024-09-01T00:00:00Z", "payerAccountID", "usageAccountID", "resourceID", "itemType", "productCode", "usageType", "regionCode", "availabilityZone", "", "", ""},
			columnIndexes:  columnIndexes,
			userTagColumns: userTagColumns,
			awsTagColumns:  awsTagColumns,
			want: &opencost.CloudCost{
				Properties: &opencost.CloudCostProperties{
					ProviderID:        "resourceID",
					Provider:          "AWS",
					AccountID:         "usageAccountID",
					AccountName:       "usageAccountID",
					InvoiceEntityID:   "payerAccountID",
					InvoiceEntityName: "payerAccountID",
					RegionID:          "regionCode",
					AvailabilityZone:  "availabilityZone",
					Service:           "productCode",
					Category:          opencost.OtherCategory,
					Labels:            opencost.CloudCostLabels{},
				},
				Window: opencost.NewClosedWindow(
					time.Date(2024, 9, 1, 0, 0, 0, 0, time.UTC),
					time.Date(2024, 9, 2, 0, 0, 0, 0, time.UTC),
				),
				ListCost: opencost.CostMetric{
					Cost:              1,
					KubernetesPercent: 0,
				},
				NetCost: opencost.CostMetric{
					Cost:              2,
					KubernetesPercent: 0,
				},
				AmortizedNetCost: opencost.CostMetric{
					Cost:              1,
					KubernetesPercent: 0,
				},
				InvoicedCost: opencost.CostMetric{
					Cost:              2,
					KubernetesPercent: 0,
				},
				AmortizedCost: opencost.CostMetric{
					Cost:              1,
					KubernetesPercent: 0,
				},
			},
			wantErr: false,
		},
		{
			name:           "valid Kubernetes RI with labels",
			row:            []string{"1", "2", "3", "4", "5", "6", "2024-09-01T00:00:00Z", "payerAccountID", "usageAccountID", "resourceID", TypeDiscountedUsage, "productCode", "usageType", "regionCode", "availabilityZone", "userTagTestValue", "awsTagTestValue", "clusterName"},
			columnIndexes:  columnIndexes,
			userTagColumns: userTagColumns,
			awsTagColumns:  awsTagColumns,
			want: &opencost.CloudCost{
				Properties: &opencost.CloudCostProperties{
					ProviderID:        "resourceID",
					Provider:          "AWS",
					AccountID:         "usageAccountID",
					AccountName:       "usageAccountID",
					InvoiceEntityID:   "payerAccountID",
					InvoiceEntityName: "payerAccountID",
					RegionID:          "regionCode",
					AvailabilityZone:  "availabilityZone",
					Service:           "productCode",
					Category:          opencost.OtherCategory,
					Labels: opencost.CloudCostLabels{
						"test":             "userTagTestValue",
						"eks:cluster-name": "clusterName",
						"aws:test":         "awsTagTestValue",
					},
				},
				Window: opencost.NewClosedWindow(
					time.Date(2024, 9, 1, 0, 0, 0, 0, time.UTC),
					time.Date(2024, 9, 2, 0, 0, 0, 0, time.UTC),
				),
				ListCost: opencost.CostMetric{
					Cost:              1,
					KubernetesPercent: 1,
				},
				NetCost: opencost.CostMetric{
					Cost:              2,
					KubernetesPercent: 1,
				},
				AmortizedNetCost: opencost.CostMetric{
					Cost:              4,
					KubernetesPercent: 1,
				},
				InvoicedCost: opencost.CostMetric{
					Cost:              2,
					KubernetesPercent: 1,
				},
				AmortizedCost: opencost.CostMetric{
					Cost:              3,
					KubernetesPercent: 1,
				},
			},
			wantErr: false,
		},
		{
			name:           "valid Kubernetes SP no labels",
			row:            []string{"1", "2", "3", "4", "5", "6", "2024-09-01T00:00:00Z", "payerAccountID", "usageAccountID", "resourceID", TypeSavingsPlanCoveredUsage, "AmazonEKS", "usageType", "regionCode", "availabilityZone", "", "", ""},
			columnIndexes:  columnIndexes,
			userTagColumns: userTagColumns,
			awsTagColumns:  awsTagColumns,
			want: &opencost.CloudCost{
				Properties: &opencost.CloudCostProperties{
					ProviderID:        "resourceID",
					Provider:          "AWS",
					AccountID:         "usageAccountID",
					AccountName:       "usageAccountID",
					InvoiceEntityID:   "payerAccountID",
					InvoiceEntityName: "payerAccountID",
					RegionID:          "regionCode",
					AvailabilityZone:  "availabilityZone",
					Service:           "AmazonEKS",
					Category:          opencost.ManagementCategory,
					Labels:            opencost.CloudCostLabels{},
				},
				Window: opencost.NewClosedWindow(
					time.Date(2024, 9, 1, 0, 0, 0, 0, time.UTC),
					time.Date(2024, 9, 2, 0, 0, 0, 0, time.UTC),
				),
				ListCost: opencost.CostMetric{
					Cost:              1,
					KubernetesPercent: 1,
				},
				NetCost: opencost.CostMetric{
					Cost:              2,
					KubernetesPercent: 1,
				},
				AmortizedNetCost: opencost.CostMetric{
					Cost:              6,
					KubernetesPercent: 1,
				},
				InvoicedCost: opencost.CostMetric{
					Cost:              2,
					KubernetesPercent: 1,
				},
				AmortizedCost: opencost.CostMetric{
					Cost:              5,
					KubernetesPercent: 1,
				},
			},
			wantErr: false,
		},
		{
			name:           "valid Kubernetes load balancer product code",
			row:            []string{"1", "2", "3", "4", "5", "6", "2024-09-01T00:00:00Z", "payerAccountID", "usageAccountID", "resourceID/lbID", TypeSavingsPlanCoveredUsage, "AWSELB", "usageType", "regionCode", "availabilityZone", "", "", ""},
			columnIndexes:  columnIndexes,
			userTagColumns: userTagColumns,
			awsTagColumns:  awsTagColumns,
			want: &opencost.CloudCost{
				Properties: &opencost.CloudCostProperties{
					ProviderID:        "lbID",
					Provider:          "AWS",
					AccountID:         "usageAccountID",
					AccountName:       "usageAccountID",
					InvoiceEntityID:   "payerAccountID",
					InvoiceEntityName: "payerAccountID",
					RegionID:          "regionCode",
					AvailabilityZone:  "availabilityZone",
					Service:           "AWSELB",
					Category:          opencost.NetworkCategory,
					Labels:            opencost.CloudCostLabels{},
				},
				Window: opencost.NewClosedWindow(
					time.Date(2024, 9, 1, 0, 0, 0, 0, time.UTC),
					time.Date(2024, 9, 2, 0, 0, 0, 0, time.UTC),
				),
				ListCost: opencost.CostMetric{
					Cost:              1,
					KubernetesPercent: 0,
				},
				NetCost: opencost.CostMetric{
					Cost:              2,
					KubernetesPercent: 0,
				},
				AmortizedNetCost: opencost.CostMetric{
					Cost:              6,
					KubernetesPercent: 0,
				},
				InvoicedCost: opencost.CostMetric{
					Cost:              2,
					KubernetesPercent: 0,
				},
				AmortizedCost: opencost.CostMetric{
					Cost:              5,
					KubernetesPercent: 0,
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := s3RowToCloudCost(tt.row, tt.columnIndexes, tt.userTagColumns, tt.awsTagColumns)
			if (err != nil) != tt.wantErr {
				t.Errorf("s3RowToCloudCost() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("s3RowToCloudCost() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_hasK8sLabel(t *testing.T) {
	tests := []struct {
		name   string
		labels opencost.CloudCostLabels
		want   bool
	}{
		{
			name:   "empty",
			labels: opencost.CloudCostLabels{},
			want:   false,
		},
		{
			name: "no k8s label",
			labels: opencost.CloudCostLabels{
				"key": "value",
			},
			want: false,
		},
		{
			name: "aws eks cluster name",
			labels: opencost.CloudCostLabels{
				TagAWSEKSClusterName: "value",
			},
			want: true,
		},
		{
			name: "eks cluster name",
			labels: opencost.CloudCostLabels{
				TagEKSClusterName: "value",
			},
			want: true,
		},
		{
			name: "eks ctl cluster name",
			labels: opencost.CloudCostLabels{
				TagEKSCtlClusterName: "value",
			},
			want: true,
		},
		{
			name: "kubernetes service name",
			labels: opencost.CloudCostLabels{
				TagKubernetesServiceName: "value",
			},
			want: true,
		},
		{
			name: "kubernetes pvc name",
			labels: opencost.CloudCostLabels{
				TagKubernetesPVCName: "value",
			},
			want: true,
		},
		{
			name: "kubernetes pv name",
			labels: opencost.CloudCostLabels{
				TagKubernetesPVName: "value",
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := hasK8sLabel(tt.labels); got != tt.want {
				t.Errorf("hasK8sLabel() = %v, want %v", got, tt.want)
			}
		})
	}
}
