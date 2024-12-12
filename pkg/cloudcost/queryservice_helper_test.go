package cloudcost

import (
	"reflect"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/filter/cloudcost"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/httputil"
)

func TestParseCloudCostRequest(t *testing.T) {
	windowStr := "2023-01-01T00:00:00Z,2023-01-02T00:00:00Z"
	start := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2023, 1, 2, 0, 0, 0, 0, time.UTC)
	validFilterStr := `service:"AmazonEC2"`
	parser := cloudcost.NewCloudCostFilterParser()
	validFilter, _ := parser.Parse(validFilterStr)
	tests := map[string]struct {
		values  map[string][]string
		want    *QueryRequest
		wantErr bool
	}{
		"missing window": {
			values:  map[string][]string{},
			want:    nil,
			wantErr: true,
		},
		"invalid window": {
			values: map[string][]string{
				"window": {"invalid"},
			},
			want:    nil,
			wantErr: true,
		},
		"valid window": {
			values: map[string][]string{
				"window": {windowStr},
			},
			want: &QueryRequest{
				Start:       start,
				End:         end,
				AggregateBy: []string{opencost.CloudCostInvoiceEntityIDProp, opencost.CloudCostAccountIDProp, opencost.CloudCostProviderProp, opencost.CloudCostProviderIDProp, opencost.CloudCostCategoryProp, opencost.CloudCostServiceProp},
				Accumulate:  "",
				Filter:      nil,
			},
			wantErr: false,
		},
		"valid aggregate": {
			values: map[string][]string{
				"window":    {windowStr},
				"aggregate": {"invoiceEntityID,accountID,label:app"},
			},
			want: &QueryRequest{
				Start:       start,
				End:         end,
				AggregateBy: []string{opencost.CloudCostInvoiceEntityIDProp, opencost.CloudCostAccountIDProp, "label:app"},
				Accumulate:  "",
				Filter:      nil,
			},
			wantErr: false,
		},
		"invalid aggregate": {
			values: map[string][]string{
				"window":    {windowStr},
				"aggregate": {"invalid"},
			},
			want:    nil,
			wantErr: true,
		},
		"valid accumulate": {
			values: map[string][]string{
				"window":     {windowStr},
				"accumulate": {"week"},
			},
			want: &QueryRequest{
				Start:       start,
				End:         end,
				AggregateBy: []string{opencost.CloudCostInvoiceEntityIDProp, opencost.CloudCostAccountIDProp, opencost.CloudCostProviderProp, opencost.CloudCostProviderIDProp, opencost.CloudCostCategoryProp, opencost.CloudCostServiceProp},
				Accumulate:  opencost.AccumulateOptionWeek,
				Filter:      nil,
			},
			wantErr: false,
		},
		"invalid accumulate": {
			values: map[string][]string{
				"window":     {windowStr},
				"accumulate": {"invalid"},
			},
			want: &QueryRequest{
				Start:       start,
				End:         end,
				AggregateBy: []string{opencost.CloudCostInvoiceEntityIDProp, opencost.CloudCostAccountIDProp, opencost.CloudCostProviderProp, opencost.CloudCostProviderIDProp, opencost.CloudCostCategoryProp, opencost.CloudCostServiceProp},
				Accumulate:  opencost.AccumulateOptionNone,
				Filter:      nil,
			},
			wantErr: false,
		},
		"valid filter": {
			values: map[string][]string{
				"window": {windowStr},
				"filter": {validFilterStr},
			},
			want: &QueryRequest{
				Start:       start,
				End:         end,
				AggregateBy: []string{opencost.CloudCostInvoiceEntityIDProp, opencost.CloudCostAccountIDProp, opencost.CloudCostProviderProp, opencost.CloudCostProviderIDProp, opencost.CloudCostCategoryProp, opencost.CloudCostServiceProp},
				Accumulate:  opencost.AccumulateOptionNone,
				Filter:      validFilter,
			},
			wantErr: false,
		},
		"invalid filter": {
			values: map[string][]string{
				"window": {windowStr},
				"filter": {"invalid"},
			},
			want:    nil,
			wantErr: true,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			qp := httputil.NewQueryParams(tt.values)
			got, err := ParseCloudCostRequest(qp)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseCloudCostRequest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseCloudCostRequest() got = %v, want %v", got, tt.want)
			}
		})
	}
}
