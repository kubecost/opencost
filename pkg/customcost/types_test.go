package customcost

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/model/pb"
)

func TestSortByCostAsc(t *testing.T) {
	ccs := &CustomCostSet{
		CustomCosts: []*CustomCost{
			{Cost: 300.0},
			{Cost: 100.0},
			{Cost: 200.0},
		},
	}

	ccs.Sort(SortPropertyCost, SortDirectionAsc)

	expected := []float32{100.0, 200.0, 300.0}
	for i, cc := range ccs.CustomCosts {
		if cc.Cost != expected[i] {
			t.Errorf("expected cost %f, got %f", expected[i], cc.Cost)
		}
	}
}

func TestSortByCostDesc(t *testing.T) {
	ccs := &CustomCostSet{
		CustomCosts: []*CustomCost{
			{Cost: 300.0},
			{Cost: 100.0},
			{Cost: 200.0},
		},
	}

	ccs.Sort(SortPropertyCost, SortDirectionDesc)

	expected := []float32{300.0, 200.0, 100.0}
	for i, cc := range ccs.CustomCosts {
		if cc.Cost != expected[i] {
			t.Errorf("expected cost %f, got %f", expected[i], cc.Cost)
		}
	}
}

func TestSortByAggregateAsc(t *testing.T) {
	ccs := &CustomCostSet{
		CustomCosts: []*CustomCost{
			{Aggregate: "c"},
			{Aggregate: "a"},
			{Aggregate: "b"},
		},
	}

	ccs.Sort(SortPropertyAggregate, SortDirectionAsc)

	expected := []string{"a", "b", "c"}
	for i, cc := range ccs.CustomCosts {
		if cc.Aggregate != expected[i] {
			t.Errorf("expected aggregate %s, got %s", expected[i], cc.Aggregate)
		}
	}
}

func TestSortByAggregateDesc(t *testing.T) {
	ccs := &CustomCostSet{
		CustomCosts: []*CustomCost{
			{Aggregate: "c"},
			{Aggregate: "a"},
			{Aggregate: "b"},
		},
	}

	ccs.Sort(SortPropertyAggregate, SortDirectionDesc)

	expected := []string{"c", "b", "a"}
	for i, cc := range ccs.CustomCosts {
		if cc.Aggregate != expected[i] {
			t.Errorf("expected aggregate %s, got %s", expected[i], cc.Aggregate)
		}
	}
}

func TestSortByCostTypeAsc(t *testing.T) {
	ccs := &CustomCostSet{
		CustomCosts: []*CustomCost{
			{CostType: CostTypeBilled},
			{CostType: CostTypeList},
			{CostType: CostTypeBlended},
		},
	}

	ccs.Sort(SortPropertyCostType, SortDirectionAsc)

	expected := []CostType{CostTypeBilled, CostTypeBlended, CostTypeList}
	for i, cc := range ccs.CustomCosts {
		if cc.CostType != expected[i] {
			t.Errorf("expected cost type %s, got %s", expected[i], cc.CostType)
		}
	}
}

func TestSortByCostTypeDesc(t *testing.T) {
	ccs := &CustomCostSet{
		CustomCosts: []*CustomCost{
			{CostType: CostTypeBilled},
			{CostType: CostTypeList},
			{CostType: CostTypeBlended},
		},
	}

	ccs.Sort(SortPropertyCostType, SortDirectionDesc)

	expected := []CostType{CostTypeList, CostTypeBlended, CostTypeBilled}
	for i, cc := range ccs.CustomCosts {
		if cc.CostType != expected[i] {
			t.Errorf("expected cost type %s, got %s", expected[i], cc.CostType)
		}
	}
}

func TestParseCustomCostResponse(t *testing.T) {
	tests := []struct {
		name       string
		ccResponse *pb.CustomCostResponse
		costType   CostType
		expected   []*CustomCost
	}{
		{
			name: "BlendedCost",
			ccResponse: &pb.CustomCostResponse{
				Costs: []*pb.CustomCost{
					{
						Id:             "1",
						Zone:           "us-east-1a",
						AccountName:    "account1",
						ChargeCategory: "category1",
						Description:    "description1",
						ResourceName:   "resource1",
						ResourceType:   "type1",
						ProviderId:     "provider1",
						BilledCost:     100.0,
						ListCost:       120.0,
						ListUnitPrice:  1.2,
						UsageQuantity:  100,
						UsageUnit:      "unit1",
					},
				},
				Domain:     "domain1",
				CostSource: "source1",
			},
			costType: CostTypeBlended,
			expected: []*CustomCost{
				{
					Id:             "1",
					Zone:           "us-east-1a",
					AccountName:    "account1",
					ChargeCategory: "category1",
					Description:    "description1",
					ResourceName:   "resource1",
					ResourceType:   "type1",
					ProviderId:     "provider1",
					Cost:           100.0,
					ListUnitPrice:  1.2,
					UsageQuantity:  100,
					UsageUnit:      "unit1",
					Domain:         "domain1",
					CostSource:     "source1",
					CostType:       CostTypeBilled,
				},
			},
		},
		{
			name: "ListCost",
			ccResponse: &pb.CustomCostResponse{
				Costs: []*pb.CustomCost{
					{
						Id:             "2",
						Zone:           "us-west-2b",
						AccountName:    "account2",
						ChargeCategory: "category2",
						Description:    "description2",
						ResourceName:   "resource2",
						ResourceType:   "type2",
						ProviderId:     "provider2",
						BilledCost:     0.0,
						ListCost:       150.0,
						ListUnitPrice:  1.5,
						UsageQuantity:  100,
						UsageUnit:      "unit2",
					},
				},
				Domain:     "domain2",
				CostSource: "source2",
			},
			costType: CostTypeList,
			expected: []*CustomCost{
				{
					Id:             "2",
					Zone:           "us-west-2b",
					AccountName:    "account2",
					ChargeCategory: "category2",
					Description:    "description2",
					ResourceName:   "resource2",
					ResourceType:   "type2",
					ProviderId:     "provider2",
					Cost:           150.0,
					ListUnitPrice:  1.5,
					UsageQuantity:  100,
					UsageUnit:      "unit2",
					Domain:         "domain2",
					CostSource:     "source2",
					CostType:       CostTypeList,
				},
			},
		},
		{
			name: "ZeroCost",
			ccResponse: &pb.CustomCostResponse{
				Costs: []*pb.CustomCost{
					{
						Id:             "3",
						Zone:           "us-central-1c",
						AccountName:    "account3",
						ChargeCategory: "category3",
						Description:    "description3",
						ResourceName:   "resource3",
						ResourceType:   "type3",
						ProviderId:     "provider3",
						BilledCost:     0.0,
						ListCost:       0.0,
						ListUnitPrice:  0.0,
						UsageQuantity:  0,
						UsageUnit:      "unit3",
					},
				},
				Domain:     "domain3",
				CostSource: "source3",
			},
			costType: CostTypeBlended,
			expected: []*CustomCost{},
		},
		{
			name: "Non Matching cost",
			ccResponse: &pb.CustomCostResponse{
				Costs: []*pb.CustomCost{
					{
						Id:             "3",
						Zone:           "us-central-1c",
						AccountName:    "account3",
						ChargeCategory: "category3",
						Description:    "description3",
						ResourceName:   "resource3",
						ResourceType:   "type3",
						ProviderId:     "provider3",
						BilledCost:     0.0,
						ListCost:       1.0,
						ListUnitPrice:  0.0,
						UsageQuantity:  0,
						UsageUnit:      "unit3",
					},
				},
				Domain:     "domain3",
				CostSource: "source3",
			},
			costType: CostTypeBilled,
			expected: []*CustomCost{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ParseCustomCostResponse(tt.ccResponse, tt.costType)
			if len(result) != len(tt.expected) {
				t.Errorf("expected %d custom costs, got %d", len(tt.expected), len(result))
			}
			for i, expectedCost := range tt.expected {
				if result[i].Id != expectedCost.Id ||
					result[i].Zone != expectedCost.Zone ||
					result[i].AccountName != expectedCost.AccountName ||
					result[i].ChargeCategory != expectedCost.ChargeCategory ||
					result[i].Description != expectedCost.Description ||
					result[i].ResourceName != expectedCost.ResourceName ||
					result[i].ResourceType != expectedCost.ResourceType ||
					result[i].ProviderId != expectedCost.ProviderId ||
					result[i].Cost != expectedCost.Cost ||
					result[i].ListUnitPrice != expectedCost.ListUnitPrice ||
					result[i].UsageQuantity != expectedCost.UsageQuantity ||
					result[i].UsageUnit != expectedCost.UsageUnit ||
					result[i].Domain != expectedCost.Domain ||
					result[i].CostSource != expectedCost.CostSource ||
					result[i].CostType != expectedCost.CostType {
					t.Errorf("expected %+v, got %+v", expectedCost, result[i])
				}
			}
		})
	}
}
