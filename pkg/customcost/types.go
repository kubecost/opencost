package customcost

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/filter"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/model/pb"
	"github.com/opencost/opencost/core/pkg/opencost"
)

type CostType string
type SortProperty string
type SortDirection string

const (
	CostTypeBlended CostType = "blended"
	CostTypeList    CostType = "list"
	CostTypeBilled  CostType = "billed"

	SortPropertyCost      SortProperty = "cost"
	SortPropertyAggregate SortProperty = "aggregate"
	SortPropertyCostType  SortProperty = "costType"

	SortDirectionAsc  SortDirection = "asc"
	SortDirectionDesc SortDirection = "desc"
)

type CostTotalRequest struct {
	Start         time.Time
	End           time.Time
	AggregateBy   []CustomCostProperty
	Accumulate    opencost.AccumulateOption
	Filter        filter.Filter
	CostType      CostType
	SortBy        SortProperty
	SortDirection SortDirection
}

type CostTimeseriesRequest struct {
	Start         time.Time
	End           time.Time
	AggregateBy   []CustomCostProperty
	Accumulate    opencost.AccumulateOption
	Filter        filter.Filter
	CostType      CostType
	SortBy        SortProperty
	SortDirection SortDirection
}

type CostResponse struct {
	Window        opencost.Window `json:"window"`
	TotalCost     float32         `json:"totalCost"`
	TotalCostType CostType        `json:"totalCostType"`
	CustomCosts   []*CustomCost   `json:"customCosts"`
}

type CustomCost struct {
	Id             string   `json:"id"`
	Zone           string   `json:"zone"`
	AccountName    string   `json:"account_name"`
	ChargeCategory string   `json:"charge_category"`
	Description    string   `json:"description"`
	ResourceName   string   `json:"resource_name"`
	ResourceType   string   `json:"resource_type"`
	ProviderId     string   `json:"provider_id"`
	Cost           float32  `json:"cost"`
	ListUnitPrice  float32  `json:"list_unit_price"`
	UsageQuantity  float32  `json:"usage_quantity"`
	UsageUnit      string   `json:"usage_unit"`
	Domain         string   `json:"domain"`
	CostSource     string   `json:"cost_source"`
	Aggregate      string   `json:"aggregate"`
	CostType       CostType `json:"cost_type"`
}

type CostTimeseriesResponse struct {
	Window     opencost.Window `json:"window"`
	Timeseries []*CostResponse `json:"timeseries"`
}

func NewCostResponse(ccs *CustomCostSet, costType CostType) *CostResponse {
	costResponse := &CostResponse{
		Window:        ccs.Window,
		CustomCosts:   []*CustomCost{},
		TotalCostType: costType,
	}

	for _, cc := range ccs.CustomCosts {
		costResponse.TotalCost += cc.Cost
		costResponse.CustomCosts = append(costResponse.CustomCosts, cc)
	}

	return costResponse
}

func ParseCostType(costTypeStr string) (CostType, error) {
	switch costTypeStr {
	case string(CostTypeBlended):
		return CostTypeBlended, nil
	case string(CostTypeList):
		return CostTypeList, nil
	case string(CostTypeBilled):
		return CostTypeBilled, nil
	default:
		return "", fmt.Errorf("unsupported cost type: %s", costTypeStr)
	}
}

func ParseCustomCostResponse(ccResponse *pb.CustomCostResponse, costType CostType) []*CustomCost {
	costs := ccResponse.GetCosts()

	customCosts := []*CustomCost{}
	for _, cost := range costs {
		selectedCost, selectedCostType := determineCost(cost, costType)
		if selectedCost == 0 {
			log.Debugf("cost %s had 0 cost for cost type %s, not including in response", cost.ProviderId, costType)
			continue
		}
		customCosts = append(customCosts, &CustomCost{
			Id:             cost.GetId(),
			Zone:           cost.GetZone(),
			AccountName:    cost.GetAccountName(),
			ChargeCategory: cost.GetChargeCategory(),
			Description:    cost.GetDescription(),
			ResourceName:   cost.GetResourceName(),
			ResourceType:   cost.GetResourceType(),
			ProviderId:     cost.GetProviderId(),
			Cost:           selectedCost,
			ListUnitPrice:  cost.GetListUnitPrice(),
			UsageQuantity:  cost.GetUsageQuantity(),
			UsageUnit:      cost.GetUsageUnit(),
			Domain:         ccResponse.GetDomain(),
			CostSource:     ccResponse.GetCostSource(),
			CostType:       selectedCostType,
		})
	}

	return customCosts
}

func determineCost(cc *pb.CustomCost, costType CostType) (float32, CostType) {
	switch costType {
	// if the cost type is blended, first check if the billed cost is non-zero
	// if it is, return the billed cost
	// if it is zero, return the list cost
	case CostTypeBlended:
		if cc.BilledCost > 0 {
			return cc.BilledCost, CostTypeBilled
		}
		return cc.ListCost, CostTypeList
		// if the cost type is list, return the list cost
	case CostTypeList:
		return cc.ListCost, CostTypeList
		// if the cost type is billed, return the billed cost
	case CostTypeBilled:
		return cc.BilledCost, CostTypeBilled
	default:
		return 0, ""
	}
}
func (cc *CustomCost) Add(other *CustomCost) {
	cc.Cost += other.Cost
	cc.ListUnitPrice += other.ListUnitPrice

	if cc.CostType != other.CostType {
		cc.CostType = CostTypeBlended
	}

	if cc.Id != other.Id {
		cc.Id = ""
	}

	if cc.Zone != other.Zone {
		cc.Zone = ""
	}

	if cc.AccountName != other.AccountName {
		cc.AccountName = ""
	}

	if cc.ChargeCategory != other.ChargeCategory {
		cc.ChargeCategory = ""
	}

	if cc.Description != other.Description {
		cc.Description = ""
	}

	if cc.ResourceName != other.ResourceName {
		cc.ResourceName = ""
	}

	if cc.ResourceType != other.ResourceType {
		cc.ResourceType = ""
	}

	if cc.ProviderId != other.ProviderId {
		cc.ProviderId = ""
	}

	if cc.UsageUnit != other.UsageUnit {
		cc.UsageUnit = ""
	} else {
		// when usage units are the same, then we can sum the usages
		cc.UsageQuantity += other.UsageQuantity
	}

	if cc.Domain != other.Domain {
		cc.Domain = ""
	}

	if cc.CostSource != other.CostSource {
		cc.CostSource = ""
	}

	if cc.Aggregate != other.Aggregate {
		cc.Aggregate = ""
	}

}

type CustomCostSet struct {
	CustomCosts []*CustomCost
	Window      opencost.Window
}

func NewCustomCostSet(window opencost.Window) *CustomCostSet {
	return &CustomCostSet{
		CustomCosts: []*CustomCost{},
		Window:      window,
	}
}

func (ccs *CustomCostSet) Add(customCost *CustomCost) {
	ccs.CustomCosts = append(ccs.CustomCosts, customCost)
}

func (ccs *CustomCostSet) Aggregate(aggregateBy []CustomCostProperty) error {
	// when no aggregation, return the original CustomCostSet
	if len(aggregateBy) == 0 {
		return nil
	}

	aggMap := make(map[string]*CustomCost)
	for _, cc := range ccs.CustomCosts {
		aggKey, err := generateAggKey(cc, aggregateBy)
		if err != nil {
			return fmt.Errorf("failed to aggregate CustomCostSet: %w", err)
		}
		cc.Aggregate = aggKey

		if existing, ok := aggMap[aggKey]; ok {
			existing.Add(cc)
		} else {
			aggMap[aggKey] = cc
		}
	}

	var newCustomCosts []*CustomCost
	for _, customCost := range aggMap {
		newCustomCosts = append(newCustomCosts, customCost)
	}
	ccs.CustomCosts = newCustomCosts

	return nil
}

func generateAggKey(cc *CustomCost, aggregateBy []CustomCostProperty) (string, error) {
	var aggKeys []string
	for _, agg := range aggregateBy {
		var aggKey string
		if agg == CustomCostZoneProp {
			aggKey = cc.Zone
		} else if agg == CustomCostAccountNameProp {
			aggKey = cc.AccountName
		} else if agg == CustomCostChargeCategoryProp {
			aggKey = cc.ChargeCategory
		} else if agg == CustomCostDescriptionProp {
			aggKey = cc.Description
		} else if agg == CustomCostResourceNameProp {
			aggKey = cc.ResourceName
		} else if agg == CustomCostResourceTypeProp {
			aggKey = cc.ResourceType
		} else if agg == CustomCostProviderIdProp {
			aggKey = cc.ProviderId
		} else if agg == CustomCostUsageUnitProp {
			aggKey = cc.UsageUnit
		} else if agg == CustomCostDomainProp {
			aggKey = cc.Domain
		} else if agg == CustomCostCostSourceProp {
			aggKey = cc.CostSource
		} else {
			return "", fmt.Errorf("unsupported aggregation type: %s", agg)
		}

		if len(aggKey) == 0 {
			aggKey = opencost.UnallocatedSuffix
		}
		aggKeys = append(aggKeys, aggKey)
	}
	aggKey := strings.Join(aggKeys, "/")

	return aggKey, nil
}

func (ccs *CustomCostSet) Sort(sortBy SortProperty, sortDirection SortDirection) {

	switch sortBy {
	case SortPropertyCost:
		if sortDirection == SortDirectionAsc {
			sort.Slice(ccs.CustomCosts, func(i, j int) bool {
				return ccs.CustomCosts[i].Cost < ccs.CustomCosts[j].Cost
			})
		} else {
			sort.Slice(ccs.CustomCosts, func(i, j int) bool {
				return ccs.CustomCosts[i].Cost > ccs.CustomCosts[j].Cost
			})
		}
	case SortPropertyAggregate:
		if sortDirection == SortDirectionAsc {
			sort.Slice(ccs.CustomCosts, func(i, j int) bool {
				return ccs.CustomCosts[i].Aggregate < ccs.CustomCosts[j].Aggregate
			})
		} else {
			sort.Slice(ccs.CustomCosts, func(i, j int) bool {
				return ccs.CustomCosts[i].Aggregate > ccs.CustomCosts[j].Aggregate
			})
		}
	case SortPropertyCostType:
		if sortDirection == SortDirectionAsc {
			sort.Slice(ccs.CustomCosts, func(i, j int) bool {
				return ccs.CustomCosts[i].CostType < ccs.CustomCosts[j].CostType
			})
		} else {
			sort.Slice(ccs.CustomCosts, func(i, j int) bool {
				return ccs.CustomCosts[i].CostType > ccs.CustomCosts[j].CostType
			})
		}
	}
}
