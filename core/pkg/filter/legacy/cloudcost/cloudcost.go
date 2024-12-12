package cloudcost

import (
	"reflect"
	"strings"

	filter "github.com/opencost/opencost/core/pkg/filter/legacy"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/mapper"
)

type CloudCostFilter struct {
	AccountIDs       []string `json:"accountIDs,omitempty"`
	Categories       []string `json:"categories,omitempty"`
	InvoiceEntityIDs []string `json:"invoiceEntityIDs,omitempty"`
	Labels           []string `json:"labels,omitempty"`
	Providers        []string `json:"providers,omitempty"`
	ProviderIDs      []string `json:"providerIDs,omitempty"`
	Services         []string `json:"services,omitempty"`
}

func (g *CloudCostFilter) Equals(that CloudCostFilter) bool {
	return reflect.DeepEqual(g.AccountIDs, that.AccountIDs) &&
		reflect.DeepEqual(g.Categories, that.Categories) &&
		reflect.DeepEqual(g.InvoiceEntityIDs, that.InvoiceEntityIDs) &&
		reflect.DeepEqual(g.Labels, that.Labels) &&
		reflect.DeepEqual(g.Providers, that.Providers) &&
		reflect.DeepEqual(g.ProviderIDs, that.ProviderIDs) &&
		reflect.DeepEqual(g.Services, that.Services)
}
func parseWildcardEnd(rawFilterValue string) (string, bool) {
	return strings.TrimSuffix(rawFilterValue, "*"), strings.HasSuffix(rawFilterValue, "*")
}

func CloudCostFilterFromParams(pmr mapper.PrimitiveMapReader) filter.Filter[*opencost.CloudCost] {
	ccFilter := convertFilterQueryParams(pmr)
	return ParseCloudCostFilter(ccFilter)
}

func convertFilterQueryParams(pmr mapper.PrimitiveMapReader) CloudCostFilter {
	return CloudCostFilter{
		AccountIDs:       pmr.GetList("filterAccountIDs", ","),
		Categories:       pmr.GetList("filterCategories", ","),
		InvoiceEntityIDs: pmr.GetList("filterInvoiceEntityIDs", ","),
		Labels:           pmr.GetList("filterLabels", ","),
		Providers:        pmr.GetList("filterProviders", ","),
		ProviderIDs:      pmr.GetList("filterProviderIDs", ","),
		Services:         pmr.GetList("filterServices", ","),
	}
}
func ParseCloudCostFilter(filters CloudCostFilter) filter.Filter[*opencost.CloudCost] {
	result := filter.And[*opencost.CloudCost]{
		Filters: []filter.Filter[*opencost.CloudCost]{},
	}

	if len(filters.InvoiceEntityIDs) > 0 {
		result.Filters = append(result.Filters, filterV1SingleValueFromList(filters.InvoiceEntityIDs, opencost.CloudCostInvoiceEntityIDProp))
	}

	if len(filters.AccountIDs) > 0 {
		result.Filters = append(result.Filters, filterV1SingleValueFromList(filters.AccountIDs, opencost.CloudCostAccountIDProp))
	}

	if len(filters.Providers) > 0 {
		result.Filters = append(result.Filters, filterV1SingleValueFromList(filters.Providers, opencost.CloudCostProviderProp))
	}

	if len(filters.ProviderIDs) > 0 {
		result.Filters = append(result.Filters, filterV1SingleValueFromList(filters.ProviderIDs, opencost.CloudCostProviderIDProp))
	}

	if len(filters.Services) > 0 {
		result.Filters = append(result.Filters, filterV1SingleValueFromList(filters.Services, opencost.CloudCostServiceProp))
	}

	if len(filters.Categories) > 0 {
		result.Filters = append(result.Filters, filterV1SingleValueFromList(filters.Categories, opencost.CloudCostCategoryProp))
	}

	if len(filters.Labels) > 0 {
		result.Filters = append(result.Filters, filterV1DoubleValueFromList(filters.Labels, opencost.CloudCostLabelProp))
	}

	if len(result.Filters) == 0 {
		return nil
	}

	return result
}

func filterV1SingleValueFromList(rawFilterValues []string, field string) filter.Filter[*opencost.CloudCost] {
	result := filter.Or[*opencost.CloudCost]{
		Filters: []filter.Filter[*opencost.CloudCost]{},
	}

	for _, filterValue := range rawFilterValues {
		filterValue = strings.TrimSpace(filterValue)
		filterValue, wildcard := parseWildcardEnd(filterValue)

		subFilter := filter.StringProperty[*opencost.CloudCost]{
			Field: field,
			Op:    filter.StringEquals,
			Value: filterValue,
		}

		if wildcard {
			subFilter.Op = filter.StringStartsWith
		}

		result.Filters = append(result.Filters, subFilter)
	}

	return result
}

// filterV1DoubleValueFromList creates an OR of key:value equality filters for
// colon-split filter values.
//
// The v1 query language (e.g. "filterLabels=app:foo,l2:bar") uses OR within
// a field (e.g. label[app] = foo OR label[l2] = bar)
func filterV1DoubleValueFromList(rawFilterValuesUnsplit []string, filterField string) filter.Filter[*opencost.CloudCost] {
	result := filter.Or[*opencost.CloudCost]{
		Filters: []filter.Filter[*opencost.CloudCost]{},
	}

	for _, unsplit := range rawFilterValuesUnsplit {
		if unsplit != "" {
			split := strings.Split(unsplit, ":")
			if len(split) != 2 {
				log.Warnf("illegal key/value filter (ignoring): %s", unsplit)
				continue
			}
			labelName := strings.TrimSpace(split[0])
			val := strings.TrimSpace(split[1])
			val, wildcard := parseWildcardEnd(val)

			subFilter := filter.StringMapProperty[*opencost.CloudCost]{
				Field: filterField,
				// All v1 filters are equality comparisons
				Op:    filter.StringMapEquals,
				Key:   labelName,
				Value: val,
			}

			if wildcard {
				subFilter.Op = filter.StringMapStartsWith
			}

			result.Filters = append(result.Filters, subFilter)
		}
	}

	return result
}
