package opencost

import (
	"fmt"

	"github.com/opencost/opencost/core/pkg/filter/ast"
	ccfilter "github.com/opencost/opencost/core/pkg/filter/cloudcost"
	"github.com/opencost/opencost/core/pkg/filter/matcher"
	"github.com/opencost/opencost/core/pkg/filter/transform"
)

// CloudCostMatcher is a matcher implementation for CloudCost instances,
// compiled using the matcher.MatchCompiler for cloud costs.
type CloudCostMatcher matcher.Matcher[*CloudCost]

// NewCloudCostMatchCompiler creates a new instance of a
// matcher.MatchCompiler[*CloudCost] which can be used to compile filter.Filter
// ASTs into matcher.Matcher[*CloudCost] implementations.
//
// If storage interfaces every support querying natively by alias (e.g. if a
// data store contained a "product" attribute on an CloudCost row), that should
// be handled by a purpose-built AST compiler.
func NewCloudCostMatchCompiler() *matcher.MatchCompiler[*CloudCost] {
	passes := []transform.CompilerPass{}

	passes = append(passes,
		transform.UnallocatedReplacementPass(),
	)
	return matcher.NewMatchCompiler(
		cloudCostFieldMap,
		cloudCostSliceFieldMap,
		cloudCostMapFieldMap,
		passes...,
	)
}

// Maps fields from a cloud cost to a string value based on an identifier
func cloudCostFieldMap(cc *CloudCost, identifier ast.Identifier) (string, error) {
	if cc == nil {
		return "", fmt.Errorf("cannot map to nil cloud cost")
	}
	if cc.Properties == nil {
		return "", fmt.Errorf("cannot map to nil properties")
	}
	if identifier.Field == nil {
		return "", fmt.Errorf("cannot map field from identifier with nil field")
	}
	switch ccfilter.CloudCostField(identifier.Field.Name) {
	case ccfilter.FieldInvoiceEntityID:
		return cc.Properties.InvoiceEntityID, nil
	case ccfilter.FieldInvoiceEntityName:
		return cc.Properties.InvoiceEntityName, nil
	case ccfilter.FieldAccountID:
		return cc.Properties.AccountID, nil
	case ccfilter.FieldAccountName:
		return cc.Properties.AccountName, nil
	case ccfilter.FieldRegionID:
		return cc.Properties.RegionID, nil
	case ccfilter.FieldAvailabilityZone:
		return cc.Properties.AvailabilityZone, nil
	case ccfilter.FieldProvider:
		return cc.Properties.Provider, nil
	case ccfilter.FieldProviderID:
		return cc.Properties.ProviderID, nil
	case ccfilter.FieldCategory:
		return cc.Properties.Category, nil
	case ccfilter.FieldService:
		return cc.Properties.Service, nil
	case ccfilter.FieldLabel:
		return cc.Properties.Labels[identifier.Key], nil
	}

	return "", fmt.Errorf("Failed to find string identifier on CloudCost: %s", identifier.Field.Name)
}

// Maps slice fields from an asset to a []string value based on an identifier
func cloudCostSliceFieldMap(cc *CloudCost, identifier ast.Identifier) ([]string, error) {
	return nil, fmt.Errorf("Cloud Cost have no slice fields")
}

// Maps map fields from a cloud cost to a map[string]string value based on an identifier
func cloudCostMapFieldMap(cc *CloudCost, identifier ast.Identifier) (map[string]string, error) {
	switch ccfilter.CloudCostField(identifier.Field.Name) {
	case ccfilter.FieldLabel:
		return cc.Properties.Labels, nil
	}
	return nil, fmt.Errorf("Failed to find map[string]string identifier on CloudCost: %s", identifier.Field.Name)
}
