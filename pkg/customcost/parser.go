package customcost

import "github.com/opencost/opencost/core/pkg/filter/ast"

// a slice of all the custom costs field instances the lexer should recognize as
// valid left-hand comparators
var customCostFilterFields = []*ast.Field{
	ast.NewField(CustomCostZoneProp),
	ast.NewField(CustomCostAccountNameProp),
	ast.NewField(CustomCostChargeCategoryProp),
	ast.NewField(CustomCostDescriptionProp),
	ast.NewField(CustomCostResourceNameProp),
	ast.NewField(CustomCostResourceTypeProp),
	ast.NewField(CustomCostProviderIdProp),
	ast.NewField(CustomCostUsageUnitProp),
	ast.NewField(CustomCostDomainProp),
	ast.NewField(CustomCostCostSourceProp),
}

// NewCustomCostFilterParser creates a new `ast.FilterParser` implementation
// which uses CustomCost specific fields
func NewCustomCostFilterParser() ast.FilterParser {
	return ast.NewFilterParser(customCostFilterFields)
}
