package cloudcost

import "github.com/opencost/opencost/core/pkg/filter/ast"

// a slice of all the cloud costs field instances the lexer should recognize as
// valid left-hand comparators
var cloudCostFilterFields []*ast.Field = []*ast.Field{
	ast.NewField(FieldInvoiceEntityID),
	ast.NewField(FieldInvoiceEntityName),
	ast.NewField(FieldAccountID),
	ast.NewField(FieldAccountName),
	ast.NewField(FieldRegionID),
	ast.NewField(FieldAvailabilityZone),
	ast.NewField(FieldProvider),
	ast.NewField(FieldProviderID),
	ast.NewField(FieldCategory),
	ast.NewField(FieldService),
	ast.NewMapField(FieldLabel),
}

// fieldMap is a lazily loaded mapping from CloudAggregationField to ast.Field
var fieldMap map[CloudCostField]*ast.Field

func init() {
	fieldMap = make(map[CloudCostField]*ast.Field, len(cloudCostFilterFields))
	for _, f := range cloudCostFilterFields {
		ff := *f
		fieldMap[CloudCostField(ff.Name)] = &ff
	}
}

// DefaultFieldByName returns only default cloud cost filter fields by name.
func DefaultFieldByName(field CloudCostField) *ast.Field {
	if af, ok := fieldMap[field]; ok {
		afcopy := *af
		return &afcopy
	}

	return nil
}

// NewCloudCostFilterParser creates a new `ast.FilterParser` implementation
// which uses CloudCost specific fields
func NewCloudCostFilterParser() ast.FilterParser {
	return ast.NewFilterParser(cloudCostFilterFields)
}
