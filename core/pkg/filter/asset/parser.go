package asset

import "github.com/opencost/opencost/core/pkg/filter/ast"

// a slice of all the asset field instances the lexer should recognize as
// valid left-hand comparators
var assetFilterFields []*ast.Field = []*ast.Field{
	ast.NewField(FieldType),
	ast.NewField(FieldName),
	ast.NewField(FieldCategory),
	ast.NewField(FieldClusterID),
	ast.NewField(FieldProject),
	ast.NewField(FieldProvider),
	ast.NewField(FieldProviderID),
	ast.NewField(FieldAccount),
	ast.NewField(FieldService),
	ast.NewMapField(FieldLabel),
	ast.NewAliasField(DepartmentProp),
	ast.NewAliasField(EnvironmentProp),
	ast.NewAliasField(ProductProp),
	ast.NewAliasField(OwnerProp),
	ast.NewAliasField(TeamProp),
}

// fieldMap is a lazily loaded mapping from AllocationField to ast.Field
var fieldMap map[AssetField]*ast.Field

func init() {
	fieldMap = make(map[AssetField]*ast.Field, len(assetFilterFields))
	for _, f := range assetFilterFields {
		ff := *f
		fieldMap[AssetField(ff.Name)] = &ff
	}
}

// DefaultFieldByName returns only default allocation filter fields by name.
func DefaultFieldByName(field AssetField) *ast.Field {
	if af, ok := fieldMap[field]; ok {
		afcopy := *af
		return &afcopy
	}

	return nil
}

// NewAssetFilterParser creates a new `ast.FilterParser` implementation
// which uses asset specific fields
func NewAssetFilterParser() ast.FilterParser {
	return ast.NewFilterParser(assetFilterFields)
}
