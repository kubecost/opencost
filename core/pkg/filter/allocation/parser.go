package allocation

import "github.com/opencost/opencost/core/pkg/filter/ast"

// a slice of all the allocation field instances the lexer should recognize as
// valid left-hand comparators
var allocationFilterFields []*ast.Field = []*ast.Field{
	ast.NewField(FieldClusterID),
	ast.NewField(FieldNode),
	ast.NewField(FieldNamespace),
	ast.NewField(FieldControllerName, ast.FieldAttributeNilable),
	ast.NewField(FieldControllerKind, ast.FieldAttributeNilable),
	ast.NewField(FieldContainer),
	ast.NewField(FieldPod),
	ast.NewField(FieldProvider),
	ast.NewAliasField(AliasDepartment),
	ast.NewAliasField(AliasEnvironment),
	ast.NewAliasField(AliasOwner),
	ast.NewAliasField(AliasProduct),
	ast.NewAliasField(AliasTeam),
	ast.NewSliceField(FieldServices),
	ast.NewMapField(FieldLabel),
	ast.NewMapField(FieldAnnotation),
}

// fieldMap is a lazily loaded mapping from AllocationField to ast.Field
var fieldMap map[AllocationField]*ast.Field

func init() {
	fieldMap = make(map[AllocationField]*ast.Field, len(allocationFilterFields))
	for _, f := range allocationFilterFields {
		ff := *f
		fieldMap[AllocationField(ff.Name)] = &ff
	}
}

// DefaultFieldByName returns only default allocation filter fields by name.
func DefaultFieldByName(field AllocationField) *ast.Field {
	if af, ok := fieldMap[field]; ok {
		afcopy := *af
		return &afcopy
	}

	return nil
}

// NewAllocationFilterParser creates a new `ast.FilterParser` implementation
// which uses allocation specific fields
func NewAllocationFilterParser() ast.FilterParser {
	return ast.NewFilterParser(allocationFilterFields)
}
