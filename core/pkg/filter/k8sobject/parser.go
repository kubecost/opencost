package k8sobject

import (
	"github.com/opencost/opencost/core/pkg/filter/ast"
)

// a slice of all the allocation field instances the lexer should recognize as
// valid left-hand comparators
var k8sObjectFilterFields []*ast.Field = []*ast.Field{
	ast.NewField(FieldNamespace),
	ast.NewField(FieldControllerName, ast.FieldAttributeNilable),
	ast.NewField(FieldControllerKind, ast.FieldAttributeNilable),
	ast.NewField(FieldPod),
	ast.NewMapField(FieldLabel),
	ast.NewMapField(FieldAnnotation),
}

// fieldMap is a lazily loaded mapping from AllocationField to ast.Field
var fieldMap map[K8sObjectField]*ast.Field

func init() {
	fieldMap = make(map[K8sObjectField]*ast.Field, len(k8sObjectFilterFields))
	for _, f := range k8sObjectFilterFields {
		ff := *f
		fieldMap[K8sObjectField(ff.Name)] = &ff
	}
}

// DefaultFieldByName returns only default allocation filter fields by name.
func DefaultFieldByName(field K8sObjectField) *ast.Field {
	if af, ok := fieldMap[field]; ok {
		afcopy := *af
		return &afcopy
	}

	return nil
}

// NewK8sObjectFilterParser creates a new `ast.FilterParser` implementation for
// K8s runtime.Objects.
func NewK8sObjectFilterParser() ast.FilterParser {
	return ast.NewFilterParser(k8sObjectFilterFields)
}
