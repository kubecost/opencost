// The ops package provides a set of functions that can be used to
// build a filter AST programatically using basic functions, versus
// building a filter AST leveraging all structural components of the
// tree.
package ops

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/opencost/opencost/core/pkg/filter/allocation"
	"github.com/opencost/opencost/core/pkg/filter/asset"
	"github.com/opencost/opencost/core/pkg/filter/ast"
	"github.com/opencost/opencost/core/pkg/filter/cloudcost"
	"github.com/opencost/opencost/core/pkg/filter/k8sobject"
	"github.com/opencost/opencost/core/pkg/util/typeutil"
)

// keyFieldType is used to extract field, key, and field type
type keyFieldType interface {
	Field() string
	Key() string
	Type() string
}

// This is somewhat of a fancy solution, but allows us to "register" DefaultFieldByName funcs
// funcs by Field type.
var defaultFieldByType = map[string]any{
	typeutil.TypeOf[allocation.AllocationField](): allocation.DefaultFieldByName,
	typeutil.TypeOf[asset.AssetField]():           asset.DefaultFieldByName,
	typeutil.TypeOf[cloudcost.CloudCostField]():   cloudcost.DefaultFieldByName,
	typeutil.TypeOf[k8sobject.K8sObjectField]():   k8sobject.DefaultFieldByName,
	// typeutil.TypeOf[containerstats.ContainerStatsField](): containerstats.DefaultFieldByName,
}

// RegisterDefaultFieldLookup registers a function that can be used to lookup a specific field type.
func RegisterDefaultFieldLookup[T ~string](lookup func(T) *ast.Field) {
	defaultFieldByType[typeutil.TypeOf[T]()] = lookup
}

// asField looks up a specific T field instance by name and returns the default
// ast.Field value for that type.
func asField[T ~string](field T) *ast.Field {
	lookup, ok := defaultFieldByType[typeutil.TypeOf[T]()]
	if !ok {
		return nil
	}

	defaultLookup, ok := lookup.(func(T) *ast.Field)
	if !ok {
		return nil
	}

	return defaultLookup(field)
}

// asFieldWithType allows for a field to be looked up by name and type.
func asFieldWithType(field string, typ string) *ast.Field {
	lookup, ok := defaultFieldByType[typ]
	if !ok {
		return nil
	}

	// This is the sacrifice being made to allow a simple filter
	// builder style API. In the cases where we have keys, the typical
	// field type gets wrapped in a KeyedFieldType, which is a string
	// that holds all the parameterized data, but no way to get back from
	// string to T-instance.

	// Since we have the type name, we can use that to lookup the specific
	// func(T) *ast.Field function to be used.
	funcType := reflect.TypeOf(lookup)

	// Assert that the function has a single parameter (type T)
	if funcType.NumIn() != 1 {
		return nil
	}

	// Get a reference to the first parameter's type (T)
	inType := funcType.In(0)

	// Create a reflect.Value for the string field, then convert it to
	// the T type from the function's parameter list. (This has to be
	// done to ensure we're executing the call with the correct types)
	fieldParam := reflect.ValueOf(field).Convert(inType)

	// Create a reflect.Value for the lookup function
	callable := reflect.ValueOf(lookup)

	// Call the function with the fieldParam value, and get the result
	result := callable.Call([]reflect.Value{fieldParam})
	if len(result) == 0 {
		return nil
	}

	// Lastly, extract the value from the reflect.Value and ensure we can
	// cast it to *ast.Field
	resultValue := result[0].Interface()
	if f, ok := resultValue.(*ast.Field); ok {
		return f
	}

	return nil
}

// KeyedFieldType is a type alias for field is a special field type that can
// be deconstructed into multiple components.
type KeyedFieldType string

func (k KeyedFieldType) Field() string {
	str := string(k)
	idx := strings.Index(str, "$")
	if idx == -1 {
		return ""
	}

	return str[0:idx]
}

func (k KeyedFieldType) Key() string {
	str := string(k)
	idx := strings.Index(str, "$")
	if idx == -1 {
		return ""
	}

	lastIndex := strings.LastIndex(str, "$")
	if lastIndex == -1 {
		return ""
	}

	return str[idx+1 : lastIndex]
}

func (k KeyedFieldType) Type() string {
	str := string(k)
	lastIndex := strings.LastIndex(str, "$")
	if lastIndex == -1 {
		return ""
	}

	return str[lastIndex+1:]
}

func WithKey[T ~string](field T, key string) KeyedFieldType {
	k := fmt.Sprintf("%s$%s$%s", field, key, typeutil.TypeOf[T]())

	return KeyedFieldType(k)
}

func toFieldAndKey[T ~string](field T) (*ast.Field, string) {
	var inner any = field
	if kft, ok := inner.(keyFieldType); ok {
		return asFieldWithType(kft.Field(), kft.Type()), kft.Key()
	}

	return asField(field), ""
}

func identifier[T ~string](field T) ast.Identifier {
	f, key := toFieldAndKey(field)

	return ast.Identifier{
		Field: f,
		Key:   key,
	}
}

func And(node, next ast.FilterNode, others ...ast.FilterNode) ast.FilterNode {
	operands := append([]ast.FilterNode{node, next}, others...)

	return &ast.AndOp{
		Operands: operands,
	}
}

func Or(node, next ast.FilterNode, others ...ast.FilterNode) ast.FilterNode {
	operands := append([]ast.FilterNode{node, next}, others...)

	return &ast.OrOp{
		Operands: operands,
	}
}

func Not(node ast.FilterNode) ast.FilterNode {
	return &ast.NotOp{
		Operand: node,
	}
}

func Eq[T ~string](field T, value string) ast.FilterNode {
	return &ast.EqualOp{
		Left:  identifier(field),
		Right: value,
	}
}

func NotEq[T ~string](field T, value string) ast.FilterNode {
	return Not(Eq(field, value))
}

func Contains[T ~string](field T, value string) ast.FilterNode {
	return &ast.ContainsOp{
		Left:  identifier(field),
		Right: value,
	}
}

func NotContains[T ~string](field T, value string) ast.FilterNode {
	return Not(Contains(field, value))
}

func ContainsPrefix[T ~string](field T, value string) ast.FilterNode {
	return &ast.ContainsPrefixOp{
		Left:  identifier(field),
		Right: value,
	}
}

func NotContainsPrefix[T ~string](field T, value string) ast.FilterNode {
	return Not(ContainsPrefix(field, value))
}

func ContainsSuffix[T ~string](field T, value string) ast.FilterNode {
	return &ast.ContainsSuffixOp{
		Left:  identifier(field),
		Right: value,
	}
}

func NotContainsSuffix[T ~string](field T, value string) ast.FilterNode {
	return Not(ContainsSuffix(field, value))
}
