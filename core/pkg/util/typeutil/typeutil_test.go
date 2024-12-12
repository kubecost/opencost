package typeutil_test

import (
	"encoding/json"
	"testing"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/typeutil"
)

type TestType struct{}
type GenericTestType[T any] struct{}

type CurrentPackageTester struct{}

func (c *CurrentPackageTester) TestFromInstance(t *testing.T, expected string) {
	cmp(t, typeutil.CurrentPackage(), expected)
}

func (c *CurrentPackageTester) TestFromNestedInstance(t *testing.T, expected string) {
	nested := func() string {
		return typeutil.CurrentPackage()
	}

	result := nested()
	cmp(t, result, expected)
}

// cmp compares two comparable values and fails the test if they are not equal.
func cmp[T comparable](t *testing.T, result, expected T) {
	if result != expected {
		t.Errorf("Expected: %+v. Got: %+v", expected, result)
	}
}

type InterfaceType interface{}

var packageScoped = typeutil.CurrentPackage()

func TestTypeOf(t *testing.T) {
	const packageName = "github.com/opencost/opencost/core/pkg/util/typeutil_test"
	const testTypeName = packageName + "/TestType"
	const genericTestTypeName = packageName + "/GenericTestType"
	const genericTypeParameterTypeName = packageName + ".GenericTestType"
	const interfaceTypeName = packageName + "/InterfaceType"

	// Basic Types
	cmp(t, typeutil.TypeOf[int](), "int")
	cmp(t, typeutil.TypeOf[int8](), "int8")
	cmp(t, typeutil.TypeOf[any](), "interface {}")
	cmp(t, typeutil.TypeOf[interface{}](), "interface {}")
	cmp(t, typeutil.TypeOf[struct{}](), "struct {}")

	// Specific Types
	cmp(t, typeutil.TypeOf[TestType](), testTypeName)
	cmp(t, typeutil.TypeOf[*TestType](), "*"+testTypeName)
	cmp(t, typeutil.TypeOf[**TestType](), "**"+testTypeName)
	cmp(t, typeutil.TypeOf[GenericTestType[string]](), genericTestTypeName+"[string]")
	cmp(t, typeutil.TypeOf[GenericTestType[GenericTestType[string]]](), genericTestTypeName+"["+genericTypeParameterTypeName+"[string]"+"]")
	cmp(t, typeutil.TypeOf[GenericTestType[*GenericTestType[string]]](), genericTestTypeName+"[*"+genericTypeParameterTypeName+"[string]"+"]")
	cmp(t, typeutil.TypeOf[GenericTestType[*GenericTestType[map[int][]float64]]](), genericTestTypeName+"[*"+genericTypeParameterTypeName+"[map[int][]float64]"+"]")

	// interface types
	cmp(t, typeutil.TypeOf[InterfaceType](), interfaceTypeName)
	cmp(t, typeutil.TypeOf[*InterfaceType](), "*"+interfaceTypeName)
	cmp(t, typeutil.TypeOf[**InterfaceType](), "**"+interfaceTypeName)

	// TypeFor variants
	var value any
	cmp(t, typeutil.TypeFor(value), "interface {}")

	var ivalue InterfaceType
	cmp(t, typeutil.TypeFor(ivalue), interfaceTypeName)

	var testType **TestType
	cmp(t, typeutil.TypeFor(testType), "**"+testTypeName)
}

func DeferredCurrentPackage() (result string) {
	defer func() {
		result = typeutil.CurrentPackage()
	}()

	return
}

func TestPackageOf(t *testing.T) {
	const currentPackageName = "github.com/opencost/opencost/core/pkg/util/typeutil_test"
	const jsonEncoderPackageName = "encoding/json"
	const opencostPackageName = "github.com/opencost/opencost/core/pkg/opencost"

	cmp(t, typeutil.PackageOf[TestType](), currentPackageName)
	cmp(t, typeutil.PackageOf[*TestType](), currentPackageName)
	cmp(t, typeutil.PackageOf[json.Encoder](), jsonEncoderPackageName)
	cmp(t, typeutil.PackageOf[*opencost.Allocation](), opencostPackageName)

	cmp(t, typeutil.CurrentPackage(), currentPackageName)

	deferredResult := DeferredCurrentPackage()
	cmp(t, deferredResult, currentPackageName)

	// Tests the CurrentPackage function within an instance function
	// this will return something like:
	// "github.com/opencost/opencost/core/pkg/util/typeutil_test.(*CurrentPackageTester).TestFromInstance"
	new(CurrentPackageTester).TestFromInstance(t, currentPackageName)

	// Tests the CurrentPackage function within an instance function that contains a nested anonymous function
	// this will return something like:
	// "github.com/opencost/opencost/core/pkg/util/typeutil_test.(*CurrentPackageTester).TestFromNestedInstance.func1"
	new(CurrentPackageTester).TestFromNestedInstance(t, currentPackageName)

	// This test the package scoped variable which calls the CurrentPackage function in the package scope
	// this will normally return something like:
	// "github.com/opencost/opencost/core/pkg/util/typeutil_test.init"
	cmp(t, packageScoped, currentPackageName)

	// PackageFor variants
	var value any
	cmp(t, typeutil.PackageFor(value), "")

	var ivalue InterfaceType
	cmp(t, typeutil.PackageFor(ivalue), currentPackageName)
}
