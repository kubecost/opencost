package typeutil

import (
	"fmt"
	"reflect"
	"runtime"
	"strings"
)

// TypeOf is a utility that can covert a T type to a package + type name for generic types.
func TypeOf[T any]() string {
	var prefix string

	t := reflect.TypeFor[T]()

	// pointer types do not carry the adequate type information, so we need to extract the
	// underlying types until we reach the non-pointer type, we prepend a * each depth
	for t != nil && t.Kind() == reflect.Pointer {
		prefix += "*"
		t = t.Elem()
	}

	// this should not be possible, but in the event that it does, we want to be loud about it
	if t == nil {
		panic(fmt.Sprintf("failed to locate non-pointer type: %+v", reflect.TypeFor[T]()))
	}

	name := t.Name()

	// special cases for built-ins struct{} and interface{}
	if name == "" {
		name = t.String()
	}

	// no package path, do not use a / separator
	if t.PkgPath() == "" {
		return prefix + name
	}

	// combine the prefix, package path, and the type name
	return fmt.Sprintf("%s%s/%s", prefix, t.PkgPath(), name)
}

// TypeFor uses type inferencing to accept a value and returns the fully qualified package
// and type name
func TypeFor[T any](value T) string {
	return TypeOf[T]()
}

// PackageOf is a utility that can return the package name for the type provided.
func PackageOf[T any]() string {
	t := reflect.TypeFor[T]()

	for t != nil && t.Kind() == reflect.Pointer {
		t = t.Elem()
	}

	// this should not be possible, but in the event that it does, we want to be loud about it
	if t == nil {
		panic(fmt.Sprintf("failed to locate package for: %+v", reflect.TypeFor[T]()))
	}

	return t.PkgPath()
}

// PackageFor uses type inferencing to accepts a value and returns
// the package name for the type of the value.
func PackageFor[T any](value T) string {
	return PackageOf[T]()
}

// PackageFromCaller returns the package name of the caller at the specified depth.
func PackageFromCaller(depth int) string {
	// get program counter for the first depth caller into this function
	if pc, _, _, ok := runtime.Caller(depth); ok {
		f := runtime.FuncForPC(pc)
		if f == nil {
			return ""
		}

		parentPkg := ""
		funcName := f.Name()
		pkg := funcName

		// if there are slashes in the fully qualified path, we want to split
		// everything before the last slash as the parent package, and everything
		// after is the package + calling convention. If there are no slashes, then
		// it's a root level package, so it's just package + calling convention
		slashIndex := strings.LastIndex(funcName, "/")
		if slashIndex >= 0 {
			parentPkg = funcName[:slashIndex]
			pkg = funcName[slashIndex:]
		}

		// the package + calling convention can be in a few forms, but since we only
		// care about the package, we can return everything up until a '.'.
		// We can make a hard assertion here that unless the go spec changes, we can
		// rely on the function calling convention to have the form <package>.<function>
		dotIndex := strings.Index(pkg, ".")
		if dotIndex < 0 {
			panic("Unable to parse package name from function call convention: " + pkg)
		}

		// the fully qualified package name is the parent package + resolved caller package
		return parentPkg + pkg[:dotIndex]
	}
	return ""
}

// CurrentPackage returns the package name of the caller. This is especially handy for automatically
// generating package scoped tracing identifiers.
func CurrentPackage() string {
	// Depth is from: (2) Caller -> (1) CurrentPackage -> (0) PackageFromCaller
	return PackageFromCaller(2)
}
