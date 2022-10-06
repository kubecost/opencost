package filter

import (
	"fmt"
	"github.com/opencost/opencost/pkg/kubecost"
	"github.com/opencost/opencost/pkg/log"
	"strings"
)

// StringPropertied is used to validate the name of a property field and return its value
type StringPropertied interface {
	// StringProperty acts as a validator and getter for a structs string properties
	StringProperty(string) (string, error)
}

// StringOperation is an enum that represents operations that can be performed
// when filtering (equality, inequality, etc.)
type StringOperation string

// If you add a FilterOp, MAKE SURE TO UPDATE ALL FILTER IMPLEMENTATIONS! Go
// does not enforce exhaustive pattern matching on "enum" types.
const (
	// StringEquals is the equality operator
	// "kube-system" FilterEquals "kube-system" = true
	// "kube-syste" FilterEquals "kube-system" = false
	StringEquals StringOperation = "stringequals"

	// StringStartsWith matches strings with the given prefix.
	// "kube-system" StartsWith "kube" = true
	//
	// When comparing with a field represented by an array/slice, this is like
	// applying FilterContains to every element of the slice.
	StringStartsWith = "stringstartswith"
)

// AllocationCondition is the lowest-level type of filter. It represents
// the a filter operation (equality, inequality, etc.) on a field (namespace,
// label, etc.).
type StringProperty[T StringPropertied] struct {
	Field string
	Op    StringOperation

	// Value is for _all_ filters. A filter of 'namespace:"kubecost"' has
	// Value="kubecost"
	Value string
}

func (sp StringProperty[T]) String() string {
	return fmt.Sprintf(`(%s %s "%s")`, sp.Op, sp.Field, sp.Value)
}

// Flattened returns itself because you cannot flatten a base condition further
func (sp StringProperty[T]) Flattened() Filter[T] {
	return sp
}

func (sp StringProperty[T]) equals(that Filter[T]) bool {
	if thatAC, ok := that.(StringProperty[T]); ok {
		return sp == thatAC
	}
	return false
}

func (sp StringProperty[T]) Matches(that T) bool {

	thatString, err := that.StringProperty(sp.Field)
	if err != nil {
		log.Errorf("Filter: StringProperty: could not retrieve field %s: %s", sp.Field, err.Error())
		return false
	}

	switch sp.Op {
	case StringEquals:
		// namespace:"__unallocated__" should match a.Properties.Namespace = ""
		if thatString == "" {
			return sp.Value == kubecost.UnallocatedSuffix
		}

		if thatString == sp.Value {
			return true
		}
	case StringStartsWith:

		// We don't need special __unallocated__ logic here because a query
		// asking for "__unallocated__" won't have a wildcard and unallocated
		// properties are the empty string.

		return strings.HasPrefix(thatString, sp.Value)
	default:
		log.Errorf("Filter: StringProperty: Unhandled filter op. This is a filter implementation error and requires immediate patching. Op: %s", sp.Op)
		return false
	}

	return false
}
