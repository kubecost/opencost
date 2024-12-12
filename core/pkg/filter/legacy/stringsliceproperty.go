package legacy

import (
	"fmt"
	"strings"

	"github.com/opencost/opencost/core/pkg/log"
)

type StringSlicePropertied interface {
	StringSliceProperty(string) ([]string, error)
}

// StringSliceOperation is an enum that represents operations that can be performed
// when filtering (equality, inequality, etc.)
type StringSliceOperation string

const (
	// StringSliceContains is an array/slice membership operator
	// ["a", "b", "c"] FilterContains "a" = true
	StringSliceContains StringSliceOperation = "stringslicecontains"

	// StringSliceContainsPrefix is like FilterContains, but using StartsWith instead
	// of Equals.
	// ["kube-system", "abc123"] ContainsPrefix ["kube"] = true
	StringSliceContainsPrefix = "stringslicecontainsprefix"
)

// StringSliceProperty is the lowest-level type of filter. It represents
// a filter operation (equality, inequality, etc.) on a property that contains a string slice
type StringSliceProperty[T StringSlicePropertied] struct {
	Field string
	Op    StringSliceOperation

	Value string
}

func (ssp StringSliceProperty[T]) String() string {
	return fmt.Sprintf(`(%s %s "%s")`, ssp.Op, ssp.Field, ssp.Value)
}

func (ssp StringSliceProperty[T]) Matches(that T) bool {

	thatSlice, err := that.StringSliceProperty(ssp.Field)
	if err != nil {
		log.Errorf("Filter: StringSliceProperty: could not retrieve field %s: %s", ssp.Field, err.Error())
		return false
	}

	switch ssp.Op {

	case StringSliceContains:
		if len(thatSlice) == 0 {
			return ssp.Value == unallocatedSuffix
		}

		for _, s := range thatSlice {
			if s == ssp.Value {
				return true
			}
		}
	case StringSliceContainsPrefix:
		// We don't need special __unallocated__ logic here because a query
		// asking for "__unallocated__" won't have a wildcard and unallocated
		// properties are the empty string.

		for _, s := range thatSlice {
			if strings.HasPrefix(s, ssp.Value) {
				return true
			}
		}

		return false
	default:
		log.Errorf("Filter: StringSliceProperty: Unhandled filter op. This is a filter implementation error and requires immediate patching. Op: %s", ssp.Op)
		return false
	}

	return false
}
