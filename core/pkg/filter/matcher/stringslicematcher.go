package matcher

import (
	"fmt"
	"strings"

	"github.com/opencost/opencost/core/pkg/filter/ast"
	"github.com/opencost/opencost/core/pkg/log"
)

// StringMatcherFactory leverages a single StringSliceFieldMapper[T] to generate instances of
// StringSliceMatcher[T].
type StringSliceMatcherFactory[T any] struct {
	fieldMapper SliceFieldMapper[T]
}

// NewStringSliceMatcherFactory creates a new StringMatcher factory for a given T type.
func NewStringSliceMatcherFactory[T any](fieldMapper SliceFieldMapper[T]) *StringSliceMatcherFactory[T] {
	return &StringSliceMatcherFactory[T]{
		fieldMapper: fieldMapper,
	}
}

// NewStringMatcher creates a new StringSliceMatcher using the provided op, field ident, and value comparison.
func (smf *StringSliceMatcherFactory[T]) NewStringSliceMatcher(op ast.FilterOp, ident ast.Identifier, value string) *StringSliceMatcher[T] {
	return &StringSliceMatcher[T]{
		Op:          op,
		Identifier:  ident,
		Value:       value,
		fieldMapper: smf.fieldMapper,
	}
}

// StringSliceProperty is the lowest-level type of filter. It represents
// a filter operation (equality, inequality, etc.) on a property that contains a string slice
type StringSliceMatcher[T any] struct {
	Op         ast.FilterOp
	Identifier ast.Identifier
	Value      string

	fieldMapper SliceFieldMapper[T]
}

func (ssp *StringSliceMatcher[T]) String() string {
	return fmt.Sprintf(`(%s %s "%s")`, ssp.Op, ssp.Identifier.String(), ssp.Value)
}

func (ssp *StringSliceMatcher[T]) Matches(that T) bool {
	thatSlice, err := ssp.fieldMapper(that, ssp.Identifier)
	if err != nil {
		log.Errorf("Filter: StringSliceMatcher: could not retrieve field %s: %s", ssp.Identifier.String(), err.Error())
		return false
	}

	switch ssp.Op {

	case ast.FilterOpContains:
		if len(thatSlice) == 0 {
			return ssp.Value == ""
		}

		for _, s := range thatSlice {
			if s == ssp.Value {
				return true
			}
		}

	case ast.FilterOpContainsPrefix:
		for _, s := range thatSlice {
			if strings.HasPrefix(s, ssp.Value) {
				return true
			}
		}

		return false

	case ast.FilterOpContainsSuffix:
		for _, s := range thatSlice {
			if strings.HasSuffix(s, ssp.Value) {
				return true
			}
		}
		return false

	default:
		log.Errorf("Filter: StringSliceMatcher: Unhandled filter op. This is a filter implementation error and requires immediate patching. Op: %s", ssp.Op)
		return false
	}

	return false
}
