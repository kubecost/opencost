package matcher

import (
	"fmt"
	"strings"

	"github.com/opencost/opencost/core/pkg/filter/ast"
	"github.com/opencost/opencost/core/pkg/log"
)

// StringMatcherFactory leverages a single StringFieldMapper[T] to generate instances of
// StringMatcher[T].
type StringMatcherFactory[T any] struct {
	fieldMapper StringFieldMapper[T]
}

// NewStringMatcherFactory creates a new StringMatcher factory for a given T type.
func NewStringMatcherFactory[T any](fieldMapper StringFieldMapper[T]) *StringMatcherFactory[T] {
	return &StringMatcherFactory[T]{
		fieldMapper: fieldMapper,
	}
}

// NewStringMatcher creates a new StringMatcher using the provided op, field ident, and value comparison.
func (smf *StringMatcherFactory[T]) NewStringMatcher(op ast.FilterOp, ident ast.Identifier, value string) *StringMatcher[T] {
	return &StringMatcher[T]{
		Op:          op,
		Identifier:  ident,
		Value:       value,
		fieldMapper: smf.fieldMapper,
	}
}

// StringMatcher matches properties of a T instance which are string.
type StringMatcher[T any] struct {
	Op         ast.FilterOp
	Identifier ast.Identifier
	Value      string

	fieldMapper StringFieldMapper[T]
}

func (sm *StringMatcher[T]) String() string {
	return fmt.Sprintf(`(%s %s "%s")`, sm.Op, sm.Identifier.String(), sm.Value)
}

// Matches is the canonical in-Go function for determining if T
// matches string property comparison rules.
func (sm *StringMatcher[T]) Matches(that T) bool {
	thatString, err := sm.fieldMapper(that, sm.Identifier)
	if err != nil {
		log.Errorf("Filter: StringMatcher: could not retrieve field %s: %s", sm.Identifier.String(), err.Error())
		return false
	}

	switch sm.Op {
	case ast.FilterOpEquals:
		return thatString == sm.Value

	case ast.FilterOpContains:
		return strings.Contains(thatString, sm.Value)

	case ast.FilterOpContainsPrefix:
		return strings.HasPrefix(thatString, sm.Value)

	case ast.FilterOpContainsSuffix:
		return strings.HasSuffix(thatString, sm.Value)

	default:
		log.Errorf("Filter: StringMatcher: Unhandled filter op. This is a filter implementation error and requires immediate patching. Op: %s", sm.Op)
		return false
	}
}
