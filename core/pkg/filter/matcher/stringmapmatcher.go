package matcher

import (
	"fmt"
	"strings"

	"github.com/opencost/opencost/core/pkg/filter/ast"
	"github.com/opencost/opencost/core/pkg/log"
)

// StringMapMatcherFactory leverages a single MapFieldMapper[T] to generate instances of
// StringMapMatcher[T].
type StringMapMatcherFactory[T any] struct {
	fieldMapper MapFieldMapper[T]
}

// NewStringMapMatcherFactory creates a new StringMapMatcher factory for a given T type.
func NewStringMapMatcherFactory[T any](fieldMapper MapFieldMapper[T]) *StringMapMatcherFactory[T] {
	return &StringMapMatcherFactory[T]{
		fieldMapper: fieldMapper,
	}
}

// NewStringMapMatcher creates a new StringMapMatcher using the provided op, field ident and key for comparison
func (smmf *StringMapMatcherFactory[T]) NewStringMapMatcher(op ast.FilterOp, ident ast.Identifier, key string) *StringMapMatcher[T] {
	return &StringMapMatcher[T]{
		Op:          op,
		Identifier:  ident,
		Key:         key,
		fieldMapper: smmf.fieldMapper,
	}
}

// // StringMapMatcher matches properties of a T instance which are map[string]string
type StringMapMatcher[T any] struct {
	Op         ast.FilterOp
	Identifier ast.Identifier
	Key        string

	fieldMapper MapFieldMapper[T]
}

func (smm *StringMapMatcher[T]) String() string {
	return fmt.Sprintf(`(%s %s "%s")`, smm.Op, smm.Identifier.String(), smm.Key)
}

func (smm *StringMapMatcher[T]) Matches(that T) bool {
	thatMap, err := smm.fieldMapper(that, smm.Identifier)
	if err != nil {
		log.Errorf("Filter: StringMapMatcher: could not retrieve field %s: %s", smm.Identifier.String(), err.Error())
		return false
	}

	switch smm.Op {
	case ast.FilterOpContains:
		_, exists := thatMap[smm.Key]
		return exists

	case ast.FilterOpContainsPrefix:
		for k := range thatMap {
			if strings.HasPrefix(k, smm.Key) {
				return true
			}
		}
		return false

	case ast.FilterOpContainsSuffix:
		for k := range thatMap {
			if strings.HasSuffix(k, smm.Key) {
				return true
			}
		}
		return false

	default:
		log.Errorf("Filter: StringMapMatcher: Unhandled matcher op. This is a filter implementation error and requires immediate patching. Op: %s", smm.Op)
		return false
	}
}
