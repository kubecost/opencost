package legacy

import (
	"fmt"
)

// And is a set of filters that should be evaluated as a logical
// AND.
type And[T any] struct {
	Filters []Filter[T]
}

func (a And[T]) String() string {
	s := "(and"
	for _, f := range a.Filters {
		s += fmt.Sprintf(" %s", f)
	}

	s += ")"
	return s
}

func (a And[T]) Matches(that T) bool {
	filters := a.Filters
	if len(filters) == 0 {
		return true
	}

	for _, filter := range filters {
		if !filter.Matches(that) {
			return false
		}
	}

	return true
}
