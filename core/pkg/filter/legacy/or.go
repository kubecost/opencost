package legacy

import (
	"fmt"
)

// Or is a set of filters that should be evaluated as a logical
// OR.
type Or[T any] struct {
	Filters []Filter[T]
}

func (o Or[T]) String() string {
	s := "(or"
	for _, f := range o.Filters {
		s += fmt.Sprintf(" %s", f)
	}

	s += ")"
	return s
}

func (o Or[T]) Matches(that T) bool {
	filters := o.Filters
	if len(filters) == 0 {
		return true
	}

	for _, filter := range filters {
		if filter.Matches(that) {
			return true
		}
	}

	return false
}
