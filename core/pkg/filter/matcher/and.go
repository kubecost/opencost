package matcher

import (
	"fmt"
)

// And is a set of filters that should be evaluated as a logical
// AND.
type And[T any] struct {
	Matchers []Matcher[T]
}

func (a *And[T]) Add(m Matcher[T]) {
	a.Matchers = append(a.Matchers, m)
}

func (a *And[T]) String() string {
	s := "(and"
	for _, f := range a.Matchers {
		s += fmt.Sprintf(" %s", f)
	}

	s += ")"
	return s
}

// Matches is the canonical in-Go function for determining if T
// matches a AND match rules.
func (a *And[T]) Matches(that T) bool {
	filters := a.Matchers
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
