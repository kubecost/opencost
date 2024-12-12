package matcher

import (
	"fmt"
)

// Or is a set of filters that should be evaluated as a logical
// OR.
type Or[T any] struct {
	Matchers []Matcher[T]
}

func (o *Or[T]) Add(m Matcher[T]) {
	o.Matchers = append(o.Matchers, m)
}

func (o *Or[T]) String() string {
	s := "(or"
	for _, f := range o.Matchers {
		s += fmt.Sprintf(" %s", f)
	}

	s += ")"
	return s
}

// Matches is the canonical in-Go function for determining if T
// matches OR match rules.
func (o *Or[T]) Matches(that T) bool {
	filters := o.Matchers
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
