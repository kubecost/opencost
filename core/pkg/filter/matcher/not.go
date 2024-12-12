package matcher

import "fmt"

// Not negates any filter contained within it
type Not[T any] struct {
	Matcher Matcher[T]
}

func (n *Not[T]) Add(m Matcher[T]) {
	n.Matcher = m
}

func (n *Not[T]) String() string {
	return fmt.Sprintf("(not %s)", n.Matcher.String())
}

// Matches inverts the result of the child matcher
func (n *Not[T]) Matches(that T) bool {
	return !n.Matcher.Matches(that)
}
