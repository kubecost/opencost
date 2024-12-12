package legacy

import "fmt"

// Not negates any filter contained within it
type Not[T any] struct {
	Filter Filter[T]
}

func (n Not[T]) String() string {
	return fmt.Sprintf("(not %s)", n.Filter.String())
}

// Matches inverts the result of the child filter
func (n Not[T]) Matches(that T) bool {
	return !n.Filter.Matches(that)
}
