package matcher

// AllPass is a filter that matches everything and is the same as no filter. It is implemented here as a guard
// against universal operations occurring in the absence of filters.
type AllPass[T any] struct{}

func (n *AllPass[T]) String() string { return "(AllPass)" }

// Matches is the canonical in-Go function for determining if T
// matches a specific implementation's rules.
func (n *AllPass[T]) Matches(T) bool { return true }
