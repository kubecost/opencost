package legacy

// AllPass is a filter that matches everything and is the same as no filter. It is implemented here as a guard
// against universal operations occurring in the absence of filters.
type AllPass[T any] struct{}

func (n AllPass[T]) String() string { return "(AllPass)" }

func (n AllPass[T]) Matches(T) bool { return true }
