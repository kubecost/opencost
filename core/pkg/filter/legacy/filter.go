package legacy

// Filter represents anything that can be used to filter given generic type T.
//
// Implement this interface with caution. While it is generic, it
// is intended to be introspectable so query handlers can perform various
// optimizations. These optimizations include:
// - Routing a query to the most optimal cache
// - Querying backing data stores efficiently (e.g. translation to SQL)
//
// Custom implementations of this interface outside of this package should not
// expect to receive these benefits. Passing a custom implementation to a
// handler may in errors.
type Filter[T any] interface {
	String() string

	// Matches is the canonical in-Go function for determining if T
	// matches a filter.
	Matches(T) bool
}
