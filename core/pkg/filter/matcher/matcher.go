package matcher

// Matcher represents anything that can be used to match against given generic type T.
type Matcher[T any] interface {
	String() string

	// Matches is the canonical in-Go function for determining if T
	// matches a specific implementation's rules.
	Matches(T) bool
}

// MatcherGroup is useful for dynamically creating group based matchers.
type MatcherGroup[T any] interface {
	Matcher[T]

	Add(Matcher[T])
}
