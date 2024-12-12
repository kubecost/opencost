package matcher

// AllCut is a matcher that matches nothing. This is useful
// for applications like authorization, where a user/group/role may be disallowed
// from viewing data entirely.
type AllCut[T any] struct{}

// String returns the string representation of the matcher instance
func (ac *AllCut[T]) String() string { return "(AllCut)" }

// Matches is the canonical in-Go function for determining if T
// matches a specific implementation's rules.
func (ac *AllCut[T]) Matches(T) bool { return false }
