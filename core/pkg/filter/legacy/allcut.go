package legacy

// AllCut is a filter that matches nothing. This is useful
// for applications like authorization, where a user/group/role may be disallowed
// from viewing data entirely.
type AllCut[T any] struct{}

func (ac AllCut[T]) String() string { return "(AllCut)" }

func (ac AllCut[T]) Matches(T) bool { return false }
