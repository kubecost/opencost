package defaults

// Default[T] returns the default value for any generic type. This is helpful for generic
// types where a type parameter can be a value type or pointer.
func Default[T any]() T {
	var t T
	return t
}
