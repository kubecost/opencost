package sliceutil

// Map accepts a slice of T and applies a transformation function to each index of a
// slice, which are inserted into a new slice of type U.
func Map[T any, U any](s []T, transform func(T) U) []U {
	result := make([]U, len(s))
	for i := 0; i < len(s); i++ {
		result[i] = transform(s[i])
	}
	return result
}
