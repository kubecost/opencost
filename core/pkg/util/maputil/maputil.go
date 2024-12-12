package maputil

// Map applies a transformation function to each value within a map to get a new map containing the
// transformed values.
func Map[K comparable, V any, T any](m map[K]V, transform func(V) T) map[K]T {
	result := make(map[K]T, len(m))
	for k, v := range m {
		result[k] = transform(v)
	}
	return result
}
