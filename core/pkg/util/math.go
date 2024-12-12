package util

import "math"

// IsApproximately returns true is a approximately equals b, within
// a delta computed as a function of the size of a and b.
func IsApproximately(a, b float64) bool {
	delta := 0.000001 * math.Max(math.Abs(a), math.Abs(b))
	return math.Abs(a-b) <= delta
}

// IsWithin returns true if a and b are within delta of each other
func IsWithin(a, b, delta float64) bool {
	return math.Abs(a-b) <= delta
}
