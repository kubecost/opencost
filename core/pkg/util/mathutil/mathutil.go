package mathutil

import "math"

func Approximately(exp, act float64) bool {
	return ApproximatelyPct(exp, act, 0.0001) // within 0.1%
}

func ApproximatelyPct(exp, act, pct float64) bool {
	delta := exp * pct
	if delta < 0.00001 {
		delta = 0.00001
	}
	return math.Abs(exp-act) < delta
}
