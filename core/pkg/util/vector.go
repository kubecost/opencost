package util

import (
	"math"
	"sort"
)

type Vector struct {
	Timestamp float64 `json:"timestamp"`
	Value     float64 `json:"value"`
}

const MapPoolSize = 4

type VectorSlice []*Vector

func (p VectorSlice) Len() int           { return len(p) }
func (p VectorSlice) Less(i, j int) bool { return p[i].Timestamp < p[j].Timestamp }
func (p VectorSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

var mapPool VectorMapPool = NewFlexibleMapPool(MapPoolSize)

// roundTimestamp rounds the given timestamp to the given precision; e.g. a
// timestamp given in seconds, rounded to precision 10, will be rounded
// to the nearest value dividible by 10 (24 goes to 20, but 25 goes to 30).
func roundTimestamp(ts float64, precision float64) float64 {
	return math.Round(ts/precision) * precision
}

// Makes a reasonable guess at capacity for vector join
func capacityFor(xvs []*Vector, yvs []*Vector) int {
	x := len(xvs)
	y := len(yvs)

	if x >= y {
		return x + (y / 4)
	}

	return y + (x / 4)
}

// ApplyVectorOp accepts two vectors, synchronizes timestamps, and executes an operation
// on each vector. See VectorJoinOp for details.
func ApplyVectorOp(xvs []*Vector, yvs []*Vector, op VectorJoinOp) []*Vector {
	// if xvs is empty, return yvs
	if xvs == nil || len(xvs) == 0 {
		return yvs
	}

	// if yvs is empty, return xvs
	if yvs == nil || len(yvs) == 0 {
		return xvs
	}

	// timestamps contains the vector slice after joining xvs and yvs
	var timestamps []*Vector

	// turn each vector slice into a map of timestamp-to-value so that
	// values at equal timestamps can be lined-up and summed
	xMap := mapPool.Get()
	defer mapPool.Put(xMap)

	for _, xv := range xvs {
		if xv.Timestamp == 0 {
			continue
		}

		// round all non-zero timestamps to the nearest 10 second mark
		xv.Timestamp = roundTimestamp(xv.Timestamp, 10.0)

		xMap[uint64(xv.Timestamp)] = xv.Value
		timestamps = append(timestamps, &Vector{
			Timestamp: xv.Timestamp,
		})
	}

	yMap := mapPool.Get()
	defer mapPool.Put(yMap)

	for _, yv := range yvs {
		if yv.Timestamp == 0 {
			continue
		}

		// round all non-zero timestamps to the nearest 10 second mark
		yv.Timestamp = roundTimestamp(yv.Timestamp, 10.0)

		yMap[uint64(yv.Timestamp)] = yv.Value
		if _, ok := xMap[uint64(yv.Timestamp)]; !ok {
			// no need to double add, since we'll range over sorted timestamps and check.
			timestamps = append(timestamps, &Vector{
				Timestamp: yv.Timestamp,
			})
		}
	}

	// iterate over each timestamp to produce a final op vector slice
	// reuse the existing slice to reduce allocations
	result := timestamps[:0]
	for _, sv := range timestamps {
		x, okX := xMap[uint64(sv.Timestamp)]
		y, okY := yMap[uint64(sv.Timestamp)]

		if op(sv, VectorValue(x, okX), VectorValue(y, okY)) {
			result = append(result, sv)
		}
	}

	// nil out remaining vectors in timestamps to GC
	for i := len(result); i < len(timestamps); i++ {
		timestamps[i] = nil
	}

	sort.Sort(VectorSlice(result))

	return result
}

// VectorJoinOp is an operation func that accepts a result vector pointer
// for a specific timestamp and two float64 pointers representing the
// input vectors for that timestamp. x or y inputs can be nil, but not
// both. The op should use x and y values to set the Value on the result
// ptr. If a result could not be generated, the op should return false,
// which will omit the vector for the specific timestamp. Otherwise,
// return true denoting a successful op.
type VectorJoinOp func(result *Vector, x *float64, y *float64) bool

// returns a nil ptr or valid float ptr based on the ok bool
func VectorValue(v float64, ok bool) *float64 {
	if !ok {
		return nil
	}

	return &v
}

// NormalizeVectorByVector produces a version of xvs (a slice of Vectors)
// which has had its timestamps rounded and its values divided by the values
// of the Vectors of yvs, such that yvs is the "unit" Vector slice.
func NormalizeVectorByVector(xvs []*Vector, yvs []*Vector) []*Vector {
	normalizeOp := func(result *Vector, x *float64, y *float64) bool {
		if x != nil && y != nil && *y != 0 {
			result.Value = *x / *y
		} else if x != nil {
			result.Value = *x
		} else if y != nil {
			result.Value = 0
		}

		return true
	}

	return ApplyVectorOp(xvs, yvs, normalizeOp)
}
