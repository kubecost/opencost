package util

import (
	"sync"
)

// A pool of vector maps for mapping float64 timestamps
// to float64 values
type VectorMapPool interface {
	Get() map[uint64]float64
	Put(map[uint64]float64)
}

// ------------

// A buffered channel implementation of a vector map pool which
// controls the total number of maps allowed in/out of the pool
// at any given moment. Attempting to Get() with no available
// maps will block until one is available. You will be unable to
// Put() a map if the buffer is full.
type FixedMapPool struct {
	maps chan map[uint64]float64
	size int
}

// Returns a map from the pool. Blocks if no maps are available for re-use
func (mp *FixedMapPool) Get() map[uint64]float64 {
	return <-mp.maps
}

// Adds a map back to the pool if there is room. Does not block on overflow.
func (mp *FixedMapPool) Put(m map[uint64]float64) {
	if len(mp.maps) >= mp.size {
		return
	}

	for k := range m {
		delete(m, k)
	}

	mp.maps <- m
}

// Creates a new fixed map pool which maintains a fixed pool size
func NewFixedMapPool(size int) VectorMapPool {
	mp := &FixedMapPool{
		maps: make(chan map[uint64]float64, size),
		size: size,
	}

	// Pre-Populate the buffer with maps
	for i := 0; i < size; i++ {
		mp.maps <- make(map[uint64]float64)
	}

	return mp
}

// ------------

// A buffered channel implementation of a vector map pool which
// controls the total number of maps allowed in/out of the pool
// at any given moment. Unlike the FixedMapPool, this pool will
// not block if maps are over requested, but will only maintain
// a buffer up the size limitation.
type FlexibleMapPool struct {
	maps chan map[uint64]float64
}

// Returns a map from the pool. Does not block on over-request.
func (mp *FlexibleMapPool) Get() map[uint64]float64 {
	select {
	case m := <-mp.maps:
		return m
	default:
		return make(map[uint64]float64)
	}
}

// Adds a map back to the pool if there is room. Does not block on overflow.
func (mp *FlexibleMapPool) Put(m map[uint64]float64) {
	for k := range m {
		delete(m, k)
	}

	// Either return the map to the buffered channel, or do nothing
	select {
	case mp.maps <- m:
		return
	default:
		return
	}
}

// Creates a new fixed map pool which maintains a fixed pool size
func NewFlexibleMapPool(size int) VectorMapPool {
	return &FlexibleMapPool{
		maps: make(chan map[uint64]float64, size),
	}
}

// ------------

// Implementation backed by sync.Pool
type UnboundedMapPool struct {
	maps *sync.Pool
}

// Returns a map from the pool. Does not block on over-request.
func (mp *UnboundedMapPool) Get() map[uint64]float64 {
	return mp.maps.Get().(map[uint64]float64)
}

// Adds a map back to the pool if there is room. Does not block on overflow.
func (mp *UnboundedMapPool) Put(m map[uint64]float64) {
	for k := range m {
		delete(m, k)
	}

	mp.maps.Put(m)
}

// Creates a new unbounded map pool which allows the runtime to decide when
// pooled values should be evicted
func NewUnboundedMapPool() VectorMapPool {
	return &UnboundedMapPool{
		maps: &sync.Pool{
			New: func() interface{} {
				return make(map[uint64]float64)
			},
		},
	}
}
