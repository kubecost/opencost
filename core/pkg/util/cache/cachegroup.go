package cache

import (
	"fmt"
	"sync"
	"time"

	"github.com/opencost/opencost/core/pkg/util/interval"
	"golang.org/x/sync/singleflight"
)

// cacheEntry contains a T item and the time it was added to the cache
type cacheEntry[T comparable] struct {
	item T
	ts   time.Time
}

// CacheGroup provides single flighting for grouping repeated calls for the same workload, as well
// as a cache that extends the lifetime of the returned result by a specific duration.
type CacheGroup[T comparable] struct {
	lock             sync.Mutex
	cache            map[string]*cacheEntry[T]
	group            singleflight.Group
	expirationLock   sync.Mutex
	expirationRunner *interval.IntervalRunner
	expiry           time.Duration
	max              int
}

// NewCacheGroup[T] creates a new cache group instance given the max number of keys to cache.
// If a new cache entry is added that exceeds the maximum, the oldest entry is evicted.
func NewCacheGroup[T comparable](max int) *CacheGroup[T] {
	return &CacheGroup[T]{
		cache: make(map[string]*cacheEntry[T]),
		max:   max,
	}
}

// Do accepts a group key and a factory function to execute a workload request. Any executions
// of Do() using an identical key will wait on the originating request rather than executing a
// new request, and the final result will be shared among any callers sharing the same key.
// Additionally, once returned, the workload for that key will remained cached. An expiration
// policy can be added for this cache by calling the WithExpiration method.
func (cg *CacheGroup[T]) Do(key string, factory func() (T, error)) (T, error) {
	// Check cache for existing data using the group key
	cg.lock.Lock()
	if result, ok := cg.cache[key]; ok {
		cg.lock.Unlock()
		return result.item, nil
	}
	cg.lock.Unlock()

	// single flight the group using the group key
	item, err, _ := cg.group.Do(key, func() (any, error) {
		i, err := factory()
		if err != nil {
			return nil, err
		}

		// assign cache once a result for the group key is returned
		cg.lock.Lock()
		cg.removeOldestBeyondCapacity()
		cg.cache[key] = &cacheEntry[T]{
			item: i,
			ts:   time.Now(),
		}
		cg.lock.Unlock()
		return i, nil
	})

	if err != nil {
		return defaultValue[T](), err
	}

	tItem, ok := item.(T)
	if !ok {
		return defaultValue[T](), fmt.Errorf("Failed to convert single flight result")
	}

	return tItem, nil
}

// WithExpiration assigns a cache expiration to cached entries, and  starts an eviction process,
// which runs on the specified interval.
func (cg *CacheGroup[T]) WithExpiration(expiry time.Duration, evictionInterval time.Duration) *CacheGroup[T] {
	cg.expirationLock.Lock()
	defer cg.expirationLock.Unlock()

	if cg.expirationRunner == nil {
		cg.expirationRunner = interval.NewIntervalRunner(func() {
			cg.lock.Lock()
			defer cg.lock.Unlock()

			cg.removeExpired()
		}, evictionInterval)
	}

	if cg.expirationRunner.Start() {
		cg.expiry = expiry
	}
	return cg
}

// DisableExpiration will shutdown the expiration process which allows cache entries to remain until 'max' is
// exceeded.
func (cg *CacheGroup[T]) DisableExpiration() {
	cg.expirationLock.Lock()
	defer cg.expirationLock.Unlock()

	if cg.expirationRunner == nil {
		cg.expirationRunner.Stop()
		cg.expirationRunner = nil
	}
}

// locates the oldest entry and removes it from the map. caller should lock
// prior to calling
func (cg *CacheGroup[T]) removeOldestBeyondCapacity() {
	// only remove the oldest entries if we're at max capacity
	if len(cg.cache) < cg.max {
		return
	}

	oldest := time.Now()
	oldestKey := ""

	for k, v := range cg.cache {
		if v.ts.Before(oldest) {
			oldest = v.ts
			oldestKey = k
		}
	}

	delete(cg.cache, oldestKey)
}

// removes any entries that have expired from the map. caller should lock prior
// to calling
func (cg *CacheGroup[T]) removeExpired() {
	if len(cg.cache) == 0 {
		return
	}

	now := time.Now()
	for k, v := range cg.cache {
		if now.Sub(v.ts) >= cg.expiry {
			delete(cg.cache, k)
		}
	}
}

// default value helper function to returns the initialized value for a T instance
// (ie: for value types, typically the 0 value. For pointer types, nil)
func defaultValue[T any]() T {
	var t T
	return t
}
