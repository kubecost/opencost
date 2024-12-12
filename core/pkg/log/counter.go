package log

import "sync"

type counter struct {
	mu   sync.RWMutex
	seen map[string]int
}

func newCounter() *counter {
	return &counter{seen: map[string]int{}}
}

func (ctr *counter) count(key string) int {
	ctr.mu.RLock()
	defer ctr.mu.RUnlock()
	return ctr.seen[key]
}

func (ctr *counter) delete(key string) {
	ctr.mu.Lock()
	delete(ctr.seen, key)
	ctr.mu.Unlock()
}

func (ctr *counter) increment(key string) int {
	ctr.mu.Lock()
	defer ctr.mu.Unlock()
	ctr.seen[key]++
	return ctr.seen[key]
}
