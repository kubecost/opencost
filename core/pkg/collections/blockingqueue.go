package collections

import (
	"sync"
)

//--------------------------------------------------------------------------
//  BlockingQueue
//--------------------------------------------------------------------------

// BlockingQueue is a queue backed by a slice which blocks if dequeueing while empty.
// This data structure should use a pool of worker goroutines to await work.
type BlockingQueue[T any] interface {
	// Enqueue pushes an item onto the queue
	Enqueue(item T)

	// Dequeue removes the first item from the queue and returns it.
	Dequeue() T

	// TryDequeue attempts to remove the first item from the queue and return it. This
	// method does not block, and instead, returns true if the item was available and false
	// otherwise
	TryDequeue() (T, bool)

	// Each blocks modification and allows iteration of the queue.
	Each(f func(int, T))

	// Length returns the length of the queue
	Length() int

	// IsEmpty returns true if the queue is empty
	IsEmpty() bool

	// Clear empties the queue
	Clear()
}

// blockingSliceQueue is an implementation of BlockingQueue which uses a slice for storage.
type blockingSliceQueue[T any] struct {
	q        []T
	l        *sync.Mutex
	nonEmpty *sync.Cond
}

// NewBlockingQueue returns a new BlockingQueue implementation
func NewBlockingQueue[T any]() BlockingQueue[T] {
	l := new(sync.Mutex)

	return &blockingSliceQueue[T]{
		q:        []T{},
		l:        l,
		nonEmpty: sync.NewCond(l),
	}
}

// Enqueue pushes an item onto the queue
func (q *blockingSliceQueue[T]) Enqueue(item T) {
	q.l.Lock()
	defer q.l.Unlock()

	q.q = append(q.q, item)
	q.nonEmpty.Broadcast()
}

// Dequeue removes the first item from the queue and returns it.
func (q *blockingSliceQueue[T]) Dequeue() T {
	q.l.Lock()
	defer q.l.Unlock()

	// need to tight loop here to ensure only one thread wins and
	// others wait again
	for len(q.q) == 0 {
		q.nonEmpty.Wait()
	}

	e := q.q[0]

	// nil 0 index to prevent leak
	q.q[0] = defaultValue[T]()
	q.q = q.q[1:]
	return e
}

// TryDequeue attempts to remove the first item from the queue and return it. This
// method does not block, and instead, returns true if the item was available and false
// otherwise
func (q *blockingSliceQueue[T]) TryDequeue() (T, bool) {
	q.l.Lock()
	defer q.l.Unlock()

	if len(q.q) == 0 {
		return defaultValue[T](), false
	}

	e := q.q[0]

	// default 0 index to prevent leak
	q.q[0] = defaultValue[T]()
	q.q = q.q[1:]
	return e, true
}

// Each blocks modification and allows iteration of the queue.
func (q *blockingSliceQueue[T]) Each(f func(int, T)) {
	q.l.Lock()
	defer q.l.Unlock()

	for i, entry := range q.q {
		f(i, entry)
	}
}

// Length returns the length of the queue
func (q *blockingSliceQueue[T]) Length() int {
	q.l.Lock()
	defer q.l.Unlock()

	return len(q.q)
}

// IsEmpty returns true if the queue is empty
func (q *blockingSliceQueue[T]) IsEmpty() bool {
	return q.Length() == 0
}

// Clear empties the queue
func (q *blockingSliceQueue[T]) Clear() {
	q.l.Lock()
	defer q.l.Unlock()

	// seems optimal here to create a new underlying slice/array to
	// avoid capacity ballooning, but does feel like an implementation
	// specific detail -- we can revisit if there are other relevant
	// use-cases
	q.q = []T{}
}

// defaultValue returns the language default for the T type
func defaultValue[T any]() T {
	var t T
	return t
}
