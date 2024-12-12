package worker

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/opencost/opencost/core/pkg/collections"
)

// Runner is a function type that takes a single input and returns nothing.
type Runner[T any] func(T)

// Worker is a transformation function from input type T to output type U.
type Worker[T any, U any] func(T) U

// WorkerPool is a pool of go routines executing a Worker on supplied inputs via
// the Run function.
type WorkerPool[T any, U any] interface {
	// Run executes a Worker in the pool on the provided input and onComplete receive chanel
	// to get the results. An error is returned if the pool is shutdown, or is in the process
	// of shutting down.
	Run(input T, onComplete chan<- U) error

	// Shutdown stops all of the workers (if running).
	Shutdown()
}

// WorkGroup is a group of inputs that leverage a WorkerPool to run inputs through workers and
// collect the results in a single slice.
type WorkGroup[T any, U any] interface {
	// Push adds a new input to the work group.
	Push(T) error

	// Wait waits for all pending worker tasks to complete, then returns all the results.
	Wait() []U
}

// entry is an internal helper type for pushing payloads to the worker queue
type entry[T any, U any] struct {
	payload    T
	onComplete chan<- U
	close      bool
}

// queuedWorkerPool is a blocking queue based implementation of a WorkerPool
type queuedWorkerPool[T any, U any] struct {
	queue      collections.BlockingQueue[entry[T, U]]
	work       Worker[T, U]
	workers    int
	isShutdown atomic.Bool
}

// ordered is a WorkGroup implementation which enforces ordering based on when
// inputs were pushed onto the group.
type ordered[T any, U any] struct {
	workPool WorkerPool[T, U]
	results  []U
	wg       sync.WaitGroup
	count    int
}

// NewWorkerPool creates a new worker pool provided the number of workers to run as well as the worker
// func used to transform inputs to outputs.
func NewWorkerPool[T any, U any](workers int, work Worker[T, U]) WorkerPool[T, U] {
	owq := &queuedWorkerPool[T, U]{
		workers: workers,
		work:    work,
		queue:   collections.NewBlockingQueue[entry[T, U]](),
	}

	// startup the designated workers
	for i := 0; i < workers; i++ {
		go owq.worker()
	}

	return owq
}

// Run executes a Worker in the pool on the provided input and onComplete receive chanel
// to get the results. An error is returned if the pool is shutdown, or is in the process
// of shutting down.
func (wq *queuedWorkerPool[T, U]) Run(input T, onComplete chan<- U) error {
	if wq.isShutdown.Load() {
		return fmt.Errorf("WorkerPoolShutdown")
	}

	wq.queue.Enqueue(entry[T, U]{
		payload:    input,
		onComplete: onComplete,
		close:      false,
	})

	return nil
}

// Shutdown stops all of the workers (if running).
func (wq *queuedWorkerPool[T, U]) Shutdown() {
	if !wq.isShutdown.CompareAndSwap(false, true) {
		return
	}

	for i := 0; i < wq.workers; i++ {
		wq.queue.Enqueue(entry[T, U]{
			close: true,
		})
	}
}

func (wq *queuedWorkerPool[T, U]) worker() {
	for {
		next := wq.queue.Dequeue()

		// shutdown the worker on sentinel value
		if next.close {
			return
		}

		result := wq.work(next.payload)

		// signal on complete if applicable
		if next.onComplete != nil {
			next.onComplete <- result
		}
	}
}

// NewGroup creates a new WorkGroup implementation for processing a group of inputs in the order in which
// they are pushed. Ordered groups do not support concurrent Push() calls.
func NewOrderedGroup[T any, U any](pool WorkerPool[T, U], size int) WorkGroup[T, U] {
	return &ordered[T, U]{
		workPool: pool,
		results:  make([]U, size),
		count:    0,
	}
}

// Push adds a new input to the work group.
func (ow *ordered[T, U]) Push(input T) error {
	current := ow.count
	if current >= len(ow.results) {
		return fmt.Errorf("MaxCapacity")
	}

	onComplete := make(chan U)
	err := ow.workPool.Run(input, onComplete)
	if err != nil {
		return err
	}

	ow.count++
	ow.wg.Add(1)

	go func(index int) {
		defer close(onComplete)

		ow.results[index] = <-onComplete
		ow.wg.Done()
	}(int(current))

	return nil
}

// Wait waits for all pending worker tasks to complete, then returns all the results.
func (ow *ordered[T, U]) Wait() []U {
	ow.wg.Wait()
	return ow.results
}

// noResultGroup is a WorkGroup implementation which arbitrarily pushes inputs to
// a runner pool to be executed concurrently. This group does not collect results.
type noResultGroup[T any] struct {
	workPool WorkerPool[T, struct{}]
	wg       sync.WaitGroup
}

// NewNoResultGroup creates a new WorkGroup implementation for processing a group of inputs concurrently. This
// work group implementation does not collect results, and therefore, requires a worker pool with a struct{} output.
func NewNoResultGroup[T any](pool WorkerPool[T, struct{}]) WorkGroup[T, struct{}] {
	return &noResultGroup[T]{
		workPool: pool,
	}
}

// Push adds a new input to the work group.
func (ow *noResultGroup[T]) Push(input T) error {
	onComplete := make(chan struct{})
	err := ow.workPool.Run(input, onComplete)
	if err != nil {
		return err
	}

	ow.wg.Add(1)

	go func() {
		defer close(onComplete)
		defer ow.wg.Done()

		<-onComplete
	}()

	return nil
}

// Wait waits for all pending worker tasks to complete, then returns all the results.
func (ow *noResultGroup[T]) Wait() []struct{} {
	ow.wg.Wait()
	return []struct{}{}
}

// collector is a WorkGroup implementation which collects non-nil results into the results slice
// and ignores any nil results.
type collector[T any, U any] struct {
	workPool   WorkerPool[T, *U]
	resultLock sync.Mutex
	results    []*U
	wg         sync.WaitGroup
}

// NewCollectionGroup creates a new WorkGroup implementation for processing a group of inputs concurrently. The
// collection group implementation will collect all non-nil results into the output slice. Thus, the worker pool
// parameter requires the output type to be a pointer.
func NewCollectionGroup[T any, U any](pool WorkerPool[T, *U]) WorkGroup[T, *U] {
	return &collector[T, U]{
		workPool: pool,
	}
}

// Push adds a new input to the work group.
func (ow *collector[T, U]) Push(input T) error {
	onComplete := make(chan *U)
	err := ow.workPool.Run(input, onComplete)
	if err != nil {
		return err
	}

	ow.wg.Add(1)

	go func() {
		defer ow.wg.Done()
		defer close(onComplete)

		result := <-onComplete
		if result != nil {
			ow.resultLock.Lock()
			ow.results = append(ow.results, result)
			ow.resultLock.Unlock()
		}
	}()

	return nil
}

// Wait waits for all pending worker tasks to complete, then returns all the results.
func (ow *collector[T, U]) Wait() []*U {
	ow.wg.Wait()
	return ow.results
}

// these constraints protect against the possibility of unexpected output from runtime.NumCPU()
const (
	defaultMinWorkers = 4
	defaultMaxWorkers = 16
)

// OptimalWorkerCount will return an optimal worker count based on runtime.NumCPU()
func OptimalWorkerCount() int {
	return OptimalWorkerCountInRange(defaultMinWorkers, defaultMaxWorkers)
}

// OptimalWorkerCount will return runtime.NumCPU() constrained to the provided min and max
// range
func OptimalWorkerCountInRange(min int, max int) int {
	cores := runtime.NumCPU()
	if cores < min {
		return min
	}
	if cores > max {
		return max
	}
	return cores
}

// ConcurrentDo runs a pool of N workers which concurrently call the provided worker func on each
// input to get ordered output corresponding to the inputs. The total number of workers is determined
// by the total number of CPUs available, bound to a range from 4-16.
func ConcurrentDo[T any, U any](worker Worker[T, U], inputs []T) []U {
	return ConcurrentDoWith(OptimalWorkerCount(), worker, inputs)
}

// ConcurrentDoWith runs a pool of workers of the specified size which concurrently call the provided worker func
// on each input to get ordered output corresponding to the inputs. Size inputs < 1 will automatically be set to 1.
func ConcurrentDoWith[T any, U any](size int, worker Worker[T, U], inputs []T) []U {
	if size < 1 {
		size = 1
	}

	workerPool := NewWorkerPool(size, worker)
	defer workerPool.Shutdown()

	workGroup := NewOrderedGroup(workerPool, len(inputs))
	for _, input := range inputs {
		workGroup.Push(input)
	}

	return workGroup.Wait()
}

// ConcurrentCollect runs a pool of N workers which concurrently call the provided worker func on each
// input to get a result slice of non-nil outputs. The total number of workers is determined
// by the total number of CPUs available, bound to a range from 4-16.
func ConcurrentCollect[T any, U any](workerFunc Worker[T, *U], inputs []T) []*U {
	return ConcurrentCollectWith(OptimalWorkerCount(), workerFunc, inputs)
}

// ConcurrentCollectWith runs a pool of workers of the specified size which concurrently call the provided worker
// func on each input to get a result slice of non-nil outputs. Size inputs < 1 will automatically be set to 1.
func ConcurrentCollectWith[T any, U any](size int, workerFunc Worker[T, *U], inputs []T) []*U {
	if size < 1 {
		size = 1
	}

	workerPool := NewWorkerPool(size, workerFunc)
	defer workerPool.Shutdown()

	workGroup := NewCollectionGroup(workerPool)
	for _, input := range inputs {
		workGroup.Push(input)
	}

	return workGroup.Wait()
}

// ConcurrentRun runs a pool of N workers which concurrently call the provided runner func on each
// input. The total number of workers is determined by the total number of CPUs available, bound to
// a range from 4-16.
func ConcurrentRun[T any](runner Runner[T], inputs []T) {
	ConcurrentRunWith(OptimalWorkerCount(), runner, inputs)
}

// ConcurrentRunWith runs a pool of runners of the specified size which concurrently call the provided runner
// func on each input. Size inputs < 1 will automatically be set to 1.
func ConcurrentRunWith[T any](size int, runner Runner[T], inputs []T) {
	if size < 1 {
		size = 1
	}

	workerPool := NewWorkerPool(size, func(input T) (void struct{}) {
		runner(input)
		return
	})

	workGroup := NewNoResultGroup(workerPool)
	for _, input := range inputs {
		workGroup.Push(input)
	}

	workGroup.Wait()
}
