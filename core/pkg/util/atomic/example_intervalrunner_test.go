package atomic_test

import (
	"fmt"
	"sync"
	"time"

	"github.com/opencost/opencost/core/pkg/util/atomic"
)

// IntervalRunner is an example implementation of AtomicRunState.
type IntervalRunner struct {
	runState atomic.AtomicRunState
	action   func()
	interval time.Duration
}

// NewIntervalRunner Creates a new instance of an interval runner to execute the provided
// function on a designated interval until explicitly stopped.
func NewIntervalRunner(action func(), interval time.Duration) *IntervalRunner {
	return &IntervalRunner{
		action:   action,
		interval: interval,
	}
}

// Start begins the interval execution. It returns true if the interval execution successfully starts.
// It will return false if the interval execcution is already running.
func (ir *IntervalRunner) Start() bool {
	// Before we attempt to start, we must ensure we are not in a stopping state, this is a common
	// pattern that should be used with the AtomicRunState
	ir.runState.WaitForReset()

	// This will atomically check the current state to ensure we can run, then advances the state.
	// If the state is already started, it will return false.
	if !ir.runState.Start() {
		return false
	}

	// our run state is advanced, let's execute our action on the interval
	// spawn a new goroutine which will loop and wait the interval each iteration
	go func() {
		for {
			// use a select statement to receive whichever channel receives data first
			select {
			// if our stop channel receives data, it means we have explicitly called
			// Stop(), and must reset our AtomicRunState to it's initial idle state
			case <-ir.runState.OnStop():
				ir.runState.Reset()
				return // exit go routine

			// After our interval elapses, fall through
			case <-time.After(ir.interval):
			}

			// Execute the function
			ir.action()

			// Loop back to the select where we will wait for the interval to elapse
			// or an explicit stop to be called
		}
	}()

	return true
}

// Stop will explicitly stop the execution of the interval runner. If an action is already executing, it will wait
// until completion before processing the stop. Any attempts to start during the stopping phase will block until
// it's possible to Start() again
func (ir *IntervalRunner) Stop() bool {
	return ir.runState.Stop()
}

func Example_intervalRunner() {
	count := 0

	// As a general test, we'll use a goroutine which waits for a specific number of
	// ticks before calling stop, then issues a signal back to the main thread
	var wg sync.WaitGroup
	wg.Add(4)

	// Create a new IntervalRunner instance to execute our print action every second
	ir := NewIntervalRunner(
		func() {
			fmt.Printf("Tick[%d]\n", count)
			count++
			// advance the wait group count
			wg.Done()
		},
		time.Second,
	)

	// Start the runner, panic on failure
	if !ir.Start() {
		panic("Failed to start interval runner!")
	}

	// spin up a second goroutine which will wait for a specific number of
	// ticks before calling Stop(). This is a bit contrived, but demonstrates
	// multiple goroutines controlling the same interval runner.
	complete := make(chan bool)
	go func() {
		wg.Wait()

		// Stop the interval runner, notify main thread
		ir.Stop()
		complete <- true
	}()

	<-complete

	// Start immediately again using a different total tick count
	count = 0
	wg.Add(2)

	// Start the runner, panic on failure
	if !ir.Start() {
		panic("Failed to start interval runner!")
	}

	// Create a new Stop waiter
	go func() {
		wg.Wait()

		// Stop the interval runner, notify main thread
		ir.Stop()
		complete <- true
	}()

	<-complete

	// Output:
	// Tick[0]
	// Tick[1]
	// Tick[2]
	// Tick[3]
	// Tick[0]
	// Tick[1]
}
