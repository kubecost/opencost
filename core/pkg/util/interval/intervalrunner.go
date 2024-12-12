package interval

import (
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
		ticker := time.NewTicker(ir.interval)
		for {
			// use a select statement to receive whichever channel receives data first
			select {
			// if our stop channel receives data, it means we have explicitly called
			// Stop(), and must reset our AtomicRunState to it's initial idle state
			case <-ir.runState.OnStop():
				ticker.Stop()
				ir.runState.Reset()
				return // exit go routine

			// After our interval elapses, fall through
			case <-ticker.C:
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
