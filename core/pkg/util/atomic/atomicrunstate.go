package atomic

import (
	"sync"
)

// AtomicRunState can be used to provide thread-safe start/stop functionality to internal run-loops
// inside a goroutine.
type AtomicRunState struct {
	lock     sync.Mutex
	stopping bool
	stop     chan struct{}
	reset    chan struct{}
}

// Start checks for an existing run state and returns false if the run state has already started. If
// the run state has not started, then it will advance to the started state and return true.
func (ars *AtomicRunState) Start() bool {
	ars.lock.Lock()
	defer ars.lock.Unlock()

	if ars.stop != nil {
		return false
	}

	ars.stop = make(chan struct{})
	return true
}

// OnStop returns a channel that should be used within a select goroutine run loop. It is set to signal
// whenever Stop() is executed. Once the channel is signaled, Reset() should be called if the runstate
// is to be used again.
func (ars *AtomicRunState) OnStop() <-chan struct{} {
	ars.lock.Lock()
	defer ars.lock.Unlock()

	return ars.stop
}

// Stops closes the stop channel triggering any selects waiting for OnStop()
func (ars *AtomicRunState) Stop() bool {
	ars.lock.Lock()
	defer ars.lock.Unlock()

	if !ars.stopping && ars.stop != nil {
		ars.stopping = true
		ars.reset = make(chan struct{})
		close(ars.stop)
		return true
	}

	return false
}

// Reset should be called in the select case for OnStop(). Note that calling Reset() prior to
// selecting OnStop() will result in failed Stop signal receive.
func (ars *AtomicRunState) Reset() {
	ars.lock.Lock()
	defer ars.lock.Unlock()

	close(ars.reset)
	ars.stopping = false
	ars.stop = nil
}

// IsRunning returns true if the state is running or in the process of stopping.
func (ars *AtomicRunState) IsRunning() bool {
	ars.lock.Lock()
	defer ars.lock.Unlock()

	return ars.stop != nil
}

// IsStopping returns true if the run state has been stopped, but not yet reset.
func (ars *AtomicRunState) IsStopping() bool {
	ars.lock.Lock()
	defer ars.lock.Unlock()

	return ars.stopping && ars.stop != nil
}

// WaitForStop will wait for a stop to occur IFF the run state is in the process of stopping.
func (ars *AtomicRunState) WaitForReset() {
	if ars.IsStopping() {
		<-ars.reset
	}
}
