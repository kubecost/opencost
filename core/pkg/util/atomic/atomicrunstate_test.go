package atomic

import (
	"sync"
	"testing"
	"time"
)

// NOTE: This test uses time.Sleep() in an attempt to specifically schedule concurrent actions for testing
// NOTE: Testing concurrency is hard, so if there are inconsistent results, make sure it's not just the timing
// NOTE: of the test on the testing hardware.
func TestRunState(t *testing.T) {
	t.Parallel()

	var ars AtomicRunState

	if !ars.Start() {
		t.Fatalf("Failed to Start() AtomicRunState")
	}

	if ars.Start() {
		t.Fatalf("Started AtomicRunState a second time")
	}

	success := make(chan bool)

	go func() {
		cycles := 0
		for {
			// Our test expects exactly 1 cycle, so if we exceed that, we fail!
			if cycles >= 2 {
				success <- false
				return
			}
			// create a "work" time before the select
			time.Sleep(1 * time.Second)

			select {
			case <-ars.OnStop():
				t.Logf("Stopped\n")
				ars.Reset()
				success <- true
				return
			case <-time.After(2 * time.Second):
				t.Logf("Tick\n")
			}
			cycles++
		}
	}()

	// Wait for one full work cycle (3 seconds), attempt Stop during "work" phase
	time.Sleep(3500 * time.Millisecond)
	ars.Stop()

	result := <-success
	if !result {
		t.Fatalf("Executed too many work cycles, expected 1 cycle")
	}
}

// leaks goroutines potentially, so only use in testing!
func waitChannelFor(wg *sync.WaitGroup) chan bool {
	ch := make(chan bool)
	go func() {
		wg.Wait()
		ch <- true
	}()
	return ch
}

func TestDoubleWait(t *testing.T) {
	t.Parallel()

	var ars AtomicRunState

	ars.WaitForReset()

	if !ars.Start() {
		t.Fatalf("Failed to Start() AtomicRunState")
	}

	if ars.Start() {
		t.Fatalf("Started AtomicRunState a second time")
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		t.Logf("GoRoutine 1 Waiting....")
		<-ars.OnStop()
		wg.Done()
	}()

	go func() {
		t.Logf("GoRoutine 2 Waiting....")
		<-ars.OnStop()
		wg.Done()
	}()

	time.Sleep(1 * time.Second)
	ars.Stop()
	select {
	case <-time.After(time.Second):
		t.Fatalf("Did not receive signal from both go routines after a second\n")
		return
	case <-waitChannelFor(&wg):
		t.Logf("Received signals from both go routines\n")
	}
	ars.Reset()
}

func TestContinuousConcurrentStartsAndStops(t *testing.T) {
	t.Parallel()

	const cycles = 5

	var ars AtomicRunState
	started := make(chan bool)

	var wg sync.WaitGroup
	wg.Add(cycles)

	// continuously try and start the ars on a tight loop
	// throttled by OnStop and WaitForReset()
	go func() {
		defer func() {
			if e := recover(); e != nil {
				// sometimes the waitgroup will hit a negative value at the end of the test
				// this is ok given the way the test behaves (chaos star/stop calls), so
				// we can safely ignore.
			}
		}()

		firstCycle := true
		for {
			ars.WaitForReset()
			if ars.Start() {
				t.Logf("Started")
				if firstCycle {
					firstCycle = false
					started <- true
				}
				wg.Done()
			}

			<-ars.OnStop()
			t.Logf("Stopped")
		}
	}()

	// wait for an initial start
	<-started

	// Loop Stop/Resets from other goroutines
	go func() {
		for {
			time.Sleep(100 * time.Millisecond)
			if ars.Stop() {
				<-ars.OnStop()
				time.Sleep(500 * time.Millisecond)
				ars.Reset()
			}
		}
	}()

	// Wait for full cycles
	select {
	case <-time.After(5 * time.Second):
		t.Fatalf("Didn't complete %d cycles after 10 seconds", cycles)
	case <-waitChannelFor(&wg):
		t.Logf("Completed!")
	}
}
