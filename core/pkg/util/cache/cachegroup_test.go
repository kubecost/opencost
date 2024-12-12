package cache

import (
	"sync"
	"testing"
	"time"
)

type Obj struct {
	Value int
}

func TestGroupCacheSingleFlighting(t *testing.T) {
	g := NewCacheGroup[*Obj](3)

	factory := func() (*Obj, error) {
		time.Sleep(2 * time.Second)
		return &Obj{10}, nil
	}

	next := make(chan struct{})
	done := make(chan struct{})

	go func() {
		now := time.Now()
		o, _ := g.Do("a", func() (*Obj, error) {
			next <- struct{}{}
			return factory()
		})
		t.Logf("Took: %d ms, Obj Value: %d\n", time.Now().Sub(now).Milliseconds(), o.Value)
	}()

	go func() {
		<-next

		time.Sleep(1 * time.Second)

		now := time.Now()
		o, _ := g.Do("a", factory)
		delta := time.Now().Sub(now)
		t.Logf("Other Go Routine Took: %d ms, Obj Value: %d\n", delta.Milliseconds(), o.Value)

		if delta > (time.Duration(1250 * time.Millisecond)) {
			t.Errorf("Delta Time > 1250ms. Delta: %d, Expected 1000ms\n", delta)
		}
		done <- struct{}{}
	}()

	<-done
}

func TestGroupCacheAfterSingleFlighting(t *testing.T) {
	g := NewCacheGroup[*Obj](3)

	factory := func() (*Obj, error) {
		time.Sleep(2 * time.Second)
		return &Obj{10}, nil
	}

	next := make(chan struct{})
	done := make(chan struct{})

	go func() {
		now := time.Now()
		o, _ := g.Do("a", func() (*Obj, error) {
			next <- struct{}{}
			return factory()
		})
		t.Logf("Took: %d ms, Obj Value: %d\n", time.Now().Sub(now).Milliseconds(), o.Value)
	}()

	go func() {
		<-next
		// wait the full 2 seconds and then some, which will ensure we are no longer
		// single flighting, and should reach into the cache
		time.Sleep(2500 * time.Millisecond)

		now := time.Now()
		o, _ := g.Do("a", factory)
		delta := time.Now().Sub(now)
		t.Logf("Other Go Routine Took: %d ms, Obj Value: %d\n", delta.Milliseconds(), o.Value)

		if delta > (time.Duration(1250 * time.Millisecond)) {
			t.Errorf("Delta Time > 1250ms. Delta: %d, Expected 1000ms\n", delta)
		}

		done <- struct{}{}
	}()

	<-done
}

func TestGroupCacheMany(t *testing.T) {
	// Apologies this test can be difficult to follow. (Concurrent tests are hard)
	// The idea here is that we test a "request" that takes 1 second to return an
	// Obj{10} result (factory).
	// * To test the single flight behavior, we make a series of requests that will
	//   happen while the initial request is in flight.
	// * The second half of requests will be made after the original request returns
	//   to ensure that we pull from cache.
	// * The failure case is if all of these actions takes too long to execute, which
	//   _should_ indicate a deadlock or problem with the API.
	g := NewCacheGroup[*Obj](3).WithExpiration(10*time.Second, 5*time.Second)

	factory := func() (*Obj, error) {
		time.Sleep(1 * time.Second)
		return &Obj{10}, nil
	}

	next := make(chan struct{})

	go func() {
		now := time.Now()
		o, _ := g.Do("a", func() (*Obj, error) {
			next <- struct{}{}
			return factory()
		})
		t.Logf("Took: %d ms, Obj Value: %d\n", time.Now().Sub(now).Milliseconds(), o.Value)
	}()

	<-next
	var wg sync.WaitGroup
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func(ii int) {
			t.Logf("Created Go Routine: %d\n", ii)
			now := time.Now()
			o, _ := g.Do("a", factory)
			delta := time.Now().Sub(now)
			t.Logf("Go Routine[%d] Took: %d ms, Obj Value: %d\n", ii, delta.Milliseconds(), o.Value)
			wg.Done()
		}(i)
		time.Sleep(250 * time.Millisecond)
	}

	select {
	case <-waitChannelFor(&wg):
		t.Logf("Successfully returned values for all requests.")
	case <-time.After(time.Second * 8):
		t.Logf("Failed to complete after 8 second timeout")
	}
}

func TestCacheGroupExpirationPolicy(t *testing.T) {
	g := NewCacheGroup[*Obj](3).WithExpiration(2*time.Second, time.Second)
	g.Do("a", func() (*Obj, error) {
		return &Obj{10}, nil
	})

	time.Sleep(2100 * time.Millisecond)
	if len(g.cache) > 0 {
		t.Errorf("Expected cache to be empty (expired). Cache length was: %d\n", len(g.cache))
	}
}

func TestCacheGroupMaxRollOff(t *testing.T) {
	g := NewCacheGroup[*Obj](3)

	g.Do("a", func() (*Obj, error) {
		return &Obj{1}, nil
	})

	g.Do("b", func() (*Obj, error) {
		return &Obj{1}, nil
	})

	g.Do("c", func() (*Obj, error) {
		return &Obj{1}, nil
	})

	g.Do("d", func() (*Obj, error) {
		return &Obj{1}, nil
	})

	if _, ok := g.cache["a"]; ok {
		t.Errorf("Expected 'a' group cache to be evicted")
	}
}

func waitChannelFor(wg *sync.WaitGroup) <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		wg.Wait()
		ch <- struct{}{}
	}()
	return ch
}
