package log

import (
	"sync"
	"testing"
)

func TestCounter_Ops(t *testing.T) {
	ctr := newCounter()

	var num int

	// Should return 0 if never seen
	num = ctr.count("something")
	if num != 0 {
		t.Fatalf("counter: count: expected %d; found %d", 0, num)
	}

	// Should return 1 if seen once
	num = ctr.increment("something")
	if num != 1 {
		t.Fatalf("counter: count: expected %d; found %d", 1, num)
	}

	// Should still return 1 if seen only once
	num = ctr.count("something")
	if num != 1 {
		t.Fatalf("counter: count: expected %d; found %d", 1, num)
	}

	// Should return 1 if seen once
	for i := 2; i <= 234; i++ {
		num = ctr.increment("something")
		if num != i {
			t.Fatalf("counter: count: expected %d; found %d", i, num)
		}
	}

	ctr.delete("something")
	num = ctr.count("something")
	if num != 0 {
		t.Fatalf("counter: count: expected %d; found %d", 0, num)
	}
}

func TestCounter_Threadsafety(t *testing.T) {
	var wg sync.WaitGroup

	// Run 1000 goroutines, logging 10000 times each as fast as they can
	for i := 1; i <= 1000; i++ {
		wg.Add(1)
		go func() {
			for j := 1; j <= 10000; j++ {
				DedupedInfof(10, "this log seen %d times", j)
			}
			wg.Done()
		}()
	}

	wg.Wait()
}

func TestDeduping(t *testing.T) {
	// Should log 10 times, then stop
	for i := 1; i <= 234; i++ {
		DedupedInfof(10, "this log seen %d times", i)
	}
}
