package retry

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

type Obj struct {
	Name string
}

func TestPtrSliceRetry(t *testing.T) {
	t.Parallel()
	const Expected uint64 = 3

	var count uint64 = 0

	f := func() ([]*Obj, error) {
		c := atomic.AddUint64(&count, 1)
		fmt.Println("Try:", c)

		if c == Expected {
			return []*Obj{
				{"A"},
				{"B"},
				{"C"},
			}, nil
		}

		return nil, fmt.Errorf("Failed: %d", c)
	}

	objs, err := Retry(context.Background(), f, 5, time.Second)
	if err != nil {
		t.Fatalf("Failed to correctly cast back to slice type")
	}

	t.Logf("Length: %d\n", len(objs))
}

func TestSuccessRetry(t *testing.T) {
	t.Parallel()
	const Expected uint64 = 3

	var count uint64 = 0

	f := func() (any, error) {
		c := atomic.AddUint64(&count, 1)
		fmt.Println("Try:", c)

		if c == Expected {
			return nil, nil
		}

		return nil, fmt.Errorf("Failed: %d", c)
	}

	_, err := Retry(context.Background(), f, 5, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
}

func TestFailRetry(t *testing.T) {
	t.Parallel()
	const Expected uint64 = 5

	expectedError := fmt.Sprintf("Failed: %d", Expected)
	var count uint64 = 0

	f := func() (any, error) {
		c := atomic.AddUint64(&count, 1)
		fmt.Println("Try:", c)
		return nil, fmt.Errorf("Failed: %d", c)
	}

	_, err := Retry(context.Background(), f, 5, time.Second)
	if count != 5 {
		t.Fatalf("Expected Count: %d, Actual: %d", Expected, count)
	}

	if err.Error() != expectedError {
		t.Fatalf("Expected error: %s, Actual error: %s", expectedError, err.Error())
	}
}

func TestCancelRetry(t *testing.T) {
	t.Parallel()

	const Expected uint64 = 5

	var count uint64 = 0

	f := func() (any, error) {
		c := atomic.AddUint64(&count, 1)
		fmt.Println("Try:", c)
		return nil, fmt.Errorf("Failed: %d", c)
	}

	wait := make(chan error)
	ctx, cancel := context.WithCancel(context.Background())

	// execute retry in go routine
	go func() {
		_, err := Retry(ctx, f, 5, time.Second)

		wait <- err
	}()

	// cancel after 2 seconds
	go func() {
		time.Sleep(time.Second * 2)
		cancel()
	}()

	// wait for error result
	e := <-wait

	if !IsRetryCancelledError(e) {
		t.Fatalf("Expected CancellationError, got: %s", e)
	}
}
