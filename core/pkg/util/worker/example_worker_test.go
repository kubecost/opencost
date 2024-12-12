package worker_test

import (
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/util/worker"
)

// slowAddTenToFloat simulates "work" -- it accepts an integer, adds 10, converts it to a float64,
// waits 1 second, then returns the result.
func slowAddTenToFloat(i int) float64 {
	result := float64(i + 10)
	time.Sleep(time.Second)
	return result
}

func Example_concurrentWorkers() {
	// Assuming we have a list of ints we want to pass to slowAddTenToFloat(),
	// rather than serially calling the function on each input (requiring a wait
	// of 1 second between calls), we'll want to execute each in a goroutine. Let's
	// say we had 100 inputs, we may not want to create that many go routines, so
	// instead, we can create a pool of goroutines that work on our inputs as fast as
	// possible.

	// Create a worker pool using 50 goroutines:
	workerPool := worker.NewWorkerPool(50, slowAddTenToFloat)

	// we want to shutdown the workerPool at the end of it's use to ensure we don't
	// leak go routines
	defer workerPool.Shutdown()

	// Loop over 100 inputs and run slowAddTenToFloat
	for i := 0; i < 100; i++ {
		// Run accepts a receive channel for each input, but it is not required.
		// To demonstrate receiving, we'll receive the results when the input
		// is 50:
		if i == 50 {
			receive := make(chan float64)
			workerPool.Run(i, receive)

			// since we don't want to slow down the input loop, let's receive the
			// result in a separate go routine
			go func(input int, rec chan float64) {
				defer close(rec)
				result := <-rec
				fmt.Printf("Receive Result: %.2f for Input: %d\n", result, input)
			}(i, receive)
		} else {
			// pass nil if receiving the result isn't necessary
			workerPool.Run(i, nil)
		}
	}

	// 100 inputs with 50 go routines should take 2 seconds, so let's wait a bit longer than that
	time.Sleep((2 * time.Second) + (500 * time.Millisecond))

	// Output:
	// Receive Result: 60.00 for Input: 50
}

func Example_concurrentOrdered() {
	// Expanding on the previous idea, let's assume that we want to receive the result for
	// every input. That would normally require some specialized synchronization and boilerplate,
	// but the worker package contains a ordered group type for exactly this functionality

	// This time, let's create a worker pool and use the MAXGOPROCS value to determine the number
	// of workers
	workerCount := worker.OptimalWorkerCount()
	workerPool := worker.NewWorkerPool(workerCount, slowAddTenToFloat)

	// Shutdown the worker pool when complete
	defer workerPool.Shutdown()

	// now we can create our ordered group type and pass in the worker pool, and since we know our
	// number of inputs (let's choose 12 this time), we can pass that to the group as well.
	const numInputs = 12
	orderedGroup := worker.NewOrderedGroup(workerPool, numInputs)

	// loop over our inputs and pass them to the group
	for i := 0; i < numInputs; i++ {
		// ordered group has a strict size constraint (set in the NewOrderedGroup func), and will
		// error if the number of inputs pushed exceeds that size constraint
		err := orderedGroup.Push(i)
		if err != nil {
			panic(err)
		}
	}

	// now we can simply call Wait() to receive the results
	results := orderedGroup.Wait()

	// Note that the order of the results is consistent with the order in which they were pushed
	for idx, result := range results {
		fmt.Printf("Received Result: %.2f for Input: %d\n", result, idx)
	}

	// Output:
	// Received Result: 10.00 for Input: 0
	// Received Result: 11.00 for Input: 1
	// Received Result: 12.00 for Input: 2
	// Received Result: 13.00 for Input: 3
	// Received Result: 14.00 for Input: 4
	// Received Result: 15.00 for Input: 5
	// Received Result: 16.00 for Input: 6
	// Received Result: 17.00 for Input: 7
	// Received Result: 18.00 for Input: 8
	// Received Result: 19.00 for Input: 9
	// Received Result: 20.00 for Input: 10
	// Received Result: 21.00 for Input: 11
}

func Example_concurrentOrderedSimple() {
	// This last example highlights a simplified version of the previous example. While
	// the ordered example provides tuning knobs for total goroutines and allows pushing
	// data dynamically, it can be quite verbose and difficult to read at times. The worker
	// package also provides a utility function that simplifies the ordered concurrent
	// processing into a worker function and a slice of inputs

	// Let's create our inputs 0-12 like in the previous example
	const numInputs = 12
	inputs := make([]int, numInputs)
	for i := 0; i < numInputs; i++ {
		inputs[i] = i
	}

	// Now, we can just call ConcurrentDo with the inputs and worker func:
	results := worker.ConcurrentDo(slowAddTenToFloat, inputs)

	// Note that the order of the results is consistent with the order of inputs
	for i := 0; i < numInputs; i++ {
		fmt.Printf("Received Result: %.2f for Input: %d\n", results[i], inputs[i])
	}

	// Output:
	// Received Result: 10.00 for Input: 0
	// Received Result: 11.00 for Input: 1
	// Received Result: 12.00 for Input: 2
	// Received Result: 13.00 for Input: 3
	// Received Result: 14.00 for Input: 4
	// Received Result: 15.00 for Input: 5
	// Received Result: 16.00 for Input: 6
	// Received Result: 17.00 for Input: 7
	// Received Result: 18.00 for Input: 8
	// Received Result: 19.00 for Input: 9
	// Received Result: 20.00 for Input: 10
	// Received Result: 21.00 for Input: 11
}
