// Code generated by moq; DO NOT EDIT.
// github.com/matryer/moq

package costmodel

import (
	"github.com/opencost/opencost/core/pkg/opencost"
	"sync"
	"time"
)

// Ensure, that AllocationModelMock does implement AllocationModel.
// If this is not the case, regenerate this file with moq.
var _ AllocationModel = &AllocationModelMock{}

// AllocationModelMock is a mock implementation of AllocationModel.
//
//	func TestSomethingThatUsesAllocationModel(t *testing.T) {
//
//		// make and configure a mocked AllocationModel
//		mockedAllocationModel := &AllocationModelMock{
//			ComputeAllocationFunc: func(start time.Time, end time.Time, resolution time.Duration) (*opencost.AllocationSet, error) {
//				panic("mock out the ComputeAllocation method")
//			},
//			DateRangeFunc: func(limitDays int) (time.Time, time.Time, error) {
//				panic("mock out the DateRange method")
//			},
//		}
//
//		// use mockedAllocationModel in code that requires AllocationModel
//		// and then make assertions.
//
//	}
type AllocationModelMock struct {
	// ComputeAllocationFunc mocks the ComputeAllocation method.
	ComputeAllocationFunc func(start time.Time, end time.Time, resolution time.Duration) (*opencost.AllocationSet, error)

	// DateRangeFunc mocks the DateRange method.
	DateRangeFunc func(limitDays int) (time.Time, time.Time, error)

	// calls tracks calls to the methods.
	calls struct {
		// ComputeAllocation holds details about calls to the ComputeAllocation method.
		ComputeAllocation []struct {
			// Start is the start argument value.
			Start time.Time
			// End is the end argument value.
			End time.Time
			// Resolution is the resolution argument value.
			Resolution time.Duration
		}
		// DateRange holds details about calls to the DateRange method.
		DateRange []struct {
			// LimitDays is the limitDays argument value.
			LimitDays int
		}
	}
	lockComputeAllocation sync.RWMutex
	lockDateRange         sync.RWMutex
}

// ComputeAllocation calls ComputeAllocationFunc.
func (mock *AllocationModelMock) ComputeAllocation(start time.Time, end time.Time, resolution time.Duration) (*opencost.AllocationSet, error) {
	if mock.ComputeAllocationFunc == nil {
		panic("AllocationModelMock.ComputeAllocationFunc: method is nil but AllocationModel.ComputeAllocation was just called")
	}
	callInfo := struct {
		Start      time.Time
		End        time.Time
		Resolution time.Duration
	}{
		Start:      start,
		End:        end,
		Resolution: resolution,
	}
	mock.lockComputeAllocation.Lock()
	mock.calls.ComputeAllocation = append(mock.calls.ComputeAllocation, callInfo)
	mock.lockComputeAllocation.Unlock()
	return mock.ComputeAllocationFunc(start, end, resolution)
}

// ComputeAllocationCalls gets all the calls that were made to ComputeAllocation.
// Check the length with:
//
//	len(mockedAllocationModel.ComputeAllocationCalls())
func (mock *AllocationModelMock) ComputeAllocationCalls() []struct {
	Start      time.Time
	End        time.Time
	Resolution time.Duration
} {
	var calls []struct {
		Start      time.Time
		End        time.Time
		Resolution time.Duration
	}
	mock.lockComputeAllocation.RLock()
	calls = mock.calls.ComputeAllocation
	mock.lockComputeAllocation.RUnlock()
	return calls
}

// DateRange calls DateRangeFunc.
func (mock *AllocationModelMock) DateRange(limitDays int) (time.Time, time.Time, error) {
	if mock.DateRangeFunc == nil {
		panic("AllocationModelMock.DateRangeFunc: method is nil but AllocationModel.DateRange was just called")
	}
	callInfo := struct {
		LimitDays int
	}{
		LimitDays: limitDays,
	}
	mock.lockDateRange.Lock()
	mock.calls.DateRange = append(mock.calls.DateRange, callInfo)
	mock.lockDateRange.Unlock()
	return mock.DateRangeFunc(limitDays)
}

// DateRangeCalls gets all the calls that were made to DateRange.
// Check the length with:
//
//	len(mockedAllocationModel.DateRangeCalls())
func (mock *AllocationModelMock) DateRangeCalls() []struct {
	LimitDays int
} {
	var calls []struct {
		LimitDays int
	}
	mock.lockDateRange.RLock()
	calls = mock.calls.DateRange
	mock.lockDateRange.RUnlock()
	return calls
}
