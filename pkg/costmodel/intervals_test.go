package costmodel

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
)

func TestGetIntervalPointsFromWindows(t *testing.T) {
	cases := []struct {
		name           string
		pvcIntervalMap map[podKey]opencost.Window
		expected       []IntervalPoint
	}{
		{
			name: "four pods w/ various overlaps",
			pvcIntervalMap: map[podKey]opencost.Window{
				// Pod running from 8 am to 9 am
				{
					Pod: "Pod1",
				}: opencost.Window(opencost.NewClosedWindow(
					time.Date(2021, 2, 19, 8, 0, 0, 0, time.UTC),
					time.Date(2021, 2, 19, 9, 0, 0, 0, time.UTC),
				)),
				// Pod running from 8:30 am to 9 am
				{
					Pod: "Pod2",
				}: opencost.Window(opencost.NewClosedWindow(
					time.Date(2021, 2, 19, 8, 30, 0, 0, time.UTC),
					time.Date(2021, 2, 19, 9, 0, 0, 0, time.UTC),
				)),
				// Pod running from 8:45 am to 9 am
				{
					Pod: "Pod3",
				}: opencost.Window(opencost.NewClosedWindow(
					time.Date(2021, 2, 19, 8, 45, 0, 0, time.UTC),
					time.Date(2021, 2, 19, 9, 0, 0, 0, time.UTC),
				)),
				// Pod running from 8 am to 8:15 am
				{
					Pod: "Pod4",
				}: opencost.Window(opencost.NewClosedWindow(
					time.Date(2021, 2, 19, 8, 0, 0, 0, time.UTC),
					time.Date(2021, 2, 19, 8, 15, 0, 0, time.UTC),
				)),
			},
			expected: []IntervalPoint{
				NewIntervalPoint(time.Date(2021, 2, 19, 8, 0, 0, 0, time.UTC), "start", podKey{Pod: "Pod1"}),
				NewIntervalPoint(time.Date(2021, 2, 19, 8, 0, 0, 0, time.UTC), "start", podKey{Pod: "Pod4"}),
				NewIntervalPoint(time.Date(2021, 2, 19, 8, 15, 0, 0, time.UTC), "end", podKey{Pod: "Pod4"}),
				NewIntervalPoint(time.Date(2021, 2, 19, 8, 30, 0, 0, time.UTC), "start", podKey{Pod: "Pod2"}),
				NewIntervalPoint(time.Date(2021, 2, 19, 8, 45, 0, 0, time.UTC), "start", podKey{Pod: "Pod3"}),
				NewIntervalPoint(time.Date(2021, 2, 19, 9, 0, 0, 0, time.UTC), "end", podKey{Pod: "Pod2"}),
				NewIntervalPoint(time.Date(2021, 2, 19, 9, 0, 0, 0, time.UTC), "end", podKey{Pod: "Pod3"}),
				NewIntervalPoint(time.Date(2021, 2, 19, 9, 0, 0, 0, time.UTC), "end", podKey{Pod: "Pod1"}),
			},
		},
		{
			name: "two pods no overlap",
			pvcIntervalMap: map[podKey]opencost.Window{
				// Pod running from 8 am to 8:30 am
				{
					Pod: "Pod1",
				}: opencost.Window(opencost.NewClosedWindow(
					time.Date(2021, 2, 19, 8, 0, 0, 0, time.UTC),
					time.Date(2021, 2, 19, 8, 30, 0, 0, time.UTC),
				)),
				// Pod running from 8:30 am to 9 am
				{
					Pod: "Pod2",
				}: opencost.Window(opencost.NewClosedWindow(
					time.Date(2021, 2, 19, 8, 30, 0, 0, time.UTC),
					time.Date(2021, 2, 19, 9, 0, 0, 0, time.UTC),
				)),
			},
			expected: []IntervalPoint{
				NewIntervalPoint(time.Date(2021, 2, 19, 8, 0, 0, 0, time.UTC), "start", podKey{Pod: "Pod1"}),
				NewIntervalPoint(time.Date(2021, 2, 19, 8, 30, 0, 0, time.UTC), "start", podKey{Pod: "Pod2"}),
				NewIntervalPoint(time.Date(2021, 2, 19, 8, 30, 0, 0, time.UTC), "end", podKey{Pod: "Pod1"}),
				NewIntervalPoint(time.Date(2021, 2, 19, 9, 0, 0, 0, time.UTC), "end", podKey{Pod: "Pod2"}),
			},
		},
		{
			name: "two pods total overlap",
			pvcIntervalMap: map[podKey]opencost.Window{
				// Pod running from 8:30 am to 9 am
				{
					Pod: "Pod1",
				}: opencost.Window(opencost.NewClosedWindow(
					time.Date(2021, 2, 19, 8, 30, 0, 0, time.UTC),
					time.Date(2021, 2, 19, 9, 0, 0, 0, time.UTC),
				)),
				// Pod running from 8:30 am to 9 am
				{
					Pod: "Pod2",
				}: opencost.Window(opencost.NewClosedWindow(
					time.Date(2021, 2, 19, 8, 30, 0, 0, time.UTC),
					time.Date(2021, 2, 19, 9, 0, 0, 0, time.UTC),
				)),
			},
			expected: []IntervalPoint{
				NewIntervalPoint(time.Date(2021, 2, 19, 8, 30, 0, 0, time.UTC), "start", podKey{Pod: "Pod1"}),
				NewIntervalPoint(time.Date(2021, 2, 19, 8, 30, 0, 0, time.UTC), "start", podKey{Pod: "Pod2"}),
				NewIntervalPoint(time.Date(2021, 2, 19, 9, 0, 0, 0, time.UTC), "end", podKey{Pod: "Pod1"}),
				NewIntervalPoint(time.Date(2021, 2, 19, 9, 0, 0, 0, time.UTC), "end", podKey{Pod: "Pod2"}),
			},
		},
		{
			name: "one pod",
			pvcIntervalMap: map[podKey]opencost.Window{
				// Pod running from 8 am to 9 am
				{
					Pod: "Pod1",
				}: opencost.Window(opencost.NewClosedWindow(
					time.Date(2021, 2, 19, 8, 0, 0, 0, time.UTC),
					time.Date(2021, 2, 19, 9, 0, 0, 0, time.UTC),
				)),
			},
			expected: []IntervalPoint{
				NewIntervalPoint(time.Date(2021, 2, 19, 8, 0, 0, 0, time.UTC), "start", podKey{Pod: "Pod1"}),
				NewIntervalPoint(time.Date(2021, 2, 19, 9, 0, 0, 0, time.UTC), "end", podKey{Pod: "Pod1"}),
			},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {

			result := getIntervalPointsFromWindows(testCase.pvcIntervalMap)

			if len(result) != len(testCase.expected) {
				t.Errorf("getIntervalPointsFromWindows test failed: %s: Got %+v but expected %+v", testCase.name, result, testCase.expected)
			}

			for i := range testCase.expected {

				// For correctness in terms of individual position of IntervalPoints, we only need to check the time/type.
				// Key is used in other associated calculations, so it must exist, but order does not matter if other sorting
				// logic is obeyed.
				if !testCase.expected[i].Time.Equal(result[i].Time) || testCase.expected[i].PointType != result[i].PointType {
					t.Errorf("getIntervalPointsFromWindows test failed: %s: Got point %s:%s but expected %s:%s", testCase.name, testCase.expected[i].PointType, testCase.expected[i].Time, result[i].PointType, result[i].Time)
				}

			}

		})
	}
}

func TestGetPVCCostCoefficients(t *testing.T) {
	pod1Key := newPodKey("cluster1", "namespace1", "pod1")
	pod2Key := newPodKey("cluster1", "namespace1", "pod2")
	pod3Key := newPodKey("cluster1", "namespace1", "pod3")
	pod4Key := newPodKey("cluster1", "namespace1", "pod4")
	ummountedPodKey := newPodKey("cluster1", opencost.UnmountedSuffix, opencost.UnmountedSuffix)

	pvc1 := &pvc{
		Bytes:     100 * 1024 * 1024 * 1024,
		Name:      "pvc1",
		Cluster:   "cluster1",
		Namespace: "namespace1",
		Start:     time.Date(2021, 2, 19, 8, 0, 0, 0, time.UTC),
		End:       time.Date(2021, 2, 19, 9, 0, 0, 0, time.UTC),
	}

	pvc2 := &pvc{
		Bytes:     100 * 1024 * 1024 * 1024,
		Name:      "pvc2",
		Cluster:   "cluster1",
		Namespace: "namespace1",
		Start:     time.Date(2021, 2, 19, 8, 0, 0, 0, time.UTC),
		End:       time.Date(2021, 2, 19, 9, 0, 0, 0, time.UTC),
	}

	pvc3 := &pvc{
		Bytes:     100 * 1024 * 1024 * 1024,
		Name:      "pvc3",
		Cluster:   "cluster1",
		Namespace: "namespace1",
		Start:     time.Date(2021, 2, 19, 8, 0, 0, 0, time.UTC),
		End:       time.Date(2021, 2, 19, 8, 0, 0, 0, time.UTC),
	}

	cases := []struct {
		name           string
		pvc            *pvc
		pvcIntervalMap map[podKey]opencost.Window
		intervals      []IntervalPoint
		resolution     time.Duration
		expected       map[podKey][]CoefficientComponent
		expError       error
	}{
		{
			name: "four pods w/ various overlaps",
			pvc:  pvc1,
			intervals: []IntervalPoint{
				NewIntervalPoint(time.Date(2021, 2, 19, 8, 0, 0, 0, time.UTC), "start", pod1Key),
				NewIntervalPoint(time.Date(2021, 2, 19, 8, 0, 0, 0, time.UTC), "start", pod4Key),
				NewIntervalPoint(time.Date(2021, 2, 19, 8, 15, 0, 0, time.UTC), "end", pod4Key),
				NewIntervalPoint(time.Date(2021, 2, 19, 8, 30, 0, 0, time.UTC), "start", pod2Key),
				NewIntervalPoint(time.Date(2021, 2, 19, 8, 45, 0, 0, time.UTC), "start", pod3Key),
				NewIntervalPoint(time.Date(2021, 2, 19, 9, 0, 0, 0, time.UTC), "end", pod2Key),
				NewIntervalPoint(time.Date(2021, 2, 19, 9, 0, 0, 0, time.UTC), "end", pod3Key),
				NewIntervalPoint(time.Date(2021, 2, 19, 9, 0, 0, 0, time.UTC), "end", pod1Key),
			},
			expError: nil,
			expected: map[podKey][]CoefficientComponent{
				pod1Key: {
					{0.5, 0.25},
					{1, 0.25},
					{0.5, 0.25},
					{1.0 / 3.0, 0.25},
				},
				pod2Key: {
					{0.5, 0.25},
					{1.0 / 3.0, 0.25},
				},
				pod3Key: {
					{1.0 / 3.0, 0.25},
				},
				pod4Key: {
					{0.5, 0.25},
				},
			},
		},
		{
			name: "two pods no overlap",
			pvc:  pvc1,
			intervals: []IntervalPoint{
				NewIntervalPoint(time.Date(2021, 2, 19, 8, 0, 0, 0, time.UTC), "start", pod1Key),
				NewIntervalPoint(time.Date(2021, 2, 19, 8, 30, 0, 0, time.UTC), "start", pod2Key),
				NewIntervalPoint(time.Date(2021, 2, 19, 8, 30, 0, 0, time.UTC), "end", pod1Key),
				NewIntervalPoint(time.Date(2021, 2, 19, 9, 0, 0, 0, time.UTC), "end", pod2Key),
			},
			expError: nil,
			expected: map[podKey][]CoefficientComponent{
				pod1Key: {
					{1.0, 0.5},
				},
				pod2Key: {
					{1.0, 0.5},
				},
			},
		},
		{
			name: "two pods total overlap",
			pvc:  pvc1,
			intervals: []IntervalPoint{
				NewIntervalPoint(time.Date(2021, 2, 19, 8, 30, 0, 0, time.UTC), "start", pod1Key),
				NewIntervalPoint(time.Date(2021, 2, 19, 8, 30, 0, 0, time.UTC), "start", pod2Key),
				NewIntervalPoint(time.Date(2021, 2, 19, 9, 0, 0, 0, time.UTC), "end", pod1Key),
				NewIntervalPoint(time.Date(2021, 2, 19, 9, 0, 0, 0, time.UTC), "end", pod2Key),
			},
			expError: nil,
			expected: map[podKey][]CoefficientComponent{
				pod1Key: {
					{0.5, 0.5},
				},
				pod2Key: {
					{0.5, 0.5},
				},
				ummountedPodKey: {
					{1.0, 0.5},
				},
			},
		},
		{
			name: "one pod",
			pvc:  pvc1,
			intervals: []IntervalPoint{
				NewIntervalPoint(time.Date(2021, 2, 19, 8, 0, 0, 0, time.UTC), "start", pod1Key),
				NewIntervalPoint(time.Date(2021, 2, 19, 9, 0, 0, 0, time.UTC), "end", pod1Key),
			},
			expError: nil,
			expected: map[podKey][]CoefficientComponent{
				pod1Key: {
					{1.0, 1.0},
				},
			},
		},
		{
			name: "two pods with gap",
			pvc:  pvc1,
			intervals: []IntervalPoint{
				NewIntervalPoint(time.Date(2021, 2, 19, 8, 0, 0, 0, time.UTC), "start", pod1Key),
				NewIntervalPoint(time.Date(2021, 2, 19, 8, 15, 0, 0, time.UTC), "end", pod1Key),
				NewIntervalPoint(time.Date(2021, 2, 19, 8, 45, 0, 0, time.UTC), "start", pod2Key),
				NewIntervalPoint(time.Date(2021, 2, 19, 9, 0, 0, 0, time.UTC), "end", pod2Key),
			},
			expError: nil,
			expected: map[podKey][]CoefficientComponent{
				pod1Key: {
					{1.0, 0.25},
				},
				pod2Key: {
					{1.0, 0.25},
				},
				ummountedPodKey: {
					{1.0, 0.5},
				},
			},
		},
		{
			name: "one pods start and end in window",
			pvc:  pvc1,
			intervals: []IntervalPoint{
				NewIntervalPoint(time.Date(2021, 2, 19, 8, 15, 0, 0, time.UTC), "start", pod1Key),
				NewIntervalPoint(time.Date(2021, 2, 19, 8, 45, 0, 0, time.UTC), "end", pod1Key),
			},
			expError: nil,
			expected: map[podKey][]CoefficientComponent{
				pod1Key: {
					{1.0, 0.5},
				},
				ummountedPodKey: {
					{1.0, 0.25},
					{1.0, 0.25},
				},
			},
		},
		{
			name: "back to back pods, full coverage",
			pvc:  pvc2,
			intervals: []IntervalPoint{
				NewIntervalPoint(time.Date(2021, 2, 19, 8, 0, 0, 0, time.UTC), "start", pod1Key),
				NewIntervalPoint(time.Date(2021, 2, 19, 8, 30, 0, 0, time.UTC), "start", pod2Key),
				NewIntervalPoint(time.Date(2021, 2, 19, 8, 30, 0, 0, time.UTC), "end", pod1Key),
				NewIntervalPoint(time.Date(2021, 2, 19, 9, 0, 0, 0, time.UTC), "end", pod2Key),
			},
			expError: nil,
			expected: map[podKey][]CoefficientComponent{
				pod1Key: {
					{1.0, 0.5},
				},
				pod2Key: {
					{1.0, 0.5},
				},
			},
		},
		{
			name: "zero duration",
			pvc:  pvc3,
			intervals: []IntervalPoint{
				NewIntervalPoint(time.Date(2021, 2, 19, 8, 0, 0, 0, time.UTC), "start", pod1Key),
				NewIntervalPoint(time.Date(2021, 2, 19, 8, 0, 0, 0, time.UTC), "end", pod1Key),
			},
			expError: fmt.Errorf("detected PVC with window of zero duration: %s/%s/%s", "cluster1", "namespace1", "pvc3"),
			expected: nil,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			result, err := getPVCCostCoefficients(testCase.intervals, testCase.pvc)
			if err != nil {
				if testCase.expError == nil {
					t.Errorf("getPVCCostCoefficients failed: got unexpected error: %v", err)
				}
				return
			}

			if err == nil && testCase.expError != nil {
				t.Errorf("getPVCCostCoefficients failed: did not get expected error: %v", testCase.expError)
			}

			if !reflect.DeepEqual(result, testCase.expected) {
				t.Errorf("getPVCCostCoefficients test failed: %s: Got %+v but expected %+v", testCase.name, result, testCase.expected)
			}

			// check that coefficients sum to 1, to ensure that 100% of PVC cost is being distributed
			sum := 0.0
			for _, coefs := range result {
				sum += getCoefficientFromComponents(coefs)
			}
			if sum != 1.0 {
				t.Errorf("getPVCCostCoefficients test failed: coefficient totals did not sum to 1.0: %f", sum)
			}
		})
	}
}
