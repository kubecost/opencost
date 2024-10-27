package costmodel

import (
	"fmt"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util"
	"github.com/opencost/opencost/pkg/prom"
)

const Ki = 1024
const Mi = Ki * 1024
const Gi = Mi * 1024

const minute = 60.0
const hour = minute * 60.0

var windowStart = time.Date(2020, 6, 16, 0, 0, 0, 0, time.UTC)
var windowEnd = time.Date(2020, 6, 17, 0, 0, 0, 0, time.UTC)
var window = opencost.NewWindow(&windowStart, &windowEnd)

var startFloat = float64(windowStart.Unix())

var podKey1 = podKey{
	namespaceKey: namespaceKey{
		Cluster:   "cluster1",
		Namespace: "namespace1",
	},
	Pod: "pod1",
}
var podKey2 = podKey{
	namespaceKey: namespaceKey{
		Cluster:   "cluster1",
		Namespace: "namespace1",
	},
	Pod: "pod2",
}
var podKey3 = podKey{
	namespaceKey: namespaceKey{
		Cluster:   "cluster2",
		Namespace: "namespace2",
	},
	Pod: "pod3",
}

var podKey4 = podKey{
	namespaceKey: namespaceKey{
		Cluster:   "cluster2",
		Namespace: "namespace2",
	},
	Pod: "pod4",
}

var podKeyUnmounted = podKey{
	namespaceKey: namespaceKey{
		Cluster:   "cluster2",
		Namespace: opencost.UnmountedSuffix,
	},
	Pod: opencost.UnmountedSuffix,
}

var kcPVKey1 = opencost.PVKey{
	Cluster: "cluster1",
	Name:    "pv1",
}

var kcPVKey2 = opencost.PVKey{
	Cluster: "cluster1",
	Name:    "pv2",
}

var kcPVKey3 = opencost.PVKey{
	Cluster: "cluster2",
	Name:    "pv3",
}

var kcPVKey4 = opencost.PVKey{
	Cluster: "cluster2",
	Name:    "pv4",
}

var podMap1 = map[podKey]*pod{
	podKey1: {
		Window:      window.Clone(),
		Start:       time.Date(2020, 6, 16, 0, 0, 0, 0, time.UTC),
		End:         time.Date(2020, 6, 17, 0, 0, 0, 0, time.UTC),
		Key:         podKey1,
		Allocations: nil,
	},
	podKey2: {
		Window:      window.Clone(),
		Start:       time.Date(2020, 6, 16, 12, 0, 0, 0, time.UTC),
		End:         time.Date(2020, 6, 17, 0, 0, 0, 0, time.UTC),
		Key:         podKey2,
		Allocations: nil,
	},
	podKey3: {
		Window:      window.Clone(),
		Start:       time.Date(2020, 6, 16, 6, 30, 0, 0, time.UTC),
		End:         time.Date(2020, 6, 17, 18, 12, 33, 0, time.UTC),
		Key:         podKey3,
		Allocations: nil,
	},
	podKey4: {
		Window:      window.Clone(),
		Start:       time.Date(2020, 6, 16, 0, 0, 0, 0, time.UTC),
		End:         time.Date(2020, 6, 17, 13, 0, 0, 0, time.UTC),
		Key:         podKey4,
		Allocations: nil,
	},
	podKeyUnmounted: {
		Window: window.Clone(),
		Start:  *window.Start(),
		End:    *window.End(),
		Key:    podKeyUnmounted,
		Allocations: map[string]*opencost.Allocation{
			opencost.UnmountedSuffix: {
				Name: fmt.Sprintf("%s/%s/%s/%s", podKeyUnmounted.Cluster, podKeyUnmounted.Namespace, podKeyUnmounted.Pod, opencost.UnmountedSuffix),
				Properties: &opencost.AllocationProperties{
					Cluster:   podKeyUnmounted.Cluster,
					Node:      "",
					Container: opencost.UnmountedSuffix,
					Namespace: podKeyUnmounted.Namespace,
					Pod:       podKeyUnmounted.Pod,
					Services:  []string{"LB1"},
				},
				Window:                     window,
				Start:                      *window.Start(),
				End:                        *window.End(),
				LoadBalancerCost:           0.60,
				LoadBalancerCostAdjustment: 0,
				PVs: opencost.PVAllocations{
					kcPVKey2: &opencost.PVAllocation{
						ByteHours: 24 * Gi,
						Cost:      2.25,
					},
				},
			},
		},
	},
}

var podMap2 = map[podKey]*pod{
	podKey1: {
		Window:      window.Clone(),
		Start:       windowStart,
		End:         windowEnd,
		Key:         podKey1,
		Allocations: nil,
	},
}

var podMap3 = map[podKey]*pod{
	podKey1: {
		Window:      window.Clone(),
		Start:       windowStart,
		End:         windowEnd,
		Key:         podKey1,
		Allocations: nil,
	},
	podKey2: {
		Window:      window.Clone(),
		Start:       windowStart,
		End:         windowEnd,
		Key:         podKey2,
		Allocations: nil,
	},
}

var podMap4 = map[podKey]*pod{
	podKey1: {
		Window: window.Clone(),
		Start:  windowStart,
		End:    windowEnd,
		Key:    podKey1,
		Allocations: map[string]*opencost.Allocation{
			opencost.UnmountedSuffix: {
				Name: fmt.Sprintf("%s/%s/%s/%s", "cluster1", opencost.UnmountedSuffix, opencost.UnmountedSuffix, opencost.UnmountedSuffix),
				Properties: &opencost.AllocationProperties{
					Cluster:   "cluster1",
					Node:      "node1",
					Container: opencost.UnmountedSuffix,
					Namespace: opencost.UnmountedSuffix,
					Pod:       opencost.UnmountedSuffix,
				},
				Window: window,
				Start:  *window.Start(),
				End:    *window.End(),
			},
		},
	},
}

var pvKey1 = pvKey{
	Cluster:          "cluster1",
	PersistentVolume: "pv1",
}

var pvKey2 = pvKey{
	Cluster:          "cluster1",
	PersistentVolume: "pv2",
}

var pvKey3 = pvKey{
	Cluster:          "cluster2",
	PersistentVolume: "pv3",
}

var pvKey4 = pvKey{
	Cluster:          "cluster2",
	PersistentVolume: "pv4",
}

var pvMap1 = map[pvKey]*pv{
	pvKey1: {
		Start:          windowStart,
		End:            windowEnd.Add(time.Hour * -6),
		Bytes:          20 * Gi,
		CostPerGiBHour: 0.05,
		Cluster:        "cluster1",
		Name:           "pv1",
		StorageClass:   "class1",
	},
	pvKey2: {
		Start:          windowStart,
		End:            windowEnd,
		Bytes:          100 * Gi,
		CostPerGiBHour: 0.05,
		Cluster:        "cluster1",
		Name:           "pv2",
		StorageClass:   "class1",
	},
	pvKey3: {
		Start:          windowStart.Add(time.Hour * 6),
		End:            windowEnd.Add(time.Hour * -6),
		Bytes:          50 * Gi,
		CostPerGiBHour: 0.03,
		Cluster:        "cluster2",
		Name:           "pv3",
		StorageClass:   "class2",
	},
	pvKey4: {
		Start:          windowStart,
		End:            windowEnd.Add(time.Hour * -6),
		Bytes:          30 * Gi,
		CostPerGiBHour: 0.05,
		Cluster:        "cluster2",
		Name:           "pv4",
		StorageClass:   "class1",
	},
}

/* pv/pvc Helpers */
func TestBuildPVMap(t *testing.T) {
	pvMap1NoBytes := make(map[pvKey]*pv, len(pvMap1))
	for thisPVKey, thisPV := range pvMap1 {
		clonePV := thisPV.clone()
		clonePV.Bytes = 0.0
		clonePV.StorageClass = ""
		pvMap1NoBytes[thisPVKey] = clonePV
	}

	testCases := map[string]struct {
		resolution              time.Duration
		resultsPVCostPerGiBHour []*prom.QueryResult
		resultsActiveMinutes    []*prom.QueryResult
		expected                map[pvKey]*pv
	}{
		"pvMap1": {
			resolution: time.Hour * 6,
			resultsPVCostPerGiBHour: []*prom.QueryResult{
				{
					Metric: map[string]interface{}{
						"cluster_id": "cluster1",
						"volumename": "pv1",
					},
					Values: []*util.Vector{
						{
							Value: 0.05,
						},
					},
				},
				{
					Metric: map[string]interface{}{
						"cluster_id": "cluster1",
						"volumename": "pv2",
					},
					Values: []*util.Vector{
						{
							Value: 0.05,
						},
					},
				},
				{
					Metric: map[string]interface{}{
						"cluster_id": "cluster2",
						"volumename": "pv3",
					},
					Values: []*util.Vector{
						{
							Value: 0.03,
						},
					},
				},
				{
					Metric: map[string]interface{}{
						"cluster_id": "cluster2",
						"volumename": "pv4",
					},
					Values: []*util.Vector{
						{
							Value: 0.05,
						},
					},
				},
			},
			resultsActiveMinutes: []*prom.QueryResult{
				{
					Metric: map[string]interface{}{
						"cluster_id":       "cluster1",
						"persistentvolume": "pv1",
					},
					Values: []*util.Vector{
						{
							Timestamp: startFloat,
						},
						{
							Timestamp: startFloat + (hour * 6),
						},
						{
							Timestamp: startFloat + (hour * 12),
						},
						{
							Timestamp: startFloat + (hour * 18),
						},
					},
				},
				{
					Metric: map[string]interface{}{
						"cluster_id":       "cluster1",
						"persistentvolume": "pv2",
					},
					Values: []*util.Vector{
						{
							Timestamp: startFloat,
						},
						{
							Timestamp: startFloat + (hour * 6),
						},
						{
							Timestamp: startFloat + (hour * 12),
						},
						{
							Timestamp: startFloat + (hour * 18),
						},
						{
							Timestamp: startFloat + (hour * 24),
						},
					},
				},
				{
					Metric: map[string]interface{}{
						"cluster_id":       "cluster2",
						"persistentvolume": "pv3",
					},
					Values: []*util.Vector{
						{
							Timestamp: startFloat + (hour * 6),
						},
						{
							Timestamp: startFloat + (hour * 12),
						},
						{
							Timestamp: startFloat + (hour * 18),
						},
					},
				},
				{
					Metric: map[string]interface{}{
						"cluster_id":       "cluster2",
						"persistentvolume": "pv4",
					},
					Values: []*util.Vector{
						{
							Timestamp: startFloat,
						},
						{
							Timestamp: startFloat + (hour * 6),
						},
						{
							Timestamp: startFloat + (hour * 12),
						},
						{
							Timestamp: startFloat + (hour * 18),
						},
					},
				},
			},
			expected: pvMap1NoBytes,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			pvMap := make(map[pvKey]*pv)
			buildPVMap(testCase.resolution, pvMap, testCase.resultsPVCostPerGiBHour, testCase.resultsActiveMinutes, []*prom.QueryResult{}, window)
			if len(pvMap) != len(testCase.expected) {
				t.Errorf("pv map does not have the expected length %d : %d", len(pvMap), len(testCase.expected))
			}

			for thisPVKey, expectedPV := range testCase.expected {
				actualPV, ok := pvMap[thisPVKey]
				if !ok {
					t.Errorf("pv map is missing key %s", thisPVKey)
				}
				if !actualPV.equal(expectedPV) {
					t.Errorf("pv does not match with key %s: %s != %s", thisPVKey, opencost.NewClosedWindow(actualPV.Start, actualPV.End), opencost.NewClosedWindow(expectedPV.Start, expectedPV.End))
				}
			}
		})
	}
}

/* Helper Helpers */

func TestGetUnmountedPodForCluster(t *testing.T) {
	testCases := map[string]struct {
		window   opencost.Window
		podMap   map[podKey]*pod
		cluster  string
		expected *pod
	}{
		"create new": {
			window:  window.Clone(),
			podMap:  podMap1,
			cluster: "cluster1",
			expected: &pod{
				Window: window.Clone(),
				Start:  *window.Start(),
				End:    *window.End(),
				Key:    getUnmountedPodKey("cluster1"),
				Allocations: map[string]*opencost.Allocation{
					opencost.UnmountedSuffix: {
						Name: fmt.Sprintf("%s/%s/%s/%s", "cluster1", opencost.UnmountedSuffix, opencost.UnmountedSuffix, opencost.UnmountedSuffix),
						Properties: &opencost.AllocationProperties{
							Cluster:   "cluster1",
							Node:      "",
							Container: opencost.UnmountedSuffix,
							Namespace: opencost.UnmountedSuffix,
							Pod:       opencost.UnmountedSuffix,
						},
						Window: window,
						Start:  *window.Start(),
						End:    *window.End(),
					},
				},
			},
		},
		"get existing": {
			window:  window.Clone(),
			podMap:  podMap1,
			cluster: "cluster2",
			expected: &pod{
				Window: window.Clone(),
				Start:  *window.Start(),
				End:    *window.End(),
				Key:    getUnmountedPodKey("cluster2"),
				Allocations: map[string]*opencost.Allocation{
					opencost.UnmountedSuffix: {
						Name: fmt.Sprintf("%s/%s/%s/%s", "cluster2", opencost.UnmountedSuffix, opencost.UnmountedSuffix, opencost.UnmountedSuffix),
						Properties: &opencost.AllocationProperties{
							Cluster:   "cluster2",
							Node:      "",
							Container: opencost.UnmountedSuffix,
							Namespace: opencost.UnmountedSuffix,
							Pod:       opencost.UnmountedSuffix,
							Services:  []string{"LB1"},
						},
						Window:                     window,
						Start:                      *window.Start(),
						End:                        *window.End(),
						LoadBalancerCost:           .60,
						LoadBalancerCostAdjustment: 0,
						PVs: opencost.PVAllocations{
							kcPVKey2: &opencost.PVAllocation{
								ByteHours: 24 * Gi,
								Cost:      2.25,
							},
						},
					},
				},
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			actual := getUnmountedPodForCluster(testCase.window, testCase.podMap, testCase.cluster)
			if !actual.equal(testCase.expected) {
				t.Errorf("Unmounted pod does not match expectation")
			}
		})
	}
}

func TestCalculateStartAndEnd(t *testing.T) {

	testCases := map[string]struct {
		resolution    time.Duration
		expectedStart time.Time
		expectedEnd   time.Time
		result        *prom.QueryResult
	}{
		"1 hour resolution, 1 hour window": {
			resolution:    time.Hour,
			expectedStart: windowStart,
			expectedEnd:   windowStart.Add(time.Hour),
			result: &prom.QueryResult{
				Values: []*util.Vector{
					{
						Timestamp: startFloat,
					},
					{
						Timestamp: startFloat + (minute * 60),
					},
				},
			},
		},
		"30 minute resolution, 1 hour window": {
			resolution:    time.Minute * 30,
			expectedStart: windowStart,
			expectedEnd:   windowStart.Add(time.Hour),
			result: &prom.QueryResult{
				Values: []*util.Vector{
					{
						Timestamp: startFloat,
					},
					{
						Timestamp: startFloat + (minute * 30),
					},
					{
						Timestamp: startFloat + (minute * 60),
					},
				},
			},
		},
		"15 minute resolution, 45 minute window": {
			resolution:    time.Minute * 15,
			expectedStart: windowStart,
			expectedEnd:   windowStart.Add(time.Minute * 45),
			result: &prom.QueryResult{
				Values: []*util.Vector{
					{
						Timestamp: startFloat + (minute * 0),
					},
					{
						Timestamp: startFloat + (minute * 15),
					},
					{
						Timestamp: startFloat + (minute * 30),
					},
					{
						Timestamp: startFloat + (minute * 45),
					},
				},
			},
		},
		"1 minute resolution, 5 minute window": {
			resolution:    time.Minute,
			expectedStart: windowStart.Add(time.Minute * 15),
			expectedEnd:   windowStart.Add(time.Minute * 20),
			result: &prom.QueryResult{
				Values: []*util.Vector{
					{
						Timestamp: startFloat + (minute * 15),
					},
					{
						Timestamp: startFloat + (minute * 16),
					},
					{
						Timestamp: startFloat + (minute * 17),
					},
					{
						Timestamp: startFloat + (minute * 18),
					},
					{
						Timestamp: startFloat + (minute * 19),
					},
					{
						Timestamp: startFloat + (minute * 20),
					},
				},
			},
		},
		"1 minute resolution, 1 minute window": {
			resolution:    time.Minute,
			expectedStart: windowStart.Add(time.Minute * 14).Add(time.Second * 30),
			expectedEnd:   windowStart.Add(time.Minute * 15).Add(time.Second * 30),
			result: &prom.QueryResult{
				Values: []*util.Vector{
					{
						Timestamp: startFloat + (minute * 15),
					},
				},
			},
		},
		"1 minute resolution, 1 minute window, at window start": {
			resolution:    time.Minute,
			expectedStart: windowStart,
			expectedEnd:   windowStart.Add(time.Second * 30),
			result: &prom.QueryResult{
				Values: []*util.Vector{
					{
						Timestamp: startFloat,
					},
				},
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			start, end := calculateStartAndEnd(testCase.result, testCase.resolution, window)
			if !start.Equal(testCase.expectedStart) {
				t.Errorf("start does not match: expected %v; got %v", testCase.expectedStart, start)
			}
			if !end.Equal(testCase.expectedEnd) {
				t.Errorf("end does not match: expected %v; got %v", testCase.expectedEnd, end)
			}
		})
	}
}

func TestApplyPodResults(t *testing.T) {
	testCases := map[string]struct {
		resolution   time.Duration
		podMap       map[podKey]*pod
		clusterStart map[string]time.Time
		clusterEnd   map[string]time.Time
		resPods      []*prom.QueryResult
		ingestPodUID bool
		podUIDKeyMap map[podKey][]podKey
		expected     map[podKey]*pod
	}{
		"success": {
			resolution: time.Hour,
			podMap:     podMap2,
			clusterStart: map[string]time.Time{
				"cluster1": windowStart,
			},
			clusterEnd: map[string]time.Time{
				"cluster1": windowEnd,
			},
			resPods: []*prom.QueryResult{
				{
					Metric: map[string]interface{}{
						"cluster_id": "cluster1",
						"namespace":  "namespace1",
						"pod":        "pod2",
						"container":  "container2",
					},
					Values: []*util.Vector{
						{
							Timestamp: startFloat,
						},
						{
							Timestamp: startFloat + (hour * 24),
						},
					},
				},
			},
			ingestPodUID: false,
			podUIDKeyMap: nil,
			expected:     podMap3,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			applyPodResults(window, testCase.resolution, testCase.podMap, testCase.clusterStart, testCase.clusterEnd, testCase.resPods, testCase.ingestPodUID, testCase.podUIDKeyMap)
			if len(testCase.podMap) != len(testCase.expected) {
				t.Errorf("pod map does not have the expected length %d : %d", len(testCase.podMap), len(testCase.expected))
			}

			for expectedPodKey, expectedPod := range testCase.expected {
				actualPod, ok := testCase.podMap[expectedPodKey]
				if !ok {
					t.Errorf("pod map is missing key %s", expectedPodKey)
				}
				if !actualPod.equal(expectedPod) {
					t.Errorf("pod does not match with key %s: %s != %s", expectedPodKey, opencost.NewClosedWindow(actualPod.Start, actualPod.End), opencost.NewClosedWindow(expectedPod.Start, expectedPod.End))
				}
			}
		})
	}
}

func TestApplyCPUCoresAllocated(t *testing.T) {
	testCases := map[string]struct {
		podMap               map[podKey]*pod
		resCPUCoresAllocated []*prom.QueryResult
		podUIDKeyMap         map[podKey][]podKey
		expected             map[podKey]*pod
	}{
		"success": {
			podMap: podMap4,
			resCPUCoresAllocated: []*prom.QueryResult{
				{
					Metric: map[string]interface{}{
						"cluster_id": "cluster1",
						"namespace":  "namespace1",
						"pod":        "pod1",
						"container":  "container1",
						"node":       "node1",
					},
					Values: []*util.Vector{
						{
							Timestamp: startFloat,
						},
						{
							Timestamp: startFloat + (hour * 24),
						},
					},
				},
			},
			podUIDKeyMap: nil,
			expected:     podMap4,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			applyCPUCoresAllocated(testCase.podMap, testCase.resCPUCoresAllocated, testCase.podUIDKeyMap)
			if len(testCase.podMap) != len(testCase.expected) {
				t.Errorf("pod map does not have the expected length %d : %d", len(testCase.podMap), len(testCase.expected))
			}

			for expectedPodKey, expectedPod := range testCase.expected {
				actualPod, ok := testCase.podMap[expectedPodKey]
				if !ok {
					t.Errorf("pod map is missing key %s", expectedPodKey)
				}
				if !actualPod.equal(expectedPod) {
					t.Errorf("pod does not match with key %s: %s != %s", expectedPodKey, opencost.NewClosedWindow(actualPod.Start, actualPod.End), opencost.NewClosedWindow(expectedPod.Start, expectedPod.End))
				}
			}
		})
	}
}

func TestApplyCPUCoresRequested(t *testing.T) {
	testCases := map[string]struct {
		podMap               map[podKey]*pod
		resCPUCoresRequested []*prom.QueryResult
		podUIDKeyMap         map[podKey][]podKey
		expected             map[podKey]*pod
	}{
		"success": {
			podMap: podMap4,
			resCPUCoresRequested: []*prom.QueryResult{
				{
					Metric: map[string]interface{}{
						"cluster_id": "cluster1",
						"namespace":  "namespace1",
						"pod":        "pod1",
						"container":  "container1",
						"node":       "node1",
					},
					Values: []*util.Vector{
						{
							Timestamp: startFloat,
						},
						{
							Timestamp: startFloat + (hour * 24),
						},
					},
				},
			},
			podUIDKeyMap: nil,
			expected:     podMap4,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			applyCPUCoresRequested(testCase.podMap, testCase.resCPUCoresRequested, testCase.podUIDKeyMap)
			if len(testCase.podMap) != len(testCase.expected) {
				t.Errorf("pod map does not have the expected length %d : %d", len(testCase.podMap), len(testCase.expected))
			}

			for expectedPodKey, expectedPod := range testCase.expected {
				actualPod, ok := testCase.podMap[expectedPodKey]
				if !ok {
					t.Errorf("pod map is missing key %s", expectedPodKey)
				}
				if !actualPod.equal(expectedPod) {
					t.Errorf("pod does not match with key %s: %s != %s", expectedPodKey, opencost.NewClosedWindow(actualPod.Start, actualPod.End), opencost.NewClosedWindow(expectedPod.Start, expectedPod.End))
				}
			}
		})
	}
}

func TestApplyCPUCoresUsedAvg(t *testing.T) {
	testCases := map[string]struct {
		podMap             map[podKey]*pod
		resCPUCoresUsedAvg []*prom.QueryResult
		podUIDKeyMap       map[podKey][]podKey
		expected           map[podKey]*pod
	}{
		"success": {
			podMap: podMap4,
			resCPUCoresUsedAvg: []*prom.QueryResult{
				{
					Metric: map[string]interface{}{
						"cluster_id": "cluster1",
						"namespace":  "namespace1",
						"pod":        "pod1",
						"container":  "container1",
						"node":       "node1",
					},
					Values: []*util.Vector{
						{
							Timestamp: startFloat,
						},
						{
							Timestamp: startFloat + (hour * 24),
						},
					},
				},
			},
			podUIDKeyMap: nil,
			expected:     podMap4,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			applyCPUCoresUsedAvg(testCase.podMap, testCase.resCPUCoresUsedAvg, testCase.podUIDKeyMap)
			if len(testCase.podMap) != len(testCase.expected) {
				t.Errorf("pod map does not have the expected length %d : %d", len(testCase.podMap), len(testCase.expected))
			}

			for expectedPodKey, expectedPod := range testCase.expected {
				actualPod, ok := testCase.podMap[expectedPodKey]
				if !ok {
					t.Errorf("pod map is missing key %s", expectedPodKey)
				}
				if !actualPod.equal(expectedPod) {
					t.Errorf("pod does not match with key %s: %s != %s", expectedPodKey, opencost.NewClosedWindow(actualPod.Start, actualPod.End), opencost.NewClosedWindow(expectedPod.Start, expectedPod.End))
				}
			}
		})
	}
}

func TestApplyCPUCoresUsedMax(t *testing.T) {
	testCases := map[string]struct {
		podMap             map[podKey]*pod
		resCPUCoresUsedMax []*prom.QueryResult
		podUIDKeyMap       map[podKey][]podKey
		expected           map[podKey]*pod
	}{
		"success": {
			podMap: podMap4,
			resCPUCoresUsedMax: []*prom.QueryResult{
				{
					Metric: map[string]interface{}{
						"cluster_id": "cluster1",
						"namespace":  "namespace1",
						"pod":        "pod1",
						"container":  "container1",
						"node":       "node1",
					},
					Values: []*util.Vector{
						{
							Timestamp: startFloat,
						},
						{
							Timestamp: startFloat + (hour * 24),
						},
					},
				},
			},
			podUIDKeyMap: nil,
			expected:     podMap4,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			applyCPUCoresUsedMax(testCase.podMap, testCase.resCPUCoresUsedMax, testCase.podUIDKeyMap)
			if len(testCase.podMap) != len(testCase.expected) {
				t.Errorf("pod map does not have the expected length %d : %d", len(testCase.podMap), len(testCase.expected))
			}

			for expectedPodKey, expectedPod := range testCase.expected {
				actualPod, ok := testCase.podMap[expectedPodKey]
				if !ok {
					t.Errorf("pod map is missing key %s", expectedPodKey)
				}
				if !actualPod.equal(expectedPod) {
					t.Errorf("pod does not match with key %s: %s != %s", expectedPodKey, opencost.NewClosedWindow(actualPod.Start, actualPod.End), opencost.NewClosedWindow(expectedPod.Start, expectedPod.End))
				}
			}
		})
	}
}

func TestApplyRAMBytesAllocated(t *testing.T) {
	testCases := map[string]struct {
		podMap               map[podKey]*pod
		resRAMBytesAllocated []*prom.QueryResult
		podUIDKeyMap         map[podKey][]podKey
		expected             map[podKey]*pod
	}{
		"success": {
			podMap: podMap4,
			resRAMBytesAllocated: []*prom.QueryResult{
				{
					Metric: map[string]interface{}{
						"cluster_id": "cluster1",
						"namespace":  "namespace1",
						"pod":        "pod1",
						"container":  "container1",
						"node":       "node1",
					},
					Values: []*util.Vector{
						{
							Timestamp: startFloat,
						},
						{
							Timestamp: startFloat + (hour * 24),
						},
					},
				},
			},
			podUIDKeyMap: nil,
			expected:     podMap4,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			applyRAMBytesAllocated(testCase.podMap, testCase.resRAMBytesAllocated, testCase.podUIDKeyMap)
			if len(testCase.podMap) != len(testCase.expected) {
				t.Errorf("pod map does not have the expected length %d : %d", len(testCase.podMap), len(testCase.expected))
			}

			for expectedPodKey, expectedPod := range testCase.expected {
				actualPod, ok := testCase.podMap[expectedPodKey]
				if !ok {
					t.Errorf("pod map is missing key %s", expectedPodKey)
				}
				if !actualPod.equal(expectedPod) {
					t.Errorf("pod does not match with key %s: %s != %s", expectedPodKey, opencost.NewClosedWindow(actualPod.Start, actualPod.End), opencost.NewClosedWindow(expectedPod.Start, expectedPod.End))
				}
			}
		})
	}
}

func TestApplyRAMBytesRequested(t *testing.T) {
	testCases := map[string]struct {
		podMap               map[podKey]*pod
		resRAMBytesRequested []*prom.QueryResult
		podUIDKeyMap         map[podKey][]podKey
		expected             map[podKey]*pod
	}{
		"success": {
			podMap: podMap4,
			resRAMBytesRequested: []*prom.QueryResult{
				{
					Metric: map[string]interface{}{
						"cluster_id": "cluster1",
						"namespace":  "namespace1",
						"pod":        "pod1",
						"container":  "container1",
						"node":       "node1",
					},
					Values: []*util.Vector{
						{
							Timestamp: startFloat,
						},
						{
							Timestamp: startFloat + (hour * 24),
						},
					},
				},
			},
			podUIDKeyMap: nil,
			expected:     podMap4,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			applyRAMBytesRequested(testCase.podMap, testCase.resRAMBytesRequested, testCase.podUIDKeyMap)
			if len(testCase.podMap) != len(testCase.expected) {
				t.Errorf("pod map does not have the expected length %d : %d", len(testCase.podMap), len(testCase.expected))
			}

			for expectedPodKey, expectedPod := range testCase.expected {
				actualPod, ok := testCase.podMap[expectedPodKey]
				if !ok {
					t.Errorf("pod map is missing key %s", expectedPodKey)
				}
				if !actualPod.equal(expectedPod) {
					t.Errorf("pod does not match with key %s: %s != %s", expectedPodKey, opencost.NewClosedWindow(actualPod.Start, actualPod.End), opencost.NewClosedWindow(expectedPod.Start, expectedPod.End))
				}
			}
		})
	}
}

func TestApplyRAMBytesUsedAvg(t *testing.T) {
	testCases := map[string]struct {
		podMap             map[podKey]*pod
		resRAMBytesUsedAvg []*prom.QueryResult
		podUIDKeyMap       map[podKey][]podKey
		expected           map[podKey]*pod
	}{
		"success": {
			podMap: podMap4,
			resRAMBytesUsedAvg: []*prom.QueryResult{
				{
					Metric: map[string]interface{}{
						"cluster_id": "cluster1",
						"namespace":  "namespace1",
						"pod":        "pod1",
						"container":  "container1",
						"node":       "node1",
					},
					Values: []*util.Vector{
						{
							Timestamp: startFloat,
						},
						{
							Timestamp: startFloat + (hour * 24),
						},
					},
				},
			},
			podUIDKeyMap: nil,
			expected:     podMap4,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			applyRAMBytesUsedAvg(testCase.podMap, testCase.resRAMBytesUsedAvg, testCase.podUIDKeyMap)
			if len(testCase.podMap) != len(testCase.expected) {
				t.Errorf("pod map does not have the expected length %d : %d", len(testCase.podMap), len(testCase.expected))
			}

			for expectedPodKey, expectedPod := range testCase.expected {
				actualPod, ok := testCase.podMap[expectedPodKey]
				if !ok {
					t.Errorf("pod map is missing key %s", expectedPodKey)
				}
				if !actualPod.equal(expectedPod) {
					t.Errorf("pod does not match with key %s: %s != %s", expectedPodKey, opencost.NewClosedWindow(actualPod.Start, actualPod.End), opencost.NewClosedWindow(expectedPod.Start, expectedPod.End))
				}
			}
		})
	}
}

func TestApplyRAMBytesUsedMax(t *testing.T) {
	testCases := map[string]struct {
		podMap             map[podKey]*pod
		resRAMBytesUsedMax []*prom.QueryResult
		podUIDKeyMap       map[podKey][]podKey
		expected           map[podKey]*pod
	}{
		"success": {
			podMap: podMap4,
			resRAMBytesUsedMax: []*prom.QueryResult{
				{
					Metric: map[string]interface{}{
						"cluster_id": "cluster1",
						"namespace":  "namespace1",
						"pod":        "pod1",
						"container":  "container1",
						"node":       "node1",
					},
					Values: []*util.Vector{
						{
							Timestamp: startFloat,
						},
						{
							Timestamp: startFloat + (hour * 24),
						},
					},
				},
			},
			podUIDKeyMap: nil,
			expected:     podMap4,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			applyRAMBytesUsedMax(testCase.podMap, testCase.resRAMBytesUsedMax, testCase.podUIDKeyMap)
			if len(testCase.podMap) != len(testCase.expected) {
				t.Errorf("pod map does not have the expected length %d : %d", len(testCase.podMap), len(testCase.expected))
			}

			for expectedPodKey, expectedPod := range testCase.expected {
				actualPod, ok := testCase.podMap[expectedPodKey]
				if !ok {
					t.Errorf("pod map is missing key %s", expectedPodKey)
				}
				if !actualPod.equal(expectedPod) {
					t.Errorf("pod does not match with key %s: %s != %s", expectedPodKey, opencost.NewClosedWindow(actualPod.Start, actualPod.End), opencost.NewClosedWindow(expectedPod.Start, expectedPod.End))
				}
			}
		})
	}
}

func TestApplyGPUUsageAvg(t *testing.T) {
	testCases := map[string]struct {
		podMap         map[podKey]*pod
		resGPUUsageAvg []*prom.QueryResult
		podUIDKeyMap   map[podKey][]podKey
		expected       map[podKey]*pod
	}{
		"success": {
			podMap: podMap4,
			resGPUUsageAvg: []*prom.QueryResult{
				{
					Metric: map[string]interface{}{
						"cluster_id": "cluster1",
						"namespace":  "namespace1",
						"pod":        "pod1",
						"container":  "container1",
						"node":       "node1",
					},
					Values: []*util.Vector{
						{
							Timestamp: startFloat,
						},
						{
							Timestamp: startFloat + (hour * 24),
						},
					},
				},
			},
			podUIDKeyMap: nil,
			expected:     podMap4,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			applyGPUUsageAvg(testCase.podMap, testCase.resGPUUsageAvg, testCase.podUIDKeyMap)
			if len(testCase.podMap) != len(testCase.expected) {
				t.Errorf("pod map does not have the expected length %d : %d", len(testCase.podMap), len(testCase.expected))
			}

			for expectedPodKey, expectedPod := range testCase.expected {
				actualPod, ok := testCase.podMap[expectedPodKey]
				if !ok {
					t.Errorf("pod map is missing key %s", expectedPodKey)
				}
				if !actualPod.equal(expectedPod) {
					t.Errorf("pod does not match with key %s: %s != %s", expectedPodKey, opencost.NewClosedWindow(actualPod.Start, actualPod.End), opencost.NewClosedWindow(expectedPod.Start, expectedPod.End))
				}
			}
		})
	}
}

func TestApplyGPUsAllocated(t *testing.T) {
	testCases := map[string]struct {
		podMap           map[podKey]*pod
		resGPUsRequested []*prom.QueryResult
		resGPUsAllocated []*prom.QueryResult
		podUIDKeyMap     map[podKey][]podKey
		expected         map[podKey]*pod
	}{
		"success": {
			podMap: podMap4,
			resGPUsRequested: []*prom.QueryResult{
				{
					Metric: map[string]interface{}{
						"cluster_id": "cluster1",
						"namespace":  "namespace1",
						"pod":        "pod1",
						"container":  "container1",
						"node":       "node1",
					},
					Values: []*util.Vector{
						{
							Timestamp: startFloat,
						},
						{
							Timestamp: startFloat + (hour * 24),
						},
					},
				},
			},
			resGPUsAllocated: nil,
			podUIDKeyMap:     nil,
			expected:         podMap4,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			applyGPUsAllocated(testCase.podMap, testCase.resGPUsRequested, testCase.resGPUsAllocated, testCase.podUIDKeyMap)
			if len(testCase.podMap) != len(testCase.expected) {
				t.Errorf("pod map does not have the expected length %d : %d", len(testCase.podMap), len(testCase.expected))
			}

			for expectedPodKey, expectedPod := range testCase.expected {
				actualPod, ok := testCase.podMap[expectedPodKey]
				if !ok {
					t.Errorf("pod map is missing key %s", expectedPodKey)
				}
				if !actualPod.equal(expectedPod) {
					t.Errorf("pod does not match with key %s: %s != %s", expectedPodKey, opencost.NewClosedWindow(actualPod.Start, actualPod.End), opencost.NewClosedWindow(expectedPod.Start, expectedPod.End))
				}
			}
		})
	}
}

func TestApplyNetworkTotals(t *testing.T) {
	testCases := map[string]struct {
		podMap                  map[podKey]*pod
		resNetworkTransferBytes []*prom.QueryResult
		resNetworkReceiveBytes  []*prom.QueryResult
		podUIDKeyMap            map[podKey][]podKey
		expected                map[podKey]*pod
	}{
		"success": {
			podMap: podMap4,
			resNetworkTransferBytes: []*prom.QueryResult{
				{
					Metric: map[string]interface{}{
						"cluster_id": "cluster1",
						"namespace":  "namespace1",
						"pod":        "pod1",
						"container":  "container1",
						"node":       "node1",
					},
					Values: []*util.Vector{
						{
							Timestamp: startFloat,
						},
						{
							Timestamp: startFloat + (hour * 24),
						},
					},
				},
			},
			resNetworkReceiveBytes: []*prom.QueryResult{
				{
					Metric: map[string]interface{}{
						"cluster_id": "cluster1",
						"namespace":  "namespace1",
						"pod":        "pod1",
						"container":  "container1",
						"node":       "node1",
					},
					Values: []*util.Vector{
						{
							Timestamp: startFloat,
						},
						{
							Timestamp: startFloat + (hour * 24),
						},
					},
				},
			},
			podUIDKeyMap: nil,
			expected:     podMap4,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			applyNetworkTotals(testCase.podMap, testCase.resNetworkTransferBytes, testCase.resNetworkReceiveBytes, testCase.podUIDKeyMap)
			if len(testCase.podMap) != len(testCase.expected) {
				t.Errorf("pod map does not have the expected length %d : %d", len(testCase.podMap), len(testCase.expected))
			}

			for expectedPodKey, expectedPod := range testCase.expected {
				actualPod, ok := testCase.podMap[expectedPodKey]
				if !ok {
					t.Errorf("pod map is missing key %s", expectedPodKey)
				}
				if !actualPod.equal(expectedPod) {
					t.Errorf("pod does not match with key %s: %s != %s", expectedPodKey, opencost.NewClosedWindow(actualPod.Start, actualPod.End), opencost.NewClosedWindow(expectedPod.Start, expectedPod.End))
				}
			}
		})
	}
}

func TestApplyNetworkAllocation(t *testing.T) {
	testCases := map[string]struct {
		podMap               map[podKey]*pod
		resNetworkGiB        []*prom.QueryResult
		resNetworkCostPerGiB []*prom.QueryResult
		podUIDKeyMap         map[podKey][]podKey
		networkCostSubType   string
		expected             map[podKey]*pod
	}{
		"success": {
			podMap: podMap4,
			resNetworkGiB: []*prom.QueryResult{
				{
					Metric: map[string]interface{}{
						"cluster_id": "cluster1",
						"namespace":  "namespace1",
						"pod":        "pod1",
						"container":  "container1",
						"node":       "node1",
					},
					Values: []*util.Vector{
						{
							Timestamp: startFloat,
						},
						{
							Timestamp: startFloat + (hour * 24),
						},
					},
				},
			},
			resNetworkCostPerGiB: []*prom.QueryResult{
				{
					Metric: map[string]interface{}{
						"cluster_id": "cluster1",
						"namespace":  "namespace1",
						"pod":        "pod1",
						"container":  "container1",
						"node":       "node1",
					},
					Values: []*util.Vector{
						{
							Timestamp: startFloat,
						},
						{
							Timestamp: startFloat + (hour * 24),
						},
					},
				},
			},
			podUIDKeyMap:       nil,
			networkCostSubType: networkInternetCost,
			expected:           podMap4,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			applyNetworkAllocation(testCase.podMap, testCase.resNetworkGiB, testCase.resNetworkCostPerGiB, testCase.podUIDKeyMap, testCase.networkCostSubType)
			if len(testCase.podMap) != len(testCase.expected) {
				t.Errorf("pod map does not have the expected length %d : %d", len(testCase.podMap), len(testCase.expected))
			}

			for expectedPodKey, expectedPod := range testCase.expected {
				actualPod, ok := testCase.podMap[expectedPodKey]
				if !ok {
					t.Errorf("pod map is missing key %s", expectedPodKey)
				}
				if !actualPod.equal(expectedPod) {
					t.Errorf("pod does not match with key %s: %s != %s", expectedPodKey, opencost.NewClosedWindow(actualPod.Start, actualPod.End), opencost.NewClosedWindow(expectedPod.Start, expectedPod.End))
				}
			}
		})
	}
}

func TestResToNamespaceLabels(t *testing.T) {
	testCases := map[string]struct {
		resNamespaceLabels []*prom.QueryResult
		expected           map[namespaceKey]map[string]string
	}{
		"success": {
			resNamespaceLabels: []*prom.QueryResult{
				{
					Metric: map[string]interface{}{
						"cluster_id": "cluster1",
						"namespace":  "namespace1",
						"pod":        "pod1",
						"container":  "container1",
						"node":       "node1",
						"label_test": "test",
					},
					Values: []*util.Vector{
						{
							Timestamp: startFloat,
						},
						{
							Timestamp: startFloat + (hour * 24),
						},
					},
				},
			},
			expected: map[namespaceKey]map[string]string{
				{
					Cluster:   "cluster1",
					Namespace: "namespace1",
				}: {
					"test": "test",
				},
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			labels := resToNamespaceLabels(testCase.resNamespaceLabels)
			if len(labels) != len(testCase.expected) {
				t.Errorf("label map does not have the expected length %d : %d", len(labels), len(testCase.expected))
			}

			for expectedLabelsKey, expectedLabels := range testCase.expected {
				actualLabels, ok := labels[expectedLabelsKey]
				if !ok {
					t.Errorf("label map is missing key %s", expectedLabelsKey)
				}

				for expectedLabelKey, expectedLabel := range expectedLabels {
					actualLabel, ok := actualLabels[expectedLabelKey]
					if !ok {
						t.Errorf("label map is missing key %s", expectedLabelKey)
					}

					if actualLabel != expectedLabel {
						t.Errorf("label does not match with key  %s: %s != %s", expectedLabelKey, actualLabel, expectedLabel)
					}
				}
			}
		})
	}
}

func TestResToPodLabels(t *testing.T) {
	testCases := map[string]struct {
		resPodLabels []*prom.QueryResult
		podUIDKeyMap map[podKey][]podKey
		ingestPodUID bool
		expected     map[podKey]map[string]string
	}{
		"success": {
			resPodLabels: []*prom.QueryResult{
				{
					Metric: map[string]interface{}{
						"cluster_id": "cluster1",
						"namespace":  "namespace1",
						"pod":        "pod1",
						"container":  "container1",
						"node":       "node1",
						"label_test": "test",
					},
					Values: []*util.Vector{
						{
							Timestamp: startFloat,
						},
						{
							Timestamp: startFloat + (hour * 24),
						},
					},
				},
			},
			podUIDKeyMap: nil,
			ingestPodUID: false,
			expected: map[podKey]map[string]string{
				{
					namespaceKey: namespaceKey{
						Cluster:   "cluster1",
						Namespace: "namespace1",
					},
					Pod: "pod1",
				}: {
					"test": "test",
				},
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			labels := resToPodLabels(testCase.resPodLabels, testCase.podUIDKeyMap, testCase.ingestPodUID)
			if len(labels) != len(testCase.expected) {
				t.Errorf("label map does not have the expected length %d : %d", len(labels), len(testCase.expected))
			}

			for expectedLabelsKey, expectedLabels := range testCase.expected {
				actualLabels, ok := labels[expectedLabelsKey]
				if !ok {
					t.Errorf("label map is missing key %s", expectedLabelsKey)
				}

				for expectedLabelKey, expectedLabel := range expectedLabels {
					actualLabel, ok := actualLabels[expectedLabelKey]
					if !ok {
						t.Errorf("label map is missing key %s", expectedLabelKey)
					}

					if actualLabel != expectedLabel {
						t.Errorf("label does not match with key  %s: %s != %s", expectedLabelKey, actualLabel, expectedLabel)
					}
				}
			}
		})
	}
}

func TestResToNamespaceAnnotations(t *testing.T) {
	testCases := map[string]struct {
		resNamespaceAnnotations []*prom.QueryResult
		expected                map[string]map[string]string
	}{
		"success": {
			resNamespaceAnnotations: []*prom.QueryResult{
				{
					Metric: map[string]interface{}{
						"cluster_id":      "cluster1",
						"namespace":       "namespace1",
						"pod":             "pod1",
						"container":       "container1",
						"node":            "node1",
						"annotation_test": "test",
					},
					Values: []*util.Vector{
						{
							Timestamp: startFloat,
						},
						{
							Timestamp: startFloat + (hour * 24),
						},
					},
				},
			},
			expected: map[string]map[string]string{
				"namespace1": {
					"test": "test",
				},
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			labels := resToNamespaceAnnotations(testCase.resNamespaceAnnotations)
			if len(labels) != len(testCase.expected) {
				t.Errorf("label map does not have the expected length %d : %d", len(labels), len(testCase.expected))
			}

			for expectedLabelsKey, expectedLabels := range testCase.expected {
				actualLabels, ok := labels[expectedLabelsKey]
				if !ok {
					t.Errorf("label map is missing key %s", expectedLabelsKey)
				}

				for expectedLabelKey, expectedLabel := range expectedLabels {
					actualLabel, ok := actualLabels[expectedLabelKey]
					if !ok {
						t.Errorf("label map is missing key %s", expectedLabelKey)
					}

					if actualLabel != expectedLabel {
						t.Errorf("label does not match with key  %s: %s != %s", expectedLabelKey, actualLabel, expectedLabel)
					}
				}
			}
		})
	}
}

func TestResToPodAnnotations(t *testing.T) {
	testCases := map[string]struct {
		resPodAnnotations []*prom.QueryResult
		podUIDKeyMap      map[podKey][]podKey
		ingestPodUID      bool
		expected          map[podKey]map[string]string
	}{
		"success": {
			resPodAnnotations: []*prom.QueryResult{
				{
					Metric: map[string]interface{}{
						"cluster_id":      "cluster1",
						"namespace":       "namespace1",
						"pod":             "pod1",
						"container":       "container1",
						"node":            "node1",
						"annotation_test": "test",
					},
					Values: []*util.Vector{
						{
							Timestamp: startFloat,
						},
						{
							Timestamp: startFloat + (hour * 24),
						},
					},
				},
			},
			podUIDKeyMap: nil,
			ingestPodUID: false,
			expected: map[podKey]map[string]string{
				{
					namespaceKey: namespaceKey{
						Cluster:   "cluster1",
						Namespace: "namespace1",
					},
					Pod: "pod1",
				}: {
					"test": "test",
				},
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			labels := resToPodAnnotations(testCase.resPodAnnotations, testCase.podUIDKeyMap, testCase.ingestPodUID)
			if len(labels) != len(testCase.expected) {
				t.Errorf("label map does not have the expected length %d : %d", len(labels), len(testCase.expected))
			}

			for expectedLabelsKey, expectedLabels := range testCase.expected {
				actualLabels, ok := labels[expectedLabelsKey]
				if !ok {
					t.Errorf("label map is missing key %s", expectedLabelsKey)
				}

				for expectedLabelKey, expectedLabel := range expectedLabels {
					actualLabel, ok := actualLabels[expectedLabelKey]
					if !ok {
						t.Errorf("label map is missing key %s", expectedLabelKey)
					}

					if actualLabel != expectedLabel {
						t.Errorf("label does not match with key  %s: %s != %s", expectedLabelKey, actualLabel, expectedLabel)
					}
				}
			}
		})
	}
}
