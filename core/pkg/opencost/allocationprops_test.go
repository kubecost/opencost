package opencost

import (
	"reflect"
	"testing"
)

func TestAllocationPropsIntersection(t *testing.T) {
	cases := map[string]struct {
		allocationProps1 *AllocationProperties
		allocationProps2 *AllocationProperties
		expected         *AllocationProperties
	}{
		"intersection two allocation properties with empty labels/annotations": {
			allocationProps1: &AllocationProperties{
				Labels:      map[string]string{},
				Annotations: map[string]string{},
			},
			allocationProps2: &AllocationProperties{
				Labels:      map[string]string{},
				Annotations: map[string]string{},
			},
			expected: &AllocationProperties{
				Labels:               nil,
				Annotations:          nil,
				NamespaceLabels:      map[string]string{},
				NamespaceAnnotations: map[string]string{},
			},
		},
		"nil intersection": {
			allocationProps1: nil,
			allocationProps2: nil,
			expected:         nil,
		},
		"intersection, with labels/annotations, no aggregated metadata": {
			allocationProps1: &AllocationProperties{
				AggregatedMetadata: false,
				Node:               "node1",
				Labels:             map[string]string{"key1": "val1"},
				Annotations:        map[string]string{"key2": "val2"},
			},
			allocationProps2: &AllocationProperties{
				AggregatedMetadata: false,
				Node:               "node1",
				Labels:             map[string]string{"key3": "val3"},
				Annotations:        map[string]string{"key4": "val4"},
			},
			expected: &AllocationProperties{
				AggregatedMetadata:   false,
				Node:                 "node1",
				Labels:               nil,
				Annotations:          nil,
				NamespaceLabels:      map[string]string{},
				NamespaceAnnotations: map[string]string{},
			},
		},
		"intersection, with labels/annotations, same values": {
			allocationProps1: &AllocationProperties{
				AggregatedMetadata: false,
				ControllerKind:     "controller1",
				Namespace:          "ns1",
				Labels:             map[string]string{"key1": "val1"},
				Annotations:        map[string]string{"key2": "val2"},
			},
			allocationProps2: &AllocationProperties{
				AggregatedMetadata: true,
				ControllerKind:     "controller2",
				Namespace:          "ns1",
				Labels:             map[string]string{"key1": "val1"},
				Annotations:        map[string]string{"key2": "val2"},
			},
			expected: &AllocationProperties{
				AggregatedMetadata:   true,
				Namespace:            "ns1",
				ControllerKind:       "",
				Labels:               map[string]string{"key1": "val1"},
				Annotations:          map[string]string{"key2": "val2"},
				NamespaceLabels:      map[string]string{},
				NamespaceAnnotations: map[string]string{},
			},
		},
		"intersection, with labels/annotations, special case container": {
			allocationProps1: &AllocationProperties{
				AggregatedMetadata: false,
				Container:          UnmountedSuffix,
				Namespace:          "ns1",
				Labels:             map[string]string{},
				Annotations:        map[string]string{},
			},
			allocationProps2: &AllocationProperties{
				AggregatedMetadata: true,
				Container:          "container3",
				Namespace:          "ns1",
				Labels:             map[string]string{"key1": "val1"},
				Annotations:        map[string]string{"key2": "val2"},
			},
			expected: &AllocationProperties{
				AggregatedMetadata:   true,
				Namespace:            "ns1",
				ControllerKind:       "",
				Labels:               map[string]string{"key1": "val1"},
				Annotations:          map[string]string{"key2": "val2"},
				NamespaceLabels:      map[string]string{},
				NamespaceAnnotations: map[string]string{},
			},
		},
		"test services are nulled when intersecting": {
			allocationProps1: &AllocationProperties{
				AggregatedMetadata: false,
				Container:          UnmountedSuffix,
				Namespace:          "ns1",
				Services: []string{
					"cool",
				},
				Labels:      map[string]string{},
				Annotations: map[string]string{},
			},
			allocationProps2: &AllocationProperties{
				AggregatedMetadata: true,
				Container:          "container3",
				Namespace:          "ns1",
				Labels:             map[string]string{"key1": "val1"},
				Annotations:        map[string]string{"key2": "val2"},
			},
			expected: &AllocationProperties{
				AggregatedMetadata:   true,
				Namespace:            "ns1",
				ControllerKind:       "",
				Labels:               map[string]string{"key1": "val1"},
				Annotations:          map[string]string{"key2": "val2"},
				NamespaceLabels:      map[string]string{},
				NamespaceAnnotations: map[string]string{},
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {

			actual := tc.allocationProps1.Intersection(tc.allocationProps2)

			if !reflect.DeepEqual(actual, tc.expected) {
				t.Fatalf("test case %s: expected %+v; got %+v", name, tc.expected, actual)
			}
		})
	}
}
func TestGenerateKey(t *testing.T) {

	customOwnerLabelConfig := NewLabelConfig()
	customOwnerLabelConfig.OwnerLabel = "example_com_project"

	cases := map[string]struct {
		aggregate       []string
		allocationProps *AllocationProperties
		labelConfig     *LabelConfig
		expected        string
	}{
		"aggregate by owner without owner labels": {
			aggregate: []string{"owner"},
			allocationProps: &AllocationProperties{
				Labels:      map[string]string{"app": "cost-analyzer"},
				Annotations: map[string]string{"owner": "test owner 123"},
			},
			expected: "test owner 123",
		},
		"aggregate by owner without labels": {
			aggregate: []string{"owner"},
			allocationProps: &AllocationProperties{
				Annotations: map[string]string{"owner": "test owner 123"},
			},
			expected: "test owner 123",
		},
		"aggregate by owner with owner label and annotation": {
			aggregate: []string{"owner"},
			allocationProps: &AllocationProperties{
				Labels:      map[string]string{"owner": "owner-label"},
				Annotations: map[string]string{"owner": "owner-annotation"},
			},
			expected: "owner-label",
		},
		"aggregate by environment with environment label and annotation": {
			aggregate: []string{"environment"},
			allocationProps: &AllocationProperties{
				Labels:      map[string]string{"env": "environment-label"},
				Annotations: map[string]string{"env": "environment-annotation"},
			},
			expected: "environment-label",
		},
		"aggregate by department with department label and annotation": {
			aggregate: []string{"department"},
			allocationProps: &AllocationProperties{
				Labels:      map[string]string{"department": "department-label"},
				Annotations: map[string]string{"department": "department-annotation"},
			},
			expected: "department-label",
		},
		"aggregate by team with team label and annotation": {
			aggregate: []string{"team"},
			allocationProps: &AllocationProperties{
				Labels:      map[string]string{"team": "team-label"},
				Annotations: map[string]string{"team": "team-annotation"},
			},
			expected: "team-label",
		},
		"aggregate by product with product label and annotation": {
			aggregate: []string{"product"},
			allocationProps: &AllocationProperties{
				Labels:      map[string]string{"app": "product-label"},
				Annotations: map[string]string{"app": "product-annotation"},
			},
			expected: "product-label",
		},
		"aggregate by product and owner with multiple labels and annotations": {
			aggregate: []string{"product", "owner"},
			allocationProps: &AllocationProperties{
				Labels:      map[string]string{"app": "product-label", "owner": "owner-label", "team": "team-label"},
				Annotations: map[string]string{"app": "product-annotation", "owner": "owner-annotation", "team": "team-annotation"},
			},
			expected: "product-label/owner-label",
		},
		"user test": {
			aggregate: []string{"owner"},
			allocationProps: &AllocationProperties{
				Labels:      map[string]string{"app_kubernetes_io_name": "x-mongo", "example_com_service_owner": "x", "component": "primary", "controller_revision_hash": "x-mongo-primary-x", "kubernetes_io_metadata_name": "app-microservices", "name": "app-microservices", "statefulset_kubernetes_io_pod_name": "x-mongo-primary-0"},
				Annotations: map[string]string{"example_com_project": "redacted"},
			},
			labelConfig: customOwnerLabelConfig,
			expected:    "redacted",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {

			lc := NewLabelConfig()

			if tc.labelConfig != nil {
				lc = tc.labelConfig
			}

			result := tc.allocationProps.GenerateKey(tc.aggregate, lc)

			if !reflect.DeepEqual(result, tc.expected) {
				t.Fatalf("expected %+v; got %+v", tc.expected, result)
			}
		})
	}
}

func TestIntersection(t *testing.T) {

	propsEmpty := AllocationProperties{}

	propsMedium := AllocationProperties{
		Cluster:        "cluster1",
		Node:           "Node1",
		Container:      "container1",
		Controller:     "controller1",
		ControllerKind: "controllerkind1",
		Namespace:      "ns1",
		Pod:            "pod1",
		Services:       []string{"service1"},
		ProviderID:     "provider1",
	}

	propsFull := AllocationProperties{
		Cluster:              "cluster2",
		Node:                 "Node2",
		Container:            "container2",
		Controller:           "controller2",
		ControllerKind:       "controllerkind2",
		Namespace:            "ns2",
		Pod:                  "pod2",
		Services:             []string{"service2"},
		ProviderID:           "provider2",
		NamespaceLabels:      AllocationLabels{"key1": "value1"},
		NamespaceAnnotations: AllocationAnnotations{"key2": "value2", "key5": "value5"},
		Labels:               AllocationLabels{"key3": "value3"},
		Annotations:          AllocationAnnotations{"key4": "value4"},
	}

	// Case 1: no intersection
	// expect empty result object
	testObj1 := AllocationProperties{}

	result := testObj1.Intersection(&propsEmpty)

	if !result.Equal(&propsEmpty) {
		t.Fatalf("Case 1: expected empty object, no intersection")
	}

	// Case 2: Only has labels/annotations
	// expect empty result object
	testObj2 := AllocationProperties{
		Labels:      map[string]string{"app": "product-label-light"},
		Annotations: map[string]string{"app": "product-annotation-light"},
	}

	result = testObj2.Intersection(&propsMedium)

	if !result.Equal(&propsEmpty) {
		t.Fatalf("Case 2: expected empty object, no intersection")
	}

	// Case 3: Has non-label/annotations set
	// expect all non label/annotation/service string array fields to be unset
	// different container names should be omitted
	testObj3 := AllocationProperties{
		Cluster:        "cluster1",
		Node:           "Node1",
		Container:      "container2",
		Controller:     "controller1",
		ControllerKind: "controllerkind1",
		Namespace:      "ns1",
		Pod:            "pod1",
		Services:       []string{"service1"},
		ProviderID:     "provider1",
	}

	expectedResult := AllocationProperties{
		Cluster:        "cluster1",
		Node:           "Node1",
		Controller:     "controller1",
		ControllerKind: "controllerkind1",
		Namespace:      "ns1",
		Pod:            "pod1",
		ProviderID:     "provider1",
	}

	result = testObj3.Intersection(&propsMedium)

	if !result.Equal(&expectedResult) {
		t.Fatalf("Case 3: expected output %v does not match actual output %v", expectedResult, result)
	}

	// Case 4: Copy over NamespaceLabels/Annots when namespace is the same
	testObj4 := AllocationProperties{
		Cluster:              "cluster2",
		Node:                 "NodeX",
		Container:            "containerX",
		Controller:           "controllerX",
		ControllerKind:       "controllerkindX",
		Namespace:            "ns2",
		Pod:                  "podX",
		Services:             []string{"serviceX"},
		ProviderID:           "providerX",
		NamespaceLabels:      AllocationLabels{"key1": "value1"},
		NamespaceAnnotations: AllocationAnnotations{"key2": "value2", "key5": "value5"},
		Labels:               AllocationLabels{"key3": "value3"},
		Annotations:          AllocationAnnotations{"key4": "value4"},
	}

	expectedResult = AllocationProperties{
		Cluster:              "cluster2",
		Namespace:            "ns2",
		NamespaceLabels:      AllocationLabels{"key1": "value1"},
		NamespaceAnnotations: AllocationAnnotations{"key2": "value2", "key5": "value5"},
	}

	result = testObj4.Intersection(&propsFull)

	if !result.Equal(&expectedResult) {
		t.Fatalf("Case 4: expected output %v does not match actual output %v", expectedResult, result)
	}

}
