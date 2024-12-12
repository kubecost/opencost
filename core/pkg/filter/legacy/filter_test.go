package legacy_test

import (
	"testing"

	filter "github.com/opencost/opencost/core/pkg/filter/legacy"
	"github.com/opencost/opencost/core/pkg/opencost"
)

func Test_String_Matches(t *testing.T) {
	cases := []struct {
		name   string
		a      *opencost.Allocation
		filter filter.Filter[*opencost.Allocation]

		expected bool
	}{
		{
			name: "ClusterID Equals -> true",
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Cluster: "cluster-one",
				},
			},
			filter: filter.StringProperty[*opencost.Allocation]{
				Field: opencost.AllocationClusterProp,
				Op:    filter.StringEquals,
				Value: "cluster-one",
			},

			expected: true,
		},
		{
			name: "ClusterID StartsWith -> true",
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Cluster: "cluster-one",
				},
			},
			filter: filter.StringProperty[*opencost.Allocation]{
				Field: opencost.AllocationClusterProp,
				Op:    filter.StringStartsWith,
				Value: "cluster",
			},

			expected: true,
		},
		{
			name: "ClusterID StartsWith -> false",
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Cluster: "k8s-one",
				},
			},
			filter: filter.StringProperty[*opencost.Allocation]{
				Field: opencost.AllocationClusterProp,
				Op:    filter.StringStartsWith,
				Value: "cluster",
			},

			expected: false,
		},
		{
			name: "ClusterID empty StartsWith '' -> true",
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Cluster: "",
				},
			},
			filter: filter.StringProperty[*opencost.Allocation]{
				Field: opencost.AllocationClusterProp,
				Op:    filter.StringStartsWith,
				Value: "",
			},

			expected: true,
		},
		{
			name: "ClusterID nonempty StartsWith '' -> true",
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Cluster: "abc",
				},
			},
			filter: filter.StringProperty[*opencost.Allocation]{
				Field: opencost.AllocationClusterProp,
				Op:    filter.StringStartsWith,
				Value: "",
			},

			expected: true,
		},
		{
			name: "Node Equals -> true",
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Node: "node123",
				},
			},
			filter: filter.StringProperty[*opencost.Allocation]{
				Field: opencost.AllocationNodeProp,
				Op:    filter.StringEquals,
				Value: "node123",
			},

			expected: true,
		},
		{
			name: "Namespace Equals Unallocated -> true",
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Namespace: "",
				},
			},
			filter: filter.StringProperty[*opencost.Allocation]{
				Field: opencost.AllocationNamespaceProp,
				Op:    filter.StringEquals,
				Value: opencost.UnallocatedSuffix,
			},

			expected: true,
		},
		{
			name: "ControllerKind Equals -> true",
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					ControllerKind: "deployment", // We generally store controller kinds as all lowercase
				},
			},
			filter: filter.StringProperty[*opencost.Allocation]{
				Field: opencost.AllocationControllerKindProp,
				Op:    filter.StringEquals,
				Value: "deployment",
			},

			expected: true,
		},
		{
			name: "ControllerName Equals -> true",
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Controller: "kc-cost-analyzer",
				},
			},
			filter: filter.StringProperty[*opencost.Allocation]{
				Field: opencost.AllocationControllerProp,
				Op:    filter.StringEquals,
				Value: "kc-cost-analyzer",
			},

			expected: true,
		},
		{
			name: "Pod (with UID) Equals -> true",
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Pod: "pod-123 UID-ABC",
				},
			},
			filter: filter.StringProperty[*opencost.Allocation]{
				Field: opencost.AllocationPodProp,
				Op:    filter.StringEquals,
				Value: "pod-123 UID-ABC",
			},

			expected: true,
		},
		{
			name: "Container Equals -> true",
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Container: "cost-model",
				},
			},
			filter: filter.StringProperty[*opencost.Allocation]{
				Field: opencost.AllocationContainerProp,
				Op:    filter.StringEquals,
				Value: "cost-model",
			},

			expected: true,
		},
		{
			name: `namespace unallocated -> true`,
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Namespace: "",
				},
			},
			filter: filter.StringProperty[*opencost.Allocation]{
				Field: opencost.AllocationNamespaceProp,
				Op:    filter.StringEquals,
				Value: opencost.UnallocatedSuffix,
			},

			expected: true,
		},
	}

	for _, c := range cases {
		result := c.filter.Matches(c.a)

		if result != c.expected {
			t.Errorf("%s: expected %t, got %t", c.name, c.expected, result)
		}
	}
}

func Test_StringSlice_Matches(t *testing.T) {
	cases := []struct {
		name   string
		a      *opencost.Allocation
		filter filter.Filter[*opencost.Allocation]

		expected bool
	}{
		{
			name: `services contains -> true`,
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Services: []string{"serv1", "serv2"},
				},
			},
			filter: filter.StringSliceProperty[*opencost.Allocation]{
				Field: opencost.AllocationServiceProp,
				Op:    filter.StringSliceContains,
				Value: "serv2",
			},

			expected: true,
		},
		{
			name: `services contains -> false`,
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Services: []string{"serv1", "serv2"},
				},
			},
			filter: filter.StringSliceProperty[*opencost.Allocation]{
				Field: opencost.AllocationServiceProp,
				Op:    filter.StringSliceContains,
				Value: "serv3",
			},

			expected: false,
		},
		{
			name: `services contains unallocated -> false`,
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Services: []string{"serv1", "serv2"},
				},
			},
			filter: filter.StringSliceProperty[*opencost.Allocation]{
				Field: opencost.AllocationServiceProp,
				Op:    filter.StringSliceContains,
				Value: opencost.UnallocatedSuffix,
			},

			expected: false,
		},
		{
			name: `services contains unallocated -> true`,
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Services: []string{},
				},
			},
			filter: filter.StringSliceProperty[*opencost.Allocation]{
				Field: opencost.AllocationServiceProp,
				Op:    filter.StringSliceContains,
				Value: opencost.UnallocatedSuffix,
			},

			expected: true,
		},
	}

	for _, c := range cases {
		result := c.filter.Matches(c.a)

		if result != c.expected {
			t.Errorf("%s: expected %t, got %t", c.name, c.expected, result)
		}
	}
}

func Test_StringMap_Matches(t *testing.T) {
	cases := []struct {
		name   string
		a      *opencost.Allocation
		filter filter.Filter[*opencost.Allocation]

		expected bool
	}{
		{
			name: `label[app]="foo" -> true`,
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Labels: map[string]string{
						"app": "foo",
					},
				},
			},
			filter: filter.StringMapProperty[*opencost.Allocation]{
				Field: opencost.AllocationLabelProp,
				Op:    filter.StringMapEquals,
				Key:   "app",
				Value: "foo",
			},

			expected: true,
		},
		{
			name: `label[app]="foo" -> different value -> false`,
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Labels: map[string]string{
						"app": "bar",
					},
				},
			},
			filter: filter.StringMapProperty[*opencost.Allocation]{
				Field: opencost.AllocationLabelProp,
				Op:    filter.StringMapEquals,
				Key:   "app",
				Value: "foo",
			},

			expected: false,
		},
		{
			name: `label[app]="foo" -> label missing -> false`,
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Labels: map[string]string{
						"someotherlabel": "someothervalue",
					},
				},
			},
			filter: filter.StringMapProperty[*opencost.Allocation]{
				Field: opencost.AllocationLabelProp,
				Op:    filter.StringMapEquals,
				Key:   "app",
				Value: "foo",
			},

			expected: false,
		},
		{
			name: `label[app]=Unallocated -> label missing -> true`,
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Labels: map[string]string{
						"someotherlabel": "someothervalue",
					},
				},
			},
			filter: filter.StringMapProperty[*opencost.Allocation]{
				Field: opencost.AllocationLabelProp,
				Op:    filter.StringMapEquals,
				Key:   "app",
				Value: opencost.UnallocatedSuffix,
			},

			expected: true,
		},
		{
			name: `label[app]=Unallocated -> label present -> false`,
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Labels: map[string]string{
						"app": "test",
					},
				},
			},
			filter: filter.StringMapProperty[*opencost.Allocation]{
				Field: opencost.AllocationLabelProp,
				Op:    filter.StringMapEquals,
				Key:   "app",
				Value: opencost.UnallocatedSuffix,
			},

			expected: false,
		},
		{
			name: `annotation[prom_modified_name]="testing123" -> true`,
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Annotations: map[string]string{
						"prom_modified_name": "testing123",
					},
				},
			},
			filter: filter.StringMapProperty[*opencost.Allocation]{
				Field: opencost.AllocationAnnotationProp,
				Op:    filter.StringMapEquals,
				Key:   "prom_modified_name",
				Value: "testing123",
			},

			expected: true,
		},
		{
			name: `annotation[app]="foo" -> different value -> false`,
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Annotations: map[string]string{
						"app": "bar",
					},
				},
			},
			filter: filter.StringMapProperty[*opencost.Allocation]{
				Field: opencost.AllocationAnnotationProp,
				Op:    filter.StringMapEquals,
				Key:   "app",
				Value: "foo",
			},

			expected: false,
		},
		{
			name: `annotation[app]="foo" -> annotation missing -> false`,
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Annotations: map[string]string{
						"someotherannotation": "someothervalue",
					},
				},
			},
			filter: filter.StringMapProperty[*opencost.Allocation]{
				Field: opencost.AllocationAnnotationProp,
				Op:    filter.StringMapEquals,
				Key:   "app",
				Value: "foo",
			},

			expected: false,
		},
	}

	for _, c := range cases {
		result := c.filter.Matches(c.a)

		if result != c.expected {
			t.Errorf("%s: expected %t, got %t", c.name, c.expected, result)
		}
	}
}

func Test_Not_Matches(t *testing.T) {
	cases := []struct {
		name   string
		a      *opencost.Allocation
		filter filter.Filter[*opencost.Allocation]

		expected bool
	}{
		{
			name: "Namespace NotEquals -> false",
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Namespace: "kube-system",
				},
			},
			filter: filter.Not[*opencost.Allocation]{
				Filter: filter.StringProperty[*opencost.Allocation]{
					Field: opencost.AllocationNamespaceProp,
					Op:    filter.StringEquals,
					Value: "kube-system",
				},
			},

			expected: false,
		},
		{
			name: "Namespace NotEquals Unallocated -> true",
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Namespace: "kube-system",
				},
			},
			filter: filter.Not[*opencost.Allocation]{
				Filter: filter.StringProperty[*opencost.Allocation]{
					Field: opencost.AllocationNamespaceProp,
					Op:    filter.StringEquals,
					Value: opencost.UnallocatedSuffix,
				},
			},
			expected: true,
		},
		{
			name: "Namespace NotEquals Unallocated -> false",
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Namespace: "",
				},
			},
			filter: filter.Not[*opencost.Allocation]{
				Filter: filter.StringProperty[*opencost.Allocation]{
					Field: opencost.AllocationNamespaceProp,
					Op:    filter.StringEquals,
					Value: opencost.UnallocatedSuffix,
				},
			},

			expected: false,
		},

		{
			name: `label[app]!=Unallocated -> label missing -> false`,
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Labels: map[string]string{
						"someotherlabel": "someothervalue",
					},
				},
			},
			filter: filter.Not[*opencost.Allocation]{
				Filter: filter.StringMapProperty[*opencost.Allocation]{
					Field: opencost.AllocationLabelProp,
					Op:    filter.StringMapEquals,
					Key:   "app",
					Value: opencost.UnallocatedSuffix,
				},
			},
			expected: false,
		},
		{
			name: `label[app]!=Unallocated -> label present -> true`,
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Labels: map[string]string{
						"app": "test",
					},
				},
			},
			filter: filter.Not[*opencost.Allocation]{
				Filter: filter.StringMapProperty[*opencost.Allocation]{
					Field: opencost.AllocationLabelProp,
					Op:    filter.StringMapEquals,
					Key:   "app",
					Value: opencost.UnallocatedSuffix,
				},
			},
			expected: true,
		},
		{
			name: `label[app]!="foo" -> label missing -> true`,
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Labels: map[string]string{
						"someotherlabel": "someothervalue",
					},
				},
			},
			filter: filter.Not[*opencost.Allocation]{
				Filter: filter.StringMapProperty[*opencost.Allocation]{
					Field: opencost.AllocationLabelProp,
					Op:    filter.StringMapEquals,
					Key:   "app",
					Value: "foo",
				},
			},

			expected: true,
		},
		{
			name: `annotation[prom_modified_name]="testing123" -> true`,
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Annotations: map[string]string{
						"prom_modified_name": "testing123",
					},
				},
			},
			filter: filter.StringMapProperty[*opencost.Allocation]{
				Field: opencost.AllocationAnnotationProp,
				Op:    filter.StringMapEquals,
				Key:   "prom_modified_name",
				Value: "testing123",
			},

			expected: true,
		},
		{
			name: `annotation[app]="foo" -> different value -> false`,
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Annotations: map[string]string{
						"app": "bar",
					},
				},
			},
			filter: filter.StringMapProperty[*opencost.Allocation]{
				Field: opencost.AllocationAnnotationProp,
				Op:    filter.StringMapEquals,
				Key:   "app",
				Value: "foo",
			},

			expected: false,
		},
		{
			name: `annotation[app]="foo" -> annotation missing -> false`,
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Annotations: map[string]string{
						"someotherannotation": "someothervalue",
					},
				},
			},
			filter: filter.StringMapProperty[*opencost.Allocation]{
				Field: opencost.AllocationAnnotationProp,
				Op:    filter.StringMapEquals,
				Key:   "app",
				Value: "foo",
			},

			expected: false,
		},
		{
			name: `annotation[app]!="foo" -> annotation missing -> true`,
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Annotations: map[string]string{
						"someotherannotation": "someothervalue",
					},
				},
			},
			filter: filter.Not[*opencost.Allocation]{
				Filter: filter.StringMapProperty[*opencost.Allocation]{
					Field: opencost.AllocationAnnotationProp,
					Op:    filter.StringMapEquals,
					Key:   "app",
					Value: "foo",
				},
			},

			expected: true,
		},
		{
			name: `namespace unallocated -> true`,
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Namespace: "",
				},
			},
			filter: filter.StringProperty[*opencost.Allocation]{
				Field: opencost.AllocationNamespaceProp,
				Op:    filter.StringEquals,
				Value: opencost.UnallocatedSuffix,
			},

			expected: true,
		},
		{
			name: `services contains -> true`,
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Services: []string{"serv1", "serv2"},
				},
			},
			filter: filter.StringSliceProperty[*opencost.Allocation]{
				Field: opencost.AllocationServiceProp,
				Op:    filter.StringSliceContains,
				Value: "serv2",
			},

			expected: true,
		},
		{
			name: `services contains -> false`,
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Services: []string{"serv1", "serv2"},
				},
			},
			filter: filter.StringSliceProperty[*opencost.Allocation]{
				Field: opencost.AllocationServiceProp,
				Op:    filter.StringSliceContains,
				Value: "serv3",
			},

			expected: false,
		},
		{
			name: `services notcontains -> true`,
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Services: []string{"serv1", "serv2"},
				},
			},
			filter: filter.Not[*opencost.Allocation]{
				Filter: filter.StringSliceProperty[*opencost.Allocation]{
					Field: opencost.AllocationServiceProp,
					Op:    filter.StringSliceContains,
					Value: "serv3",
				},
			},
			expected: true,
		},
		{
			name: `services notcontains -> false`,
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Services: []string{"serv1", "serv2"},
				},
			},
			filter: filter.Not[*opencost.Allocation]{
				Filter: filter.StringSliceProperty[*opencost.Allocation]{
					Field: opencost.AllocationServiceProp,
					Op:    filter.StringSliceContains,
					Value: "serv2",
				},
			},

			expected: false,
		},
		{
			name: `services notcontains unallocated -> true`,
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Services: []string{"serv1", "serv2"},
				},
			},
			filter: filter.Not[*opencost.Allocation]{
				Filter: filter.StringSliceProperty[*opencost.Allocation]{
					Field: opencost.AllocationServiceProp,
					Op:    filter.StringSliceContains,
					Value: opencost.UnallocatedSuffix,
				},
			},

			expected: true,
		},
		{
			name: `services notcontains unallocated -> false`,
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Services: []string{},
				},
			},
			filter: filter.Not[*opencost.Allocation]{
				Filter: filter.StringSliceProperty[*opencost.Allocation]{
					Field: opencost.AllocationServiceProp,
					Op:    filter.StringSliceContains,
					Value: opencost.UnallocatedSuffix,
				},
			},

			expected: false,
		},
		{
			name: `services containsprefix -> true`,
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Services: []string{"serv1", "serv2"},
				},
			},
			filter: filter.StringSliceProperty[*opencost.Allocation]{
				Field: opencost.AllocationServiceProp,
				Op:    filter.StringSliceContainsPrefix,
				Value: "serv",
			},

			expected: true,
		},
		{
			name: `services containsprefix -> false`,
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Services: []string{"foo", "bar"},
				},
			},
			filter: filter.StringSliceProperty[*opencost.Allocation]{
				Field: opencost.AllocationServiceProp,
				Op:    filter.StringSliceContainsPrefix,
				Value: "serv",
			},

			expected: false,
		},
		{
			name: `services contains unallocated -> false`,
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Services: []string{"serv1", "serv2"},
				},
			},
			filter: filter.StringSliceProperty[*opencost.Allocation]{
				Field: opencost.AllocationServiceProp,
				Op:    filter.StringSliceContains,
				Value: opencost.UnallocatedSuffix,
			},

			expected: false,
		},
		{
			name: `services contains unallocated -> true`,
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Services: []string{},
				},
			},
			filter: filter.StringSliceProperty[*opencost.Allocation]{
				Field: opencost.AllocationServiceProp,
				Op:    filter.StringSliceContains,
				Value: opencost.UnallocatedSuffix,
			},

			expected: true,
		},
	}

	for _, c := range cases {
		result := c.filter.Matches(c.a)

		if result != c.expected {
			t.Errorf("%s: expected %t, got %t", c.name, c.expected, result)
		}
	}
}

func Test_None_Matches(t *testing.T) {
	cases := []struct {
		name string
		a    *opencost.Allocation
	}{
		{
			name: "nil",
			a:    nil,
		},
		{
			name: "nil properties",
			a: &opencost.Allocation{
				Properties: nil,
			},
		},
		{
			name: "empty properties",
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{},
			},
		},
		{
			name: "ClusterID",
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Cluster: "cluster-one",
				},
			},
		},
		{
			name: "Node",
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Node: "node123",
				},
			},
		},
		{
			name: "Namespace",
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Namespace: "kube-system",
				},
			},
		},
		{
			name: "ControllerKind",
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					ControllerKind: "deployment", // We generally store controller kinds as all lowercase
				},
			},
		},
		{
			name: "ControllerName",
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Controller: "kc-cost-analyzer",
				},
			},
		},
		{
			name: "Pod",
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Pod: "pod-123 UID-ABC",
				},
			},
		},
		{
			name: "Container",
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Container: "cost-model",
				},
			},
		},
		{
			name: `label`,
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Labels: map[string]string{
						"app": "foo",
					},
				},
			},
		},
		{
			name: `annotation`,
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Annotations: map[string]string{
						"prom_modified_name": "testing123",
					},
				},
			},
		},
		{
			name: `services`,
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Services: []string{"serv1", "serv2"},
				},
			},
		},
	}

	for _, c := range cases {
		result := filter.AllCut[*opencost.Allocation]{}.Matches(c.a)

		if result {
			t.Errorf("%s: should have been rejected", c.name)
		}
	}
}

func Test_And_Matches(t *testing.T) {
	cases := []struct {
		name   string
		a      *opencost.Allocation
		filter filter.Filter[*opencost.Allocation]

		expected bool
	}{
		{
			name: `label[app]="foo" and namespace="kubecost" -> both true`,
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Namespace: "kubecost",
					Labels: map[string]string{
						"app": "foo",
					},
				},
			},
			filter: filter.And[*opencost.Allocation]{[]filter.Filter[*opencost.Allocation]{
				filter.StringMapProperty[*opencost.Allocation]{
					Field: opencost.AllocationLabelProp,
					Op:    filter.StringMapEquals,
					Key:   "app",
					Value: "foo",
				},
				filter.StringProperty[*opencost.Allocation]{
					Field: opencost.AllocationNamespaceProp,
					Op:    filter.StringEquals,
					Value: "kubecost",
				},
			}},
			expected: true,
		},
		{
			name: `label[app]="foo" and namespace="kubecost" -> first true`,
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Namespace: "kube-system",
					Labels: map[string]string{
						"app": "foo",
					},
				},
			},
			filter: filter.And[*opencost.Allocation]{[]filter.Filter[*opencost.Allocation]{
				filter.StringMapProperty[*opencost.Allocation]{
					Field: opencost.AllocationLabelProp,
					Op:    filter.StringMapEquals,
					Key:   "app",
					Value: "foo",
				},
				filter.StringProperty[*opencost.Allocation]{
					Field: opencost.AllocationNamespaceProp,
					Op:    filter.StringEquals,
					Value: "kubecost",
				},
			}},
			expected: false,
		},
		{
			name: `label[app]="foo" and namespace="kubecost" -> second true`,
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Namespace: "kubecost",
					Labels: map[string]string{
						"app": "bar",
					},
				},
			},
			filter: filter.And[*opencost.Allocation]{[]filter.Filter[*opencost.Allocation]{
				filter.StringMapProperty[*opencost.Allocation]{
					Field: opencost.AllocationLabelProp,
					Op:    filter.StringMapEquals,
					Key:   "app",
					Value: "foo",
				},
				filter.StringProperty[*opencost.Allocation]{
					Field: opencost.AllocationNamespaceProp,
					Op:    filter.StringEquals,
					Value: "kubecost",
				},
			}},
			expected: false,
		},
		{
			name: `label[app]="foo" and namespace="kubecost" -> both false`,
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Namespace: "kube-system",
					Labels: map[string]string{
						"app": "bar",
					},
				},
			},
			filter: filter.And[*opencost.Allocation]{[]filter.Filter[*opencost.Allocation]{
				filter.StringMapProperty[*opencost.Allocation]{
					Field: opencost.AllocationLabelProp,
					Op:    filter.StringMapEquals,
					Key:   "app",
					Value: "foo",
				},
				filter.StringProperty[*opencost.Allocation]{
					Field: opencost.AllocationNamespaceProp,
					Op:    filter.StringEquals,
					Value: "kubecost",
				},
			}},
			expected: false,
		},
		{
			name: `(and none) matches nothing`,
			a: &opencost.Allocation{
				Properties: &opencost.AllocationProperties{
					Namespace: "kube-system",
					Labels: map[string]string{
						"app": "bar",
					},
				},
			},
			filter: filter.And[*opencost.Allocation]{[]filter.Filter[*opencost.Allocation]{
				filter.AllCut[*opencost.Allocation]{},
			}},
			expected: false,
		},
	}

	for _, c := range cases {
		result := c.filter.Matches(c.a)

		if result != c.expected {
			t.Errorf("%s: expected %t, got %t", c.name, c.expected, result)
		}
	}
}
