package opencost

import (
	"testing"

	filter21 "github.com/opencost/opencost/core/pkg/filter"
	afilter "github.com/opencost/opencost/core/pkg/filter/allocation"
	"github.com/opencost/opencost/core/pkg/filter/ast"
	"github.com/opencost/opencost/core/pkg/filter/ops"
)

func Test_AllocationFilterCondition_Matches(t *testing.T) {
	labelConfig := &LabelConfig{
		DepartmentLabel:  "keydepartment",
		EnvironmentLabel: "keyenvironment",
		OwnerLabel:       "keyowner",
		ProductLabel:     "keyproduct",
		TeamLabel:        "keyteam",
	}

	cases := []struct {
		name   string
		a      *Allocation
		filter filter21.Filter

		expected bool
	}{
		{
			name: "ClusterID Equals -> true",
			a: &Allocation{
				Properties: &AllocationProperties{
					Cluster: "cluster-one",
				},
			},
			filter:   ops.Eq(afilter.FieldClusterID, "cluster-one"),
			expected: true,
		},
		{
			name: "ClusterID StartsWith -> true",
			a: &Allocation{
				Properties: &AllocationProperties{
					Cluster: "cluster-one",
				},
			},
			filter:   ops.ContainsPrefix(afilter.FieldClusterID, "cluster"),
			expected: true,
		},
		{
			name: "ClusterID StartsWith -> false",
			a: &Allocation{
				Properties: &AllocationProperties{
					Cluster: "k8s-one",
				},
			},
			filter: ops.ContainsPrefix(afilter.FieldClusterID, "cluster"),

			expected: false,
		},
		{
			name: "ClusterID empty StartsWith '' -> true",
			a: &Allocation{
				Properties: &AllocationProperties{
					Cluster: "",
				},
			},
			filter:   ops.ContainsPrefix(afilter.FieldClusterID, ""),
			expected: true,
		},
		{
			name: "ClusterID nonempty StartsWith '' -> true",
			a: &Allocation{
				Properties: &AllocationProperties{
					Cluster: "abc",
				},
			},
			filter:   ops.ContainsPrefix(afilter.FieldClusterID, ""),
			expected: true,
		},
		{
			name: "Node Equals -> true",
			a: &Allocation{
				Properties: &AllocationProperties{
					Node: "node123",
				},
			},
			filter:   ops.Eq(afilter.FieldNode, "node123"),
			expected: true,
		},
		{
			name: "Namespace NotEquals -> false",
			a: &Allocation{
				Properties: &AllocationProperties{
					Namespace: "kube-system",
				},
			},
			filter:   ops.NotEq(afilter.FieldNamespace, "kube-system"),
			expected: false,
		},
		{
			name: "Namespace NotEquals Unallocated -> true",
			a: &Allocation{
				Properties: &AllocationProperties{
					Namespace: "kube-system",
				},
			},
			filter:   ops.NotEq(afilter.FieldNamespace, UnallocatedSuffix),
			expected: true,
		},
		{
			name: "Namespace NotEquals Unallocated -> false",
			a: &Allocation{
				Properties: &AllocationProperties{
					Namespace: "",
				},
			},
			filter:   ops.NotEq(afilter.FieldNamespace, UnallocatedSuffix),
			expected: false,
		},
		{
			name: "Namespace Equals Unallocated -> true",
			a: &Allocation{
				Properties: &AllocationProperties{
					Namespace: "",
				},
			},
			filter:   ops.Eq(afilter.FieldNamespace, UnallocatedSuffix),
			expected: true,
		},
		{
			name: "ControllerKind Equals -> true",
			a: &Allocation{
				Properties: &AllocationProperties{
					ControllerKind: "deployment", // We generally store controller kinds as all lowercase
				},
			},
			filter:   ops.Eq(afilter.FieldControllerKind, "deployment"),
			expected: true,
		},
		{
			name: "ControllerName Equals -> true",
			a: &Allocation{
				Properties: &AllocationProperties{
					Controller: "kc-cost-analyzer",
				},
			},
			filter:   ops.Eq(afilter.FieldControllerName, "kc-cost-analyzer"),
			expected: true,
		},
		{
			name: "Pod (with UID) Equals -> true",
			a: &Allocation{
				Properties: &AllocationProperties{
					Pod: "pod-123 UID-ABC",
				},
			},
			filter:   ops.Eq(afilter.FieldPod, "pod-123 UID-ABC"),
			expected: true,
		},
		{
			name: "Container Equals -> true",
			a: &Allocation{
				Properties: &AllocationProperties{
					Container: "cost-model",
				},
			},
			filter:   ops.Eq(afilter.FieldContainer, "cost-model"),
			expected: true,
		},
		{
			name: `label[app]="foo" -> true`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Labels: map[string]string{
						"app": "foo",
					},
				},
			},
			filter:   ops.Eq(ops.WithKey(afilter.FieldLabel, "app"), "foo"),
			expected: true,
		},
		{
			name: `label[app]="foo" -> different value -> false`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Labels: map[string]string{
						"app": "bar",
					},
				},
			},
			filter:   ops.Eq(ops.WithKey(afilter.FieldLabel, "app"), "foo"),
			expected: false,
		},
		{
			name: `label[app]="foo" -> label missing -> false`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Labels: map[string]string{
						"someotherlabel": "someothervalue",
					},
				},
			},
			filter:   ops.Eq(ops.WithKey(afilter.FieldLabel, "app"), "foo"),
			expected: false,
		},
		{
			name: `label[app]=Unallocated -> label missing -> true`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Labels: map[string]string{
						"someotherlabel": "someothervalue",
					},
				},
			},
			filter:   ops.Eq(ops.WithKey(afilter.FieldLabel, "app"), UnallocatedSuffix),
			expected: true,
		},
		{
			name: `label[app]=Unallocated -> label present -> false`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Labels: map[string]string{
						"app": "test",
					},
				},
			},
			filter:   ops.Eq(ops.WithKey(afilter.FieldLabel, "app"), UnallocatedSuffix),
			expected: false,
		},
		{
			name: `label[app]!=Unallocated -> label missing -> false`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Labels: map[string]string{
						"someotherlabel": "someothervalue",
					},
				},
			},
			filter:   ops.NotEq(ops.WithKey(afilter.FieldLabel, "app"), UnallocatedSuffix),
			expected: false,
		},
		{
			name: `label[app]!=Unallocated -> label present -> true`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Labels: map[string]string{
						"app": "test",
					},
				},
			},
			filter:   ops.NotEq(ops.WithKey(afilter.FieldLabel, "app"), UnallocatedSuffix),
			expected: true,
		},
		{
			name: `label[app]!="foo" -> label missing -> true`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Labels: map[string]string{
						"someotherlabel": "someothervalue",
					},
				},
			},
			filter:   ops.NotEq(ops.WithKey(afilter.FieldLabel, "app"), "foo"),
			expected: true,
		},
		{
			name: `annotation[prom_modified_name]="testing123" -> true`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Annotations: map[string]string{
						"prom_modified_name": "testing123",
					},
				},
			},
			filter:   ops.Eq(ops.WithKey(afilter.FieldAnnotation, "prom_modified_name"), "testing123"),
			expected: true,
		},
		{
			name: `annotation[app]="foo" -> different value -> false`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Annotations: map[string]string{
						"app": "bar",
					},
				},
			},
			filter:   ops.Eq(ops.WithKey(afilter.FieldAnnotation, "app"), "foo"),
			expected: false,
		},
		{
			name: `annotation[app]="foo" -> annotation missing -> false`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Annotations: map[string]string{
						"someotherannotation": "someothervalue",
					},
				},
			},
			filter:   ops.Eq(ops.WithKey(afilter.FieldAnnotation, "app"), "foo"),
			expected: false,
		},
		{
			name: `annotation[app]!="foo" -> annotation missing -> true`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Annotations: map[string]string{
						"someotherannotation": "someothervalue",
					},
				},
			},
			filter:   ops.NotEq(ops.WithKey(afilter.FieldAnnotation, "app"), "foo"),
			expected: true,
		},
		{
			name: `namespace unallocated -> true`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Namespace: "",
				},
			},
			filter:   ops.Eq(afilter.FieldNamespace, UnallocatedSuffix),
			expected: true,
		},
		{
			name: `services contains -> true`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Services: []string{"serv1", "serv2"},
				},
			},
			filter:   ops.Contains(afilter.FieldServices, "serv2"),
			expected: true,
		},
		{
			name: `services contains -> false`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Services: []string{"serv1", "serv2"},
				},
			},
			filter:   ops.Contains(afilter.FieldServices, "serv3"),
			expected: false,
		},
		{
			name: `services notcontains -> true`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Services: []string{"serv1", "serv2"},
				},
			},
			filter:   ops.NotContains(afilter.FieldServices, "serv3"),
			expected: true,
		},
		{
			name: `services notcontains -> false`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Services: []string{"serv1", "serv2"},
				},
			},
			filter:   ops.NotContains(afilter.FieldServices, "serv2"),
			expected: false,
		},
		{
			name: `services notcontains unallocated -> true`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Services: []string{"serv1", "serv2"},
				},
			},
			filter:   ops.NotContains(afilter.FieldServices, UnallocatedSuffix),
			expected: true,
		},
		{
			name: `services notcontains unallocated -> false`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Services: []string{},
				},
			},
			filter:   ops.NotContains(afilter.FieldServices, UnallocatedSuffix),
			expected: false,
		},
		{
			name: `services containsprefix -> true`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Services: []string{"serv1", "serv2"},
				},
			},
			filter:   ops.ContainsPrefix(afilter.FieldServices, "serv"),
			expected: true,
		},
		{
			name: `services containsprefix -> false`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Services: []string{"foo", "bar"},
				},
			},
			filter:   ops.ContainsPrefix(afilter.FieldServices, "serv"),
			expected: false,
		},
		{
			name: `services contains unallocated -> false`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Services: []string{"serv1", "serv2"},
				},
			},
			filter:   ops.Contains(afilter.FieldServices, UnallocatedSuffix),
			expected: false,
		},
		{
			name: `services contains unallocated -> true`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Services: []string{},
				},
			},
			filter:   ops.Contains(afilter.FieldServices, UnallocatedSuffix),
			expected: true,
		},
		{
			name: `department equals -> true`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Labels: AllocationLabels{
						"keydepartment": "foo",
					},
				},
			},
			// The ops package doesn't handle alias construction quite right,
			// so we construct it more manually here
			filter: &ast.EqualOp{
				Left: ast.Identifier{
					Field: ast.NewAliasField(afilter.AliasDepartment),
				},
				Right: "foo",
			},
			expected: true,
		},
		{
			name: `department != unallocated -> true`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Annotations: AllocationAnnotations{
						"keydepartment": "foo",
					},
				},
			},
			// The ops package doesn't handle alias construction quite right,
			// so we construct it more manually here
			filter: &ast.NotOp{
				Operand: &ast.EqualOp{
					Left: ast.Identifier{
						Field: ast.NewAliasField(afilter.AliasDepartment),
					},
					Right: UnallocatedSuffix,
				},
			},
			expected: true,
		},
		{
			name: `product == unallocated -> true`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Annotations: AllocationAnnotations{
						"keydepartment": "foo",
					},
				},
			},
			filter: &ast.EqualOp{
				Left: ast.Identifier{
					Field: ast.NewAliasField(afilter.AliasProduct),
				},
				Right: UnallocatedSuffix,
			},
			expected: true,
		},
		{
			name: `product == "" -> true`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Labels: AllocationLabels{
						"keydepartment": "foo",
					},
					Annotations: AllocationAnnotations{
						"keyowner": "bar",
					},
				},
			},
			filter: &ast.EqualOp{
				Left: ast.Identifier{
					Field: ast.NewAliasField(afilter.AliasProduct),
				},
				Right: "",
			},
			expected: true,
		},
	}

	for _, c := range cases {
		compiler := NewAllocationMatchCompiler(labelConfig)
		compiled, err := compiler.Compile(c.filter)
		if err != nil {
			t.Fatalf("err compiling filter '%s': %s", ast.ToPreOrderShortString(c.filter), err)
		}

		result := compiled.Matches(c.a)
		if result != c.expected {
			t.Errorf("%s: expected %t, got %t", c.name, c.expected, result)
		}
	}
}

func Test_AllocationFilterContradiction_Matches(t *testing.T) {
	cases := []struct {
		name string
		a    *Allocation
	}{
		{
			name: "nil",
			a:    nil,
		},
		{
			name: "nil properties",
			a: &Allocation{
				Properties: nil,
			},
		},
		{
			name: "empty properties",
			a: &Allocation{
				Properties: &AllocationProperties{},
			},
		},
		{
			name: "ClusterID",
			a: &Allocation{
				Properties: &AllocationProperties{
					Cluster: "cluster-one",
				},
			},
		},
		{
			name: "Node",
			a: &Allocation{
				Properties: &AllocationProperties{
					Node: "node123",
				},
			},
		},
		{
			name: "Namespace",
			a: &Allocation{
				Properties: &AllocationProperties{
					Namespace: "kube-system",
				},
			},
		},
		{
			name: "ControllerKind",
			a: &Allocation{
				Properties: &AllocationProperties{
					ControllerKind: "deployment", // We generally store controller kinds as all lowercase
				},
			},
		},
		{
			name: "ControllerName",
			a: &Allocation{
				Properties: &AllocationProperties{
					Controller: "kc-cost-analyzer",
				},
			},
		},
		{
			name: "Pod",
			a: &Allocation{
				Properties: &AllocationProperties{
					Pod: "pod-123 UID-ABC",
				},
			},
		},
		{
			name: "Container",
			a: &Allocation{
				Properties: &AllocationProperties{
					Container: "cost-model",
				},
			},
		},
		{
			name: `label`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Labels: map[string]string{
						"app": "foo",
					},
				},
			},
		},
		{
			name: `annotation`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Annotations: map[string]string{
						"prom_modified_name": "testing123",
					},
				},
			},
		},
		{
			name: `services`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Services: []string{"serv1", "serv2"},
				},
			},
		},
	}

	for _, c := range cases {
		filter := &ast.ContradictionOp{}
		compiler := NewAllocationMatchCompiler(nil)
		compiled, err := compiler.Compile(filter)
		if err != nil {
			t.Fatalf("err compiling filter '%s': %s", ast.ToPreOrderShortString(filter), err)
		}

		result := compiled.Matches(c.a)
		if result {
			t.Errorf("%s: should have been rejected", c.name)
		}
	}
}

func Test_AllocationFilterAnd_Matches(t *testing.T) {
	cases := []struct {
		name   string
		a      *Allocation
		filter filter21.Filter

		expected bool
	}{
		{
			name: `label[app]="foo" and namespace="kubecost" -> both true`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Namespace: "kubecost",
					Labels: map[string]string{
						"app": "foo",
					},
				},
			},
			filter: ops.And(
				ops.Eq(ops.WithKey(afilter.FieldLabel, "app"), "foo"),
				ops.Eq(afilter.FieldNamespace, "kubecost"),
			),
			expected: true,
		},
		{
			name: `label[app]="foo" and namespace="kubecost" -> first true`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Namespace: "kube-system",
					Labels: map[string]string{
						"app": "foo",
					},
				},
			},
			filter: ops.And(
				ops.Eq(ops.WithKey(afilter.FieldLabel, "app"), "foo"),
				ops.Eq(afilter.FieldNamespace, "kubecost"),
			),
			expected: false,
		},
		{
			name: `label[app]="foo" and namespace="kubecost" -> second true`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Namespace: "kubecost",
					Labels: map[string]string{
						"app": "bar",
					},
				},
			},
			filter: ops.And(
				ops.Eq(ops.WithKey(afilter.FieldLabel, "app"), "foo"),
				ops.Eq(afilter.FieldNamespace, "kubecost"),
			),
			expected: false,
		},
		{
			name: `label[app]="foo" and namespace="kubecost" -> both false`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Namespace: "kube-system",
					Labels: map[string]string{
						"app": "bar",
					},
				},
			},
			filter: ops.And(
				ops.Eq(ops.WithKey(afilter.FieldLabel, "app"), "foo"),
				ops.Eq(afilter.FieldNamespace, "kubecost"),
			),
			expected: false,
		},
		{
			name: `contradiction matches nothing`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Namespace: "kube-system",
					Labels: map[string]string{
						"app": "bar",
					},
				},
			},
			filter:   &ast.ContradictionOp{},
			expected: false,
		},
	}

	for _, c := range cases {
		compiler := NewAllocationMatchCompiler(nil)
		compiled, err := compiler.Compile(c.filter)
		if err != nil {
			t.Fatalf("err compiling filter '%s': %s", ast.ToPreOrderShortString(c.filter), err)
		}

		result := compiled.Matches(c.a)
		if result != c.expected {
			t.Errorf("%s: expected %t, got %t", c.name, c.expected, result)
		}
	}
}

func Test_AllocationFilterOr_Matches(t *testing.T) {
	cases := []struct {
		name   string
		a      *Allocation
		filter filter21.Filter

		expected bool
	}{
		{
			name: `label[app]="foo" or namespace="kubecost" -> both true`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Namespace: "kubecost",
					Labels: map[string]string{
						"app": "foo",
					},
				},
			},
			filter: ops.Or(
				ops.Eq(ops.WithKey(afilter.FieldLabel, "app"), "foo"),
				ops.Eq(afilter.FieldNamespace, "kubecost"),
			),
			expected: true,
		},
		{
			name: `label[app]="foo" or namespace="kubecost" -> first true`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Namespace: "kube-system",
					Labels: map[string]string{
						"app": "foo",
					},
				},
			},
			filter: ops.Or(
				ops.Eq(ops.WithKey(afilter.FieldLabel, "app"), "foo"),
				ops.Eq(afilter.FieldNamespace, "kubecost"),
			),
			expected: true,
		},
		{
			name: `label[app]="foo" or namespace="kubecost" -> second true`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Namespace: "kubecost",
					Labels: map[string]string{
						"app": "bar",
					},
				},
			},
			filter: ops.Or(
				ops.Eq(ops.WithKey(afilter.FieldLabel, "app"), "foo"),
				ops.Eq(afilter.FieldNamespace, "kubecost"),
			),
			expected: true,
		},
		{
			name: `label[app]="foo" or namespace="kubecost" -> both false`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Namespace: "kube-system",
					Labels: map[string]string{
						"app": "bar",
					},
				},
			},
			filter: ops.Or(
				ops.Eq(ops.WithKey(afilter.FieldLabel, "app"), "foo"),
				ops.Eq(afilter.FieldNamespace, "kubecost"),
			),
			expected: false,
		},
	}

	for _, c := range cases {
		compiler := NewAllocationMatchCompiler(nil)
		compiled, err := compiler.Compile(c.filter)
		if err != nil {
			t.Fatalf("err compiling filter '%s': %s", ast.ToPreOrderShortString(c.filter), err)
		}

		result := compiled.Matches(c.a)
		if result != c.expected {
			t.Errorf("%s: expected %t, got %t", c.name, c.expected, result)
		}
	}
}
