package matcher_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/opencost/opencost/core/pkg/filter/allocation"
	"github.com/opencost/opencost/core/pkg/filter/ast"
	"github.com/opencost/opencost/core/pkg/filter/matcher"
	"github.com/opencost/opencost/core/pkg/filter/transform"
)

// MatcherCompiler for Allocation instances providing functions which map identifers
// to values within the allocation
var allocCompiler = matcher.NewMatchCompiler(
	AllocFieldMap,
	AllocSliceFieldMap,
	AllocMapFieldMap,
	transform.PrometheusKeySanitizePass(),
	transform.UnallocatedReplacementPass(),
)

// AST parser for allocation syntax
var allocParser ast.FilterParser = allocation.NewAllocationFilterParser()

func newAlloc(props *AllocationProperties) *Allocation {
	a := &Allocation{
		Properties: props,
	}

	a.Name = a.Properties.String()
	return a
}

func TestCompileAndMatch(t *testing.T) {
	cases := []struct {
		input          string
		shouldMatch    []*Allocation
		shouldNotMatch []*Allocation
	}{
		{
			input: `namespace:"kubecost"`,
			shouldMatch: []*Allocation{
				newAlloc(&AllocationProperties{Namespace: "kubecost"}),
			},
			shouldNotMatch: []*Allocation{
				newAlloc(&AllocationProperties{Namespace: "kube-system"}),
			},
		},
		{
			input: `cluster:"cluster-one"+namespace:"kubecost"+controllerKind:"daemonset"+controllerName:"kubecost-network-costs"+container:"kubecost-network-costs"`,
			shouldMatch: []*Allocation{
				newAlloc(&AllocationProperties{
					Cluster:        "cluster-one",
					Namespace:      "kubecost",
					ControllerKind: "daemonset",
					Controller:     "kubecost-network-costs",
					Pod:            "kubecost-network-costs-abc123",
					Container:      "kubecost-network-costs",
				}),
			},
			shouldNotMatch: []*Allocation{
				newAlloc(&AllocationProperties{
					Cluster:        "cluster-one",
					Namespace:      "default",
					ControllerKind: "deployment",
					Controller:     "workload-abc",
					Pod:            "workload-abc-123abc",
					Container:      "abc",
				}),
			},
		},
		{
			input: `namespace!:"kubecost","kube-system"`,
			shouldMatch: []*Allocation{
				newAlloc(&AllocationProperties{Namespace: "abc"}),
			},
			shouldNotMatch: []*Allocation{
				newAlloc(&AllocationProperties{Namespace: "kubecost"}),
				newAlloc(&AllocationProperties{Namespace: "kube-system"}),
			},
		},
		{
			input: `namespace:"kubecost","kube-system"`,
			shouldMatch: []*Allocation{
				newAlloc(&AllocationProperties{Namespace: "kubecost"}),
				newAlloc(&AllocationProperties{Namespace: "kube-system"}),
			},
			shouldNotMatch: []*Allocation{
				newAlloc(&AllocationProperties{Namespace: "abc"}),
			},
		},
		{
			input: `node:"node a b c" , "node 12 3"` + string('\n') + "+" + string('\n') + string('\r') + `namespace : "kubecost"`,
			shouldMatch: []*Allocation{
				newAlloc(&AllocationProperties{Namespace: "kubecost", Node: "node a b c"}),
				newAlloc(&AllocationProperties{Namespace: "kubecost", Node: "node 12 3"}),
			},
			shouldNotMatch: []*Allocation{
				newAlloc(&AllocationProperties{Namespace: "kubecost"}),
				newAlloc(&AllocationProperties{Namespace: "kubecost", Node: "nodeabc"}),
			},
		},
		{
			input: `label[app_abc]:"cost_analyzer"`,
			shouldMatch: []*Allocation{
				newAlloc(&AllocationProperties{
					Namespace: "kubecost",
					Labels: map[string]string{
						"test":    "test123",
						"app_abc": "cost_analyzer",
					},
				}),
			},
			shouldNotMatch: []*Allocation{
				newAlloc(&AllocationProperties{
					Namespace: "kubecost",
					Labels: map[string]string{
						"foo": "bar",
					},
				}),
			},
		},
		{
			input: `services~:"123","abc"`,
			shouldMatch: []*Allocation{
				newAlloc(&AllocationProperties{
					Namespace: "kubecost",
					Services: []string{
						"foo",
						"bar",
						"123",
					},
				}),
				newAlloc(&AllocationProperties{
					Namespace: "kubecost",
					Services: []string{
						"foo",
						"abc",
						"test",
					},
				}),
				newAlloc(&AllocationProperties{
					Namespace: "kubecost",
					Services: []string{
						"123",
						"abc",
						"test",
					},
				}),
			},
			shouldNotMatch: []*Allocation{
				newAlloc(&AllocationProperties{
					Namespace: "kubecost",
					Services: []string{
						"foo",
						"bar",
					},
				}),
			},
		},
		{
			input: `services!:"123","abc"`,
		},
		{
			input: `label[app-abc]:"cost_analyzer"`,
			shouldMatch: []*Allocation{
				newAlloc(&AllocationProperties{
					Labels: map[string]string{
						"app_abc": "cost_analyzer",
					},
				}),
			},
			shouldNotMatch: []*Allocation{
				newAlloc(&AllocationProperties{
					Labels: map[string]string{
						"app-abc": "cost_analyzer",
					},
				}),
			},
		},
		{
			input: `label[app_abc]:"cost_analyzer"+label[foo]:"bar"`,
		},
		{
			input: `
namespace:"kubecost" +
label[app]:"cost_analyzer" +
annotation[a1]:"b2" +
cluster:"cluster-one" +
node!:
  "node-123",
  "node-456" +
controllerName:
  "kubecost-cost-analyzer",
  "kubecost-prometheus-server" +
controllerKind!:
  "daemonset",
  "statefulset",
  "job" +
container!:"123-abc_foo" +
pod!:"aaaaaaaaaaaaaaaaaaaaaaaaa" +
services!:"abc123"
`,
		},
		{
			input: `namespace:"__unallocated__"`,
			shouldMatch: []*Allocation{
				newAlloc(&AllocationProperties{Namespace: ""}),
			},
			shouldNotMatch: []*Allocation{
				newAlloc(&AllocationProperties{Namespace: "kube-system"}),
			},
		},
		{
			input: `namespace!:"__unallocated__"`,
			shouldMatch: []*Allocation{
				newAlloc(&AllocationProperties{Namespace: "kubecost"}),
			},
			shouldNotMatch: []*Allocation{
				newAlloc(&AllocationProperties{Namespace: ""}),
			},
		},
		{
			input: `controllerKind:"__unallocated__"`,
			shouldMatch: []*Allocation{
				newAlloc(&AllocationProperties{ControllerKind: ""}),
			},
			shouldNotMatch: []*Allocation{
				newAlloc(&AllocationProperties{ControllerKind: "deployment"}),
			},
		},
		{
			input: `controllerKind!:"__unallocated__"`,
			shouldMatch: []*Allocation{
				newAlloc(&AllocationProperties{ControllerKind: "deployment"}),
			},
			shouldNotMatch: []*Allocation{
				newAlloc(&AllocationProperties{ControllerKind: ""}),
			},
		},
		{
			input: `label[app]:"__unallocated__"`,
			shouldMatch: []*Allocation{
				newAlloc(&AllocationProperties{Labels: map[string]string{"foo": "bar"}}),
			},
			shouldNotMatch: []*Allocation{
				newAlloc(&AllocationProperties{Labels: map[string]string{"app": "test"}}),
			},
		},
		{
			input: `label[app]!:"__unallocated__"`,
			shouldMatch: []*Allocation{
				newAlloc(&AllocationProperties{Labels: map[string]string{"app": "test"}}),
			},
			shouldNotMatch: []*Allocation{
				newAlloc(&AllocationProperties{Labels: map[string]string{"foo": "bar"}}),
			},
		},
		{
			input: `services:"__unallocated__"`,
			shouldMatch: []*Allocation{
				newAlloc(&AllocationProperties{Services: []string{}}),
			},
			shouldNotMatch: []*Allocation{
				newAlloc(&AllocationProperties{Services: []string{"svc1", "svc2"}}),
			},
		},
		{
			input: `services!:"__unallocated__"`,
			shouldMatch: []*Allocation{
				newAlloc(&AllocationProperties{Services: []string{"svc1", "svc2"}}),
			},
			shouldNotMatch: []*Allocation{
				newAlloc(&AllocationProperties{Services: []string{}}),
			},
		},
		{
			input: `label[cloud.google.com/gke-nodepool]:"gke-nodepool-1"`,
			shouldMatch: []*Allocation{
				newAlloc(&AllocationProperties{
					Labels: map[string]string{
						"cloud_google_com_gke_nodepool": "gke-nodepool-1",
					},
				}),
			},
			shouldNotMatch: []*Allocation{
				newAlloc(&AllocationProperties{
					Labels: map[string]string{
						"cloud.google.com/gke-nodepool": "gke-nodepool-1",
					},
				}),
			},
		},
		{
			input: `label:"cloud.google.com/gke-nodepool"`,
			shouldMatch: []*Allocation{
				newAlloc(&AllocationProperties{
					Labels: map[string]string{
						"cloud_google_com_gke_nodepool": "gke-nodepool-1",
					},
				}),
			},
			shouldNotMatch: []*Allocation{
				newAlloc(&AllocationProperties{
					Labels: map[string]string{
						"cloud.google.com/gke-nodepool": "gke-nodepool-1",
					},
				}),
			},
		},
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			t.Logf("Query: %s", c.input)
			tree, err := allocParser.Parse(c.input)
			if err != nil {
				t.Fatalf("Unexpected parse error: %s", err)
			}
			t.Logf("%s", ast.ToPreOrderString(tree))

			matcher, err := allocCompiler.Compile(tree)
			t.Logf("Result: %s", matcher)
			if err != nil {
				t.Fatalf("Unexpected parse error: %s", err)
			}
			for _, shouldMatch := range c.shouldMatch {
				if !matcher.Matches(shouldMatch) {
					t.Errorf("Failed to match %s", shouldMatch.Name)
				}
			}
			for _, shouldNotMatch := range c.shouldNotMatch {
				if matcher.Matches(shouldNotMatch) {
					t.Errorf("Incorrectly matched %s", shouldNotMatch.Name)
				}
			}
		})
	}
}

// Allocation Mock

// Maps fields from an allocation to a string value based on an identifier
func AllocFieldMap(a *Allocation, identifier ast.Identifier) (string, error) {
	switch identifier.Field.Name {
	case "namespace":
		return a.Properties.Namespace, nil
	case "node":
		return a.Properties.Node, nil
	case "cluster":
		return a.Properties.Cluster, nil
	case "controllerName":
		return a.Properties.Controller, nil
	case "controllerKind":
		return a.Properties.ControllerKind, nil
	case "pod":
		return a.Properties.Pod, nil
	case "container":
		return a.Properties.Container, nil
	case "label":
		return a.Properties.Labels[identifier.Key], nil
	case "annotation":
		return a.Properties.Annotations[identifier.Key], nil
	}

	return "", fmt.Errorf("Failed to find string identifier on Allocation: %s", identifier.Field.Name)
}

// Maps slice fields from an allocation to a []string value based on an identifier
func AllocSliceFieldMap(a *Allocation, identifier ast.Identifier) ([]string, error) {
	switch identifier.Field.Name {
	case "services":
		return a.Properties.Services, nil
	}

	return nil, fmt.Errorf("Failed to find []string identifier on Allocation: %s", identifier.Field.Name)
}

// Maps map fields from an allocation to a map[string]string value based on an identifier
func AllocMapFieldMap(a *Allocation, identifier ast.Identifier) (map[string]string, error) {
	switch identifier.Field.Name {
	case "label":
		return a.Properties.Labels, nil
	case "annotation":
		return a.Properties.Annotations, nil
	}
	return nil, fmt.Errorf("Failed to find map[string]string identifier on Allocation: %s", identifier.Field.Name)
}

type AllocationProperties struct {
	Cluster        string            `json:"cluster,omitempty"`
	Node           string            `json:"node,omitempty"`
	Container      string            `json:"container,omitempty"`
	Controller     string            `json:"controller,omitempty"`
	ControllerKind string            `json:"controllerKind,omitempty"`
	Namespace      string            `json:"namespace,omitempty"`
	Pod            string            `json:"pod,omitempty"`
	Services       []string          `json:"services,omitempty"`
	ProviderID     string            `json:"providerID,omitempty"`
	Labels         map[string]string `json:"labels,omitempty"`
	Annotations    map[string]string `json:"annotations,omitempty"`
}

func (p *AllocationProperties) String() string {
	if p == nil {
		return "<nil>"
	}

	strs := []string{}

	if p.Cluster != "" {
		strs = append(strs, "Cluster:"+p.Cluster)
	}

	if p.Node != "" {
		strs = append(strs, "Node:"+p.Node)
	}

	if p.Container != "" {
		strs = append(strs, "Container:"+p.Container)
	}

	if p.Controller != "" {
		strs = append(strs, "Controller:"+p.Controller)
	}

	if p.ControllerKind != "" {
		strs = append(strs, "ControllerKind:"+p.ControllerKind)
	}

	if p.Namespace != "" {
		strs = append(strs, "Namespace:"+p.Namespace)
	}

	if p.Pod != "" {
		strs = append(strs, "Pod:"+p.Pod)
	}

	if p.ProviderID != "" {
		strs = append(strs, "ProviderID:"+p.ProviderID)
	}

	if len(p.Services) > 0 {
		strs = append(strs, "Services:"+strings.Join(p.Services, ";"))
	}

	var labelStrs []string
	for k, prop := range p.Labels {
		labelStrs = append(labelStrs, fmt.Sprintf("%s:%s", k, prop))
	}
	strs = append(strs, fmt.Sprintf("Labels:{%s}", strings.Join(labelStrs, ",")))

	var annotationStrs []string
	for k, prop := range p.Annotations {
		annotationStrs = append(annotationStrs, fmt.Sprintf("%s:%s", k, prop))
	}
	strs = append(strs, fmt.Sprintf("Annotations:{%s}", strings.Join(annotationStrs, ",")))

	return fmt.Sprintf("{%s}", strings.Join(strs, "; "))
}

type Allocation struct {
	Name       string
	Properties *AllocationProperties
}
