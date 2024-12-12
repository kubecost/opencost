package ast

import (
	"fmt"
	"reflect"
	"testing"
)

func ExampleTransformLeaves() {
	originalTree := &AndOp{
		Operands: []FilterNode{
			&EqualOp{
				Left: Identifier{
					Field: &Field{
						Name: "field1",
					},
					Key: "foo",
				},
				Right: "bar",
			},

			&EqualOp{
				Left: Identifier{
					Field: &Field{
						Name: "field2",
					},
				},
				Right: "bar",
			},
		},
	}

	// This transformer applies "Not" to all leaves
	transformFunc := func(node FilterNode) FilterNode {
		switch concrete := node.(type) {
		case *AndOp, *OrOp, *NotOp:
			panic("Leaf transformer should not be called on non-leaf nodes")
		default:
			return &NotOp{Operand: concrete}
		}
	}

	newTree := TransformLeaves(originalTree, transformFunc)
	fmt.Println(ToPreOrderString(newTree))
	// Output:
	// And {
	//   Not {
	//     Equals { Left: field1[foo], Right: bar }
	//   }
	//   Not {
	//     Equals { Left: field2, Right: bar }
	//   }
	// }
}

func TestFields(t *testing.T) {
	type testCase struct {
		name   string
		filter FilterNode
		exp    []Field
	}

	fieldNamespace := *NewField("namespace")
	fieldCluster := *NewField("cluster")
	fieldControllerKind := *NewField("controllerKind")

	testCases := []testCase{
		{
			name:   ``,
			filter: &VoidOp{},
			exp:    []Field{},
		},
		{
			name: `namespace:"kubecost"`,
			filter: &EqualOp{
				Left: Identifier{
					Field: NewField("namespace"),
					Key:   "",
				},
				Right: "kubecost",
			},
			exp: []Field{fieldNamespace},
		},
		{
			name: `namespace: "kubecost" | cluster:"cluster-one" | controllerKind:"deployment"`,
			filter: &OrOp{
				Operands: []FilterNode{
					&EqualOp{
						Left: Identifier{
							Field: NewField("namespace"),
							Key:   "",
						},
						Right: "kubecost",
					},
					&EqualOp{
						Left: Identifier{
							Field: NewField("cluster"),
							Key:   "",
						},
						Right: "cluster-one",
					},
					&EqualOp{
						Left: Identifier{
							Field: NewField("controllerKind"),
							Key:   "",
						},
						Right: "deployment",
					},
				},
			},
			exp: []Field{fieldCluster, fieldControllerKind, fieldNamespace},
		},
		{
			name: `namespace: "kubecost" | namespace:"kube-system" | namespace:"default"`,
			filter: &OrOp{
				Operands: []FilterNode{
					&EqualOp{
						Left: Identifier{
							Field: NewField("namespace"),
							Key:   "",
						},
						Right: "kubecost",
					},
					&EqualOp{
						Left: Identifier{
							Field: NewField("namespace"),
							Key:   "",
						},
						Right: "kube-system",
					},
					&EqualOp{
						Left: Identifier{
							Field: NewField("namespace"),
							Key:   "",
						},
						Right: "default",
					},
				},
			},
			exp: []Field{fieldNamespace},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			act := Fields(tc.filter)
			if !reflect.DeepEqual(tc.exp, act) {
				t.Errorf("fields do not match; expected %v; got %v", tc.exp, act)
			}
		})
	}
}
