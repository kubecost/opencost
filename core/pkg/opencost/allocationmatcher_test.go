package opencost

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	afilter "github.com/opencost/opencost/core/pkg/filter/allocation"
	"github.com/opencost/opencost/core/pkg/filter/ast"
	"github.com/opencost/opencost/core/pkg/filter/ops"
)

func TestAliasPass(t *testing.T) {
	labelConfig := &LabelConfig{
		DepartmentLabel:  "keydepartment",
		EnvironmentLabel: "keyenvironment",
		OwnerLabel:       "keyowner",
		ProductLabel:     "keyproduct",
		TeamLabel:        "keyteam",
	}

	cases := []struct {
		name     string
		input    ast.FilterNode
		expected ast.FilterNode
	}{
		{
			name: "department equal",
			input: &ast.EqualOp{
				Left: ast.Identifier{
					Field: ast.NewAliasField(afilter.AliasDepartment),
				},
				Right: "x",
			},
			expected: ops.Or(
				ops.And(
					ops.Contains(afilter.FieldLabel, "keydepartment"),
					ops.Eq(ops.WithKey(afilter.FieldLabel, "keydepartment"), "x"),
				),
				ops.And(
					ops.Not(ops.Contains(afilter.FieldLabel, "keydepartment")),
					ops.And(
						ops.Contains(afilter.FieldAnnotation, "keydepartment"),
						ops.Eq(ops.WithKey(afilter.FieldAnnotation, "keydepartment"), "x"),
					),
				),
			),
		},
	}

	for _, c := range cases {
		pass := NewAllocationAliasPass(*labelConfig)

		t.Run(c.name, func(t *testing.T) {
			result, err := pass.Exec(c.input)
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}

			if diff := cmp.Diff(c.expected, result); len(diff) > 0 {
				t.Errorf("diff: %s", diff)
			}
		})
	}
}
