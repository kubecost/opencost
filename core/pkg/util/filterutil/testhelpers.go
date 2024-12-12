package filterutil

import (
	"sort"
	"strings"

	"github.com/opencost/opencost/core/pkg/filter/ast"
)

func testingOnlyLess(left, right ast.FilterNode) bool {
	leftStr := ast.ToPreOrderShortString(left)
	rightStr := ast.ToPreOrderShortString(right)
	return strings.Compare(leftStr, rightStr) < 0
}

func testingOnlySortedOperands(operands []ast.FilterNode) []ast.FilterNode {
	var copy []ast.FilterNode
	for _, operand := range operands {
		copy = append(copy, operand)
	}
	sort.SliceStable(copy, func(i, j int) bool {
		leftSorted := TestingOnlySortNode(copy[i])
		rightSorted := TestingOnlySortNode(copy[j])

		return testingOnlyLess(leftSorted, rightSorted)
	})
	return copy
}

// TestingOnlySortNode sorts the provided node deterministically, intended only
// for use in unit tests to ensure that filter parsing steps produce logically-
// equivalent filters. This is useful only for cases where filters are
// constructed nondeterministically, like via a map iteration.
func TestingOnlySortNode(n ast.FilterNode) ast.FilterNode {
	switch concrete := n.(type) {
	case *ast.AndOp:
		return &ast.AndOp{
			Operands: testingOnlySortedOperands(concrete.Operands),
		}
	case *ast.OrOp:
		return &ast.OrOp{
			Operands: testingOnlySortedOperands(concrete.Operands),
		}
	case *ast.NotOp:
		return &ast.NotOp{
			Operand: TestingOnlySortNode(concrete.Operand),
		}
	// This isn't great, but non-container ops are mostly safe. We don't need
	// full deepcopy because this is for testing-only comparison
	default:
		return concrete
	}
}
