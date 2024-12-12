package transform

import "github.com/opencost/opencost/core/pkg/filter/ast"

const unallocatedSuffix = "__unallocated__"

var unallocPass CompilerPass = new(unallocReplacePass)

// UnallocatedReplacementPass returns a CompilerPass implementation which replaces all
// __unallocated__ with empty string
func UnallocatedReplacementPass() CompilerPass {
	return unallocPass
}

type unallocReplacePass struct{}

// Exec executes the pass on the provided AST. This method may either return
// a new AST or modify and return the AST parameter. The parameter into this
// method may be changed directly.
func (pks *unallocReplacePass) Exec(filter ast.FilterNode) (ast.FilterNode, error) {
	ast.PreOrderTraversal(filter, func(fn ast.FilterNode, ts ast.TraversalState) {
		switch n := fn.(type) {
		case *ast.EqualOp:
			n.Right = replaceUnallocated(n.Right)
		case *ast.ContainsOp:
			n.Right = replaceUnallocated(n.Right)
		case *ast.ContainsPrefixOp:
			n.Right = replaceUnallocated(n.Right)
		case *ast.ContainsSuffixOp:
			n.Right = replaceUnallocated(n.Right)
		}
	})
	return filter, nil
}

// replaces unallocated with empty string if valid
func replaceUnallocated(s string) string {
	if s == unallocatedSuffix {
		return ""
	}
	return s
}
