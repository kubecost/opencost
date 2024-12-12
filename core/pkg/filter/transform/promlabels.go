package transform

import (
	"regexp"

	"github.com/opencost/opencost/core/pkg/filter/ast"
)

// regex for invalid prometheus label characters
var invalidKey = regexp.MustCompile(`[^a-zA-Z0-9_]`)
var promKeyPass CompilerPass = new(promKeySanitizePass)

// PrometheusKeySanitizePass returns a
func PrometheusKeySanitizePass() CompilerPass {
	return promKeyPass
}

type promKeySanitizePass struct{}

// Exec executes the pass on the provided AST. This method may either return
// a new AST or modify and return the AST parameter. The parameter into this
// method may be changed directly.
func (pks *promKeySanitizePass) Exec(filter ast.FilterNode) (ast.FilterNode, error) {
	ast.PreOrderTraversal(filter, func(fn ast.FilterNode, ts ast.TraversalState) {
		switch n := fn.(type) {
		case *ast.EqualOp:
			sanitize(&n.Left)
		case *ast.ContainsOp:
			left := &n.Left
			// if we use a contains operator on a map, we sanitize the value
			if left.Field.IsMap() && left.Key == "" {
				n.Right = sanitizeKey(n.Right)
			} else {
				sanitize(left)
			}
		case *ast.ContainsPrefixOp:
			left := &n.Left
			// if we use a contains operator on a map, we sanitize the value
			if left.Field.IsMap() && left.Key == "" {
				n.Right = sanitizeKey(n.Right)
			} else {
				sanitize(left)
			}
		case *ast.ContainsSuffixOp:
			left := &n.Left
			// if we use a contains operator on a map, we sanitize the value
			if left.Field.IsMap() && left.Key == "" {
				n.Right = sanitizeKey(n.Right)
			} else {
				sanitize(left)
			}
		}
	})
	return filter, nil
}

// sanitizes the identifier
func sanitize(left *ast.Identifier) {
	if left.Key != "" {
		left.Key = sanitizeKey(left.Key)
	}
}

// replaces all invalid characters with underscore
func sanitizeKey(s string) string {
	return invalidKey.ReplaceAllString(s, "_")
}
