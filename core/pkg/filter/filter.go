package filter

import "github.com/opencost/opencost/core/pkg/filter/ast"

// Filter is just the root node of an AST. There are various compiler implementations
// available to create data source specific filtering from the AST.
type Filter = ast.FilterNode
