package ast

// FilterOp is an enum that represents operations that can be performed
// when filtering (equality, inequality, etc.)
type FilterOp string

// If you add a FilterOp, MAKE SURE TO UPDATE ALL FILTER IMPLEMENTATIONS! Go
// does not enforce exhaustive pattern matching on "enum" types.
const (
	// FilterOpEquals is the equality operator
	//
	// "kube-system" FilterOpEquals "kube-system" = true
	// "kube-syste" FilterOpEquals "kube-system" = false
	FilterOpEquals FilterOp = "equals"

	// FilterOpNotEquals is the inverse of equals.
	FilterOpNotEquals = "notequals"

	// FilterOpContains supports string fields, slice fields, and map fields.
	// For maps, this is equivalent to map.HasKey(x)
	//
	// "kube-system" FilterOpContains "e-s" = true
	// ["a", "b", "c"] FilterOpContains "a" = true
	// { "namespace": "kubecost", "cluster": "cluster-one" } FilterOpContains "namespace" = true
	FilterOpContains = "contains"

	// FilterOpNotContains is the inverse of contains.
	FilterOpNotContains = "notcontains"

	// FilterOpContainsPrefix is like FilterOpContains, but checks against the start of a string.
	// For maps, this checks to see if any of the keys start with the prefix
	//
	// "kube-system" ContainsPrefix "kube" = true
	// ["kube-system", "abc123"] ContainsPrefix "kube" = true
	// { "kube-label": "test", "abc": "123" } ContainsPrefix "ab" = true
	FilterOpContainsPrefix = "containsprefix"

	// FilterOpNotContainsPrefix is the inverse of FilterOpContainsPrefix
	FilterOpNotContainsPrefix = "notcontainsprefix"

	// FilterOpContainsSuffix is like FilterOpContains, but checks against the end of a string.
	// For maps, this checks to see if any of the keys end with the suffix
	//
	// "kube-system" ContainsSuffix "system" = true
	// ["kube-system", "abc123"] ContainsSuffix "system" = true
	// { "kube-label": "test", "abc": "123" } ContainsSuffix "-label" = true
	FilterOpContainsSuffix = "containssuffix"

	// FilterOpNotContainsSuffix is the inverse of FilterOpContainsSuffix
	FilterOpNotContainsSuffix = "notcontainssuffix"

	// FilterOpVoid is base-depth operator that is used for an empty filter
	FilterOpVoid = "void"

	// FilterOpContradiction is a base-depth operator that filters all data.
	FilterOpContradiction = "contradiction"

	// FilterOpAnd is an operator that succeeds if all parameters succeed.
	FilterOpAnd = "and"

	// FilterOpOr is an operator that succeeds if any parameter succeeds
	FilterOpOr = "or"

	// FilterOpNot is an operator that contains a single operand
	FilterOpNot = "not"
)

// VoidOp is base-depth operator that is used for an empty filter
type VoidOp struct{}

// Op returns the FilterOp enumeration value for the operator.
func (_ *VoidOp) Op() FilterOp {
	return FilterOpVoid
}

// ContradictionOp is a base-depth operator that filters all data.
type ContradictionOp struct{}

// Op returns the FilterOp enumeration value for the operator.
func (_ *ContradictionOp) Op() FilterOp {
	return FilterOpContradiction
}

// AndOp is a filter operation that contains a flat list of nodes which should all resolve
// to true in order for the result to be true.
type AndOp struct {
	Operands []FilterNode
}

// Op returns the FilterOp enumeration value for the operator.
func (_ *AndOp) Op() FilterOp {
	return FilterOpAnd
}

// Add appends a filter node to the flat list of operands within the AND operator
func (ao *AndOp) Add(node FilterNode) {
	ao.Operands = append(ao.Operands, node)
}

// OrOp is a filter operation that contains a flat list of nodes which at least one node
// should resolve to true in order for the result to be true.
type OrOp struct {
	Operands []FilterNode
}

// Op returns the FilterOp enumeration value for the operator.
func (_ *OrOp) Op() FilterOp {
	return FilterOpOr
}

// Add appends a filter node to the flat list of operands within the OR operator
func (oo *OrOp) Add(node FilterNode) {
	oo.Operands = append(oo.Operands, node)
}

// NotOp is a filter operation that logically inverts result of the child operand.
type NotOp struct {
	Operand FilterNode
}

// Op returns the FilterOp enumeration value for the operator.
func (_ *NotOp) Op() FilterOp {
	return FilterOpNot
}

// Add sets the not operand to the parameter
func (no *NotOp) Add(node FilterNode) {
	no.Operand = node
}

// EqualOp is a filter operation that compares a resolvable identifier (Left) to a
// string value (Right)
type EqualOp struct {
	// Left contains a resolvable Identifier (property of an input type) which can be
	// used to compare against the Right value.
	Left Identifier

	// Right contains the value which we wish to compare the resolved identifier to.
	Right string
}

// Op returns the FilterOp enumeration value for the operator.
func (_ *EqualOp) Op() FilterOp {
	return FilterOpEquals
}

// ContainsOp is a filter operation that checks to see if a resolvable identifier (Left) contains a
// string value (Right)
type ContainsOp struct {
	// Left contains a resolvable Identifier (property of an input type) which can be
	// used to query against using the Right value.
	Left Identifier

	// Right contains the value which we use to search the resolved Left identifier with.
	Right string
}

// Op returns the FilterOp enumeration value for the operator.
func (_ *ContainsOp) Op() FilterOp {
	return FilterOpContains
}

// ContainsPrefixOp is a filter operation that checks to see if a resolvable identifier (Left) starts with a
// string value (Right)
type ContainsPrefixOp struct {
	// Left contains a resolvable Identifier (property of an input type) which can be
	// used to query against using the Right value.
	Left Identifier

	// Right contains the value which we use to search the resolved Left identifier with.
	Right string
}

// Op returns the FilterOp enumeration value for the operator.
func (_ *ContainsPrefixOp) Op() FilterOp {
	return FilterOpContainsPrefix
}

// ContainsSuffixOp is a filter operation that checks to see if a resolvable identifier (Left) ends with a
// string value (Right)
type ContainsSuffixOp struct {
	// Left contains a resolvable Identifier (property of an input type) which can be
	// used to query against using the Right value.
	Left Identifier

	// Right contains the value which we use to search the resolved Left identifier with.
	Right string
}

// Op returns the FilterOp enumeration value for the operator.
func (_ *ContainsSuffixOp) Op() FilterOp {
	return FilterOpContainsSuffix
}

func Not(fn FilterNode) FilterNode {
	return &NotOp{Operand: fn}
}

func IsVoid(fn FilterNode) bool {
	_, ok := fn.(*VoidOp)
	return ok
}
