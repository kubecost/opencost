package ast

// FilterNode is the the base instance of a tree leaf node, which is a conditional operator
// which contains operands that may also be leaf nodes. A go type-switch should be used to
// reduce the FilterNode to a concrete type to operate on. If only the type of operator is
// required, the `Op()` field can be used.
type FilterNode interface {
	Op() FilterOp
}

// FilterGroup is a specialized interface for ops which can collect N operands.
type FilterGroup interface {
	FilterNode

	// Adds a new leaf node to the FilterGroup
	Add(FilterNode)
}

// Identifier is a struct that contains the data required to resolve a specific operand to a concrete
// value during operator compilation.
type Identifier struct {
	Field *Field
	Key   string
}

// Equal returns true if the identifiers are equal
func (id *Identifier) Equal(ident Identifier) bool {
	return id.Field.Equal(ident.Field) && id.Key == ident.Key
}

// String returns the string representation for the Identifier
func (id *Identifier) String() string {
	if id == nil {
		return "<nil>"
	}
	if id.Field == nil {
		return "<nil field>"
	}
	s := id.Field.Name
	if id.Key != "" {
		s += "[" + id.Key + "]"
	}
	return s
}
