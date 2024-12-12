package ast

// FieldType is an enumeration of specific types relevant to lexing and
// parsing a filter.
type FieldType int

const (
	FieldTypeDefault FieldType = 1 << iota
	FieldTypeSlice
	FieldTypeMap
	FieldTypeAlias
)

// FieldAttribute is an enumeration of specific attributes that can be set
// on each type of field.
type FieldAttribute int

const (
	FieldAttributeNilable FieldAttribute = 1 << (iota + 4)
)

// fieldType with attributes is a convenience function for creating a field type with
// attributes flags set.
func fieldTypeWithAttributes(ft FieldType, attrs ...FieldAttribute) FieldType {
	for _, attr := range attrs {
		ft.Set(attr)
	}

	return ft
}

// Set updates the field type with the provided attribute.
func (ft *FieldType) Set(attr FieldAttribute) {
	*ft |= FieldType(attr)
}

// Unset removes the provided attribute from the field type.
func (ft *FieldType) Unset(attr FieldAttribute) {
	*ft &= ^FieldType(attr)
}

// Is returns true if the field type has the provided attribute.
func (ft FieldType) Is(attr FieldAttribute) bool {
	return ft&FieldType(attr) != 0
}

// IsDefault returns true if the type is the default/base type.
func (ft FieldType) IsDefault() bool {
	return ft&FieldTypeDefault != 0
}

// IsSlice returns true if the type is a slice type.
func (ft FieldType) IsSlice() bool {
	return ft&FieldTypeSlice != 0
}

// IsMap returns true if the type is a map type.
func (ft FieldType) IsMap() bool {
	return ft&FieldTypeMap != 0
}

// IsAlias returns true if the type is an alias type.
func (ft FieldType) IsAlias() bool {
	return ft&FieldTypeAlias != 0
}

// Field is a Lexer input which acts as a mapping of identifiers used to lex/parse filters.
type Field struct {
	// Name contains the name of the specific field as it appears in language.
	Name string

	fieldType FieldType
}

// Field equivalence is determined by name and type.
func (f *Field) Equal(other *Field) bool {
	if f == nil || other == nil {
		return false
	}

	return f.Name == other.Name && f.fieldType == other.fieldType
}

// IsSlice returns true if the field is a slice. This instructs the lexer that the field
// should allow contains operations.
func (f *Field) IsSlice() bool {
	return f.fieldType.IsSlice()
}

// IsMap returns true if the field is a map. This instructs the lexer that the field should
// allow keyed-access operations.
func (f *Field) IsMap() bool {
	return f.fieldType.IsMap()
}

// IsAlias returns true if the field is an alias type. This instructs the lexer that the field
// is an alias for custom logical resolution by an external compiler.
func (f *Field) IsAlias() bool {
	return f.fieldType.IsAlias()
}

// IsNilable returns true if the field is an default field type that can contain a nil value. Only
// specific compilers will need to know this information. ie: Go does not have a nil value for strings,
// but SQL does.
func (f *Field) IsNilable() bool {
	return f.fieldType.Is(FieldAttributeNilable)
}

// NewField creates a default string field using the provided name.
func NewField[T ~string](name T, attrs ...FieldAttribute) *Field {
	return &Field{
		Name:      string(name),
		fieldType: fieldTypeWithAttributes(FieldTypeDefault, attrs...),
	}
}

// NewSliceField creates a slice field using the provided name.
func NewSliceField[T ~string](name T, attrs ...FieldAttribute) *Field {
	return &Field{
		Name:      string(name),
		fieldType: fieldTypeWithAttributes(FieldTypeSlice, attrs...),
	}
}

// NewMapField creates a new map field using the provided name.
func NewMapField[T ~string](name T, attrs ...FieldAttribute) *Field {
	return &Field{
		Name:      string(name),
		fieldType: fieldTypeWithAttributes(FieldTypeMap, attrs...),
	}
}

// NewAliasField creates a new alias field using the provided name.
func NewAliasField[T ~string](name T, attrs ...FieldAttribute) *Field {
	return &Field{
		Name:      string(name),
		fieldType: fieldTypeWithAttributes(FieldTypeAlias, attrs...),
	}
}
