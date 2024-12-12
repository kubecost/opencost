package allocation

import (
	"github.com/opencost/opencost/core/pkg/filter/fieldstrings"
)

// AllocationField is an enum that represents Allocation-specific fields that can be
// filtered on (namespace, label, etc.)
type AllocationField string

// If you add a AllocationFilterField, make sure to update field maps to return the correct
// Allocation value
// does not enforce exhaustive pattern matching on "enum" types.
const (
	FieldClusterID      AllocationField = AllocationField(fieldstrings.FieldClusterID)
	FieldNode           AllocationField = AllocationField(fieldstrings.FieldNode)
	FieldNamespace      AllocationField = AllocationField(fieldstrings.FieldNamespace)
	FieldControllerKind AllocationField = AllocationField(fieldstrings.FieldControllerKind)
	FieldControllerName AllocationField = AllocationField(fieldstrings.FieldControllerName)
	FieldPod            AllocationField = AllocationField(fieldstrings.FieldPod)
	FieldContainer      AllocationField = AllocationField(fieldstrings.FieldContainer)
	FieldProvider       AllocationField = AllocationField(fieldstrings.FieldProvider)
	FieldServices       AllocationField = AllocationField(fieldstrings.FieldServices)
	FieldLabel          AllocationField = AllocationField(fieldstrings.FieldLabel)
	FieldAnnotation     AllocationField = AllocationField(fieldstrings.FieldAnnotation)
)

// AllocationAlias represents an alias field type for allocations.
// Filtering based on label aliases (team, department, etc.) should be a
// responsibility of the query handler. By the time it reaches this
// structured representation, we shouldn't have to be aware of what is
// aliased to what. The aliases correspond to either a label or annotation,
// defined by the user.
type AllocationAlias string

const (
	AliasDepartment  AllocationAlias = AllocationAlias(fieldstrings.AliasDepartment)
	AliasEnvironment AllocationAlias = AllocationAlias(fieldstrings.AliasEnvironment)
	AliasOwner       AllocationAlias = AllocationAlias(fieldstrings.AliasOwner)
	AliasProduct     AllocationAlias = AllocationAlias(fieldstrings.AliasProduct)
	AliasTeam        AllocationAlias = AllocationAlias(fieldstrings.AliasTeam)
)
