package asset

import (
	"github.com/opencost/opencost/core/pkg/filter/fieldstrings"
)

// AssetField is an enum that represents Asset-specific fields that can be
// filtered on (namespace, label, etc.)
type AssetField string

// If you add a AssetField, make sure to update field maps to return the correct
// Asset value does not enforce exhaustive pattern matching on "enum" types.
const (
	FieldName       AssetField = AssetField(fieldstrings.FieldName)
	FieldType       AssetField = AssetField(fieldstrings.FieldType)
	FieldCategory   AssetField = AssetField(fieldstrings.FieldCategory)
	FieldClusterID  AssetField = AssetField(fieldstrings.FieldClusterID)
	FieldProject    AssetField = AssetField(fieldstrings.FieldProject)
	FieldProvider   AssetField = AssetField(fieldstrings.FieldProvider)
	FieldProviderID AssetField = AssetField(fieldstrings.FieldProviderID)
	FieldAccount    AssetField = AssetField(fieldstrings.FieldAccount)
	FieldService    AssetField = AssetField(fieldstrings.FieldService)
	FieldLabel      AssetField = AssetField(fieldstrings.FieldLabel)
)

// AssetAlias represents an alias field type for assets.
// Filtering based on label aliases (team, department, etc.) should be a
// responsibility of the query handler. By the time it reaches this
// structured representation, we shouldn't have to be aware of what is
// aliased to what.
type AssetAlias string

const (
	DepartmentProp  AssetAlias = AssetAlias(fieldstrings.AliasDepartment)
	EnvironmentProp AssetAlias = AssetAlias(fieldstrings.AliasEnvironment)
	OwnerProp       AssetAlias = AssetAlias(fieldstrings.AliasOwner)
	ProductProp     AssetAlias = AssetAlias(fieldstrings.AliasProduct)
	TeamProp        AssetAlias = AssetAlias(fieldstrings.AliasTeam)
)
