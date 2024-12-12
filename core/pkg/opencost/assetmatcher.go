package opencost

import (
	"fmt"
	"strings"

	afilter "github.com/opencost/opencost/core/pkg/filter/asset"
	"github.com/opencost/opencost/core/pkg/filter/ast"
	"github.com/opencost/opencost/core/pkg/filter/matcher"
	"github.com/opencost/opencost/core/pkg/filter/transform"
)

// AssetMatcher is a matcher implementation for Asset instances,
// compiled using the matcher.MatchCompiler.
type AssetMatcher matcher.Matcher[Asset]

// NewAssetMatchCompiler creates a new instance of a
// matcher.MatchCompiler[Asset] which can be used to compile filter.Filter
// ASTs into matcher.Matcher[Asset] implementations.
//
// If the label config is nil, the compiler will fail to compile alias filters
// if any are present in the AST.
//
// If storage interfaces every support querying natively by alias (e.g. if a
// data store contained a "product" attribute on an Asset row), that should
// be handled by a purpose-built AST compiler.
func NewAssetMatchCompiler() *matcher.MatchCompiler[Asset] {
	passes := []transform.CompilerPass{}

	passes = append(passes,
		transform.PrometheusKeySanitizePass(),
		transform.UnallocatedReplacementPass(),
	)
	return matcher.NewMatchCompiler(
		assetFieldMap,
		assetSliceFieldMap,
		assetMapFieldMap,
		passes...,
	)
}

// Maps fields from an asset to a string value based on an identifier
func assetFieldMap(a Asset, identifier ast.Identifier) (string, error) {
	if identifier.Field == nil {
		return "", fmt.Errorf("cannot map field from identifier with nil field")
	}
	if a == nil {
		return "", fmt.Errorf("cannot map field for nil Asset")
	}

	// Check special fields before defaulting to properties-based fields
	switch afilter.AssetField(identifier.Field.Name) {
	case afilter.FieldType:
		return strings.ToLower(a.Type().String()), nil
	case afilter.FieldLabel:
		labels := a.GetLabels()
		if labels == nil {
			return "", nil
		}
		return labels[identifier.Key], nil
	}

	props := a.GetProperties()
	if props == nil {
		return "", fmt.Errorf("cannot map field for Asset with nil props")
	}

	switch afilter.AssetField(identifier.Field.Name) {
	case afilter.FieldName:
		return props.Name, nil
	case afilter.FieldCategory:
		return props.Category, nil
	case afilter.FieldClusterID:
		return props.Cluster, nil
	case afilter.FieldProject:
		return props.Project, nil
	case afilter.FieldProvider:
		return props.Provider, nil
	case afilter.FieldProviderID:
		return props.ProviderID, nil
	case afilter.FieldAccount:
		return props.Account, nil
	case afilter.FieldService:
		return props.Service, nil
	}

	return "", fmt.Errorf("Failed to find string identifier on Asset: %s", identifier.Field.Name)
}

// Maps slice fields from an asset to a []string value based on an identifier
func assetSliceFieldMap(a Asset, identifier ast.Identifier) ([]string, error) {
	return nil, fmt.Errorf("Assets have no slice fields")
}

// Maps map fields from an Asset to a map[string]string value based on an identifier
func assetMapFieldMap(a Asset, identifier ast.Identifier) (map[string]string, error) {
	if a == nil {
		return nil, fmt.Errorf("cannot get map field for nil Asset")
	}
	switch afilter.AssetField(identifier.Field.Name) {
	case afilter.FieldLabel:
		return a.GetLabels(), nil
	}
	return nil, fmt.Errorf("Failed to find map[string]string identifier on Asset: %s", identifier.Field.Name)
}
