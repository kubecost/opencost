package opencost

import (
	"fmt"

	afilter "github.com/opencost/opencost/core/pkg/filter/allocation"
	"github.com/opencost/opencost/core/pkg/filter/ast"
	"github.com/opencost/opencost/core/pkg/filter/matcher"
	"github.com/opencost/opencost/core/pkg/filter/ops"
	"github.com/opencost/opencost/core/pkg/filter/transform"
)

// AllocationMatcher is a matcher implementation for Allocation instances,
// compiled using the matcher.MatchCompiler for allocations.
type AllocationMatcher matcher.Matcher[*Allocation]

// NewAllocationMatchCompiler creates a new instance of a
// matcher.MatchCompiler[*Allocation] which can be used to compile filter.Filter
// ASTs into matcher.Matcher[*Allocation] implementations.
//
// If the label config is nil, the compiler will fail to compile alias filters
// if any are present in the AST.
//
// If storage interfaces every support querying natively by alias (e.g. if a
// data store contained a "product" attribute on an Allocation row), that should
// be handled by a purpose-built AST compiler.
func NewAllocationMatchCompiler(labelConfig *LabelConfig) *matcher.MatchCompiler[*Allocation] {
	passes := []transform.CompilerPass{}

	// The label config pass should be the first pass
	if labelConfig != nil {
		passes = append(passes, NewAllocationAliasPass(*labelConfig))
	}

	passes = append(passes,
		transform.PrometheusKeySanitizePass(),
		transform.UnallocatedReplacementPass(),
	)
	return matcher.NewMatchCompiler(
		allocationFieldMap,
		allocationSliceFieldMap,
		allocationMapFieldMap,
		passes...,
	)
}

// Maps fields from an allocation to a string value based on an identifier
func allocationFieldMap(a *Allocation, identifier ast.Identifier) (string, error) {
	if a == nil {
		return "", fmt.Errorf("cannot map to nil allocation")
	}
	if a.Properties == nil {
		return "", fmt.Errorf("cannot map to nil properties")
	}
	if identifier.Field == nil {
		return "", fmt.Errorf("cannot map field from identifier with nil field")
	}
	switch afilter.AllocationField(identifier.Field.Name) {
	case afilter.FieldNamespace:
		return a.Properties.Namespace, nil
	case afilter.FieldNode:
		return a.Properties.Node, nil
	case afilter.FieldClusterID:
		return a.Properties.Cluster, nil
	case afilter.FieldControllerName:
		return a.Properties.Controller, nil
	case afilter.FieldControllerKind:
		return a.Properties.ControllerKind, nil
	case afilter.FieldPod:
		return a.Properties.Pod, nil
	case afilter.FieldContainer:
		return a.Properties.Container, nil
	case afilter.FieldProvider:
		return a.Properties.ProviderID, nil
	case afilter.FieldLabel:
		return a.Properties.Labels[identifier.Key], nil
	case afilter.FieldAnnotation:
		return a.Properties.Annotations[identifier.Key], nil
	}

	return "", fmt.Errorf("Failed to find string identifier on Allocation: %s", identifier.Field.Name)
}

// Maps slice fields from an allocation to a []string value based on an identifier
func allocationSliceFieldMap(a *Allocation, identifier ast.Identifier) ([]string, error) {
	switch afilter.AllocationField(identifier.Field.Name) {
	case afilter.FieldServices:
		return a.Properties.Services, nil
	}

	return nil, fmt.Errorf("Failed to find []string identifier on Allocation: %s", identifier.Field.Name)
}

// Maps map fields from an allocation to a map[string]string value based on an identifier
func allocationMapFieldMap(a *Allocation, identifier ast.Identifier) (map[string]string, error) {
	switch afilter.AllocationField(identifier.Field.Name) {
	case afilter.FieldLabel:
		return a.Properties.Labels, nil
	case afilter.FieldAnnotation:
		return a.Properties.Annotations, nil
	}
	return nil, fmt.Errorf("Failed to find map[string]string identifier on Allocation: %s", identifier.Field.Name)
}

// allocatioAliasPass implements the transform.CompilerPass interface, providing
// a pass which converts alias nodes to logically-equivalent label/annotation
// filter nodes based on the label config.
type allocationAliasPass struct {
	Config              LabelConfig
	AliasNameToAliasKey map[afilter.AllocationAlias]string
}

// NewAliasPass creates a compiler pass that converts alias nodes to
// logically-equivalent label/annotation nodes based on the label config.
//
// Due to the special alias logic that combines label and annotation behavior
// when filtering on alias, an alias filter is logically equivalent to the
// following expression:
//
// (or
//
//	(and (contains labels <parseraliaskey>)
//	     (<op> labels[<parseraliaskey>] <filtervalue>))
//	(and (not (contains labels <parseraliaskey>))
//	     (and (contains annotations departmentkey)
//	          (<op> annotations[<parseraliaskey>] <filtervalue>))))
func NewAllocationAliasPass(config LabelConfig) transform.CompilerPass {
	aliasNameToAliasKey := map[afilter.AllocationAlias]string{
		afilter.AliasDepartment:  config.DepartmentLabel,
		afilter.AliasEnvironment: config.EnvironmentLabel,
		afilter.AliasOwner:       config.OwnerLabel,
		afilter.AliasProduct:     config.ProductLabel,
		afilter.AliasTeam:        config.TeamLabel,
	}

	return &allocationAliasPass{
		Config:              config,
		AliasNameToAliasKey: aliasNameToAliasKey,
	}
}

// Exec implements the transform.CompilerPass interface for an alias pass.
// See aliasPass struct documentation for an explanation.
func (p *allocationAliasPass) Exec(filter ast.FilterNode) (ast.FilterNode, error) {
	if p.AliasNameToAliasKey == nil {
		return nil, fmt.Errorf("cannot perform alias conversion with nil mapping of alias name -> key")
	}

	var transformErr error
	leafTransformerFunc := func(node ast.FilterNode) ast.FilterNode {
		if transformErr != nil {
			return node
		}

		var field *ast.Field
		var filterValue string
		var filterOp ast.FilterOp

		switch concrete := node.(type) {
		// These ops are not alias ops, alias ops can only be base-level ops
		// like =, !=, etc. No modification required here.
		case *ast.AndOp, *ast.OrOp, *ast.NotOp, *ast.VoidOp, *ast.ContradictionOp:
			return node

		case *ast.EqualOp:
			field = concrete.Left.Field
			filterValue = concrete.Right
			filterOp = ast.FilterOpEquals
		case *ast.ContainsOp:
			field = concrete.Left.Field
			filterValue = concrete.Right
			filterOp = ast.FilterOpContains
		case *ast.ContainsPrefixOp:
			field = concrete.Left.Field
			filterValue = concrete.Right
			filterOp = ast.FilterOpContainsPrefix
		case *ast.ContainsSuffixOp:
			field = concrete.Left.Field
			filterValue = concrete.Right
			filterOp = ast.FilterOpContainsSuffix
		default:
			transformErr = fmt.Errorf("unknown op '%s' during alias pass", concrete.Op())
			return node
		}

		if field == nil {
			return node
		}
		if !field.IsAlias() {
			return node
		}

		filterFieldAlias := afilter.AllocationAlias(field.Name)
		parserAliasKey, ok := p.AliasNameToAliasKey[filterFieldAlias]
		if !ok {
			transformErr = fmt.Errorf("unknown alias field '%s'", filterFieldAlias)
			return node
		}

		newFilter, err := convertAliasFilterToLabelAnnotationFilter(parserAliasKey, filterValue, filterOp)
		if err != nil {
			transformErr = fmt.Errorf("performing alias conversion for node '%+v': %w", node, err)
			return node
		}

		return newFilter
	}

	newFilter := ast.TransformLeaves(filter, leafTransformerFunc)

	if transformErr != nil {
		return nil, fmt.Errorf("alias pass transform: %w", transformErr)
	}

	return newFilter, nil
}

// convertAliasFilterToLabelAnnotationFilter constructs a new filter node using
// only operations on labels and annotations that is logically equivalent to an
// alias node from relevant data extracted from the original alias node.
func convertAliasFilterToLabelAnnotationFilter(aliasKey string, filterValue string, op ast.FilterOp) (ast.FilterNode, error) {
	labelKey := ops.WithKey(afilter.FieldLabel, aliasKey)
	annotationKey := ops.WithKey(afilter.FieldAnnotation, aliasKey)

	var labelOp ast.FilterNode
	var annotationOp ast.FilterNode

	// This should only need to implement conversion for base-level ops like
	// equals, contains, etc.
	switch op {
	case ast.FilterOpEquals:
		labelOp = ops.Eq(labelKey, filterValue)
		annotationOp = ops.Eq(annotationKey, filterValue)
	case ast.FilterOpContains:
		labelOp = ops.Contains(labelKey, filterValue)
		annotationOp = ops.Contains(annotationKey, filterValue)
	case ast.FilterOpContainsPrefix:
		labelOp = ops.ContainsPrefix(labelKey, filterValue)
		annotationOp = ops.ContainsPrefix(annotationKey, filterValue)
	case ast.FilterOpContainsSuffix:
		labelOp = ops.ContainsSuffix(labelKey, filterValue)
		annotationOp = ops.ContainsSuffix(annotationKey, filterValue)
	default:
		return nil, fmt.Errorf("unsupported op type '%s' for alias conversion", op)
	}

	// This handles the case where a label EXISTS/IS PRESENT for (is extant)
	// for an aliased field. That's the primary case.
	extantCaseNode := ops.Or(
		ops.And(
			ops.Contains(afilter.FieldLabel, aliasKey),
			labelOp,
		),
		ops.And(
			ops.Not(ops.Contains(afilter.FieldLabel, aliasKey)),
			ops.And(
				ops.Contains(afilter.FieldAnnotation, aliasKey),
				annotationOp,
			),
		),
	)
	var node ast.FilterNode
	// This handles the special case of unallocated aliased value. There's
	// two forms of this; first is where the label/annotation exists, but
	// has an empty string value. That's actually handled by the extant case,
	// because the API passes through that empty string. The other is when
	// the aliased label/annotation doesn't exist for an allocation. That's
	// what this modification to the tree handles. This matters when you're
	// trying to drill into/identify workloads "not allocated" within that
	// specific aliased field.
	if filterValue == "" || filterValue == UnallocatedSuffix {
		node = ops.Or(
			extantCaseNode,
			ops.And(
				ops.Not(ops.Contains(afilter.FieldLabel, aliasKey)),
				ops.Not(ops.Contains(afilter.FieldAnnotation, aliasKey)),
			),
		)
	} else {
		node = extantCaseNode
	}

	return node, nil
}
