package filterutil

import (
	"strings"

	"github.com/opencost/opencost/core/pkg/clusters"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/mapper"
	"github.com/opencost/opencost/core/pkg/util/promutil"
	"github.com/opencost/opencost/core/pkg/util/typeutil"

	"github.com/opencost/opencost/core/pkg/filter"
	afilter "github.com/opencost/opencost/core/pkg/filter/allocation"
	assetfilter "github.com/opencost/opencost/core/pkg/filter/asset"
	"github.com/opencost/opencost/core/pkg/filter/ast"
	cloudcostfilter "github.com/opencost/opencost/core/pkg/filter/cloudcost"
)

// ============================================================================
// This file contains:
// Parsing (HTTP query params -> v2.1 filter) for V1 of query param filters
//
// e.g. "filterNamespaces=ku&filterControllers=deployment:kc"
// ============================================================================

// This is somewhat of a fancy solution, but allows us to "register" DefaultFieldByName funcs
// funcs by Field type.
var defaultFieldByType = map[string]any{
	// typeutil.TypeOf[cloudfilter.CloudAggregationField](): cloudfilter.DefaultFieldByName,
	typeutil.TypeOf[afilter.AllocationField]():        afilter.DefaultFieldByName,
	typeutil.TypeOf[assetfilter.AssetField]():         assetfilter.DefaultFieldByName,
	typeutil.TypeOf[cloudcostfilter.CloudCostField](): cloudcostfilter.DefaultFieldByName,
}

// DefaultFieldByName looks up a specific T field instance by name and returns the default
// ast.Field value for that type.
func DefaultFieldByName[T ~string](field T) *ast.Field {
	lookup, ok := defaultFieldByType[typeutil.TypeOf[T]()]
	if !ok {
		log.Errorf("Failed to get default field lookup for: %s", typeutil.TypeOf[T]())
		return nil
	}

	defaultLookup, ok := lookup.(func(T) *ast.Field)
	if !ok {
		log.Errorf("Failed to cast default field lookup for: %s", typeutil.TypeOf[T]())
		return nil
	}

	return defaultLookup(field)
}

// AllocationFilterFromParamsV1 takes a set of HTTP query parameters and
// converts them to an AllocationFilter, which is a structured in-Go
// representation of a set of filters.
//
// The HTTP query parameters are the "v1" filters attached to the Allocation
// API: "filterNamespaces=", "filterNodes=", etc.
//
// It takes an optional LabelConfig, which if provided enables "label-mapped"
// filters like "filterDepartments".
//
// It takes an optional ClusterMap, which if provided enables cluster name
// filtering. This turns all `filterClusters=foo` arguments into the equivalent
// of `clusterID = "foo" OR clusterName = "foo"`.
func AllocationFilterFromParamsV1(
	params AllocationFilterV1,
	labelConfig *opencost.LabelConfig,
	clusterMap clusters.ClusterMap,
) filter.Filter {

	var filterOps []ast.FilterNode

	// ClusterMap does not provide a cluster name -> cluster ID mapping in the
	// interface, probably because there could be multiple IDs with the same
	// name. However, V1 filter logic demands that the parameters to
	// filterClusters= be checked against both cluster ID AND cluster name.
	//
	// To support expected filterClusters= behavior, we construct a mapping
	// of cluster name -> cluster IDs (could be multiple IDs for the same name)
	// so that we can create AllocationFilters that use only ClusterIDEquals.
	//
	//
	// AllocationFilter intentionally does not support cluster name filters
	// because those should be considered presentation-layer only.
	clusterNameToIDs := map[string][]string{}
	if clusterMap != nil {
		cMap := clusterMap.AsMap()
		for _, info := range cMap {
			if info == nil {
				continue
			}

			if _, ok := clusterNameToIDs[info.Name]; ok {
				clusterNameToIDs[info.Name] = append(clusterNameToIDs[info.Name], info.ID)
			} else {
				clusterNameToIDs[info.Name] = []string{info.ID}
			}
		}
	}

	// The proliferation of > 0 guards in the function is to avoid constructing
	// empty filter structs. While it is functionally equivalent to add empty
	// filter structs (they evaluate to true always) there could be overhead
	// when calling Matches() repeatedly for no purpose.

	if len(params.Clusters) > 0 {
		var ops []ast.FilterNode

		// filter my cluster identifier
		ops = push(ops, filterV1SingleValueFromList(params.Clusters, afilter.FieldClusterID))

		for _, rawFilterValue := range params.Clusters {
			clusterNameFilter, wildcard := parseWildcardEnd(rawFilterValue)

			clusterIDsToFilter := []string{}
			for clusterName := range clusterNameToIDs {
				if wildcard && strings.HasPrefix(clusterName, clusterNameFilter) {
					clusterIDsToFilter = append(clusterIDsToFilter, clusterNameToIDs[clusterName]...)
				} else if !wildcard && clusterName == clusterNameFilter {
					clusterIDsToFilter = append(clusterIDsToFilter, clusterNameToIDs[clusterName]...)
				}
			}

			for _, clusterID := range clusterIDsToFilter {
				ops = append(ops, &ast.EqualOp{
					Left: ast.Identifier{
						Field: afilter.DefaultFieldByName(afilter.FieldClusterID),
						Key:   "",
					},
					Right: clusterID,
				})
			}
		}

		//
		clustersOp := opsToOr(ops)
		filterOps = push(filterOps, clustersOp)
	}

	if len(params.Nodes) > 0 {
		filterOps = push(filterOps, filterV1SingleValueFromList(params.Nodes, afilter.FieldNode))
	}

	if len(params.Namespaces) > 0 {
		filterOps = push(filterOps, filterV1SingleValueFromList(params.Namespaces, afilter.FieldNamespace))
	}

	if len(params.ControllerKinds) > 0 {
		filterOps = push(filterOps, filterV1SingleValueFromList(params.ControllerKinds, afilter.FieldControllerKind))
	}

	// filterControllers= accepts controllerkind:controllername filters, e.g.
	// "deployment:kubecost-cost-analyzer"
	//
	// Thus, we have to make a custom OR filter for this condition.
	if len(params.Controllers) > 0 {
		var ops []ast.FilterNode

		for _, rawFilterValue := range params.Controllers {
			split := strings.Split(rawFilterValue, ":")
			if len(split) == 1 {
				filterValue, wildcard := parseWildcardEnd(split[0])

				subFilter := toEqualOp(afilter.FieldControllerName, "", filterValue, wildcard)
				ops = append(ops, subFilter)
			} else if len(split) == 2 {
				kindFilterVal := split[0]
				nameFilterVal, wildcard := parseWildcardEnd(split[1])

				kindFilter := toEqualOp(afilter.FieldControllerKind, "", kindFilterVal, false)
				nameFilter := toEqualOp(afilter.FieldControllerName, "", nameFilterVal, wildcard)

				// The controller name AND the controller kind must match
				ops = append(ops, &ast.AndOp{
					Operands: []ast.FilterNode{
						kindFilter,
						nameFilter,
					},
				})
			} else {
				log.Warnf("illegal filter for controller: %s", rawFilterValue)
			}
		}
		controllersOp := opsToOr(ops)
		filterOps = push(filterOps, controllersOp)
	}

	if len(params.Pods) > 0 {
		filterOps = push(filterOps, filterV1SingleValueFromList(params.Pods, afilter.FieldPod))
	}

	if len(params.Containers) > 0 {
		filterOps = push(filterOps, filterV1SingleValueFromList(params.Containers, afilter.FieldContainer))
	}

	// Label-mapped queries require a label config to be present.
	if labelConfig != nil {
		if len(params.Departments) > 0 {
			filterOps = push(filterOps, filterV1LabelAliasMappedFromList(params.Departments, labelConfig.DepartmentLabel))
		}
		if len(params.Environments) > 0 {
			filterOps = push(filterOps, filterV1LabelAliasMappedFromList(params.Environments, labelConfig.EnvironmentLabel))
		}
		if len(params.Owners) > 0 {
			filterOps = push(filterOps, filterV1LabelAliasMappedFromList(params.Owners, labelConfig.OwnerLabel))
		}
		if len(params.Products) > 0 {
			filterOps = push(filterOps, filterV1LabelAliasMappedFromList(params.Products, labelConfig.ProductLabel))
		}
		if len(params.Teams) > 0 {
			filterOps = push(filterOps, filterV1LabelAliasMappedFromList(params.Teams, labelConfig.TeamLabel))
		}
	} else {
		log.Debugf("No label config is available. Not creating filters for label-mapped 'fields'.")
	}

	if len(params.Annotations) > 0 {
		filterOps = push(filterOps, filterV1DoubleValueFromList(params.Annotations, afilter.FieldAnnotation))
	}

	if len(params.Labels) > 0 {
		filterOps = push(filterOps, filterV1DoubleValueFromList(params.Labels, afilter.FieldLabel))
	}

	if len(params.Services) > 0 {
		var ops []ast.FilterNode

		// filterServices= is the only filter that uses the "contains" operator.
		for _, filterValue := range params.Services {
			// TODO: wildcard support
			filterValue, wildcard := parseWildcardEnd(filterValue)

			subFilter := toContainsOp(afilter.FieldServices, "", filterValue, wildcard)
			ops = append(ops, subFilter)
		}

		serviceOps := opsToOr(ops)
		filterOps = push(filterOps, serviceOps)
	}

	andFilter := opsToAnd(filterOps)
	if andFilter == nil {
		return &ast.VoidOp{} // no filter
	}

	return andFilter
}

func AllocationSharerFromParamsV1(params AllocationFilterV1) filter.Filter {
	var filterOps []ast.FilterNode

	if len(params.Namespaces) > 0 {
		filterOps = push(filterOps, filterV1SingleValueFromList(params.Namespaces, afilter.FieldNamespace))
	}

	if len(params.Labels) > 0 {
		filterOps = push(filterOps, filterV1DoubleValueFromList(params.Labels, afilter.FieldLabel))
	}

	return opsToAnd(filterOps)
}

func AssetFilterFromParamsV1(
	qp mapper.PrimitiveMapReader,
	clusterMap clusters.ClusterMap,
) filter.Filter {

	var filterOps []ast.FilterNode

	// ClusterMap does not provide a cluster name -> cluster ID mapping in the
	// interface, probably because there could be multiple IDs with the same
	// name. However, V1 filter logic demands that the parameters to
	// filterClusters= be checked against both cluster ID AND cluster name.
	//
	// To support expected filterClusters= behavior, we construct a mapping
	// of cluster name -> cluster IDs (could be multiple IDs for the same name)
	// so that we can create AllocationFilters that use only ClusterIDEquals.
	//
	//
	// AllocationFilter intentionally does not support cluster name filters
	// because those should be considered presentation-layer only.
	clusterNameToIDs := map[string][]string{}
	if clusterMap != nil {
		cMap := clusterMap.AsMap()
		for _, info := range cMap {
			if info == nil {
				continue
			}

			if _, ok := clusterNameToIDs[info.Name]; ok {
				clusterNameToIDs[info.Name] = append(clusterNameToIDs[info.Name], info.ID)
			} else {
				clusterNameToIDs[info.Name] = []string{info.ID}
			}
		}
	}

	// The proliferation of > 0 guards in the function is to avoid constructing
	// empty filter structs. While it is functionally equivalent to add empty
	// filter structs (they evaluate to true always) there could be overhead
	// when calling Matches() repeatedly for no purpose.

	if filterClusters := qp.GetList(ParamFilterClusters, ","); len(filterClusters) > 0 {
		var ops []ast.FilterNode

		// filter my cluster identifier
		ops = push(ops, filterV1SingleValueFromList(filterClusters, assetfilter.FieldClusterID))

		for _, rawFilterValue := range filterClusters {
			clusterNameFilter, wildcard := parseWildcardEnd(rawFilterValue)

			clusterIDsToFilter := []string{}
			for clusterName := range clusterNameToIDs {
				if wildcard && strings.HasPrefix(clusterName, clusterNameFilter) {
					clusterIDsToFilter = append(clusterIDsToFilter, clusterNameToIDs[clusterName]...)
				} else if !wildcard && clusterName == clusterNameFilter {
					clusterIDsToFilter = append(clusterIDsToFilter, clusterNameToIDs[clusterName]...)
				}
			}

			for _, clusterID := range clusterIDsToFilter {
				ops = append(ops, &ast.EqualOp{
					Left: ast.Identifier{
						Field: assetfilter.DefaultFieldByName(assetfilter.FieldClusterID),
						Key:   "",
					},
					Right: clusterID,
				})
			}
		}

		clustersOp := opsToOr(ops)
		filterOps = push(filterOps, clustersOp)
	}

	if raw := qp.GetList(ParamFilterAccounts, ","); len(raw) > 0 {
		filterOps = push(filterOps, filterV1SingleValueFromList(raw, assetfilter.FieldAccount))
	}

	if raw := qp.GetList(ParamFilterCategories, ","); len(raw) > 0 {
		filterOps = push(filterOps, filterV1SingleValueFromList(raw, assetfilter.FieldCategory))
	}

	if raw := qp.GetList(ParamFilterNames, ","); len(raw) > 0 {
		filterOps = push(filterOps, filterV1SingleValueFromList(raw, assetfilter.FieldName))
	}

	if raw := qp.GetList(ParamFilterProjects, ","); len(raw) > 0 {
		filterOps = push(filterOps, filterV1SingleValueFromList(raw, assetfilter.FieldProject))
	}

	if raw := qp.GetList(ParamFilterProviders, ","); len(raw) > 0 {
		filterOps = push(filterOps, filterV1SingleValueFromList(raw, assetfilter.FieldProvider))
	}

	if raw := GetList(ParamFilterProviderIDs, ParamFilterProviderIDsV2, qp); len(raw) > 0 {
		filterOps = push(filterOps, filterV1SingleValueFromList(raw, assetfilter.FieldProviderID))
	}

	if raw := qp.GetList(ParamFilterServices, ","); len(raw) > 0 {
		filterOps = push(filterOps, filterV1SingleValueFromList(raw, assetfilter.FieldService))
	}

	if raw := qp.GetList(ParamFilterTypes, ","); len(raw) > 0 {
		// Types have a special situation where we allow users to enter them
		// capitalized or uncapitalized
		for i := range raw {
			raw[i] = strings.ToLower(raw[i])
		}
		filterOps = push(filterOps, filterV1SingleValueFromList(raw, assetfilter.FieldType))
	}

	if raw := qp.GetList(ParamFilterLabels, ","); len(raw) > 0 {
		filterOps = push(filterOps, filterV1DoubleValueFromList(raw, assetfilter.FieldLabel))
	}

	if raw := qp.GetList(ParamFilterRegions, ","); len(raw) > 0 {
		filterOps = push(filterOps, filterV1SingleLabelKeyFromList(raw, "label_topology_kubernetes_io_region", assetfilter.FieldLabel))
	}

	andFilter := opsToAnd(filterOps)
	if andFilter == nil {
		return &ast.VoidOp{} // no filter
	}

	return andFilter
}

// CloudCostFilterFromParams creates a filter AST from the provided query parameter
// map for CloudCost.
func CloudCostFilterFromParams(pmr mapper.PrimitiveMapReader) filter.Filter {
	var filterOps []ast.FilterNode

	if raw := pmr.GetList(ParamFilterAccountIDs, ","); len(raw) > 0 {
		filterOps = append(filterOps, filterV1SingleValueFromList(raw, cloudcostfilter.FieldAccountID))
	}

	if raw := pmr.GetList(ParamFilterCategories, ","); len(raw) > 0 {
		filterOps = append(filterOps, filterV1SingleValueFromList(raw, cloudcostfilter.FieldCategory))
	}

	if raw := pmr.GetList(ParamFilterInvoiceEntityIDs, ","); len(raw) > 0 {
		filterOps = append(filterOps, filterV1SingleValueFromList(raw, cloudcostfilter.FieldInvoiceEntityID))
	}

	if raw := pmr.GetList(ParamFilterLabels, ","); len(raw) > 0 {
		filterOps = append(filterOps, filterV1DoubleValueFromList(raw, cloudcostfilter.FieldLabel))
	}

	if raw := pmr.GetList(ParamFilterProviders, ","); len(raw) > 0 {
		filterOps = append(filterOps, filterV1SingleValueFromList(raw, cloudcostfilter.FieldProvider))
	}

	if raw := pmr.GetList(ParamFilterProviderIDs, ","); len(raw) > 0 {
		filterOps = append(filterOps, filterV1SingleValueFromList(raw, cloudcostfilter.FieldProviderID))
	}

	if raw := pmr.GetList(ParamFilterServices, ","); len(raw) > 0 {
		filterOps = append(filterOps, filterV1SingleValueFromList(raw, cloudcostfilter.FieldService))
	}

	andFilter := opsToAnd(filterOps)
	if andFilter == nil {
		return &ast.VoidOp{} // no filter
	}

	return andFilter
}

// filterV1SingleValueFromList creates an OR of equality filters for a given
// filter field.
//
// The v1 query language (e.g. "filterNamespaces=XYZ,ABC") uses OR within
// a field (e.g. namespace = XYZ OR namespace = ABC)
func filterV1SingleValueFromList[T ~string](rawFilterValues []string, filterField T) ast.FilterNode {
	var ops []ast.FilterNode

	for _, filterValue := range rawFilterValues {
		filterValue = strings.TrimSpace(filterValue)
		filterValue, wildcard := parseWildcardEnd(filterValue)

		subFilter := toEqualOp(filterField, "", filterValue, wildcard)
		ops = append(ops, subFilter)
	}

	return opsToOr(ops)
}

func filterV1SingleLabelKeyFromList[T ~string](rawFilterValues []string, labelName string, labelField T) ast.FilterNode {
	var ops []ast.FilterNode
	labelName = promutil.SanitizeLabelName(labelName)

	for _, filterValue := range rawFilterValues {
		filterValue = strings.TrimSpace(filterValue)
		filterValue, wildcard := parseWildcardEnd(filterValue)

		subFilter := toEqualOp(labelField, labelName, filterValue, wildcard)

		ops = append(ops, subFilter)
	}

	return opsToOr(ops)
}

// filterV1LabelAliasMappedFromList is like filterV1SingleValueFromList but is
// explicitly for labels and annotations because "label-mapped" filters (like filterTeams=)
// are actually label filters with a fixed label key.
func filterV1LabelAliasMappedFromList(rawFilterValues []string, labelName string) ast.FilterNode {
	var ops []ast.FilterNode
	labelName = promutil.SanitizeLabelName(labelName)

	for _, filterValue := range rawFilterValues {
		filterValue = strings.TrimSpace(filterValue)
		filterValue, wildcard := parseWildcardEnd(filterValue)

		subFilter := toAllocationAliasOp(labelName, filterValue, wildcard)

		ops = append(ops, subFilter)
	}

	return opsToOr(ops)
}

// filterV1DoubleValueFromList creates an OR of key:value equality filters for
// colon-split filter values.
//
// The v1 query language (e.g. "filterLabels=app:foo,l2:bar") uses OR within
// a field (e.g. label[app] = foo OR label[l2] = bar)
func filterV1DoubleValueFromList[T ~string](rawFilterValuesUnsplit []string, filterField T) ast.FilterNode {
	var ops []ast.FilterNode

	for _, unsplit := range rawFilterValuesUnsplit {
		if unsplit != "" {
			split := strings.Split(unsplit, ":")
			if len(split) != 2 {
				log.Warnf("illegal key/value filter (ignoring): %s", unsplit)
				continue
			}
			labelName := promutil.SanitizeLabelName(strings.TrimSpace(split[0]))
			val := strings.TrimSpace(split[1])
			val, wildcard := parseWildcardEnd(val)

			subFilter := toEqualOp(filterField, labelName, val, wildcard)
			ops = append(ops, subFilter)
		}
	}

	return opsToOr(ops)
}

// parseWildcardEnd checks if the given filter value is wildcarded, meaning
// it ends in "*". If it does, it removes the suffix and returns the cleaned
// string and true. Otherwise, it returns the same filter and false.
//
// parseWildcardEnd("kube*") = "kube", true
// parseWildcardEnd("kube") = "kube", false
func parseWildcardEnd(rawFilterValue string) (string, bool) {
	return strings.TrimSuffix(rawFilterValue, "*"), strings.HasSuffix(rawFilterValue, "*")
}

func push(a []ast.FilterNode, item ast.FilterNode) []ast.FilterNode {
	if item == nil {
		return a
	}

	return append(a, item)
}

func opsToOr(ops []ast.FilterNode) ast.FilterNode {
	if len(ops) == 0 {
		return nil
	}

	if len(ops) == 1 {
		return ops[0]
	}

	return &ast.OrOp{
		Operands: ops,
	}
}

func opsToAnd(ops []ast.FilterNode) ast.FilterNode {
	if len(ops) == 0 {
		return nil
	}

	if len(ops) == 1 {
		return ops[0]
	}

	return &ast.AndOp{
		Operands: ops,
	}
}

func toEqualOp[T ~string](field T, key string, value string, wildcard bool) ast.FilterNode {
	left := ast.Identifier{
		Field: DefaultFieldByName(field),
		Key:   key,
	}
	right := value

	if wildcard {
		return &ast.ContainsPrefixOp{
			Left:  left,
			Right: right,
		}
	}

	return &ast.EqualOp{
		Left:  left,
		Right: right,
	}
}

func toContainsOp[T ~string](field T, key string, value string, wildcard bool) ast.FilterNode {
	left := ast.Identifier{
		Field: DefaultFieldByName(field),
		Key:   key,
	}
	right := value

	if wildcard {
		return &ast.ContainsPrefixOp{
			Left:  left,
			Right: right,
		}
	}

	return &ast.ContainsOp{
		Left:  left,
		Right: right,
	}
}

func toAllocationAliasOp(labelName string, filterValue string, wildcard bool) *ast.OrOp {
	// labels.Contains(labelName)
	labelContainsKey := &ast.ContainsOp{
		Left: ast.Identifier{
			Field: afilter.DefaultFieldByName(afilter.FieldLabel),
			Key:   "",
		},
		Right: labelName,
	}

	// annotations.Contains(labelName)
	annotationContainsKey := &ast.ContainsOp{
		Left: ast.Identifier{
			Field: afilter.DefaultFieldByName(afilter.FieldAnnotation),
			Key:   "",
		},
		Right: labelName,
	}

	// labels[labelName] equals/startswith filterValue
	var labelSubFilter ast.FilterNode
	if wildcard {
		labelSubFilter = &ast.ContainsPrefixOp{
			Left: ast.Identifier{
				Field: afilter.DefaultFieldByName(afilter.FieldLabel),
				Key:   labelName,
			},
			Right: filterValue,
		}
	} else {
		labelSubFilter = &ast.EqualOp{
			Left: ast.Identifier{
				Field: afilter.DefaultFieldByName(afilter.FieldLabel),
				Key:   labelName,
			},
			Right: filterValue,
		}
	}

	// annotations[labelName] equals/startswith filterValue
	var annotationSubFilter ast.FilterNode
	if wildcard {
		annotationSubFilter = &ast.ContainsPrefixOp{
			Left: ast.Identifier{
				Field: afilter.DefaultFieldByName(afilter.FieldAnnotation),
				Key:   labelName,
			},
			Right: filterValue,
		}
	} else {
		annotationSubFilter = &ast.EqualOp{
			Left: ast.Identifier{
				Field: afilter.DefaultFieldByName(afilter.FieldAnnotation),
				Key:   labelName,
			},
			Right: filterValue,
		}
	}

	// Logically, this is equivalent to:
	// (labels.Contains(labelName) && labels[labelName] = filterValue) ||
	// (!labels.Contains(labelName) && annotations.Contains(labelName) && annotations[labelName] = filterValue)

	return &ast.OrOp{
		Operands: []ast.FilterNode{
			&ast.AndOp{
				Operands: []ast.FilterNode{
					labelContainsKey,
					labelSubFilter,
				},
			},
			&ast.AndOp{
				Operands: []ast.FilterNode{
					&ast.NotOp{
						Operand: ast.Clone(labelContainsKey),
					},
					annotationContainsKey,
					annotationSubFilter,
				},
			},
		},
	}
}

// GetList provides a list of values from the first key if they exist, otherwise, it returns
// the values from the second key.
func GetList(primaryKey, secondaryKey string, qp mapper.PrimitiveMapReader) []string {
	if raw := qp.GetList(primaryKey, ","); len(raw) > 0 {
		return raw
	}

	return qp.GetList(secondaryKey, ",")
}
