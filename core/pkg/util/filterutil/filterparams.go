package filterutil

import (
	"reflect"

	"github.com/opencost/opencost/core/pkg/filter/fieldstrings"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/mapper"
)

const (
	ParamFilterClusters        = "filterClusters"
	ParamFilterNodes           = "filterNodes"
	ParamFilterNamespaces      = "filterNamespaces"
	ParamFilterControllerKinds = "filterControllerKinds"
	ParamFilterControllers     = "filterControllers"
	ParamFilterPods            = "filterPods"
	ParamFilterContainers      = "filterContainers"

	ParamFilterDepartments  = "filterDepartments"
	ParamFilterEnvironments = "filterEnvironments"
	ParamFilterOwners       = "filterOwners"
	ParamFilterProducts     = "filterProducts"
	ParamFilterTeams        = "filterTeams"

	ParamFilterAnnotations = "filterAnnotations"
	ParamFilterLabels      = "filterLabels"
	ParamFilterServices    = "filterServices"

	ParamFilterAccountIDs       = "filterAccountIDs"
	ParamFilterInvoiceEntityIDs = "filterInvoiceEntityIDs"

	ParamFilterAccounts      = "filterAccounts"
	ParamFilterCategories    = "filterCategories"
	ParamFilterNames         = "filterNames"
	ParamFilterProjects      = "filterProjects"
	ParamFilterProviders     = "filterProviders"
	ParamFilterProviderIDs   = "filterProviderIDs"
	ParamFilterProviderIDsV2 = "filterProviderIds"
	ParamFilterRegions       = "filterRegions"
	ParamFilterTypes         = "filterTypes"
)

// ValidAssetFilterParams returns a list of all possible filter parameters
func ValidAssetFilterParams() []string {
	return []string{
		ParamFilterAccounts,
		ParamFilterCategories,
		ParamFilterClusters,
		ParamFilterLabels,
		ParamFilterNames,
		ParamFilterProjects,
		ParamFilterProviders,
		ParamFilterProviderIDs,
		ParamFilterProviderIDsV2,
		ParamFilterRegions,
		ParamFilterServices,
		ParamFilterTypes,
	}
}

// AllocationPropToV1FilterParamKey maps allocation string property
// representations to v1 filter param keys for legacy filter config support
// (e.g. reports). Example mapping: "cluster" -> "filterClusters"
var AllocationPropToV1FilterParamKey = map[string]string{
	opencost.AllocationClusterProp:        ParamFilterClusters,
	opencost.AllocationNodeProp:           ParamFilterNodes,
	opencost.AllocationNamespaceProp:      ParamFilterNamespaces,
	opencost.AllocationControllerProp:     ParamFilterControllers,
	opencost.AllocationControllerKindProp: ParamFilterControllerKinds,
	opencost.AllocationPodProp:            ParamFilterPods,
	opencost.AllocationLabelProp:          ParamFilterLabels,
	opencost.AllocationServiceProp:        ParamFilterServices,
	opencost.AllocationDepartmentProp:     ParamFilterDepartments,
	opencost.AllocationEnvironmentProp:    ParamFilterEnvironments,
	opencost.AllocationOwnerProp:          ParamFilterOwners,
	opencost.AllocationProductProp:        ParamFilterProducts,
	opencost.AllocationTeamProp:           ParamFilterTeams,
}

// Map to store Kubecost Asset property to Asset Filter types.
// AssetPropToV1FilterParamKey maps asset string property representations to v1
// filter param keys for legacy filter config support (e.g. reports). Example
// mapping: "category" -> "filterCategories"
var AssetPropToV1FilterParamKey = map[opencost.AssetProperty]string{
	opencost.AssetNameProp:       ParamFilterNames,
	opencost.AssetTypeProp:       ParamFilterTypes,
	opencost.AssetAccountProp:    ParamFilterAccounts,
	opencost.AssetCategoryProp:   ParamFilterCategories,
	opencost.AssetClusterProp:    ParamFilterClusters,
	opencost.AssetProjectProp:    ParamFilterProjects,
	opencost.AssetProviderProp:   ParamFilterProviders,
	opencost.AssetProviderIDProp: ParamFilterProviderIDs,
	opencost.AssetServiceProp:    ParamFilterServices,
}

var CloudPropToV1FilterParamKey = map[string]string{
	fieldstrings.FieldAccountID:       ParamFilterAccountIDs,
	fieldstrings.FieldCategory:        ParamFilterCategories,
	fieldstrings.FieldInvoiceEntityID: ParamFilterInvoiceEntityIDs,
	fieldstrings.FieldLabel:           ParamFilterLabels,
	fieldstrings.FieldProvider:        ParamFilterProviders,
	fieldstrings.FieldProviderID:      ParamFilterProviderIDs,
	fieldstrings.FieldService:         ParamFilterServices,
}

// AllHTTPParamKeys returns all HTTP GET parameters used for v1 filters. It is
// intended to help validate HTTP queries in handlers to help avoid e.g.
// spelling errors.
func AllHTTPParamKeys() []string {
	return []string{
		ParamFilterClusters,
		ParamFilterNodes,
		ParamFilterNamespaces,
		ParamFilterControllerKinds,
		ParamFilterControllers,
		ParamFilterPods,
		ParamFilterContainers,

		ParamFilterDepartments,
		ParamFilterEnvironments,
		ParamFilterOwners,
		ParamFilterProducts,
		ParamFilterTeams,

		ParamFilterAnnotations,
		ParamFilterLabels,
		ParamFilterServices,
	}
}

func ConvertFilterQueryParams(qp mapper.PrimitiveMapReader, labelConfig *opencost.LabelConfig) AllocationFilterV1 {
	filter := AllocationFilterV1{
		Annotations:     qp.GetList(ParamFilterAnnotations, ","),
		Containers:      qp.GetList(ParamFilterContainers, ","),
		Controllers:     qp.GetList(ParamFilterControllers, ","),
		ControllerKinds: qp.GetList(ParamFilterControllerKinds, ","),
		Clusters:        qp.GetList(ParamFilterClusters, ","),
		Labels:          qp.GetList(ParamFilterLabels, ","),
		Namespaces:      qp.GetList(ParamFilterNamespaces, ","),
		Nodes:           qp.GetList(ParamFilterNodes, ","),
		Pods:            qp.GetList(ParamFilterPods, ","),
		Services:        qp.GetList(ParamFilterServices, ","),
	}

	if labelConfig != nil {
		filter.Departments = qp.GetList(ParamFilterDepartments, ",")
		filter.Environments = qp.GetList(ParamFilterEnvironments, ",")
		filter.Owners = qp.GetList(ParamFilterOwners, ",")
		filter.Products = qp.GetList(ParamFilterProducts, ",")
		filter.Teams = qp.GetList(ParamFilterTeams, ",")
	} else {
		log.Debugf("No label config is available. Not creating filters for label-mapped 'fields'.")
	}

	return filter
}

type AllocationFilterV1 struct {
	Annotations     []string `json:"annotations,omitempty"`
	Containers      []string `json:"containers,omitempty"`
	Controllers     []string `json:"controllers,omitempty"`
	ControllerKinds []string `json:"controllerKinds,omitempty"`
	Clusters        []string `json:"clusters,omitempty"`
	Departments     []string `json:"departments,omitempty"`
	Environments    []string `json:"environments,omitempty"`
	Labels          []string `json:"labels,omitempty"`
	Namespaces      []string `json:"namespaces,omitempty"`
	Nodes           []string `json:"nodes,omitempty"`
	Owners          []string `json:"owners,omitempty"`
	Pods            []string `json:"pods,omitempty"`
	Products        []string `json:"products,omitempty"`
	Services        []string `json:"services,omitempty"`
	Teams           []string `json:"teams,omitempty"`
}

func (f AllocationFilterV1) Equals(that AllocationFilterV1) bool {
	return reflect.DeepEqual(f.Annotations, that.Annotations) &&
		reflect.DeepEqual(f.Containers, that.Containers) &&
		reflect.DeepEqual(f.Controllers, that.Controllers) &&
		reflect.DeepEqual(f.ControllerKinds, that.ControllerKinds) &&
		reflect.DeepEqual(f.Clusters, that.Clusters) &&
		reflect.DeepEqual(f.Departments, that.Departments) &&
		reflect.DeepEqual(f.Environments, that.Environments) &&
		reflect.DeepEqual(f.Labels, that.Labels) &&
		reflect.DeepEqual(f.Namespaces, that.Namespaces) &&
		reflect.DeepEqual(f.Nodes, that.Nodes) &&
		reflect.DeepEqual(f.Owners, that.Owners) &&
		reflect.DeepEqual(f.Pods, that.Pods) &&
		reflect.DeepEqual(f.Products, that.Products) &&
		reflect.DeepEqual(f.Services, that.Services) &&
		reflect.DeepEqual(f.Teams, that.Teams)
}
