package fieldstrings

// These strings are the central source of filter fields across all types of
// filters. Many filter types share fields; defining common consts means that
// there should be no drift between types.
const (
	FieldClusterID      string = "cluster"
	FieldNode           string = "node"
	FieldNamespace      string = "namespace"
	FieldControllerKind string = "controllerKind"
	FieldControllerName string = "controllerName"
	FieldPod            string = "pod"
	FieldContainer      string = "container"
	FieldProvider       string = "provider"
	FieldServices       string = "services"
	FieldLabel          string = "label"
	FieldAnnotation     string = "annotation"

	FieldName       string = "name"
	FieldType       string = "assetType"
	FieldCategory   string = "category"
	FieldProject    string = "project"
	FieldProviderID string = "providerID"
	FieldAccount    string = "account"
	FieldService    string = "service"

	FieldInvoiceEntityID   string = "invoiceEntityID"
	FieldInvoiceEntityName string = "invoiceEntityName"
	FieldAccountID         string = "accountID"
	FieldAccountName       string = "accountName"
	FieldRegionID          string = "regionID"
	FieldAvailabilityZone  string = "availabilityZone"

	AliasDepartment  string = "department"
	AliasEnvironment string = "environment"
	AliasOwner       string = "owner"
	AliasProduct     string = "product"
	AliasTeam        string = "team"
)
