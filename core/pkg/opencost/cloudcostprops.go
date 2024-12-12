package opencost

import (
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"

	"github.com/opencost/opencost/core/pkg/log"
	"golang.org/x/exp/maps"
)

type CloudCostProperty string

// IsLabel returns true if the allocation property has a label prefix
func (apt *CloudCostProperty) IsLabel() bool {
	return strings.HasPrefix(string(*apt), "label:")
}

// GetLabel returns the label string associated with the label property if it exists.
// Otherwise, empty string is returned.
func (apt *CloudCostProperty) GetLabel() string {
	if apt.IsLabel() {
		return strings.TrimSpace(strings.TrimPrefix(string(*apt), "label:"))
	}
	return ""
}

const (
	CloudCostInvoiceEntityIDProp   string = "invoiceEntityID"
	CloudCostInvoiceEntityNameProp string = "invoiceEntityName"
	CloudCostAccountIDProp         string = "accountID"
	CloudCostAccountNameProp       string = "accountName"
	CloudCostRegionIDProp          string = "regionID"
	CloudCostAvailabilityZoneProp  string = "availabilityZone"
	CloudCostProviderProp          string = "provider"
	CloudCostProviderIDProp        string = "providerID"
	CloudCostCategoryProp          string = "category"
	CloudCostServiceProp           string = "service"
	CloudCostLabelProp             string = "label"
	CloudCostLabelSetProp          string = "labelSet"
)

func ParseCloudProperties(props []string) ([]CloudCostProperty, error) {
	properties := []CloudCostProperty{}
	added := make(map[CloudCostProperty]struct{})

	for _, prop := range props {
		property, err := ParseCloudCostProperty(prop)
		if err != nil {
			return nil, fmt.Errorf("Failed to parse property: %w", err)
		}

		if _, ok := added[property]; !ok {
			added[property] = struct{}{}
			properties = append(properties, property)
		}
	}

	return properties, nil
}

func ParseCloudCostProperty(text string) (CloudCostProperty, error) {
	switch strings.TrimSpace(strings.ToLower(text)) {
	case "invoiceentityid":
		return CloudCostProperty(CloudCostInvoiceEntityIDProp), nil
	case "invoiceentityname":
		return CloudCostProperty(CloudCostInvoiceEntityNameProp), nil
	case "accountid":
		return CloudCostProperty(CloudCostAccountIDProp), nil
	case "accountname":
		return CloudCostProperty(CloudCostAccountNameProp), nil
	case "regionid":
		return CloudCostProperty(CloudCostRegionIDProp), nil
	case "availabilityzone":
		return CloudCostProperty(CloudCostAvailabilityZoneProp), nil
	case "provider":
		return CloudCostProperty(CloudCostProviderProp), nil
	case "providerid":
		return CloudCostProperty(CloudCostProviderIDProp), nil
	case "category":
		return CloudCostProperty(CloudCostCategoryProp), nil
	case "service":
		return CloudCostProperty(CloudCostServiceProp), nil
	}

	if strings.HasPrefix(text, "label:") {
		label := strings.TrimSpace(strings.TrimPrefix(text, "label:"))
		return CloudCostProperty(fmt.Sprintf("label:%s", label)), nil
	}

	return "", fmt.Errorf("invalid cloud cost property: %s", text)
}

const (
	// CloudCostClusterManagementCategory describes CloudCost representing Hosted Kubernetes Fees
	CloudCostClusterManagementCategory string = "Cluster Management"

	// CloudCostDiskCategory describes CloudCost representing Disk usage
	CloudCostDiskCategory string = "Disk"

	// CloudCostLoadBalancerCategory describes CloudCost representing Load Balancer usage
	CloudCostLoadBalancerCategory string = "Load Balancer"

	// CloudCostNetworkCategory describes CloudCost representing Network usage
	CloudCostNetworkCategory string = "Network"

	// CloudCostVirtualMachineCategory describes CloudCost representing VM usage
	CloudCostVirtualMachineCategory string = "Virtual Machine"

	// CloudCostOtherCategory describes CloudCost that do not belong to a defined category
	CloudCostOtherCategory string = "Other"
)

type CloudCostLabels map[string]string

func (ccl CloudCostLabels) Clone() CloudCostLabels {
	result := make(map[string]string, len(ccl))
	for k, v := range ccl {
		result[k] = v
	}
	return result
}

func (ccl CloudCostLabels) Equal(that CloudCostLabels) bool {
	if len(ccl) != len(that) {
		return false
	}

	// Maps are of equal length, so if all keys are in both maps, we don't
	// have to check the keys of the other map.
	for k, val := range ccl {
		if thatVal, ok := that[k]; !ok || val != thatVal {
			return false
		}
	}

	return true
}

// Intersection returns the set of labels that have the same key and value in the receiver and arg
func (ccl CloudCostLabels) Intersection(that CloudCostLabels) CloudCostLabels {
	intersection := make(map[string]string)
	if len(ccl) == 0 || len(that) == 0 {
		return intersection
	}

	// Pick the smaller of the two label sets
	smallerLabels := ccl
	largerLabels := that
	if len(ccl) > len(that) {
		smallerLabels = that
		largerLabels = ccl
	}

	// Loop through the smaller label set
	for k, sVal := range smallerLabels {
		if lVal, ok := largerLabels[k]; ok && sVal == lVal {
			intersection[k] = sVal
		}
	}
	return intersection
}

type CloudCostProperties struct {
	ProviderID        string          `json:"providerID,omitempty"`
	Provider          string          `json:"provider,omitempty"`
	AccountID         string          `json:"accountID,omitempty"`
	AccountName       string          `json:"accountName,omitempty"` // @bingen:field[version=3]
	InvoiceEntityID   string          `json:"invoiceEntityID,omitempty"`
	InvoiceEntityName string          `json:"invoiceEntityName,omitempty"` // @bingen:field[version=3]
	RegionID          string          `json:"regionID,omitempty"`          // @bingen:field[version=3]
	AvailabilityZone  string          `json:"availabilityZone,omitempty"`  // @bingen:field[version=3]
	Service           string          `json:"service,omitempty"`
	Category          string          `json:"category,omitempty"`
	Labels            CloudCostLabels `json:"labels,omitempty"`
}

func (ccp *CloudCostProperties) Equal(that *CloudCostProperties) bool {
	return ccp.ProviderID == that.ProviderID &&
		ccp.Provider == that.Provider &&
		ccp.AccountID == that.AccountID &&
		ccp.AccountName == that.AccountName &&
		ccp.InvoiceEntityID == that.InvoiceEntityID &&
		ccp.InvoiceEntityName == that.InvoiceEntityName &&
		ccp.RegionID == that.RegionID &&
		ccp.AvailabilityZone == that.AvailabilityZone &&
		ccp.Service == that.Service &&
		ccp.Category == that.Category &&
		ccp.Labels.Equal(that.Labels)
}

func (ccp *CloudCostProperties) Clone() *CloudCostProperties {
	return &CloudCostProperties{
		ProviderID:        ccp.ProviderID,
		Provider:          ccp.Provider,
		AccountID:         ccp.AccountID,
		AccountName:       ccp.AccountName,
		InvoiceEntityID:   ccp.InvoiceEntityID,
		InvoiceEntityName: ccp.InvoiceEntityName,
		RegionID:          ccp.RegionID,
		AvailabilityZone:  ccp.AvailabilityZone,
		Service:           ccp.Service,
		Category:          ccp.Category,
		Labels:            ccp.Labels.Clone(),
	}
}

// Intersection ensure the values of two CloudCostAggregateProperties are maintain only if they are equal
func (ccp *CloudCostProperties) Intersection(that *CloudCostProperties) *CloudCostProperties {
	if ccp == nil || that == nil {
		return nil
	}

	if ccp.Equal(that) {
		return ccp
	}
	intersectionCCP := &CloudCostProperties{}
	if ccp.Equal(intersectionCCP) || that.Equal(intersectionCCP) {
		return intersectionCCP
	}

	if ccp.Provider == that.Provider {
		intersectionCCP.Provider = ccp.Provider
	}
	if ccp.ProviderID == that.ProviderID {
		intersectionCCP.ProviderID = ccp.ProviderID
	}
	if ccp.AccountID == that.AccountID {
		intersectionCCP.AccountID = ccp.AccountID
	}
	if ccp.AccountName == that.AccountName {
		intersectionCCP.AccountName = ccp.AccountName
	}
	if ccp.InvoiceEntityID == that.InvoiceEntityID {
		intersectionCCP.InvoiceEntityID = ccp.InvoiceEntityID
	}
	if ccp.InvoiceEntityName == that.InvoiceEntityName {
		intersectionCCP.InvoiceEntityName = ccp.InvoiceEntityName
	}
	if ccp.RegionID == that.RegionID {
		intersectionCCP.RegionID = ccp.RegionID
	}
	if ccp.AvailabilityZone == that.AvailabilityZone {
		intersectionCCP.AvailabilityZone = ccp.AvailabilityZone
	}
	if ccp.Service == that.Service {
		intersectionCCP.Service = ccp.Service
	}
	if ccp.Category == that.Category {
		intersectionCCP.Category = ccp.Category
	}
	intersectionCCP.Labels = ccp.Labels.Intersection(that.Labels)

	return intersectionCCP
}

// GenerateKey takes a list of properties and creates a "/" seperated key based on the values of the requested properties.
// Invalid values are ignored with a warning. A nil input returns the default key, while an empty slice  returns the empty string
func (ccp *CloudCostProperties) GenerateKey(props []string) string {

	// nil props replaced with default property list
	if props == nil {
		return ccp.hashKey()
	}

	values := make([]string, len(props))
	for i, prop := range props {
		propVal := UnallocatedSuffix

		switch true {
		case prop == CloudCostProviderProp:
			if ccp.Provider != "" {
				propVal = ccp.Provider
			}
		case prop == CloudCostProviderIDProp:
			if ccp.ProviderID != "" {
				propVal = ccp.ProviderID
			}
		case prop == CloudCostCategoryProp:
			if ccp.Category != "" {
				propVal = ccp.Category
			}
		case prop == CloudCostInvoiceEntityIDProp:
			if ccp.InvoiceEntityID != "" {
				propVal = ccp.InvoiceEntityID
			}
		case prop == CloudCostInvoiceEntityNameProp:
			if ccp.InvoiceEntityName != "" {
				propVal = ccp.InvoiceEntityName
			}
		case prop == CloudCostAccountIDProp:
			if ccp.AccountID != "" {
				propVal = ccp.AccountID
			}
		case prop == CloudCostAccountNameProp:
			if ccp.AccountName != "" {
				propVal = ccp.AccountName
			}
		case prop == CloudCostRegionIDProp:
			if ccp.RegionID != "" {
				propVal = ccp.RegionID
			}
		case prop == CloudCostAvailabilityZoneProp:
			if ccp.AvailabilityZone != "" {
				propVal = ccp.AvailabilityZone
			}
		case prop == CloudCostServiceProp:
			if ccp.Service != "" {
				propVal = ccp.Service
			}
		case strings.HasPrefix(prop, "label:"):
			labels := ccp.Labels
			if labels != nil {
				labelName := strings.TrimPrefix(prop, "label:")
				if labelValue, ok := labels[labelName]; ok && labelValue != "" {
					propVal = labelValue
				}
			}
		default:
			// This case should never be reached, as input up until this point
			// should be checked and rejected if invalid. But if we do get a
			// value we don't recognize, log a warning.
			log.Warnf("CloudCost: GenerateKey: illegal aggregation parameter: %s", prop)

		}

		values[i] = propVal
	}

	return strings.Join(values, "/")
}

// HashKey creates a key on the entire property set including labels of a uniform length.
// This key is meant to be used when constructing unaggregated CloudCostSet for storage.
// Including labels prevents CloudCosts that are missing providerIDs from having their labels
// erased as they are saved to cloud cost set.
func (ccp *CloudCostProperties) hashKey() string {
	builder := strings.Builder{}
	builder.WriteString(ccp.ProviderID)
	builder.WriteString(ccp.Provider)
	builder.WriteString(ccp.AccountID)
	builder.WriteString(ccp.AccountName)
	builder.WriteString(ccp.InvoiceEntityID)
	builder.WriteString(ccp.InvoiceEntityName)
	builder.WriteString(ccp.RegionID)
	builder.WriteString(ccp.AvailabilityZone)
	builder.WriteString(ccp.Service)
	builder.WriteString(ccp.Category)

	// Sort label keys before adding key/value pairs to the hash string to ensure label set is
	// always returns the same key
	labelKeys := maps.Keys(ccp.Labels)
	sort.Strings(labelKeys)
	for _, k := range labelKeys {
		builder.WriteString(k)
		builder.WriteString(ccp.Labels[k])
	}
	hasher := fnv.New64a()
	hasher.Write([]byte(builder.String()))
	return hex.EncodeToString(hasher.Sum(nil))
}
