package azure

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/json"
)

const azureDateLayout = "2006-01-02"
const AzureEnterpriseDateLayout = "01/02/2006"

var groupRegex = regexp.MustCompile("(/[^/]+)")

// BillingRowValues holder for Azure Billing Values
type BillingRowValues struct {
	Date              time.Time
	MeterCategory     string
	SubscriptionID    string
	SubscriptionName  string
	InvoiceEntityID   string
	InvoiceEntityName string
	Region            string
	InstanceID        string
	Service           string
	Tags              map[string]string
	AdditionalInfo    map[string]any
	Cost              float64
	NetCost           float64
}

func (brv *BillingRowValues) IsCompute(category string) bool {
	if category == opencost.ComputeCategory {
		return true
	}

	if category == opencost.StorageCategory || category == opencost.NetworkCategory {
		if brv.Service == "Microsoft.Compute" {
			return true
		}
	}
	if category == opencost.NetworkCategory && brv.MeterCategory == "Virtual Network" {
		return true
	}
	if category == opencost.NetworkCategory && brv.MeterCategory == "Bandwidth" {
		return true
	}
	return false
}

// BillingExportParser holds indexes of relevent fields in Azure Billing CSV in addition to the correct data format
type BillingExportParser struct {
	Date              int
	MeterCategory     int
	InvoiceEntityID   int
	InvoiceEntityName int
	SubscriptionID    int
	SubscriptionName  int
	Region            int
	InstanceID        int
	Service           int
	Tags              int
	AdditionalInfo    int
	Cost              int
	NetCost           int
	DateFormat        string
}

// match "SubscriptionGuid" in "Abonnement-GUID (SubscriptionGuid)"
var getParenContentRegEx = regexp.MustCompile("\\((.*?)\\)")

func NewBillingParseSchema(headers []string) (*BillingExportParser, error) {
	// clear BOM from headers
	if len(headers) != 0 {
		headers[0] = strings.TrimPrefix(headers[0], "\xEF\xBB\xBF")
	}

	headerIndexes := map[string]int{}
	for i, header := range headers {
		// Azure Headers in different regions will have english headers in parentheses
		match := getParenContentRegEx.FindStringSubmatch(header)
		if len(match) != 0 {
			header = match[len(match)-1]
		}
		headerIndexes[strings.ToLower(header)] = i
	}

	abp := &BillingExportParser{}

	// Set Date Column and Date Format
	if i, ok := headerIndexes["usagedatetime"]; ok {
		abp.Date = i
		abp.DateFormat = azureDateLayout
	} else if j, ok2 := headerIndexes["date"]; ok2 {
		abp.Date = j
		abp.DateFormat = AzureEnterpriseDateLayout
	} else {
		return nil, fmt.Errorf("NewBillingParseSchema: failed to find Date field")
	}

	// set Subscription ID
	if i, ok := headerIndexes["subscriptionid"]; ok {
		abp.SubscriptionID = i
	} else if j, ok2 := headerIndexes["subscriptionguid"]; ok2 {
		abp.SubscriptionID = j
	} else {
		return nil, fmt.Errorf("NewBillingParseSchema: failed to find Subscription ID field")
	}

	// set Subscription Name
	if i, ok := headerIndexes["subscriptionname"]; ok {
		abp.SubscriptionName = i
	} else {
		// if no subscription name column use subscriptionID column
		abp.SubscriptionName = abp.SubscriptionID
	}

	// Set Billing ID
	if i, ok := headerIndexes["billingaccountid"]; ok {
		abp.InvoiceEntityID = i
	} else if j, ok2 := headerIndexes["billingaccountname"]; ok2 {
		abp.InvoiceEntityID = j
	} else {
		// if no billing ID column is present use subscription ID
		abp.InvoiceEntityID = abp.SubscriptionID
	}

	// Set Billing Account Name
	if i, ok := headerIndexes["billingaccountname"]; ok {
		abp.InvoiceEntityName = i
	} else {
		// if no billing name column is present use billing ID index
		abp.InvoiceEntityName = abp.InvoiceEntityID
	}

	// Set Region
	if i, ok := headerIndexes["resourcelocation"]; ok {
		abp.Region = i
	} else if j, ok2 := headerIndexes["meterregion"]; ok2 {
		abp.Region = j
	} else if k, ok3 := headerIndexes["location"]; ok3 {
		abp.Region = k
	} else {
		return nil, fmt.Errorf("NewBillingParseSchema: failed to find Region field")
	}

	// Set Instance ID
	if i, ok := headerIndexes["instanceid"]; ok {
		abp.InstanceID = i
	} else if j, ok2 := headerIndexes["instancename"]; ok2 {
		abp.InstanceID = j
	} else if k, ok3 := headerIndexes["resourceid"]; ok3 {
		abp.InstanceID = k
	} else {
		return nil, fmt.Errorf("NewBillingParseSchema: failed to find Instance ID field")
	}

	// Set Meter Category
	if i, ok := headerIndexes["metercategory"]; ok {
		abp.MeterCategory = i
	} else {
		return nil, fmt.Errorf("NewBillingParseSchema: failed to find Meter Category field")
	}

	// Set Tags
	if i, ok := headerIndexes["tags"]; ok {
		abp.Tags = i
	} else {
		return nil, fmt.Errorf("NewBillingParseSchema: failed to find Tags field")
	}

	// Set Additional Info
	if i, ok := headerIndexes["additionalinfo"]; ok {
		abp.AdditionalInfo = i
	} else {
		return nil, fmt.Errorf("NewBillingParseSchema: failed to find Additional Info field")
	}

	// Set Service
	if i, ok := headerIndexes["consumedservice"]; ok {
		abp.Service = i
	} else {
		return nil, fmt.Errorf("NewBillingParseSchema: failed to find Service field")
	}

	// Set Net Cost
	if i, ok := headerIndexes["costinbillingcurrency"]; ok {
		abp.NetCost = i
	} else if j, ok2 := headerIndexes["pretaxcost"]; ok2 {
		abp.NetCost = j
	} else if k, ok3 := headerIndexes["cost"]; ok3 {
		abp.NetCost = k
	} else {
		return nil, fmt.Errorf("NewBillingParseSchema: failed to find Net Cost field")
	}

	// Set Cost
	if i, ok := headerIndexes["paygcostinbillingcurrency"]; ok {
		abp.Cost = i
	} else {
		// if no Cost column is present use Net Cost column
		abp.Cost = abp.NetCost
	}

	return abp, nil
}

func (bep *BillingExportParser) ParseRow(start, end time.Time, record []string) *BillingRowValues {
	usageDate, err := time.Parse(bep.DateFormat, record[bep.Date])
	if err != nil {
		// try other format, and switch if successful
		if bep.DateFormat == azureDateLayout {
			bep.DateFormat = AzureEnterpriseDateLayout
		} else {
			bep.DateFormat = azureDateLayout
		}
		usageDate, err = time.Parse(bep.DateFormat, record[bep.Date])
		// If parse still fails then return line
		if err != nil {
			log.Errorf("failed to parse usage date: '%s'", record[bep.Date])
			return nil
		}
	}

	// skip if usage data isn't in subject window
	if usageDate.Before(start) || !usageDate.Before(end) {
		return nil
	}

	cost, err := strconv.ParseFloat(record[bep.Cost], 64)
	if err != nil {
		log.Errorf("failed to parse cost: '%s'", record[bep.Cost])
		return nil
	}

	netCost, err := strconv.ParseFloat(record[bep.NetCost], 64)
	if err != nil {
		log.Errorf("failed to parse net cost: '%s'", record[bep.NetCost])
		return nil
	}

	additionalInfo := make(map[string]any)
	additionalInfoJson := encloseInBrackets(record[bep.AdditionalInfo])
	if additionalInfoJson != "" {
		err = json.Unmarshal([]byte(additionalInfoJson), &additionalInfo)
		if err != nil {
			log.Errorf("Could not parse additional information %s, with Error: %s", additionalInfoJson, err.Error())
		}
	}

	tags := make(map[string]string)
	tagJson := encloseInBrackets(record[bep.Tags])
	if tagJson != "" {
		tagsAny := make(map[string]any)
		err = json.Unmarshal([]byte(tagJson), &tagsAny)
		if err != nil {
			log.Errorf("Could not parse tags: %v, with Error: %s", tagJson, err.Error())
		}

		for name, value := range tagsAny {
			if valueStr, ok := value.(string); ok && valueStr != "" {
				tags[name] = valueStr
			}
		}
	}

	return &BillingRowValues{
		Date:              usageDate,
		MeterCategory:     record[bep.MeterCategory],
		SubscriptionID:    record[bep.SubscriptionID],
		SubscriptionName:  record[bep.SubscriptionName],
		InvoiceEntityID:   record[bep.InvoiceEntityID],
		InvoiceEntityName: record[bep.InvoiceEntityName],
		Region:            record[bep.Region],
		InstanceID:        record[bep.InstanceID],
		Service:           record[bep.Service],
		Tags:              tags,
		AdditionalInfo:    additionalInfo,
		Cost:              cost,
		NetCost:           netCost,
	}
}

// enclose json strings in brackets if they are missing
func encloseInBrackets(jsonString string) string {
	if jsonString == "" || (jsonString[0] == '{' && jsonString[len(jsonString)-1] == '}') {
		return jsonString
	}
	return fmt.Sprintf("{%s}", jsonString)
}

// isVMSSShared represents a bool that lets you know while setting providerID we were
// able to get the actual VMName associated with a VM of a group of VMs in VMSS.
func AzureSetProviderID(abv *BillingRowValues) (providerID string, isVMSSShared bool) {
	category := SelectAzureCategory(abv.MeterCategory)
	if value, ok := abv.AdditionalInfo["VMName"]; ok {
		return "azure://" + resourceGroupToLowerCase(abv.InstanceID) + getVMNumberForVMSS(fmt.Sprintf("%v", value)), false
	} else if value, ok := abv.AdditionalInfo["VmName"]; ok {
		return "azure://" + resourceGroupToLowerCase(abv.InstanceID) + getVMNumberForVMSS(fmt.Sprintf("%v", value)), false
	} else if value2, ook := abv.AdditionalInfo["IpAddress"]; ook && abv.MeterCategory == "Virtual Network" {
		return fmt.Sprintf("%v", value2), false
	}

	if category == opencost.StorageCategory || (category == opencost.NetworkCategory && abv.MeterCategory == "Bandwidth") {
		if value2, ok2 := abv.Tags["creationSource"]; ok2 {
			creationSource := fmt.Sprintf("%v", value2)
			return strings.TrimPrefix(creationSource, "aks-"), true
		} else if value2, ok2 := abv.Tags["aks-managed-creationSource"]; ok2 {
			creationSource := fmt.Sprintf("%v", value2)
			return strings.TrimPrefix(creationSource, "vmssclient-"), true
		} else {
			return getSubStringAfterFinalSlash(abv.InstanceID), true
		}
	}
	return "azure://" + resourceGroupToLowerCase(abv.InstanceID), true
}

func SelectAzureCategory(meterCategory string) string {
	if meterCategory == "Virtual Machines" || meterCategory == "Virtual Machines Licenses" {
		return opencost.ComputeCategory
	} else if meterCategory == "Storage" {
		return opencost.StorageCategory
	} else if meterCategory == "Load Balancer" || meterCategory == "Bandwidth" || meterCategory == "Virtual Network" {
		return opencost.NetworkCategory
	} else {
		return opencost.OtherCategory
	}
}

func resourceGroupToLowerCase(providerID string) string {
	var sb strings.Builder
	for matchNum, group := range groupRegex.FindAllString(providerID, -1) {
		if matchNum == 3 {
			sb.WriteString(strings.ToLower(group))
		} else {
			sb.WriteString(group)
		}
	}
	return sb.String()
}

// Returns the substring after the final "/" in a string
func getSubStringAfterFinalSlash(id string) string {
	index := strings.LastIndex(id, "/")
	if index == -1 {
		log.DedupedInfof(5, "azure.getSubStringAfterFinalSlash: failed to parse %s", id)
		return id
	}
	return id[index+1:]
}

func getVMNumberForVMSS(vmName string) string {
	vmNameSplit := strings.Split(vmName, "_")
	if len(vmNameSplit) > 1 {
		return "/virtualMachines/" + vmNameSplit[1]
	}
	return ""
}
