package alibaba

import (
	"fmt"
	"strings"

	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/requests"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/bssopenapi"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/pkg/cloud"
)

const (
	boaIsNode    = "i-"    // isNode if prefix of instance_id is i-
	boaIsDisk    = "d-"    // isDisk if prefix is disk is d-
	boaIsNetwork = "piece" //usage unit of network resource in Alibaba is Piece
)

type BoaQuerier struct {
	BOAConfiguration
	ConnectionStatus cloud.ConnectionStatus
}

func (bq *BoaQuerier) GetStatus() cloud.ConnectionStatus {
	// initialize status if it has not done so; this can happen if the integration is inactive
	if bq.ConnectionStatus.String() == "" {
		bq.ConnectionStatus = cloud.InitialStatus
	}
	return bq.ConnectionStatus
}

func (bq *BoaQuerier) Equals(config cloud.Config) bool {
	thatConfig, ok := config.(*BoaQuerier)
	if !ok {
		return false
	}

	return bq.BOAConfiguration.Equals(&thatConfig.BOAConfiguration)
}

// QueryInstanceBill performs the request to the BSS client and get the response for the current page number
func (bq *BoaQuerier) QueryInstanceBill(client *bssopenapi.Client, isBillingItem bool, invocationScheme, granularity, billingCycle, billingDate string, pageNum int) (*bssopenapi.QueryInstanceBillResponse, error) {
	log.Debugf("QueryInstanceBill: query for BSS Open API for billing date: %s with pageNum: %d ", billingDate, pageNum)
	request := bssopenapi.CreateQueryInstanceBillRequest()
	request.Scheme = invocationScheme
	request.BillingCycle = billingCycle
	request.IsBillingItem = requests.NewBoolean(true)
	request.Granularity = granularity
	request.BillingDate = billingDate
	request.PageNum = requests.NewInteger(pageNum)
	response, err := client.QueryInstanceBill(request)
	if err != nil {
		return nil, fmt.Errorf("QueryInstanceBill: Failed to hit the BSS Open API with error for page num %d: %v", pageNum, err)
	}
	log.Debugf("QueryInstanceBill: Total Number of total items for billing Date: %s pageNum: %d is %d", billingDate, pageNum, response.Data.TotalCount)
	return response, nil
}

// QueryBoaPaginated Calls the API in a paginated fashion. There's no paramter in API that can distinguish if it hasMorePages
// hence the logic of processedItem <= TotalItem.
func (bq *BoaQuerier) QueryBoaPaginated(client *bssopenapi.Client, isBillingItem bool, invocationScheme, granularity, billingCycle, billingDate string, fn func(*bssopenapi.QueryInstanceBillResponse) bool) error {
	pageNum := 1
	processedItem := 0 // setting default here to hit the API for the first time
	totalItem := 1
	for processedItem < totalItem {
		log.Debugf("QueryBoaPaginated: query for BSS Open API for billing date: %s with pageNum: %d", billingDate, pageNum)
		response, err := bq.QueryInstanceBill(client, isBillingItem, invocationScheme, granularity, billingCycle, billingDate, pageNum)
		if err != nil {
			return fmt.Errorf("QueryBoaPaginated for billing cycle : %s, billing date: %s, page num %d: %v", billingCycle, billingDate, pageNum, err)
		}
		fn(response)
		totalItem = response.Data.TotalCount
		processedItem += response.Data.PageSize
		pageNum += 1
	}
	return nil
}

// GetBoaQueryInstanceBillFunc gives the item to the handler function in boaIntegration.go to process
// computeItem, topNItem and aggregatedItem
func GetBoaQueryInstanceBillFunc(fn func(bssopenapi.Item) error, billingDate string) func(output *bssopenapi.QueryInstanceBillResponse) bool {
	processBOAItems := func(output *bssopenapi.QueryInstanceBillResponse) bool {
		// This could be connection error were unable to fetch response output from Client
		if output == nil {
			log.Errorf("BoaQuerier: No Response from the ALibaba BSS Open API client for billing Date: %s", billingDate)
			return false
		}

		// These infer that the rest call was successful but the Cloud Usage resource for those days were 0
		if output.Data.TotalCount == 0 {
			log.Warnf("BoaQuerier: Total Item Count is 0 for billing Date: %s ", billingDate)
			return false
		}

		for _, item := range output.Data.Items.Item {
			fn(item)
		}
		return true
	}
	return processBOAItems
}

// SelectAlibabaCategory processes the Alibaba service to associated Kubecost category
func SelectAlibabaCategory(item bssopenapi.Item) string {
	if (item != bssopenapi.Item{}) {
		// Provider ID has prefix "i-" for node in Alibaba
		if strings.HasPrefix(item.InstanceID, boaIsNode) {
			return opencost.ComputeCategory
		}
		// Provider ID for disk start with "d-" for storage type in Alibaba
		if strings.HasPrefix(item.InstanceID, boaIsDisk) {
			return opencost.StorageCategory
		}
		// Network has the highest priority and is based on the usage type of "piece" in Alibaba
		if item.UsageUnit == boaIsNetwork {
			return opencost.NetworkCategory
		}
	}

	// Alibaba CUR integration report has service lower case mostly unlike AWS
	// TO-DO: Can investigate further product codes but bare minimal differentiation for start
	switch strings.ToLower(item.ProductCode) {
	case "slb", "eip", "nis", "gtm":
		return opencost.NetworkCategory
	case "ecs", "eds", "sas":
		return opencost.ComputeCategory
	case "ack":
		return opencost.ManagementCategory
	case "ebs", "oss", "scu":
		return opencost.StorageCategory
	default:
		return opencost.OtherCategory
	}
}
