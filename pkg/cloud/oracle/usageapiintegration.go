package oracle

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/pkg/cloud"
	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/usageapi"
)

type UsageApiIntegration struct {
	UsageApiConfiguration
	ConnectionStatus cloud.ConnectionStatus
}

func (uai *UsageApiIntegration) GetCloudCost(start time.Time, end time.Time) (*opencost.CloudCostSetRange, error) {
	client, err := uai.GetUsageApiClient()
	if err != nil {
		uai.ConnectionStatus = cloud.FailedConnection
		return nil, fmt.Errorf("getting oracle usage api client: %s", err.Error())
	}

	req := usageapi.RequestSummarizedUsagesRequest{
		RequestSummarizedUsagesDetails: usageapi.RequestSummarizedUsagesDetails{
			Granularity:       usageapi.RequestSummarizedUsagesDetailsGranularityDaily,
			GroupBy:           []string{"resourceId", "service", "subscriptionId", "tenantName"},
			IsAggregateByTime: common.Bool(false),
			TimeUsageStarted:  &common.SDKTime{Time: start},
			TimeUsageEnded:    &common.SDKTime{Time: end},
			QueryType:         usageapi.RequestSummarizedUsagesDetailsQueryTypeCost,
			TenantId:          common.String(uai.TenancyID),
		},
		Limit: common.Int(500),
	}

	resp, err := client.RequestSummarizedUsages(context.Background(), req)
	if err != nil {
		uai.ConnectionStatus = cloud.FailedConnection
		return nil, fmt.Errorf("failed to query usage: %w", err)
	}

	ccsr, err := opencost.NewCloudCostSetRange(start, end, opencost.AccumulateOptionDay, uai.Key())
	if err != nil {
		return nil, err
	}

	// Set status to missing data if query comes back empty and the status isn't already successful
	if len(resp.Items) == 0 && uai.ConnectionStatus != cloud.SuccessfulConnection {
		uai.ConnectionStatus = cloud.MissingData
		return ccsr, nil
	}

	for _, item := range resp.Items {
		resourceId := ""
		if item.ResourceId != nil {
			resourceId = *item.ResourceId
		}

		tenantName := ""
		if item.TenantName != nil {
			tenantName = *item.TenantName
		}

		subscriptionId := ""
		if item.SubscriptionId != nil {
			subscriptionId = *item.SubscriptionId
		}

		service := ""
		if item.Service != nil {
			service = *item.Service
		}

		category := SelectOCICategory(service)

		// Iterate through the slice of tags, assigning
		// keys and values to the map of labels
		labels := opencost.CloudCostLabels{}
		for _, tag := range item.Tags {
			if tag.Key == nil || tag.Value == nil {
				continue
			}
			labels[*tag.Key] = *tag.Value
		}

		properties := &opencost.CloudCostProperties{
			ProviderID:      resourceId,
			Provider:        opencost.OracleProvider,
			AccountID:       uai.TenancyID,
			AccountName:     tenantName,
			InvoiceEntityID: subscriptionId,
			RegionID:        uai.Region,
			Service:         service,
			Category:        category,
			Labels:          labels,
		}

		winStart := item.TimeUsageStarted.Time
		winEnd := start.AddDate(0, 0, 1)

		listRate := 0.0
		if item.ListRate != nil {
			listRate = float64(*item.ListRate)
		}

		attrCostToParse := ""
		if item.AttributedCost != nil {
			attrCostToParse = *item.AttributedCost
		}

		attrCost, err := strconv.ParseFloat(attrCostToParse, 64)
		if err != nil {
			return nil, fmt.Errorf("unable to parse float '%s': %s", attrCostToParse, err.Error())
		}

		computedAmt := 0.0
		if item.ComputedAmount != nil {
			computedAmt = float64(*item.ComputedAmount)
		}

		cc := &opencost.CloudCost{
			Properties: properties,
			Window:     opencost.NewWindow(&winStart, &winEnd),
			//todo: which returned costs go where?
			ListCost: opencost.CostMetric{
				Cost: listRate,
			},
			NetCost: opencost.CostMetric{
				Cost: computedAmt,
			},
			AmortizedNetCost: opencost.CostMetric{
				Cost: attrCost,
			},
			AmortizedCost: opencost.CostMetric{
				Cost: attrCost,
			},
			InvoicedCost: opencost.CostMetric{
				Cost: computedAmt,
			},
		}

		ccsr.LoadCloudCost(cc)
	}

	uai.ConnectionStatus = cloud.SuccessfulConnection
	return ccsr, nil
}

func (uai *UsageApiIntegration) GetStatus() cloud.ConnectionStatus {
	// initialize status if it has not done so; this can happen if the integration is inactive
	if uai.ConnectionStatus.String() == "" {
		uai.ConnectionStatus = cloud.InitialStatus
	}
	return uai.ConnectionStatus
}

func SelectOCICategory(service string) string {
	if service == "Compute" {
		return opencost.ComputeCategory
	} else if service == "Block Storage" || service == "Object Storage" {
		return opencost.StorageCategory
	} else if service == "Load Balancer" || service == "Virtual Cloud Network" {
		return opencost.NetworkCategory
	} else {
		return opencost.OtherCategory
	}
}
