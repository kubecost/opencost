package azure

import (
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
)

type AzureStorageIntegration struct {
	AzureStorageBillingParser
}

func (asi *AzureStorageIntegration) GetCloudCost(start, end time.Time) (*opencost.CloudCostSetRange, error) {
	ccsr, err := opencost.NewCloudCostSetRange(start, end, opencost.AccumulateOptionDay, asi.Key())
	if err != nil {
		return nil, err
	}

	err = asi.ParseBillingData(start, end, func(abv *BillingRowValues) error {
		s := abv.Date
		e := abv.Date.Add(timeutil.Day)
		window := opencost.NewWindow(&s, &e)

		k8sPtc := 0.0
		if AzureIsK8s(abv.Tags) {
			k8sPtc = 1.0
		}

		providerID, _ := AzureSetProviderID(abv)
		// Create CloudCost
		// Using the NetCost as a 'placeholder' for Invoiced and Amortized Net costs now,
		// until we can revisit and spend the time to do the calculations correctly
		cc := &opencost.CloudCost{
			Properties: &opencost.CloudCostProperties{
				ProviderID:        providerID,
				Provider:          opencost.AzureProvider,
				AccountID:         abv.SubscriptionID,
				AccountName:       abv.SubscriptionName,
				InvoiceEntityID:   abv.InvoiceEntityID,
				InvoiceEntityName: abv.InvoiceEntityName,
				RegionID:          abv.Region,
				Service:           abv.Service,
				Category:          SelectAzureCategory(abv.MeterCategory),
				Labels:            abv.Tags,
			},
			Window: window,
			AmortizedNetCost: opencost.CostMetric{
				Cost:              abv.NetCost,
				KubernetesPercent: k8sPtc,
			},
			InvoicedCost: opencost.CostMetric{
				Cost:              abv.NetCost,
				KubernetesPercent: k8sPtc,
			},
			ListCost: opencost.CostMetric{
				Cost:              abv.Cost,
				KubernetesPercent: k8sPtc,
			},
			NetCost: opencost.CostMetric{
				Cost:              abv.NetCost,
				KubernetesPercent: k8sPtc,
			},
			// NOTE: on Azure, there is no "AmortizedCost" per se, so we use
			// AmortizedNetCost, or NetCost, instead.
			AmortizedCost: opencost.CostMetric{
				Cost:              abv.NetCost,
				KubernetesPercent: k8sPtc,
			},
		}

		ccsr.LoadCloudCost(cc)

		return nil
	})
	if err != nil {
		return nil, err
	}
	return ccsr, nil
}

// Check for the presence of k8s labels
func AzureIsK8s(labels map[string]string) bool {
	for key := range labels {
		if strings.HasPrefix(key, "aks-managed-") {
			return true
		}
		if strings.HasPrefix(key, "kubernetes.io-created-") {
			return true
		}
		if strings.HasPrefix(key, "k8s-azure-created-") {
			return true
		}
	}
	return false
}
