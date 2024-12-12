package cloudcost

import (
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
)

func DefaultMockCloudCostSet(start, end time.Time, provider, integration string) *opencost.CloudCostSet {
	ccs := opencost.NewCloudCostSet(start, end)

	ccs.Integration = integration

	ccs.Insert(&opencost.CloudCost{
		Window: ccs.Window,
		Properties: &opencost.CloudCostProperties{
			Provider:        provider,
			AccountID:       "account1",
			InvoiceEntityID: "invoiceEntity1",
			Service:         provider + "-storage",
			Category:        opencost.StorageCategory,
			Labels: opencost.CloudCostLabels{
				"label1": "value1",
				"label2": "value2",
				"label3": "value3",
			},
			ProviderID: "id1",
		},
		ListCost: opencost.CostMetric{
			Cost:              100,
			KubernetesPercent: 0,
		},
		NetCost: opencost.CostMetric{
			Cost:              100,
			KubernetesPercent: 0,
		},
	})

	ccs.Insert(&opencost.CloudCost{
		Window: ccs.Window,
		Properties: &opencost.CloudCostProperties{
			Provider:        provider,
			AccountID:       "account1",
			InvoiceEntityID: "invoiceEntity1",
			Service:         provider + "-compute",
			Category:        opencost.ComputeCategory,
			Labels: opencost.CloudCostLabels{
				"label1": "value1",
				"label2": "value2",
				"label3": "value3",
			},
			ProviderID: "id2",
		},
		ListCost: opencost.CostMetric{
			Cost:              2000,
			KubernetesPercent: 1,
		},
		NetCost: opencost.CostMetric{
			Cost:              1800,
			KubernetesPercent: 1,
		},
	})

	ccs.Insert(&opencost.CloudCost{
		Window: ccs.Window,
		Properties: &opencost.CloudCostProperties{
			Provider:        provider,
			AccountID:       "account2",
			InvoiceEntityID: "invoiceEntity2",
			Service:         provider + "-compute",
			Category:        opencost.ComputeCategory,
			Labels: opencost.CloudCostLabels{
				"label1": "value1",
				"label2": "value2",
				"label3": "value3",
			},
			ProviderID: "id3",
		},
		ListCost: opencost.CostMetric{
			Cost:              8000,
			KubernetesPercent: 1,
		},
		NetCost: opencost.CostMetric{
			Cost:              8000,
			KubernetesPercent: 1,
		},
	})

	return ccs
}
