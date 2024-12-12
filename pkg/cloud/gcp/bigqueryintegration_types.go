package gcp

import (
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
)

type CloudCostLoader struct {
	CloudCost        *opencost.CloudCost
	FlexibleCUDRates map[time.Time]FlexibleCUDRates
}

// Load populates the fields of a CloudCostValues with bigquery.Value from provided slice
func (ccl *CloudCostLoader) Load(values []bigquery.Value, schema bigquery.Schema) error {

	// Create Cloud Cost Properties
	properties := opencost.CloudCostProperties{
		Provider: opencost.GCPProvider,
	}
	var window opencost.Window
	var description string
	var cost float64
	var listCost float64
	var creditAmount float64
	var cudCreditAmount float64
	var flexibleCUDCreditAmount float64

	for i, field := range schema {
		if field == nil {
			log.DedupedErrorf(5, "GCP: BigQuery: found nil field in schema")
			continue
		}

		// ignore nil values
		if values[i] == nil {
			continue
		}

		switch field.Name {
		case UsageDateColumnName:
			usageDate, ok := values[i].(time.Time)
			if !ok {
				// It would be very surprising if an unparsable time came back from the API, so it should be ok to return here.
				return fmt.Errorf("error parsing usage date: %v", values[0])
			}
			// start and end will be the day that the usage occurred on
			s := usageDate
			e := s.Add(timeutil.Day)
			window = opencost.NewClosedWindow(s, e)
		case BillingAccountIDColumnName:
			invoiceEntityID, ok := values[i].(string)
			if !ok {
				log.DedupedErrorf(5, "error parsing GCP CloudCost %s: %v", BillingAccountIDColumnName, values[i])
				invoiceEntityID = ""
			}
			properties.InvoiceEntityID = invoiceEntityID
			// Use InvoiceEntityID as InvoiceEntityName
			properties.InvoiceEntityName = invoiceEntityID
		case ProjectIDColumnName:
			accountID, ok := values[i].(string)
			if !ok {
				log.DedupedErrorf(5, "error parsing GCP CloudCost %s: %v", ProjectIDColumnName, values[i])
				accountID = ""
			}
			properties.AccountID = accountID
		case ProjectNameColumnName:
			accountName, ok := values[i].(string)
			if !ok {
				log.DedupedErrorf(5, "error parsing GCP CloudCost %s: %v", ProjectNameColumnName, values[i])
				accountName = ""
			}
			properties.AccountName = accountName
		case RegionColumnName:
			regionID, ok := values[i].(string)
			if !ok {
				log.DedupedErrorf(5, "error parsing GCP CloudCost %s: %v", RegionColumnName, values[i])
				regionID = ""
			}
			properties.RegionID = regionID
		case ZoneColumnName:
			zone, ok := values[i].(string)
			if !ok {
				log.DedupedErrorf(5, "error parsing GCP CloudCost %s: %v", ZoneColumnName, values[i])
				zone = ""
			}
			properties.AvailabilityZone = zone
		case ServiceDescriptionColumnName:
			service, ok := values[i].(string)
			if !ok {
				log.DedupedErrorf(5, "error parsing GCP CloudCost %s: %v", ServiceDescriptionColumnName, values[i])
				service = ""
			}
			properties.Service = service
		case SKUDescriptionColumnName:
			d, ok := values[i].(string)
			if !ok {
				log.DedupedErrorf(5, "error parsing GCP CloudCost %s: %v", SKUDescriptionColumnName, values[i])
				d = ""
			}
			description = d
		case LabelsColumnName:
			labelJSON, ok := values[i].(string)
			if !ok {
				log.DedupedErrorf(5, "error parsing GCP CloudCost %s: %v", LabelsColumnName, values[i])
			}
			labelList := []map[string]string{}
			err := json.Unmarshal([]byte(labelJSON), &labelList)
			if err != nil {
				log.Warnf("GCP Cloud Assets: error unmarshaling GCP CloudCost labels: %s", err)
			}
			labels := map[string]string{}
			for _, pair := range labelList {
				key := pair["key"]
				value := pair["value"]
				labels[key] = value
			}
			properties.Labels = labels
		case ResourceNameColumnName:
			resouceNameValue := values[i]
			if resouceNameValue == nil {
				properties.ProviderID = ""
				continue
			}
			resource, ok := resouceNameValue.(string)
			if !ok {
				log.DedupedErrorf(5, "error parsing GCP CloudCost %s: %v", ResourceNameColumnName, values[i])
				properties.ProviderID = ""
				continue
			}

			properties.ProviderID = ParseProviderID(resource)
		case ResourceGlobalNameColumnName:
			// skip if we already got ProviderID from resource.name, as resource.global_name is a fallback for when
			// resource.name is null
			if len(properties.ProviderID) > 0 {
				continue
			}

			resourceGlobalNameValue := values[i]
			if resourceGlobalNameValue == nil {
				properties.ProviderID = ""
				continue
			}
			resourceGlobalName, ok := resourceGlobalNameValue.(string)
			if !ok {
				log.DedupedErrorf(5, "error parsing GCP CloudCost %s: %v", ResourceGlobalNameColumnName, values[i])
				properties.ProviderID = ""
				continue
			}

			properties.ProviderID = ParseProviderID(resourceGlobalName)
		case CostColumnName:
			costValue, ok := values[i].(float64)
			if !ok {
				log.DedupedErrorf(5, "error parsing GCP CloudCost %s: %v", CostColumnName, values[i])
				costValue = 0.0
			}
			cost = costValue
		case ListCostColumnName:
			listCostValue, ok := values[i].(float64)
			if !ok {
				log.Errorf("error parsing GCP CloudCost %s: %v", ListCostColumnName, values[i])
				listCostValue = 0
			}
			listCost = listCostValue
		case CreditsColumnName:
			creditSlice, ok := values[i].([]bigquery.Value)
			if !ok {
				log.DedupedErrorf(5, "error parsing GCP CloudCost %s: %v", CreditsColumnName, values[i])
			}
			for _, credit := range creditSlice {
				creditValues, ok := credit.([]bigquery.Value)
				if !ok {
					log.DedupedErrorf(5, "error parsing GCP CloudCost credit values: %v", creditValues)
					continue
				}
				amount, ok := creditValues[1].(float64)
				if !ok {
					log.DedupedErrorf(5, "error parsing GCP CloudCost credit amount: %v", creditValues[1])
					continue
				}
				creditType, ok := creditValues[4].(string)
				if !ok {
					log.DedupedErrorf(5, "error parsing GCP CloudCost credit type: %v", creditValues[4])
					continue
				}
				switch creditType {
				case "COMMITTED_USAGE_DISCOUNT":
					cudCreditAmount += amount
				case "COMMITTED_USAGE_DISCOUNT_DOLLAR_BASE":
					flexibleCUDCreditAmount += amount
				default:
					creditAmount += amount
				}
			}
		default:
			log.DedupedErrorf(5, "GCP: BigQuery: found unrecognized column name %s", field.Name)
		}
	}

	// Check required Fields
	if window.IsOpen() {
		return fmt.Errorf("GCP: BigQuery: error parsing, item had invalid window")
	}

	// Determine amount paid for credit received from Global CUD
	var flexibleCUDPayedAmount float64
	var flexibleCUDNetPayedAmount float64
	if ccl.FlexibleCUDRates != nil {
		if rates, ok := ccl.FlexibleCUDRates[*window.Start()]; ok {
			flexibleCUDNetPayedAmount = flexibleCUDCreditAmount * rates.NetRate
			flexibleCUDPayedAmount = flexibleCUDCreditAmount * rates.Rate
		}
	}

	// Determine Category
	properties.Category = SelectCategory(properties.Service, description)

	// price_at_list is a new column in the billing export which may be nil
	if listCost == 0.0 {
		listCost = cost
	}

	// Net Cost is cost with all credit amounts applied
	netCost := cost + creditAmount + cudCreditAmount + flexibleCUDCreditAmount

	// Amortized Cost is Cost plus CUD credits and amortized CUD payments
	amortizedCost := cost + cudCreditAmount + flexibleCUDCreditAmount + flexibleCUDPayedAmount

	// Amortized Net Cost is Cost with all credits and amortized CUD payments
	amortizedNetCost := cost + creditAmount + cudCreditAmount + flexibleCUDCreditAmount + flexibleCUDNetPayedAmount

	// Using the NetCost as a 'placeholder' for these costs now, until we can revisit and spend the time to do
	// the calculations correctly
	invoicedCost := netCost

	// Update Cost for Commitments that will have matching resource id's and should not their non-amortized costs rolled
	// into values
	if strings.HasPrefix(description, "Commitment v1") {
		listCost = 0
		netCost = 0
	}

	// Update Cost for Global CUDs to prevent double counting values, which are added in during amortization
	if strings.HasPrefix(description, "Commitment - dollar based v1:") {
		amortizedCost = 0
		amortizedNetCost = 0
	}

	// percent k8s is determined by the presence of labels
	k8sPercent := 0.0
	if IsK8s(properties.Labels) {
		k8sPercent = 1.0
	}

	ccl.CloudCost = &opencost.CloudCost{
		Properties: &properties,
		Window:     window,
		ListCost: opencost.CostMetric{
			Cost:              listCost,
			KubernetesPercent: k8sPercent,
		},
		AmortizedCost: opencost.CostMetric{
			Cost:              amortizedCost,
			KubernetesPercent: k8sPercent,
		},
		AmortizedNetCost: opencost.CostMetric{
			Cost:              amortizedNetCost,
			KubernetesPercent: k8sPercent,
		},
		InvoicedCost: opencost.CostMetric{
			Cost:              invoicedCost,
			KubernetesPercent: k8sPercent,
		},
		NetCost: opencost.CostMetric{
			Cost:              netCost,
			KubernetesPercent: k8sPercent,
		},
	}

	return nil
}

type FlexibleCUDCreditTotalsLoader struct {
	values map[time.Time]float64
}

func (ctl *FlexibleCUDCreditTotalsLoader) Load(values []bigquery.Value, schema bigquery.Schema) error {

	usageDate, ok := values[0].(time.Time)
	if !ok {
		// It would be very surprising if an unparsable time came back from the API, so it should be ok to return here.
		return fmt.Errorf("error parsing usage date: %v", values[0])
	}

	amount, ok := values[1].(float64)
	if !ok {
		return fmt.Errorf("error parsing amount: %v", values[1])
	}

	if ctl.values == nil {
		ctl.values = map[time.Time]float64{}
	}

	ctl.values[usageDate] = amount

	return nil
}

type flexibleCUDCostTotals struct {
	cost    float64
	credits float64
}

type FlexibleCUDCostTotalsLoader struct {
	values map[time.Time]flexibleCUDCostTotals
}

func (ctl *FlexibleCUDCostTotalsLoader) Load(values []bigquery.Value, schema bigquery.Schema) error {
	usageDate, ok := values[0].(time.Time)
	if !ok {
		// It would be very surprising if an unparsable time came back from the API, so it should be ok to return here.
		return fmt.Errorf("error parsing usage date: %v", values[0])
	}

	cost, ok := values[1].(float64)
	if !ok {
		return fmt.Errorf("error parsing cost: %v", values[1])
	}

	credits, ok := values[2].(float64)
	if !ok {
		return fmt.Errorf("error parsing credits: %v", values[2])
	}

	if ctl.values == nil {
		ctl.values = map[time.Time]flexibleCUDCostTotals{}
	}

	ctl.values[usageDate] = flexibleCUDCostTotals{
		cost:    cost,
		credits: credits,
	}

	return nil
}
