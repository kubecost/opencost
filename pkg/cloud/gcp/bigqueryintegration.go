package gcp

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"google.golang.org/api/iterator"
)

type BigQueryIntegration struct {
	BigQueryQuerier
}

const (
	UsageDateColumnName          = "usage_date"
	BillingAccountIDColumnName   = "billing_id"
	ProjectIDColumnName          = "project_id"
	ProjectNameColumnName        = "project_name"
	RegionColumnName             = "region"
	ZoneColumnName               = "zone"
	ServiceDescriptionColumnName = "service"
	SKUDescriptionColumnName     = "description"
	LabelsColumnName             = "labels"
	ResourceNameColumnName       = "resource"
	ResourceGlobalNameColumnName = "global_resource"
	CostColumnName               = "cost"
	ListCostColumnName           = "list_cost"
	CreditsColumnName            = "credits"
)

const BiqQueryWherePartitionFmt = `DATE(_PARTITIONTIME) >= "%s" AND DATE(_PARTITIONTIME) < "%s"`
const BiqQueryWhereDateFmt = `usage_start_time >= "%s" AND usage_start_time < "%s"`

func (bqi *BigQueryIntegration) GetCloudCost(start time.Time, end time.Time) (*opencost.CloudCostSetRange, error) {
	cudRates, err := bqi.GetFlexibleCUDRates(start, end)
	if err != nil {
		return nil, fmt.Errorf("error retrieving CUD rates: %w", err)
	}

	// Build Query
	selectColumns := []string{
		fmt.Sprintf("TIMESTAMP_TRUNC(usage_start_time, day) as %s", UsageDateColumnName),
		fmt.Sprintf("billing_account_id as %s", BillingAccountIDColumnName),
		fmt.Sprintf("project.id as %s", ProjectIDColumnName),
		fmt.Sprintf("project.name as %s", ProjectNameColumnName),
		fmt.Sprintf("location.region as %s", RegionColumnName),
		fmt.Sprintf("location.zone as %s", ZoneColumnName),
		fmt.Sprintf("service.description as %s", ServiceDescriptionColumnName),
		fmt.Sprintf("sku.description as %s", SKUDescriptionColumnName),
		fmt.Sprintf("resource.name as %s", ResourceNameColumnName),
		fmt.Sprintf("resource.global_name as %s", ResourceGlobalNameColumnName),
		fmt.Sprintf("TO_JSON_STRING(labels) as %s", LabelsColumnName),
		fmt.Sprintf("SUM(cost) as %s", CostColumnName),
		fmt.Sprintf("SUM(cost_at_list) as %s", ListCostColumnName),
		fmt.Sprintf("ARRAY_CONCAT_AGG(credits) as %s", CreditsColumnName),
	}

	groupByColumns := []string{
		UsageDateColumnName,
		BillingAccountIDColumnName,
		ProjectIDColumnName,
		ProjectNameColumnName,
		RegionColumnName,
		ZoneColumnName,
		ServiceDescriptionColumnName,
		SKUDescriptionColumnName,
		LabelsColumnName,
		ResourceNameColumnName,
		ResourceGlobalNameColumnName,
	}

	whereConjuncts := GetWhereConjuncts(start, end)

	columnStr := strings.Join(selectColumns, ", ")
	table := fmt.Sprintf(" `%s` bd ", bqi.GetBillingDataDataset())
	whereClause := strings.Join(whereConjuncts, " AND ")
	groupByStr := strings.Join(groupByColumns, ", ")
	queryStr := `
		SELECT %s
		FROM %s
		WHERE %s
		GROUP BY %s
	`

	querystr := fmt.Sprintf(queryStr, columnStr, table, whereClause, groupByStr)

	// Perform Query and parse values

	ccsr, err := opencost.NewCloudCostSetRange(start, end, opencost.AccumulateOptionDay, bqi.Key())
	if err != nil {
		return ccsr, fmt.Errorf("error creating new CloudCostSetRange: %s", err)
	}

	iter, err := bqi.Query(context.Background(), querystr)
	if err != nil {
		return ccsr, fmt.Errorf("error querying: %s", err)
	}

	// Parse query into CloudCostSetRange

	for {
		ccl := CloudCostLoader{
			FlexibleCUDRates: cudRates,
		}
		err = iter.Next(&ccl)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return ccsr, err
		}
		if ccl.CloudCost == nil {
			continue
		}
		ccsr.LoadCloudCost(ccl.CloudCost)

	}

	return ccsr, nil

}

// GetWhereConjuncts creates a list of Where filter statements that filter for usage start date and partition time
// additional filters can be added before combining into the final where clause
func GetWhereConjuncts(start time.Time, end time.Time) []string {
	partitionStart := start
	partitionEnd := end.AddDate(0, 0, 2)
	wherePartition := fmt.Sprintf(BiqQueryWherePartitionFmt, partitionStart.Format("2006-01-02"), partitionEnd.Format("2006-01-02"))
	whereDate := fmt.Sprintf(BiqQueryWhereDateFmt, start.Format("2006-01-02"), end.Format("2006-01-02"))
	return []string{wherePartition, whereDate}
}

// FlexibleCUDRates are the total amount paid / total amount credited per day for all Flexible CUDs. Since credited will be a negative value
// this will be a negative ratio. This can then be multiplied with the credits from Flexible CUDs on specific line items to determine
// the amount paid for the credit it received. This allows us to amortize the Flexible CUD costs which are not associated with resources
// in the billing export. AmountPayed itself may have some credits on it so a Rate and a NetRate are created.
// Having both allow us to populate AmortizedCost and AmortizedNetCost respectively.
type FlexibleCUDRates struct {
	NetRate float64
	Rate    float64
}

// GetFlexibleCUDRates returns a map of FlexibleCUDRates keyed on the start time of the day which those
// FlexibleCUDRates were derived from.
func (bqi *BigQueryIntegration) GetFlexibleCUDRates(start time.Time, end time.Time) (map[time.Time]FlexibleCUDRates, error) {
	costsByDate, err := bqi.queryFlexibleCUDTotalCosts(start, end)
	if err != nil {
		return nil, fmt.Errorf("GetFlexibleCUDRates: %w", err)
	}

	creditsByDate, err := bqi.queryFlexibleCUDTotalCredits(start, end)
	if err != nil {
		return nil, fmt.Errorf("GetFlexibleCUDRates: %w", err)
	}

	results := map[time.Time]FlexibleCUDRates{}
	for date, amountCredited := range creditsByDate {
		// Protection against divide by zero
		if amountCredited == 0 {
			log.Warnf("GetFlexibleCUDRates: 0 value total credit for Flexible CUDs for date %s", date.Format(time.RFC3339))
			continue
		}
		amountPayed, ok := costsByDate[date]
		if !ok {
			log.Warnf("GetFlexibleCUDRates: could not find Flexible CUD payments for date %s", date.Format(time.RFC3339))
			continue
		}

		// amountPayed itself may have some credits on it so a Rate and a NetRate are created.
		// Having both allow us to populate AmortizedCost and AmortizedNetCost respectively.
		results[date] = FlexibleCUDRates{
			NetRate: (amountPayed.cost + amountPayed.credits) / amountCredited,
			Rate:    amountPayed.cost / amountCredited,
		}

	}
	return results, nil
}

func (bqi *BigQueryIntegration) queryFlexibleCUDTotalCosts(start time.Time, end time.Time) (map[time.Time]flexibleCUDCostTotals, error) {
	queryFmt := `
		SELECT
		  TIMESTAMP_TRUNC(usage_start_time, day) as usage_date, 
		  sum(cost), 
		  IFNULL(SUM((Select SUM(amount) FROM bd.credits)),0),
		FROM %s
		WHERE %s
		GROUP BY usage_date
	`

	table := fmt.Sprintf(" `%s` bd ", bqi.GetBillingDataDataset())
	whereConjuncts := GetWhereConjuncts(start, end)
	whereConjuncts = append(whereConjuncts, "sku.description like 'Commitment - dollar based v1:%'")
	whereClause := strings.Join(whereConjuncts, " AND ")
	query := fmt.Sprintf(queryFmt, table, whereClause)

	iter, err := bqi.Query(context.Background(), query)
	if err != nil {
		return nil, fmt.Errorf("queryCUDAmountPayed: query error %w", err)
	}
	var loader FlexibleCUDCostTotalsLoader
	for {
		err = iter.Next(&loader)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("queryCUDAmountPayed: load error %w", err)
		}
	}
	return loader.values, nil
}

func (bqi *BigQueryIntegration) queryFlexibleCUDTotalCredits(start time.Time, end time.Time) (map[time.Time]float64, error) {
	queryFmt := `SELECT
	TIMESTAMP_TRUNC(usage_start_time, day) as usage_date,
	sum(credits.amount)
	FROM %s
	CROSS JOIN UNNEST(bd.credits) AS credits
	WHERE %s
	GROUP BY usage_date
	`

	table := fmt.Sprintf(" `%s` bd ", bqi.GetBillingDataDataset())
	whereConjuncts := GetWhereConjuncts(start, end)
	whereConjuncts = append(whereConjuncts, "credits.type = 'COMMITTED_USAGE_DISCOUNT_DOLLAR_BASE'")
	whereClause := strings.Join(whereConjuncts, " AND ")
	query := fmt.Sprintf(queryFmt, table, whereClause)

	iter, err := bqi.Query(context.Background(), query)
	if err != nil {
		return nil, fmt.Errorf("queryFlexibleCUDTotalCredits: query error %w", err)
	}
	var loader FlexibleCUDCreditTotalsLoader
	for {
		err = iter.Next(&loader)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("queryFlexibleCUDTotalCredits: load error %w", err)
		}
	}
	return loader.values, nil
}
