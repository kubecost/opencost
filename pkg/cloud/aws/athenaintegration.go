package aws

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/pkg/cloud"
)

const LabelColumnPrefix = "resource_tags_user_"
const AWSLabelColumnPrefix = "resource_tags_aws_"
const AthenaResourceTagPrefix = "resource_tags_"

// athenaDateLayout is the default AWS date format
const AthenaDateLayout = "2006-01-02 15:04:05.000"

// Cost Columns
const AthenaPricingColumn = "line_item_unblended_cost"

// Amortized Cost Columns
const AthenaRIPricingColumn = "reservation_effective_cost"
const AthenaSPPricingColumn = "savings_plan_savings_plan_effective_cost"

// Net Cost Columns
const AthenaNetPricingColumn = "line_item_net_unblended_cost"

var AthenaNetPricingCoalesce = fmt.Sprintf("COALESCE(%s, %s, 0)", AthenaNetPricingColumn, AthenaPricingColumn)

// Amortized Net Cost Columns
const AthenaNetRIPricingColumn = "reservation_net_effective_cost"

var AthenaNetRIPricingCoalesce = fmt.Sprintf("COALESCE(%s, %s, 0)", AthenaNetRIPricingColumn, AthenaRIPricingColumn)

const AthenaNetSPPricingColumn = "savings_plan_net_savings_plan_effective_cost"

var AthenaNetSPPricingCoalesce = fmt.Sprintf("COALESCE(%s, %s, 0)", AthenaNetSPPricingColumn, AthenaSPPricingColumn)

// athenaDateTruncColumn Aggregates line items from the hourly level to daily. "line_item_usage_start_date" is used because at
// all time values 00:00-23:00 it will truncate to the correct date.
const AthenaDateColumn = "line_item_usage_start_date"
const AthenaDateTruncColumn = "DATE_TRUNC('day'," + AthenaDateColumn + ") as usage_date"

const AthenaWhereDateFmt = `line_item_usage_start_date >= date '%s' AND line_item_usage_start_date < date '%s'`
const AthenaWhereUsage = "(line_item_line_item_type = 'Usage' OR line_item_line_item_type = 'DiscountedUsage' OR line_item_line_item_type = 'SavingsPlanCoveredUsage' OR line_item_line_item_type = 'EdpDiscount' OR line_item_line_item_type = 'PrivateRateDiscount')"

// AthenaQueryIndexes is a struct for holding the context of a query
type AthenaQueryIndexes struct {
	Query                  string
	ColumnIndexes          map[string]int
	TagColumns             []string
	AWSTagColumns          []string
	ListCostColumn         string
	NetCostColumn          string
	AmortizedNetCostColumn string
	AmortizedCostColumn    string
	IsK8sColumn            string
}

type AthenaIntegration struct {
	AthenaQuerier
}

// Query Athena for CUR data and build a new CloudCostSetRange containing the info
func (ai *AthenaIntegration) GetCloudCost(start, end time.Time) (*opencost.CloudCostSetRange, error) {
	log.Infof("AthenaIntegration[%s]: GetCloudCost: %s", ai.Key(), opencost.NewWindow(&start, &end).String())
	// Query for all column names
	allColumns, err := ai.GetColumns()
	if err != nil {
		return nil, fmt.Errorf("GetCloudCost: error getting Athena columns: %w", err)
	}

	// List known, hard-coded columns to query
	groupByColumns := []string{
		AthenaDateTruncColumn,
		"line_item_resource_id",
		"bill_payer_account_id",
		"line_item_usage_account_id",
		"line_item_product_code",
		"line_item_usage_type",
		"product_region_code",
		"line_item_availability_zone",
	}

	// Create query indices
	aqi := AthenaQueryIndexes{}

	// Add is k8s column
	isK8sColumn := ai.GetIsKubernetesColumn(allColumns)
	groupByColumns = append(groupByColumns, isK8sColumn)
	aqi.IsK8sColumn = isK8sColumn

	// Determine which columns are user-defined tags and add those to the list
	// of columns to query.
	for column := range allColumns {
		if strings.HasPrefix(column, LabelColumnPrefix) {
			quotedTag := fmt.Sprintf(`"%s"`, column)
			groupByColumns = append(groupByColumns, quotedTag)
			aqi.TagColumns = append(aqi.TagColumns, quotedTag)
		}
		if strings.HasPrefix(column, AWSLabelColumnPrefix) {
			groupByColumns = append(groupByColumns, column)
			aqi.AWSTagColumns = append(aqi.AWSTagColumns, column)
		}
	}
	var selectColumns []string

	// Duplicate GroupBy Columns into select columns
	selectColumns = append(selectColumns, groupByColumns...)

	// Clean Up group by columns
	ai.RemoveColumnAliases(groupByColumns)

	// Build list cost column and add it to the select columns
	listCostColumn := ai.GetListCostColumn()
	selectColumns = append(selectColumns, listCostColumn)
	aqi.ListCostColumn = listCostColumn

	// Build net cost column and add it to select columns
	netCostColumn := ai.GetNetCostColumn(allColumns)
	selectColumns = append(selectColumns, netCostColumn)
	aqi.NetCostColumn = netCostColumn

	// Build amortized net cost column and add it to select columns
	amortizedNetCostColumn := ai.GetAmortizedNetCostColumn(allColumns)
	selectColumns = append(selectColumns, amortizedNetCostColumn)
	aqi.AmortizedNetCostColumn = amortizedNetCostColumn

	// Build Amortized cost column and add it to select columns
	amortizedCostColumn := ai.GetAmortizedCostColumn(allColumns)
	selectColumns = append(selectColumns, amortizedCostColumn)
	aqi.AmortizedCostColumn = amortizedCostColumn

	// Build map of query columns to use for parsing query
	aqi.ColumnIndexes = map[string]int{}
	for i, column := range selectColumns {
		aqi.ColumnIndexes[column] = i
	}
	whereDate := fmt.Sprintf(AthenaWhereDateFmt, start.Format("2006-01-02"), end.Format("2006-01-02"))
	wherePartitions := ai.GetPartitionWhere(start, end)

	// Query for all line items with a resource_id or from AWS Marketplace, which did not end before
	// the range or start after it. This captures all costs with any amount of
	// overlap with the range, for which we will only extract the relevant costs
	whereConjuncts := []string{
		wherePartitions,
		whereDate,
		AthenaWhereUsage,
	}
	columnStr := strings.Join(selectColumns, ", ")
	whereClause := strings.Join(whereConjuncts, " AND ")
	groupByStr := strings.Join(groupByColumns, ", ")
	queryStr := `
		SELECT %s
		FROM "%s"
		WHERE %s
		GROUP BY %s
	`
	aqi.Query = fmt.Sprintf(queryStr, columnStr, ai.Table, whereClause, groupByStr)

	ccsr, err := opencost.NewCloudCostSetRange(start, end, opencost.AccumulateOptionDay, ai.Key())
	if err != nil {
		return nil, err
	}

	// Generate row handling function.
	rowHandler := func(row types.Row) {
		cc, err2 := athenaRowToCloudCost(row, aqi)
		if err2 != nil {
			log.Errorf("AthenaIntegration: GetCloudCost: error while parsing row: %s", err2.Error())
			return
		}
		ccsr.LoadCloudCost(cc)
	}
	log.Debugf("AthenaIntegration[%s]: GetCloudCost: querying: %s", ai.Key(), aqi.Query)
	// Query CUR data and fill out CCSR
	err = ai.Query(context.TODO(), aqi.Query, GetAthenaQueryFunc(rowHandler))
	if err != nil {
		return nil, err
	}

	ai.ConnectionStatus = ai.GetConnectionStatusFromResult(ccsr, ai.ConnectionStatus)

	return ccsr, nil

}

func (ai *AthenaIntegration) GetListCostColumn() string {
	var listCostBuilder strings.Builder
	listCostBuilder.WriteString("CASE line_item_line_item_type")
	listCostBuilder.WriteString(" WHEN 'EdpDiscount' THEN 0")
	listCostBuilder.WriteString(" WHEN 'PrivateRateDiscount' THEN 0")
	listCostBuilder.WriteString(" ELSE ")
	listCostBuilder.WriteString(AthenaPricingColumn)
	listCostBuilder.WriteString(" END")
	return fmt.Sprintf("SUM(%s) as list_cost", listCostBuilder.String())
}

func (ai *AthenaIntegration) GetNetCostColumn(allColumns map[string]bool) string {
	netCostColumn := ""
	if allColumns[AthenaNetPricingColumn] { // if Net pricing exists
		netCostColumn = AthenaNetPricingCoalesce
	} else { // Non-net for if there's no net pricing.
		netCostColumn = AthenaPricingColumn
	}
	return fmt.Sprintf("SUM(%s) as net_cost", netCostColumn)
}

func (ai *AthenaIntegration) GetAmortizedCostColumn(allColumns map[string]bool) string {
	amortizedCostCase := ai.GetAmortizedCostCase(allColumns)
	return fmt.Sprintf("SUM(%s) as amortized_cost", amortizedCostCase)
}

func (ai *AthenaIntegration) GetAmortizedNetCostColumn(allColumns map[string]bool) string {
	amortizedNetCostCase := ""
	if allColumns[AthenaNetPricingColumn] { // if Net pricing exists
		amortizedNetCostCase = ai.GetAmortizedNetCostCase(allColumns)
	} else { // Non-net for if there's no net pricing.
		amortizedNetCostCase = ai.GetAmortizedCostCase(allColumns)
	}
	return fmt.Sprintf("SUM(%s) as amortized_net_cost", amortizedNetCostCase)
}

func (ai *AthenaIntegration) GetAmortizedCostCase(allColumns map[string]bool) string {
	// Use unblended costs if Reserved Instances/Savings Plans aren't in use
	if !allColumns[AthenaRIPricingColumn] && !allColumns[AthenaSPPricingColumn] {
		return AthenaPricingColumn
	}

	var costBuilder strings.Builder
	costBuilder.WriteString("CASE line_item_line_item_type")
	if allColumns[AthenaRIPricingColumn] {
		costBuilder.WriteString(" WHEN 'DiscountedUsage' THEN ")
		costBuilder.WriteString(AthenaRIPricingColumn)
	}

	if allColumns[AthenaSPPricingColumn] {
		costBuilder.WriteString(" WHEN 'SavingsPlanCoveredUsage' THEN ")
		costBuilder.WriteString(AthenaSPPricingColumn)
	}

	costBuilder.WriteString(" ELSE ")
	costBuilder.WriteString(AthenaPricingColumn)
	costBuilder.WriteString(" END")
	return costBuilder.String()
}

func (ai *AthenaIntegration) GetAmortizedNetCostCase(allColumns map[string]bool) string {
	// Use net unblended costs if Reserved Instances/Savings Plans aren't in use
	if !allColumns[AthenaNetRIPricingColumn] && !allColumns[AthenaNetSPPricingColumn] {
		return AthenaNetPricingCoalesce
	}

	var costBuilder strings.Builder
	costBuilder.WriteString("CASE line_item_line_item_type")
	if allColumns[AthenaNetRIPricingColumn] {
		costBuilder.WriteString(" WHEN 'DiscountedUsage' THEN ")
		costBuilder.WriteString(AthenaNetRIPricingCoalesce)
	}

	if allColumns[AthenaNetSPPricingColumn] {
		costBuilder.WriteString(" WHEN 'SavingsPlanCoveredUsage' THEN ")
		costBuilder.WriteString(AthenaNetSPPricingCoalesce)
	}

	costBuilder.WriteString(" ELSE ")
	costBuilder.WriteString(AthenaNetPricingCoalesce)
	costBuilder.WriteString(" END")
	return costBuilder.String()
}

func (ai *AthenaIntegration) RemoveColumnAliases(columns []string) {
	for i, column := range columns {
		if strings.Contains(column, " as ") {
			columnValues := strings.Split(column, " as ")
			columns[i] = columnValues[0]
		}
	}
}

func (ai *AthenaIntegration) ConvertLabelToAWSTag(label string) string {
	// if the label already has the column prefix assume that it is in the correct format
	if strings.HasPrefix(label, LabelColumnPrefix) {
		return label
	}
	// replace characters with underscore
	tag := label
	tag = strings.ReplaceAll(tag, ".", "_")
	tag = strings.ReplaceAll(tag, "/", "_")
	tag = strings.ReplaceAll(tag, ":", "_")
	tag = strings.ReplaceAll(tag, "-", "_")
	// add prefix and return
	return LabelColumnPrefix + tag
}

// GetIsKubernetesColumn builds a column that determines if a row represents kubernetes spend
func (ai *AthenaIntegration) GetIsKubernetesColumn(allColumns map[string]bool) string {
	disjuncts := []string{
		"line_item_product_code = 'AmazonEKS'", // EKS is always kubernetes
	}
	// tagColumns is a list of columns where the presence of a value indicates that a resource is part of a kubernetes cluster
	tagColumns := []string{
		"resource_tags_aws_eks_cluster_name",
		"resource_tags_user_eks_cluster_name",
		"resource_tags_user_alpha_eksctl_io_cluster_name",
		"resource_tags_user_kubernetes_io_service_name",
		"resource_tags_user_kubernetes_io_created_for_pvc_name",
		"resource_tags_user_kubernetes_io_created_for_pv_name",
	}

	for _, tagColumn := range tagColumns {
		// if tag column is present in the CUR check for it
		if _, ok := allColumns[tagColumn]; ok {
			disjunctStr := fmt.Sprintf("%s <> ''", tagColumn)
			disjuncts = append(disjuncts, disjunctStr)
		}
	}

	return fmt.Sprintf("(%s) as is_kubernetes", strings.Join(disjuncts, " OR "))
}

func (ai *AthenaIntegration) GetPartitionWhere(start, end time.Time) string {
	month := time.Date(start.Year(), start.Month(), 1, 0, 0, 0, 0, time.UTC)
	endMonth := time.Date(end.Year(), end.Month(), 1, 0, 0, 0, 0, time.UTC)
	var disjuncts []string
	for !month.After(endMonth) {
		disjuncts = append(disjuncts, fmt.Sprintf("(year = '%d' AND month = '%d')", month.Year(), month.Month()))
		month = month.AddDate(0, 1, 0)
	}
	str := fmt.Sprintf("(%s)", strings.Join(disjuncts, " OR "))
	return str
}

func athenaRowToCloudCost(row types.Row, aqi AthenaQueryIndexes) (*opencost.CloudCost, error) {
	if len(row.Data) < len(aqi.ColumnIndexes) {
		return nil, fmt.Errorf("rowToCloudCost: row with fewer than %d columns (has only %d)", len(aqi.ColumnIndexes), len(row.Data))
	}

	// Iterate through the slice of tag columns, assigning
	// values to the column names, minus the tag prefix.
	labels := opencost.CloudCostLabels{}
	for _, tagColumnName := range aqi.TagColumns {
		// remove quotes
		labelName := strings.TrimPrefix(tagColumnName, `"`)
		labelName = strings.TrimSuffix(labelName, `"`)
		// remove prefix
		labelName = strings.TrimPrefix(labelName, LabelColumnPrefix)
		value := GetAthenaRowValue(row, aqi.ColumnIndexes, tagColumnName)
		if value != "" {
			labels[labelName] = value
		}
	}

	for _, awsColumnName := range aqi.AWSTagColumns {
		// partially remove prefix leaving "aws_"
		labelName := strings.TrimPrefix(awsColumnName, AthenaResourceTagPrefix)
		value := GetAthenaRowValue(row, aqi.ColumnIndexes, awsColumnName)
		if value != "" {
			labels[labelName] = value
		}
	}

	invoiceEntityID := GetAthenaRowValue(row, aqi.ColumnIndexes, "bill_payer_account_id")
	accountID := GetAthenaRowValue(row, aqi.ColumnIndexes, "line_item_usage_account_id")
	startStr := GetAthenaRowValue(row, aqi.ColumnIndexes, AthenaDateTruncColumn)
	providerID := GetAthenaRowValue(row, aqi.ColumnIndexes, "line_item_resource_id")
	productCode := GetAthenaRowValue(row, aqi.ColumnIndexes, "line_item_product_code")
	usageType := GetAthenaRowValue(row, aqi.ColumnIndexes, "line_item_usage_type")
	regionCode := GetAthenaRowValue(row, aqi.ColumnIndexes, "product_region_code")
	availabilityZone := GetAthenaRowValue(row, aqi.ColumnIndexes, "line_item_availability_zone")
	isK8s, _ := strconv.ParseBool(GetAthenaRowValue(row, aqi.ColumnIndexes, aqi.IsK8sColumn))
	k8sPct := 0.0
	if isK8s {
		k8sPct = 1.0
	}

	listCost, err := GetAthenaRowValueFloat(row, aqi.ColumnIndexes, aqi.ListCostColumn)
	if err != nil {
		return nil, err
	}

	netCost, err := GetAthenaRowValueFloat(row, aqi.ColumnIndexes, aqi.NetCostColumn)
	if err != nil {
		return nil, err
	}

	amortizedNetCost, err := GetAthenaRowValueFloat(row, aqi.ColumnIndexes, aqi.AmortizedNetCostColumn)
	if err != nil {
		return nil, err
	}

	amortizedCost, err := GetAthenaRowValueFloat(row, aqi.ColumnIndexes, aqi.AmortizedCostColumn)
	if err != nil {
		return nil, err
	}

	// Identify resource category in the CUR
	category := SelectAWSCategory(providerID, usageType, productCode)

	// Retrieve final stanza of product code for ProviderID
	if productCode == "AWSELB" || productCode == "AmazonFSx" {
		providerID = ParseARN(providerID)
	}

	if productCode == "AmazonEKS" && category == opencost.ComputeCategory {
		if strings.Contains(usageType, "CPU") {
			providerID = fmt.Sprintf("%s/CPU", providerID)
		} else if strings.Contains(usageType, "GB") {
			providerID = fmt.Sprintf("%s/RAM", providerID)
		}
	}

	properties := opencost.CloudCostProperties{
		ProviderID:        providerID,
		Provider:          opencost.AWSProvider,
		AccountID:         accountID,
		AccountName:       accountID,
		InvoiceEntityID:   invoiceEntityID,
		InvoiceEntityName: invoiceEntityID,
		RegionID:          regionCode,
		AvailabilityZone:  availabilityZone,
		Service:           productCode,
		Category:          category,
		Labels:            labels,
	}

	start, err := time.Parse(AthenaDateLayout, startStr)
	if err != nil {
		return nil, fmt.Errorf("unable to parse %s: '%s'", AthenaDateTruncColumn, err.Error())
	}
	end := start.AddDate(0, 0, 1)

	cc := &opencost.CloudCost{
		Properties: &properties,
		Window:     opencost.NewWindow(&start, &end),
		ListCost: opencost.CostMetric{
			Cost:              listCost,
			KubernetesPercent: k8sPct,
		},
		NetCost: opencost.CostMetric{
			Cost:              netCost,
			KubernetesPercent: k8sPct,
		},
		AmortizedNetCost: opencost.CostMetric{
			Cost:              amortizedNetCost,
			KubernetesPercent: k8sPct,
		},
		AmortizedCost: opencost.CostMetric{
			Cost:              amortizedCost,
			KubernetesPercent: k8sPct,
		},
		InvoicedCost: opencost.CostMetric{
			Cost:              netCost, // We are using Net Cost for Invoiced Cost for now as it is the closest approximation
			KubernetesPercent: k8sPct,
		},
	}

	return cc, nil
}

func (ai *AthenaIntegration) GetConnectionStatusFromResult(result cloud.EmptyChecker, currentStatus cloud.ConnectionStatus) cloud.ConnectionStatus {
	if result.IsEmpty() && currentStatus != cloud.SuccessfulConnection {
		return cloud.MissingData
	}
	return cloud.SuccessfulConnection
}
