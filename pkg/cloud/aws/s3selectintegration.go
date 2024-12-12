package aws

import (
	"encoding/csv"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
)

const S3SelectDateLayout = "2006-01-02T15:04:05Z"

// S3Object is aliased as "s" in queries
const S3SelectBillPayerAccountID = `s."bill/PayerAccountId"`
const S3SelectAccountID = `s."lineItem/UsageAccountId"`
const S3SelectItemType = `s."lineItem/LineItemType"`
const S3SelectStartDate = `s."lineItem/UsageStartDate"`
const S3SelectProductCode = `s."lineItem/ProductCode"`
const S3SelectResourceID = `s."lineItem/ResourceId"`
const S3SelectUsageType = `s."lineItem/UsageType"`
const S3SelectRegionCode = `s."product/regionCode"`
const S3SelectAvailabilityZone = `s."lineItem/AvailabilityZone"`

const S3SelectListCost = `s."lineItem/UnblendedCost"`
const S3SelectNetCost = `s."lineItem/NetUnblendedCost"`

// These two may be used for Amortized<Net>Cost
const S3SelectRICost = `s."reservation/EffectiveCost"`
const S3SelectSPCost = `s."savingsPlan/SavingsPlanEffectiveCost"`
const S3SelectNetRICost = `s."reservation/NetEffectiveCost"`
const S3SelectNetSPCost = `s."savingsPlan/NetSavingsPlanEffectiveCost"`

const S3SelectUserLabelPrefix = "resourceTags/user:"
const S3SelectAWSLabelPrefix = "resourceTags/aws:"
const S3SelectResourceTagsPrefix = "resourceTags/"

const (
	TypeSavingsPlanCoveredUsage = "SavingsPlanCoveredUsage"
	TypeDiscountedUsage         = "DiscountedUsage"
	TypeEDPDiscount             = "EdpDiscount"
	TypePrivateRateDiscount     = "PrivateRateDiscount"
)

type S3SelectIntegration struct {
	S3SelectQuerier
}

func (s3si *S3SelectIntegration) GetCloudCost(
	start,
	end time.Time,
) (*opencost.CloudCostSetRange, error) {
	log.Infof(
		"S3SelectIntegration[%s]: GetCloudCost: %s",
		s3si.Key(),
		opencost.NewWindow(&start, &end).String(),
	)

	// ccsr to populate with cloudcosts.
	ccsr, err := opencost.NewCloudCostSetRange(
		start,
		end,
		opencost.AccumulateOptionDay,
		s3si.Key(),
	)
	if err != nil {
		return nil, err
	}
	// acquire S3 client
	client, err := s3si.GetS3Client()
	if err != nil {
		return nil, err
	}
	// Acquire query keys
	queryKeys, err := s3si.GetQueryKeys(start, end, client)
	if err != nil {
		return nil, err
	}
	// Acquire headers
	headers, err := s3si.GetHeaders(queryKeys[0], client)
	if err != nil {
		return nil, err
	}

	allColumns := map[string]struct{}{}
	for _, header := range headers {
		allColumns[header] = struct{}{}
	}

	formattedStart := start.Format("2006-01-02")
	formattedEnd := end.Format("2006-01-02")
	selectColumns := []string{
		S3SelectStartDate,
		S3SelectBillPayerAccountID,
		S3SelectAccountID,
		S3SelectResourceID,
		S3SelectItemType,
		S3SelectProductCode,
		S3SelectUsageType,
		S3SelectRegionCode,
		S3SelectAvailabilityZone,
		S3SelectListCost,
	}

	if _, ok := allColumns[S3SelectNetCost]; ok {
		selectColumns = append(selectColumns, S3SelectNetCost)
	}

	// Check for Reservation columns in CUR and query if available

	if _, ok := allColumns[S3SelectRICost]; ok {
		selectColumns = append(selectColumns, S3SelectRICost)
	}

	if _, ok := allColumns[S3SelectNetRICost]; ok {
		selectColumns = append(selectColumns, S3SelectNetRICost)
	}

	// Check for Savings Plan Columns in CUR and query if available

	if _, ok := allColumns[S3SelectSPCost]; ok {
		selectColumns = append(selectColumns, S3SelectSPCost)
	}

	if _, ok := allColumns[S3SelectNetSPCost]; ok {
		selectColumns = append(selectColumns, S3SelectNetSPCost)
	}

	// Determine which columns are user-defined tags and add those to the list
	// of columns to query.
	userLabelColumns := []string{}
	awsLabelColumns := []string{}
	for column := range allColumns {
		if strings.HasPrefix(column, S3SelectUserLabelPrefix) {
			quotedTag := fmt.Sprintf(`s."%s"`, column)
			selectColumns = append(selectColumns, quotedTag)
			userLabelColumns = append(userLabelColumns, quotedTag)
		}
		if strings.HasPrefix(column, S3SelectAWSLabelPrefix) {
			quotedTag := fmt.Sprintf(`s."%s"`, column)
			selectColumns = append(selectColumns, quotedTag)
			awsLabelColumns = append(awsLabelColumns, quotedTag)
		}
	}

	// Build map of query columns to use for parsing query
	columnIndexes := map[string]int{}
	for i, column := range selectColumns {
		columnIndexes[column] = i
	}
	// Build query
	selectStr := strings.Join(selectColumns, ", ")
	queryStr := `SELECT %s FROM s3object s
	WHERE (CAST(s."lineItem/UsageStartDate" AS TIMESTAMP) BETWEEN CAST('%s' AS TIMESTAMP) AND CAST('%s' AS TIMESTAMP))
	AND (s."lineItem/LineItemType" = 'Usage' OR s."lineItem/LineItemType" = 'DiscountedUsage' OR s."lineItem/LineItemType" = 'SavingsPlanCoveredUsage' OR s."lineItem/LineItemType" = 'EdpDiscount' OR s."lineItem/LineItemType" = 'PrivateRateDiscount')
	`
	query := fmt.Sprintf(queryStr, selectStr, formattedStart, formattedEnd)

	processResults := func(reader *csv.Reader) error {
		_, err2 := reader.Read()
		if err2 == io.EOF {
			return nil
		}
		for {
			row, err3 := reader.Read()
			if err3 == io.EOF {
				return nil
			}

			cc, err3 := s3RowToCloudCost(row, columnIndexes, userLabelColumns, awsLabelColumns)
			if err3 != nil {
				log.Errorf("error creating cloud cost from row: %s", err3.Error())
				continue
			}

			ccsr.LoadCloudCost(cc)
		}
	}
	err = s3si.Query(query, queryKeys, client, processResults)
	if err != nil {
		return nil, err
	}

	return ccsr, nil
}

func s3RowToCloudCost(row []string, columnIndexes map[string]int, userLabelColumns, awsLabelColumns []string) (*opencost.CloudCost, error) {
	startStr := GetCSVRowValue(row, columnIndexes, S3SelectStartDate)
	billPayerAccountID := GetCSVRowValue(row, columnIndexes, S3SelectBillPayerAccountID)
	itemAccountID := GetCSVRowValue(row, columnIndexes, S3SelectAccountID)
	itemProviderID := GetCSVRowValue(row, columnIndexes, S3SelectResourceID)
	lineItemType := GetCSVRowValue(row, columnIndexes, S3SelectItemType)
	itemProductCode := GetCSVRowValue(row, columnIndexes, S3SelectProductCode)
	usageType := GetCSVRowValue(row, columnIndexes, S3SelectUsageType)
	regionCode := GetCSVRowValue(row, columnIndexes, S3SelectRegionCode)
	availabilityZone := GetCSVRowValue(row, columnIndexes, S3SelectAvailabilityZone)

	// Iterate through the slice of tag columns, assigning
	// values to the column names, minus the tag prefix.
	labels := opencost.CloudCostLabels{}
	for _, labelColumnName := range userLabelColumns {
		// remove quotes
		labelName := strings.TrimPrefix(labelColumnName, `s."`)
		labelName = strings.TrimSuffix(labelName, `"`)
		// remove prefix
		labelName = strings.TrimPrefix(labelName, S3SelectUserLabelPrefix)
		value := GetCSVRowValue(row, columnIndexes, labelColumnName)
		if value != "" {
			labels[labelName] = value
		}
	}
	for _, awsLabelColumnName := range awsLabelColumns {
		// remove quotes
		labelName := strings.TrimPrefix(awsLabelColumnName, `s."`)
		labelName = strings.TrimSuffix(labelName, `"`)
		// partially remove prefix leaving "aws:"
		labelName = strings.TrimPrefix(labelName, S3SelectResourceTagsPrefix)
		value := GetCSVRowValue(row, columnIndexes, awsLabelColumnName)
		if value != "" {
			labels[labelName] = value
		}
	}

	isKubernetes := 0.0
	if itemProductCode == "AmazonEKS" || hasK8sLabel(labels) {
		isKubernetes = 1.0
	}

	var (
		amortizedCost    float64
		amortizedNetCost float64
		listCost         float64
		netCost          float64
		err              error
	)
	// Get list and net costs
	if lineItemType != TypeEDPDiscount && lineItemType != TypePrivateRateDiscount {
		listCost, err = GetCSVRowValueFloat(row, columnIndexes, S3SelectListCost)
		if err != nil {
			return nil, err
		}
	}

	// Get net cost if available
	netCost = listCost
	if _, ok := columnIndexes[S3SelectNetCost]; ok {
		netCost, err = GetCSVRowValueFloat(row, columnIndexes, S3SelectNetCost)
		if err != nil {
			return nil, err
		}
	}

	// If there is a reservation_reservation_a_r_n on the line item use the awsRIPricingSUMColumn as cost
	amortizedCost = listCost
	amortizedNetCost = listCost
	if lineItemType == TypeDiscountedUsage {
		if _, ok := columnIndexes[S3SelectRICost]; ok {
			amortizedCost, err = GetCSVRowValueFloat(row, columnIndexes, S3SelectRICost)
			if err != nil {
				return nil, err
			}
			amortizedNetCost = amortizedCost
		}
		if _, ok := columnIndexes[S3SelectNetRICost]; ok {
			amortizedNetCost, err = GetCSVRowValueFloat(row, columnIndexes, S3SelectNetRICost)
			if err != nil {
				return nil, err
			}
		}
		// If there is a lineItemType of SavingsPlanCoveredUsage use the awsSPPricingSUMColumn
	} else if lineItemType == TypeSavingsPlanCoveredUsage {
		if _, ok := columnIndexes[S3SelectSPCost]; ok {
			amortizedCost, err = GetCSVRowValueFloat(row, columnIndexes, S3SelectSPCost)
			if err != nil {
				return nil, err
			}
			amortizedNetCost = amortizedCost
		}
		if _, ok := columnIndexes[S3SelectNetSPCost]; ok {
			amortizedNetCost, err = GetCSVRowValueFloat(row, columnIndexes, S3SelectNetSPCost)
			if err != nil {
				return nil, err
			}
		}
	}

	category := SelectAWSCategory(itemProviderID, usageType, itemProductCode)
	// Retrieve final stanza of product code for ProviderID
	if itemProductCode == "AWSELB" || itemProductCode == "AmazonFSx" {
		itemProviderID = ParseARN(itemProviderID)
	}

	properties := opencost.CloudCostProperties{}
	properties.Provider = opencost.AWSProvider
	properties.InvoiceEntityID = billPayerAccountID
	properties.InvoiceEntityName = billPayerAccountID
	properties.AccountID = itemAccountID
	properties.AccountName = itemAccountID
	properties.Category = category
	properties.Service = itemProductCode
	properties.ProviderID = itemProviderID
	properties.RegionID = regionCode
	properties.AvailabilityZone = availabilityZone
	properties.Labels = labels

	itemStart, err := time.Parse(S3SelectDateLayout, startStr)
	if err != nil {
		return nil, fmt.Errorf(
			"Unable to parse '%s': '%s'",
			S3SelectStartDate,
			err.Error(),
		)
	}
	itemStart = itemStart.Truncate(time.Hour * 24)
	itemEnd := itemStart.AddDate(0, 0, 1)

	return &opencost.CloudCost{
		Properties: &properties,
		Window:     opencost.NewWindow(&itemStart, &itemEnd),
		ListCost: opencost.CostMetric{
			Cost:              listCost,
			KubernetesPercent: isKubernetes,
		},
		NetCost: opencost.CostMetric{
			Cost:              netCost,
			KubernetesPercent: isKubernetes,
		},
		AmortizedNetCost: opencost.CostMetric{
			Cost:              amortizedNetCost,
			KubernetesPercent: isKubernetes,
		},
		AmortizedCost: opencost.CostMetric{
			Cost:              amortizedCost,
			KubernetesPercent: isKubernetes,
		},
		InvoicedCost: opencost.CostMetric{
			Cost:              netCost,
			KubernetesPercent: isKubernetes,
		},
	}, nil
}

const (
	TagAWSEKSClusterName     = "aws:eks:cluster-name"
	TagEKSClusterName        = "eks:cluster-name"
	TagEKSCtlClusterName     = "alpha.eksctl.io/cluster-name"
	TagKubernetesServiceName = "kubernetes.io/service-name"
	TagKubernetesPVCName     = "kubernetes.io/created-for/pvc/name"
	TagKubernetesPVName      = "kubernetes.io/created-for/pv/name"
)

// hsK8sLabel checks if the labels contain a k8s label
func hasK8sLabel(labels opencost.CloudCostLabels) bool {
	if _, ok := labels[TagAWSEKSClusterName]; ok {
		return true
	}
	if _, ok := labels[TagEKSClusterName]; ok {
		return true
	}
	if _, ok := labels[TagEKSCtlClusterName]; ok {
		return true
	}
	if _, ok := labels[TagKubernetesServiceName]; ok {
		return true
	}
	if _, ok := labels[TagKubernetesPVCName]; ok {
		return true
	}
	if _, ok := labels[TagKubernetesPVName]; ok {
		return true
	}
	return false
}
