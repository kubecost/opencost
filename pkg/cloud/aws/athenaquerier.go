package aws

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/stringutil"
	"github.com/opencost/opencost/pkg/cloud"
)

type AthenaQuerier struct {
	AthenaConfiguration
	ConnectionStatus cloud.ConnectionStatus
}

func (aq *AthenaQuerier) GetStatus() cloud.ConnectionStatus {
	// initialize status if it has not done so; this can happen if the integration is inactive
	if aq.ConnectionStatus.String() == "" {
		aq.ConnectionStatus = cloud.InitialStatus
	}
	return aq.ConnectionStatus
}

func (aq *AthenaQuerier) Equals(config cloud.Config) bool {
	thatConfig, ok := config.(*AthenaQuerier)
	if !ok {
		return false
	}

	return aq.AthenaConfiguration.Equals(&thatConfig.AthenaConfiguration)
}

// GetColumns returns a list of the names of all columns in the configured
// Athena table
func (aq *AthenaQuerier) GetColumns() (map[string]bool, error) {
	columnSet := map[string]bool{}

	// This Query is supported by Athena tables and views
	q := `SELECT column_name FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s'`
	query := fmt.Sprintf(q, aq.Database, aq.Table)

	athenaErr := aq.Query(context.TODO(), query, GetAthenaQueryFunc(func(row types.Row) {
		columnSet[*row.Data[0].VarCharValue] = true
	}))

	if athenaErr != nil {
		return columnSet, athenaErr
	}

	if len(columnSet) == 0 {
		log.Infof("No columns retrieved from Athena")
	}

	return columnSet, nil
}

func (aq *AthenaQuerier) Query(ctx context.Context, query string, fn func(*athena.GetQueryResultsOutput) bool) error {
	err := aq.Validate()
	if err != nil {
		aq.ConnectionStatus = cloud.InvalidConfiguration
		return err
	}

	log.Debugf("AthenaQuerier[%s]: Performing Query: %s", aq.Key(), query)
	err = aq.queryAthenaPaginated(ctx, query, fn)
	if err != nil {
		aq.ConnectionStatus = cloud.FailedConnection
		return err
	}
	return nil
}

func (aq *AthenaQuerier) GetAthenaClient() (*athena.Client, error) {
	cfg, err := aq.Authorizer.CreateAWSConfig(aq.Region)
	if err != nil {
		return nil, err
	}
	cli := athena.NewFromConfig(cfg)
	return cli, nil
}

// QueryAthenaPaginated executes athena query and processes results. An error from this method indicates a
// FAILED_CONNECTION CloudConnectionStatus and should immediately stop the caller to maintain the correct CloudConnectionStatus
func (aq *AthenaQuerier) queryAthenaPaginated(ctx context.Context, query string, fn func(*athena.GetQueryResultsOutput) bool) error {

	queryExecutionCtx := &types.QueryExecutionContext{
		Database: aws.String(aq.Database),
	}

	if aq.Catalog != "" {
		queryExecutionCtx.Catalog = aws.String(aq.Catalog)
	}
	resultConfiguration := &types.ResultConfiguration{
		OutputLocation: aws.String(aq.Bucket),
	}
	startQueryExecutionInput := &athena.StartQueryExecutionInput{
		QueryString:           aws.String(query),
		QueryExecutionContext: queryExecutionCtx,
		ResultConfiguration:   resultConfiguration,
	}

	// Only set if there is a value, the default input is nil
	if aq.Workgroup != "" {
		startQueryExecutionInput.WorkGroup = aws.String(aq.Workgroup)
	}

	// Create Athena Client
	cli, err := aq.GetAthenaClient()
	if err != nil {
		return fmt.Errorf("QueryAthenaPaginated: GetAthenaClient error: %s", err.Error())
	}

	// Query Athena
	startQueryExecutionOutput, err := cli.StartQueryExecution(ctx, startQueryExecutionInput)
	if err != nil {
		return fmt.Errorf("QueryAthenaPaginated: start query error: %s", err.Error())
	}
	err = waitForQueryToComplete(ctx, cli, startQueryExecutionOutput.QueryExecutionId)
	if err != nil {
		return fmt.Errorf("QueryAthenaPaginated: query execution error: %s", err.Error())
	}
	queryResultsInput := &athena.GetQueryResultsInput{
		QueryExecutionId: startQueryExecutionOutput.QueryExecutionId,
		MaxResults:       aws.Int32(1000), // this is the default value
	}
	getQueryResultsPaginator := athena.NewGetQueryResultsPaginator(cli, queryResultsInput)
	for getQueryResultsPaginator.HasMorePages() {
		pg, err := getQueryResultsPaginator.NextPage(ctx)
		if err != nil {
			log.Errorf("queryAthenaPaginated: NextPage error: %s", err.Error())
			continue
		}
		fn(pg)
	}
	return nil
}

func waitForQueryToComplete(ctx context.Context, client *athena.Client, queryExecutionID *string) error {
	inp := &athena.GetQueryExecutionInput{
		QueryExecutionId: queryExecutionID,
	}
	isQueryStillRunning := true
	for isQueryStillRunning {
		qe, err := client.GetQueryExecution(ctx, inp)
		if err != nil {
			return err
		}
		if qe.QueryExecution.Status.State == "SUCCEEDED" {
			isQueryStillRunning = false
			continue
		}
		if qe.QueryExecution.Status.State != "RUNNING" && qe.QueryExecution.Status.State != "QUEUED" {
			return fmt.Errorf("no query results available for query %s", *queryExecutionID)
		}
		time.Sleep(2 * time.Second)
	}
	return nil
}

// GetAthenaRowValue retrieve value from athena row based on column names and used stringutil.Bank() to prevent duplicate
// allocation of strings
func GetAthenaRowValue(row types.Row, queryColumnIndexes map[string]int, columnName string) string {
	columnIndex, ok := queryColumnIndexes[columnName]
	if !ok {
		return ""
	}
	valuePointer := row.Data[columnIndex].VarCharValue
	if valuePointer == nil {
		return ""
	}
	return stringutil.Bank(*valuePointer)
}

// getAthenaRowValueFloat retrieve value from athena row based on column names and convert to float if possible
func GetAthenaRowValueFloat(row types.Row, queryColumnIndexes map[string]int, columnName string) (float64, error) {

	columnIndex, ok := queryColumnIndexes[columnName]
	if !ok {
		return 0.0, fmt.Errorf("getAthenaRowValueFloat: missing column index: %s", columnName)
	}

	valuePointer := row.Data[columnIndex].VarCharValue
	if valuePointer == nil {
		return 0.0, fmt.Errorf("getAthenaRowValueFloat: nil field")
	}

	cost, err := strconv.ParseFloat(*valuePointer, 64)
	if err != nil {
		return cost, fmt.Errorf("getAthenaRowValueFloat: failed to parse %s: '%s': %s", columnName, *valuePointer, err.Error())
	}
	return cost, nil
}

func SelectAWSCategory(providerID, usageType, service string) string {
	// Network has the highest priority and is based on the usage type ending in "Bytes"
	if strings.HasSuffix(usageType, "Bytes") {
		return opencost.NetworkCategory
	}
	// The node and volume conditions are mutually exclusive.
	// Provider ID has prefix "i-"
	if strings.HasPrefix(providerID, "i-") {
		// GuardDuty has a ProviderID prefix of "i-", but should not be categorized as compute
		if strings.ToUpper(service) == "AMAZONGUARDDUTY" {
			return opencost.OtherCategory
		}
		return opencost.ComputeCategory
	}
	// Provider ID has prefix "vol-"
	if strings.HasPrefix(providerID, "vol-") {
		return opencost.StorageCategory
	}

	// Default categories based on service
	switch strings.ToUpper(service) {
	case "AWSELB", "AWSGLUE", "AMAZONROUTE53":
		return opencost.NetworkCategory
	case "AMAZONEC2", "AWSLAMBDA", "AMAZONELASTICACHE":
		return opencost.ComputeCategory
	case "AMAZONEKS":
		// Check if line item is a fargate pod
		if strings.Contains(providerID, ":pod/") {
			return opencost.ComputeCategory
		}
		return opencost.ManagementCategory
	case "AMAZONS3", "AMAZONATHENA", "AMAZONRDS", "AMAZONDYNAMODB", "AWSSECRETSMANAGER", "AMAZONFSX":
		return opencost.StorageCategory
	default:
		return opencost.OtherCategory
	}
}

var parseARNRx = regexp.MustCompile("^.+\\/(.+)?") // Capture "a406f7761142e4ef58a8f2ba478d2db2" from "arn:aws:elasticloadbalancing:us-east-1:297945954695:loadbalancer/a406f7761142e4ef58a8f2ba478d2db2"

func ParseARN(id string) string {
	match := parseARNRx.FindStringSubmatch(id)
	if len(match) == 0 {
		if id != "" {
			log.DedupedInfof(10, "aws.parseARN: failed to parse %s", id)
		}
		return id
	}
	return match[len(match)-1]
}

func GetAthenaQueryFunc(fn func(types.Row)) func(*athena.GetQueryResultsOutput) bool {
	pageNum := 0
	processItemQueryResults := func(page *athena.GetQueryResultsOutput) bool {
		if page == nil {
			log.Errorf("AthenaQuerier: Athena page is nil")
			return false
		} else if page.ResultSet == nil {
			log.Errorf("AthenaQuerier: Athena page.ResultSet is nil")
			return false
		}
		rows := page.ResultSet.Rows
		if pageNum == 0 {
			rows = page.ResultSet.Rows[1:len(page.ResultSet.Rows)]
		}

		for _, row := range rows {
			fn(row)
		}
		pageNum++
		return true
	}
	return processItemQueryResults
}
