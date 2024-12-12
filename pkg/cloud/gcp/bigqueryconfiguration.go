package gcp

import (
	"context"
	"fmt"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/pkg/cloud"
)

type BigQueryConfiguration struct {
	ProjectID  string     `json:"projectID"`
	Dataset    string     `json:"dataset"`
	Table      string     `json:"table"`
	Authorizer Authorizer `json:"authorizer"`
}

func (bqc *BigQueryConfiguration) Validate() error {

	if bqc.Authorizer == nil {
		return fmt.Errorf("BigQueryConfig: missing configurer")
	}

	err := bqc.Authorizer.Validate()
	if err != nil {
		return fmt.Errorf("BigQueryConfig: issue with GCP Authorizer: %s", err.Error())
	}

	if bqc.ProjectID == "" {
		return fmt.Errorf("BigQueryConfig: missing ProjectID")
	}

	if bqc.Dataset == "" {
		return fmt.Errorf("BigQueryConfig: missing Dataset")
	}

	if bqc.Table == "" {
		return fmt.Errorf("BigQueryConfig: missing Table")
	}

	return nil
}

func (bqc *BigQueryConfiguration) Equals(config cloud.Config) bool {
	if config == nil {
		return false
	}
	thatConfig, ok := config.(*BigQueryConfiguration)
	if !ok {
		return false
	}

	if bqc.Authorizer != nil {
		if !bqc.Authorizer.Equals(thatConfig.Authorizer) {
			return false
		}
	} else {
		if thatConfig.Authorizer != nil {
			return false
		}
	}

	if bqc.ProjectID != thatConfig.ProjectID {
		return false
	}

	if bqc.Dataset != thatConfig.Dataset {
		return false
	}

	if bqc.Table != thatConfig.Table {
		return false
	}

	return true
}

func (bqc *BigQueryConfiguration) Sanitize() cloud.Config {
	return &BigQueryConfiguration{
		ProjectID:  bqc.ProjectID,
		Dataset:    bqc.Dataset,
		Table:      bqc.Table,
		Authorizer: bqc.Authorizer.Sanitize().(Authorizer),
	}
}

// Key uses the Usage Project Id as the Provider Key for GCP
func (bqc *BigQueryConfiguration) Key() string {
	return fmt.Sprintf("%s/%s", bqc.ProjectID, bqc.GetBillingDataDataset())
}

func (bqc *BigQueryConfiguration) Provider() string {
	return opencost.GCPProvider
}

func (bqc *BigQueryConfiguration) GetBillingDataDataset() string {
	return fmt.Sprintf("%s.%s", bqc.Dataset, bqc.Table)
}

func (bqc *BigQueryConfiguration) GetBigQueryClient(ctx context.Context) (*bigquery.Client, error) {
	clientOpts, err := bqc.Authorizer.CreateGCPClientOptions()
	if err != nil {
		return nil, err
	}
	return bigquery.NewClient(ctx, bqc.ProjectID, clientOpts...)
}

// UnmarshalJSON assumes data is save as an BigQueryConfigurationDTO
func (bqc *BigQueryConfiguration) UnmarshalJSON(b []byte) error {
	var f interface{}
	err := json.Unmarshal(b, &f)
	if err != nil {
		return err
	}

	fmap := f.(map[string]interface{})

	projectID, err := cloud.GetInterfaceValue[string](fmap, "projectID")
	if err != nil {
		return fmt.Errorf("BigQueryConfiguration: FromInterface: %s", err.Error())
	}
	bqc.ProjectID = projectID

	dataset, err := cloud.GetInterfaceValue[string](fmap, "dataset")
	if err != nil {
		return fmt.Errorf("BigQueryConfiguration: FromInterface: %s", err.Error())
	}
	bqc.Dataset = dataset

	table, err := cloud.GetInterfaceValue[string](fmap, "table")
	if err != nil {
		return fmt.Errorf("BigQueryConfiguration: FromInterface: %s", err.Error())
	}
	bqc.Table = table

	authAny, ok := fmap["authorizer"]
	if !ok {
		return fmt.Errorf("StorageConfiguration: UnmarshalJSON: missing authorizer")
	}
	authorizer, err := cloud.AuthorizerFromInterface(authAny, SelectAuthorizerByType)
	if err != nil {
		return fmt.Errorf("StorageConfiguration: UnmarshalJSON: %s", err.Error())
	}
	bqc.Authorizer = authorizer
	return nil
}

func ConvertBigQueryConfigToConfig(bqc BigQueryConfig) cloud.KeyedConfig {
	if bqc.IsEmpty() {
		return nil
	}

	BillingDataDataset := strings.Split(bqc.BillingDataDataset, ".")
	dataset := BillingDataDataset[0]
	var table string
	if len(BillingDataDataset) > 1 {
		table = BillingDataDataset[1]
	}

	bigQueryConfiguration := &BigQueryConfiguration{
		ProjectID:  bqc.ProjectID,
		Dataset:    dataset,
		Table:      table,
		Authorizer: &WorkloadIdentity{}, // Default to WorkloadIdentity
	}

	if len(bqc.Key) != 0 {
		bigQueryConfiguration.Authorizer = &ServiceAccountKey{
			Key: bqc.Key,
		}
	}

	return bigQueryConfiguration
}
