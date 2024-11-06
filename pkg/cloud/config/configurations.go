package config

import (
	"encoding/json"
	"fmt"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/pkg/cloud"
	"github.com/opencost/opencost/pkg/cloud/alibaba"
	"github.com/opencost/opencost/pkg/cloud/aws"
	"github.com/opencost/opencost/pkg/cloud/azure"
	"github.com/opencost/opencost/pkg/cloud/gcp"
	"github.com/opencost/opencost/pkg/cloud/oracle"
)

// MultiCloudConfig struct is used to unmarshal cloud configs for each provider out of cloud-integration file
// Deprecated: v1.104 use Configurations
type MultiCloudConfig struct {
	AzureConfigs   []azure.AzureStorageConfig `json:"azure"`
	GCPConfigs     []gcp.BigQueryConfig       `json:"gcp"`
	AWSConfigs     []aws.AwsAthenaInfo        `json:"aws"`
	AlibabaConfigs []alibaba.AlibabaInfo      `json:"alibaba"`
}

func (mcc MultiCloudConfig) loadConfigurations(configs *Configurations) {
	// Load AWS configs
	for _, awsConfig := range mcc.AWSConfigs {
		kc := aws.ConvertAwsAthenaInfoToConfig(awsConfig)
		err := configs.Insert(kc)
		if err != nil {
			log.Errorf("MultiCloudConfig: error converting AWS config %s", err.Error())
		}

	}

	// Load GCP configs
	for _, gcpConfig := range mcc.GCPConfigs {
		kc := gcp.ConvertBigQueryConfigToConfig(gcpConfig)
		err := configs.Insert(kc)
		if err != nil {
			log.Errorf("MultiCloudConfig: error converting GCP config %s", err.Error())
		}
	}

	// Load Azure configs
	for _, azureConfig := range mcc.AzureConfigs {
		kc := azure.ConvertAzureStorageConfigToConfig(azureConfig)
		err := configs.Insert(kc)
		if err != nil {
			log.Errorf("MultiCloudConfig: error converting Azure config %s", err.Error())
		}
	}

	// Load Alibaba Cloud Configs
	for _, aliCloudConfig := range mcc.AlibabaConfigs {
		kc := alibaba.ConvertAlibabaInfoToConfig(aliCloudConfig)
		err := configs.Insert(kc)
		if err != nil {
			log.Errorf("MultiCloudConfig: error converting Alibaba config %s", err.Error())
		}
	}
}

// Configurations is a general use container for all configuration types
type Configurations struct {
	AWS     *AWSConfigs     `json:"aws,omitempty"`
	GCP     *GCPConfigs     `json:"gcp,omitempty"`
	Azure   *AzureConfigs   `json:"azure,omitempty"`
	Alibaba *AlibabaConfigs `json:"alibaba,omitempty"`
	OCI     *OCIConfigs     `json:"oci,omitempty"`
}

// UnmarshalJSON custom json unmarshalling to maintain support for MultiCloudConfig format
func (c *Configurations) UnmarshalJSON(bytes []byte) error {
	// This has been tested for backwards compatability, and it works in both config formats.
	// It also coincidentally works if you mix-and-match both the old format and the new
	// format.
	// Create inline type to gain access to default Unmarshalling
	type ConfUnmarshaller *Configurations
	var conf ConfUnmarshaller = c
	err := json.Unmarshal(bytes, conf)
	// If unmarshal is successful, return
	if err == nil {
		return nil
	}

	// Attempt to unmarshal into old config object
	multiConfig := &MultiCloudConfig{}
	err = json.Unmarshal(bytes, multiConfig)
	if err != nil {
		return err
	}
	multiConfig.loadConfigurations(c)
	return nil
}

func (c *Configurations) Equals(that *Configurations) bool {
	if c == nil && that == nil {
		return true
	}
	if c == nil || that == nil {
		return false
	}

	if !c.AWS.Equals(that.AWS) {
		return false
	}

	if !c.GCP.Equals(that.GCP) {
		return false
	}

	if !c.Azure.Equals(that.Azure) {
		return false
	}

	if !c.Alibaba.Equals(that.Alibaba) {
		return false
	}

	if !c.OCI.Equals(that.OCI) {
		return false
	}

	return true
}

func (c *Configurations) Insert(keyedConfig cloud.Config) error {
	switch keyedConfig.(type) {
	case *aws.AthenaConfiguration:
		if c.AWS == nil {
			c.AWS = &AWSConfigs{}
		}
		c.AWS.Athena = append(c.AWS.Athena, keyedConfig.(*aws.AthenaConfiguration))
	case *aws.S3Configuration:
		if c.AWS == nil {
			c.AWS = &AWSConfigs{}
		}
		c.AWS.S3 = append(c.AWS.S3, keyedConfig.(*aws.S3Configuration))
	case *gcp.BigQueryConfiguration:
		if c.GCP == nil {
			c.GCP = &GCPConfigs{}
		}
		c.GCP.BigQuery = append(c.GCP.BigQuery, keyedConfig.(*gcp.BigQueryConfiguration))
	case *azure.StorageConfiguration:
		if c.Azure == nil {
			c.Azure = &AzureConfigs{}
		}
		c.Azure.Storage = append(c.Azure.Storage, keyedConfig.(*azure.StorageConfiguration))
	case *alibaba.BOAConfiguration:
		if c.Alibaba == nil {
			c.Alibaba = &AlibabaConfigs{}
		}
		c.Alibaba.BOA = append(c.Alibaba.BOA, keyedConfig.(*alibaba.BOAConfiguration))
	case *oracle.UsageApiConfiguration:
		if c.OCI == nil {
			c.OCI = &OCIConfigs{}
		}
		c.OCI.UsageAPI = append(c.OCI.UsageAPI, keyedConfig.(*oracle.UsageApiConfiguration))
	default:
		return fmt.Errorf("Configurations: Insert: failed to insert config of type: %T", keyedConfig)
	}
	return nil
}

func (c *Configurations) ToSlice() []cloud.KeyedConfig {
	var keyedConfigs []cloud.KeyedConfig
	if c.AWS != nil {
		for _, athenaConfig := range c.AWS.Athena {
			keyedConfigs = append(keyedConfigs, athenaConfig)
		}

		for _, s3Config := range c.AWS.S3 {
			keyedConfigs = append(keyedConfigs, s3Config)
		}
	}

	if c.GCP != nil {
		for _, bigQueryConfig := range c.GCP.BigQuery {
			keyedConfigs = append(keyedConfigs, bigQueryConfig)
		}
	}

	if c.Azure != nil {
		for _, azureStorageConfig := range c.Azure.Storage {
			keyedConfigs = append(keyedConfigs, azureStorageConfig)
		}
	}

	if c.Alibaba != nil {
		for _, boaConfig := range c.Alibaba.BOA {
			keyedConfigs = append(keyedConfigs, boaConfig)
		}
	}

	if c.OCI != nil {
		for _, usageConfig := range c.OCI.UsageAPI {
			keyedConfigs = append(keyedConfigs, usageConfig)
		}
	}

	return keyedConfigs

}

type AWSConfigs struct {
	Athena []*aws.AthenaConfiguration `json:"athena,omitempty"`
	S3     []*aws.S3Configuration     `json:"s3,omitempty"`
}

func (ac *AWSConfigs) Equals(that *AWSConfigs) bool {
	if ac == nil && that == nil {
		return true
	}
	if ac == nil || that == nil {
		return false
	}
	// Check Athena
	if len(ac.Athena) != len(that.Athena) {
		return false
	}
	for i, thisAthena := range ac.Athena {
		thatAthena := that.Athena[i]
		if !thisAthena.Equals(thatAthena) {
			return false
		}
	}

	// Check S3
	if len(ac.S3) != len(that.S3) {
		return false
	}
	for i, thisS3 := range ac.S3 {
		thatS3 := that.S3[i]
		if !thisS3.Equals(thatS3) {
			return false
		}
	}

	return true
}

type GCPConfigs struct {
	BigQuery []*gcp.BigQueryConfiguration `json:"bigQuery,omitempty"`
}

func (gc *GCPConfigs) Equals(that *GCPConfigs) bool {
	if gc == nil && that == nil {
		return true
	}
	if gc == nil || that == nil {
		return false
	}
	// Check BigQuery
	if len(gc.BigQuery) != len(that.BigQuery) {
		return false
	}
	for i, thisBigQuery := range gc.BigQuery {
		thatBigQuery := that.BigQuery[i]
		if !thisBigQuery.Equals(thatBigQuery) {
			return false
		}
	}

	return true
}

type AzureConfigs struct {
	Storage []*azure.StorageConfiguration `json:"storage,omitempty"`
}

func (ac *AzureConfigs) Equals(that *AzureConfigs) bool {
	if ac == nil && that == nil {
		return true
	}
	if ac == nil || that == nil {
		return false
	}
	// Check Storage
	if len(ac.Storage) != len(that.Storage) {
		return false
	}
	for i, thisStorage := range ac.Storage {
		thatStorage := that.Storage[i]
		if !thisStorage.Equals(thatStorage) {
			return false
		}
	}

	return true
}

type AlibabaConfigs struct {
	BOA []*alibaba.BOAConfiguration `json:"boa,omitempty"`
}

func (ac *AlibabaConfigs) Equals(that *AlibabaConfigs) bool {
	if ac == nil && that == nil {
		return true
	}
	if ac == nil || that == nil {
		return false
	}
	// Check BOA
	if len(ac.BOA) != len(that.BOA) {
		return false
	}
	for i, thisBOA := range ac.BOA {
		thatBOA := that.BOA[i]
		if !thisBOA.Equals(thatBOA) {
			return false
		}
	}

	return true
}

type OCIConfigs struct {
	UsageAPI []*oracle.UsageApiConfiguration `json:"usageApi,omitempty"`
}

func (oc *OCIConfigs) Equals(that *OCIConfigs) bool {
	if oc == nil && that == nil {
		return true
	}
	if oc == nil || that == nil {
		return false
	}
	// Check Usage API
	if len(oc.UsageAPI) != len(that.UsageAPI) {
		return false
	}
	for i, thisUsageAPI := range oc.UsageAPI {
		thatUsageAPI := that.UsageAPI[i]
		if !thisUsageAPI.Equals(thatUsageAPI) {
			return false
		}
	}

	return true
}
