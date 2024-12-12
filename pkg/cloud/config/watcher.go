package config

import (
	"fmt"
	"io/ioutil"
	"path"

	"github.com/opencost/opencost/pkg/cloud"
	"github.com/opencost/opencost/pkg/cloud/alibaba"
	"github.com/opencost/opencost/pkg/cloud/aws"
	"github.com/opencost/opencost/pkg/cloud/azure"
	"github.com/opencost/opencost/pkg/cloud/gcp"
	"github.com/opencost/opencost/pkg/cloud/models"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util/fileutil"
	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/pkg/env"
)

const authSecretPath = "/var/secrets/service-key.json"
const storageConfigSecretPath = "/var/azure-storage-config/azure-storage-config.json"
const cloudIntegrationSecretPath = "/cloud-integration/cloud-integration.json"

type HelmWatcher struct {
	providerConfig models.ProviderConfig
}

// GetConfigs checks secret files and config map set via the helm chart for Cloud Billing integrations. Returns
// only one billing integration due to values being shared by different configuration types.
func (hw *HelmWatcher) GetConfigs() []cloud.KeyedConfig {
	var configs []cloud.KeyedConfig

	customPricing, _ := hw.providerConfig.GetCustomPricingData()

	// check for Azure Storage config in secret file
	exists, err := fileutil.FileExists(storageConfigSecretPath)
	if err != nil {
		log.Errorf("HelmWatcher: AzureStorage: error checking file at '%s': %s", storageConfigSecretPath, err.Error())
	}

	// If file does not exist implies that this configuration method was not used
	if exists {
		result, err2 := ioutil.ReadFile(storageConfigSecretPath)
		if err2 != nil {
			log.Errorf("HelmWatcher: AzureStorage: Error reading file: %s", err2.Error())
			return nil
		}

		asc := &azure.AzureStorageConfig{}
		err2 = json.Unmarshal(result, asc)
		if err2 != nil {
			log.Errorf("HelmWatcher: AzureStorage: Error reading json: %s", err2.Error())
			return nil
		}
		if asc != nil && !asc.IsEmpty() {
			// If subscription id is not set it may be present in the rate card API
			if asc.SubscriptionId == "" {
				ask := &azure.AzureServiceKey{}
				err3 := loadFile(authSecretPath, ask)
				if err3 != nil {
					log.Errorf("HelmWatcher: AzureStorage: AzureRateCard: %s", err3)
				}
				if ask != nil {
					asc.SubscriptionId = ask.SubscriptionID
				}
			}
			// If SubscriptionID is still empty check the customPricing
			if asc.SubscriptionId == "" {
				asc.SubscriptionId = customPricing.AzureSubscriptionID
			}
			kc := azure.ConvertAzureStorageConfigToConfig(*asc)
			configs = append(configs, kc)
			return configs
		}

	}

	exists, err = fileutil.FileExists(authSecretPath)
	if err != nil {
		log.Errorf("HelmWatcher:  error checking file at '%s': %s", authSecretPath, err.Error())
	}

	// If the Auth Secret is not set then the config file watch will be responsible for providing the configurer for the
	// config values present in the CustomPricing object
	if exists {
		if customPricing.BillingDataDataset != "" {
			// Big Query Configuration
			bqc := gcp.BigQueryConfig{
				ProjectID:          customPricing.ProjectID,
				BillingDataDataset: customPricing.BillingDataDataset,
			}

			key := make(map[string]string)
			err2 := loadFile(authSecretPath, &key)
			if err2 != nil {
				log.Errorf("HelmWatcher: GCP: %s", err2)
			}
			if key != nil && len(key) != 0 {
				bqc.Key = key
			}

			kc := gcp.ConvertBigQueryConfigToConfig(bqc)
			configs = append(configs, kc)
			return configs
		}

		if customPricing.AthenaBucketName != "" {
			aai := aws.AwsAthenaInfo{
				AthenaBucketName: customPricing.AthenaBucketName,
				AthenaRegion:     customPricing.AthenaRegion,
				AthenaDatabase:   customPricing.AthenaDatabase,
				AthenaTable:      customPricing.AthenaTable,
				AthenaWorkgroup:  customPricing.AthenaWorkgroup,
				AccountID:        customPricing.AthenaProjectID,
				MasterPayerARN:   customPricing.MasterPayerARN,
			}

			// If Account ID is blank check ProjectID
			if aai.AccountID == "" {
				aai.AccountID = customPricing.ProjectID
			}

			var accessKey aws.AWSAccessKey
			err2 := loadFile(authSecretPath, &accessKey)
			if err2 != nil {
				log.Errorf("HelmWatcher: AWS: %s", err2)
			}

			aai.ServiceKeyName = accessKey.AccessKeyID
			aai.ServiceKeySecret = accessKey.SecretAccessKey

			kc := aws.ConvertAwsAthenaInfoToConfig(aai)
			configs = append(configs, kc)
			return configs

		}
	}

	return configs
}

type ConfigFileWatcher struct {
	providerConfig models.ProviderConfig
}

// GetConfigs checks secret files and config map set via the helm chart for Cloud Billing integrations. Returns
// only one billing integration due to values being shared by different configuration types.
func (cfw *ConfigFileWatcher) GetConfigs() []cloud.KeyedConfig {
	var configs []cloud.KeyedConfig

	customPricing, _ := cfw.providerConfig.GetCustomPricingData()

	// Detect Azure Storage configuration
	if customPricing.AzureSubscriptionID != "" {
		asc := azure.AzureStorageConfig{
			SubscriptionId: customPricing.AzureSubscriptionID,
			AccountName:    customPricing.AzureStorageAccount,
			AccessKey:      customPricing.AzureStorageAccessKey,
			ContainerName:  customPricing.AzureStorageContainer,
			ContainerPath:  customPricing.AzureContainerPath,
			AzureCloud:     customPricing.AzureCloud,
		}
		kc := azure.ConvertAzureStorageConfigToConfig(asc)
		configs = append(configs, kc)
		return configs

	}

	// Detect Big Query Configuration
	if customPricing.BillingDataDataset != "" {
		bqc := gcp.BigQueryConfig{
			ProjectID:          customPricing.ProjectID,
			BillingDataDataset: customPricing.BillingDataDataset,
		}

		var key map[string]string
		err2 := loadFile(env.GetConfigPathWithDefault("/models/")+"key.json", &key)
		if err2 != nil {
			log.Errorf("ConfigFileWatcher: GCP: %s", err2)
		}
		if key != nil && len(key) != 0 {
			bqc.Key = key
		}

		kc := gcp.ConvertBigQueryConfigToConfig(bqc)
		configs = append(configs, kc)
		return configs
	}

	// Detect AWS configuration
	if customPricing.AthenaBucketName != "" {
		aai := aws.AwsAthenaInfo{
			AthenaBucketName: customPricing.AthenaBucketName,
			AthenaRegion:     customPricing.AthenaRegion,
			AthenaDatabase:   customPricing.AthenaDatabase,
			AthenaTable:      customPricing.AthenaTable,
			AthenaWorkgroup:  customPricing.AthenaWorkgroup,
			ServiceKeyName:   customPricing.ServiceKeyName,
			ServiceKeySecret: customPricing.ServiceKeySecret,
			AccountID:        customPricing.AthenaProjectID,
			MasterPayerARN:   customPricing.MasterPayerARN,
		}

		// If Account ID is blank check ProjectID
		if aai.AccountID == "" {
			aai.AccountID = customPricing.ProjectID
		}

		// If the sample nil service key name is set, zero it out so that it is not
		// misinterpreted as a real service key.
		if aai.ServiceKeyName == "AKIXXX" {
			aai.ServiceKeyName = ""
		}

		kc := aws.ConvertAwsAthenaInfoToConfig(aai)
		configs = append(configs, kc)
		return configs
	}

	//detect Alibaba Configuration

	if customPricing.AlibabaClusterRegion != "" {
		aliCloudInfo := alibaba.AlibabaInfo{
			AlibabaClusterRegion:    customPricing.AlibabaClusterRegion,
			AlibabaServiceKeyName:   customPricing.AlibabaServiceKeyName,
			AlibabaServiceKeySecret: customPricing.AlibabaServiceKeySecret,
			AlibabaAccountID:        customPricing.ProjectID,
		}
		kc := alibaba.ConvertAlibabaInfoToConfig(aliCloudInfo)
		configs = append(configs, kc)
		return configs
	}
	return configs
}

// MultiCloudWatcher ingests values a MultiCloudConfig from the file pulled in from the secret by the helm chart
type MultiCloudWatcher struct {
}

func (mcw *MultiCloudWatcher) GetConfigs() []cloud.KeyedConfig {
	var multiConfigPath string

	if env.IsKubernetesEnabled() {
		multiConfigPath = path.Join(env.GetConfigPathWithDefault("/var/configs"), cloudIntegrationSecretPath)
	} else {
		multiConfigPath = env.GetCloudCostConfigPath()
	}
	exists, err := fileutil.FileExists(multiConfigPath)
	if err != nil {
		log.Errorf("MultiCloudWatcher:  error checking file at '%s': %s", multiConfigPath, err.Error())
	}

	// If config does not exist implies that this configuration method was not used
	if !exists {
		// check the original location of secret mount
		multiConfigPath = path.Join("/var", cloudIntegrationSecretPath)
		exists, err = fileutil.FileExists(multiConfigPath)
		if err != nil {
			log.Errorf("MultiCloudWatcher:  error checking file at '%s': %s", multiConfigPath, err.Error())
		}

		// If config does not exist implies that this configuration method was not used
		if !exists {
			return nil
		}
	}

	log.Debugf("MultiCloudWatcher GetConfigs: multiConfigPath: %s", multiConfigPath)

	configurations := &Configurations{}
	err = loadFile(multiConfigPath, configurations)
	if err != nil {
		log.Errorf("MultiCloudWatcher: Error getting file '%s': %s", multiConfigPath, err.Error())
		return nil
	}

	return configurations.ToSlice()
}

func GetCloudBillingWatchers(providerConfig models.ProviderConfig) map[ConfigSource]cloud.KeyedConfigWatcher {
	watchers := make(map[ConfigSource]cloud.KeyedConfigWatcher, 3)
	watchers[MultiCloudSource] = &MultiCloudWatcher{}
	if providerConfig != nil {
		watchers[HelmSource] = &HelmWatcher{providerConfig: providerConfig}
		watchers[ConfigFileSource] = &ConfigFileWatcher{providerConfig: providerConfig}
	}

	return watchers
}

// loadFile unmarshals the json content of a file into the provided object
// an empty return with no error indicates that the file did not exist.
func loadFile[T any](path string, content T) error {
	exists, err := fileutil.FileExists(path)
	if err != nil {
		return fmt.Errorf("loadFile: error checking file at '%s': %s", path, err.Error())
	}

	// If file does not exist implies that this configuration method was not used
	if !exists {
		return nil
	}

	result, err := ioutil.ReadFile(path)
	if err != nil {
		return fmt.Errorf("loadFile: Error reading file: %s", err.Error())
	}

	err = json.Unmarshal(result, content)
	if err != nil {
		return fmt.Errorf("loadFile: Error reading json: %s", err.Error())
	}

	return nil
}

// ConfigSource is an Enum of the sources int value of the Source determines its priority
type ConfigSource int

const (
	UnknownSource ConfigSource = iota
	ConfigControllerSource
	MultiCloudSource
	ConfigFileSource
	HelmSource
)

func GetConfigSource(str string) ConfigSource {
	switch str {
	case "configController":
		return ConfigControllerSource
	case "configfile":
		return ConfigFileSource
	case "helm":
		return HelmSource
	case "multicloud":
		return MultiCloudSource
	default:
		return UnknownSource
	}
}

func (cs ConfigSource) String() string {
	switch cs {
	case ConfigControllerSource:
		return "configController"
	case ConfigFileSource:
		return "configfile"
	case HelmSource:
		return "helm"
	case MultiCloudSource:
		return "multicloud"
	case UnknownSource:
		return "unknown"
	default:
		return "unknown"
	}
}
