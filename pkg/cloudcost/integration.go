package cloudcost

import (
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/pkg/cloud"
	"github.com/opencost/opencost/pkg/cloud/alibaba"
	"github.com/opencost/opencost/pkg/cloud/aws"
	"github.com/opencost/opencost/pkg/cloud/azure"
	"github.com/opencost/opencost/pkg/cloud/gcp"
	"github.com/opencost/opencost/pkg/cloud/oracle"
)

// CloudCostIntegration is an interface for retrieving daily granularity CloudCost data for a given range
type CloudCostIntegration interface {
	GetCloudCost(time.Time, time.Time) (*opencost.CloudCostSetRange, error)
	GetStatus() cloud.ConnectionStatus
}

// GetIntegrationFromConfig coverts any valid KeyedConfig into the appropriate BillingIntegration if possible
func GetIntegrationFromConfig(kc cloud.KeyedConfig) CloudCostIntegration {
	switch keyedConfig := kc.(type) {
	// AthenaIntegration
	case *aws.AthenaConfiguration:
		return &aws.AthenaIntegration{
			AthenaQuerier: aws.AthenaQuerier{
				AthenaConfiguration: *keyedConfig,
			},
		}
	case *aws.AthenaQuerier:
		return &aws.AthenaIntegration{
			AthenaQuerier: *keyedConfig,
		}
	case *aws.AthenaIntegration:
		return keyedConfig
	// BigQueryIntegration
	case *gcp.BigQueryConfiguration:
		return &gcp.BigQueryIntegration{
			BigQueryQuerier: gcp.BigQueryQuerier{
				BigQueryConfiguration: *keyedConfig,
			},
		}
	case *gcp.BigQueryQuerier:
		return &gcp.BigQueryIntegration{
			BigQueryQuerier: *keyedConfig,
		}
	case *gcp.BigQueryIntegration:
		return keyedConfig
	// AzureStorageIntegration
	case *azure.StorageConfiguration:
		return &azure.AzureStorageIntegration{
			AzureStorageBillingParser: azure.AzureStorageBillingParser{
				StorageConnection: azure.StorageConnection{
					StorageConfiguration: *keyedConfig},
			},
		}
	case *azure.StorageConnection:
		return &azure.AzureStorageIntegration{
			AzureStorageBillingParser: azure.AzureStorageBillingParser{
				StorageConnection: azure.StorageConnection{
					StorageConfiguration: keyedConfig.StorageConfiguration},
			},
		}
	case *azure.AzureStorageBillingParser:
		return &azure.AzureStorageIntegration{
			AzureStorageBillingParser: azure.AzureStorageBillingParser{
				StorageConnection: azure.StorageConnection{
					StorageConfiguration: keyedConfig.StorageConfiguration},
			},
		}
	case *azure.AzureStorageIntegration:
		return &azure.AzureStorageIntegration{
			AzureStorageBillingParser: azure.AzureStorageBillingParser{
				StorageConnection: azure.StorageConnection{
					StorageConfiguration: keyedConfig.StorageConfiguration},
			},
		}
	// S3SelectIntegration
	case *aws.S3Configuration:
		return &aws.S3SelectIntegration{
			S3SelectQuerier: aws.S3SelectQuerier{
				S3Connection: aws.S3Connection{
					S3Configuration: *keyedConfig,
				},
			},
		}
	case *aws.S3Connection:
		return &aws.S3SelectIntegration{
			S3SelectQuerier: aws.S3SelectQuerier{
				S3Connection: *keyedConfig,
			},
		}
	case *aws.S3SelectQuerier:
		return &aws.S3SelectIntegration{
			S3SelectQuerier: *keyedConfig,
		}
	case *aws.S3SelectIntegration:
		return keyedConfig
	// Alibaba BOA Integration
	case *alibaba.BOAConfiguration:
		return nil
	case *oracle.UsageApiConfiguration:
		return &oracle.UsageApiIntegration{
			UsageApiConfiguration: *keyedConfig,
		}
	default:
		return nil
	}
}
