package azure

import (
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/pkg/cloud"
)

const DefaultCredentialAuthorizerType = "AzureDefaultCredential"

// Authorizer configs provide credentials from azidentity to connect to Azure services.
type Authorizer interface {
	cloud.Authorizer
	GetCredential() (azcore.TokenCredential, error)
}

// SelectAuthorizerByType is an implementation of AuthorizerSelectorFn and acts as a register for Authorizer types
func SelectAuthorizerByType(typeStr string) (Authorizer, error) {
	switch typeStr {
	case DefaultCredentialAuthorizerType:
		return &DefaultAzureCredentialHolder{}, nil
	default:
		return nil, fmt.Errorf("azure: provider authorizer type '%s' is not valid", typeStr)
	}
}

type DefaultAzureCredentialHolder struct{}

func (dac *DefaultAzureCredentialHolder) MarshalJSON() ([]byte, error) {
	fmap := make(map[string]any, 1)
	fmap[cloud.AuthorizerTypeProperty] = DefaultCredentialAuthorizerType

	return json.Marshal(fmap)
}

func (dac *DefaultAzureCredentialHolder) Validate() error {
	return nil
}

func (dac *DefaultAzureCredentialHolder) Equals(config cloud.Config) bool {
	if config == nil {
		return false
	}
	_, ok := config.(*DefaultAzureCredentialHolder)
	if !ok {
		return false
	}
	return true
}

func (dac *DefaultAzureCredentialHolder) Sanitize() cloud.Config {
	return &DefaultAzureCredentialHolder{}
}

func (dac *DefaultAzureCredentialHolder) GetCredential() (azcore.TokenCredential, error) {
	return azidentity.NewDefaultAzureCredential(nil)
}
