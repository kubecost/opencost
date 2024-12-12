package oracle

import (
	"encoding/json"
	"fmt"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/pkg/cloud"
	"github.com/oracle/oci-go-sdk/v65/usageapi"
)

type UsageApiConfiguration struct {
	TenancyID  string     `json:"tenancyID"`
	Region     string     `json:"region"`
	Authorizer Authorizer `json:"authorizer"`
}

func (uac *UsageApiConfiguration) Validate() error {
	// Validate Authorizer
	if uac.Authorizer == nil {
		return fmt.Errorf("UsageApiConfiguration: missing Authorizer")
	}

	err := uac.Authorizer.Validate()
	if err != nil {
		return fmt.Errorf("UsageApiConfiguration: %s", err)
	}

	// Validate base properties
	if uac.TenancyID == "" {
		return fmt.Errorf("UsageApiConfiguration: missing tenancyID")
	}

	if uac.Region == "" {
		return fmt.Errorf("UsageApiConfiguration: missing region")
	}

	return nil
}

func (uac *UsageApiConfiguration) Equals(config cloud.Config) bool {
	if config == nil {
		return false
	}
	thatConfig, ok := config.(*UsageApiConfiguration)
	if !ok {
		return false
	}

	if uac.Authorizer != nil {
		if !uac.Authorizer.Equals(thatConfig.Authorizer) {
			return false
		}
	} else {
		if thatConfig.Authorizer != nil {
			return false
		}
	}

	if uac.TenancyID != thatConfig.TenancyID {
		return false
	}

	if uac.Region != thatConfig.Region {
		return false
	}

	return true
}

func (uac *UsageApiConfiguration) Sanitize() cloud.Config {
	return &UsageApiConfiguration{
		TenancyID:  uac.TenancyID,
		Region:     uac.Region,
		Authorizer: uac.Authorizer.Sanitize().(Authorizer),
	}
}

func (uac *UsageApiConfiguration) Key() string {
	return uac.TenancyID
}

func (uac *UsageApiConfiguration) Provider() string {
	return opencost.OracleProvider
}

func (uac *UsageApiConfiguration) GetUsageApiClient() (*usageapi.UsageapiClient, error) {
	configProvider, err := uac.Authorizer.CreateOCIConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to create oci config: %s", err.Error())
	}
	client, err := usageapi.NewUsageapiClientWithConfigurationProvider(configProvider)
	if err != nil {
		return nil, fmt.Errorf("failed to create usage api client: %s", err.Error())
	}
	return &client, nil
}

func (uac *UsageApiConfiguration) UnmarshalJSON(b []byte) error {
	var f interface{}
	err := json.Unmarshal(b, &f)
	if err != nil {
		return err
	}

	fmap := f.(map[string]interface{})

	tenancyId, err := cloud.GetInterfaceValue[string](fmap, "tenancyID")
	if err != nil {
		return fmt.Errorf("UsageApiConfiguration: UnmarshalJSON: %w", err)
	}
	uac.TenancyID = tenancyId

	region, err := cloud.GetInterfaceValue[string](fmap, "region")
	if err != nil {
		return fmt.Errorf("UsageApiConfiguration: UnmarshalJSON: %w", err)
	}
	uac.Region = region

	authAny, ok := fmap["authorizer"]
	if !ok {
		return fmt.Errorf("UsageApiConfiguration: UnmarshalJSON: missing authorizer")
	}
	authorizer, err := cloud.AuthorizerFromInterface(authAny, SelectAuthorizerByType)
	if err != nil {
		return fmt.Errorf("UsageApiConfiguration: UnmarshalJSON: %w", err)
	}
	uac.Authorizer = authorizer

	return nil
}
