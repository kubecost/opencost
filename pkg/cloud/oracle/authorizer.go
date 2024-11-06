package oracle

import (
	"encoding/json"
	"fmt"

	"github.com/opencost/opencost/pkg/cloud"
	"github.com/oracle/oci-go-sdk/v65/common"
)

const RawConfigProviderAuthorizerType = "OCIRawConfigProvider"

// Authorizer provides which is used in when creating clients in the OCI SDK
type Authorizer interface {
	cloud.Authorizer
	CreateOCIConfig() (common.ConfigurationProvider, error)
}

// SelectAuthorizerByType is an implementation of AuthorizerSelectorFn and acts as a register for Authorizer types
func SelectAuthorizerByType(typeStr string) (Authorizer, error) {
	switch typeStr {
	case RawConfigProviderAuthorizerType:
		return &RawConfigProvider{}, nil
	default:
		return nil, fmt.Errorf("OCI: provider authorizer type '%s' is not valid", typeStr)
	}
}

// RawConfigProvider holds OCI credentials and fulfils the common.ConfigurationProvider interface
type RawConfigProvider struct {
	TenancyID            string  `json:"tenancyID"`
	UserID               string  `json:"userID"`
	Region               string  `json:"region"`
	Fingerprint          string  `json:"fingerprint"`
	PrivateKey           string  `json:"privateKey"`
	PrivateKeyPassphrase *string `json:"privateKeyPassphrase"`
}

// MarshalJSON custom json marshalling functions, sets properties as tagged in struct and sets the authorizer type property
func (ak *RawConfigProvider) MarshalJSON() ([]byte, error) {
	fmap := make(map[string]any, 6)
	fmap[cloud.AuthorizerTypeProperty] = RawConfigProviderAuthorizerType
	fmap["tenancyId"] = ak.TenancyID
	fmap["userId"] = ak.UserID
	fmap["region"] = ak.Region
	fmap["fingerprint"] = ak.Fingerprint
	fmap["privateKey"] = ak.PrivateKey
	fmap["privateKeyPassphrase"] = ak.PrivateKeyPassphrase
	return json.Marshal(fmap)
}

func (ak *RawConfigProvider) Validate() error {
	if ak.TenancyID == "" {
		return fmt.Errorf("RawConfigProvider: missing tenancy ID")
	}
	if ak.UserID == "" {
		return fmt.Errorf("RawConfigProvider: missing user ID")
	}
	if ak.Fingerprint == "" {
		return fmt.Errorf("RawConfigProvider: missing key fingerprint")
	}
	if ak.Region == "" {
		return fmt.Errorf("RawConfigProvider: missing region")
	}
	if ak.PrivateKey == "" {
		return fmt.Errorf("RawConfigProvider: missing private key")
	}
	if ak.PrivateKeyPassphrase != nil {
		if *ak.PrivateKeyPassphrase == "" {
			return fmt.Errorf("RawConfigProvider: missing private key passphrase")
		}
	}

	return nil
}

func (ak *RawConfigProvider) Equals(config cloud.Config) bool {
	if config == nil {
		return false
	}
	thatConfig, ok := config.(*RawConfigProvider)
	if !ok {
		return false
	}

	if ak.TenancyID != thatConfig.TenancyID {
		return false
	}
	if ak.UserID != thatConfig.UserID {
		return false
	}
	if ak.Fingerprint != thatConfig.Fingerprint {
		return false
	}
	if ak.Region != thatConfig.Region {
		return false
	}
	if ak.PrivateKey != thatConfig.PrivateKey {
		return false
	}
	if ak.PrivateKeyPassphrase == nil && thatConfig.PrivateKeyPassphrase != nil {
		return false
	}
	if ak.PrivateKeyPassphrase != nil && thatConfig.PrivateKeyPassphrase == nil {
		return false
	}
	if ak.PrivateKeyPassphrase != nil && thatConfig.PrivateKeyPassphrase != nil {
		if *ak.PrivateKeyPassphrase != *thatConfig.PrivateKeyPassphrase {
			return false
		}
	}

	return true
}

func (ak *RawConfigProvider) Sanitize() cloud.Config {
	redacted := cloud.Redacted
	return &RawConfigProvider{
		TenancyID:            ak.TenancyID,
		UserID:               ak.UserID,
		Fingerprint:          ak.Fingerprint,
		Region:               ak.Region,
		PrivateKey:           cloud.Redacted,
		PrivateKeyPassphrase: &redacted,
	}
}

func (ak *RawConfigProvider) CreateOCIConfig() (common.ConfigurationProvider, error) {
	return common.NewRawConfigurationProvider(ak.TenancyID, ak.UserID, ak.Region, ak.Fingerprint, ak.PrivateKey, ak.PrivateKeyPassphrase), nil
}
