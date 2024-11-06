package config

import (
	"fmt"
	"strings"

	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/pkg/cloud"
	"github.com/opencost/opencost/pkg/cloud/aws"
	"github.com/opencost/opencost/pkg/cloud/azure"
	"github.com/opencost/opencost/pkg/cloud/gcp"
	"github.com/opencost/opencost/pkg/cloud/oracle"
)

const (
	S3ConfigType           = "s3"
	AthenaConfigType       = "athena"
	BigQueryConfigType     = "bigquery"
	AzureStorageConfigType = "azurestorage"
	UsageApiConfigType     = "usageapi"
)

func ConfigTypeFromConfig(config cloud.KeyedConfig) (string, error) {
	switch config.(type) {
	case *aws.S3Configuration:
		return S3ConfigType, nil
	case *aws.AthenaConfiguration:
		return AthenaConfigType, nil
	case *gcp.BigQueryConfiguration:
		return BigQueryConfigType, nil
	case *azure.StorageConfiguration:
		return AzureStorageConfigType, nil
	case *oracle.UsageApiConfiguration:
		return UsageApiConfigType, nil
	}
	return "", fmt.Errorf("failed to config type for config with key: %s, type %T", config.Key(), config)
}

type Statuses map[ConfigSource]map[string]*Status

func (s Statuses) Get(key string, source ConfigSource) (*Status, bool) {
	if _, ok := s[source]; !ok {
		return nil, false
	}
	status, ok := s[source][key]
	return status, ok
}

func (s Statuses) Insert(status *Status) {
	if _, ok := s[status.Source]; !ok {
		s[status.Source] = map[string]*Status{}
	}
	s[status.Source][status.Key] = status
}

func (s Statuses) List() []*Status {
	var list []*Status
	for _, statusesByKey := range s {
		for _, status := range statusesByKey {
			list = append(list, status)
		}
	}
	return list
}

type Status struct {
	Source     ConfigSource      `json:"source"`
	Key        string            `json:"key"`
	Active     bool              `json:"active"`
	Valid      bool              `json:"valid"`
	ConfigType string            `json:"configType"`
	Config     cloud.KeyedConfig `json:"config"`
}

func (s *Status) UnmarshalJSON(b []byte) error {
	var f interface{}
	err := json.Unmarshal(b, &f)
	if err != nil {
		return err
	}

	fmap := f.(map[string]interface{})

	sourceFloat, err := cloud.GetInterfaceValue[float64](fmap, "source")
	if err != nil {
		return fmt.Errorf("Status: UnmarshalJSON: %s", err.Error())
	}
	source := ConfigSource(int(sourceFloat))

	key, err := cloud.GetInterfaceValue[string](fmap, "key")
	if err != nil {
		return fmt.Errorf("Status: UnmarshalJSON: %s", err.Error())
	}

	active, err := cloud.GetInterfaceValue[bool](fmap, "active")
	if err != nil {
		return fmt.Errorf("Status: UnmarshalJSON: %s", err.Error())
	}

	valid, err := cloud.GetInterfaceValue[bool](fmap, "valid")
	if err != nil {
		return fmt.Errorf("Status: UnmarshalJSON: %s", err.Error())
	}

	configType, err := cloud.GetInterfaceValue[string](fmap, "configType")
	if err != nil {
		return fmt.Errorf("Status: UnmarshalJSON: %s", err.Error())
	}

	// Pick correct implementation to unmarshal into
	var config cloud.KeyedConfig
	switch strings.ToLower(configType) {
	case S3ConfigType:
		config = &aws.S3Configuration{}
	case AthenaConfigType:
		config = &aws.AthenaConfiguration{}
	case BigQueryConfigType:
		config = &gcp.BigQueryConfiguration{}
	case AzureStorageConfigType:
		config = &azure.StorageConfiguration{}
	case UsageApiConfigType:
		config = &oracle.UsageApiConfiguration{}
	default:
		return fmt.Errorf("Status: UnmarshalJSON: config type '%s' is not recognized", configType)
	}

	configAny, err := cloud.GetInterfaceValue[any](fmap, "config")
	if err != nil {
		return fmt.Errorf("Status: UnmarshalJSON: %s", err.Error())
	}

	// convert the interface back to a []Byte so that it can be unmarshalled into the correct type
	fBin, err := json.Marshal(configAny)
	if err != nil {
		return fmt.Errorf("Status: UnmarshalJSON: could not marshal value %v: %w", f, err)
	}

	err = json.Unmarshal(fBin, config)
	if err != nil {
		return fmt.Errorf("Status: UnmarshalJSON: failed to unmarshal into Configuration type %T from value %v: %w", config, f, err)
	}

	// Set Values
	s.Source = source
	s.Key = key
	s.Active = active
	s.Valid = valid
	s.ConfigType = configType
	s.Config = config
	return nil
}
