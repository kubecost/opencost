package config

import (
	"fmt"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/pkg/cloud"
)

type MockConfig struct {
}

func (mc *MockConfig) Validate() error {
	return nil
}

func (mc *MockConfig) Equals(config cloud.Config) bool {
	_, ok := config.(*MockConfig)
	return ok
}

func (mc *MockConfig) Sanitize() cloud.Config {
	return &MockConfig{}
}

// MockKeyedConfig implements KeyedConfig it only requires a key to be valid, there is an additional property allowing
// MockKeyedConfig with the same key to not be equal
type MockKeyedConfig struct {
	key      string
	property string
	valid    bool
}

func NewMockKeyedConfig(key, property string, valid bool) cloud.KeyedConfig {
	return &MockKeyedConfig{
		key:      key,
		property: property,
		valid:    valid,
	}
}

func (mkc *MockKeyedConfig) Validate() error {
	if !mkc.valid {
		return fmt.Errorf("MockKeyedConfig: set to invalid")
	}
	if mkc.key == "" {
		return fmt.Errorf("MockKeyedConfig: missing key")
	}
	return nil
}

func (mkc *MockKeyedConfig) Equals(config cloud.Config) bool {
	that, ok := config.(*MockKeyedConfig)
	if !ok {
		return false
	}

	if mkc.key != that.key {
		return false
	}

	if mkc.property != that.property {
		return false
	}

	if mkc.valid != that.valid {
		return false
	}

	return true
}

func (mkc *MockKeyedConfig) Sanitize() cloud.Config {
	return &MockKeyedConfig{
		key:      mkc.key,
		property: mkc.property,
		valid:    mkc.valid,
	}
}

func (mkc *MockKeyedConfig) Key() string {
	return mkc.key
}

func (mkc *MockKeyedConfig) Provider() string {
	return opencost.CustomProvider
}

type MockKeyedConfigWatcher struct {
	Integrations []cloud.KeyedConfig
}

func (mkcw *MockKeyedConfigWatcher) GetConfigs() []cloud.KeyedConfig {
	return mkcw.Integrations
}
