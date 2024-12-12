package config

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	cloudconfig "github.com/opencost/opencost/pkg/cloud"
	"github.com/opencost/opencost/pkg/cloud/aws"
	"github.com/opencost/opencost/pkg/cloud/gcp"
)

// Baseline valid config
var validAthenaConf = &aws.AthenaConfiguration{
	Bucket:     "bucket",
	Region:     "region",
	Database:   "database",
	Table:      "table",
	Workgroup:  "workgroup",
	Account:    "account",
	Authorizer: &aws.ServiceAccount{},
}

// Config with the same key as the baseline but is not equal to it because of the change in the non-keyed property Workgroup
var validAthenaConfModifiedProperty = &aws.AthenaConfiguration{
	Bucket:     "bucket",
	Region:     "region",
	Database:   "database",
	Table:      "table",
	Workgroup:  "workgroup1",
	Account:    "account",
	Authorizer: &aws.ServiceAccount{},
}

// Config with the same key as baseline but is invalid due to missing Authorizer
var invalidAthenaConf = &aws.AthenaConfiguration{
	Bucket:     "bucket",
	Region:     "region",
	Database:   "database",
	Table:      "table",
	Workgroup:  "workgroup",
	Account:    "account",
	Authorizer: nil,
}

// A valid config with a different key from the baseline
var validBigQueryConf = &gcp.BigQueryConfiguration{
	ProjectID:  "projectID",
	Dataset:    "dataset",
	Table:      "table",
	Authorizer: &gcp.WorkloadIdentity{},
}

func TestIntegrationController_pullWatchers(t *testing.T) {
	testCases := map[string]struct {
		initialStatuses  []*Status
		configWatchers   map[ConfigSource]cloudconfig.KeyedConfigWatcher
		expectedStatuses []*Status
	}{
		// Helm Source
		"Helm Source init": {
			initialStatuses: []*Status{},
			configWatchers: map[ConfigSource]cloudconfig.KeyedConfigWatcher{
				HelmSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						validAthenaConf,
					},
				},
			},
			expectedStatuses: []*Status{
				{
					Source:     HelmSource,
					Key:        validAthenaConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
			},
		},
		"Helm Source No Change": {
			initialStatuses: []*Status{
				{
					Source:     HelmSource,
					Key:        validAthenaConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
			},
			configWatchers: map[ConfigSource]cloudconfig.KeyedConfigWatcher{
				HelmSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						validAthenaConf,
					},
				},
			},
			expectedStatuses: []*Status{
				{
					Source:     HelmSource,
					Key:        validAthenaConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
			},
		},
		"Helm Source Update Config": {
			initialStatuses: []*Status{
				{
					Source:     HelmSource,
					Key:        validAthenaConfModifiedProperty.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConfModifiedProperty,
				},
			},
			configWatchers: map[ConfigSource]cloudconfig.KeyedConfigWatcher{
				HelmSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						validAthenaConf,
					},
				},
			},
			expectedStatuses: []*Status{
				{
					Source:     HelmSource,
					Key:        validAthenaConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
			},
		},
		"Helm Source Update Config Invalid": {
			initialStatuses: []*Status{
				{
					Source:     HelmSource,
					Key:        validAthenaConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
			},
			configWatchers: map[ConfigSource]cloudconfig.KeyedConfigWatcher{
				HelmSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						invalidAthenaConf,
					},
				},
			},
			expectedStatuses: []*Status{
				{
					Source:     HelmSource,
					Key:        invalidAthenaConf.Key(),
					Active:     false,
					Valid:      false,
					ConfigType: AthenaConfigType,
					Config:     invalidAthenaConf,
				},
			},
		},
		"Helm Source New Config": {
			initialStatuses: []*Status{
				{
					Source:     HelmSource,
					Key:        validAthenaConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
			},
			configWatchers: map[ConfigSource]cloudconfig.KeyedConfigWatcher{
				HelmSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						validBigQueryConf,
					},
				},
			},
			expectedStatuses: []*Status{
				{
					Source:     HelmSource,
					Key:        validBigQueryConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validBigQueryConf,
				},
			},
		},
		// Config File
		"Config File Source init": {
			initialStatuses: []*Status{},
			configWatchers: map[ConfigSource]cloudconfig.KeyedConfigWatcher{
				ConfigFileSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						validAthenaConf,
					},
				},
			},
			expectedStatuses: []*Status{
				{
					Source:     ConfigFileSource,
					Key:        validAthenaConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
			},
		},
		"Config File No Change": {
			initialStatuses: []*Status{
				{
					Source:     ConfigFileSource,
					Key:        validAthenaConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
			},
			configWatchers: map[ConfigSource]cloudconfig.KeyedConfigWatcher{
				ConfigFileSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						validAthenaConf,
					},
				},
			},
			expectedStatuses: []*Status{
				{
					Source:     ConfigFileSource,
					Key:        validAthenaConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
			},
		},
		"Config File Update Config": {
			initialStatuses: []*Status{
				{
					Source:     ConfigFileSource,
					Key:        validAthenaConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
			},
			configWatchers: map[ConfigSource]cloudconfig.KeyedConfigWatcher{
				ConfigFileSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						validAthenaConf,
					},
				},
			},
			expectedStatuses: []*Status{
				{
					Source:     ConfigFileSource,
					Key:        validAthenaConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
			},
		},
		"Config File Update Config Invalid": {
			initialStatuses: []*Status{
				{
					Source:     ConfigFileSource,
					Key:        validAthenaConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
			},
			configWatchers: map[ConfigSource]cloudconfig.KeyedConfigWatcher{
				ConfigFileSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						invalidAthenaConf,
					},
				},
			},
			expectedStatuses: []*Status{
				{
					Source:     ConfigFileSource,
					Key:        invalidAthenaConf.Key(),
					Active:     false,
					Valid:      false,
					ConfigType: AthenaConfigType,
					Config:     invalidAthenaConf,
				},
			},
		},
		"Config File New Config": {
			initialStatuses: []*Status{
				{
					Source:     ConfigFileSource,
					Key:        validAthenaConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
			},
			configWatchers: map[ConfigSource]cloudconfig.KeyedConfigWatcher{
				ConfigFileSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						validBigQueryConf,
					},
				},
			},
			expectedStatuses: []*Status{
				{
					Source:     ConfigFileSource,
					Key:        validBigQueryConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: BigQueryConfigType,
					Config:     validBigQueryConf,
				},
			},
		},
		// Multi Cloud
		"Multi Cloud Source init": {
			initialStatuses: []*Status{},
			configWatchers: map[ConfigSource]cloudconfig.KeyedConfigWatcher{
				MultiCloudSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						validAthenaConf,
					},
				},
			},
			expectedStatuses: []*Status{
				{
					Source:     MultiCloudSource,
					Key:        validAthenaConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
			},
		},
		"Multi Cloud No Change": {
			initialStatuses: []*Status{
				{
					Source:     MultiCloudSource,
					Key:        validAthenaConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
			},
			configWatchers: map[ConfigSource]cloudconfig.KeyedConfigWatcher{
				MultiCloudSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						validAthenaConf,
					},
				},
			},
			expectedStatuses: []*Status{
				{
					Source:     MultiCloudSource,
					Key:        validAthenaConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
			},
		},
		"Multi Cloud Update Config": {
			initialStatuses: []*Status{
				{
					Source:     MultiCloudSource,
					Key:        validAthenaConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
			},
			configWatchers: map[ConfigSource]cloudconfig.KeyedConfigWatcher{
				MultiCloudSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						validAthenaConfModifiedProperty,
					},
				},
			},
			expectedStatuses: []*Status{
				{
					Source:     MultiCloudSource,
					Key:        validAthenaConfModifiedProperty.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConfModifiedProperty,
				},
			},
		},
		"Multi Cloud Update Config Invalid": {
			initialStatuses: []*Status{
				{
					Source:     MultiCloudSource,
					Key:        validAthenaConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
			},
			configWatchers: map[ConfigSource]cloudconfig.KeyedConfigWatcher{
				MultiCloudSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						invalidAthenaConf,
					},
				},
			},
			expectedStatuses: []*Status{
				{
					Source:     MultiCloudSource,
					Key:        invalidAthenaConf.Key(),
					Active:     false,
					Valid:      false,
					ConfigType: AthenaConfigType,
					Config:     invalidAthenaConf,
				},
			},
		},
		"Multi Cloud New Config": {
			initialStatuses: []*Status{
				{
					Source:     MultiCloudSource,
					Key:        validAthenaConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
			},
			configWatchers: map[ConfigSource]cloudconfig.KeyedConfigWatcher{
				MultiCloudSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						validBigQueryConf,
					},
				},
			},
			expectedStatuses: []*Status{
				{
					Source:     MultiCloudSource,
					Key:        validBigQueryConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: BigQueryConfigType,
					Config:     validBigQueryConf,
				},
			},
		},
		"Multi Cloud Delete All": {
			initialStatuses: []*Status{
				{
					Source:     MultiCloudSource,
					Key:        validAthenaConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
			},
			configWatchers: map[ConfigSource]cloudconfig.KeyedConfigWatcher{
				MultiCloudSource: &MockKeyedConfigWatcher{},
			},
			expectedStatuses: []*Status{},
		},
		// Watch Interaction
		"New Helm, Existing Config File": {
			initialStatuses: []*Status{
				{
					Source:     ConfigFileSource,
					Key:        validAthenaConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
			},
			configWatchers: map[ConfigSource]cloudconfig.KeyedConfigWatcher{
				ConfigFileSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						validAthenaConf,
					},
				},
				HelmSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						validBigQueryConf,
					},
				},
			},
			expectedStatuses: []*Status{
				{
					Source:     ConfigFileSource,
					Key:        validAthenaConf.Key(),
					Active:     false,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
				{
					Source:     HelmSource,
					Key:        validBigQueryConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: BigQueryConfigType,
					Config:     validBigQueryConf,
				},
			},
		},
		"Update Helm, Existing Config File": {
			initialStatuses: []*Status{
				{
					Source:     HelmSource,
					Key:        validAthenaConf.Key(),
					Active:     false,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
				{
					Source:     ConfigFileSource,
					Key:        validBigQueryConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: BigQueryConfigType,
					Config:     validBigQueryConf,
				},
			},
			configWatchers: map[ConfigSource]cloudconfig.KeyedConfigWatcher{
				ConfigFileSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						validBigQueryConf,
					},
				},
				HelmSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						validAthenaConfModifiedProperty,
					},
				},
			},
			expectedStatuses: []*Status{
				{
					Source:     HelmSource,
					Key:        validAthenaConfModifiedProperty.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConfModifiedProperty,
				},
				{
					Source:     ConfigFileSource,
					Key:        validBigQueryConf.Key(),
					Active:     false,
					Valid:      true,
					ConfigType: BigQueryConfigType,
					Config:     validBigQueryConf,
				},
			},
		},
		"New Helm Invalid, Existing Config File": {
			initialStatuses: []*Status{
				{
					Source:     ConfigFileSource,
					Key:        validBigQueryConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: BigQueryConfigType,
					Config:     validBigQueryConf,
				},
			},
			configWatchers: map[ConfigSource]cloudconfig.KeyedConfigWatcher{
				ConfigFileSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						validBigQueryConf,
					},
				},
				HelmSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						invalidAthenaConf,
					},
				},
			},
			expectedStatuses: []*Status{
				{
					Source:     ConfigFileSource,
					Key:        validBigQueryConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: BigQueryConfigType,
					Config:     validBigQueryConf,
				},
				{
					Source:     HelmSource,
					Key:        invalidAthenaConf.Key(),
					Active:     false,
					Valid:      false,
					ConfigType: AthenaConfigType,
					Config:     invalidAthenaConf,
				},
			},
		},
		"Update Helm Invalid, Existing Config File": {
			initialStatuses: []*Status{
				{
					Source:     ConfigFileSource,
					Key:        validBigQueryConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: BigQueryConfigType,
					Config:     validBigQueryConf,
				},
				{
					Source:     HelmSource,
					Key:        validAthenaConf.Key(),
					Active:     false,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
			},
			configWatchers: map[ConfigSource]cloudconfig.KeyedConfigWatcher{
				ConfigFileSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						validBigQueryConf,
					},
				},
				HelmSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						invalidAthenaConf,
					},
				},
			},
			expectedStatuses: []*Status{
				{
					Source:     ConfigFileSource,
					Key:        validBigQueryConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: BigQueryConfigType,
					Config:     validBigQueryConf,
				},
				{
					Source:     HelmSource,
					Key:        invalidAthenaConf.Key(),
					Active:     false,
					Valid:      false,
					ConfigType: AthenaConfigType,
					Config:     invalidAthenaConf,
				},
			},
		},
		"New Config File, Existing Helm": {
			initialStatuses: []*Status{
				{
					Source:     HelmSource,
					Key:        validAthenaConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
			},
			configWatchers: map[ConfigSource]cloudconfig.KeyedConfigWatcher{
				HelmSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						validAthenaConf,
					},
				},
				ConfigFileSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						validBigQueryConf,
					},
				},
			},
			expectedStatuses: []*Status{
				{
					Source:     HelmSource,
					Key:        validAthenaConf.Key(),
					Active:     false,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
				{
					Source:     ConfigFileSource,
					Key:        validBigQueryConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: BigQueryConfigType,
					Config:     validBigQueryConf,
				},
			},
		},
		"Update Config File, Existing Helm": {
			initialStatuses: []*Status{
				{
					Source:     HelmSource,
					Key:        validBigQueryConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: BigQueryConfigType,
					Config:     validBigQueryConf,
				},
			},
			configWatchers: map[ConfigSource]cloudconfig.KeyedConfigWatcher{
				HelmSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{},
				},
				ConfigFileSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						validAthenaConfModifiedProperty,
					},
				},
			},
			expectedStatuses: []*Status{
				{
					Source:     ConfigFileSource,
					Key:        validAthenaConfModifiedProperty.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConfModifiedProperty,
				},
			},
		},
		"New Config File Invalid, Existing Helm": {
			initialStatuses: []*Status{
				{
					Source:     HelmSource,
					Key:        validBigQueryConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: BigQueryConfigType,
					Config:     validBigQueryConf,
				},
			},
			configWatchers: map[ConfigSource]cloudconfig.KeyedConfigWatcher{
				HelmSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						validBigQueryConf,
					},
				},
				ConfigFileSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						invalidAthenaConf,
					},
				},
			},
			expectedStatuses: []*Status{
				{
					Source:     HelmSource,
					Key:        validBigQueryConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: BigQueryConfigType,
					Config:     validBigQueryConf,
				},
				{
					Source:     ConfigFileSource,
					Key:        invalidAthenaConf.Key(),
					Active:     false,
					Valid:      false,
					ConfigType: AthenaConfigType,
					Config:     invalidAthenaConf,
				},
			},
		},
		"Update Config File Invalid, Existing Helm": {
			initialStatuses: []*Status{
				{
					Source:     HelmSource,
					Key:        validBigQueryConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: BigQueryConfigType,
					Config:     validBigQueryConf,
				},
				{
					Source:     ConfigFileSource,
					Key:        validAthenaConf.Key(),
					Active:     false,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
			},
			configWatchers: map[ConfigSource]cloudconfig.KeyedConfigWatcher{
				HelmSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						validBigQueryConf,
					},
				},
				ConfigFileSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						invalidAthenaConf,
					},
				},
			},
			expectedStatuses: []*Status{
				{
					Source:     HelmSource,
					Key:        validBigQueryConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: BigQueryConfigType,
					Config:     validBigQueryConf,
				},
				{
					Source:     ConfigFileSource,
					Key:        invalidAthenaConf.Key(),
					Active:     false,
					Valid:      false,
					ConfigType: AthenaConfigType,
					Config:     invalidAthenaConf,
				},
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			// Test set up and validation
			initialStatuses, err := buildStatuses(tc.initialStatuses)
			if err != nil {
				t.Errorf("initial statuses: %s", err.Error())
			}

			expectedStatuses, err := buildStatuses(tc.expectedStatuses)
			if err != nil {
				t.Errorf("initial statuses: %s", err.Error())
			}

			tempDir := os.TempDir()
			path := filepath.Join(tempDir, configFile)
			defer os.Remove(path)

			storage := &FileControllerStorage{
				path: path,
			}

			// Initialize controller
			icd := &Controller{
				storage:  storage,
				watchers: tc.configWatchers,
			}
			err = icd.storage.save(initialStatuses)
			if err != nil {
				t.Errorf("failed to save initial statuses: %s", err.Error())
			}

			// Functionality being tested
			icd.pullWatchers()

			// Test Result
			status, err := icd.storage.load()
			if err != nil {
				t.Errorf("failed to load status file: %s", err.Error())
			}

			err = checkStatuses(status, expectedStatuses)
			if err != nil {
				t.Errorf("statuses equality check failed: %s", err.Error())
			}
		})
	}
}

func TestIntegrationController_CreateConfig(t *testing.T) {
	testCases := map[string]struct {
		initial   []*Status
		expected  []*Status
		input     cloudconfig.KeyedConfig
		expectErr bool
	}{
		"Invalid Config": {
			initial:   nil,
			expected:  nil,
			input:     invalidAthenaConf,
			expectErr: true,
		},
		"config exists from this source": {
			initial: []*Status{
				makeStatus(validAthenaConf, true, ConfigControllerSource),
			},
			expected: []*Status{
				makeStatus(validAthenaConf, true, ConfigControllerSource),
			},
			input:     validAthenaConf,
			expectErr: true,
		},
		"config exists from this source altered": {
			initial: []*Status{
				makeStatus(validAthenaConf, true, ConfigControllerSource),
			},
			expected: []*Status{
				makeStatus(validAthenaConf, true, ConfigControllerSource),
			},
			input:     validAthenaConfModifiedProperty,
			expectErr: true,
		},
		"config exists from other source enabled": {
			initial: []*Status{
				makeStatus(validAthenaConf, true, MultiCloudSource),
			},
			expected: []*Status{
				makeStatus(validAthenaConf, false, MultiCloudSource),
				makeStatus(validAthenaConf, true, ConfigControllerSource),
			},
			input:     validAthenaConf,
			expectErr: false,
		},
		"config exists from other source disabled": {
			initial: []*Status{
				makeStatus(validAthenaConf, false, MultiCloudSource),
			},
			expected: []*Status{
				makeStatus(validAthenaConf, false, MultiCloudSource),
				makeStatus(validAthenaConf, true, ConfigControllerSource),
			},
			input:     validAthenaConf,
			expectErr: false,
		},
		"config into empty": {
			initial: []*Status{},
			expected: []*Status{
				makeStatus(validAthenaConf, true, ConfigControllerSource),
			},
			input:     validAthenaConf,
			expectErr: false,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			// Test set up and validation
			initialStatuses, err := buildStatuses(tc.initial)
			if err != nil {
				t.Errorf("initial statuses: %s", err.Error())
			}

			expectedStatuses, err := buildStatuses(tc.expected)
			if err != nil {
				t.Errorf("initial statuses: %s", err.Error())
			}

			tempDir := os.TempDir()
			path := filepath.Join(tempDir, configFile)
			defer os.Remove(path)

			storage := &FileControllerStorage{
				path: path,
			}

			// Initialize controller
			icd := &Controller{
				storage: storage,
			}
			err = icd.storage.save(initialStatuses)
			if err != nil {
				t.Errorf("failed to save initial statuses: %s", err.Error())
			}

			// Functionality being tested
			err = icd.CreateConfig(tc.input)

			// Test Result
			if err != nil && !tc.expectErr {
				t.Errorf("unexpected error when creating config: %s", err.Error())
			}
			if err == nil && tc.expectErr {
				t.Errorf("no error where expect")
			}

			status, err := icd.storage.load()
			if err != nil {
				t.Errorf("failed to load status file: %s", err.Error())
			}

			err = checkStatuses(status, expectedStatuses)
			if err != nil {
				t.Errorf("statuses equality check failed: %s", err.Error())
			}
		})
	}

}

func TestIntegrationController_EnableConfig(t *testing.T) {
	testCases := map[string]struct {
		initial     []*Status
		expected    []*Status
		inputKey    string
		inputSource string
		expectErr   bool
	}{
		"config doesn't exist": {
			initial:     []*Status{},
			expected:    []*Status{},
			inputKey:    validAthenaConf.Key(),
			inputSource: ConfigControllerSource.String(),
			expectErr:   true,
		},
		"config is already enabled": {
			initial: []*Status{
				makeStatus(validAthenaConf, true, ConfigControllerSource),
			},
			expected: []*Status{
				makeStatus(validAthenaConf, true, ConfigControllerSource),
			},
			inputKey:    validAthenaConf.Key(),
			inputSource: ConfigControllerSource.String(),
			expectErr:   true,
		},
		"alternate source": {
			initial: []*Status{
				makeStatus(validAthenaConf, false, MultiCloudSource),
			},
			expected: []*Status{
				makeStatus(validAthenaConf, true, MultiCloudSource),
			},
			inputKey:    validAthenaConf.Key(),
			inputSource: MultiCloudSource.String(),
			expectErr:   false,
		},
		"enabled disabled single config": {
			initial: []*Status{
				makeStatus(validAthenaConf, false, ConfigControllerSource),
			},
			expected: []*Status{
				makeStatus(validAthenaConf, true, ConfigControllerSource),
			},
			inputKey:    validAthenaConf.Key(),
			inputSource: ConfigControllerSource.String(),
			expectErr:   false,
		},
		"enable config which is enabled by another source": {
			initial: []*Status{
				makeStatus(validAthenaConf, false, ConfigControllerSource),
				makeStatus(validAthenaConf, true, MultiCloudSource),
			},
			expected: []*Status{
				makeStatus(validAthenaConf, true, ConfigControllerSource),
				makeStatus(validAthenaConf, false, MultiCloudSource),
			},
			inputKey:    validAthenaConf.Key(),
			inputSource: ConfigControllerSource.String(),
			expectErr:   false,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			// Test set up and validation
			initialStatuses, err := buildStatuses(tc.initial)
			if err != nil {
				t.Errorf("initial statuses: %s", err.Error())
			}

			expectedStatuses, err := buildStatuses(tc.expected)
			if err != nil {
				t.Errorf("initial statuses: %s", err.Error())
			}

			tempDir := os.TempDir()
			path := filepath.Join(tempDir, configFile)
			defer os.Remove(path)

			storage := &FileControllerStorage{
				path: path,
			}

			// Initialize controller
			icd := &Controller{
				storage: storage,
			}

			err = icd.storage.save(initialStatuses)
			if err != nil {
				t.Errorf("failed to save initial statuses: %s", err.Error())
			}

			// Functionality being tested
			err = icd.EnableConfig(tc.inputKey, tc.inputSource)

			// Test Result
			if err != nil && !tc.expectErr {
				t.Errorf("unexpected error when enabling config: %s", err.Error())
			}
			if err == nil && tc.expectErr {
				t.Errorf("no error where expect")
			}

			status, err := icd.storage.load()
			if err != nil {
				t.Errorf("failed to load status file: %s", err.Error())
			}

			err = checkStatuses(status, expectedStatuses)
			if err != nil {
				t.Errorf("statuses equality check failed: %s", err.Error())
			}
		})
	}
}

func TestIntegrationController_DisableConfig(t *testing.T) {
	testCases := map[string]struct {
		initial     []*Status
		expected    []*Status
		inputKey    string
		inputSource string
		expectErr   bool
	}{
		"config doesn't exist": {
			initial:     []*Status{},
			expected:    []*Status{},
			inputKey:    validAthenaConf.Key(),
			inputSource: ConfigControllerSource.String(),
			expectErr:   true,
		},

		"config is already disabled": {
			initial: []*Status{
				makeStatus(validAthenaConf, false, ConfigControllerSource),
				makeStatus(validAthenaConf, true, MultiCloudSource),
			},
			expected: []*Status{
				makeStatus(validAthenaConf, false, ConfigControllerSource),
				makeStatus(validAthenaConf, true, MultiCloudSource),
			},
			inputKey:    validAthenaConf.Key(),
			inputSource: ConfigControllerSource.String(),
			expectErr:   true,
		},
		"disable single config": {
			initial: []*Status{
				makeStatus(validAthenaConf, true, ConfigControllerSource),
			},
			expected: []*Status{
				makeStatus(validAthenaConf, false, ConfigControllerSource),
			},
			inputKey:    validAthenaConf.Key(),
			inputSource: ConfigControllerSource.String(),
			expectErr:   false,
		},
		"alternate source": {
			initial: []*Status{
				makeStatus(validAthenaConf, true, MultiCloudSource),
			},
			expected: []*Status{
				makeStatus(validAthenaConf, false, MultiCloudSource),
			},
			inputKey:    validAthenaConf.Key(),
			inputSource: MultiCloudSource.String(),
			expectErr:   false,
		},
		"disable config, matching config from separate source": {
			initial: []*Status{
				makeStatus(validAthenaConf, true, ConfigControllerSource),
				makeStatus(validAthenaConf, false, MultiCloudSource),
			},
			expected: []*Status{
				makeStatus(validAthenaConf, false, ConfigControllerSource),
				makeStatus(validAthenaConf, false, MultiCloudSource),
			},
			inputKey:    validAthenaConf.Key(),
			inputSource: ConfigControllerSource.String(),
			expectErr:   false,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			// Test set up and validation
			initialStatuses, err := buildStatuses(tc.initial)
			if err != nil {
				t.Errorf("initial statuses: %s", err.Error())
			}

			expectedStatuses, err := buildStatuses(tc.expected)
			if err != nil {
				t.Errorf("initial statuses: %s", err.Error())
			}

			tempDir := os.TempDir()
			path := filepath.Join(tempDir, configFile)
			defer os.Remove(path)

			storage := &FileControllerStorage{
				path: path,
			}

			// Initialize controller
			icd := &Controller{
				storage: storage,
			}

			err = icd.storage.save(initialStatuses)
			if err != nil {
				t.Errorf("failed to save initial statuses: %s", err.Error())
			}

			// Functionality being tested
			err = icd.DisableConfig(tc.inputKey, tc.inputSource)

			// Test Result
			if err != nil && !tc.expectErr {
				t.Errorf("unexpected error when disabling config: %s", err.Error())
			}
			if err == nil && tc.expectErr {
				t.Errorf("no error where expect")
			}

			status, err := icd.storage.load()
			if err != nil {
				t.Errorf("failed to load status file: %s", err.Error())
			}

			err = checkStatuses(status, expectedStatuses)
			if err != nil {
				t.Errorf("statuses equality check failed: %s", err.Error())
			}
		})
	}
}

func TestIntegrationController_DeleteConfig(t *testing.T) {
	testCases := map[string]struct {
		initial     []*Status
		expected    []*Status
		inputKey    string
		inputSource string
		expectErr   bool
	}{
		"config doesn't exist": {
			initial:     []*Status{},
			expected:    []*Status{},
			inputKey:    validAthenaConf.Key(),
			inputSource: ConfigControllerSource.String(),
			expectErr:   true,
		},
		"invalid source": {
			initial:     []*Status{},
			expected:    []*Status{},
			inputKey:    validAthenaConf.Key(),
			inputSource: MultiCloudSource.String(),
			expectErr:   true,
		},
		"delete single config": {
			initial: []*Status{
				makeStatus(validAthenaConf, true, ConfigControllerSource),
			},
			expected:    []*Status{},
			inputKey:    validAthenaConf.Key(),
			inputSource: ConfigControllerSource.String(),
			expectErr:   false,
		},
		"disable config, matching config from separate source": {
			initial: []*Status{
				makeStatus(validAthenaConf, true, ConfigControllerSource),
				makeStatus(validAthenaConf, false, MultiCloudSource),
			},
			expected: []*Status{
				makeStatus(validAthenaConf, false, MultiCloudSource),
			},
			inputKey:    validAthenaConf.Key(),
			inputSource: ConfigControllerSource.String(),
			expectErr:   false,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			// Test set up and validation
			initialStatuses, err := buildStatuses(tc.initial)
			if err != nil {
				t.Errorf("initial statuses: %s", err.Error())
			}

			expectedStatuses, err := buildStatuses(tc.expected)
			if err != nil {
				t.Errorf("initial statuses: %s", err.Error())
			}

			tempDir := os.TempDir()
			path := filepath.Join(tempDir, configFile)
			defer os.Remove(path)

			storage := &FileControllerStorage{
				path: path,
			}

			// Initialize controller
			icd := &Controller{
				storage: storage,
			}

			err = icd.storage.save(initialStatuses)
			if err != nil {
				t.Errorf("failed to save initial statuses: %s", err.Error())
			}

			// Functionality being tested
			err = icd.DeleteConfig(tc.inputKey, tc.inputSource)

			// Test Result
			if err != nil && !tc.expectErr {
				t.Errorf("unexpected error when deleting config: %s", err.Error())
			}
			if err == nil && tc.expectErr {
				t.Errorf("no error where expect")
			}

			status, err := icd.storage.load()
			if err != nil {
				t.Errorf("failed to load status file: %s", err.Error())
			}

			err = checkStatuses(status, expectedStatuses)
			if err != nil {
				t.Errorf("statuses equality check failed: %s", err.Error())
			}
		})
	}
}

func makeStatus(config cloudconfig.KeyedConfig, active bool, source ConfigSource) *Status {
	err := config.Validate()
	valid := err == nil

	configType, err := ConfigTypeFromConfig(config)
	if err != nil {
		panic(fmt.Errorf("config type not recognised: %w", err))
	}

	return &Status{
		Source:     source,
		Key:        config.Key(),
		Active:     active,
		Valid:      valid,
		ConfigType: configType,
		Config:     config,
	}
}

func buildStatuses(statusList []*Status) (Statuses, error) {
	statuses := Statuses{}
	for _, status := range statusList {
		if _, ok := statuses.Get(status.Key, status.Source); ok {
			return nil, fmt.Errorf("invalid test, duplicate status with key: %s source: %s", status.Key, status.Source.String())
		}
		statuses.Insert(status)
	}
	return statuses, nil
}

func checkStatuses(actual, expected Statuses) error {
	if len(actual.List()) != len(expected.List()) {
		return fmt.Errorf("integration statueses did not have the correct length actaul: %d, expected: %d", len(actual.List()), len(expected.List()))
	}

	for _, actualStatus := range actual.List() {
		expectedStatus, ok := expected.Get(actualStatus.Key, actualStatus.Source)
		if !ok {
			return fmt.Errorf("expected integration statuses is missing with integration key: %s, source: %s", actualStatus.Key, actualStatus.Source.String())
		}

		// failure here indicates an issue with the configID
		if actualStatus.Key != expectedStatus.Key {
			return fmt.Errorf("integration status does not have the correct Key values actual: %s, expected: %s", actualStatus.Key, expectedStatus.Key)
		}

		// failure here indicates an issue with the configID
		if actualStatus.Key != expectedStatus.Key {
			return fmt.Errorf("integration status does not have the correct Source values actual: %s, expected: %s", actualStatus.Source, expectedStatus.Source)
		}

		if actualStatus.Active != expectedStatus.Active {
			return fmt.Errorf("integration status does not have the correct Active values actual: %v, expected: %v", actualStatus.Active, expectedStatus.Active)
		}

		if actualStatus.Valid != expectedStatus.Valid {
			return fmt.Errorf("integration status does not have the correct Valid values actual: %v, expected: %v", actualStatus.Valid, expectedStatus.Valid)
		}

		if !actualStatus.Config.Equals(expectedStatus.Config) {
			return fmt.Errorf("integration status does not have the correct config values actual: %v, expected: %v", actualStatus.Config, expectedStatus.Config)
		}
	}
	return nil
}
