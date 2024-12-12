package provider

import (
	"fmt"
	"os"
	gopath "path"
	"strconv"
	"sync"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/pkg/cloud/alibaba"
	"github.com/opencost/opencost/pkg/cloud/aws"
	"github.com/opencost/opencost/pkg/cloud/azure"
	"github.com/opencost/opencost/pkg/cloud/gcp"
	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/cloud/oracle"
	"github.com/opencost/opencost/pkg/cloud/otc"
	"github.com/opencost/opencost/pkg/cloud/utils"
	"github.com/opencost/opencost/pkg/config"
	"github.com/opencost/opencost/pkg/env"
)

const closedSourceConfigMount = "models/"

// ProviderConfig is a utility class that provides a thread-safe configuration storage/cache for all Provider
// implementations
type ProviderConfig struct {
	lock            *sync.Mutex
	configManager   *config.ConfigFileManager
	configFile      *config.ConfigFile
	customPricing   *models.CustomPricing
	watcherHandleID config.HandlerID
}

// NewProviderConfig creates a new ConfigFile and returns the ProviderConfig
func NewProviderConfig(configManager *config.ConfigFileManager, fileName string) *ProviderConfig {
	configFile := configManager.ConfigFileAt(configPathFor(fileName))
	pc := &ProviderConfig{
		lock:          new(sync.Mutex),
		configManager: configManager,
		configFile:    configFile,
		customPricing: nil,
	}

	// add the provider config func as handler for the config file changes
	pc.watcherHandleID = configFile.AddChangeHandler(pc.onConfigFileUpdated)
	return pc
}

// onConfigFileUpdated handles any time the config file contents are updated, created, or deleted
func (pc *ProviderConfig) onConfigFileUpdated(changeType config.ChangeType, data []byte) {
	// TODO: (bolt) Currently this has the side-effect of setting pc.customPricing twice when the update
	// TODO: (bolt) is made from this ProviderConfig instance. We'll need to implement a way of identifying
	// TODO: (bolt) when to ignore updates when the change and handler are the same source
	log.Infof("CustomPricing Config Updated: %s", changeType)

	switch changeType {
	case config.ChangeTypeCreated:
		fallthrough
	case config.ChangeTypeModified:
		pc.lock.Lock()
		defer pc.lock.Unlock()

		customPricing := new(models.CustomPricing)
		err := json.Unmarshal(data, customPricing)
		if err != nil {
			log.Infof("Could not decode Custom Pricing file at path %s. Using default.", pc.configFile.Path())
			customPricing = DefaultPricing()
		}

		pc.customPricing = customPricing
		if pc.customPricing.SpotGPU == "" {
			pc.customPricing.SpotGPU = DefaultPricing().SpotGPU // Migration for users without this value set by default.
		}

		if pc.customPricing.ShareTenancyCosts == "" {
			pc.customPricing.ShareTenancyCosts = models.DefaultShareTenancyCost
		}
	}
}

// Non-ThreadSafe logic to load the config file if a cache does not exist. Flag to write
// the default config if the config file doesn't exist.
func (pc *ProviderConfig) loadConfig(writeIfNotExists bool) (*models.CustomPricing, error) {
	if pc.customPricing != nil {
		return pc.customPricing, nil
	}

	exists, err := pc.configFile.Exists()
	// File Error other than NotExists
	if err != nil {
		log.Infof("Custom Pricing file at path '%s' read error: '%s'", pc.configFile.Path(), err.Error())
		return DefaultPricing(), err
	}

	// File Doesn't Exist
	if !exists {
		log.Infof("Could not find Custom Pricing file at path '%s'", pc.configFile.Path())
		pc.customPricing = DefaultPricing()
		// If config file is not present use the contents from mount models/ as pricing data
		// in closed source rather than from from  DefaultPricing as first source of truth.
		// since most images will already have a mount, to avail this facility user needs to delete the
		// config file manually from configpath else default pricing still holds good.
		fileName := filenameInConfigPath(pc.configFile.Path())
		defaultPricing, err := ReturnPricingFromConfigs(fileName)
		if err == nil {
			pc.customPricing = defaultPricing
		}

		// Only write the file if flag enabled
		if writeIfNotExists {
			cj, err := json.Marshal(pc.customPricing)
			if err != nil {
				return pc.customPricing, err
			}

			err = pc.configFile.Write(cj)
			if err != nil {
				log.Infof("Could not write Custom Pricing file to path '%s'", pc.configFile.Path())
				return pc.customPricing, err
			}
		}

		return pc.customPricing, nil
	}

	// File Exists - Read all contents of file, unmarshal json
	byteValue, err := pc.configFile.Read()
	if err != nil {
		log.Infof("Could not read Custom Pricing file at path %s", pc.configFile.Path())
		// If read fails, we don't want to cache default, assuming that the file is valid
		return DefaultPricing(), err
	}

	var customPricing models.CustomPricing
	err = json.Unmarshal(byteValue, &customPricing)
	if err != nil {
		log.Infof("Could not decode Custom Pricing file at path %s", pc.configFile.Path())
		return DefaultPricing(), err
	}

	pc.customPricing = &customPricing
	if pc.customPricing.SpotGPU == "" {
		pc.customPricing.SpotGPU = DefaultPricing().SpotGPU // Migration for users without this value set by default.
	}

	if pc.customPricing.ShareTenancyCosts == "" {
		pc.customPricing.ShareTenancyCosts = models.DefaultShareTenancyCost
	}

	// If the sample nil service key name is set, zero it out so that it is not
	// misinterpreted as a real service key.
	if pc.customPricing.ServiceKeyName == "AKIXXX" {
		pc.customPricing.ServiceKeyName = ""
	}

	return pc.customPricing, nil
}

// ThreadSafe method for retrieving the custom pricing config.
func (pc *ProviderConfig) GetCustomPricingData() (*models.CustomPricing, error) {
	pc.lock.Lock()
	defer pc.lock.Unlock()

	return pc.loadConfig(true)
}

// ConfigFileManager returns the ConfigFileManager instance used to manage the CustomPricing
// configuration. In the event of a multi-provider setup, this instance should be used to
// configure any other configuration providers.
func (pc *ProviderConfig) ConfigFileManager() *config.ConfigFileManager {
	return pc.configManager
}

// Allows a call to manually update the configuration while maintaining proper thread-safety
// for read/write methods.
func (pc *ProviderConfig) Update(updateFunc func(*models.CustomPricing) error) (*models.CustomPricing, error) {
	pc.lock.Lock()
	defer pc.lock.Unlock()

	// Load Config, set flag to _not_ write if failure to find file.
	// We're about to write the updated values, so we don't want to double write.
	c, _ := pc.loadConfig(false)

	// Execute Update - On error, return the in-memory config but don't update cache
	// explicitly
	err := updateFunc(c)
	if err != nil {
		return c, fmt.Errorf("error updating provider config: %w", err)
	}

	// Cache Update (possible the ptr already references the cached value)
	pc.customPricing = c

	cj, err := json.Marshal(c)
	if err != nil {
		return c, fmt.Errorf("error marshaling JSON for provider config: %w", err)
	}

	err = pc.configFile.Write(cj)
	if err != nil {
		return c, fmt.Errorf("error writing config file for provider config: %w", err)
	}

	return c, nil
}

// ThreadSafe update of the config using a string map
func (pc *ProviderConfig) UpdateFromMap(a map[string]string) (*models.CustomPricing, error) {
	// Run our Update() method using SetCustomPricingField logic
	return pc.Update(func(c *models.CustomPricing) error {
		for k, v := range a {
			// Just so we consistently supply / receive the same values, uppercase the first letter.
			kUpper := utils.ToTitle.String(k)
			if kUpper == "CPU" || kUpper == "SpotCPU" || kUpper == "RAM" || kUpper == "SpotRAM" || kUpper == "GPU" || kUpper == "Storage" {
				val, err := strconv.ParseFloat(v, 64)
				if err != nil {
					return fmt.Errorf("unable to parse CPU from string to float: %s", err.Error())
				}
				v = fmt.Sprintf("%f", val/730)
			}

			err := models.SetCustomPricingField(c, kUpper, v)
			if err != nil {
				return fmt.Errorf("error setting custom pricing field: %w", err)
			}
		}

		return nil
	})
}

// DefaultPricing should be returned so we can do computation even if no file is supplied.
func DefaultPricing() *models.CustomPricing {
	// https://cloud.google.com/compute/all-pricing
	return &models.CustomPricing{
		Provider:    "base",
		Description: "Default prices based on GCP us-central1",

		// E2 machine types in GCP us-central1 (Iowa)
		CPU:     "0.021811", // per vCPU hour
		SpotCPU: "0.006543", // per vCPU hour
		RAM:     "0.002923", // per G(i?)B hour
		SpotRAM: "0.000877", // per G(i?)B hour

		// There are many GPU types. This serves as a reasonably-appropriate
		// estimate within a broad range (0.35 up to 3.93)
		GPU: "0.95", // per GPU hour
		// Same story as above.
		SpotGPU: "0.308", // per GPU hour

		// This is the "Standard provision space" pricing in the "Disk pricing"
		// table.
		//
		// (($.04 / month) per G(i?)B) *
		//   month/730 hours =
		//     0.00005479452054794521
		Storage: "0.00005479452",

		ZoneNetworkEgress:     "0.01",
		RegionNetworkEgress:   "0.01",
		InternetNetworkEgress: "0.12",
		CustomPricesEnabled:   "false",
		ShareTenancyCosts:     "true",
	}
}

// Returns the configuration directory concatenated with a specific config file name
func configPathFor(filename string) string {
	path := env.GetConfigPathWithDefault("/models/")
	return gopath.Join(path, filename)
}

// Gives the config file name in a full qualified file name
func filenameInConfigPath(fqfn string) string {
	_, fileName := gopath.Split(fqfn)
	return fileName
}

// ReturnPricingFromConfigs is a safe function to return pricing from configs of opensource to the closed source
// before defaulting it with the above function DefaultPricing
func ReturnPricingFromConfigs(filename string) (*models.CustomPricing, error) {
	if _, err := os.Stat(closedSourceConfigMount); os.IsNotExist(err) {
		return &models.CustomPricing{}, fmt.Errorf("ReturnPricingFromConfigs: %s likely running in provider config in opencost itself with err: %v", closedSourceConfigMount, err)
	}
	providerConfigFile := gopath.Join(closedSourceConfigMount, filename)
	if _, err := os.Stat(providerConfigFile); err != nil {
		return &models.CustomPricing{}, fmt.Errorf("ReturnPricingFromConfigs: unable to find file %s with err: %v", providerConfigFile, err)
	}
	configFile, err := os.ReadFile(providerConfigFile)
	if err != nil {
		return &models.CustomPricing{}, fmt.Errorf("ReturnPricingFromConfigs: unable to open file %s with err: %v", providerConfigFile, err)
	}

	defaultPricing := &models.CustomPricing{}
	err = json.Unmarshal(configFile, defaultPricing)
	if err != nil {
		return &models.CustomPricing{}, fmt.Errorf("ReturnPricingFromConfigs: unable to open file %s with err: %v", providerConfigFile, err)
	}
	return defaultPricing, nil
}

func ExtractConfigFromProviders(prov models.Provider) models.ProviderConfig {
	if prov == nil {
		log.Errorf("cannot extract config from nil provider")
		return nil
	}
	switch p := prov.(type) {
	case *CSVProvider:
		return ExtractConfigFromProviders(p.CustomProvider)
	case *CustomProvider:
		return p.Config
	case *gcp.GCP:
		return p.Config
	case *aws.AWS:
		return p.Config
	case *azure.Azure:
		return p.Config
	case *alibaba.Alibaba:
		return p.Config
	case *oracle.Oracle:
		return p.Config
	case *otc.OTC:
		return p.Config
	default:
		log.Errorf("failed to extract config from provider")
		return nil
	}
}
