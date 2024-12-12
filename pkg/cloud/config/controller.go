package config

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
	"github.com/opencost/opencost/pkg/cloud"
	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/env"
)

const configFile = "cloud-configurations.json"

// Controller manages the cloud.Config using config Watcher(s) to track various configuration
// methods. To do this it has a map of config watchers mapped on configuration source and a list Observers that it updates
// upon any change detected from the config watchers.
type Controller struct {
	storage   controllerStorage
	lock      sync.RWMutex
	observers []Observer
	watchers  map[ConfigSource]cloud.KeyedConfigWatcher
}

// NewController initializes an Config Controller
func NewController(providerConfig models.ProviderConfig) *Controller {

	watchers := GetCloudBillingWatchers(providerConfig)

	storage := &FileControllerStorage{
		path: filepath.Join(env.GetConfigPathWithDefault(env.DefaultConfigMountPath), configFile),
	}

	ic := &Controller{
		storage:  storage,
		watchers: watchers,
	}

	ic.start()

	return ic
}

// NewMemoryController initializes a Config Controller backed in memory
func NewMemoryController(providerConfig models.ProviderConfig) *Controller {
	watchers := GetCloudBillingWatchers(providerConfig)

	ic := &Controller{
		storage:  &MemoryControllerStorage{},
		watchers: watchers,
	}

	ic.start()

	return ic
}

func (c *Controller) start() {
	c.pullWatchers()

	go func() {
		ticker := timeutil.NewJobTicker()
		defer ticker.Close()

		for {
			ticker.TickIn(10 * time.Second)

			<-ticker.Ch

			c.pullWatchers()
		}
	}()
}

// pullWatchers retrieve configs from watchers and update configs according to priority of sources
func (c *Controller) pullWatchers() {
	c.lock.Lock()
	defer c.lock.Unlock()
	statuses, err := c.storage.load()
	if err != nil {
		log.Warnf("Controller: pullWatchers: %s. Proceeding to create the file", err.Error())
		statuses = Statuses{}
		err = c.storage.save(statuses)
		if err != nil {
			log.Errorf("Controller: pullWatchers: failed to save statuses %s", err.Error())
		}
	}
	for source, watcher := range c.watchers {
		watcherConfsByKey := map[string]cloud.KeyedConfig{}
		for _, wConf := range watcher.GetConfigs() {
			watcherConfsByKey[wConf.Key()] = wConf
		}

		// remove existing configs that are no longer present in the source
		for _, status := range statuses.List() {
			if status.Source == source {
				if _, ok := watcherConfsByKey[status.Key]; !ok {
					err := c.deleteConfig(status.Key, status.Source, statuses)
					if err != nil {
						log.Errorf("Controller: pullWatchers: %s", err.Error())
					}
				}

			}
		}

		for key, conf := range watcherConfsByKey {

			// Check existing configs for matching key and source
			if existingStatus, ok := statuses.Get(key, source); ok {
				// if config has not changed continue
				if existingStatus.Config.Equals(conf) {
					continue
				}
				// remove the existing config
				err := c.deleteConfig(key, source, statuses)
				if err != nil {
					log.Errorf("Controller: pullWatchers: %s", err.Error())
				}

			}

			err := conf.Validate()
			valid := err == nil

			configType, err := ConfigTypeFromConfig(conf)
			if err != nil {
				log.Errorf("Controller: pullWatchers: failed to get config type for config with key: %s", conf.Key())
				continue
			}

			status := Status{
				Key:        key,
				Source:     source,
				Active:     valid, // if valid, then new config will be active
				Valid:      valid,
				ConfigType: configType,
				Config:     conf,
			}

			// handle a config with a new unique key for a source or an update config from a source which was inactive before
			if valid {
				for _, matchStat := range statuses.List() {
					//// skip matching configs
					//if matchID.Equals(cID) {
					//	continue
					//}

					if matchStat.Active {
						// if source is non-multi-cloud disable all other non-multi-cloud sourced configs
						if source == HelmSource || source == ConfigFileSource {
							if matchStat.Source == HelmSource || matchStat.Source == ConfigFileSource {
								matchStat.Active = false
								c.broadcastRemoveConfig(matchStat.Key)
							}
						}

						// check for configs with the same key that are active
						if matchStat.Key == key {
							// If source has higher priority disable other active configs
							matchStat.Active = false
							c.broadcastRemoveConfig(matchStat.Key)
						}
					}
				}
			}

			// update config and put to observers if active
			statuses.Insert(&status)
			if status.Active {
				c.broadcastAddConfig(conf)
			}
			err = c.storage.save(statuses)
			if err != nil {
				log.Errorf("Controller: pullWatchers: failed to save statuses %s", err.Error())
			}
		}
	}
}

// CreateConfig adds a new config to status with a source of ConfigControllerSource
// It will disable any config with the same key
// fails if there is an existing config with the same key and source
func (c *Controller) CreateConfig(conf cloud.KeyedConfig) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	err := conf.Validate()
	if err != nil {
		return fmt.Errorf("provided configuration was invalid: %w", err)
	}

	statuses, err := c.storage.load()
	if err != nil {
		return fmt.Errorf("failed to load statuses")
	}
	source := ConfigControllerSource
	key := conf.Key()

	_, ok := statuses.Get(key, source)
	if ok {
		return fmt.Errorf("config with key %s from source %s already exist", key, source.String())
	}

	configType, err := ConfigTypeFromConfig(conf)
	if err != nil {
		return fmt.Errorf("config did not have recoginzed config: %w", err)
	}

	statuses.Insert(&Status{
		Key:        key,
		Source:     source,
		Valid:      true,
		Active:     true,
		ConfigType: configType,
		Config:     conf,
	})

	// check for configurations with the same configuration key that are already active.
	for _, confStat := range statuses.List() {
		if confStat.Key != key || confStat.Source == source {
			continue
		}

		// if active disable
		if confStat.Active == true {
			confStat.Active = false
			c.broadcastRemoveConfig(key)
		}

	}

	c.broadcastAddConfig(conf)
	err = c.storage.save(statuses)
	if err != nil {
		return fmt.Errorf("failed to save statues: %w", err)
	}
	return nil
}

// EnableConfig enables a config with the given key and source, and disables any config with a matching key
func (c *Controller) EnableConfig(key, sourceStr string) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	statuses, err := c.storage.load()
	if err != nil {
		return fmt.Errorf("failed to load statuses")
	}

	source := GetConfigSource(sourceStr)
	cs, ok := statuses.Get(key, source)
	if !ok {
		return fmt.Errorf("config with key %s from source %s does not exist", key, sourceStr)
	}
	if cs.Active {
		return fmt.Errorf("config with key %s from source %s is already active", key, sourceStr)
	}

	// check for configurations with the same configuration key that are already active.
	for _, confStat := range statuses.List() {
		if confStat.Key != key || confStat.Source == source {
			continue
		}

		// if active disable
		if confStat.Active == true {
			confStat.Active = false
			c.broadcastRemoveConfig(key)
		}

	}

	cs.Active = true
	c.broadcastAddConfig(cs.Config)
	c.storage.save(statuses)
	return nil
}

// DisableConfig updates an config status if it was enabled
func (c *Controller) DisableConfig(key, sourceStr string) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	statuses, err := c.storage.load()
	if err != nil {
		return fmt.Errorf("failed to load statuses")
	}
	source := GetConfigSource(sourceStr)
	is, ok := statuses.Get(key, source)
	if !ok {
		return fmt.Errorf("Controller: DisableConfig: config with key %s from source %s does not exist", key, source)
	}
	if !is.Active {
		return fmt.Errorf("Controller: DisableConfig: config with key %s from source %s is already disabled", key, source)
	}

	is.Active = false
	c.broadcastRemoveConfig(key)
	c.storage.save(statuses)
	return nil
}

// DeleteConfig removes a config from the statuses and deletes the config on all observers if it was active
// This can only be used on configs with ConfigControllerSource
func (c *Controller) DeleteConfig(key, sourceStr string) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	source := GetConfigSource(sourceStr)
	if source != ConfigControllerSource {
		return fmt.Errorf("controller does not own config with key %s from source %s, manage this config at its source", key, source.String())
	}

	statuses, err := c.storage.load()
	if err != nil {
		return fmt.Errorf("failed to load statuses")
	}

	err = c.deleteConfig(key, source, statuses)
	if err != nil {
		return fmt.Errorf("Controller: DeleteConfig: %w", err)
	}
	return nil
}

func (c *Controller) deleteConfig(key string, source ConfigSource, statuses Statuses) error {
	is, ok := statuses.Get(key, source)
	if !ok {
		return fmt.Errorf("config with key %s from source %s does not exist", key, source.String())
	}

	// delete config on observers if active
	if is.Active {
		c.broadcastRemoveConfig(key)
	}
	delete(statuses[source], key)
	c.storage.save(statuses)
	return nil
}

func (c *Controller) ExportConfigs(key string) (*Configurations, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	configs := new(Configurations)

	activeConfigs := c.getActiveConfigs()
	if key != "" {
		conf, ok := activeConfigs[key]
		if !ok {
			return nil, fmt.Errorf("Config with key %s does not exist or is inactive", key)
		}
		sanitizedConfig := conf.Sanitize()
		err := configs.Insert(sanitizedConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to insert config: %w", err)
		}
		return configs, nil
	}

	for _, conf := range activeConfigs {
		sanitizedConfig := conf.Sanitize()
		err := configs.Insert(sanitizedConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to insert config: %w", err)
		}
	}
	return configs, nil
}

func (c *Controller) getActiveConfigs() map[string]cloud.KeyedConfig {
	activeConfigs := make(map[string]cloud.KeyedConfig)
	statuses, err := c.storage.load()
	if err != nil {
		log.Errorf("GetStatus: failed to load cloud statuses")
	}
	for _, cs := range statuses.List() {
		if cs.Active {
			activeConfigs[cs.Key] = cs.Config
		}
	}
	return activeConfigs
}

// broadcastRemoveConfig ask observers to remove and stop all processes related to a configuration with a given key
func (c *Controller) broadcastRemoveConfig(key string) {
	var wg sync.WaitGroup
	for _, obs := range c.observers {
		observer := obs
		wg.Add(1)
		go func() {
			defer wg.Done()
			observer.DeleteConfig(key)
		}()
	}
	wg.Wait()
}

// broadcastAddConfig gives observers a new config to handle
func (c *Controller) broadcastAddConfig(conf cloud.KeyedConfig) {
	var wg sync.WaitGroup
	for _, obs := range c.observers {
		observer := obs
		wg.Add(1)
		go func() {
			defer wg.Done()
			observer.PutConfig(conf)
		}()
	}
	wg.Wait()
}

// RegisterObserver gives out the current active list configs and adds the observer to the push list
func (c *Controller) RegisterObserver(obs Observer) {
	c.lock.Lock()
	defer c.lock.Unlock()
	obs.SetConfigs(c.getActiveConfigs())
	c.observers = append(c.observers, obs)
}

func (c *Controller) GetStatus() []Status {
	c.lock.RLock()
	defer c.lock.RUnlock()
	var status []Status
	statuses, err := c.storage.load()
	if err != nil {
		log.Errorf("GetStatus: failed to load cloud statuses")
	}
	for _, intStat := range statuses.List() {
		status = append(status, *intStat)
	}
	return status
}

type controllerStorage interface {
	load() (Statuses, error)
	save(statuses Statuses) error
}

type FileControllerStorage struct {
	path string
}

func (fcs *FileControllerStorage) load() (Statuses, error) {
	raw, err := os.ReadFile(fcs.path)
	if err != nil {
		return nil, fmt.Errorf("failed to load config statuses from file: %w", err)
	}

	statuses := Statuses{}
	err = json.Unmarshal(raw, &statuses)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal config statuses: %s", err.Error())
	}

	return statuses, nil
}

func (fcs *FileControllerStorage) save(statuses Statuses) error {

	raw, err := json.Marshal(statuses)
	if err != nil {
		return fmt.Errorf("failed to marshal config statuses: %s", err)
	}

	err = os.WriteFile(fcs.path, raw, 0644)
	if err != nil {
		return fmt.Errorf("failed to save config statuses to file: %s", err)
	}

	return nil
}

// MemoryControllerStorage is a ControllerStorage implementation that is backed by a byte array that
// is marshalled in and out of to ensure that behaviours is same as the file backed version
type MemoryControllerStorage struct {
	bytes []byte
}

func (mcs *MemoryControllerStorage) load() (Statuses, error) {
	if mcs.bytes == nil {
		return Statuses{}, nil
	}

	statuses := Statuses{}
	err := json.Unmarshal(mcs.bytes, &statuses)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal config statuses: %s", err.Error())
	}

	return statuses, nil
}

func (mcs *MemoryControllerStorage) save(statuses Statuses) error {

	raw, err := json.Marshal(statuses)
	if err != nil {
		return fmt.Errorf("failed to marshal config statuses: %s", err)
	}

	mcs.bytes = raw

	return nil
}
