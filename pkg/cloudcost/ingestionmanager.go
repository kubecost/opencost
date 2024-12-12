package cloudcost

import (
	"fmt"
	"sync"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/pkg/cloud"
	"github.com/opencost/opencost/pkg/cloud/config"
)

// IngestionManager is a config.Observer which creates Ingestor instances based on the signals that it receives from the
// config.Controller
type IngestionManager struct {
	lock      sync.Mutex
	ingestors map[string]*ingestor
	config    IngestorConfig
	repo      Repository
}

// NewIngestionManager creates a new IngestionManager and registers it with the provided integration controller
func NewIngestionManager(controller *config.Controller, repo Repository, ingConf IngestorConfig) *IngestionManager {
	// return empty ingestion manager if store or integration controller are nil
	if controller == nil || repo == nil {
		return &IngestionManager{
			ingestors: map[string]*ingestor{},
		}
	}

	im := &IngestionManager{
		ingestors: map[string]*ingestor{},
		repo:      repo,
		config:    ingConf,
	}
	controller.RegisterObserver(im)

	return im
}

// PutConfig is an imperative function which puts an ingestor for the provided Integration
func (im *IngestionManager) PutConfig(kc cloud.KeyedConfig) {
	im.lock.Lock()
	defer im.lock.Unlock()
	err := im.createIngestor(kc)
	if err != nil {
		log.Errorf("IngestionManager: PutConfig failed to create billing integration: %s", err.Error())
	}
}

// DeleteConfig is an imperative function which removes an ingestor with a matching key
func (im *IngestionManager) DeleteConfig(key string) {
	im.lock.Lock()
	defer im.lock.Unlock()
	im.deleteIngestor(key)
}

// SetConfigs is a declarative function for setting which BillingIntegrations IngestionManager should have ingestors for
func (im *IngestionManager) SetConfigs(configs map[string]cloud.KeyedConfig) {
	im.lock.Lock()
	defer im.lock.Unlock()
	// delete any exiting ingestors
	for key, _ := range im.ingestors {
		im.deleteIngestor(key)
	}
	// create  ingestors for provided
	for _, conf := range configs {
		err := im.createIngestor(conf)
		if err != nil {
			log.Errorf("IngestionManager: error creating ingestor: %s", err.Error())
		}
	}
}

func (im *IngestionManager) StartAll() {
	im.lock.Lock()
	defer im.lock.Unlock()
	var wg sync.WaitGroup
	wg.Add(len(im.ingestors))
	for key := range im.ingestors {
		ing := im.ingestors[key]
		go func() {
			defer wg.Done()
			ing.Start(false)

		}()
	}
	wg.Wait()
}

func (im *IngestionManager) StopAll() {
	im.lock.Lock()
	defer im.lock.Unlock()
	var wg sync.WaitGroup
	wg.Add(len(im.ingestors))
	for key := range im.ingestors {
		ing := im.ingestors[key]
		go func() {
			defer wg.Done()
			ing.Stop()
		}()
	}
	wg.Wait()
}

func (im *IngestionManager) RebuildAll() {
	im.lock.Lock()
	defer im.lock.Unlock()
	var wg sync.WaitGroup
	wg.Add(len(im.ingestors))
	for key := range im.ingestors {
		go func(ing *ingestor) {
			defer wg.Done()
			ing.Stop()
			ing.Start(true)

		}(im.ingestors[key])
	}
	wg.Wait()
}

func (im *IngestionManager) Rebuild(integrationKey string) error {
	im.lock.Lock()
	defer im.lock.Unlock()
	ing, ok := im.ingestors[integrationKey]
	if !ok {
		return fmt.Errorf("CloudCost: IngestionManager: Rebuild: failed to rebuild, integration with key does not exist: %s", integrationKey)
	}
	ing.Stop()
	ing.Start(true)
	return nil
}

func (im *IngestionManager) RepairAll(start, end time.Time) error {
	im.lock.Lock()
	defer im.lock.Unlock()
	s := opencost.RoundForward(start, im.config.Resolution)
	e := opencost.RoundForward(end, im.config.Resolution)
	windows, err := opencost.GetWindowsForQueryWindow(s, e, im.config.QueryWindow)
	if err != nil {
		return fmt.Errorf("CloudCost: IngestionManager: Repair could not retrieve windows: %s", err.Error())
	}

	for key := range im.ingestors {
		go func(ing *ingestor) {
			for _, window := range windows {
				ing.BuildWindow(*window.Start(), *window.End())
			}
		}(im.ingestors[key])
	}

	return nil
}

func (im *IngestionManager) Repair(integrationKey string, start, end time.Time) error {
	im.lock.Lock()
	defer im.lock.Unlock()
	s := opencost.RoundForward(start, im.config.Resolution)
	e := opencost.RoundForward(end, im.config.Resolution)
	windows, err := opencost.GetWindowsForQueryWindow(s, e, im.config.QueryWindow)
	if err != nil {
		return fmt.Errorf("CloudCost: IngestionManager: Repair could not retrieve windows: %s", err.Error())
	}
	ing, ok := im.ingestors[integrationKey]
	if !ok {
		return fmt.Errorf("CloudCost: IngestionManager: Repair: failed to rebuild, integration with key does not exist: %s", integrationKey)
	}
	go func(ing *ingestor) {
		for _, window := range windows {
			ing.BuildWindow(*window.Start(), *window.End())
		}
	}(ing)
	return nil
}

// deleteIngestor stops then removes an ingestor from the map of ingestors
func (im *IngestionManager) deleteIngestor(integrationKey string) {
	ing, ok := im.ingestors[integrationKey]
	if !ok {
		return
	}
	log.Infof("CloudCost: IngestionManager: deleting integration with key: %s", integrationKey)
	ing.Stop()

	delete(im.ingestors, integrationKey)
}

// createIngestor stops existing ingestor with matching key then creates and starts and new ingestor
func (im *IngestionManager) createIngestor(config cloud.KeyedConfig) error {
	if config == nil {
		return fmt.Errorf("cannot create ingestor from nil integration")
	}
	// delete ingestor with matching key if it exists
	im.deleteIngestor(config.Key())
	log.Infof("CloudCost: IngestionManager: creating integration with key: %s", config.Key())
	ing, err := NewIngestor(im.config, im.repo, config)
	if err != nil {
		return fmt.Errorf("IngestionManager: createIngestor: %w", err)
	}

	ing.Start(false)

	im.ingestors[config.Key()] = ing

	return nil
}
