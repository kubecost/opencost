package cloudcost

import (
	"fmt"
	"sync"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
	"golang.org/x/exp/maps"
)

// MemoryRepository is an implementation of Repository that uses a map keyed on config key and window start along with a
// RWMutex to make it threadsafe
type MemoryRepository struct {
	rwLock sync.RWMutex
	data   map[string]map[time.Time]*opencost.CloudCostSet
}

func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{
		data: make(map[string]map[time.Time]*opencost.CloudCostSet),
	}
}

func (m *MemoryRepository) Has(startTime time.Time, billingIntegration string) (bool, error) {
	m.rwLock.RLock()
	defer m.rwLock.RUnlock()

	billingIntegrationData, ok := m.data[billingIntegration]
	if !ok {
		return false, nil
	}

	_, ook := billingIntegrationData[startTime.UTC()]
	return ook, nil
}

func (m *MemoryRepository) Get(startTime time.Time, billingIntegration string) (*opencost.CloudCostSet, error) {
	m.rwLock.RLock()
	defer m.rwLock.RUnlock()

	billingIntegrationData, ok := m.data[billingIntegration]
	if !ok {
		return nil, nil
	}

	ccs, ook := billingIntegrationData[startTime.UTC()]
	if !ook {
		return nil, nil
	}
	return ccs.Clone(), nil
}

func (m *MemoryRepository) Keys() ([]string, error) {
	m.rwLock.RLock()
	defer m.rwLock.RUnlock()

	keys := maps.Keys(m.data)
	return keys, nil
}

func (m *MemoryRepository) Put(ccs *opencost.CloudCostSet) error {
	m.rwLock.Lock()
	defer m.rwLock.Unlock()

	if ccs == nil {
		return fmt.Errorf("MemoryRepository: Put: cannot save nil")
	}

	if ccs.Window.IsOpen() {
		return fmt.Errorf("MemoryRepository: Put: cloud cost set has invalid window %s", ccs.Window.String())
	}

	if ccs.Integration == "" {
		return fmt.Errorf("MemoryRepository: Put: cloud cost set does not have an integration value")
	}

	if _, ok := m.data[ccs.Integration]; !ok {
		m.data[ccs.Integration] = make(map[time.Time]*opencost.CloudCostSet)
	}

	m.data[ccs.Integration][ccs.Window.Start().UTC()] = ccs
	return nil
}

// Expire deletes all items in the map with a start time before the given limit
func (m *MemoryRepository) Expire(limit time.Time) error {
	m.rwLock.Lock()
	defer m.rwLock.Unlock()

	for key, integration := range m.data {
		for startTime := range integration {
			if startTime.Before(limit) {
				delete(integration, startTime)
			}
		}
		// remove integration if it is now empty
		if len(integration) == 0 {
			delete(m.data, key)
		}
	}
	return nil
}
