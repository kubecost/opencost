package customcost

import (
	"fmt"
	"sync"
	"time"

	"github.com/opencost/opencost/core/pkg/model/pb"
	"golang.org/x/exp/maps"
	"google.golang.org/protobuf/proto"
)

// MemoryRepository is an implementation of Repository that uses a map keyed on config key and window start along with a
// RWMutex to make it threadsafe
type MemoryRepository struct {
	rwLock sync.RWMutex
	data   map[string]map[time.Time][]byte
}

func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{
		data: make(map[string]map[time.Time][]byte),
	}
}

func (m *MemoryRepository) Has(startTime time.Time, domain string) (bool, error) {
	m.rwLock.RLock()
	defer m.rwLock.RUnlock()

	domainData, ok := m.data[domain]
	if !ok {
		return false, nil
	}

	_, ook := domainData[startTime.UTC()]
	return ook, nil
}

func (m *MemoryRepository) Get(startTime time.Time, domain string) (*pb.CustomCostResponse, error) {
	m.rwLock.RLock()
	defer m.rwLock.RUnlock()

	domainData, ok := m.data[domain]
	if !ok {
		return &pb.CustomCostResponse{}, nil
	}

	b, ook := domainData[startTime.UTC()]
	if !ook {
		return &pb.CustomCostResponse{}, nil
	}

	ccr := &pb.CustomCostResponse{}
	err := proto.Unmarshal(b, ccr)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling data: %w", err)
	}
	return ccr, nil
}

func (m *MemoryRepository) Keys() ([]string, error) {
	m.rwLock.RLock()
	defer m.rwLock.RUnlock()

	keys := maps.Keys(m.data)
	return keys, nil
}

func (m *MemoryRepository) Put(ccr *pb.CustomCostResponse) error {
	m.rwLock.Lock()
	defer m.rwLock.Unlock()

	if ccr == nil {
		return fmt.Errorf("MemoryRepository: Put: cannot save nil")
	}

	if ccr.Start == nil || ccr.End == nil {
		return fmt.Errorf("MemoryRepository: Put: custom cost response has invalid window")
	}

	if ccr.GetDomain() == "" {
		return fmt.Errorf("MemoryRepository: Put: custom cost response does not have a domain value")
	}

	if _, ok := m.data[ccr.GetDomain()]; !ok {
		m.data[ccr.GetDomain()] = make(map[time.Time][]byte)
	}
	b, err := proto.Marshal(ccr)
	if err != nil {
		return fmt.Errorf("MemoryRepository: Put: custom cost could not be marshalled")
	}
	m.data[ccr.GetDomain()][ccr.Start.AsTime().UTC()] = b

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
