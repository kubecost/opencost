package clustercache

import (
	"sync"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

// GenericStore is a generic store implementation. It converts objects to a different type using a transform function.
// The main purpose is to reduce a memory footprint by storing only the necessary data.
type GenericStore[Input UIDGetter, Output any] struct {
	mutex         sync.RWMutex
	items         map[types.UID]Output
	transformFunc func(input Input) Output

	// storing this cyclic reflector allows us to defer watching
	reflector *cache.Reflector
	onInit    func()
}

// NewGenericStore creates a new instance of GenericStore.
func NewGenericStore[Input UIDGetter, Output any](transformFunc func(input Input) Output) *GenericStore[Input, Output] {
	return &GenericStore[Input, Output]{
		items:         make(map[types.UID]Output),
		transformFunc: transformFunc,
	}
}

type UIDGetter interface {
	GetUID() types.UID
}

func CreateStore[Input UIDGetter, Output any](
	restClient rest.Interface,
	resource string,
	transformFunc func(input Input) Output,
) *GenericStore[Input, Output] {
	lw := cache.NewListWatchFromClient(restClient, resource, v1.NamespaceAll, fields.Everything())
	store := NewGenericStore(transformFunc)
	var zeroValue Input
	store.reflector = cache.NewReflector(lw, zeroValue, store, 0)

	return store
}

func (s *GenericStore[Input, Output]) Watch(stopCh <-chan struct{}, onInit func()) {
	s.onInit = onInit

	// reflector.Run() will eventually call Replace() on the store with the initial contents
	// of the resource list. we'll call onInit after that happens the _first_ time
	go s.reflector.Run(stopCh)
}

// Add inserts an object into the store.
func (s *GenericStore[Input, Output]) Add(obj any) error {
	return s.Update(obj)
}

// Update updates the existing entry in the store.
func (s *GenericStore[Input, Output]) Update(obj any) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	item := obj.(Input)
	s.items[item.GetUID()] = s.transformFunc(item)

	return nil
}

// Delete removes an object from the store.
func (s *GenericStore[Input, Output]) Delete(obj any) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	item := obj.(Input)
	delete(s.items, item.GetUID())

	return nil
}

// GetAll returns all stored objects.
func (s *GenericStore[Input, Output]) GetAll() []Output {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	allItems := make([]Output, 0, len(s.items))
	for _, item := range s.items {
		allItems = append(allItems, item)
	}
	return allItems
}

// Replace replaces the current list of items in the store.
func (s *GenericStore[Input, Output]) Replace(list []any, _ string) error {
	s.mutex.Lock()
	s.items = make(map[types.UID]Output, len(list))
	s.mutex.Unlock()

	for _, o := range list {
		err := s.Add(o)
		if err != nil {
			return err
		}
	}

	// call onInit after the initial list has been processed
	if s.onInit != nil {
		s.onInit()
		s.onInit = nil
	}

	return nil
}

// Stubs to satisfy the cache.Store interface
func (s *GenericStore[Input, Output]) List() []interface{} {
	return nil
}
func (s *GenericStore[Input, Output]) ListKeys() []string {
	return nil
}
func (s *GenericStore[Input, Output]) Get(_ interface{}) (item interface{}, exists bool, err error) {
	return nil, false, nil
}
func (s *GenericStore[Input, Output]) GetByKey(_ string) (item interface{}, exists bool, err error) {
	return nil, false, nil
}
func (s *GenericStore[Input, Output]) Resync() error {
	return nil
}
