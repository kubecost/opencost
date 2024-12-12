package services

import (
	"sync"

	"github.com/julienschmidt/httprouter"
	"github.com/opencost/opencost/core/pkg/log"
)

// HTTPService defines an implementation prototype for an object capable of registering
// endpoints on an http router which provide an service relevant to the cost-model.
type HTTPService interface {
	// Register assigns the endpoints and returns an error on failure.
	Register(*httprouter.Router) error
}

// HTTPServices defines an implementation prototype for an object capable of managing and registering
// predefined HTTPService routes
type HTTPServices interface {
	// Add a HTTPService implementation for registration
	Add(service HTTPService)

	// RegisterAll registers all the services added with the provided router
	RegisterAll(*httprouter.Router) error
}

type defaultHTTPServices struct {
	sync.Mutex
	services []HTTPService
}

// Add a HTTPService implementation for
func (dhs *defaultHTTPServices) Add(service HTTPService) {
	if service == nil {
		log.Warnf("Attempting to Add nil HTTPService")
		return
	}

	dhs.Lock()
	defer dhs.Unlock()

	dhs.services = append(dhs.services, service)
}

// RegisterAll registers all the services added with the provided router
func (dhs *defaultHTTPServices) RegisterAll(router *httprouter.Router) error {
	dhs.Lock()
	defer dhs.Unlock()

	for _, svc := range dhs.services {
		svc.Register(router)
	}

	return nil
}

// NewCostModelServices creates an HTTPServices implementation containing any predefined
// http services used with the cost-model
func NewCostModelServices() HTTPServices {
	return &defaultHTTPServices{
		services: []HTTPService{
			NewClusterManagerService(),
		},
	}
}
