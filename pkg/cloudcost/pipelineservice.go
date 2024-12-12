package cloudcost

import (
	"fmt"
	"net/http"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/opencost/opencost/core/pkg/opencost"
	proto "github.com/opencost/opencost/core/pkg/protocol"
	cloudconfig "github.com/opencost/opencost/pkg/cloud"
	"github.com/opencost/opencost/pkg/cloud/config"
	"github.com/opencost/opencost/pkg/env"
)

var protocol = proto.HTTP()

// PipelineService exposes CloudCost pipeline controls and diagnostics endpoints
type PipelineService struct {
	ingestionManager *IngestionManager
	store            Repository
	configController *config.Controller
}

// NewPipelineService is a constructor for a PipelineService
func NewPipelineService(repo Repository, ic *config.Controller, ingConf IngestorConfig) *PipelineService {
	im := NewIngestionManager(ic, repo, ingConf)
	return &PipelineService{
		ingestionManager: im,
		store:            repo,
		configController: ic,
	}
}

// Status merges status values from the config.Controller and the IngestionManager to give a combined view of that state
// of configs and their ingestion status
func (dp *PipelineService) Status() []Status {
	// Pull config status from the config controller
	confStatuses := dp.configController.GetStatus()

	statuses := make([]Status, 0, len(confStatuses))

	refreshRate := time.Hour * time.Duration(env.GetCloudCostRefreshRateHours())
	for _, confStat := range confStatuses {
		var conf cloudconfig.Config
		var provider string
		if confStat.Config != nil {
			conf = confStat.Config.Sanitize()
			provider = confStat.Config.Provider()
		}

		var ingestorStatus IngestorStatus
		if ing, ok := dp.ingestionManager.ingestors[confStat.Key]; ok {
			ingestorStatus = ing.Status()
		}

		// These are the statuses
		status := Status{
			Key:              confStat.Key,
			Source:           confStat.Source.String(),
			Active:           confStat.Active,
			Valid:            confStat.Valid,
			Config:           conf,
			Provider:         provider,
			ConnectionStatus: ingestorStatus.ConnectionStatus.String(),
			LastRun:          ingestorStatus.LastRun,
			NextRun:          ingestorStatus.NextRun,
			Runs:             ingestorStatus.Runs,
			Created:          ingestorStatus.Created,
			Coverage:         ingestorStatus.Coverage.String(),
			RefreshRate:      refreshRate.String(),
		}
		statuses = append(statuses, status)
	}

	return statuses
}

// GetCloudCostRebuildHandler creates a handler from a http request which initiates a rebuild of cloud cost pipeline, if an
// integrationKey is provided then it only rebuilds the specified billing integration
func (s *PipelineService) GetCloudCostRebuildHandler() func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	// If Reporting Service is nil, always return 501
	if s == nil {
		return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
			http.Error(w, "Cloud Cost Pipeline Service is nil", http.StatusNotImplemented)
		}
	}
	if s.ingestionManager == nil {
		return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
			http.Error(w, "Cloud Cost Pipeline Service Ingestion Manager is nil", http.StatusNotImplemented)
		}
	}
	// Return valid handler func
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		w.Header().Set("Content-Type", "application/json")

		commit := r.URL.Query().Get("commit") == "true" || r.URL.Query().Get("commit") == "1"

		if !commit {
			protocol.WriteData(w, "Pass parameter 'commit=true' to confirm Cloud Cost rebuild")
			return
		}

		integrationKey := r.URL.Query().Get("integrationKey")

		// If no providerKey argument was provider, restart all Cloud Asset Pipelines
		if integrationKey == "" {
			s.ingestionManager.RebuildAll()
			protocol.WriteData(w, "Rebuilding Cloud Usage For All Providers")
			return
		} else {
			err := s.ingestionManager.Rebuild(integrationKey)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			protocol.WriteData(w, fmt.Sprintf("Rebuilding Cloud Usage For Provider %s", integrationKey))
			return
		}
	}
}

// GetCloudCostRepairHandler creates a handler from a http request which initiates a repair of cloud cost for a given window, if an
// integrationKey is provided then it only repairs the specified integration
func (s *PipelineService) GetCloudCostRepairHandler() func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	// If Reporting Service is nil, always return 501
	if s == nil {
		return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
			http.Error(w, "Reporting Service is nil", http.StatusNotImplemented)
		}
	}
	if s.ingestionManager == nil {
		return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
			http.Error(w, "Cloud Cost Pipeline Service Ingestion Manager is nil", http.StatusNotImplemented)
		}
	}
	// Return valid handler func
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		w.Header().Set("Content-Type", "application/json")

		windowStr := r.URL.Query().Get("window")

		var window opencost.Window
		if windowStr != "" {
			win, err := opencost.ParseWindowWithOffset(windowStr, env.GetParsedUTCOffset())
			if err != nil {
				http.Error(w, fmt.Sprintf("Invalid parameter: %s", err), http.StatusBadRequest)
				return
			}
			window = win
		}

		integrationKey := r.URL.Query().Get("integrationKey")

		// If no providerKey argument was provider, restart all Cloud Asset Pipelines
		if integrationKey == "" {
			err := s.ingestionManager.RepairAll(*window.Start(), *window.End())
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			protocol.WriteData(w, "Rebuilding Cloud Usage For All Providers")
			return
		} else {
			err := s.ingestionManager.Repair(integrationKey, *window.Start(), *window.End())
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			protocol.WriteData(w, fmt.Sprintf("Rebuilding Cloud Usage For Provider %s", integrationKey))
			return
		}
	}
}

// GetCloudCostStatusHandler creates a handler from a http request which returns a list of the billing integration status
func (s *PipelineService) GetCloudCostStatusHandler() func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	// If Reporting Service is nil, always return 501
	if s == nil {
		return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
			http.Error(w, "Reporting Service is nil", http.StatusNotImplemented)
		}
	}
	if s.ingestionManager == nil {
		return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
			http.Error(w, "Cloud Cost Pipeline Service Ingestion Manager is nil", http.StatusNotImplemented)
		}
	}

	// Return valid handler func
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		w.Header().Set("Content-Type", "application/json")

		protocol.WriteData(w, s.Status())
	}
}
