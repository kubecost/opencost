package cloudcost

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/stringutil"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
	"github.com/opencost/opencost/pkg/cloud"
	"github.com/opencost/opencost/pkg/env"
	"github.com/opencost/opencost/pkg/errors"
)

// IngestorStatus includes diagnostic values for a given Ingestor
type IngestorStatus struct {
	Created          time.Time
	LastRun          time.Time
	NextRun          time.Time
	Runs             int
	Coverage         opencost.Window
	ConnectionStatus cloud.ConnectionStatus
}

// IngestorConfig is a configuration struct for an Ingestor
type IngestorConfig struct {
	MonthToDateRunInterval int
	RefreshRate            time.Duration
	Resolution             time.Duration
	Duration               time.Duration
	QueryWindow            time.Duration
	RunWindow              time.Duration
}

// DefaultIngestorConfiguration retrieves an IngestorConfig from env variables
func DefaultIngestorConfiguration() IngestorConfig {
	return IngestorConfig{
		Resolution:             timeutil.Day,
		Duration:               timeutil.Day * time.Duration(env.GetDataRetentionDailyResolutionDays()),
		MonthToDateRunInterval: env.GetCloudCostMonthToDateInterval(),
		RefreshRate:            time.Hour * time.Duration(env.GetCloudCostRefreshRateHours()),
		QueryWindow:            timeutil.Day * time.Duration(env.GetCloudCostQueryWindowDays()),
		RunWindow:              timeutil.Day * time.Duration(env.GetCloudCostRunWindowDays()),
	}
}

// ingestor runs the process for ingesting CloudCost from its CloudCostIntegration and store it in a Repository
type ingestor struct {
	key          string
	integration  CloudCostIntegration
	config       IngestorConfig
	repo         Repository
	runID        string
	lastRun      time.Time
	runs         int
	creationTime time.Time
	coverage     opencost.Window
	coverageLock sync.Mutex
	isRunning    atomic.Bool
	isStopping   atomic.Bool
	exitBuildCh  chan string
	exitRunCh    chan string
}

// NewIngestor is an initializer for ingestor
func NewIngestor(ingestorConfig IngestorConfig, repo Repository, config cloud.KeyedConfig) (*ingestor, error) {
	if repo == nil {
		return nil, fmt.Errorf("CloudCost: NewIngestor: repository connot be nil")
	}
	if config == nil {
		return nil, fmt.Errorf("CloudCost: NewIngestor: integration connot be nil")
	}
	cci := GetIntegrationFromConfig(config)
	if cci == nil {
		return nil, fmt.Errorf("CloudCost: NewIngestor: provider integration config was not a valid type: %T", config)
	}
	now := time.Now().UTC()
	midnight := opencost.RoundForward(now, timeutil.Day)
	return &ingestor{
		config:       ingestorConfig,
		repo:         repo,
		key:          config.Key(),
		integration:  cci,
		creationTime: now,
		lastRun:      now,
		coverage:     opencost.NewClosedWindow(midnight, midnight),
	}, nil
}

func (ing *ingestor) LoadWindow(start, end time.Time) {
	windows, err := opencost.GetWindows(start, end, timeutil.Day)
	if err != nil {
		log.Errorf("CloudCost[%s]: ingestor: invalid window %s", ing.key, opencost.NewWindow(&start, &end))
		return
	}

	for _, window := range windows {
		has, err2 := ing.repo.Has(*window.Start(), ing.key)
		if err2 != nil {
			log.Errorf("CloudCost[%s]: ingestor: error when loading window: %s", ing.key, err2.Error())
		}
		if !has {
			ing.BuildWindow(start, end)
			return
		}
		ing.expandCoverage(window)
		log.Debugf("CloudCost[%s]: ingestor: skipping build for window %s, coverage already exists", ing.key, window.String())
	}

}

func (ing *ingestor) BuildWindow(start, end time.Time) {
	log.Infof("CloudCost[%s]: ingestor: building window %s", ing.key, opencost.NewWindow(&start, &end))
	ccsr, err := ing.integration.GetCloudCost(start, end)
	if err != nil {
		log.Errorf("CloudCost[%s]: ingestor: build failed for window %s: %s", ing.key, opencost.NewWindow(&start, &end), err.Error())
		return
	}
	for _, ccs := range ccsr.CloudCostSets {
		log.Debugf("BuildWindow[%s]: GetCloudCost: writing cloud costs for window %s: %d", ccs.Integration, ccs.Window, len(ccs.CloudCosts))
		err2 := ing.repo.Put(ccs)
		if err2 != nil {
			log.Errorf("CloudCost[%s]: ingestor: failed to save Cloud Cost Set with window %s: %s", ing.key, ccs.GetWindow().String(), err2.Error())
		}
		ing.expandCoverage(ccs.Window)
	}
}

func (ing *ingestor) Start(rebuild bool) {

	// If already running, log that and return.
	if !ing.isRunning.CompareAndSwap(false, true) {
		log.Infof("CloudCost: ingestor: is already running")
		return
	}

	ing.runID = stringutil.RandSeq(5)

	ing.exitBuildCh = make(chan string)
	ing.exitRunCh = make(chan string)

	// Build the store once, advancing backward in time from the earliest
	// point of coverage.
	go ing.build(rebuild)

	go ing.run()
}

func (ing *ingestor) Stop() {
	// If already stopping, log that and return.
	if !ing.isStopping.CompareAndSwap(false, true) {
		log.Infof("CloudCost: ingestor: is already stopping")
		return
	}

	msg := "Stopping"

	// If the processes are running (and thus there are channels available for
	// stopping them) then stop all sub-processes (i.e. build and run)
	var wg sync.WaitGroup

	if ing.exitBuildCh != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ing.exitBuildCh <- msg
		}()
	}

	if ing.exitRunCh != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ing.exitRunCh <- msg
		}()
	}

	wg.Wait()

	// Declare that the store is officially no longer running. This allows
	// Start to be called again, restarting the store from scratch.
	ing.isRunning.Store(false)
	ing.isStopping.Store(false)
}

// Status returns an IngestorStatus that describes the current state of the ingestor
func (ing *ingestor) Status() IngestorStatus {
	return IngestorStatus{
		Created:          ing.creationTime,
		LastRun:          ing.lastRun,
		NextRun:          ing.lastRun.Add(ing.config.RefreshRate).UTC(),
		Runs:             ing.runs,
		Coverage:         ing.coverage,
		ConnectionStatus: ing.integration.GetStatus(),
	}
}

func (ing *ingestor) build(rebuild bool) {
	defer errors.HandlePanic()

	// Profile the full Duration of the build time
	buildStart := time.Now()

	// Build as far back as the configures build Duration
	limit := opencost.RoundBack(time.Now().UTC().Add(-ing.config.Duration), ing.config.Resolution)

	queryWindowStr := timeutil.FormatStoreResolution(ing.config.QueryWindow)
	log.Infof("CloudCost[%s]: ingestor: build[%s]: Starting build back to %s in blocks of %s", ing.key, ing.runID, limit.String(), queryWindowStr)

	// Start with a window of the configured Duration and ending on the given
	// start time. Build windows repeating until the window reaches the
	// given limit time

	// Round end times back to nearest Resolution points in the past,
	// querying for exactly one interval
	e := opencost.RoundBack(time.Now().UTC(), ing.config.Resolution)
	s := e.Add(-ing.config.QueryWindow)

	// Continue until limit is reached
	for limit.Before(e) {
		// If exit instruction is received, log and return
		select {
		case <-ing.exitBuildCh:
			log.Debugf("CloudCost[%s]: ingestor: build[%s]: exiting", ing.key, ing.runID)
			return
		default:
		}

		// Profile the current build step
		stepStart := time.Now()

		// if rebuild is not specified then check for existing coverage on window
		if rebuild {
			ing.BuildWindow(s, e)
		} else {
			ing.LoadWindow(s, e)
		}

		log.Infof("CloudCost[%s]: ingestor: build[%s]:  %s in %v", ing.key, ing.runID, opencost.NewClosedWindow(s, e), time.Since(stepStart))

		// Shift to next QueryWindow
		s = s.Add(-ing.config.QueryWindow)
		if s.Before(limit) {
			s = limit
		}
		e = e.Add(-ing.config.QueryWindow)
	}

	log.Infof(fmt.Sprintf("CloudCost[%s]: ingestor: build[%s]: completed in %v", ing.key, ing.runID, time.Since(buildStart)))

	// In order to be able to Stop, we have to wait on an exit message
	// here
	<-ing.exitBuildCh

}

func (ing *ingestor) run() {
	defer errors.HandlePanic()

	ticker := timeutil.NewJobTicker()
	defer ticker.Close()
	ticker.TickIn(0)

	for {
		// If an exit instruction is received, break the run loop
		select {
		case <-ing.exitRunCh:
			log.Debugf("CloudCost[%s]: ingestor: Run[%s] exiting", ing.key, ing.runID)
			return
		case <-ticker.Ch:
			// Wait for next tick
		}

		// Start from the last covered time, minus the RunWindow
		start := ing.lastRun
		start = start.Add(-ing.config.RunWindow)

		// Every Nth (determined by the MonthToDateRunInterval) run should be a month to date run. Where the start is
		// truncated to the beginning of its current month this can mean that early in a new month we will build all of
		// last month and the first few days of the current month.
		if ing.runs%ing.config.MonthToDateRunInterval == 0 {
			start = time.Date(start.Year(), start.Month(), 1, 0, 0, 0, 0, time.UTC)
			log.Infof("CloudCost[%s]: ingestor: Run[%s]: running month-to-date update starting at %s", ing.key, ing.runID, start.String())
		}

		// Round start time back to the nearest Resolution point in the past from the
		// last update to the QueryWindow
		s := opencost.RoundBack(start.UTC(), ing.config.Resolution)
		e := s.Add(ing.config.QueryWindow)

		// Start with a window of the configured Duration and starting on the given
		// start time. Do the following, repeating until the window reaches the
		// current time:
		// 1. Instruct builder to build window
		// 2. Move window forward one Resolution
		for time.Now().After(s) {
			profStart := time.Now()
			ing.BuildWindow(s, e)

			log.Debugf("CloudCost[%s]: ingestor: Run[%s]: completed %s in %v", ing.key, ing.runID, opencost.NewWindow(&s, &e), time.Since(profStart))

			s = s.Add(ing.config.QueryWindow)
			e = e.Add(ing.config.QueryWindow)
			// prevent builds into the future
			if e.After(time.Now().UTC()) {
				e = opencost.RoundForward(time.Now().UTC(), ing.config.Resolution)
			}

		}
		ing.lastRun = time.Now().UTC()

		limit := opencost.RoundBack(time.Now().UTC(), ing.config.Resolution).Add(-ing.config.Duration)
		err := ing.repo.Expire(limit)
		if err != nil {
			log.Errorf("CloudCost: Ingestor: failed to expire Data: %s", err)
		}

		ing.coverageLock.Lock()
		ing.coverage = ing.coverage.ContractStart(limit)
		ing.coverageLock.Unlock()

		ing.runs++

		ticker.TickIn(ing.config.RefreshRate)
	}
}

func (ing *ingestor) expandCoverage(window opencost.Window) {
	if window.IsOpen() {
		return
	}
	ing.coverageLock.Lock()
	defer ing.coverageLock.Unlock()

	coverage := ing.coverage.ExpandStart(*window.Start())
	coverage = coverage.ExpandEnd(*window.End())

	ing.coverage = coverage
}
