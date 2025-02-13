package costmodel

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/opencost/opencost/pkg/errors"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util"
	"github.com/opencost/opencost/core/pkg/util/httputil"
	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
	"github.com/opencost/opencost/pkg/env"
)

const (
	// SplitTypeWeighted signals that shared costs should be shared
	// proportionally, rather than evenly
	SplitTypeWeighted = "weighted"

	// UnallocatedSubfield indicates an allocation datum that does not have the
	// chosen Aggregator; e.g. during aggregation by some label, there may be
	// cost data that do not have the given label.
	UnallocatedSubfield = "__unallocated__"
)

// ParseAggregationProperties attempts to parse and return aggregation properties
// encoded under the given key. If none exist, or if parsing fails, an error
// is returned with empty AllocationProperties.
func ParseAggregationProperties(aggregations []string) ([]string, error) {
	aggregateBy := []string{}
	// In case of no aggregation option, aggregate to the container, with a key Cluster/Node/Namespace/Pod/Container
	if len(aggregations) == 0 {
		aggregateBy = []string{
			opencost.AllocationClusterProp,
			opencost.AllocationNodeProp,
			opencost.AllocationNamespaceProp,
			opencost.AllocationPodProp,
			opencost.AllocationContainerProp,
		}
	} else if len(aggregations) == 1 && aggregations[0] == "all" {
		aggregateBy = []string{}
	} else {
		for _, agg := range aggregations {
			aggregate := strings.TrimSpace(agg)
			if aggregate != "" {
				if prop, err := opencost.ParseProperty(aggregate); err == nil {
					aggregateBy = append(aggregateBy, string(prop))
				} else if strings.HasPrefix(aggregate, "label:") {
					aggregateBy = append(aggregateBy, aggregate)
				} else if strings.HasPrefix(aggregate, "annotation:") {
					aggregateBy = append(aggregateBy, aggregate)
				}
			}
		}
	}
	return aggregateBy, nil
}

func (a *Accesses) warmAggregateCostModelCache() {
	const clusterCostsCacheMinutes = 5.0

	// Only allow one concurrent cache-warming operation
	sem := util.NewSemaphore(1)

	// Set default values, pulling them from application settings where applicable, and warm the cache
	// for the given duration. Cache is intentionally set to expire (i.e. noExpireCache=false) so that
	// if the default parameters change, the old cached defaults with eventually expire. Thus, the
	// timing of the cache expiry/refresh is the only mechanism ensuring 100% cache warmth.
	warmFunc := func(duration, offset time.Duration, cacheEfficiencyData bool) error {
		fmtDuration, fmtOffset := timeutil.DurationOffsetStrings(duration, offset)
		durationHrs, _ := timeutil.FormatDurationStringDaysToHours(fmtDuration)

		windowStr := fmt.Sprintf("%s offset %s", fmtDuration, fmtOffset)
		window, err := opencost.ParseWindowUTC(windowStr)
		if err != nil {
			return fmt.Errorf("invalid window from window string: %s", windowStr)
		}

		key := fmt.Sprintf("%s:%s", durationHrs, fmtOffset)

		totals, err := a.ComputeClusterCosts(a.DataSource, a.CloudProvider, duration, offset, cacheEfficiencyData)
		if err != nil {
			log.Infof("Error building cluster costs cache %s", key)
		}
		maxMinutesWithData := 0.0
		for _, cluster := range totals {
			if cluster.DataMinutes > maxMinutesWithData {
				maxMinutesWithData = cluster.DataMinutes
			}
		}
		if len(totals) > 0 && maxMinutesWithData > clusterCostsCacheMinutes {
			a.ClusterCostsCache.Set(key, totals, a.GetCacheExpiration(window.Duration()))
			log.Infof("caching %s cluster costs for %s", fmtDuration, a.GetCacheExpiration(window.Duration()))
		} else {
			log.Warnf("not caching %s cluster costs: no data or less than %f minutes data ", fmtDuration, clusterCostsCacheMinutes)
		}
		return err
	}

	// 1 day
	go func(sem *util.Semaphore) {
		defer errors.HandlePanic()

		offset := time.Minute
		duration := 24 * time.Hour

		for {
			sem.Acquire()
			warmFunc(duration, offset, true)
			sem.Return()

			log.Infof("aggregation: warm cache: %s", timeutil.DurationString(duration))
			time.Sleep(a.GetCacheRefresh(duration))
		}
	}(sem)

	if !env.IsETLEnabled() {
		// 2 day
		go func(sem *util.Semaphore) {
			defer errors.HandlePanic()

			offset := time.Minute
			duration := 2 * 24 * time.Hour

			for {
				sem.Acquire()
				warmFunc(duration, offset, false)
				sem.Return()

				log.Infof("aggregation: warm cache: %s", timeutil.DurationString(duration))
				time.Sleep(a.GetCacheRefresh(duration))
			}
		}(sem)

		// 7 day
		go func(sem *util.Semaphore) {
			defer errors.HandlePanic()

			offset := time.Minute
			duration := 7 * 24 * time.Hour

			for {
				sem.Acquire()
				err := warmFunc(duration, offset, false)
				sem.Return()

				log.Infof("aggregation: warm cache: %s", timeutil.DurationString(duration))
				if err == nil {
					time.Sleep(a.GetCacheRefresh(duration))
				} else {
					time.Sleep(5 * time.Minute)
				}
			}
		}(sem)

		// 30 day
		go func(sem *util.Semaphore) {
			defer errors.HandlePanic()

			for {
				offset := time.Minute
				duration := 30 * 24 * time.Hour

				sem.Acquire()
				err := warmFunc(duration, offset, false)
				sem.Return()
				if err == nil {
					time.Sleep(a.GetCacheRefresh(duration))
				} else {
					time.Sleep(5 * time.Minute)
				}
			}
		}(sem)
	}
}

func (a *Accesses) ComputeAllocationHandlerSummary(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")

	qp := httputil.NewQueryParams(r.URL.Query())

	// Window is a required field describing the window of time over which to
	// compute allocation data.
	window, err := opencost.ParseWindowWithOffset(qp.Get("window", ""), env.GetParsedUTCOffset())
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid 'window' parameter: %s", err), http.StatusBadRequest)
	}

	// Step is an optional parameter that defines the duration per-set, i.e.
	// the window for an AllocationSet, of the AllocationSetRange to be
	// computed. Defaults to the window size, making one set.
	step := qp.GetDuration("step", window.Duration())

	// Resolution is an optional parameter, defaulting to the configured ETL
	// resolution.
	resolution := qp.GetDuration("resolution", env.GetETLResolution())

	// Aggregation is a required comma-separated list of fields by which to
	// aggregate results. Some fields allow a sub-field, which is distinguished
	// with a colon; e.g. "label:app".
	// Examples: "namespace", "namespace,label:app"
	aggregations := qp.GetList("aggregate", ",")
	aggregateBy, err := ParseAggregationProperties(aggregations)
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid 'aggregate' parameter: %s", err), http.StatusBadRequest)
	}

	// Accumulate is an optional parameter, defaulting to false, which if true
	// sums each Set in the Range, producing one Set.
	accumulate := qp.GetBool("accumulate", false)

	// Query for AllocationSets in increments of the given step duration,
	// appending each to the AllocationSetRange.
	asr := opencost.NewAllocationSetRange()
	stepStart := *window.Start()
	for window.End().After(stepStart) {
		stepEnd := stepStart.Add(step)
		stepWindow := opencost.NewWindow(&stepStart, &stepEnd)

		as, err := a.Model.ComputeAllocation(*stepWindow.Start(), *stepWindow.End(), resolution)
		if err != nil {
			WriteError(w, InternalServerError(err.Error()))
			return
		}
		asr.Append(as)

		stepStart = stepEnd
	}

	// Aggregate, if requested
	if len(aggregateBy) > 0 {
		err = asr.AggregateBy(aggregateBy, nil)
		if err != nil {
			WriteError(w, InternalServerError(err.Error()))
			return
		}
	}

	// Accumulate, if requested
	if accumulate {
		asr, err = asr.Accumulate(opencost.AccumulateOptionAll)
		if err != nil {
			WriteError(w, InternalServerError(err.Error()))
			return
		}
	}

	sasl := []*opencost.SummaryAllocationSet{}
	for _, as := range asr.Slice() {
		sas := opencost.NewSummaryAllocationSet(as, nil, nil, false, false)
		sasl = append(sasl, sas)
	}
	sasr := opencost.NewSummaryAllocationSetRange(sasl...)

	w.Write(WrapData(sasr, nil))
}

// ComputeAllocationHandler computes an AllocationSetRange from the CostModel.
func (a *Accesses) ComputeAllocationHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")

	qp := httputil.NewQueryParams(r.URL.Query())

	// Window is a required field describing the window of time over which to
	// compute allocation data.
	window, err := opencost.ParseWindowWithOffset(qp.Get("window", ""), env.GetParsedUTCOffset())
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid 'window' parameter: %s", err), http.StatusBadRequest)
	}

	// Resolution is an optional parameter, defaulting to the configured ETL
	// resolution.
	resolution := qp.GetDuration("resolution", env.GetETLResolution())

	// Step is an optional parameter that defines the duration per-set, i.e.
	// the window for an AllocationSet, of the AllocationSetRange to be
	// computed. Defaults to the window size, making one set.
	step := qp.GetDuration("step", window.Duration())

	// Aggregation is an optional comma-separated list of fields by which to
	// aggregate results. Some fields allow a sub-field, which is distinguished
	// with a colon; e.g. "label:app".
	// Examples: "namespace", "namespace,label:app"
	aggregations := qp.GetList("aggregate", ",")
	aggregateBy, err := ParseAggregationProperties(aggregations)
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid 'aggregate' parameter: %s", err), http.StatusBadRequest)
	}

	// IncludeIdle, if true, uses Asset data to incorporate Idle Allocation
	includeIdle := qp.GetBool("includeIdle", false)
	// Accumulate is an optional parameter, defaulting to false, which if true
	// sums each Set in the Range, producing one Set.
	accumulate := qp.GetBool("accumulate", false)

	// Accumulate is an optional parameter that accumulates an AllocationSetRange
	// by the resolution of the given time duration.
	// Defaults to 0. If a value is not passed then the parameter is not used.
	accumulateBy := opencost.AccumulateOption(qp.Get("accumulateBy", ""))

	// if accumulateBy is not explicitly set, and accumulate is true, ensure result is accumulated
	if accumulateBy == opencost.AccumulateOptionNone && accumulate {
		accumulateBy = opencost.AccumulateOptionAll
	}

	// IdleByNode, if true, computes idle allocations at the node level.
	// Otherwise it is computed at the cluster level. (Not relevant if idle
	// is not included.)
	idleByNode := qp.GetBool("idleByNode", false)
	sharedLoadBalancer := qp.GetBool("sharelb", false)

	// IncludeProportionalAssetResourceCosts, if true,
	includeProportionalAssetResourceCosts := qp.GetBool("includeProportionalAssetResourceCosts", false)

	// include aggregated labels/annotations if true
	includeAggregatedMetadata := qp.GetBool("includeAggregatedMetadata", false)

	shareIdle := qp.GetBool("shareIdle", false)

	asr, err := a.Model.QueryAllocation(window, resolution, step, aggregateBy, includeIdle, idleByNode, includeProportionalAssetResourceCosts, includeAggregatedMetadata, sharedLoadBalancer, accumulateBy, shareIdle)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "bad request") {
			WriteError(w, BadRequest(err.Error()))
		} else {
			WriteError(w, InternalServerError(err.Error()))
		}

		return
	}

	w.Write(WrapData(asr, nil))
}

// The below was transferred from a different package in order to maintain
// previous behavior. Ultimately, we should clean this up at some point.
// TODO move to util and/or standardize everything

type Error struct {
	StatusCode int
	Body       string
}

func WriteError(w http.ResponseWriter, err Error) {
	status := err.StatusCode
	if status == 0 {
		status = http.StatusInternalServerError
	}
	w.WriteHeader(status)

	resp, _ := json.Marshal(&Response{
		Code:    status,
		Message: fmt.Sprintf("Error: %s", err.Body),
	})
	w.Write(resp)
}

func BadRequest(message string) Error {
	return Error{
		StatusCode: http.StatusBadRequest,
		Body:       message,
	}
}

func InternalServerError(message string) Error {
	if message == "" {
		message = "Internal Server Error"
	}
	return Error{
		StatusCode: http.StatusInternalServerError,
		Body:       message,
	}
}

func NotFound() Error {
	return Error{
		StatusCode: http.StatusNotFound,
		Body:       "Not Found",
	}
}
