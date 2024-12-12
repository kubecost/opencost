package costmodel

import (
	"fmt"
	"math"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/opencost/opencost/pkg/cloud/provider"
	"github.com/patrickmn/go-cache"
	prometheusClient "github.com/prometheus/client_golang/api"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util"
	"github.com/opencost/opencost/core/pkg/util/httputil"
	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/core/pkg/util/promutil"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/env"
	"github.com/opencost/opencost/pkg/errors"
	"github.com/opencost/opencost/pkg/prom"
	"github.com/opencost/opencost/pkg/thanos"
)

const (
	// SplitTypeWeighted signals that shared costs should be shared
	// proportionally, rather than evenly
	SplitTypeWeighted = "weighted"

	// UnallocatedSubfield indicates an allocation datum that does not have the
	// chosen Aggregator; e.g. during aggregation by some label, there may be
	// cost data that do not have the given label.
	UnallocatedSubfield = "__unallocated__"

	clusterCostsCacheMinutes = 5.0
)

// Aggregation describes aggregated cost data, containing cumulative cost and
// allocation data per resource, vectors of rate data per resource, efficiency
// data, and metadata describing the type of aggregation operation.
type Aggregation struct {
	Aggregator                 string                         `json:"aggregation"`
	Subfields                  []string                       `json:"subfields,omitempty"`
	Environment                string                         `json:"environment"`
	Cluster                    string                         `json:"cluster,omitempty"`
	Properties                 *opencost.AllocationProperties `json:"-"`
	Start                      time.Time                      `json:"-"`
	End                        time.Time                      `json:"-"`
	CPUAllocationHourlyAverage float64                        `json:"cpuAllocationAverage"`
	CPUAllocationVectors       []*util.Vector                 `json:"-"`
	CPUAllocationTotal         float64                        `json:"-"`
	CPUCost                    float64                        `json:"cpuCost"`
	CPUCostVector              []*util.Vector                 `json:"cpuCostVector,omitempty"`
	CPUEfficiency              float64                        `json:"cpuEfficiency"`
	CPURequestedVectors        []*util.Vector                 `json:"-"`
	CPUUsedVectors             []*util.Vector                 `json:"-"`
	Efficiency                 float64                        `json:"efficiency"`
	GPUAllocationHourlyAverage float64                        `json:"gpuAllocationAverage"`
	GPUAllocationVectors       []*util.Vector                 `json:"-"`
	GPUCost                    float64                        `json:"gpuCost"`
	GPUCostVector              []*util.Vector                 `json:"gpuCostVector,omitempty"`
	GPUAllocationTotal         float64                        `json:"-"`
	RAMAllocationHourlyAverage float64                        `json:"ramAllocationAverage"`
	RAMAllocationVectors       []*util.Vector                 `json:"-"`
	RAMAllocationTotal         float64                        `json:"-"`
	RAMCost                    float64                        `json:"ramCost"`
	RAMCostVector              []*util.Vector                 `json:"ramCostVector,omitempty"`
	RAMEfficiency              float64                        `json:"ramEfficiency"`
	RAMRequestedVectors        []*util.Vector                 `json:"-"`
	RAMUsedVectors             []*util.Vector                 `json:"-"`
	PVAllocationHourlyAverage  float64                        `json:"pvAllocationAverage"`
	PVAllocationVectors        []*util.Vector                 `json:"-"`
	PVAllocationTotal          float64                        `json:"-"`
	PVCost                     float64                        `json:"pvCost"`
	PVCostVector               []*util.Vector                 `json:"pvCostVector,omitempty"`
	NetworkCost                float64                        `json:"networkCost"`
	NetworkCostVector          []*util.Vector                 `json:"networkCostVector,omitempty"`
	SharedCost                 float64                        `json:"sharedCost"`
	TotalCost                  float64                        `json:"totalCost"`
	TotalCostVector            []*util.Vector                 `json:"totalCostVector,omitempty"`
}

// TotalHours determines the amount of hours the Aggregation covers, as a
// function of the cost vectors and the resolution of those vectors' data
func (a *Aggregation) TotalHours(resolutionHours float64) float64 {
	length := 1

	if length < len(a.CPUCostVector) {
		length = len(a.CPUCostVector)
	}
	if length < len(a.RAMCostVector) {
		length = len(a.RAMCostVector)
	}
	if length < len(a.PVCostVector) {
		length = len(a.PVCostVector)
	}
	if length < len(a.GPUCostVector) {
		length = len(a.GPUCostVector)
	}
	if length < len(a.NetworkCostVector) {
		length = len(a.NetworkCostVector)
	}

	return float64(length) * resolutionHours
}

// RateCoefficient computes the coefficient by which the total cost needs to be
// multiplied in order to convert totals costs into per-rate costs.
func (a *Aggregation) RateCoefficient(rateStr string, resolutionHours float64) float64 {
	// monthly rate = (730.0)*(total cost)/(total hours)
	// daily rate = (24.0)*(total cost)/(total hours)
	// hourly rate = (1.0)*(total cost)/(total hours)

	// default to hourly rate
	coeff := 1.0
	switch rateStr {
	case "daily":
		coeff = timeutil.HoursPerDay
	case "monthly":
		coeff = timeutil.HoursPerMonth
	}

	return coeff / a.TotalHours(resolutionHours)
}

type SharedResourceInfo struct {
	ShareResources  bool
	SharedNamespace map[string]bool
	LabelSelectors  map[string]map[string]bool
}

type SharedCostInfo struct {
	Name      string
	Cost      float64
	ShareType string
}

func (s *SharedResourceInfo) IsSharedResource(costDatum *CostData) bool {
	// exists in a shared namespace
	if _, ok := s.SharedNamespace[costDatum.Namespace]; ok {
		return true
	}
	// has at least one shared label (OR, not AND in the case of multiple labels)
	for labelName, labelValues := range s.LabelSelectors {
		if val, ok := costDatum.Labels[labelName]; ok && labelValues[val] {
			return true
		}
	}
	return false
}

func NewSharedResourceInfo(shareResources bool, sharedNamespaces []string, labelNames []string, labelValues []string) *SharedResourceInfo {
	sr := &SharedResourceInfo{
		ShareResources:  shareResources,
		SharedNamespace: make(map[string]bool),
		LabelSelectors:  make(map[string]map[string]bool),
	}

	for _, ns := range sharedNamespaces {
		sr.SharedNamespace[strings.Trim(ns, " ")] = true
	}

	// Creating a map of label name to label value, but only if
	// the cardinality matches
	if len(labelNames) == len(labelValues) {
		for i := range labelNames {
			cleanedLname := promutil.SanitizeLabelName(strings.Trim(labelNames[i], " "))
			if values, ok := sr.LabelSelectors[cleanedLname]; ok {
				values[strings.Trim(labelValues[i], " ")] = true
			} else {
				sr.LabelSelectors[cleanedLname] = map[string]bool{strings.Trim(labelValues[i], " "): true}
			}
		}
	}

	return sr
}

func GetTotalContainerCost(costData map[string]*CostData, rate string, cp models.Provider, discount float64, customDiscount float64, idleCoefficients map[string]float64) float64 {
	totalContainerCost := 0.0
	for _, costDatum := range costData {
		clusterID := costDatum.ClusterID
		cpuv, ramv, gpuv, pvvs, netv := getPriceVectors(cp, costDatum, rate, discount, customDiscount, idleCoefficients[clusterID])
		totalContainerCost += totalVectors(cpuv)
		totalContainerCost += totalVectors(ramv)
		totalContainerCost += totalVectors(gpuv)
		for _, pv := range pvvs {
			totalContainerCost += totalVectors(pv)
		}
		totalContainerCost += totalVectors(netv)
	}
	return totalContainerCost
}

func (a *Accesses) ComputeIdleCoefficient(costData map[string]*CostData, cli prometheusClient.Client, cp models.Provider, discount float64, customDiscount float64, window, offset time.Duration) (map[string]float64, error) {
	coefficients := make(map[string]float64)

	profileName := "ComputeIdleCoefficient: ComputeClusterCosts"
	profileStart := time.Now()

	var clusterCosts map[string]*ClusterCosts
	var err error
	fmtWindow, fmtOffset := timeutil.DurationOffsetStrings(window, offset)
	key := fmt.Sprintf("%s:%s", fmtWindow, fmtOffset)
	if data, valid := a.ClusterCostsCache.Get(key); valid {
		clusterCosts = data.(map[string]*ClusterCosts)
	} else {
		clusterCosts, err = a.ComputeClusterCosts(cli, cp, window, offset, false)
		if err != nil {
			return nil, err
		}
	}

	measureTime(profileStart, profileThreshold, profileName)

	for cid, costs := range clusterCosts {
		if costs.CPUCumulative == 0 && costs.RAMCumulative == 0 && costs.StorageCumulative == 0 {
			log.Warnf("No ClusterCosts data for cluster '%s'. Is it emitting data?", cid)
			coefficients[cid] = 1.0
			continue
		}

		if costs.TotalCumulative == 0 {
			return nil, fmt.Errorf("TotalCumulative cluster cost for cluster '%s' returned 0 over window '%s' offset '%s'", cid, fmtWindow, fmtOffset)
		}

		totalContainerCost := 0.0
		for _, costDatum := range costData {
			if costDatum.ClusterID == cid {
				cpuv, ramv, gpuv, pvvs, _ := getPriceVectors(cp, costDatum, "", discount, customDiscount, 1)
				totalContainerCost += totalVectors(cpuv)
				totalContainerCost += totalVectors(ramv)
				totalContainerCost += totalVectors(gpuv)
				for _, pv := range pvvs {
					totalContainerCost += totalVectors(pv)
				}
			}
		}

		coeff := totalContainerCost / costs.TotalCumulative
		coefficients[cid] = coeff
	}

	return coefficients, nil
}

// AggregationOptions provides optional parameters to AggregateCostData, allowing callers to perform more complex operations
type AggregationOptions struct {
	Discount               float64            // percent by which to discount CPU, RAM, and GPU cost
	CustomDiscount         float64            // additional custom discount applied to all prices
	IdleCoefficients       map[string]float64 // scales costs by amount of idle resources on a per-cluster basis
	IncludeEfficiency      bool               // set to true to receive efficiency/usage data
	IncludeTimeSeries      bool               // set to true to receive time series data
	Rate                   string             // set to "hourly", "daily", or "monthly" to receive cost rate, rather than cumulative cost
	ResolutionHours        float64
	SharedResourceInfo     *SharedResourceInfo
	SharedCosts            map[string]*SharedCostInfo
	FilteredContainerCount int
	FilteredEnvironments   map[string]int
	SharedSplit            string
	TotalContainerCost     float64
}

// Helper method to test request/usgae values against allocation averages for efficiency scores. Generate a warning log if
// clamp is required
func clampAverage(requestsAvg float64, usedAverage float64, allocationAvg float64, resource string) (float64, float64) {
	rAvg := requestsAvg
	if rAvg > allocationAvg {
		log.Debugf("Average %s Requested (%f) > Average %s Allocated (%f). Clamping.", resource, rAvg, resource, allocationAvg)
		rAvg = allocationAvg
	}

	uAvg := usedAverage
	if uAvg > allocationAvg {
		log.Debugf(" Average %s Used (%f) > Average %s Allocated (%f). Clamping.", resource, uAvg, resource, allocationAvg)
		uAvg = allocationAvg
	}

	return rAvg, uAvg
}

// AggregateCostData aggregates raw cost data by field; e.g. namespace, cluster, service, or label. In the case of label, callers
// must pass a slice of subfields indicating the labels by which to group. Provider is used to define custom resource pricing.
// See AggregationOptions for optional parameters.
func AggregateCostData(costData map[string]*CostData, field string, subfields []string, cp models.Provider, opts *AggregationOptions) map[string]*Aggregation {
	discount := opts.Discount
	customDiscount := opts.CustomDiscount
	idleCoefficients := opts.IdleCoefficients
	includeTimeSeries := opts.IncludeTimeSeries
	includeEfficiency := opts.IncludeEfficiency
	rate := opts.Rate
	sr := opts.SharedResourceInfo

	resolutionHours := 1.0
	if opts.ResolutionHours > 0.0 {
		resolutionHours = opts.ResolutionHours
	}

	if idleCoefficients == nil {
		idleCoefficients = make(map[string]float64)
	}

	// aggregations collects key-value pairs of resource group-to-aggregated data
	// e.g. namespace-to-data or label-value-to-data
	aggregations := make(map[string]*Aggregation)

	// sharedResourceCost is the running total cost of resources that should be reported
	// as shared across all other resources, rather than reported as a stand-alone category
	sharedResourceCost := 0.0

	for _, costDatum := range costData {
		idleCoefficient, ok := idleCoefficients[costDatum.ClusterID]
		if !ok {
			idleCoefficient = 1.0
		}
		if sr != nil && sr.ShareResources && sr.IsSharedResource(costDatum) {
			cpuv, ramv, gpuv, pvvs, netv := getPriceVectors(cp, costDatum, rate, discount, customDiscount, idleCoefficient)
			sharedResourceCost += totalVectors(cpuv)
			sharedResourceCost += totalVectors(ramv)
			sharedResourceCost += totalVectors(gpuv)
			sharedResourceCost += totalVectors(netv)
			for _, pv := range pvvs {
				sharedResourceCost += totalVectors(pv)
			}
		} else {
			if field == "cluster" {
				aggregateDatum(cp, aggregations, costDatum, field, subfields, rate, costDatum.ClusterID, discount, customDiscount, idleCoefficient, false)
			} else if field == "node" {
				aggregateDatum(cp, aggregations, costDatum, field, subfields, rate, costDatum.NodeName, discount, customDiscount, idleCoefficient, false)
			} else if field == "namespace" {
				aggregateDatum(cp, aggregations, costDatum, field, subfields, rate, costDatum.Namespace, discount, customDiscount, idleCoefficient, false)
			} else if field == "service" {
				if len(costDatum.Services) > 0 {
					aggregateDatum(cp, aggregations, costDatum, field, subfields, rate, costDatum.Namespace+"/"+costDatum.Services[0], discount, customDiscount, idleCoefficient, false)
				} else {
					aggregateDatum(cp, aggregations, costDatum, field, subfields, rate, UnallocatedSubfield, discount, customDiscount, idleCoefficient, false)
				}
			} else if field == "deployment" {
				if len(costDatum.Deployments) > 0 {
					aggregateDatum(cp, aggregations, costDatum, field, subfields, rate, costDatum.Namespace+"/"+costDatum.Deployments[0], discount, customDiscount, idleCoefficient, false)
				} else {
					aggregateDatum(cp, aggregations, costDatum, field, subfields, rate, UnallocatedSubfield, discount, customDiscount, idleCoefficient, false)
				}
			} else if field == "statefulset" {
				if len(costDatum.Statefulsets) > 0 {
					aggregateDatum(cp, aggregations, costDatum, field, subfields, rate, costDatum.Namespace+"/"+costDatum.Statefulsets[0], discount, customDiscount, idleCoefficient, false)
				} else {
					aggregateDatum(cp, aggregations, costDatum, field, subfields, rate, UnallocatedSubfield, discount, customDiscount, idleCoefficient, false)
				}
			} else if field == "daemonset" {
				if len(costDatum.Daemonsets) > 0 {
					aggregateDatum(cp, aggregations, costDatum, field, subfields, rate, costDatum.Namespace+"/"+costDatum.Daemonsets[0], discount, customDiscount, idleCoefficient, false)
				} else {
					aggregateDatum(cp, aggregations, costDatum, field, subfields, rate, UnallocatedSubfield, discount, customDiscount, idleCoefficient, false)
				}
			} else if field == "controller" {
				if controller, kind, hasController := costDatum.GetController(); hasController {
					key := fmt.Sprintf("%s/%s:%s", costDatum.Namespace, kind, controller)
					aggregateDatum(cp, aggregations, costDatum, field, subfields, rate, key, discount, customDiscount, idleCoefficient, false)
				} else {
					aggregateDatum(cp, aggregations, costDatum, field, subfields, rate, UnallocatedSubfield, discount, customDiscount, idleCoefficient, false)
				}
			} else if field == "label" {
				found := false
				if costDatum.Labels != nil {
					for _, sf := range subfields {
						if subfieldName, ok := costDatum.Labels[sf]; ok {
							aggregateDatum(cp, aggregations, costDatum, field, subfields, rate, subfieldName, discount, customDiscount, idleCoefficient, false)
							found = true
							break
						}
					}
				}
				if !found {
					aggregateDatum(cp, aggregations, costDatum, field, subfields, rate, UnallocatedSubfield, discount, customDiscount, idleCoefficient, false)
				}
			} else if field == "annotation" {
				found := false
				if costDatum.Annotations != nil {
					for _, sf := range subfields {
						if subfieldName, ok := costDatum.Annotations[sf]; ok {
							aggregateDatum(cp, aggregations, costDatum, field, subfields, rate, subfieldName, discount, customDiscount, idleCoefficient, false)
							found = true
							break
						}
					}
				}
				if !found {
					aggregateDatum(cp, aggregations, costDatum, field, subfields, rate, UnallocatedSubfield, discount, customDiscount, idleCoefficient, false)
				}
			} else if field == "pod" {
				aggregateDatum(cp, aggregations, costDatum, field, subfields, rate, costDatum.Namespace+"/"+costDatum.PodName, discount, customDiscount, idleCoefficient, false)
			} else if field == "container" {
				key := fmt.Sprintf("%s/%s/%s/%s", costDatum.ClusterID, costDatum.Namespace, costDatum.PodName, costDatum.Name)
				aggregateDatum(cp, aggregations, costDatum, field, subfields, rate, key, discount, customDiscount, idleCoefficient, true)
			}
		}
	}

	for key, agg := range aggregations {
		sharedCoefficient := 1 / float64(len(opts.FilteredEnvironments)+len(aggregations))

		agg.CPUCost = totalVectors(agg.CPUCostVector)
		agg.RAMCost = totalVectors(agg.RAMCostVector)
		agg.GPUCost = totalVectors(agg.GPUCostVector)
		agg.PVCost = totalVectors(agg.PVCostVector)
		agg.NetworkCost = totalVectors(agg.NetworkCostVector)
		if opts.SharedSplit == SplitTypeWeighted {
			d := opts.TotalContainerCost - sharedResourceCost
			if d == 0 {
				log.Warnf("Total container cost '%f' and shared resource cost '%f are the same'. Setting sharedCoefficient to 1", opts.TotalContainerCost, sharedResourceCost)
				sharedCoefficient = 1.0
			} else {
				sharedCoefficient = (agg.CPUCost + agg.RAMCost + agg.GPUCost + agg.PVCost + agg.NetworkCost) / d
			}
		}
		agg.SharedCost = sharedResourceCost * sharedCoefficient

		for _, v := range opts.SharedCosts {
			agg.SharedCost += v.Cost * sharedCoefficient
		}

		if rate != "" {
			rateCoeff := agg.RateCoefficient(rate, resolutionHours)
			agg.CPUCost *= rateCoeff
			agg.RAMCost *= rateCoeff
			agg.GPUCost *= rateCoeff
			agg.PVCost *= rateCoeff
			agg.NetworkCost *= rateCoeff
			agg.SharedCost *= rateCoeff
		}

		agg.TotalCost = agg.CPUCost + agg.RAMCost + agg.GPUCost + agg.PVCost + agg.NetworkCost + agg.SharedCost

		// Evicted and Completed Pods can still show up here, but have 0 cost.
		// Filter these by default. Any reason to keep them?
		if agg.TotalCost == 0 {
			delete(aggregations, key)
			continue
		}

		// CPU, RAM, and PV allocation are cumulative per-datum, whereas GPU is rate per-datum
		agg.CPUAllocationHourlyAverage = totalVectors(agg.CPUAllocationVectors) / agg.TotalHours(resolutionHours)
		agg.RAMAllocationHourlyAverage = totalVectors(agg.RAMAllocationVectors) / agg.TotalHours(resolutionHours)
		agg.GPUAllocationHourlyAverage = averageVectors(agg.GPUAllocationVectors)
		agg.PVAllocationHourlyAverage = totalVectors(agg.PVAllocationVectors) / agg.TotalHours(resolutionHours)

		// TODO niko/etl does this check out for GPU data? Do we need to rewrite GPU queries to be
		// cumulative?
		agg.CPUAllocationTotal = totalVectors(agg.CPUAllocationVectors)
		agg.GPUAllocationTotal = totalVectors(agg.GPUAllocationVectors)
		agg.PVAllocationTotal = totalVectors(agg.PVAllocationVectors)
		agg.RAMAllocationTotal = totalVectors(agg.RAMAllocationVectors)

		if includeEfficiency {
			// Default both RAM and CPU to 0% efficiency so that a 0-requested, 0-allocated, 0-used situation
			// returns 0% efficiency, which should be a red-flag.
			//
			// If non-zero numbers are available, then efficiency is defined as:
			//   idlePercentage =  (requested - used) / allocated
			//   efficiency = (1.0 - idlePercentage)
			//
			// It is possible to score > 100% efficiency, which is meant to be interpreted as a red flag.
			// It is not possible to score < 0% efficiency.

			agg.CPUEfficiency = 0.0
			CPUIdle := 0.0
			if agg.CPUAllocationHourlyAverage > 0.0 {
				avgCPURequested := averageVectors(agg.CPURequestedVectors)
				avgCPUUsed := averageVectors(agg.CPUUsedVectors)

				// Clamp averages, log range violations
				avgCPURequested, avgCPUUsed = clampAverage(avgCPURequested, avgCPUUsed, agg.CPUAllocationHourlyAverage, "CPU")

				CPUIdle = ((avgCPURequested - avgCPUUsed) / agg.CPUAllocationHourlyAverage)
				agg.CPUEfficiency = 1.0 - CPUIdle
			}

			agg.RAMEfficiency = 0.0
			RAMIdle := 0.0
			if agg.RAMAllocationHourlyAverage > 0.0 {
				avgRAMRequested := averageVectors(agg.RAMRequestedVectors)
				avgRAMUsed := averageVectors(agg.RAMUsedVectors)

				// Clamp averages, log range violations
				avgRAMRequested, avgRAMUsed = clampAverage(avgRAMRequested, avgRAMUsed, agg.RAMAllocationHourlyAverage, "RAM")

				RAMIdle = ((avgRAMRequested - avgRAMUsed) / agg.RAMAllocationHourlyAverage)
				agg.RAMEfficiency = 1.0 - RAMIdle
			}

			// Score total efficiency by the sum of CPU and RAM efficiency, weighted by their
			// respective total costs.
			agg.Efficiency = 0.0
			if (agg.CPUCost + agg.RAMCost) > 0 {
				agg.Efficiency = ((agg.CPUCost * agg.CPUEfficiency) + (agg.RAMCost * agg.RAMEfficiency)) / (agg.CPUCost + agg.RAMCost)
			}
		}

		// convert RAM from bytes to GiB
		agg.RAMAllocationHourlyAverage = agg.RAMAllocationHourlyAverage / 1024 / 1024 / 1024
		// convert storage from bytes to GiB
		agg.PVAllocationHourlyAverage = agg.PVAllocationHourlyAverage / 1024 / 1024 / 1024

		// remove time series data if it is not explicitly requested
		if !includeTimeSeries {
			agg.CPUCostVector = nil
			agg.RAMCostVector = nil
			agg.GPUCostVector = nil
			agg.PVCostVector = nil
			agg.NetworkCostVector = nil
			agg.TotalCostVector = nil
		} else { // otherwise compute a totalcostvector
			v1 := addVectors(agg.CPUCostVector, agg.RAMCostVector)
			v2 := addVectors(v1, agg.GPUCostVector)
			v3 := addVectors(v2, agg.PVCostVector)
			v4 := addVectors(v3, agg.NetworkCostVector)
			agg.TotalCostVector = v4
		}
		// Typesafety checks
		if math.IsNaN(agg.CPUAllocationHourlyAverage) || math.IsInf(agg.CPUAllocationHourlyAverage, 0) {
			log.Warnf("CPUAllocationHourlyAverage is %f for '%s: %s/%s'", agg.CPUAllocationHourlyAverage, agg.Cluster, agg.Aggregator, agg.Environment)
			agg.CPUAllocationHourlyAverage = 0
		}
		if math.IsNaN(agg.CPUCost) || math.IsInf(agg.CPUCost, 0) {
			log.Warnf("CPUCost is %f for '%s: %s/%s'", agg.CPUCost, agg.Cluster, agg.Aggregator, agg.Environment)
			agg.CPUCost = 0
		}
		if math.IsNaN(agg.CPUEfficiency) || math.IsInf(agg.CPUEfficiency, 0) {
			log.Warnf("CPUEfficiency is %f for '%s: %s/%s'", agg.CPUEfficiency, agg.Cluster, agg.Aggregator, agg.Environment)
			agg.CPUEfficiency = 0
		}
		if math.IsNaN(agg.Efficiency) || math.IsInf(agg.Efficiency, 0) {
			log.Warnf("Efficiency is %f for '%s: %s/%s'", agg.Efficiency, agg.Cluster, agg.Aggregator, agg.Environment)
			agg.Efficiency = 0
		}
		if math.IsNaN(agg.GPUAllocationHourlyAverage) || math.IsInf(agg.GPUAllocationHourlyAverage, 0) {
			log.Warnf("GPUAllocationHourlyAverage is %f for '%s: %s/%s'", agg.GPUAllocationHourlyAverage, agg.Cluster, agg.Aggregator, agg.Environment)
			agg.GPUAllocationHourlyAverage = 0
		}
		if math.IsNaN(agg.GPUCost) || math.IsInf(agg.GPUCost, 0) {
			log.Warnf("GPUCost is %f for '%s: %s/%s'", agg.GPUCost, agg.Cluster, agg.Aggregator, agg.Environment)
			agg.GPUCost = 0
		}
		if math.IsNaN(agg.RAMAllocationHourlyAverage) || math.IsInf(agg.RAMAllocationHourlyAverage, 0) {
			log.Warnf("RAMAllocationHourlyAverage is %f for '%s: %s/%s'", agg.RAMAllocationHourlyAverage, agg.Cluster, agg.Aggregator, agg.Environment)
			agg.RAMAllocationHourlyAverage = 0
		}
		if math.IsNaN(agg.RAMCost) || math.IsInf(agg.RAMCost, 0) {
			log.Warnf("RAMCost is %f for '%s: %s/%s'", agg.RAMCost, agg.Cluster, agg.Aggregator, agg.Environment)
			agg.RAMCost = 0
		}
		if math.IsNaN(agg.RAMEfficiency) || math.IsInf(agg.RAMEfficiency, 0) {
			log.Warnf("RAMEfficiency is %f for '%s: %s/%s'", agg.RAMEfficiency, agg.Cluster, agg.Aggregator, agg.Environment)
			agg.RAMEfficiency = 0
		}
		if math.IsNaN(agg.PVAllocationHourlyAverage) || math.IsInf(agg.PVAllocationHourlyAverage, 0) {
			log.Warnf("PVAllocationHourlyAverage is %f for '%s: %s/%s'", agg.PVAllocationHourlyAverage, agg.Cluster, agg.Aggregator, agg.Environment)
			agg.PVAllocationHourlyAverage = 0
		}
		if math.IsNaN(agg.PVCost) || math.IsInf(agg.PVCost, 0) {
			log.Warnf("PVCost is %f for '%s: %s/%s'", agg.PVCost, agg.Cluster, agg.Aggregator, agg.Environment)
			agg.PVCost = 0
		}
		if math.IsNaN(agg.NetworkCost) || math.IsInf(agg.NetworkCost, 0) {
			log.Warnf("NetworkCost is %f for '%s: %s/%s'", agg.NetworkCost, agg.Cluster, agg.Aggregator, agg.Environment)
			agg.NetworkCost = 0
		}
		if math.IsNaN(agg.SharedCost) || math.IsInf(agg.SharedCost, 0) {
			log.Warnf("SharedCost is %f for '%s: %s/%s'", agg.SharedCost, agg.Cluster, agg.Aggregator, agg.Environment)
			agg.SharedCost = 0
		}
		if math.IsNaN(agg.TotalCost) || math.IsInf(agg.TotalCost, 0) {
			log.Warnf("TotalCost is %f for '%s: %s/%s'", agg.TotalCost, agg.Cluster, agg.Aggregator, agg.Environment)
			agg.TotalCost = 0
		}
	}

	return aggregations
}

func aggregateDatum(cp models.Provider, aggregations map[string]*Aggregation, costDatum *CostData, field string, subfields []string, rate string, key string, discount float64, customDiscount float64, idleCoefficient float64, includeProperties bool) {
	// add new entry to aggregation results if a new key is encountered
	if _, ok := aggregations[key]; !ok {
		agg := &Aggregation{
			Aggregator:  field,
			Environment: key,
		}
		if len(subfields) > 0 {
			agg.Subfields = subfields
		}
		if includeProperties {
			props := &opencost.AllocationProperties{}
			props.Cluster = costDatum.ClusterID
			props.Node = costDatum.NodeName
			if controller, kind, hasController := costDatum.GetController(); hasController {
				props.Controller = controller
				props.ControllerKind = kind
			}
			props.Labels = costDatum.Labels
			props.Annotations = costDatum.Annotations
			props.Namespace = costDatum.Namespace
			props.Pod = costDatum.PodName
			props.Services = costDatum.Services
			props.Container = costDatum.Name
			agg.Properties = props
		}

		aggregations[key] = agg
	}

	mergeVectors(cp, costDatum, aggregations[key], rate, discount, customDiscount, idleCoefficient)
}

func mergeVectors(cp models.Provider, costDatum *CostData, aggregation *Aggregation, rate string, discount float64, customDiscount float64, idleCoefficient float64) {
	aggregation.CPUAllocationVectors = addVectors(costDatum.CPUAllocation, aggregation.CPUAllocationVectors)
	aggregation.CPURequestedVectors = addVectors(costDatum.CPUReq, aggregation.CPURequestedVectors)
	aggregation.CPUUsedVectors = addVectors(costDatum.CPUUsed, aggregation.CPUUsedVectors)

	aggregation.RAMAllocationVectors = addVectors(costDatum.RAMAllocation, aggregation.RAMAllocationVectors)
	aggregation.RAMRequestedVectors = addVectors(costDatum.RAMReq, aggregation.RAMRequestedVectors)
	aggregation.RAMUsedVectors = addVectors(costDatum.RAMUsed, aggregation.RAMUsedVectors)

	aggregation.GPUAllocationVectors = addVectors(costDatum.GPUReq, aggregation.GPUAllocationVectors)

	for _, pvcd := range costDatum.PVCData {
		aggregation.PVAllocationVectors = addVectors(pvcd.Values, aggregation.PVAllocationVectors)
	}

	cpuv, ramv, gpuv, pvvs, netv := getPriceVectors(cp, costDatum, rate, discount, customDiscount, idleCoefficient)
	aggregation.CPUCostVector = addVectors(cpuv, aggregation.CPUCostVector)
	aggregation.RAMCostVector = addVectors(ramv, aggregation.RAMCostVector)
	aggregation.GPUCostVector = addVectors(gpuv, aggregation.GPUCostVector)
	aggregation.NetworkCostVector = addVectors(netv, aggregation.NetworkCostVector)
	for _, vectorList := range pvvs {
		aggregation.PVCostVector = addVectors(aggregation.PVCostVector, vectorList)
	}
}

// Returns the blended discounts applied to the node as a result of global discounts and reserved instance
// discounts
func getDiscounts(costDatum *CostData, cpuCost float64, ramCost float64, discount float64) (float64, float64) {
	if costDatum.NodeData == nil {
		return discount, discount
	}
	if costDatum.NodeData.IsSpot() {
		return 0, 0
	}

	reserved := costDatum.NodeData.Reserved

	// blended discounts
	blendedCPUDiscount := discount
	blendedRAMDiscount := discount

	if reserved != nil && reserved.CPUCost > 0 && reserved.RAMCost > 0 {
		reservedCPUDiscount := 0.0
		if cpuCost == 0 {
			log.Warnf("No cpu cost found for cluster '%s' node '%s'", costDatum.ClusterID, costDatum.NodeName)
		} else {
			reservedCPUDiscount = 1.0 - (reserved.CPUCost / cpuCost)
		}
		reservedRAMDiscount := 0.0
		if ramCost == 0 {
			log.Warnf("No ram cost found for cluster '%s' node '%s'", costDatum.ClusterID, costDatum.NodeName)
		} else {
			reservedRAMDiscount = 1.0 - (reserved.RAMCost / ramCost)
		}

		// AWS passes the # of reserved CPU and RAM as -1 to represent "All"
		if reserved.ReservedCPU < 0 && reserved.ReservedRAM < 0 {
			blendedCPUDiscount = reservedCPUDiscount
			blendedRAMDiscount = reservedRAMDiscount
		} else {
			nodeCPU, ierr := strconv.ParseInt(costDatum.NodeData.VCPU, 10, 64)
			nodeRAM, ferr := strconv.ParseFloat(costDatum.NodeData.RAMBytes, 64)
			if ierr == nil && ferr == nil {
				nodeRAMGB := nodeRAM / 1024 / 1024 / 1024
				reservedRAMGB := float64(reserved.ReservedRAM) / 1024 / 1024 / 1024
				nonReservedCPU := nodeCPU - reserved.ReservedCPU
				nonReservedRAM := nodeRAMGB - reservedRAMGB

				if nonReservedCPU == 0 {
					blendedCPUDiscount = reservedCPUDiscount
				} else {
					if nodeCPU == 0 {
						log.Warnf("No ram found for cluster '%s' node '%s'", costDatum.ClusterID, costDatum.NodeName)
					} else {
						blendedCPUDiscount = (float64(reserved.ReservedCPU) * reservedCPUDiscount) + (float64(nonReservedCPU)*discount)/float64(nodeCPU)
					}
				}

				if nonReservedRAM == 0 {
					blendedRAMDiscount = reservedRAMDiscount
				} else {
					if nodeRAMGB == 0 {
						log.Warnf("No ram found for cluster '%s' node '%s'", costDatum.ClusterID, costDatum.NodeName)
					} else {
						blendedRAMDiscount = (reservedRAMGB * reservedRAMDiscount) + (nonReservedRAM*discount)/nodeRAMGB
					}
				}
			}
		}
	}

	return blendedCPUDiscount, blendedRAMDiscount
}

func parseVectorPricing(cfg *models.CustomPricing, costDatum *CostData, cpuCostStr, ramCostStr, gpuCostStr, pvCostStr string) (float64, float64, float64, float64, bool) {
	usesCustom := false
	cpuCost, err := strconv.ParseFloat(cpuCostStr, 64)
	if err != nil || math.IsNaN(cpuCost) || math.IsInf(cpuCost, 0) || cpuCost == 0 {
		cpuCost, err = strconv.ParseFloat(cfg.CPU, 64)
		usesCustom = true
		if err != nil || math.IsNaN(cpuCost) || math.IsInf(cpuCost, 0) {
			cpuCost = 0
		}
	}
	ramCost, err := strconv.ParseFloat(ramCostStr, 64)
	if err != nil || math.IsNaN(ramCost) || math.IsInf(ramCost, 0) || ramCost == 0 {
		ramCost, err = strconv.ParseFloat(cfg.RAM, 64)
		usesCustom = true
		if err != nil || math.IsNaN(ramCost) || math.IsInf(ramCost, 0) {
			ramCost = 0
		}
	}
	gpuCost, err := strconv.ParseFloat(gpuCostStr, 64)
	if err != nil || math.IsNaN(gpuCost) || math.IsInf(gpuCost, 0) {
		gpuCost, err = strconv.ParseFloat(cfg.GPU, 64)
		if err != nil || math.IsNaN(gpuCost) || math.IsInf(gpuCost, 0) {
			gpuCost = 0
		}
	}
	pvCost, err := strconv.ParseFloat(pvCostStr, 64)
	if err != nil || math.IsNaN(cpuCost) || math.IsInf(cpuCost, 0) {
		pvCost, err = strconv.ParseFloat(cfg.Storage, 64)
		if err != nil || math.IsNaN(pvCost) || math.IsInf(pvCost, 0) {
			pvCost = 0
		}
	}
	return cpuCost, ramCost, gpuCost, pvCost, usesCustom
}

func getPriceVectors(cp models.Provider, costDatum *CostData, rate string, discount float64, customDiscount float64, idleCoefficient float64) ([]*util.Vector, []*util.Vector, []*util.Vector, [][]*util.Vector, []*util.Vector) {

	var cpuCost float64
	var ramCost float64
	var gpuCost float64
	var pvCost float64
	var usesCustom bool

	// If custom pricing is enabled and can be retrieved, replace
	// default cost values with custom values
	customPricing, err := cp.GetConfig()
	if err != nil {
		log.Errorf("failed to load custom pricing: %s", err)
	}
	if provider.CustomPricesEnabled(cp) && err == nil {
		var cpuCostStr string
		var ramCostStr string
		var gpuCostStr string
		var pvCostStr string
		if costDatum.NodeData.IsSpot() {
			cpuCostStr = customPricing.SpotCPU
			ramCostStr = customPricing.SpotRAM
			gpuCostStr = customPricing.SpotGPU
		} else {
			cpuCostStr = customPricing.CPU
			ramCostStr = customPricing.RAM
			gpuCostStr = customPricing.GPU
		}
		pvCostStr = customPricing.Storage
		cpuCost, ramCost, gpuCost, pvCost, usesCustom = parseVectorPricing(customPricing, costDatum, cpuCostStr, ramCostStr, gpuCostStr, pvCostStr)
	} else if costDatum.NodeData == nil && err == nil {
		cpuCostStr := customPricing.CPU
		ramCostStr := customPricing.RAM
		gpuCostStr := customPricing.GPU
		pvCostStr := customPricing.Storage
		cpuCost, ramCost, gpuCost, pvCost, usesCustom = parseVectorPricing(customPricing, costDatum, cpuCostStr, ramCostStr, gpuCostStr, pvCostStr)
	} else {
		cpuCostStr := costDatum.NodeData.VCPUCost
		ramCostStr := costDatum.NodeData.RAMCost
		gpuCostStr := costDatum.NodeData.GPUCost
		pvCostStr := costDatum.NodeData.StorageCost
		cpuCost, ramCost, gpuCost, pvCost, usesCustom = parseVectorPricing(customPricing, costDatum, cpuCostStr, ramCostStr, gpuCostStr, pvCostStr)
	}

	if usesCustom {
		log.DedupedWarningf(5, "No pricing data found for node `%s` , using custom pricing", costDatum.NodeName)
	}

	cpuDiscount, ramDiscount := getDiscounts(costDatum, cpuCost, ramCost, discount)

	log.Debugf("Node Name: %s", costDatum.NodeName)
	log.Debugf("Blended CPU Discount: %f", cpuDiscount)
	log.Debugf("Blended RAM Discount: %f", ramDiscount)

	// TODO should we try to apply the rate coefficient here or leave it as a totals-only metric?
	rateCoeff := 1.0

	if idleCoefficient == 0 {
		idleCoefficient = 1.0
	}

	cpuv := make([]*util.Vector, 0, len(costDatum.CPUAllocation))
	for _, val := range costDatum.CPUAllocation {
		cpuv = append(cpuv, &util.Vector{
			Timestamp: math.Round(val.Timestamp/10) * 10,
			Value:     (val.Value * cpuCost * (1 - cpuDiscount) * (1 - customDiscount) / idleCoefficient) * rateCoeff,
		})
	}

	ramv := make([]*util.Vector, 0, len(costDatum.RAMAllocation))
	for _, val := range costDatum.RAMAllocation {
		ramv = append(ramv, &util.Vector{
			Timestamp: math.Round(val.Timestamp/10) * 10,
			Value:     ((val.Value / 1024 / 1024 / 1024) * ramCost * (1 - ramDiscount) * (1 - customDiscount) / idleCoefficient) * rateCoeff,
		})
	}

	gpuv := make([]*util.Vector, 0, len(costDatum.GPUReq))
	for _, val := range costDatum.GPUReq {
		gpuv = append(gpuv, &util.Vector{
			Timestamp: math.Round(val.Timestamp/10) * 10,
			Value:     (val.Value * gpuCost * (1 - discount) * (1 - customDiscount) / idleCoefficient) * rateCoeff,
		})
	}

	pvvs := make([][]*util.Vector, 0, len(costDatum.PVCData))
	for _, pvcData := range costDatum.PVCData {
		pvv := make([]*util.Vector, 0, len(pvcData.Values))
		if pvcData.Volume != nil {
			cost, _ := strconv.ParseFloat(pvcData.Volume.Cost, 64)

			// override with custom pricing if enabled
			if provider.CustomPricesEnabled(cp) {
				cost = pvCost
			}

			for _, val := range pvcData.Values {
				pvv = append(pvv, &util.Vector{
					Timestamp: math.Round(val.Timestamp/10) * 10,
					Value:     ((val.Value / 1024 / 1024 / 1024) * cost * (1 - customDiscount) / idleCoefficient) * rateCoeff,
				})
			}
			pvvs = append(pvvs, pvv)
		}
	}

	netv := make([]*util.Vector, 0, len(costDatum.NetworkData))
	for _, val := range costDatum.NetworkData {
		netv = append(netv, &util.Vector{
			Timestamp: math.Round(val.Timestamp/10) * 10,
			Value:     val.Value,
		})
	}

	return cpuv, ramv, gpuv, pvvs, netv
}

func averageVectors(vectors []*util.Vector) float64 {
	if len(vectors) == 0 {
		return 0.0
	}
	return totalVectors(vectors) / float64(len(vectors))
}

func totalVectors(vectors []*util.Vector) float64 {
	total := 0.0
	for _, vector := range vectors {
		total += vector.Value
	}
	return total
}

// addVectors adds two slices of Vectors. Vector timestamps are rounded to the
// nearest ten seconds to allow matching of Vectors within a delta allowance.
// Matching Vectors are summed, while unmatched Vectors are passed through.
// e.g. [(t=1, 1), (t=2, 2)] + [(t=2, 2), (t=3, 3)] = [(t=1, 1), (t=2, 4), (t=3, 3)]
func addVectors(xvs []*util.Vector, yvs []*util.Vector) []*util.Vector {
	sumOp := func(result *util.Vector, x *float64, y *float64) bool {
		if x != nil && y != nil {
			result.Value = *x + *y
		} else if y != nil {
			result.Value = *y
		} else if x != nil {
			result.Value = *x
		}

		return true
	}

	return util.ApplyVectorOp(xvs, yvs, sumOp)
}

// minCostDataLength sets the minimum number of time series data required to
// cache both raw and aggregated cost data
const minCostDataLength = 2

// EmptyDataError describes an error caused by empty cost data for some
// defined interval
type EmptyDataError struct {
	err    error
	window opencost.Window
}

// Error implements the error interface
func (ede *EmptyDataError) Error() string {
	err := fmt.Sprintf("empty data for range: %s", ede.window)
	if ede.err != nil {
		err += fmt.Sprintf(": %s", ede.err)
	}
	return err
}

func costDataTimeSeriesLength(costData map[string]*CostData) int {
	l := 0
	for _, cd := range costData {
		if l < len(cd.RAMAllocation) {
			l = len(cd.RAMAllocation)
		}
		if l < len(cd.CPUAllocation) {
			l = len(cd.CPUAllocation)
		}
	}
	return l
}

// ScaleHourlyCostData converts per-hour cost data to per-resolution data. If the target resolution is higher (i.e. < 1.0h)
// then we can do simple multiplication by the fraction-of-an-hour and retain accuracy. If the target resolution is
// lower (i.e. > 1.0h) then we sum groups of hourly data by resolution to maintain fidelity.
// e.g. (100 hours of per-hour hourly data, resolutionHours=10) => 10 data points, grouped and summed by 10-hour window
// e.g. (20 minutes of per-minute hourly data, resolutionHours=1/60) => 20 data points, scaled down by a factor of 60
func ScaleHourlyCostData(data map[string]*CostData, resolutionHours float64) map[string]*CostData {
	scaled := map[string]*CostData{}

	for key, datum := range data {
		datum.RAMReq = scaleVectorSeries(datum.RAMReq, resolutionHours)
		datum.RAMUsed = scaleVectorSeries(datum.RAMUsed, resolutionHours)
		datum.RAMAllocation = scaleVectorSeries(datum.RAMAllocation, resolutionHours)
		datum.CPUReq = scaleVectorSeries(datum.CPUReq, resolutionHours)
		datum.CPUUsed = scaleVectorSeries(datum.CPUUsed, resolutionHours)
		datum.CPUAllocation = scaleVectorSeries(datum.CPUAllocation, resolutionHours)
		datum.GPUReq = scaleVectorSeries(datum.GPUReq, resolutionHours)
		datum.NetworkData = scaleVectorSeries(datum.NetworkData, resolutionHours)

		for _, pvcDatum := range datum.PVCData {
			pvcDatum.Values = scaleVectorSeries(pvcDatum.Values, resolutionHours)
		}

		scaled[key] = datum
	}

	return scaled
}

func scaleVectorSeries(vs []*util.Vector, resolutionHours float64) []*util.Vector {
	// if scaling to a lower resolution, compress the hourly data for maximum accuracy
	if resolutionHours > 1.0 {
		return compressVectorSeries(vs, resolutionHours)
	}

	// if scaling to a higher resolution, simply scale each value down by the fraction of an hour
	for _, v := range vs {
		v.Value *= resolutionHours
	}
	return vs
}

func compressVectorSeries(vs []*util.Vector, resolutionHours float64) []*util.Vector {
	if len(vs) == 0 {
		return vs
	}

	compressed := []*util.Vector{}

	threshold := float64(60 * 60 * resolutionHours)
	var acc *util.Vector

	for i, v := range vs {
		if acc == nil {
			// start a new accumulation from current datum
			acc = &util.Vector{
				Value:     vs[i].Value,
				Timestamp: vs[i].Timestamp,
			}
			continue
		}
		if v.Timestamp-acc.Timestamp < threshold {
			// v should be accumulated in current datum
			acc.Value += v.Value
		} else {
			// v falls outside current datum's threshold; append and start a new one
			compressed = append(compressed, acc)
			acc = &util.Vector{
				Value:     vs[i].Value,
				Timestamp: vs[i].Timestamp,
			}
		}
	}
	// append any remaining, incomplete accumulation
	if acc != nil {
		compressed = append(compressed, acc)
	}

	return compressed
}

type AggregateQueryOpts struct {
	Rate                           string
	Filters                        map[string]string
	SharedResources                *SharedResourceInfo
	ShareSplit                     string
	AllocateIdle                   bool
	IncludeTimeSeries              bool
	IncludeEfficiency              bool
	DisableAggregateCostModelCache bool
	ClearCache                     bool
	NoCache                        bool
	NoExpireCache                  bool
	RemoteEnabled                  bool
	DisableSharedOverhead          bool
	UseETLAdapter                  bool
}

func DefaultAggregateQueryOpts() *AggregateQueryOpts {
	return &AggregateQueryOpts{
		Rate:                           "",
		Filters:                        map[string]string{},
		SharedResources:                nil,
		ShareSplit:                     SplitTypeWeighted,
		AllocateIdle:                   false,
		IncludeTimeSeries:              true,
		IncludeEfficiency:              true,
		DisableAggregateCostModelCache: env.IsAggregateCostModelCacheDisabled(),
		ClearCache:                     false,
		NoCache:                        false,
		NoExpireCache:                  false,
		RemoteEnabled:                  env.IsRemoteEnabled(),
		DisableSharedOverhead:          false,
		UseETLAdapter:                  false,
	}
}

// ComputeAggregateCostModel computes cost data for the given window, then aggregates it by the given fields.
// Data is cached on two levels: the aggregation is cached as well as the underlying cost data.
func (a *Accesses) ComputeAggregateCostModel(promClient prometheusClient.Client, window opencost.Window, field string, subfields []string, opts *AggregateQueryOpts) (map[string]*Aggregation, string, error) {
	// Window is the range of the query, i.e. (start, end)
	// It must be closed, i.e. neither start nor end can be nil
	if window.IsOpen() {
		return nil, "", fmt.Errorf("illegal window: %s", window)
	}

	// Resolution is the duration of each datum in the cost model range query,
	// which corresponds to both the step size given to Prometheus query_range
	// and to the window passed to the range queries.
	// i.e. by default, we support 1h resolution for queries of windows defined
	// in terms of days or integer multiples of hours (e.g. 1d, 12h)
	resolution := time.Hour

	// Determine resolution by size of duration and divisibility of window.
	// By default, resolution is 1hr. If the window is smaller than 1hr, then
	// resolution goes down to 1m. If the window is not a multiple of 1hr, then
	// resolution goes down to 1m. If the window is greater than 1d, then
	// resolution gets scaled up to improve performance by reducing the amount
	// of data being computed.
	durMins := int64(math.Trunc(window.Minutes()))
	if durMins < 24*60 { // less than 1d
		// TODO should we have additional options for going by
		// e.g. 30m? 10m? 5m?
		if durMins%60 != 0 || durMins < 3*60 { // not divisible by 1h or less than 3h
			resolution = time.Minute
		}
	} else { // greater than 1d
		if durMins >= 7*24*60 { // greater than (or equal to) 7 days
			resolution = 24.0 * time.Hour
		} else if durMins >= 2*24*60 { // greater than (or equal to) 2 days
			resolution = 2.0 * time.Hour
		}
	}

	// Parse options
	if opts == nil {
		opts = DefaultAggregateQueryOpts()
	}
	rate := opts.Rate
	filters := opts.Filters
	sri := opts.SharedResources
	shared := opts.ShareSplit
	allocateIdle := opts.AllocateIdle
	includeTimeSeries := opts.IncludeTimeSeries
	includeEfficiency := opts.IncludeEfficiency
	disableAggregateCostModelCache := opts.DisableAggregateCostModelCache
	clearCache := opts.ClearCache
	noCache := opts.NoCache
	noExpireCache := opts.NoExpireCache
	remoteEnabled := opts.RemoteEnabled
	disableSharedOverhead := opts.DisableSharedOverhead

	// retainFuncs override filterFuncs. Make sure shared resources do not
	// get filtered out.
	retainFuncs := []FilterFunc{}
	retainFuncs = append(retainFuncs, func(cd *CostData) (bool, string) {
		if sri != nil {
			return sri.IsSharedResource(cd), ""
		}
		return false, ""
	})

	// Parse cost data filters into FilterFuncs
	filterFuncs := []FilterFunc{}

	aggregateEnvironment := func(costDatum *CostData) string {
		if field == "cluster" {
			return costDatum.ClusterID
		} else if field == "node" {
			return costDatum.NodeName
		} else if field == "namespace" {
			return costDatum.Namespace
		} else if field == "service" {
			if len(costDatum.Services) > 0 {
				return costDatum.Namespace + "/" + costDatum.Services[0]
			}
		} else if field == "deployment" {
			if len(costDatum.Deployments) > 0 {
				return costDatum.Namespace + "/" + costDatum.Deployments[0]
			}
		} else if field == "daemonset" {
			if len(costDatum.Daemonsets) > 0 {
				return costDatum.Namespace + "/" + costDatum.Daemonsets[0]
			}
		} else if field == "statefulset" {
			if len(costDatum.Statefulsets) > 0 {
				return costDatum.Namespace + "/" + costDatum.Statefulsets[0]
			}
		} else if field == "label" {
			if costDatum.Labels != nil {
				for _, sf := range subfields {
					if subfieldName, ok := costDatum.Labels[sf]; ok {
						return fmt.Sprintf("%s=%s", sf, subfieldName)
					}
				}
			}
		} else if field == "annotation" {
			if costDatum.Annotations != nil {
				for _, sf := range subfields {
					if subfieldName, ok := costDatum.Annotations[sf]; ok {
						return fmt.Sprintf("%s=%s", sf, subfieldName)
					}
				}
			}
		} else if field == "pod" {
			return costDatum.Namespace + "/" + costDatum.PodName
		} else if field == "container" {
			return costDatum.Namespace + "/" + costDatum.PodName + "/" + costDatum.Name
		}
		return ""
	}

	if filters["podprefix"] != "" {
		pps := []string{}
		for _, fp := range strings.Split(filters["podprefix"], ",") {
			if fp != "" {
				cleanedFilter := strings.TrimSpace(fp)
				pps = append(pps, cleanedFilter)
			}
		}
		filterFuncs = append(filterFuncs, func(cd *CostData) (bool, string) {
			aggEnv := aggregateEnvironment(cd)
			for _, pp := range pps {
				cleanedFilter := strings.TrimSpace(pp)
				if strings.HasPrefix(cd.PodName, cleanedFilter) {
					return true, aggEnv
				}
			}
			return false, aggEnv
		})
	}

	if filters["namespace"] != "" {
		// namespaces may be comma-separated, e.g. kubecost,default
		// multiple namespaces are evaluated as an OR relationship
		nss := strings.Split(filters["namespace"], ",")
		filterFuncs = append(filterFuncs, func(cd *CostData) (bool, string) {
			aggEnv := aggregateEnvironment(cd)
			for _, ns := range nss {
				nsTrim := strings.TrimSpace(ns)
				if cd.Namespace == nsTrim {
					return true, aggEnv
				} else if strings.HasSuffix(nsTrim, "*") { // trigger wildcard prefix filtering
					nsTrimAsterisk := strings.TrimSuffix(nsTrim, "*")
					if strings.HasPrefix(cd.Namespace, nsTrimAsterisk) {
						return true, aggEnv
					}
				}
			}
			return false, aggEnv
		})
	}

	if filters["node"] != "" {
		// nodes may be comma-separated, e.g. aws-node-1,aws-node-2
		// multiple nodes are evaluated as an OR relationship
		nodes := strings.Split(filters["node"], ",")
		filterFuncs = append(filterFuncs, func(cd *CostData) (bool, string) {
			aggEnv := aggregateEnvironment(cd)
			for _, node := range nodes {
				nodeTrim := strings.TrimSpace(node)
				if cd.NodeName == nodeTrim {
					return true, aggEnv
				} else if strings.HasSuffix(nodeTrim, "*") { // trigger wildcard prefix filtering
					nodeTrimAsterisk := strings.TrimSuffix(nodeTrim, "*")
					if strings.HasPrefix(cd.NodeName, nodeTrimAsterisk) {
						return true, aggEnv
					}
				}
			}
			return false, aggEnv
		})
	}

	if filters["cluster"] != "" {
		// clusters may be comma-separated, e.g. cluster-one,cluster-two
		// multiple clusters are evaluated as an OR relationship
		cs := strings.Split(filters["cluster"], ",")
		filterFuncs = append(filterFuncs, func(cd *CostData) (bool, string) {
			aggEnv := aggregateEnvironment(cd)
			for _, c := range cs {
				cTrim := strings.TrimSpace(c)
				id, name := cd.ClusterID, cd.ClusterName
				if id == cTrim || name == cTrim {
					return true, aggEnv
				} else if strings.HasSuffix(cTrim, "*") { // trigger wildcard prefix filtering
					cTrimAsterisk := strings.TrimSuffix(cTrim, "*")
					if strings.HasPrefix(id, cTrimAsterisk) || strings.HasPrefix(name, cTrimAsterisk) {
						return true, aggEnv
					}
				}
			}
			return false, aggEnv
		})
	}

	if filters["labels"] != "" {
		// labels are expected to be comma-separated and to take the form key=value
		// e.g. app=cost-analyzer,app.kubernetes.io/instance=kubecost
		// each different label will be applied as an AND
		// multiple values for a single label will be evaluated as an OR
		labelValues := map[string][]string{}
		ls := strings.Split(filters["labels"], ",")
		for _, l := range ls {
			lTrim := strings.TrimSpace(l)
			label := strings.Split(lTrim, "=")
			if len(label) == 2 {
				ln := promutil.SanitizeLabelName(strings.TrimSpace(label[0]))
				lv := strings.TrimSpace(label[1])
				labelValues[ln] = append(labelValues[ln], lv)
			} else {
				// label is not of the form name=value, so log it and move on
				log.Warnf("ComputeAggregateCostModel: skipping illegal label filter: %s", l)
			}
		}

		// Generate FilterFunc for each set of label filters by invoking a function instead of accessing
		// values by closure to prevent reference-type looping bug.
		// (see https://github.com/golang/go/wiki/CommonMistakes#using-reference-to-loop-iterator-variable)
		for label, values := range labelValues {
			ff := (func(l string, vs []string) FilterFunc {
				return func(cd *CostData) (bool, string) {
					ae := aggregateEnvironment(cd)
					for _, v := range vs {
						if v == "__unallocated__" { // Special case. __unallocated__ means return all pods without the attached label
							if _, ok := cd.Labels[l]; !ok {
								return true, ae
							}
						}
						if cd.Labels[l] == v {
							return true, ae
						} else if strings.HasSuffix(v, "*") { // trigger wildcard prefix filtering
							vTrim := strings.TrimSuffix(v, "*")
							if strings.HasPrefix(cd.Labels[l], vTrim) {
								return true, ae
							}
						}
					}
					return false, ae
				}
			})(label, values)
			filterFuncs = append(filterFuncs, ff)
		}
	}

	if filters["annotations"] != "" {
		// annotations are expected to be comma-separated and to take the form key=value
		// e.g. app=cost-analyzer,app.kubernetes.io/instance=kubecost
		// each different annotation will be applied as an AND
		// multiple values for a single annotation will be evaluated as an OR
		annotationValues := map[string][]string{}
		as := strings.Split(filters["annotations"], ",")
		for _, annot := range as {
			aTrim := strings.TrimSpace(annot)
			annotation := strings.Split(aTrim, "=")
			if len(annotation) == 2 {
				an := promutil.SanitizeLabelName(strings.TrimSpace(annotation[0]))
				av := strings.TrimSpace(annotation[1])
				annotationValues[an] = append(annotationValues[an], av)
			} else {
				// annotation is not of the form name=value, so log it and move on
				log.Warnf("ComputeAggregateCostModel: skipping illegal annotation filter: %s", annot)
			}
		}

		// Generate FilterFunc for each set of annotation filters by invoking a function instead of accessing
		// values by closure to prevent reference-type looping bug.
		// (see https://github.com/golang/go/wiki/CommonMistakes#using-reference-to-loop-iterator-variable)
		for annotation, values := range annotationValues {
			ff := (func(l string, vs []string) FilterFunc {
				return func(cd *CostData) (bool, string) {
					ae := aggregateEnvironment(cd)
					for _, v := range vs {
						if v == "__unallocated__" { // Special case. __unallocated__ means return all pods without the attached label
							if _, ok := cd.Annotations[l]; !ok {
								return true, ae
							}
						}
						if cd.Annotations[l] == v {
							return true, ae
						} else if strings.HasSuffix(v, "*") { // trigger wildcard prefix filtering
							vTrim := strings.TrimSuffix(v, "*")
							if strings.HasPrefix(cd.Annotations[l], vTrim) {
								return true, ae
							}
						}
					}
					return false, ae
				}
			})(annotation, values)
			filterFuncs = append(filterFuncs, ff)
		}
	}

	// clear cache prior to checking the cache so that a clearCache=true
	// request always returns a freshly computed value
	if clearCache {
		a.AggregateCache.Flush()
		a.CostDataCache.Flush()
	}

	cacheExpiry := a.GetCacheExpiration(window.Duration())
	if noExpireCache {
		cacheExpiry = cache.NoExpiration
	}

	// parametrize cache key by all request parameters
	aggKey := GenerateAggKey(window, field, subfields, opts)

	thanosOffset := time.Now().Add(-thanos.OffsetDuration())
	if a.ThanosClient != nil && window.End().After(thanosOffset) {
		log.Infof("ComputeAggregateCostModel: setting end time backwards to first present data")

		// Apply offsets to both end and start times to maintain correct time range
		deltaDuration := window.End().Sub(thanosOffset)
		s := window.Start().Add(-1 * deltaDuration)
		e := time.Now().Add(-thanos.OffsetDuration())
		window.Set(&s, &e)
	}

	dur, off := window.DurationOffsetStrings()
	key := fmt.Sprintf(`%s:%s:%fh:%t`, dur, off, resolution.Hours(), remoteEnabled)

	// report message about which of the two caches hit. by default report a miss
	cacheMessage := fmt.Sprintf("ComputeAggregateCostModel: L1 cache miss: %s L2 cache miss: %s", aggKey, key)

	// check the cache for aggregated response; if cache is hit and not disabled, return response
	if value, found := a.AggregateCache.Get(aggKey); found && !disableAggregateCostModelCache && !noCache {
		result, ok := value.(map[string]*Aggregation)
		if !ok {
			// disable cache and recompute if type cast fails
			log.Errorf("ComputeAggregateCostModel: caching error: failed to cast aggregate data to struct: %s", aggKey)
			return a.ComputeAggregateCostModel(promClient, window, field, subfields, opts)
		}
		return result, fmt.Sprintf("aggregate cache hit: %s", aggKey), nil
	}

	if window.Hours() >= 1.0 {
		// exclude the last window of the time frame to match Prometheus definitions of range, offset, and resolution
		start := window.Start().Add(resolution)
		window.Set(&start, window.End())
	} else {
		// don't cache requests for durations of less than one hour
		disableAggregateCostModelCache = true
	}

	// attempt to retrieve cost data from cache
	var costData map[string]*CostData
	var err error
	cacheData, found := a.CostDataCache.Get(key)
	if found && !disableAggregateCostModelCache && !noCache {
		ok := false
		costData, ok = cacheData.(map[string]*CostData)
		cacheMessage = fmt.Sprintf("ComputeAggregateCostModel: L1 cache miss: %s, L2 cost data cache hit: %s", aggKey, key)
		if !ok {
			log.Errorf("ComputeAggregateCostModel: caching error: failed to cast cost data to struct: %s", key)
		}
	} else {
		log.Infof("ComputeAggregateCostModel: missed cache: %s (found %t, disableAggregateCostModelCache %t, noCache %t)", key, found, disableAggregateCostModelCache, noCache)

		costData, err = a.Model.ComputeCostDataRange(promClient, a.CloudProvider, window, resolution, "", "", remoteEnabled)
		if err != nil {
			if prom.IsErrorCollection(err) {
				return nil, "", err
			}
			if pce, ok := err.(prom.CommError); ok {
				return nil, "", pce
			}
			if strings.Contains(err.Error(), "data is empty") {
				return nil, "", &EmptyDataError{err: err, window: window}
			}
			return nil, "", err
		}

		// compute length of the time series in the cost data and only compute
		// aggregates and cache if the length is sufficiently high
		costDataLen := costDataTimeSeriesLength(costData)

		if costDataLen == 0 {
			return nil, "", &EmptyDataError{window: window}
		}
		if costDataLen >= minCostDataLength && !noCache {
			log.Infof("ComputeAggregateCostModel: setting L2 cache: %s", key)
			a.CostDataCache.Set(key, costData, cacheExpiry)
		}
	}

	c, err := a.CloudProvider.GetConfig()
	if err != nil {
		return nil, "", err
	}
	discount, err := ParsePercentString(c.Discount)
	if err != nil {
		return nil, "", err
	}
	customDiscount, err := ParsePercentString(c.NegotiatedDiscount)
	if err != nil {
		return nil, "", err
	}

	sc := make(map[string]*SharedCostInfo)
	if !disableSharedOverhead {
		costPerMonth := c.GetSharedOverheadCostPerMonth()
		durationCoefficient := window.Hours() / timeutil.HoursPerMonth
		sc["total"] = &SharedCostInfo{
			Name: "total",
			Cost: costPerMonth * durationCoefficient,
		}
	}

	idleCoefficients := make(map[string]float64)
	if allocateIdle {
		dur, off, err := window.DurationOffset()
		if err != nil {
			log.Errorf("ComputeAggregateCostModel: error computing idle coefficient: illegal window: %s (%s)", window, err)
			return nil, "", err
		}

		if a.ThanosClient != nil && off < thanos.OffsetDuration() {
			// Determine difference between the Thanos offset and the requested
			// offset; e.g. off=1h, thanosOffsetDuration=3h => diff=2h
			diff := thanos.OffsetDuration() - off

			// Reduce duration by difference and increase offset by difference
			// e.g. 24h offset 0h => 21h offset 3h
			dur = dur - diff
			off = thanos.OffsetDuration()

			log.Infof("ComputeAggregateCostModel: setting duration, offset to %s, %s due to Thanos", dur, off)

			// Idle computation cannot be fulfilled for some windows, specifically
			// those with sum(duration, offset) < Thanos offset, because there is
			// no data within that window.
			if dur <= 0 {
				return nil, "", fmt.Errorf("requested idle coefficients from Thanos for illegal duration, offset: %s, %s (original window %s)", dur, off, window)
			}
		}

		idleCoefficients, err = a.ComputeIdleCoefficient(costData, promClient, a.CloudProvider, discount, customDiscount, dur, off)
		if err != nil {
			durStr, offStr := timeutil.DurationOffsetStrings(dur, off)
			log.Errorf("ComputeAggregateCostModel: error computing idle coefficient: duration=%s, offset=%s, err=%s", durStr, offStr, err)
			return nil, "", err
		}
	}

	totalContainerCost := 0.0
	if shared == SplitTypeWeighted {
		totalContainerCost = GetTotalContainerCost(costData, rate, a.CloudProvider, discount, customDiscount, idleCoefficients)
	}

	// filter cost data by namespace and cluster after caching for maximal cache hits
	costData, filteredContainerCount, filteredEnvironments := FilterCostData(costData, retainFuncs, filterFuncs)

	// aggregate cost model data by given fields and cache the result for the default expiration
	aggOpts := &AggregationOptions{
		Discount:               discount,
		CustomDiscount:         customDiscount,
		IdleCoefficients:       idleCoefficients,
		IncludeEfficiency:      includeEfficiency,
		IncludeTimeSeries:      includeTimeSeries,
		Rate:                   rate,
		ResolutionHours:        resolution.Hours(),
		SharedResourceInfo:     sri,
		SharedCosts:            sc,
		FilteredContainerCount: filteredContainerCount,
		FilteredEnvironments:   filteredEnvironments,
		TotalContainerCost:     totalContainerCost,
		SharedSplit:            shared,
	}
	result := AggregateCostData(costData, field, subfields, a.CloudProvider, aggOpts)

	// If sending time series data back, switch scale back to hourly data. At this point,
	// resolutionHours may have converted our hourly data to more- or less-than hourly data.
	if includeTimeSeries {
		for _, aggs := range result {
			ScaleAggregationTimeSeries(aggs, resolution.Hours())
		}
	}

	// compute length of the time series in the cost data and only cache
	// aggregation results if the length is sufficiently high
	costDataLen := costDataTimeSeriesLength(costData)
	if costDataLen >= minCostDataLength && window.Hours() > 1.0 && !noCache {
		// Set the result map (rather than a pointer to it) because map is a reference type
		log.Infof("ComputeAggregateCostModel: setting aggregate cache: %s", aggKey)
		a.AggregateCache.Set(aggKey, result, cacheExpiry)
	} else {
		log.Infof("ComputeAggregateCostModel: not setting aggregate cache: %s (not enough data: %t; duration less than 1h: %t; noCache: %t)", key, costDataLen < minCostDataLength, window.Hours() < 1, noCache)
	}

	return result, cacheMessage, nil
}

// ScaleAggregationTimeSeries reverses the scaling done by ScaleHourlyCostData, returning
// the aggregation's time series to hourly data.
func ScaleAggregationTimeSeries(aggregation *Aggregation, resolutionHours float64) {
	for _, v := range aggregation.CPUCostVector {
		v.Value /= resolutionHours
	}

	for _, v := range aggregation.GPUCostVector {
		v.Value /= resolutionHours
	}

	for _, v := range aggregation.RAMCostVector {
		v.Value /= resolutionHours
	}

	for _, v := range aggregation.PVCostVector {
		v.Value /= resolutionHours
	}

	for _, v := range aggregation.NetworkCostVector {
		v.Value /= resolutionHours
	}

	for _, v := range aggregation.TotalCostVector {
		v.Value /= resolutionHours
	}

	return
}

// String returns a string representation of the encapsulated shared resources, which
// can be used to uniquely identify a set of shared resources. Sorting sets of shared
// resources ensures that strings representing permutations of the same combination match.
func (s *SharedResourceInfo) String() string {
	if s == nil {
		return ""
	}

	nss := []string{}
	for ns := range s.SharedNamespace {
		nss = append(nss, ns)
	}
	sort.Strings(nss)
	nsStr := strings.Join(nss, ",")

	labels := []string{}
	for lbl, vals := range s.LabelSelectors {
		for val := range vals {
			if lbl != "" && val != "" {
				labels = append(labels, fmt.Sprintf("%s=%s", lbl, val))
			}
		}
	}
	sort.Strings(labels)
	labelStr := strings.Join(labels, ",")

	return fmt.Sprintf("%s:%s", nsStr, labelStr)
}

type aggKeyParams struct {
	duration   string
	offset     string
	filters    map[string]string
	field      string
	subfields  []string
	rate       string
	sri        *SharedResourceInfo
	shareType  string
	idle       bool
	timeSeries bool
	efficiency bool
}

// GenerateAggKey generates a parameter-unique key for caching the aggregate cost model
func GenerateAggKey(window opencost.Window, field string, subfields []string, opts *AggregateQueryOpts) string {
	if opts == nil {
		opts = DefaultAggregateQueryOpts()
	}

	// Covert to duration, offset so that cache hits occur, even when timestamps have
	// shifted slightly.
	duration, offset := window.DurationOffsetStrings()

	// parse, trim, and sort podprefix filters
	podPrefixFilters := []string{}
	if ppfs, ok := opts.Filters["podprefix"]; ok && ppfs != "" {
		for _, psf := range strings.Split(ppfs, ",") {
			podPrefixFilters = append(podPrefixFilters, strings.TrimSpace(psf))
		}
	}
	sort.Strings(podPrefixFilters)
	podPrefixFiltersStr := strings.Join(podPrefixFilters, ",")

	// parse, trim, and sort namespace filters
	nsFilters := []string{}
	if nsfs, ok := opts.Filters["namespace"]; ok && nsfs != "" {
		for _, nsf := range strings.Split(nsfs, ",") {
			nsFilters = append(nsFilters, strings.TrimSpace(nsf))
		}
	}
	sort.Strings(nsFilters)
	nsFilterStr := strings.Join(nsFilters, ",")

	// parse, trim, and sort node filters
	nodeFilters := []string{}
	if nodefs, ok := opts.Filters["node"]; ok && nodefs != "" {
		for _, nodef := range strings.Split(nodefs, ",") {
			nodeFilters = append(nodeFilters, strings.TrimSpace(nodef))
		}
	}
	sort.Strings(nodeFilters)
	nodeFilterStr := strings.Join(nodeFilters, ",")

	// parse, trim, and sort cluster filters
	cFilters := []string{}
	if cfs, ok := opts.Filters["cluster"]; ok && cfs != "" {
		for _, cf := range strings.Split(cfs, ",") {
			cFilters = append(cFilters, strings.TrimSpace(cf))
		}
	}
	sort.Strings(cFilters)
	cFilterStr := strings.Join(cFilters, ",")

	// parse, trim, and sort label filters
	lFilters := []string{}
	if lfs, ok := opts.Filters["labels"]; ok && lfs != "" {
		for _, lf := range strings.Split(lfs, ",") {
			// trim whitespace from the label name and the label value
			// of each label name/value pair, then reconstruct
			// e.g. "tier = frontend, app = kubecost" == "app=kubecost,tier=frontend"
			lfa := strings.Split(lf, "=")
			if len(lfa) == 2 {
				lfn := strings.TrimSpace(lfa[0])
				lfv := strings.TrimSpace(lfa[1])
				lFilters = append(lFilters, fmt.Sprintf("%s=%s", lfn, lfv))
			} else {
				// label is not of the form name=value, so log it and move on
				log.Warnf("GenerateAggKey: skipping illegal label filter: %s", lf)
			}
		}
	}
	sort.Strings(lFilters)
	lFilterStr := strings.Join(lFilters, ",")

	// parse, trim, and sort annotation filters
	aFilters := []string{}
	if afs, ok := opts.Filters["annotations"]; ok && afs != "" {
		for _, af := range strings.Split(afs, ",") {
			// trim whitespace from the annotation name and the annotation value
			// of each annotation name/value pair, then reconstruct
			// e.g. "tier = frontend, app = kubecost" == "app=kubecost,tier=frontend"
			afa := strings.Split(af, "=")
			if len(afa) == 2 {
				afn := strings.TrimSpace(afa[0])
				afv := strings.TrimSpace(afa[1])
				aFilters = append(aFilters, fmt.Sprintf("%s=%s", afn, afv))
			} else {
				// annotation is not of the form name=value, so log it and move on
				log.Warnf("GenerateAggKey: skipping illegal annotation filter: %s", af)
			}
		}
	}
	sort.Strings(aFilters)
	aFilterStr := strings.Join(aFilters, ",")

	filterStr := fmt.Sprintf("%s:%s:%s:%s:%s:%s", nsFilterStr, nodeFilterStr, cFilterStr, lFilterStr, aFilterStr, podPrefixFiltersStr)

	sort.Strings(subfields)
	fieldStr := fmt.Sprintf("%s:%s", field, strings.Join(subfields, ","))

	if offset == "1m" {
		offset = ""
	}

	return fmt.Sprintf("%s:%s:%s:%s:%s:%s:%s:%t:%t:%t", duration, offset, filterStr, fieldStr, opts.Rate,
		opts.SharedResources, opts.ShareSplit, opts.AllocateIdle, opts.IncludeTimeSeries,
		opts.IncludeEfficiency)
}

// Aggregator is capable of computing the aggregated cost model. This is
// a brutal interface, which should be cleaned up, but it's necessary for
// being able to swap in an ETL-backed implementation.
type Aggregator interface {
	ComputeAggregateCostModel(promClient prometheusClient.Client, window opencost.Window, field string, subfields []string, opts *AggregateQueryOpts) (map[string]*Aggregation, string, error)
}

func (a *Accesses) warmAggregateCostModelCache() {
	// Only allow one concurrent cache-warming operation
	sem := util.NewSemaphore(1)

	// Set default values, pulling them from application settings where applicable, and warm the cache
	// for the given duration. Cache is intentionally set to expire (i.e. noExpireCache=false) so that
	// if the default parameters change, the old cached defaults with eventually expire. Thus, the
	// timing of the cache expiry/refresh is the only mechanism ensuring 100% cache warmth.
	warmFunc := func(duration, offset time.Duration, cacheEfficiencyData bool) (error, error) {
		if a.ThanosClient != nil {
			duration = thanos.OffsetDuration()
			log.Infof("Setting Offset to %s", duration)
		}
		fmtDuration, fmtOffset := timeutil.DurationOffsetStrings(duration, offset)
		durationHrs, err := timeutil.FormatDurationStringDaysToHours(fmtDuration)
		promClient := a.GetPrometheusClient(true)

		windowStr := fmt.Sprintf("%s offset %s", fmtDuration, fmtOffset)
		window, err := opencost.ParseWindowUTC(windowStr)
		if err != nil {
			return nil, fmt.Errorf("invalid window from window string: %s", windowStr)
		}

		field := "namespace"
		subfields := []string{}

		aggOpts := DefaultAggregateQueryOpts()
		aggOpts.Rate = ""
		aggOpts.Filters = map[string]string{}
		aggOpts.IncludeTimeSeries = false
		aggOpts.IncludeEfficiency = true
		aggOpts.DisableAggregateCostModelCache = true
		aggOpts.ClearCache = false
		aggOpts.NoCache = false
		aggOpts.NoExpireCache = false
		aggOpts.ShareSplit = SplitTypeWeighted
		aggOpts.RemoteEnabled = env.IsRemoteEnabled()
		aggOpts.AllocateIdle = provider.AllocateIdleByDefault(a.CloudProvider)

		sharedNamespaces := provider.SharedNamespaces(a.CloudProvider)
		sharedLabelNames, sharedLabelValues := provider.SharedLabels(a.CloudProvider)

		if len(sharedNamespaces) > 0 || len(sharedLabelNames) > 0 {
			aggOpts.SharedResources = NewSharedResourceInfo(true, sharedNamespaces, sharedLabelNames, sharedLabelValues)
		}

		aggKey := GenerateAggKey(window, field, subfields, aggOpts)
		log.Infof("aggregation: cache warming defaults: %s", aggKey)
		key := fmt.Sprintf("%s:%s", durationHrs, fmtOffset)

		_, _, aggErr := a.ComputeAggregateCostModel(promClient, window, field, subfields, aggOpts)
		if aggErr != nil {
			log.Infof("Error building cache %s: %s", window, aggErr)
		}

		totals, err := a.ComputeClusterCosts(promClient, a.CloudProvider, duration, offset, cacheEfficiencyData)
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
		return aggErr, err
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
				aggErr, err := warmFunc(duration, offset, false)
				sem.Return()

				log.Infof("aggregation: warm cache: %s", timeutil.DurationString(duration))
				if aggErr == nil && err == nil {
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
				aggErr, err := warmFunc(duration, offset, false)
				sem.Return()
				if aggErr == nil && err == nil {
					time.Sleep(a.GetCacheRefresh(duration))
				} else {
					time.Sleep(5 * time.Minute)
				}
			}
		}(sem)
	}
}

var (
	// Convert UTC-RFC3339 pairs to configured UTC offset
	// e.g. with UTC offset of -0600, 2020-07-01T00:00:00Z becomes
	// 2020-07-01T06:00:00Z == 2020-07-01T00:00:00-0600
	// TODO niko/etl fix the frontend because this is confusing if you're
	// actually asking for UTC time (...Z) and we swap that "Z" out for the
	// configured UTC offset without asking
	rfc3339      = `\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\dZ`
	rfc3339Regex = regexp.MustCompile(fmt.Sprintf(`(%s),(%s)`, rfc3339, rfc3339))

	durRegex     = regexp.MustCompile(`^(\d+)(m|h|d|s)$`)
	percentRegex = regexp.MustCompile(`(\d+\.*\d*)%`)
)

// AggregateCostModelHandler handles requests to the aggregated cost model API. See
// ComputeAggregateCostModel for details.
func (a *Accesses) AggregateCostModelHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")

	windowStr := r.URL.Query().Get("window")

	match := rfc3339Regex.FindStringSubmatch(windowStr)
	if match != nil {
		start, _ := time.Parse(time.RFC3339, match[1])
		start = start.Add(-env.GetParsedUTCOffset()).In(time.UTC)
		end, _ := time.Parse(time.RFC3339, match[2])
		end = end.Add(-env.GetParsedUTCOffset()).In(time.UTC)
		windowStr = fmt.Sprintf("%sZ,%sZ", start.Format("2006-01-02T15:04:05"), end.Format("2006-01-02T15:04:05Z"))
	}

	// determine duration and offset from query parameters
	window, err := opencost.ParseWindowWithOffset(windowStr, env.GetParsedUTCOffset())
	if err != nil || window.Start() == nil {
		WriteError(w, BadRequest(fmt.Sprintf("invalid window: %s", err)))
		return
	}

	isDurationStr := durRegex.MatchString(windowStr)

	// legacy offset option should override window offset
	if r.URL.Query().Get("offset") != "" {
		offset := r.URL.Query().Get("offset")
		// Shift window by offset, but only when manually set with separate
		// parameter and window was provided as a duration string. Otherwise,
		// do not alter the (duration, offset) from ParseWindowWithOffset.
		if offset != "1m" && isDurationStr {
			match := durRegex.FindStringSubmatch(offset)
			if match != nil && len(match) == 3 {
				dur := time.Minute
				if match[2] == "h" {
					dur = time.Hour
				}
				if match[2] == "d" {
					dur = 24 * time.Hour
				}
				if match[2] == "s" {
					dur = time.Second
				}

				num, _ := strconv.ParseInt(match[1], 10, 64)
				window = window.Shift(-time.Duration(num) * dur)
			}
		}
	}

	opts := DefaultAggregateQueryOpts()

	// parse remaining query parameters
	namespace := r.URL.Query().Get("namespace")
	cluster := r.URL.Query().Get("cluster")
	labels := r.URL.Query().Get("labels")
	annotations := r.URL.Query().Get("annotations")
	podprefix := r.URL.Query().Get("podprefix")
	field := r.URL.Query().Get("aggregation")
	sharedNamespaces := r.URL.Query().Get("sharedNamespaces")
	sharedLabelNames := r.URL.Query().Get("sharedLabelNames")
	sharedLabelValues := r.URL.Query().Get("sharedLabelValues")
	remote := r.URL.Query().Get("remote") != "false"
	subfieldStr := r.URL.Query().Get("aggregationSubfield")
	subfields := []string{}
	if len(subfieldStr) > 0 {
		s := strings.Split(r.URL.Query().Get("aggregationSubfield"), ",")
		for _, rawLabel := range s {
			subfields = append(subfields, promutil.SanitizeLabelName(rawLabel))
		}
	}

	idleFlag := r.URL.Query().Get("allocateIdle")
	if idleFlag == "default" {
		c, _ := a.CloudProvider.GetConfig()
		opts.AllocateIdle = (c.DefaultIdle == "true")
	} else {
		opts.AllocateIdle = (idleFlag == "true")
	}

	opts.Rate = r.URL.Query().Get("rate")

	opts.ShareSplit = r.URL.Query().Get("sharedSplit")

	// timeSeries == true maintains the time series dimension of the data,
	// which by default gets summed over the entire interval
	opts.IncludeTimeSeries = r.URL.Query().Get("timeSeries") == "true"

	// efficiency has been deprecated in favor of a default to always send efficiency
	opts.IncludeEfficiency = true

	// TODO niko/caching rename "recomputeCache"
	// disableCache, if set to "true", tells this function to recompute and
	// cache the requested data
	opts.DisableAggregateCostModelCache = r.URL.Query().Get("disableCache") == "true"

	// clearCache, if set to "true", tells this function to flush the cache,
	// then recompute and cache the requested data
	opts.ClearCache = r.URL.Query().Get("clearCache") == "true"

	// noCache avoids the cache altogether, both reading from and writing to
	opts.NoCache = r.URL.Query().Get("noCache") == "true"

	// noExpireCache should only be used by cache warming to set non-expiring caches
	opts.NoExpireCache = false

	// etl triggers ETL adapter
	opts.UseETLAdapter = r.URL.Query().Get("etl") == "true"

	// aggregation field is required
	if field == "" {
		WriteError(w, BadRequest("Missing aggregation field parameter"))
		return
	}

	// aggregation subfield is required when aggregation field is "label"
	if (field == "label" || field == "annotation") && len(subfields) == 0 {
		WriteError(w, BadRequest("Missing aggregation subfield parameter"))
		return
	}

	// enforce one of the available rate options
	if opts.Rate != "" && opts.Rate != "hourly" && opts.Rate != "daily" && opts.Rate != "monthly" {
		WriteError(w, BadRequest("Rate parameter only supports: hourly, daily, monthly or empty"))
		return
	}

	// parse cost data filters
	// namespace and cluster are exact-string-matches
	// labels are expected to be comma-separated and to take the form key=value
	// e.g. app=cost-analyzer,app.kubernetes.io/instance=kubecost
	opts.Filters = map[string]string{
		"namespace":   namespace,
		"cluster":     cluster,
		"labels":      labels,
		"annotations": annotations,
		"podprefix":   podprefix,
	}

	// parse shared resources
	sn := []string{}
	sln := []string{}
	slv := []string{}
	if sharedNamespaces != "" {
		sn = strings.Split(sharedNamespaces, ",")
	}
	if sharedLabelNames != "" {
		sln = strings.Split(sharedLabelNames, ",")
		slv = strings.Split(sharedLabelValues, ",")
		if len(sln) != len(slv) || slv[0] == "" {
			WriteError(w, BadRequest("Supply exactly one shared label value per shared label name"))
			return
		}
	}
	if len(sn) > 0 || len(sln) > 0 {
		opts.SharedResources = NewSharedResourceInfo(true, sn, sln, slv)
	}

	// enable remote if it is available and not disabled
	opts.RemoteEnabled = remote && env.IsRemoteEnabled()

	promClient := a.GetPrometheusClient(remote)

	var data map[string]*Aggregation
	var message string
	data, message, err = a.AggAPI.ComputeAggregateCostModel(promClient, window, field, subfields, opts)

	// Find any warnings in http request context
	warning, _ := httputil.GetWarning(r)

	if err != nil {
		if emptyErr, ok := err.(*EmptyDataError); ok {
			if warning == "" {
				w.Write(WrapData(map[string]interface{}{}, emptyErr))
			} else {
				w.Write(WrapDataWithWarning(map[string]interface{}{}, emptyErr, warning))
			}
			return
		}
		if boundaryErr, ok := err.(*opencost.BoundaryError); ok {
			if window.Start() != nil && window.Start().After(time.Now().Add(-90*24*time.Hour)) {
				// Asking for data within a 90 day period: it will be available
				// after the pipeline builds
				msg := "Data will be available after ETL is built"

				match := percentRegex.FindStringSubmatch(boundaryErr.Message)
				if len(match) > 1 {
					completionPct, err := strconv.ParseFloat(match[1], 64)
					if err == nil {
						msg = fmt.Sprintf("%s (%.1f%% complete)", msg, completionPct)
					}
				}

				WriteError(w, InternalServerError(msg))
			} else {
				// Boundary error outside of 90 day period; may not be available
				WriteError(w, InternalServerError(boundaryErr.Error()))
			}
			return
		}
		errStr := fmt.Sprintf("error computing aggregate cost model: %s", err)
		WriteError(w, InternalServerError(errStr))
		return
	}

	if warning == "" {
		w.Write(WrapDataWithMessage(data, nil, message))
	} else {
		w.Write(WrapDataWithMessageAndWarning(data, nil, message, warning))
	}
}

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
