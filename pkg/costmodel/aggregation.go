package costmodel

import (
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/opencost/opencost/pkg/cloud/provider"
	"github.com/opencost/opencost/pkg/errors"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util"
	"github.com/opencost/opencost/core/pkg/util/httputil"
	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/core/pkg/util/promutil"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
	"github.com/opencost/opencost/pkg/cloud/models"
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
		cpuv, ramv, gpuv, pvvs, netv := getPriceVectors(cp, costDatum, discount, customDiscount, idleCoefficients[clusterID])
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

func (a *Accesses) ComputeIdleCoefficient(costData map[string]*CostData, discount float64, customDiscount float64, window, offset time.Duration) (map[string]float64, error) {
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
		clusterCosts, err = a.ComputeClusterCosts(a.DataSource, a.CloudProvider, window, offset, false)
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
				cpuv, ramv, gpuv, pvvs, _ := getPriceVectors(a.CloudProvider, costDatum, discount, customDiscount, 1)
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

func parseVectorPricing(cfg *models.CustomPricing, cpuCostStr, ramCostStr, gpuCostStr, pvCostStr string) (float64, float64, float64, float64, bool) {
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

func getPriceVectors(cp models.Provider, costDatum *CostData, discount float64, customDiscount float64, idleCoefficient float64) ([]*util.Vector, []*util.Vector, []*util.Vector, [][]*util.Vector, []*util.Vector) {

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
		cpuCost, ramCost, gpuCost, pvCost, usesCustom = parseVectorPricing(customPricing, cpuCostStr, ramCostStr, gpuCostStr, pvCostStr)
	} else if costDatum.NodeData == nil && err == nil {
		cpuCostStr := customPricing.CPU
		ramCostStr := customPricing.RAM
		gpuCostStr := customPricing.GPU
		pvCostStr := customPricing.Storage
		cpuCost, ramCost, gpuCost, pvCost, usesCustom = parseVectorPricing(customPricing, cpuCostStr, ramCostStr, gpuCostStr, pvCostStr)
	} else {
		cpuCostStr := costDatum.NodeData.VCPUCost
		ramCostStr := costDatum.NodeData.RAMCost
		gpuCostStr := costDatum.NodeData.GPUCost
		pvCostStr := costDatum.NodeData.StorageCost
		cpuCost, ramCost, gpuCost, pvCost, usesCustom = parseVectorPricing(customPricing, cpuCostStr, ramCostStr, gpuCostStr, pvCostStr)
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

func totalVectors(vectors []*util.Vector) float64 {
	total := 0.0
	for _, vector := range vectors {
		total += vector.Value
	}
	return total
}

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
