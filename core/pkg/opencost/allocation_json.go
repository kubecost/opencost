package opencost

import (
	"fmt"
	"math"
	"time"

	"github.com/opencost/opencost/core/pkg/util/json"
)

// AllocationJSON  exists because there are expected JSON response fields
// that are calculated values from methods on an annotation
type AllocationJSON struct {
	Name                           string                          `json:"name"`
	Properties                     *AllocationProperties           `json:"properties"`
	Window                         Window                          `json:"window"`
	Start                          string                          `json:"start"`
	End                            string                          `json:"end"`
	Minutes                        *float64                        `json:"minutes"`
	CPUCores                       *float64                        `json:"cpuCores"`
	CPUCoreRequestAverage          *float64                        `json:"cpuCoreRequestAverage"`
	CPUCoreUsageAverage            *float64                        `json:"cpuCoreUsageAverage"`
	CPUCoreHours                   *float64                        `json:"cpuCoreHours"`
	CPUCost                        *float64                        `json:"cpuCost"`
	CPUCostAdjustment              *float64                        `json:"cpuCostAdjustment"`
	CPUCostIdle                    *float64                        `json:"cpuCostIdle"`
	CPUEfficiency                  *float64                        `json:"cpuEfficiency"`
	GPUCount                       *float64                        `json:"gpuCount"`
	GPURequestAverage              *float64                        `json:"-"`
	GPUUsageAverage                *float64                        `json:"-"`
	GPUHours                       *float64                        `json:"gpuHours"`
	GPUCost                        *float64                        `json:"gpuCost"`
	GPUCostAdjustment              *float64                        `json:"gpuCostAdjustment"`
	GPUCostIdle                    *float64                        `json:"gpuCostIdle"`
	GPUEfficiency                  *float64                        `json:"gpuEfficiency"`
	NetworkTransferBytes           *float64                        `json:"networkTransferBytes"`
	NetworkReceiveBytes            *float64                        `json:"networkReceiveBytes"`
	NetworkCost                    *float64                        `json:"networkCost"`
	NetworkCrossZoneCost           *float64                        `json:"networkCrossZoneCost"`
	NetworkCrossRegionCost         *float64                        `json:"networkCrossRegionCost"`
	NetworkInternetCost            *float64                        `json:"networkInternetCost"`
	NetworkCostAdjustment          *float64                        `json:"networkCostAdjustment"`
	LoadBalancerCost               *float64                        `json:"loadBalancerCost"`
	LoadBalancerCostAdjustment     *float64                        `json:"loadBalancerCostAdjustment"`
	PVBytes                        *float64                        `json:"pvBytes"`
	PVByteHours                    *float64                        `json:"pvByteHours"`
	PVCost                         *float64                        `json:"pvCost"`
	PVs                            PVAllocations                   `json:"pvs"`
	PVCostAdjustment               *float64                        `json:"pvCostAdjustment"`
	RAMBytes                       *float64                        `json:"ramBytes"`
	RAMByteRequestAverage          *float64                        `json:"ramByteRequestAverage"`
	RAMByteUsageAverage            *float64                        `json:"ramByteUsageAverage"`
	RAMByteHours                   *float64                        `json:"ramByteHours"`
	RAMCost                        *float64                        `json:"ramCost"`
	RAMCostAdjustment              *float64                        `json:"ramCostAdjustment"`
	RAMCostIdle                    *float64                        `json:"ramCostIdle"`
	RAMEfficiency                  *float64                        `json:"ramEfficiency"`
	ExternalCost                   *float64                        `json:"externalCost"`
	SharedCost                     *float64                        `json:"sharedCost"`
	TotalCost                      *float64                        `json:"totalCost"`
	TotalEfficiency                *float64                        `json:"totalEfficiency"`
	RawAllocationOnly              *RawAllocationOnlyData          `json:"rawAllocationOnly,omitempty"`
	ProportionalAssetResourceCosts *ProportionalAssetResourceCosts `json:"proportionalAssetResourceCosts,omitempty"`
	LoadBalancers                  LbAllocations                   `json:"lbAllocations"`
	SharedCostBreakdown            *SharedCostBreakdowns           `json:"sharedCostBreakdown,omitempty"`
	GPUAllocation                  *GPUAllocation                  `json:"gpuAllocation"`
}

func (aj *AllocationJSON) BuildFromAllocation(a *Allocation) {
	if aj == nil {
		return
	}
	aj.Name = a.Name
	aj.Properties = a.Properties
	aj.Window = a.Window
	aj.Start = a.Start.UTC().Format(time.RFC3339)
	aj.End = a.End.UTC().Format(time.RFC3339)
	aj.Minutes = formatFloat64ForResponse(a.Minutes())
	aj.CPUCores = formatFloat64ForResponse(a.CPUCores())
	aj.CPUCoreRequestAverage = formatFloat64ForResponse(a.CPUCoreRequestAverage)
	aj.CPUCoreUsageAverage = formatFloat64ForResponse(a.CPUCoreUsageAverage)
	aj.CPUCoreHours = formatFloat64ForResponse(a.CPUCoreHours)
	aj.CPUCost = formatFloat64ForResponse(a.CPUCost)
	aj.CPUCostAdjustment = formatFloat64ForResponse(a.CPUCostAdjustment)
	aj.CPUCostIdle = formatFloat64ForResponse(a.CPUCostIdle)
	aj.CPUEfficiency = formatFloat64ForResponse(a.CPUEfficiency())
	aj.GPUCount = formatFloat64ForResponse(a.GPUs())
	aj.GPUHours = formatFloat64ForResponse(a.GPUHours)
	aj.GPUCost = formatFloat64ForResponse(a.GPUCost)
	aj.GPUCostAdjustment = formatFloat64ForResponse(a.GPUCostAdjustment)
	aj.GPUCostIdle = formatFloat64ForResponse(a.GPUCostIdle)
	aj.GPUEfficiency = formatFloat64ForResponse(a.GPUEfficiency())
	aj.NetworkTransferBytes = formatFloat64ForResponse(a.NetworkTransferBytes)
	aj.NetworkReceiveBytes = formatFloat64ForResponse(a.NetworkReceiveBytes)
	aj.NetworkCost = formatFloat64ForResponse(a.NetworkCost)
	aj.NetworkCrossZoneCost = formatFloat64ForResponse(a.NetworkCrossZoneCost)
	aj.NetworkCrossRegionCost = formatFloat64ForResponse(a.NetworkCrossRegionCost)
	aj.NetworkInternetCost = formatFloat64ForResponse(a.NetworkInternetCost)
	aj.NetworkCostAdjustment = formatFloat64ForResponse(a.NetworkCostAdjustment)
	aj.LoadBalancerCost = formatFloat64ForResponse(a.LoadBalancerCost)
	aj.LoadBalancerCostAdjustment = formatFloat64ForResponse(a.LoadBalancerCostAdjustment)
	aj.PVBytes = formatFloat64ForResponse(a.PVBytes())
	aj.PVByteHours = formatFloat64ForResponse(a.PVByteHours())
	aj.PVCost = formatFloat64ForResponse(a.PVCost())
	aj.PVs = a.PVs
	aj.PVCostAdjustment = formatFloat64ForResponse(a.PVCostAdjustment)
	aj.RAMBytes = formatFloat64ForResponse(a.RAMBytes())
	aj.RAMByteRequestAverage = formatFloat64ForResponse(a.RAMBytesRequestAverage)
	aj.RAMByteUsageAverage = formatFloat64ForResponse(a.RAMBytesUsageAverage)
	aj.RAMByteHours = formatFloat64ForResponse(a.RAMByteHours)
	aj.RAMCost = formatFloat64ForResponse(a.RAMCost)
	aj.RAMCostAdjustment = formatFloat64ForResponse(a.RAMCostAdjustment)
	aj.RAMCostIdle = formatFloat64ForResponse(a.RAMCostIdle)
	aj.RAMEfficiency = formatFloat64ForResponse(a.RAMEfficiency())
	aj.SharedCost = formatFloat64ForResponse(a.SharedCost)
	aj.ExternalCost = formatFloat64ForResponse(a.ExternalCost)
	aj.TotalCost = formatFloat64ForResponse(a.TotalCost())
	aj.TotalEfficiency = formatFloat64ForResponse(a.TotalEfficiency())
	aj.RawAllocationOnly = a.RawAllocationOnly
	aj.ProportionalAssetResourceCosts = &a.ProportionalAssetResourceCosts
	aj.LoadBalancers = a.LoadBalancers
	aj.SharedCostBreakdown = &a.SharedCostBreakdown
	aj.GPUAllocation = a.GPUAllocation
}

// formatFloat64ForResponse - take an existing float64, round it to 6 decimal places and return is possible, or return nil if invalid
func formatFloat64ForResponse(f float64) *float64 {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return nil
	}

	// 6 digits of precision is the maximum the API should return
	result := math.Round(f*100000) / 100000.0
	return &result
}

// MarshalJSON implements json.Marshaler interface
func (a *Allocation) MarshalJSON() ([]byte, error) {

	aj := &AllocationJSON{}
	aj.BuildFromAllocation(a)
	buffer, err := json.Marshal(aj)
	if err != nil {
		return nil, fmt.Errorf("unable to marshal allocation %s to JSON: %s", aj.Name, err)
	}
	return buffer, nil
}

// UnmarshalJSON prevent nil pointer on PVAllocations
func (a *Allocation) UnmarshalJSON(b []byte) error {
	// initialize PV to prevent nil panic
	a.PVs = PVAllocations{}
	// Aliasing Allocation and casting to alias gives access to the default unmarshaller
	type alloc Allocation
	err := json.Unmarshal(b, (*alloc)(a))
	if err != nil {
		return err
	}
	// clear PVs if they are empty, it is not initialized when empty
	if len(a.PVs) == 0 {
		a.PVs = nil
	}
	return nil
}

// MarshalJSON marshals PVAllocation as map[*PVKey]*PVAllocation this allows PVKey to retain its values through marshalling
func (pv PVAllocations) MarshalJSON() (b []byte, err error) {
	pointerMap := make(map[*PVKey]*PVAllocation)
	for pvKey, pvAlloc := range pv {
		kp := pvKey
		pointerMap[&kp] = pvAlloc
	}
	return json.Marshal(pointerMap)
}

// MarshalText converts PVKey to string to make it compatible with JSON Marshaller as an Object key
// this function is required to have a value caller for the actual values to be saved
func (pvk PVKey) MarshalText() (text []byte, err error) {
	return []byte(pvk.String()), nil
}

// UnmarshalText converts JSON key string to PVKey it compatible with JSON Unmarshaller from an Object key
// this function is required to have a pointer caller for values to be pulled into marshalling struct
func (pvk *PVKey) UnmarshalText(text []byte) error {
	return pvk.FromString(string(text))
}

// MarshalJSON JSON-encodes the AllocationSet
func (as *AllocationSet) MarshalJSON() ([]byte, error) {
	if as == nil {
		return json.Marshal(map[string]*Allocation{})
	}
	return json.Marshal(as.Allocations)
}

// MarshalJSON JSON-encodes the range
func (asr *AllocationSetRange) MarshalJSON() ([]byte, error) {
	if asr == nil {
		return json.Marshal([]*AllocationSet{})
	}

	return json.Marshal(asr.Allocations)
}
