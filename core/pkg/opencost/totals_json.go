package opencost

import (
	"time"

	"github.com/opencost/opencost/core/pkg/util/formatutil"
)

type AllocationTotalsResponse struct {
	Start                          time.Time `json:"start"`
	End                            time.Time `json:"end"`
	Cluster                        string    `json:"cluster"`
	Node                           string    `json:"node"`
	Count                          int       `json:"count"`
	CPUCost                        *float64  `json:"cpuCost"`
	CPUCostAdjustment              *float64  `json:"cpuCostAdjustment"`
	GPUCost                        *float64  `json:"gpuCost"`
	GPUCostAdjustment              *float64  `json:"gpuCostAdjustment"`
	LoadBalancerCost               *float64  `json:"loadBalancerCost"`
	LoadBalancerCostAdjustment     *float64  `json:"loadBalancerCostAdjustment"`
	NetworkCost                    *float64  `json:"networkCost"`
	NetworkCostAdjustment          *float64  `json:"networkCostAdjustment"`
	PersistentVolumeCost           *float64  `json:"persistentVolumeCost"`
	PersistentVolumeCostAdjustment *float64  `json:"persistentVolumeCostAdjustment"`
	RAMCost                        *float64  `json:"ramCost"`
	RAMCostAdjustment              *float64  `json:"ramCostAdjustment"`
	TotalCost                      *float64  `json:"totalCost"`
}

func (arts *AllocationTotals) ToResponse() *AllocationTotalsResponse {
	if arts == nil {
		return nil
	}

	return &AllocationTotalsResponse{
		Start:                          arts.Start,
		End:                            arts.End,
		Cluster:                        arts.Cluster,
		Node:                           arts.Node,
		Count:                          arts.Count,
		CPUCost:                        formatutil.Float64ToResponse(arts.CPUCost),
		CPUCostAdjustment:              formatutil.Float64ToResponse(arts.CPUCostAdjustment),
		GPUCost:                        formatutil.Float64ToResponse(arts.GPUCost),
		GPUCostAdjustment:              formatutil.Float64ToResponse(arts.GPUCostAdjustment),
		LoadBalancerCost:               formatutil.Float64ToResponse(arts.LoadBalancerCost),
		LoadBalancerCostAdjustment:     formatutil.Float64ToResponse(arts.LoadBalancerCostAdjustment),
		NetworkCost:                    formatutil.Float64ToResponse(arts.NetworkCost),
		NetworkCostAdjustment:          formatutil.Float64ToResponse(arts.NetworkCostAdjustment),
		PersistentVolumeCost:           formatutil.Float64ToResponse(arts.PersistentVolumeCost),
		PersistentVolumeCostAdjustment: formatutil.Float64ToResponse(arts.PersistentVolumeCostAdjustment),
		RAMCost:                        formatutil.Float64ToResponse(arts.RAMCost),
		RAMCostAdjustment:              formatutil.Float64ToResponse(arts.RAMCostAdjustment),
		TotalCost:                      formatutil.Float64ToResponse(arts.TotalCost()),
	}
}

type AllocationTotalsResultResponse struct {
	Cluster map[string]*AllocationTotalsResponse `json:"cluster"`
	Node    map[string]*AllocationTotalsResponse `json:"node"`
}

func (atr *AllocationTotalsResult) ToResponse() *AllocationTotalsResultResponse {
	response := &AllocationTotalsResultResponse{
		Cluster: map[string]*AllocationTotalsResponse{},
		Node:    map[string]*AllocationTotalsResponse{},
	}

	for k, v := range atr.Cluster {
		response.Cluster[k] = v.ToResponse()
	}

	for k, v := range atr.Node {
		response.Node[k] = v.ToResponse()
	}

	return response
}
