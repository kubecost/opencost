//go:build incubating

package costmodel

import (
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/pkg/env"
	"github.com/opencost/opencost/pkg/prom"
)

const (
	queryFmtNodeCPUCores = `avg(avg_over_time(kube_node_status_capacity_cpu_cores[%s])) by (%s, node)`
	queryFmtNodeRAMBytes = `avg(avg_over_time(kube_node_status_capacity_memory_bytes[%s])) by (%s, node)`
	queryFmtNodeGPUCount = `avg(avg_over_time(node_gpu_count[%s])) by (%s, node, provider_id)`
)

// NodeTotals contains the cpu, ram, and gpu costs for a given node over a specific timeframe.
type NodeTotals struct {
	Start   time.Time
	End     time.Time
	Cluster string
	Node    string
	CPUCost float64
	RAMCost float64
	GPUCost float64
}

// ComputeAllocationWithNodeTotals uses the CostModel instance to compute an AllocationSet
// for the window defined by the given start and end times. The Allocations returned are unaggregated
// (i.e. down to the container level), and the node totals should contained additional data that can be
// used to calculate the idle costs at the node level.
func (cm *CostModel) ComputeAllocationWithNodeTotals(start, end time.Time, resolution time.Duration) (*opencost.AllocationSet, map[string]*NodeTotals, error) {
	nodeMap := make(map[string]*NodeTotals)

	// If the duration is short enough, compute the AllocationSet directly
	if end.Sub(start) <= cm.MaxPrometheusQueryDuration {
		as, nodeData, err := cm.computeAllocation(start, end, resolution)
		appendNodeData(nodeMap, start, end, nodeData)

		return as, nodeMap, err
	}

	// If the duration exceeds the configured MaxPrometheusQueryDuration, then
	// query for maximum-sized AllocationSets, collect them, and accumulate.

	// s and e track the coverage of the entire given window over multiple
	// internal queries.
	s, e := start, start

	// Collect AllocationSets in a range, then accumulate
	// TODO optimize by collecting consecutive AllocationSets, accumulating as we go
	asr := opencost.NewAllocationSetRange()

	for e.Before(end) {
		// By default, query for the full remaining duration. But do not let
		// any individual query duration exceed the configured max Prometheus
		// query duration.
		duration := end.Sub(e)
		if duration > cm.MaxPrometheusQueryDuration {
			duration = cm.MaxPrometheusQueryDuration
		}

		// Set start and end parameters (s, e) for next individual computation.
		e = s.Add(duration)

		// Compute the individual AllocationSet for just (s, e)
		as, nodeData, err := cm.computeAllocation(s, e, resolution)
		appendNodeData(nodeMap, s, e, nodeData)
		if err != nil {
			return opencost.NewAllocationSet(start, end), nodeMap, fmt.Errorf("error computing allocation for %s: %s", opencost.NewClosedWindow(s, e), err)
		}

		// Append to the range
		asr.Append(as)

		// Set s equal to e to set up the next query, if one exists.
		s = e
	}

	// Populate annotations, labels, and services on each Allocation. This is
	// necessary because Properties.Intersection does not propagate any values
	// stored in maps or slices for performance reasons. In this case, however,
	// it is both acceptable and necessary to do so.
	allocationAnnotations := map[string]map[string]string{}
	allocationLabels := map[string]map[string]string{}
	allocationServices := map[string]map[string]bool{}

	// Also record errors and warnings, then append them to the results later.
	errors := []string{}
	warnings := []string{}

	for _, as := range asr.Allocations {
		for k, a := range as.Allocations {
			if len(a.Properties.Annotations) > 0 {
				if _, ok := allocationAnnotations[k]; !ok {
					allocationAnnotations[k] = map[string]string{}
				}
				for name, val := range a.Properties.Annotations {
					allocationAnnotations[k][name] = val
				}
			}

			if len(a.Properties.Labels) > 0 {
				if _, ok := allocationLabels[k]; !ok {
					allocationLabels[k] = map[string]string{}
				}
				for name, val := range a.Properties.Labels {
					allocationLabels[k][name] = val
				}
			}

			if len(a.Properties.Services) > 0 {
				if _, ok := allocationServices[k]; !ok {
					allocationServices[k] = map[string]bool{}
				}
				for _, val := range a.Properties.Services {
					allocationServices[k][val] = true
				}
			}
		}

		errors = append(errors, as.Errors...)
		warnings = append(warnings, as.Warnings...)
	}

	// Accumulate to yield the result AllocationSet. After this step, we will
	// be nearly complete, but without the raw allocation data, which must be
	// recomputed.
	resultASR, err := asr.Accumulate(opencost.AccumulateOptionAll)
	if err != nil {
		return opencost.NewAllocationSet(start, end), nil, fmt.Errorf("error accumulating data for %s: %s", opencost.NewClosedWindow(s, e), err)
	}
	if resultASR != nil && len(resultASR.Allocations) == 0 {
		return opencost.NewAllocationSet(start, end), nil, nil
	}
	if length := len(resultASR.Allocations); length != 1 {
		return opencost.NewAllocationSet(start, end), nil, fmt.Errorf("expected 1 accumulated allocation set, found %d sets", length)
	}
	result := resultASR.Allocations[0]

	// Apply the annotations, labels, and services to the post-accumulation
	// results. (See above for why this is necessary.)
	for k, a := range result.Allocations {
		if annotations, ok := allocationAnnotations[k]; ok {
			a.Properties.Annotations = annotations
		}

		if labels, ok := allocationLabels[k]; ok {
			a.Properties.Labels = labels
		}

		if services, ok := allocationServices[k]; ok {
			a.Properties.Services = []string{}
			for s := range services {
				a.Properties.Services = append(a.Properties.Services, s)
			}
		}

		// Expand the Window of all Allocations within the AllocationSet
		// to match the Window of the AllocationSet, which gets expanded
		// at the end of this function.
		a.Window = a.Window.ExpandStart(start).ExpandEnd(end)
	}

	// Maintain RAM and CPU max usage values by iterating over the range,
	// computing maximums on a rolling basis, and setting on the result set.
	for _, as := range asr.Allocations {
		for key, alloc := range as.Allocations {
			resultAlloc := result.Get(key)
			if resultAlloc == nil {
				continue
			}

			if resultAlloc.RawAllocationOnly == nil {
				resultAlloc.RawAllocationOnly = &opencost.RawAllocationOnlyData{}
			}

			if alloc.RawAllocationOnly == nil {
				// This will happen inevitably for unmounted disks, but should
				// ideally not happen for any allocation with CPU and RAM data.
				if !alloc.IsUnmounted() {
					log.DedupedWarningf(10, "ComputeAllocation: raw allocation data missing for %s", key)
				}
				continue
			}

			if alloc.RawAllocationOnly.CPUCoreUsageMax > resultAlloc.RawAllocationOnly.CPUCoreUsageMax {
				resultAlloc.RawAllocationOnly.CPUCoreUsageMax = alloc.RawAllocationOnly.CPUCoreUsageMax
			}

			if alloc.RawAllocationOnly.RAMBytesUsageMax > resultAlloc.RawAllocationOnly.RAMBytesUsageMax {
				resultAlloc.RawAllocationOnly.RAMBytesUsageMax = alloc.RawAllocationOnly.RAMBytesUsageMax
			}
		}
	}

	// Expand the window to match the queried time range.
	result.Window = result.Window.ExpandStart(start).ExpandEnd(end)

	// Append errors and warnings
	result.Errors = errors
	result.Warnings = warnings

	return result, nodeMap, nil
}

func appendNodeData(nodeMap map[string]*NodeTotals, s, e time.Time, nodeData map[nodeKey]*nodePricing) {
	for k, v := range nodeData {
		key := k.String()
		if _, ok := nodeMap[key]; !ok {
			nodeMap[key] = &NodeTotals{
				Start:   s,
				End:     e,
				Cluster: k.Cluster,
				Node:    k.Node,
				CPUCost: 0.0,
				RAMCost: 0.0,
				GPUCost: 0.0,
			}
		}

		hours := e.Sub(s).Hours()

		// NOTE: These theoretically shouldn't overlap due to the way the
		// NOTE: metrics are accumulated, so this logic is safe.
		if s.Before(nodeMap[key].Start) {
			nodeMap[key].Start = s
		}
		if e.After(nodeMap[key].End) {
			nodeMap[key].End = e
		}
		nodeMap[key].CPUCost += v.CPUCores * (v.CostPerCPUHr * hours)
		nodeMap[key].RAMCost += v.RAMGiB * (v.CostPerRAMGiBHr * hours)
		nodeMap[key].GPUCost += v.GPUCount * (v.CostPerGPUHr * hours)
	}
}

// extendedNodeQueryResults is a place holder data type for the incubating
// feature for extending the node details that can be returned with allocation
// data
type extendedNodeQueryResults struct {
	nodeCPUCoreResults  []*prom.QueryResult
	nodeRAMByteResults  []*prom.QueryResult
	nodeGPUCountResults []*prom.QueryResult
}

// queryExtendedNodeData makes additional prometheus queries for node data to append on
// the AllocationNodePricing struct.
func queryExtendedNodeData(ctx *prom.Context, start, end time.Time, durStr, resStr string) (*extendedNodeQueryResults, error) {
	queryNodeCPUCores := fmt.Sprintf(queryFmtNodeCPUCores, durStr, env.GetPromClusterLabel())
	resChQueryNodeCPUCores := ctx.QueryAtTime(queryNodeCPUCores, end)

	queryNodeRAMBytes := fmt.Sprintf(queryFmtNodeRAMBytes, durStr, env.GetPromClusterLabel())
	resChQueryNodeRAMBytes := ctx.QueryAtTime(queryNodeRAMBytes, end)

	queryNodeGPUCount := fmt.Sprintf(queryFmtNodeGPUCount, durStr, env.GetPromClusterLabel())
	resChQueryNodeGPUCount := ctx.QueryAtTime(queryNodeGPUCount, end)

	nodeCPUCoreResults, _ := resChQueryNodeCPUCores.Await()
	nodeRAMByteResults, _ := resChQueryNodeRAMBytes.Await()
	nodeGPUCountResults, _ := resChQueryNodeGPUCount.Await()

	return &extendedNodeQueryResults{
		nodeCPUCoreResults:  nodeCPUCoreResults,
		nodeRAMByteResults:  nodeRAMByteResults,
		nodeGPUCountResults: nodeGPUCountResults,
	}, nil
}

// applyExtendedNodeData is a place holder function for the incubating feature
// which appends additional node data to the given node map
func applyExtendedNodeData(nodeMap map[nodeKey]*nodePricing, results *extendedNodeQueryResults) {
	if results == nil {
		log.Warnf("Extended Node Results were nil. Ignoring...")
		return
	}

	applyNodeCPUCores(nodeMap, results.nodeCPUCoreResults)
	applyNodeRAMBytes(nodeMap, results.nodeRAMByteResults)
	applyNodeGPUCount(nodeMap, results.nodeGPUCountResults)
}

func applyNodeCPUCores(nodeMap map[nodeKey]*nodePricing, nodeCPUCoreResults []*prom.QueryResult) {
	for _, res := range nodeCPUCoreResults {
		cluster, err := res.GetString(env.GetPromClusterLabel())
		if err != nil {
			cluster = env.GetClusterID()
		}

		node, err := res.GetString("node")
		if err != nil {
			log.Warnf("CostModel.ComputeAllocation: Node CPU Cores query result missing field: %s", err)
			continue
		}

		key := newNodeKey(cluster, node)
		if _, ok := nodeMap[key]; !ok {
			log.Warnf("Unexpectedly found node key that doesn't exist: %s-%s", cluster, node)
			nodeMap[key] = &nodePricing{
				Name: node,
			}
		}

		nodeMap[key].CPUCores = res.Values[0].Value
	}
}

func applyNodeRAMBytes(nodeMap map[nodeKey]*nodePricing, nodeRAMByteResults []*prom.QueryResult) {
	for _, res := range nodeRAMByteResults {
		cluster, err := res.GetString(env.GetPromClusterLabel())
		if err != nil {
			cluster = env.GetClusterID()
		}

		node, err := res.GetString("node")
		if err != nil {
			log.Warnf("CostModel.ComputeAllocation: Node CPU Cores query result missing field: %s", err)
			continue
		}

		key := newNodeKey(cluster, node)
		if _, ok := nodeMap[key]; !ok {
			log.Warnf("Unexpectedly found node key that doesn't exist: %s-%s", cluster, node)
			nodeMap[key] = &nodePricing{
				Name: node,
			}
		}

		nodeMap[key].RAMGiB = res.Values[0].Value / 1024.0 / 1024.0 / 1024.0
	}
}

func applyNodeGPUCount(nodeMap map[nodeKey]*nodePricing, nodeGPUCountResults []*prom.QueryResult) {
	for _, res := range nodeGPUCountResults {
		cluster, err := res.GetString(env.GetPromClusterLabel())
		if err != nil {
			cluster = env.GetClusterID()
		}

		node, err := res.GetString("node")
		if err != nil {
			log.Warnf("CostModel.ComputeAllocation: Node CPU Cores query result missing field: %s", err)
			continue
		}

		key := newNodeKey(cluster, node)
		if _, ok := nodeMap[key]; !ok {
			log.Warnf("Unexpectedly found node key that doesn't exist: %s-%s", cluster, node)
			nodeMap[key] = &nodePricing{
				Name: node,
			}
		}

		nodeMap[key].GPUCount = res.Values[0].Value
	}
}

// nodePricing describes the resource costs associated with a given node,
// as well as the source of the information (e.g. prometheus, custom)
type nodePricing struct {
	Name            string
	NodeType        string
	ProviderID      string
	Preemptible     bool
	CPUCores        float64
	CostPerCPUHr    float64
	RAMGiB          float64
	CostPerRAMGiBHr float64
	GPUCount        float64
	CostPerGPUHr    float64
	Discount        float64
	Source          string
}
