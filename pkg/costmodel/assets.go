package costmodel

import (
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
)

func (cm *CostModel) ComputeAssets(start, end time.Time) (*opencost.AssetSet, error) {
	assetSet := opencost.NewAssetSet(start, end)

	nodeMap, err := cm.ClusterNodes(start, end)
	if err != nil {
		return nil, fmt.Errorf("error computing node assets for %s: %w", opencost.NewClosedWindow(start, end), err)

	}

	lbMap, err := cm.ClusterLoadBalancers(start, end)
	if err != nil {
		return nil, fmt.Errorf("error computing load balancer assets for %s: %w", opencost.NewClosedWindow(start, end), err)
	}

	diskMap, err := cm.ClusterDisks(start, end)
	if err != nil {
		return nil, fmt.Errorf("error computing disk assets for %s: %w", opencost.NewClosedWindow(start, end), err)
	}

	for _, d := range diskMap {
		s := d.Start
		if s.Before(start) || s.After(end) {
			log.Debugf("CostModel.ComputeAssets: disk '%s' start outside window: %s not in [%s, %s]", d.Name, s.Format("2006-01-02T15:04:05"), start.Format("2006-01-02T15:04:05"), end.Format("2006-01-02T15:04:05"))
			s = start
		}

		e := d.End
		if e.Before(start) || e.After(end) {
			log.Debugf("CostModel.ComputeAssets: disk '%s' end outside window: %s not in [%s, %s]", d.Name, e.Format("2006-01-02T15:04:05"), start.Format("2006-01-02T15:04:05"), end.Format("2006-01-02T15:04:05"))
			e = end
		}

		hours := e.Sub(s).Hours()

		disk := opencost.NewDisk(d.Name, d.Cluster, d.ProviderID, s, e, opencost.NewWindow(&start, &end))
		cm.PropertiesFromCluster(disk.Properties)
		disk.Cost = d.Cost
		disk.ByteHours = d.Bytes * hours
		if d.BytesUsedAvgPtr != nil {
			byteHours := *d.BytesUsedAvgPtr * hours
			disk.ByteHoursUsed = &byteHours
		}
		if d.BytesUsedMaxPtr != nil {
			usageMax := *d.BytesUsedMaxPtr
			disk.ByteUsageMax = &usageMax
		}

		if d.Local {
			disk.Local = 1.0
		}
		disk.Breakdown = &opencost.Breakdown{
			Idle:   d.Breakdown.Idle,
			System: d.Breakdown.System,
			User:   d.Breakdown.User,
			Other:  d.Breakdown.Other,
		}
		disk.StorageClass = d.StorageClass
		disk.VolumeName = d.VolumeName
		disk.ClaimName = d.ClaimName
		disk.ClaimNamespace = d.ClaimNamespace
		assetSet.Insert(disk, nil)
	}

	for _, lb := range lbMap {
		s := lb.Start
		if s.Before(start) || s.After(end) {
			log.Debugf("CostModel.ComputeAssets: load balancer '%s' start outside window: %s not in [%s, %s]", lb.Name, s.Format("2006-01-02T15:04:05"), start.Format("2006-01-02T15:04:05"), end.Format("2006-01-02T15:04:05"))
			s = start
		}

		e := lb.End
		if e.Before(start) || e.After(end) {
			log.Debugf("CostModel.ComputeAssets: load balancer '%s' end outside window: %s not in [%s, %s]", lb.Name, e.Format("2006-01-02T15:04:05"), start.Format("2006-01-02T15:04:05"), end.Format("2006-01-02T15:04:05"))
			e = end
		}

		loadBalancer := opencost.NewLoadBalancer(lb.Name, lb.Cluster, lb.ProviderID, s, e, opencost.NewWindow(&start, &end), lb.Private, lb.Ip)
		cm.PropertiesFromCluster(loadBalancer.Properties)
		loadBalancer.Cost = lb.Cost
		assetSet.Insert(loadBalancer, nil)
	}

	for _, n := range nodeMap {
		// check label, to see if node from fargate, if so ignore.
		if n.Labels != nil {
			if value, ok := n.Labels["label_eks_amazonaws_com_compute_type"]; ok && value == "fargate" {
				continue
			}
		}
		s := n.Start
		if s.Before(start) || s.After(end) {
			log.Debugf("CostModel.ComputeAssets: node '%s' start outside window: %s not in [%s, %s]", n.Name, s.Format("2006-01-02T15:04:05"), start.Format("2006-01-02T15:04:05"), end.Format("2006-01-02T15:04:05"))
			s = start
		}

		e := n.End
		if e.Before(start) || e.After(end) {
			log.Debugf("CostModel.ComputeAssets: node '%s' end outside window: %s not in [%s, %s]", n.Name, e.Format("2006-01-02T15:04:05"), start.Format("2006-01-02T15:04:05"), end.Format("2006-01-02T15:04:05"))
			e = end
		}

		hours := e.Sub(s).Hours()

		node := opencost.NewNode(n.Name, n.Cluster, n.ProviderID, s, e, opencost.NewWindow(&start, &end))
		cm.PropertiesFromCluster(node.Properties)
		node.NodeType = n.NodeType
		node.CPUCoreHours = n.CPUCores * hours
		node.RAMByteHours = n.RAMBytes * hours
		node.GPUHours = n.GPUCount * hours
		node.CPUBreakdown = &opencost.Breakdown{
			Idle:   n.CPUBreakdown.Idle,
			System: n.CPUBreakdown.System,
			User:   n.CPUBreakdown.User,
			Other:  n.CPUBreakdown.Other,
		}
		node.RAMBreakdown = &opencost.Breakdown{
			Idle:   n.RAMBreakdown.Idle,
			System: n.RAMBreakdown.System,
			User:   n.RAMBreakdown.User,
			Other:  n.RAMBreakdown.Other,
		}
		node.CPUCost = n.CPUCost
		node.GPUCost = n.GPUCost
		node.GPUCount = n.GPUCount
		node.RAMCost = n.RAMCost

		if n.Overhead != nil {
			node.Overhead = &opencost.NodeOverhead{
				RamOverheadFraction: n.Overhead.RamOverheadFraction,
				CpuOverheadFraction: n.Overhead.CpuOverheadFraction,
				OverheadCostFraction: ((n.Overhead.CpuOverheadFraction * n.CPUCost) +
					(n.Overhead.RamOverheadFraction * n.RAMCost)) / node.TotalCost(),
			}
		} else {
			node.Overhead = &opencost.NodeOverhead{}
		}
		node.Discount = n.Discount
		if n.Preemptible {
			node.Preemptible = 1.0
		}
		node.SetLabels(opencost.AssetLabels(n.Labels))
		assetSet.Insert(node, nil)
	}

	return assetSet, nil
}

func (cm *CostModel) ClusterDisks(start, end time.Time) (map[DiskIdentifier]*Disk, error) {
	return ClusterDisks(cm.PrometheusClient, cm.Provider, start, end)
}

func (cm *CostModel) ClusterLoadBalancers(start, end time.Time) (map[LoadBalancerIdentifier]*LoadBalancer, error) {
	return ClusterLoadBalancers(cm.PrometheusClient, start, end)
}

func (cm *CostModel) ClusterNodes(start, end time.Time) (map[NodeIdentifier]*Node, error) {
	return ClusterNodes(cm.Provider, cm.PrometheusClient, start, end)
}

// propertiesFromCluster populates static cluster properties to individual asset properties
func (cm *CostModel) PropertiesFromCluster(props *opencost.AssetProperties) {
	// If properties does not have cluster value, do nothing
	if props.Cluster == "" {
		return
	}

	clusterMap := cm.ClusterMap.AsMap()
	ci, ok := clusterMap[props.Cluster]
	if !ok {
		log.Debugf("CostMode.PropertiesFromCluster: cluster '%s' was not found in ClusterMap", props.Cluster)
		return
	}

	props.Project = ci.Project
	props.Account = ci.Account
	props.Provider = ci.Provider
}
