package filterutil

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/clusters"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/mapper"
)

var assetCompiler = opencost.NewAssetMatchCompiler()

func TestAssetFiltersFromParamsV1(t *testing.T) {
	cases := []struct {
		name           string
		qp             map[string]string
		shouldMatch    []opencost.Asset
		shouldNotMatch []opencost.Asset
	}{
		{
			name: "empty",
			qp:   map[string]string{},
			shouldMatch: []opencost.Asset{
				&opencost.Node{},
				&opencost.Any{},
				&opencost.Cloud{},
				&opencost.LoadBalancer{},
				&opencost.ClusterManagement{},
				&opencost.Disk{},
				&opencost.Network{},
				&opencost.SharedAsset{},
			},
			shouldNotMatch: []opencost.Asset{},
		},
		{
			name: "type: node",
			qp: map[string]string{
				ParamFilterTypes: "node",
			},
			shouldMatch: []opencost.Asset{
				&opencost.Node{},
			},
			shouldNotMatch: []opencost.Asset{
				&opencost.Any{},
				&opencost.Cloud{},
				&opencost.LoadBalancer{},
				&opencost.ClusterManagement{},
				&opencost.Disk{},
				&opencost.Network{},
				&opencost.SharedAsset{},
			},
		},
		{
			name: "type: node capitalized",
			qp: map[string]string{
				ParamFilterTypes: "Node",
			},
			shouldMatch: []opencost.Asset{
				&opencost.Node{},
			},
			shouldNotMatch: []opencost.Asset{
				&opencost.Any{},
				&opencost.Cloud{},
				&opencost.LoadBalancer{},
				&opencost.ClusterManagement{},
				&opencost.Disk{},
				&opencost.Network{},
				&opencost.SharedAsset{},
			},
		},
		{
			name: "type: disk",
			qp: map[string]string{
				ParamFilterTypes: "disk",
			},
			shouldMatch: []opencost.Asset{
				&opencost.Disk{},
			},
			shouldNotMatch: []opencost.Asset{
				&opencost.Any{},
				&opencost.Cloud{},
				&opencost.Network{},
				&opencost.Node{},
				&opencost.LoadBalancer{},
				&opencost.ClusterManagement{},
				&opencost.SharedAsset{},
			},
		},
		{
			name: "type: loadbalancer",
			qp: map[string]string{
				ParamFilterTypes: "loadbalancer",
			},
			shouldMatch: []opencost.Asset{
				&opencost.LoadBalancer{},
			},
			shouldNotMatch: []opencost.Asset{
				&opencost.Any{},
				&opencost.Cloud{},
				&opencost.Node{},
				&opencost.ClusterManagement{},
				&opencost.Disk{},
				&opencost.Network{},
				&opencost.SharedAsset{},
			},
		},
		{
			name: "type: clustermanagement",
			qp: map[string]string{
				ParamFilterTypes: "clustermanagement",
			},
			shouldMatch: []opencost.Asset{
				&opencost.ClusterManagement{},
			},
			shouldNotMatch: []opencost.Asset{
				&opencost.Any{},
				&opencost.Cloud{},
				&opencost.LoadBalancer{},
				&opencost.Node{},
				&opencost.Disk{},
				&opencost.Network{},
				&opencost.SharedAsset{},
			},
		},
		{
			name: "type: network",
			qp: map[string]string{
				ParamFilterTypes: "network",
			},
			shouldMatch: []opencost.Asset{
				&opencost.Network{},
			},
			shouldNotMatch: []opencost.Asset{
				&opencost.Any{},
				&opencost.Cloud{},
				&opencost.LoadBalancer{},
				&opencost.ClusterManagement{},
				&opencost.Node{},
				&opencost.Disk{},
				&opencost.SharedAsset{},
			},
		},
		{
			name: "account",
			qp: map[string]string{
				ParamFilterAccounts: "foo,bar",
			},
			shouldMatch: []opencost.Asset{
				&opencost.Node{
					Properties: &opencost.AssetProperties{
						Account: "foo",
					},
				},
				&opencost.Network{
					Properties: &opencost.AssetProperties{
						Account: "bar",
					},
				},
			},
			shouldNotMatch: []opencost.Asset{
				&opencost.Network{
					Properties: &opencost.AssetProperties{
						Account: "baz",
					},
				},
			},
		},
		{
			name: "category",
			qp: map[string]string{
				ParamFilterCategories: "Network,Compute",
			},
			shouldMatch: []opencost.Asset{
				&opencost.Network{
					Properties: &opencost.AssetProperties{
						Category: opencost.NetworkCategory,
					},
				},
				&opencost.Node{
					Properties: &opencost.AssetProperties{
						Category: opencost.ComputeCategory,
					},
				},
			},
			shouldNotMatch: []opencost.Asset{
				&opencost.ClusterManagement{
					Properties: &opencost.AssetProperties{
						Category: opencost.ManagementCategory,
					},
				},
			},
		},
		{
			name: "cluster",
			qp: map[string]string{
				ParamFilterClusters: "cluster-one",
			},
			shouldMatch: []opencost.Asset{
				&opencost.LoadBalancer{
					Properties: &opencost.AssetProperties{
						Cluster: "cluster-one",
					},
				},
				&opencost.Node{
					Properties: &opencost.AssetProperties{
						Cluster: "cluster-one",
					},
				},
			},
			shouldNotMatch: []opencost.Asset{
				&opencost.ClusterManagement{
					Properties: &opencost.AssetProperties{
						Cluster: "cluster-two",
					},
				},
			},
		},
		{
			name: "project",
			qp: map[string]string{
				ParamFilterProjects: "proj1,proj2",
			},
			shouldMatch: []opencost.Asset{
				&opencost.Disk{
					Properties: &opencost.AssetProperties{
						Project: "proj1",
					},
				},
				&opencost.Node{
					Properties: &opencost.AssetProperties{
						Project: "proj2",
					},
				},
			},
			shouldNotMatch: []opencost.Asset{
				&opencost.ClusterManagement{
					Properties: &opencost.AssetProperties{
						Project: "proj3",
					},
				},
			},
		},
		{
			name: "provider",
			qp: map[string]string{
				ParamFilterProviders: "p1,p2",
			},
			shouldMatch: []opencost.Asset{
				&opencost.Disk{
					Properties: &opencost.AssetProperties{
						Provider: "p1",
					},
				},
				&opencost.Network{
					Properties: &opencost.AssetProperties{
						Provider: "p2",
					},
				},
			},
			shouldNotMatch: []opencost.Asset{
				&opencost.Node{
					Properties: &opencost.AssetProperties{
						Provider: "p3",
					},
				},
			},
		},
		{
			name: "providerID v1",
			qp: map[string]string{
				ParamFilterProviderIDs: "p1,p2",
			},
			shouldMatch: []opencost.Asset{
				&opencost.Disk{
					Properties: &opencost.AssetProperties{
						ProviderID: "p1",
					},
				},
				&opencost.Network{
					Properties: &opencost.AssetProperties{
						ProviderID: "p2",
					},
				},
			},
			shouldNotMatch: []opencost.Asset{
				&opencost.Node{
					Properties: &opencost.AssetProperties{
						ProviderID: "p3",
					},
				},
			},
		},
		{
			name: "providerID v2",
			qp: map[string]string{
				ParamFilterProviderIDsV2: "p1,p2",
			},
			shouldMatch: []opencost.Asset{
				&opencost.Disk{
					Properties: &opencost.AssetProperties{
						ProviderID: "p1",
					},
				},
				&opencost.Network{
					Properties: &opencost.AssetProperties{
						ProviderID: "p2",
					},
				},
			},
			shouldNotMatch: []opencost.Asset{
				&opencost.Node{
					Properties: &opencost.AssetProperties{
						ProviderID: "p3",
					},
				},
			},
		},
		{
			name: "service",
			qp: map[string]string{
				ParamFilterServices: "p1,p2",
			},
			shouldMatch: []opencost.Asset{
				&opencost.Disk{
					Properties: &opencost.AssetProperties{
						Service: "p1",
					},
				},
				&opencost.Network{
					Properties: &opencost.AssetProperties{
						Service: "p2",
					},
				},
			},
			shouldNotMatch: []opencost.Asset{
				&opencost.Node{
					Properties: &opencost.AssetProperties{
						Service: "p3",
					},
				},
			},
		},
		{
			name: "label",
			qp: map[string]string{
				ParamFilterLabels: "foo:bar,baz:qux",
			},
			shouldMatch: []opencost.Asset{
				&opencost.Disk{
					Labels: opencost.AssetLabels{
						"foo": "bar",
						"baz": "other",
					},
				},
				&opencost.Node{
					Labels: opencost.AssetLabels{
						"baz": "qux",
					},
				},
			},
			shouldNotMatch: []opencost.Asset{
				&opencost.ClusterManagement{
					Labels: opencost.AssetLabels{
						"baz": "other",
					},
				},
			},
		},
		{
			name: "region",
			qp: map[string]string{
				ParamFilterRegions: "r1,r2",
			},
			shouldMatch: []opencost.Asset{
				&opencost.Node{
					Labels: opencost.AssetLabels{
						"label_topology_kubernetes_io_region": "r1",
					},
				},
				&opencost.Node{
					Labels: opencost.AssetLabels{
						"label_topology_kubernetes_io_region": "r2",
					},
				},
			},
			shouldNotMatch: []opencost.Asset{
				&opencost.Node{
					Labels: opencost.AssetLabels{
						"label_topology_kubernetes_io_region": "r3",
					},
				},
			},
		},
		{
			name: "complex",
			qp: map[string]string{
				ParamFilterRegions:  "r1,r2",
				ParamFilterTypes:    "node",
				ParamFilterAccounts: "a*",
			},
			shouldMatch: []opencost.Asset{
				&opencost.Node{
					Labels: opencost.AssetLabels{
						"label_topology_kubernetes_io_region": "r1",
					},
					Properties: &opencost.AssetProperties{
						Account: "a1",
					},
				},
				&opencost.Node{
					Labels: opencost.AssetLabels{
						"label_topology_kubernetes_io_region": "r2",
					},
					Properties: &opencost.AssetProperties{
						Account: "a2",
					},
				},
			},
			shouldNotMatch: []opencost.Asset{
				&opencost.Node{
					Properties: &opencost.AssetProperties{
						Account: "b1",
					},
				},
				&opencost.Node{
					Properties: &opencost.AssetProperties{
						Account: "3a",
					},
				},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// Convert map[string]string representation to the mapper
			// library type
			qpMap := mapper.NewMap()
			for k, v := range c.qp {
				qpMap.Set(k, v)
			}
			qpMapper := mapper.NewMapper(qpMap)

			clustersMap := mockClusterMap{
				m: map[string]*clusters.ClusterInfo{
					"mapped-cluster-ID-1": {
						ID:   "mapped-cluster-ID-ABC",
						Name: "cluster ABC",
					},
				},
			}

			filterTree := AssetFilterFromParamsV1(qpMapper, clustersMap)
			filter, err := assetCompiler.Compile(filterTree)
			if err != nil {
				t.Fatalf("compiling filter: %s", err)
			}
			for _, asset := range c.shouldMatch {
				if !filter.Matches(asset) {
					t.Errorf("should have matched: %s", asset.String())
				}
			}
			for _, asset := range c.shouldNotMatch {
				if filter.Matches(asset) {
					t.Errorf("incorrectly matched: %s", asset.String())
				}
			}
		})
	}
}
