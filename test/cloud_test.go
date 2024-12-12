package test

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/clusters"

	"github.com/opencost/opencost/pkg/cloud/provider"
	"github.com/opencost/opencost/pkg/clustercache"
	"github.com/opencost/opencost/pkg/config"
	"github.com/opencost/opencost/pkg/costmodel"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	providerIDMap  = "spec.providerID"
	nameMap        = "metadata.name"
	labelMapFoo    = "metadata.labels.foo"
	labelMapFooBar = "metadata.labels.foo.bar"
)

func TestRegionValueFromMapField(t *testing.T) {
	wantRegion := "useast"
	wantpid := strings.ToLower("/subscriptions/0bd50fdf-c923-4e1e-850c-196dd3dcc5d3/resourceGroups/MC_test_test_eastus/providers/Microsoft.Compute/virtualMachines/aks-agentpool-20139558-0")
	providerIDWant := wantRegion + "," + wantpid

	n := &clustercache.Node{}
	n.SpecProviderID = "azure:///subscriptions/0bd50fdf-c923-4e1e-850c-196dd3dcc5d3/resourceGroups/MC_test_test_eastus/providers/Microsoft.Compute/virtualMachines/aks-agentpool-20139558-0"
	n.Labels = make(map[string]string)
	n.Labels[v1.LabelTopologyRegion] = wantRegion
	got := provider.NodeValueFromMapField(providerIDMap, n, true)
	if got != providerIDWant {
		t.Errorf("Assert on '%s' want '%s' got '%s'", providerIDMap, providerIDWant, got)
	}

}
func TestTransformedValueFromMapField(t *testing.T) {
	providerIDWant := "i-05445591e0d182d42"
	n := &clustercache.Node{}
	n.SpecProviderID = "aws:///us-east-1a/i-05445591e0d182d42"
	got := provider.NodeValueFromMapField(providerIDMap, n, false)
	if got != providerIDWant {
		t.Errorf("Assert on '%s' want '%s' got '%s'", providerIDMap, providerIDWant, got)
	}

	providerIDWant2 := strings.ToLower("/subscriptions/0bd50fdf-c923-4e1e-850c-196dd3dcc5d3/resourceGroups/MC_test_test_eastus/providers/Microsoft.Compute/virtualMachines/aks-agentpool-20139558-0")
	n2 := &clustercache.Node{}
	n2.SpecProviderID = "azure:///subscriptions/0bd50fdf-c923-4e1e-850c-196dd3dcc5d3/resourceGroups/MC_test_test_eastus/providers/Microsoft.Compute/virtualMachines/aks-agentpool-20139558-0"
	got2 := provider.NodeValueFromMapField(providerIDMap, n2, false)
	if got2 != providerIDWant2 {
		t.Errorf("Assert on '%s' want '%s' got '%s'", providerIDMap, providerIDWant2, got2)
	}

	providerIDWant3 := strings.ToLower("/subscriptions/0bd50fdf-c923-4e1e-850c-196dd3dcc5d3/resourceGroups/mc_testspot_testspot_eastus/providers/Microsoft.Compute/virtualMachineScaleSets/aks-nodepool1-19213364-vmss/virtualMachines/0")
	n3 := &clustercache.Node{}
	n3.SpecProviderID = "azure:///subscriptions/0bd50fdf-c923-4e1e-850c-196dd3dcc5d3/resourceGroups/mc_testspot_testspot_eastus/providers/Microsoft.Compute/virtualMachineScaleSets/aks-nodepool1-19213364-vmss/virtualMachines/0"
	got3 := provider.NodeValueFromMapField(providerIDMap, n3, false)
	if got3 != providerIDWant3 {
		t.Errorf("Assert on '%s' want '%s' got '%s'", providerIDMap, providerIDWant3, got3)
	}
}

func TestNodeValueFromMapField(t *testing.T) {
	providerIDWant := "providerid"
	nameWant := "gke-standard-cluster-1-pool-1-91dc432d-cg69"
	labelFooWant := "labelfoo"

	n := &clustercache.Node{}
	n.SpecProviderID = providerIDWant
	n.Name = nameWant
	n.Labels = make(map[string]string)
	n.Labels["foo"] = labelFooWant

	got := provider.NodeValueFromMapField(providerIDMap, n, false)
	if got != providerIDWant {
		t.Errorf("Assert on '%s' want '%s' got '%s'", providerIDMap, providerIDWant, got)
	}

	got = provider.NodeValueFromMapField(nameMap, n, false)
	if got != nameWant {
		t.Errorf("Assert on '%s' want '%s' got '%s'", nameMap, nameWant, got)
	}

	got = provider.NodeValueFromMapField(labelMapFoo, n, false)
	if got != labelFooWant {
		t.Errorf("Assert on '%s' want '%s' got '%s'", labelMapFoo, labelFooWant, got)
	}

}

func TestPVPriceFromCSV(t *testing.T) {
	nameWant := "pvc-08e1f205-d7a9-4430-90fc-7b3965a18c4d"
	pv := &clustercache.PersistentVolume{}
	pv.Name = nameWant

	confMan := config.NewConfigFileManager(&config.ConfigFileManagerOpts{
		LocalConfigPath: "./",
	})

	wantPrice := "0.1337"
	c := &provider.CSVProvider{
		CSVLocation: "../configs/pricing_schema_pv.csv",
		CustomProvider: &provider.CustomProvider{
			Config: provider.NewProviderConfig(confMan, "../configs/default.json"),
		},
	}
	c.DownloadPricingData()
	k := c.GetPVKey(pv, make(map[string]string), "")
	resPV, err := c.PVPricing(k)
	if err != nil {
		t.Errorf("Error in NodePricing: %s", err.Error())
	} else {
		gotPrice := resPV.Cost
		wantPriceFloat, _ := strconv.ParseFloat(wantPrice, 64)
		gotPriceFloat, _ := strconv.ParseFloat(gotPrice, 64)
		if gotPriceFloat != wantPriceFloat {
			t.Errorf("Wanted price '%f' got price '%f'", wantPriceFloat, gotPriceFloat)
		}
	}

}

func TestPVPriceFromCSVStorageClass(t *testing.T) {
	nameWant := "pvc-08e1f205-d7a9-4430-90fc-7b3965a18c4d"
	storageClassWant := "storageclass0"
	pv := &clustercache.PersistentVolume{}
	pv.Name = nameWant
	pv.Spec.StorageClassName = storageClassWant

	confMan := config.NewConfigFileManager(&config.ConfigFileManagerOpts{
		LocalConfigPath: "./",
	})

	wantPrice := "0.1338"
	c := &provider.CSVProvider{
		CSVLocation: "../configs/pricing_schema_pv_storageclass.csv",
		CustomProvider: &provider.CustomProvider{
			Config: provider.NewProviderConfig(confMan, "../configs/default.json"),
		},
	}
	c.DownloadPricingData()
	k := c.GetPVKey(pv, make(map[string]string), "")
	resPV, err := c.PVPricing(k)
	if err != nil {
		t.Errorf("Error in NodePricing: %s", err.Error())
	} else {
		gotPrice := resPV.Cost
		wantPriceFloat, _ := strconv.ParseFloat(wantPrice, 64)
		gotPriceFloat, _ := strconv.ParseFloat(gotPrice, 64)
		if gotPriceFloat != wantPriceFloat {
			t.Errorf("Wanted price '%f' got price '%f'", wantPriceFloat, gotPriceFloat)
		}
	}

}

func TestNodePriceFromCSVWithGPU(t *testing.T) {
	providerIDWant := "providerid"
	nameWant := "gke-standard-cluster-1-pool-1-91dc432d-cg69"
	labelFooWant := "labelfoo"
	wantGPU := "2"

	confMan := config.NewConfigFileManager(&config.ConfigFileManagerOpts{
		LocalConfigPath: "./",
	})

	n := &clustercache.Node{}
	n.SpecProviderID = providerIDWant
	n.Name = nameWant
	n.Labels = make(map[string]string)
	n.Labels["foo"] = labelFooWant
	n.Labels["nvidia.com/gpu_type"] = "Quadro_RTX_4000"
	n.Status.Capacity = v1.ResourceList{"nvidia.com/gpu": *resource.NewScaledQuantity(2, 0)}
	wantPrice := "1.633700"

	n2 := &clustercache.Node{}
	n2.SpecProviderID = providerIDWant
	n2.Name = nameWant
	n2.Labels = make(map[string]string)
	n2.Labels["foo"] = labelFooWant
	n2.Labels["gpu.nvidia.com/class"] = "Quadro_RTX_4001"
	n2.Status.Capacity = v1.ResourceList{"nvidia.com/gpu": *resource.NewScaledQuantity(2, 0)}
	wantPrice2 := "1.733700"

	c := &provider.CSVProvider{
		CSVLocation: "../configs/pricing_schema.csv",
		CustomProvider: &provider.CustomProvider{
			Config: provider.NewProviderConfig(confMan, "../configs/default.json"),
		},
	}

	c.DownloadPricingData()
	k := c.GetKey(n.Labels, n)
	resN, _, err := c.NodePricing(k)
	if err != nil {
		t.Errorf("Error in NodePricing: %s", err.Error())
	} else {
		gotGPU := resN.GPU
		gotPrice := resN.Cost
		if gotGPU != wantGPU {
			t.Errorf("Wanted gpu count '%s' got gpu count '%s'", wantGPU, gotGPU)
		}
		wantPriceFloat, _ := strconv.ParseFloat(wantPrice, 64)
		gotPriceFloat, _ := strconv.ParseFloat(gotPrice, 64)
		if gotPriceFloat != wantPriceFloat {
			t.Errorf("Wanted price '%f' got price '%f'", wantPriceFloat, gotPriceFloat)
		}

	}

	k2 := c.GetKey(n2.Labels, n2)
	resN2, _, err := c.NodePricing(k2)
	if err != nil {
		t.Errorf("Error in NodePricing: %s", err.Error())
	} else {
		gotGPU := resN2.GPU
		gotPrice := resN2.Cost
		if gotGPU != wantGPU {
			t.Errorf("Wanted gpu count '%s' got gpu count '%s'", wantGPU, gotGPU)
		}
		wantPriceFloat, _ := strconv.ParseFloat(wantPrice2, 64)
		gotPriceFloat, _ := strconv.ParseFloat(gotPrice, 64)
		if gotPriceFloat != wantPriceFloat {
			t.Errorf("Wanted price '%f' got price '%f'", wantPriceFloat, gotPriceFloat)
		}

	}

}

func TestNodePriceFromCSVSpecialChar(t *testing.T) {
	nameWant := "gke-standard-cluster-1-pool-1-91dc432d-cg69"

	confMan := config.NewConfigFileManager(&config.ConfigFileManagerOpts{
		LocalConfigPath: "./",
	})

	n := &clustercache.Node{}
	n.Name = nameWant
	n.Labels = make(map[string]string)
	n.Labels["<http://metadata.label.servers.com/label|metadata.label.servers.com/label>"] = nameWant

	wantPrice := "0.133700"

	c := &provider.CSVProvider{
		CSVLocation: "../configs/pricing_schema_special_char.csv",
		CustomProvider: &provider.CustomProvider{
			Config: provider.NewProviderConfig(confMan, "../configs/default.json"),
		},
	}
	c.DownloadPricingData()
	k := c.GetKey(n.Labels, n)
	resN, _, err := c.NodePricing(k)
	if err != nil {
		t.Errorf("Error in NodePricing: %s", err.Error())
	} else {
		gotPrice := resN.Cost
		wantPriceFloat, _ := strconv.ParseFloat(wantPrice, 64)
		gotPriceFloat, _ := strconv.ParseFloat(gotPrice, 64)
		if gotPriceFloat != wantPriceFloat {
			t.Errorf("Wanted price '%f' got price '%f'", wantPriceFloat, gotPriceFloat)
		}
	}
}

func TestNodePriceFromCSV(t *testing.T) {
	providerIDWant := "providerid"
	nameWant := "gke-standard-cluster-1-pool-1-91dc432d-cg69"
	labelFooWant := "labelfoo"

	confMan := config.NewConfigFileManager(&config.ConfigFileManagerOpts{
		LocalConfigPath: "./",
	})

	n := &clustercache.Node{}
	n.SpecProviderID = providerIDWant
	n.Name = nameWant
	n.Labels = make(map[string]string)
	n.Labels["foo"] = labelFooWant

	wantPrice := "0.133700"

	c := &provider.CSVProvider{
		CSVLocation: "../configs/pricing_schema.csv",
		CustomProvider: &provider.CustomProvider{
			Config: provider.NewProviderConfig(confMan, "../configs/default.json"),
		},
	}
	c.DownloadPricingData()
	k := c.GetKey(n.Labels, n)
	resN, _, err := c.NodePricing(k)
	if err != nil {
		t.Errorf("Error in NodePricing: %s", err.Error())
	} else {
		gotPrice := resN.Cost
		wantPriceFloat, _ := strconv.ParseFloat(wantPrice, 64)
		gotPriceFloat, _ := strconv.ParseFloat(gotPrice, 64)
		if gotPriceFloat != wantPriceFloat {
			t.Errorf("Wanted price '%f' got price '%f'", wantPriceFloat, gotPriceFloat)
		}
	}

	unknownN := &clustercache.Node{}
	unknownN.SpecProviderID = providerIDWant
	unknownN.Name = "unknownname"
	unknownN.Labels = make(map[string]string)
	unknownN.Labels["foo"] = labelFooWant
	unknownN.Labels[v1.LabelTopologyRegion] = "fakeregion"
	k2 := c.GetKey(unknownN.Labels, unknownN)
	resN2, _, _ := c.NodePricing(k2)
	if resN2 != nil {
		t.Errorf("CSV provider should return nil on missing node")
	}

	c2 := &provider.CSVProvider{
		CSVLocation: "../configs/fake.csv",
		CustomProvider: &provider.CustomProvider{
			Config: provider.NewProviderConfig(confMan, "../configs/default.json"),
		},
	}
	k3 := c.GetKey(n.Labels, n)
	resN3, _, _ := c2.NodePricing(k3)
	if resN3 != nil {
		t.Errorf("CSV provider should return nil on missing csv")
	}
}

func TestNodePriceFromCSVWithRegion(t *testing.T) {
	providerIDWant := "gke-standard-cluster-1-pool-1-91dc432d-cg69"
	nameWant := "foo"
	labelFooWant := "labelfoo"

	confMan := config.NewConfigFileManager(&config.ConfigFileManagerOpts{
		LocalConfigPath: "./",
	})

	n := &clustercache.Node{}
	n.SpecProviderID = providerIDWant
	n.Name = nameWant
	n.Labels = make(map[string]string)
	n.Labels["foo"] = labelFooWant
	n.Labels[v1.LabelTopologyRegion] = "regionone"
	wantPrice := "0.133700"

	n2 := &clustercache.Node{}
	n2.SpecProviderID = providerIDWant
	n2.Name = nameWant
	n2.Labels = make(map[string]string)
	n2.Labels["foo"] = labelFooWant
	n2.Labels[v1.LabelTopologyRegion] = "regiontwo"
	wantPrice2 := "0.133800"

	n3 := &clustercache.Node{}
	n3.SpecProviderID = providerIDWant
	n3.Name = nameWant
	n3.Labels = make(map[string]string)
	n3.Labels["foo"] = labelFooWant
	n3.Labels[v1.LabelTopologyRegion] = "fakeregion"
	wantPrice3 := "0.1339"

	c := &provider.CSVProvider{
		CSVLocation: "../configs/pricing_schema_region.csv",
		CustomProvider: &provider.CustomProvider{
			Config: provider.NewProviderConfig(confMan, "../configs/default.json"),
		},
	}
	c.DownloadPricingData()
	k := c.GetKey(n.Labels, n)
	resN, _, err := c.NodePricing(k)
	if err != nil {
		t.Errorf("Error in NodePricing: %s", err.Error())
	} else {
		gotPrice := resN.Cost
		wantPriceFloat, _ := strconv.ParseFloat(wantPrice, 64)
		gotPriceFloat, _ := strconv.ParseFloat(gotPrice, 64)
		if gotPriceFloat != wantPriceFloat {
			t.Errorf("Wanted price '%f' got price '%f'", wantPriceFloat, gotPriceFloat)
		}
	}
	k2 := c.GetKey(n2.Labels, n2)
	resN2, _, err := c.NodePricing(k2)
	if err != nil {
		t.Errorf("Error in NodePricing: %s", err.Error())
	} else {
		gotPrice := resN2.Cost
		wantPriceFloat, _ := strconv.ParseFloat(wantPrice2, 64)
		gotPriceFloat, _ := strconv.ParseFloat(gotPrice, 64)
		if gotPriceFloat != wantPriceFloat {
			t.Errorf("Wanted price '%f' got price '%f'", wantPriceFloat, gotPriceFloat)
		}
	}
	k3 := c.GetKey(n3.Labels, n3)
	resN3, _, err := c.NodePricing(k3)
	if err != nil {
		t.Errorf("Error in NodePricing: %s", err.Error())
	} else {
		gotPrice := resN3.Cost
		wantPriceFloat, _ := strconv.ParseFloat(wantPrice3, 64)
		gotPriceFloat, _ := strconv.ParseFloat(gotPrice, 64)
		if gotPriceFloat != wantPriceFloat {
			t.Errorf("Wanted price '%f' got price '%f'", wantPriceFloat, gotPriceFloat)
		}
	}

	unknownN := &clustercache.Node{}
	unknownN.SpecProviderID = "fake providerID"
	unknownN.Name = "unknownname"
	unknownN.Labels = make(map[string]string)
	unknownN.Labels[v1.LabelTopologyRegion] = "fakeregion"
	unknownN.Labels["foo"] = labelFooWant
	k4 := c.GetKey(unknownN.Labels, unknownN)
	resN4, _, _ := c.NodePricing(k4)
	if resN4 != nil {
		t.Errorf("CSV provider should return nil on missing node, instead returned %+v", resN4)
	}

	c2 := &provider.CSVProvider{
		CSVLocation: "../configs/fake.csv",
		CustomProvider: &provider.CustomProvider{
			Config: provider.NewProviderConfig(confMan, "../configs/default.json"),
		},
	}
	k5 := c.GetKey(n.Labels, n)
	resN5, _, _ := c2.NodePricing(k5)
	if resN5 != nil {
		t.Errorf("CSV provider should return nil on missing csv")
	}
}

type FakeCache struct {
	nodes []*clustercache.Node
	clustercache.ClusterCache
}

func (f FakeCache) GetAllNodes() []*clustercache.Node {
	return f.nodes
}

func (f FakeCache) GetAllDaemonSets() []*clustercache.DaemonSet {
	return nil
}

func NewFakeNodeCache(nodes []*clustercache.Node) FakeCache {
	return FakeCache{
		nodes: nodes,
	}
}

type FakeClusterMap struct {
	clusters.ClusterMap
}

func TestNodePriceFromCSVWithBadConfig(t *testing.T) {
	os.Setenv("CONFIG_PATH", "../config")
	confMan := config.NewConfigFileManager(&config.ConfigFileManagerOpts{
		LocalConfigPath: "./",
	})

	c := &provider.CSVProvider{
		CSVLocation: "../configs/pricing_schema_case.csv",
		CustomProvider: &provider.CustomProvider{
			Config: provider.NewProviderConfig(confMan, "invalid.json"),
		},
	}
	c.DownloadPricingData()

	n := &clustercache.Node{}
	n.SpecProviderID = "fake"
	n.Name = "nameWant"
	n.Labels = make(map[string]string)
	n.Labels["foo"] = "labelFooWant"
	n.Labels[v1.LabelTopologyRegion] = "regionone"

	fc := NewFakeNodeCache([]*clustercache.Node{n})
	fm := FakeClusterMap{}
	d, _ := time.ParseDuration("1m")

	model := costmodel.NewCostModel(nil, nil, fc, fm, d)

	_, err := model.GetNodeCost(c)
	if err != nil {
		t.Errorf("Error in node pricing: %s", err)
	}
}

func TestSourceMatchesFromCSV(t *testing.T) {
	os.Setenv("CONFIG_PATH", "../configs")

	confMan := config.NewConfigFileManager(&config.ConfigFileManagerOpts{
		LocalConfigPath: "./",
	})

	c := &provider.CSVProvider{
		CSVLocation: "../configs/pricing_schema_case.csv",
		CustomProvider: &provider.CustomProvider{
			Config: provider.NewProviderConfig(confMan, "/default.json"),
		},
	}
	c.DownloadPricingData()

	n := &clustercache.Node{}
	n.SpecProviderID = "fake"
	n.Name = "nameWant"
	n.Labels = make(map[string]string)
	n.Labels["foo"] = "labelFooWant"
	n.Labels[v1.LabelTopologyRegion] = "regionone"

	n2 := &clustercache.Node{}
	n2.SpecProviderID = "azure:///subscriptions/123a7sd-asd-1234-578a9-123abcdef/resourceGroups/case_12_STaGe_TeSt7/providers/Microsoft.Compute/virtualMachineScaleSets/vmss-agent-worker0-12stagetest7-ezggnore/virtualMachines/7"
	n2.Labels = make(map[string]string)
	n2.Labels[v1.LabelTopologyRegion] = "eastus2"
	n2.Labels["foo"] = "labelFooWant"

	k := c.GetKey(n2.Labels, n2)
	resN, _, err := c.NodePricing(k)
	if err != nil {
		t.Errorf("Error in NodePricing: %s", err.Error())
	} else {
		wantPrice := "0.13370357"
		gotPrice := resN.Cost
		if gotPrice != wantPrice {
			t.Errorf("Wanted price '%s' got price '%s'", wantPrice, gotPrice)
		}
	}

	n3 := &clustercache.Node{}
	n3.SpecProviderID = "fake"
	n3.Name = "nameWant"
	n3.Labels = make(map[string]string)
	n3.Labels[v1.LabelTopologyRegion] = "eastus2"
	n3.Labels[v1.LabelInstanceTypeStable] = "Standard_F32s_v2"

	fc := NewFakeNodeCache([]*clustercache.Node{n, n2, n3})
	fm := FakeClusterMap{}
	d, _ := time.ParseDuration("1m")

	model := costmodel.NewCostModel(nil, nil, fc, fm, d)

	_, err = model.GetNodeCost(c)
	if err != nil {
		t.Errorf("Error in node pricing: %s", err)
	}
	p, err := model.GetPricingSourceCounts()
	if err != nil {
		t.Errorf("Error in pricing source counts: %s", err)
	} else if p.TotalNodes != 3 {
		t.Errorf("Wanted 3 nodes got %d", p.TotalNodes)
	}
	if p.PricingTypeCounts[""] != 1 {
		t.Errorf("Wanted 1 default match got %d: %+v", p.PricingTypeCounts[""], p.PricingTypeCounts)
	}
	if p.PricingTypeCounts["csvExact"] != 1 {
		t.Errorf("Wanted 1 exact match got %d: %+v", p.PricingTypeCounts["csvExact"], p.PricingTypeCounts)
	}
	if p.PricingTypeCounts["csvClass"] != 1 {
		t.Errorf("Wanted 1 class match got %d: %+v", p.PricingTypeCounts["csvClass"], p.PricingTypeCounts)
	}

}

func TestNodePriceFromCSVWithCase(t *testing.T) {
	n := &clustercache.Node{}
	n.SpecProviderID = "azure:///subscriptions/123a7sd-asd-1234-578a9-123abcdef/resourceGroups/case_12_STaGe_TeSt7/providers/Microsoft.Compute/virtualMachineScaleSets/vmss-agent-worker0-12stagetest7-ezggnore/virtualMachines/7"
	n.Labels = make(map[string]string)
	n.Labels[v1.LabelTopologyRegion] = "eastus2"
	wantPrice := "0.13370357"

	confMan := config.NewConfigFileManager(&config.ConfigFileManagerOpts{
		LocalConfigPath: "./",
	})

	c := &provider.CSVProvider{
		CSVLocation: "../configs/pricing_schema_case.csv",
		CustomProvider: &provider.CustomProvider{
			Config: provider.NewProviderConfig(confMan, "../configs/default.json"),
		},
	}

	c.DownloadPricingData()
	k := c.GetKey(n.Labels, n)
	resN, _, err := c.NodePricing(k)
	if err != nil {
		t.Errorf("Error in NodePricing: %s", err.Error())
	} else {
		gotPrice := resN.Cost
		wantPriceFloat, _ := strconv.ParseFloat(wantPrice, 64)
		gotPriceFloat, _ := strconv.ParseFloat(gotPrice, 64)
		if gotPriceFloat != wantPriceFloat {
			t.Errorf("Wanted price '%f' got price '%f'", wantPriceFloat, gotPriceFloat)
		}
	}

}

func TestNodePriceFromCSVMixed(t *testing.T) {
	labelFooWant := "OnDemand"

	confMan := config.NewConfigFileManager(&config.ConfigFileManagerOpts{
		LocalConfigPath: "./",
	})

	n := &clustercache.Node{}
	n.Labels = make(map[string]string)
	n.Labels["TestClusterUsage"] = labelFooWant
	n.Labels["nvidia.com/gpu_type"] = "a100-ondemand"
	n.Status.Capacity = v1.ResourceList{"nvidia.com/gpu": *resource.NewScaledQuantity(2, 0)}
	wantPrice := "1.904110"

	labelFooWant2 := "Reserved"
	n2 := &clustercache.Node{}
	n2.Labels = make(map[string]string)
	n2.Labels["TestClusterUsage"] = labelFooWant2
	n2.Labels["nvidia.com/gpu_type"] = "a100-reserved"
	n2.Status.Capacity = v1.ResourceList{"nvidia.com/gpu": *resource.NewScaledQuantity(1, 0)}

	wantPrice2 := "1.654795"

	c := &provider.CSVProvider{
		CSVLocation: "../configs/pricing_schema_mixed_gpu_ondemand.csv",
		CustomProvider: &provider.CustomProvider{
			Config: provider.NewProviderConfig(confMan, "../configs/default.json"),
		},
	}
	c.DownloadPricingData()
	k := c.GetKey(n.Labels, n)
	resN, _, err := c.NodePricing(k)
	if err != nil {
		t.Errorf("Error in NodePricing: %s", err.Error())
	} else {
		gotPrice := resN.Cost
		wantPriceFloat, _ := strconv.ParseFloat(wantPrice, 64)
		gotPriceFloat, _ := strconv.ParseFloat(gotPrice, 64)
		if gotPriceFloat != wantPriceFloat {
			t.Errorf("Wanted price '%f' got price '%f'", wantPriceFloat, gotPriceFloat)
		}
	}
	k2 := c.GetKey(n2.Labels, n2)
	resN2, _, err2 := c.NodePricing(k2)
	if err2 != nil {
		t.Errorf("Error in NodePricing: %s", err.Error())
	} else {
		gotPrice := resN2.Cost
		wantPriceFloat, _ := strconv.ParseFloat(wantPrice2, 64)
		gotPriceFloat, _ := strconv.ParseFloat(gotPrice, 64)
		if gotPriceFloat != wantPriceFloat {
			t.Errorf("Wanted price '%f' got price '%f'", wantPriceFloat, gotPriceFloat)
		}
	}

}

func TestNodePriceFromCSVByClass(t *testing.T) {
	n := &clustercache.Node{}
	n.SpecProviderID = "fakeproviderid"
	n.Labels = make(map[string]string)
	n.Labels[v1.LabelTopologyRegion] = "eastus2"
	n.Labels[v1.LabelInstanceTypeStable] = "Standard_F32s_v2"
	wantpricefloat := 0.13370357
	wantPrice := fmt.Sprintf("%f", (math.Round(wantpricefloat*1000000) / 1000000))

	confMan := config.NewConfigFileManager(&config.ConfigFileManagerOpts{
		LocalConfigPath: "./",
	})

	c := &provider.CSVProvider{
		CSVLocation: "../configs/pricing_schema_case.csv",
		CustomProvider: &provider.CustomProvider{
			Config: provider.NewProviderConfig(confMan, "../configs/default.json"),
		},
	}

	c.DownloadPricingData()

	k := c.GetKey(n.Labels, n)
	resN, _, err := c.NodePricing(k)
	if err != nil {
		t.Errorf("Error in NodePricing: %s", err.Error())
	} else {
		gotPrice := resN.Cost
		if gotPrice != wantPrice {
			t.Errorf("Wanted price '%s' got price '%s'", wantPrice, gotPrice)
		}
	}

	n2 := &clustercache.Node{}
	n2.SpecProviderID = "fakeproviderid"
	n2.Labels = make(map[string]string)
	n2.Labels[v1.LabelTopologyRegion] = "fakeregion"
	n2.Labels[v1.LabelInstanceTypeStable] = "Standard_F32s_v2"
	k2 := c.GetKey(n2.Labels, n)

	c.DownloadPricingData()
	resN2, _, err := c.NodePricing(k2)

	if resN2 != nil {
		t.Errorf("CSV provider should return nil on missing node, instead returned %+v", resN2)
	}

}
