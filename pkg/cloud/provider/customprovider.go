package provider

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util"
	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/cloud/utils"
	"github.com/opencost/opencost/pkg/clustercache"
	"github.com/opencost/opencost/pkg/env"
)

type NodePrice struct {
	CPU string
	RAM string
	GPU string
}

type CustomProvider struct {
	Clientset               clustercache.ClusterCache
	Pricing                 map[string]*NodePrice
	SpotLabel               string
	SpotLabelValue          string
	GPULabel                string
	GPULabelValue           string
	ClusterRegion           string
	ClusterAccountID        string
	DownloadPricingDataLock sync.RWMutex
	Config                  models.ProviderConfig
}

var volTypes = map[string]string{
	"EBS:VolumeUsage.gp2":    "gp2",
	"EBS:VolumeUsage.gp3":    "gp3",
	"EBS:VolumeUsage":        "standard",
	"EBS:VolumeUsage.sc1":    "sc1",
	"EBS:VolumeP-IOPS.piops": "io1",
	"EBS:VolumeUsage.st1":    "st1",
	"EBS:VolumeUsage.piops":  "io1",
	"gp2":                    "EBS:VolumeUsage.gp2",
	"gp3":                    "EBS:VolumeUsage.gp3",
	"standard":               "EBS:VolumeUsage",
	"sc1":                    "EBS:VolumeUsage.sc1",
	"io1":                    "EBS:VolumeUsage.piops",
	"st1":                    "EBS:VolumeUsage.st1",
}

type customPVKey struct {
	Labels                 map[string]string
	StorageClassParameters map[string]string
	StorageClassName       string
	Name                   string
	DefaultRegion          string
	ProviderID             string
}

// PricingSourceSummary returns the pricing source summary for the provider.
// The summary represents what was _parsed_ from the pricing source, not what
// was returned from the relevant API.
func (cp *CustomProvider) PricingSourceSummary() interface{} {
	return cp.Pricing
}

type customProviderKey struct {
	SpotLabel      string
	SpotLabelValue string
	GPULabel       string
	GPULabelValue  string
	Labels         map[string]string
}

func (*CustomProvider) ClusterManagementPricing() (string, float64, error) {
	return "", 0.0, nil
}

func (*CustomProvider) GetLocalStorageQuery(window, offset time.Duration, rate bool, used bool) string {
	return ""
}

func (cp *CustomProvider) GetConfig() (*models.CustomPricing, error) {
	return cp.Config.GetCustomPricingData()
}

func (*CustomProvider) GetManagementPlatform() (string, error) {
	return "", nil
}

func (*CustomProvider) ApplyReservedInstancePricing(nodes map[string]*models.Node) {

}

func (cp *CustomProvider) UpdateConfigFromConfigMap(a map[string]string) (*models.CustomPricing, error) {
	return cp.Config.UpdateFromMap(a)
}

func (cp *CustomProvider) UpdateConfig(r io.Reader, updateType string) (*models.CustomPricing, error) {
	// Parse config updates from reader
	a := make(map[string]interface{})
	err := json.NewDecoder(r).Decode(&a)
	if err != nil {
		return nil, err
	}

	// Update Config
	c, err := cp.Config.Update(func(c *models.CustomPricing) error {
		for k, v := range a {
			kUpper := utils.ToTitle.String(k) // Just so we consistently supply / receive the same values, uppercase the first letter.
			vstr, ok := v.(string)
			if ok {
				err := models.SetCustomPricingField(c, kUpper, vstr)
				if err != nil {
					return fmt.Errorf("error setting custom pricing field: %w", err)
				}
			} else {
				return fmt.Errorf("type error while updating config for %s", kUpper)
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	defer cp.DownloadPricingData()
	return c, nil
}

func (cp *CustomProvider) ClusterInfo() (map[string]string, error) {
	conf, err := cp.GetConfig()
	if err != nil {
		return nil, err
	}
	m := make(map[string]string)
	if conf.ClusterName != "" {
		m["name"] = conf.ClusterName
	}
	m["provider"] = opencost.CustomProvider
	m["region"] = cp.ClusterRegion
	m["account"] = cp.ClusterAccountID
	m["id"] = env.GetClusterID()
	return m, nil
}

func (*CustomProvider) GetAddresses() ([]byte, error) {
	return nil, nil
}

func (*CustomProvider) GetDisks() ([]byte, error) {
	return nil, nil
}

func (*CustomProvider) GetOrphanedResources() ([]models.OrphanedResource, error) {
	return nil, errors.New("not implemented")
}

func (cp *CustomProvider) AllNodePricing() (interface{}, error) {
	cp.DownloadPricingDataLock.RLock()
	defer cp.DownloadPricingDataLock.RUnlock()

	return cp.Pricing, nil
}

func (cp *CustomProvider) NodePricing(key models.Key) (*models.Node, models.PricingMetadata, error) {
	cp.DownloadPricingDataLock.RLock()
	defer cp.DownloadPricingDataLock.RUnlock()

	meta := models.PricingMetadata{}

	k := key.Features()
	var gpuCount string
	if _, ok := cp.Pricing[k]; !ok {
		// Default is saying that there is no pricing info for the cluster and we should fall back to the default values.
		// An interesting case is if the default values weren't loaded.
		k = "default"
	}
	if key.GPUType() != "" {
		k += ",gpu"    // TODO: support multiple custom gpu types.
		gpuCount = "1" // TODO: support more than one gpu.
	}

	var cpuCost, ramCost, gpuCost string
	if pricing, ok := cp.Pricing[k]; !ok {
		log.Warnf("No pricing found for key=%s, setting values to 0", k)
		cpuCost = "0.0"
		ramCost = "0.0"
		gpuCost = "0.0"
	} else {
		cpuCost = pricing.CPU
		ramCost = pricing.RAM
		gpuCost = pricing.GPU
	}

	return &models.Node{
		VCPUCost: cpuCost,
		RAMCost:  ramCost,
		GPUCost:  gpuCost,
		GPU:      gpuCount,
	}, meta, nil
}

func (cp *CustomProvider) DownloadPricingData() error {
	cp.DownloadPricingDataLock.Lock()
	defer cp.DownloadPricingDataLock.Unlock()

	if cp.Pricing == nil {
		m := make(map[string]*NodePrice)
		cp.Pricing = m
	}
	p, err := cp.Config.GetCustomPricingData()
	if err != nil {
		return err
	}
	cp.SpotLabel = p.SpotLabel
	cp.SpotLabelValue = p.SpotLabelValue
	cp.GPULabel = p.GpuLabel
	cp.GPULabelValue = p.GpuLabelValue
	cp.Pricing["default"] = &NodePrice{
		CPU: p.CPU,
		RAM: p.RAM,
	}
	cp.Pricing["default,spot"] = &NodePrice{
		CPU: p.SpotCPU,
		RAM: p.SpotRAM,
	}
	cp.Pricing["default,gpu"] = &NodePrice{
		CPU: p.CPU,
		RAM: p.RAM,
		GPU: p.GPU,
	}
	return nil
}

func (cp *CustomProvider) GetKey(labels map[string]string, n *clustercache.Node) models.Key {
	return &customProviderKey{
		SpotLabel:      cp.SpotLabel,
		SpotLabelValue: cp.SpotLabelValue,
		GPULabel:       cp.GPULabel,
		GPULabelValue:  cp.GPULabelValue,
		Labels:         labels,
	}
}

// ExternalAllocations represents tagged assets outside the scope of kubernetes.
// "start" and "end" are dates of the format YYYY-MM-DD
// "aggregator" is the tag used to determine how to allocate those assets, ie namespace, pod, etc.
func (*CustomProvider) ExternalAllocations(start string, end string, aggregator []string, filterType string, filterValue string, crossCluster bool) ([]*models.OutOfClusterAllocation, error) {
	return nil, nil // TODO: transform the QuerySQL lines into the new OutOfClusterAllocation Struct
}

func (*CustomProvider) QuerySQL(query string) ([]byte, error) {
	return nil, nil
}

func (cp *CustomProvider) GpuPricing(nodeLabels map[string]string) (string, error) {
	return "", nil
}

func (cp *CustomProvider) PVPricing(pvk models.PVKey) (*models.PV, error) {
	cpricing, err := cp.Config.GetCustomPricingData()
	if err != nil {
		return nil, err
	}
	return &models.PV{
		Cost: cpricing.Storage,
	}, nil
}

func (cp *CustomProvider) NetworkPricing() (*models.Network, error) {
	cpricing, err := cp.Config.GetCustomPricingData()
	if err != nil {
		return nil, err
	}
	znec, err := strconv.ParseFloat(cpricing.ZoneNetworkEgress, 64)
	if err != nil {
		return nil, err
	}
	rnec, err := strconv.ParseFloat(cpricing.RegionNetworkEgress, 64)
	if err != nil {
		return nil, err
	}
	inec, err := strconv.ParseFloat(cpricing.InternetNetworkEgress, 64)
	if err != nil {
		return nil, err
	}

	return &models.Network{
		ZoneNetworkEgressCost:     znec,
		RegionNetworkEgressCost:   rnec,
		InternetNetworkEgressCost: inec,
	}, nil
}

func (cp *CustomProvider) LoadBalancerPricing() (*models.LoadBalancer, error) {
	cpricing, err := cp.Config.GetCustomPricingData()
	if err != nil {
		return nil, err
	}
	fffrc, err := strconv.ParseFloat(cpricing.FirstFiveForwardingRulesCost, 64)
	if err != nil {
		return nil, err
	}
	afrc, err := strconv.ParseFloat(cpricing.AdditionalForwardingRuleCost, 64)
	if err != nil {
		return nil, err
	}
	lbidc, err := strconv.ParseFloat(cpricing.LBIngressDataCost, 64)
	if err != nil {
		return nil, err
	}
	var totalCost float64
	numForwardingRules := 1.0 // hard-code at 1 for now
	dataIngressGB := 0.0      // hard-code at 0 for now

	if numForwardingRules < 5 {
		totalCost = fffrc*numForwardingRules + lbidc*dataIngressGB
	} else {
		totalCost = fffrc*5 + afrc*(numForwardingRules-5) + lbidc*dataIngressGB
	}
	return &models.LoadBalancer{
		Cost: totalCost,
	}, nil
}

func (*CustomProvider) GetPVKey(pv *clustercache.PersistentVolume, parameters map[string]string, defaultRegion string) models.PVKey {
	return &customPVKey{
		Labels:                 pv.Labels,
		StorageClassName:       pv.Spec.StorageClassName,
		StorageClassParameters: parameters,
		DefaultRegion:          defaultRegion,
	}
}

func (key *customPVKey) ID() string {
	return key.ProviderID
}

func (key *customPVKey) GetStorageClass() string {
	return key.StorageClassName
}

// Features returns a comma separated string of features for a given PV
// (@pokom): This was imported from aws which caused a cyclical dependency. This _should_ be refactored to be specific to a custom pvkey
func (key *customPVKey) Features() string {
	storageClass := key.StorageClassParameters["type"]
	if storageClass == "standard" {
		storageClass = "gp2"
	}
	// Storage class names are generally EBS volume types (gp2)
	// Keys in Pricing are based on UsageTypes (EBS:VolumeType.gp2)
	// Converts between the 2
	region, ok := util.GetRegion(key.Labels)
	if !ok {
		region = key.DefaultRegion
	}
	class, ok := volTypes[storageClass]
	if !ok {
		log.Debugf("No voltype mapping for %s's storageClass: %s", key.Name, storageClass)
	}
	return region + "," + class
}

func (k *customProviderKey) GPUCount() int {
	return 0
}

func (cpk *customProviderKey) GPUType() string {
	if t, ok := cpk.Labels[cpk.GPULabel]; ok {
		return t
	}
	return ""
}

func (cpk *customProviderKey) ID() string {
	return ""
}

func (cpk *customProviderKey) Features() string {
	if cpk.Labels[cpk.SpotLabel] != "" && cpk.Labels[cpk.SpotLabel] == cpk.SpotLabelValue {
		return "default,spot"
	}
	return "default" // TODO: multiple custom pricing support.
}

func (cp *CustomProvider) ServiceAccountStatus() *models.ServiceAccountStatus {
	return &models.ServiceAccountStatus{
		Checks: []*models.ServiceAccountCheck{},
	}
}

func (cp *CustomProvider) PricingSourceStatus() map[string]*models.PricingSource {
	return make(map[string]*models.PricingSource)
}

func (cp *CustomProvider) CombinedDiscountForNode(instanceType string, isPreemptible bool, defaultDiscount, negotiatedDiscount float64) float64 {
	return 1.0 - ((1.0 - defaultDiscount) * (1.0 - negotiatedDiscount))
}

func (cp *CustomProvider) Regions() []string {
	return []string{}
}
