package oracle

import (
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

const nodePoolIdAnnotation = "oci.oraclecloud.com/node-pool-id"
const virtualPoolIdAnnotation = "oci.oraclecloud.com/virtual-node-pool-id"
const virtualNodeLabel = "node-role.kubernetes.io/virtual-node"
const managementPlatformOKE = "oke"
const currencyCodeUSD = "USD"

type Oracle struct {
	Config                  models.ProviderConfig
	Clientset               clustercache.ClusterCache
	ClusterRegion           string
	ClusterAccountID        string
	DownloadPricingDataLock sync.RWMutex
	OSEnvLock               sync.Mutex
	RateCardStore           *RateCardStore
	ServiceAccountChecks    *models.ServiceAccountChecks
	DefaultPricing          DefaultPricing
}

func (o *Oracle) ClusterInfo() (map[string]string, error) {
	c, err := o.GetConfig()
	if err != nil {
		return nil, err
	}
	m := make(map[string]string)
	m["name"] = "Oracle Cluster #1"
	if clusterName := o.getClusterName(c); clusterName != "" {
		m["name"] = clusterName
	}
	m["provider"] = opencost.OracleProvider
	m["account"] = o.ClusterAccountID
	m["region"] = o.ClusterRegion
	m["remoteReadEnabled"] = strconv.FormatBool(env.IsRemoteEnabled())
	m["id"] = env.GetClusterID()
	return m, nil
}

func (o *Oracle) NodePricing(key models.Key) (*models.Node, models.PricingMetadata, error) {
	if err := o.ensurePricingData(); err != nil {
		return nil, models.PricingMetadata{}, err
	}
	o.DownloadPricingDataLock.RLock()
	defer o.DownloadPricingDataLock.RUnlock()
	return o.RateCardStore.ForKey(key, o.DefaultPricing)
}

func (o *Oracle) PVPricing(pvk models.PVKey) (*models.PV, error) {
	if err := o.ensurePricingData(); err != nil {
		return nil, err
	}
	o.DownloadPricingDataLock.RLock()
	defer o.DownloadPricingDataLock.RUnlock()
	return o.RateCardStore.ForPVK(pvk, o.DefaultPricing)
}

func (o *Oracle) NetworkPricing() (*models.Network, error) {
	if err := o.ensurePricingData(); err != nil {
		return nil, err
	}
	o.DownloadPricingDataLock.RLock()
	defer o.DownloadPricingDataLock.RUnlock()
	return o.RateCardStore.ForEgressRegion(o.ClusterRegion, o.DefaultPricing)
}

func (o *Oracle) LoadBalancerPricing() (*models.LoadBalancer, error) {
	if err := o.ensurePricingData(); err != nil {
		return nil, err
	}
	o.DownloadPricingDataLock.RLock()
	defer o.DownloadPricingDataLock.RUnlock()
	return o.RateCardStore.ForLB(o.DefaultPricing)
}

func (o *Oracle) AllNodePricing() (interface{}, error) {
	if err := o.ensurePricingData(); err != nil {
		return nil, err
	}
	o.DownloadPricingDataLock.RLock()
	defer o.DownloadPricingDataLock.RUnlock()
	return o.RateCardStore.Store(), nil
}

// DownloadPricingData refreshes the RateCardStore pricing data.
func (o *Oracle) DownloadPricingData() error {
	o.DownloadPricingDataLock.Lock()
	defer o.DownloadPricingDataLock.Unlock()
	cfg, err := o.GetConfig()
	if err != nil {
		return err
	}
	if o.RateCardStore == nil {
		url := env.GetOCIPricingURL()
		o.RateCardStore = NewRateCardStore(url, cfg.CurrencyCode)
	}
	if _, err := o.RateCardStore.Refresh(); err != nil {
		return err
	}
	o.DefaultPricing = DefaultPricing{
		OCPU:    cfg.CPU,
		Memory:  cfg.RAM,
		GPU:     cfg.GPU,
		Storage: cfg.Storage,
		Egress:  cfg.InternetNetworkEgress,
		LB:      cfg.DefaultLBPrice,
	}
	return nil
}

func (o *Oracle) GetKey(labels map[string]string, n *clustercache.Node) models.Key {
	var gpuCount int
	var gpuType string
	if gpuc, ok := n.Status.Capacity["nvidia.com/gpu"]; ok {
		gpuCount = int(gpuc.Value())
		gpuType = "nvidia.com/gpu"
	}
	instanceType, _ := util.GetInstanceType(labels)
	return &oracleKey{
		providerID:   n.SpecProviderID,
		instanceType: instanceType,
		labels:       labels,
		gpuCount:     gpuCount,
		gpuType:      gpuType,
	}
}

func (o *Oracle) GetPVKey(pv *clustercache.PersistentVolume, parameters map[string]string, _ string) models.PVKey {
	var providerID string
	var driver string
	if pv.Spec.CSI != nil {
		providerID = pv.Spec.CSI.VolumeHandle
		driver = pv.Spec.CSI.Driver
	}
	return &oraclePVKey{
		storageClass: pv.Spec.StorageClassName,
		providerID:   providerID,
		driver:       driver,
		parameters:   parameters,
	}
}

func (o *Oracle) UpdateConfig(r io.Reader, _ string) (*models.CustomPricing, error) {
	return o.Config.Update(func(pricing *models.CustomPricing) error {
		a := make(map[string]interface{})
		err := json.NewDecoder(r).Decode(&a)
		if err != nil {
			return err
		}
		for k, v := range a {
			kUpper := utils.ToTitle.String(k)
			vstr, ok := v.(string)
			if ok {
				err := models.SetCustomPricingField(pricing, kUpper, vstr)
				if err != nil {
					return fmt.Errorf("error setting custom pricing field: %w", err)
				}
			} else {
				return fmt.Errorf("type error while updating config for %s", kUpper)
			}
		}

		if env.IsRemoteEnabled() {
			err := utils.UpdateClusterMeta(env.GetClusterID(), o.getClusterName(pricing))
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (o *Oracle) UpdateConfigFromConfigMap(m map[string]string) (*models.CustomPricing, error) {
	return o.Config.UpdateFromMap(m)
}

func (o *Oracle) GetConfig() (*models.CustomPricing, error) {
	c, err := o.Config.GetCustomPricingData()
	if err != nil {
		return nil, err
	}
	if c.Discount == "" {
		c.Discount = "0%"
	}
	if c.NegotiatedDiscount == "" {
		c.NegotiatedDiscount = "0%"
	}
	if c.CurrencyCode == "" {
		c.CurrencyCode = currencyCodeUSD
	}
	return c, nil
}

func (o *Oracle) GetManagementPlatform() (string, error) {
	nodes := o.Clientset.GetAllNodes()
	for _, node := range nodes {
		if _, ok := node.Annotations[nodePoolIdAnnotation]; ok {
			return managementPlatformOKE, nil
		}
		if _, ok := node.Annotations[virtualPoolIdAnnotation]; ok {
			return managementPlatformOKE, nil
		}
	}
	return "", nil
}

func (o *Oracle) PricingSourceStatus() map[string]*models.PricingSource {
	listPricing := "List Pricing"
	return map[string]*models.PricingSource{
		listPricing: {
			Name:      listPricing,
			Enabled:   true,
			Available: true,
		},
	}
}

func (o *Oracle) ClusterManagementPricing() (string, float64, error) {
	if err := o.ensurePricingData(); err != nil {
		return "", 0.0, err
	}
	managementPlatform, _ := o.GetManagementPlatform()
	if managementPlatform != managementPlatformOKE {
		return "", 0.0, nil // Self-managed cluster.
	}
	o.DownloadPricingDataLock.Lock()
	defer o.DownloadPricingDataLock.Unlock()
	// TODO: Support lookup of cluster type, as BASIC_CLUSTER types are free.
	return managementPlatformOKE, o.RateCardStore.ForManagedCluster("ENHANCED_CLUSTER"), nil
}

func (o *Oracle) CombinedDiscountForNode(instanceType string, isPreemptible bool, defaultDiscount, negotiatedDiscount float64) float64 {
	return 1.0 - ((1.0 - defaultDiscount) * (1.0 - negotiatedDiscount))
}

func (o *Oracle) Regions() []string {
	regionOverrides := env.GetRegionOverrideList()
	if len(regionOverrides) > 0 {
		log.Debugf("Overriding Oracle regions with configured region list: %+v", regionOverrides)
		return regionOverrides
	}
	return oracleRegions()
}

func (o *Oracle) PricingSourceSummary() interface{} {
	if err := o.ensurePricingData(); err != nil {
		return err
	}
	o.DownloadPricingDataLock.Lock()
	defer o.DownloadPricingDataLock.Unlock()
	return o.RateCardStore.Store()
}

func (o *Oracle) getClusterName(cfg *models.CustomPricing) string {
	if cfg.ClusterName != "" {
		return cfg.ClusterName
	}
	for _, node := range o.Clientset.GetAllNodes() {
		if clusterName, ok := node.Labels["name"]; ok {
			return clusterName
		}
	}
	return ""
}

func (o *Oracle) ensurePricingData() error {
	if o.RateCardStore == nil {
		return o.DownloadPricingData()
	}
	return nil
}

func (o *Oracle) GetAddresses() ([]byte, error) {
	return nil, nil
}

func (o *Oracle) GetDisks() ([]byte, error) {
	return nil, nil
}

func (o *Oracle) GetOrphanedResources() ([]models.OrphanedResource, error) {
	return nil, nil
}

func (o *Oracle) GetLocalStorageQuery(duration time.Duration, duration2 time.Duration, b bool, b2 bool) string {
	return ""
}

func (o *Oracle) ApplyReservedInstancePricing(m map[string]*models.Node) {}

func (o *Oracle) ServiceAccountStatus() *models.ServiceAccountStatus {
	return o.ServiceAccountChecks.GetStatus()
}
