package linode

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/linode/linodego"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/pkg/cloud/models"
)

func (l *Linode) CombinedDiscountForNode(instanceType string, isPreemptible bool, defaultDiscount, negotiatedDiscount float64) float64 {
	return 1.0 - ((1.0 - defaultDiscount) * (1.0 - negotiatedDiscount))
}

func (l *Linode) ClusterManagementPricing() (string, float64, error) {
	cpricing, err := l.Config.GetCustomPricingData()
	if err != nil {
		log.Errorf("Could not get Linode custom pricing: %+v", err)

		return "", 0.0, err
	}

	cpPricing, err := strconv.ParseFloat(cpricing.ControlPlaneCosts, 64)
	if err != nil {
		return "", 0.0, err
	}

	if l.isHaCluster.Load() {
		cpPricing, err = strconv.ParseFloat(cpricing.HAControlPlaneCosts, 64)
		if err != nil {
			return "", 0.0, err
		}
	}

	return "", cpPricing, nil
}

func (l *Linode) LoadBalancerPricing() (*models.LoadBalancer, error) {
	cpricing, err := l.Config.GetCustomPricingData()
	if err != nil {
		log.Errorf("Could not get Linode custom pricing: %+v", err)

		return nil, err
	}

	lbPricing, err := strconv.ParseFloat(cpricing.DefaultLBPrice, 64)
	if err != nil {
		return nil, err
	}

	return &models.LoadBalancer{
		Cost: lbPricing,
	}, nil
}

func (l *Linode) NetworkPricing() (*models.Network, error) {
	cpricing, err := l.Config.GetCustomPricingData()
	if err != nil {
		log.Errorf("Could not get Linode custom pricing: %+v", err)

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

func (l *Linode) DownloadPricingData() error {
	return l.init()
}

func (l *Linode) NodePricing(key models.Key) (*models.Node, models.PricingMetadata, error) {
	meta := models.PricingMetadata{}

	split := strings.Split(key.Features(), ",")
	if pricing, ok := l.linodePricing.Load(split[0]); ok {
		if info, ok := pricing.(LinodePricing)[split[1]]; ok {
			// Spliting price between CPU and RAM is a workaround,
			// because OpenCost expects RAM cost.
			price := info.Price.Hourly / float32(info.VCPUs)

			return &models.Node{
				Cost:        "0.0",
				PricingType: models.DefaultPrices,
				// CPU
				VCPU:     fmt.Sprintf("%d", info.VCPUs),
				VCPUCost: fmt.Sprintf("%f", price/2),
				// Memory
				RAM:      fmt.Sprintf("%d", info.Memory),
				RAMBytes: fmt.Sprintf("%d", info.Memory*1024),
				RAMCost:  fmt.Sprintf("%f", price/2),
				// Storage
				Storage:     fmt.Sprintf("%d", info.Disk),
				StorageCost: "0.0",
				// Metadata
				InstanceType: split[1],
				Region:       split[0],
			}, meta, nil
		}
	}

	log.Debugf("Node pricing not found for %s", key.Features())

	return nil, meta, fmt.Errorf("unable to find node pricing matching the features `%s`", key.Features())
}

func (l *Linode) GpuPricing(nodeLabels map[string]string) (string, error) {
	return "", nil
}

func (l *Linode) PVPricing(pvk models.PVKey) (*models.PV, error) {
	cpricing, err := l.Config.GetCustomPricingData()
	if err != nil {
		log.Errorf("Could not get Linode custom pricing: %+v", err)

		return nil, err
	}

	return &models.PV{
		Cost:  cpricing.Storage,
		Class: pvk.GetStorageClass(),
	}, nil
}

// PricingSourceSummary returns the pricing source summary for the provider.
// The summary represents what was _parsed_ from the pricing source, not
// everything that was _available_ in the pricing source.
func (l *Linode) PricingSourceSummary() interface{} {
	pricing := make(LinodePricing)

	l.linodePricing.Range(func(key, value any) bool {
		pricing[key.(string)] = value.(*linodego.LinodeType)
		return true
	})

	return pricing
}

func (l *Linode) AllNodePricing() (interface{}, error) {
	return l.PricingSourceSummary(), nil
}

func (l *Linode) PricingSourceStatus() map[string]*models.PricingSource {
	return map[string]*models.PricingSource{
		InstanceAPIPricing: {
			Name:      InstanceAPIPricing,
			Enabled:   true,
			Available: true,
		},
	}
}

func (l *Linode) refreshPricing(clusterID int) error {
	lc := l.linodeClient.Load().(linodego.Client)

	ctx, cancel := context.WithTimeout(context.Background(), linodeAPITimeout)
	defer cancel()

	lkeCluster, err := lc.GetLKECluster(ctx, clusterID)
	if err != nil {
		log.Errorf("Could not get LKE cluster: %+v", err)

		return err
	}
	l.isHaCluster.Store(lkeCluster.ControlPlane.HighAvailability)

	ctx, cancel = context.WithTimeout(context.Background(), linodeAPITimeout)
	defer cancel()

	regions, err := lc.ListRegions(ctx, nil)
	if err != nil {
		log.Errorf("Could not get Linode regions: %+v", err)

		return err
	}

	ctx, cancel = context.WithTimeout(context.Background(), linodeAPITimeout)
	defer cancel()

	linodeTypes, err := lc.ListTypes(ctx, nil)
	if err != nil {
		log.Errorf("Could not get Linode types: %+v", err)

		return err
	}

	regionList := []string{}
	for _, region := range regions {
		regionList = append(regionList, region.ID)
		l.linodePricing.LoadOrStore(region.ID, LinodePricing{})
	}

	for _, t := range linodeTypes {
		l.linodePricing.Range(func(_ any, prices any) bool {
			prices.(LinodePricing)[t.ID] = &t
			return true
		})

		for i := range t.RegionPrices {
			pricing, _ := l.linodePricing.LoadOrStore(t.RegionPrices[i].ID, LinodePricing{})
			pricing.(LinodePricing)[t.ID] = &t
		}
	}

	l.regions.Store(regionList)
	return nil
}
