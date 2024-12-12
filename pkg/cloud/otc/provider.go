package otc

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util"
	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/clustercache"
	"github.com/opencost/opencost/pkg/env"
)

// OTC node pricing attributes
type OTCNodeAttributes struct {
	Type  string // like s2.large.1
	OS    string // like windows
	Price string // (in EUR) like 0.023
	RAM   string // (in GB) like 2
	VCPU  string // like 8
}

type OTCPVAttributes struct {
	Type  string // like vss.ssd
	Price string // (in EUR/GB/h) like 0.01
}

// OTC pricing is either for a node, a persistent volume (or a database, network, cluster, ...)
type OTCPricing struct {
	NodeAttributes *OTCNodeAttributes
	PVAttributes   *OTCPVAttributes
}

// the main provider struct
type OTC struct {
	Clientset               clustercache.ClusterCache
	Pricing                 map[string]*OTCPricing
	Config                  models.ProviderConfig
	ClusterRegion           string
	projectID               string
	clusterManagementPrice  float64
	BaseCPUPrice            string
	BaseRAMPrice            string
	BaseGPUPrice            string
	ValidPricingKeys        map[string]bool
	DownloadPricingDataLock sync.RWMutex
}

// Kubernetes to OTC OS conversion
/* Note:
Kubernetes cannot fill the "kubernetes.io/os" label with the variety that OTC provides
because it is based on the runtime.GOOS variable (https://kubernetes.io/docs/reference/labels-annotations-taints/#kubernetes-io-os)
which can only contain some os. The pricing between everything but windows differs
by about 5ct - 30ct per hour, so most os get treated like Open Linux.
*/
var kubernetesOSTypes = map[string]string{
	"linux":        "Open Linux",
	"windows":      "Windows",
	"Open Linux":   "linux",
	"Oracle Linux": "linux",
	"SUSE Linux":   "linux",
	"SUSE for SAP": "linux",
	"RedHat Linux": "linux",
	"Windows":      "windows",
}

// Currently assumes that no GPU is present
// but aws does that too, so its fine.
type otcKey struct {
	ProviderID string
	Labels     map[string]string
}

func (k *otcKey) GPUCount() int {
	return 0
}

func (k *otcKey) GPUType() string {
	return ""
}

func (k *otcKey) ID() string {
	return k.ProviderID
}

type otcPVKey struct {
	RegionID               string
	Type                   string
	Size                   string // in GB
	Labels                 map[string]string
	ProviderId             string
	StorageClassParameters map[string]string
}

func (k *otcPVKey) Features() string {
	fmt.Printf("features for pv %s", k.ID())
	return k.RegionID + "," + k.Type
}

func (k *otcKey) Features() string {
	instanceType, _ := util.GetInstanceType(k.Labels)
	operatingSystem, _ := util.GetOperatingSystem(k.Labels)
	ClusterRegion, _ := util.GetRegion(k.Labels)

	key := ClusterRegion + "," + instanceType + "," + operatingSystem
	return key
}

// Extract/generate a key that holds the data required to calculate
// the cost of the given node (like s2.large.4).
func (otc *OTC) GetKey(labels map[string]string, n *clustercache.Node) models.Key {
	return &otcKey{
		Labels:     labels,
		ProviderID: labels["providerID"],
	}
}

// Returns the storage class for a persistent volume key.
func (k *otcPVKey) GetStorageClass() string {
	return k.Type
}

// Returns the provider id for a persistent volume key.
func (k *otcPVKey) ID() string {
	return k.ProviderId
}

func (otc *OTC) GetPVKey(pv *clustercache.PersistentVolume, parameters map[string]string, defaultRegion string) models.PVKey {
	providerID := ""
	return &otcPVKey{
		Labels:                 pv.Labels,
		Type:                   pv.Spec.StorageClassName,
		StorageClassParameters: parameters,
		RegionID:               defaultRegion,
		ProviderId:             providerID,
	}
}

// Takes a resopnse from the otc api and the respective service name as an input
// and extracts the resulting data into a product slice.
func (otc *OTC) loadStructFromResponse(resp http.Response, serviceName string) ([]Product, error) {
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// Unmarshal the first bit of the response.
	wrapper := make(map[string]map[string]interface{})
	err = json.Unmarshal(body, &wrapper)
	if err != nil {
		return nil, err
	}

	// Unmarshal the second, more specific, bit of the response.
	data := make(map[string][]Product)
	tmp, err := json.Marshal(wrapper["response"]["result"])
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(tmp, &data)
	if err != nil {
		return nil, err
	}

	return data[serviceName], nil
}

// The product (price) data that is fetched from OTC
//
// If OsUnit, VCpu and Ram aren't given, the product
// is a persistent volume, else it's a node.
type Product struct {
	OpiFlavour  string `json:"opiFlavour"`
	OsUnit      string `json:"osUnit,omitempty"`
	PriceAmount string `json:"priceAmount"`
	VCpu        string `json:"vCpu,omitempty"`
	Ram         string `json:"ram,omitempty"`
}

/*
Download the pricing data from the OTC API

When a node has a specified price of e.g. 0.014 and
the kubernetes node has a RAM attribute of 8232873984 Bytes.

The price in Prometheus will be composed of:
  - the cpu/h price multiplied with the amount of VCPUs:
    0.006904 * 1 => 0.006904
  - the RAM/h price multiplied with the amount of ram in GiB:
    0.000925 * (8232873984/1024/1024/1024) => 0.0070924

And the resulting node_total_hourly_price{} metric in Prometheus
will approach the total node cost retrieved from OTC:

	==> 0.006904 + 0.0070924 = 0.013996399999999999
	    ~ 0.014
*/
func (otc *OTC) DownloadPricingData() error {
	otc.DownloadPricingDataLock.Lock()
	defer otc.DownloadPricingDataLock.Unlock()

	// Fetch pricing data from the otc.json config in case downloading the pricing maps fails.
	c, err := otc.Config.GetCustomPricingData()
	if err != nil {
		log.Errorf("Error downloading default pricing data: %s", err.Error())
	}

	otc.BaseCPUPrice = c.CPU
	otc.BaseRAMPrice = c.RAM
	otc.BaseGPUPrice = c.GPU
	otc.clusterManagementPrice = 0.10 // TODO: What is the cluster management price?
	otc.projectID = c.ProjectID

	// Slice with all nodes currently present in the cluster.
	nodeList := otc.Clientset.GetAllNodes()

	// Slice with all storage classes.
	storageClasses := otc.Clientset.GetAllStorageClasses()
	for _, tmp := range storageClasses {
		fmt.Println("storage class found:")
		fmt.Println(tmp.Parameters)
		fmt.Println(tmp.Labels)
		fmt.Println(tmp.TypeMeta)
		fmt.Println(tmp.Size)
	}

	// Slice with all persistent volumes present in the cluster
	pvList := otc.Clientset.GetAllPersistentVolumes()

	// Create a slice of all existing keys in the current cluster.
	// (keys like "eu-de,s3.medium.1,linux" or "eu-de,s3.xlarge.2,windows")
	inputkeys := make(map[string]bool)
	tmp := []string{}
	for _, node := range nodeList {
		labels := node.Labels
		key := otc.GetKey(labels, node)
		inputkeys[key.Features()] = true
		tmp = append(tmp, key.Features())
	}
	for _, pv := range pvList {
		fmt.Println("storage class name \"" + pv.Spec.StorageClassName + "\" found")
		key := otc.GetPVKey(pv, map[string]string{}, "eu-de")
		inputkeys[key.Features()] = true
		tmp = append(tmp, key.Features())
	}

	otc.Pricing = make(map[string]*OTCPricing)
	otc.ValidPricingKeys = make(map[string]bool)

	// Get pricing data from API.
	nodePricingURL := "https://calculator.otc-service.com/de/open-telekom-price-api/?serviceName=ecs" /* + "&limitMax=200"*/ + "&columns%5B1%5D=opiFlavour" + "&columns%5B2%5D=osUnit" + "&columns%5B3%5D=vCpu" + "&columns%5B4%5D=ram" + "&columns%5B5%5D=priceAmount"
	pvPricingURL := "https://calculator.otc-service.com/de/open-telekom-price-api/?serviceName%5B0%5D=evs&columns%5B1%5D=opiFlavour&columns%5B2%5D=priceAmount&limitFrom=0&region%5B3%5D=eu-de"

	log.Info("Started downloading OTC pricing data...")
	resp, err := http.Get(nodePricingURL)
	if err != nil {
		return err
	}
	pvResp, err := http.Get(pvPricingURL)
	if err != nil {
		return err
	}
	log.Info("Succesfully downloaded OTC pricing data")

	var products []Product

	nodeProducts, err := otc.loadStructFromResponse(*resp, "ecs")
	if err != nil {
		return err
	}
	products = append(products, nodeProducts...)
	pvProducts, err := otc.loadStructFromResponse(*pvResp, "evs")
	if err != nil {
		return err
	}
	products = append(products, pvProducts...)

	// convert the otc-reponse product-structs to opencost-compatible node structs
	const ClusterRegion = "eu-de"
	for _, product := range products {
		var productPricing *OTCPricing
		var key string
		// if os is empty the product must be a persistent volume
		if product.OsUnit == "" {
			productPricing = &OTCPricing{
				PVAttributes: &OTCPVAttributes{
					Type:  product.OpiFlavour,
					Price: strings.Split(strings.ReplaceAll(product.PriceAmount, ",", "."), " ")[0],
				},
			}
			key = ClusterRegion + "," + productPricing.PVAttributes.Type
		} else {
			// else it must be a node
			adjustedOS := kubernetesOSTypes[product.OsUnit]
			productPricing = &OTCPricing{
				NodeAttributes: &OTCNodeAttributes{
					Type:  product.OpiFlavour,
					OS:    adjustedOS,
					Price: strings.Split(strings.ReplaceAll(product.PriceAmount, ",", "."), " ")[0],
					RAM:   strings.Split(product.Ram, " ")[0],
					VCPU:  product.VCpu,
				},
			}
			key = ClusterRegion + "," + productPricing.NodeAttributes.Type + "," + productPricing.NodeAttributes.OS
		}

		// create a key similiar to the ones created with otcKey.Features()
		// so that the pricing data can be fetched using an otcKey
		log.Info("product \"" + key + "\" found")

		otc.Pricing[key] = productPricing
		otc.ValidPricingKeys[key] = true
	}

	return nil
}

func (otc *OTC) NetworkPricing() (*models.Network, error) {
	cpricing, err := otc.Config.GetCustomPricingData()
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

// NodePricing(Key) (*Node, PricingMetadata, error)
// Read the keys features and determine the price of the Node described by
// the key to construct a Pricing Node object to return and work with.
func (otc *OTC) NodePricing(k models.Key) (*models.Node, models.PricingMetadata, error) {
	otc.DownloadPricingDataLock.RLock()
	defer otc.DownloadPricingDataLock.RUnlock()

	key := k.Features()
	meta := models.PricingMetadata{}

	log.Info("looking for pricing data of node with key features " + key)
	pricing, ok := otc.Pricing[key]
	if ok {
		// The pricing key was found in the pricing list of the otc provider.
		// Now create a pricing node from that data and return it.
		log.Info("pricing data found")
		return otc.createNode(pricing, k)
	} else if _, ok := otc.ValidPricingKeys[key]; ok {
		// The pricing key is actually valid, but somehow it could not be found.
		// Try re-downloading the pricing data to check for changes.
		log.Info("key is valid, but no associated pricing data could be found; trying to re-download pricing data")
		otc.DownloadPricingDataLock.RUnlock()
		err := otc.DownloadPricingData()
		otc.DownloadPricingDataLock.RLock()
		if err != nil {
			return &models.Node{
				Cost:             otc.BaseCPUPrice,
				BaseCPUPrice:     otc.BaseCPUPrice,
				BaseRAMPrice:     otc.BaseRAMPrice,
				BaseGPUPrice:     otc.BaseGPUPrice,
				UsesBaseCPUPrice: true,
			}, meta, err
		}
		pricing, ok = otc.Pricing[key]
		if !ok {
			// The given key does not exist in OTC or locally, return a default pricing node.
			return &models.Node{
				Cost:             otc.BaseCPUPrice,
				BaseCPUPrice:     otc.BaseCPUPrice,
				BaseRAMPrice:     otc.BaseRAMPrice,
				BaseGPUPrice:     otc.BaseGPUPrice,
				UsesBaseCPUPrice: true,
			}, meta, fmt.Errorf("unable to find any Pricing data for \"%s\"", key)
		}
		// The local pricing date was just outdated.
		log.Info("pricing data found after re-download")
		return otc.createNode(pricing, k)
	} else {
		// The given key is not valid, fall back to base pricing (handled by the costmodel)?
		log.Info("given key \"" + key + "\" is invalid; falling back to default pricing")
		return nil, meta, fmt.Errorf("invalid Pricing Key \"%s\"", key)
	}
}

// create a Pricing Node from the internal pricing struct and a key describing the kubernetes node
func (otc *OTC) createNode(pricing *OTCPricing, key models.Key) (*models.Node, models.PricingMetadata, error) {
	// aws does some fancy stuff here, but it probably isn't that necessary

	// so just return the pricing node constructed directly from the internal struct
	meta := models.PricingMetadata{}
	return &models.Node{
		Cost:         pricing.NodeAttributes.Price,
		VCPU:         pricing.NodeAttributes.VCPU,
		RAM:          pricing.NodeAttributes.RAM,
		BaseCPUPrice: otc.BaseCPUPrice,
		BaseRAMPrice: otc.BaseRAMPrice,
		BaseGPUPrice: otc.BaseGPUPrice,
	}, meta, nil
}

// give the order to read the custom provider config file
func (otc *OTC) GetConfig() (*models.CustomPricing, error) {
	c, err := otc.Config.GetCustomPricingData()
	if err != nil {
		return nil, err
	}
	return c, nil
}

// load balancer cost
// taken straight up from aws
func (otc *OTC) LoadBalancerPricing() (*models.LoadBalancer, error) {
	return &models.LoadBalancer{
		Cost: 0.05,
	}, nil
}

// returns general info about the cluster
// This method HAS to be overwritten as long as the CustomProvider
// Field of the OTC struct is not set when initializing the provider
// in "provider.go" (see all the other providers).
func (otc *OTC) ClusterInfo() (map[string]string, error) {
	c, err := otc.GetConfig()
	if err != nil {
		return nil, err
	}
	m := make(map[string]string)
	m["name"] = "OTC Cluster #1"
	if clusterName := otc.getClusterName(c); clusterName != "" {
		m["name"] = clusterName
	}
	m["provider"] = opencost.OTCProvider
	m["account"] = c.ProjectID
	m["region"] = otc.ClusterRegion
	m["remoteReadEnabled"] = strconv.FormatBool(env.IsRemoteEnabled())
	m["id"] = env.GetClusterID()
	return m, nil
}

func (otc *OTC) getClusterName(cfg *models.CustomPricing) string {
	if cfg.ClusterName != "" {
		return cfg.ClusterName
	}
	for _, node := range otc.Clientset.GetAllNodes() {
		if clusterName, ok := node.Labels["name"]; ok {
			return clusterName
		}
	}
	return ""
}

// search for pricing data matching the given persistent volume key
// in the provider's pricing list and return it
func (otc *OTC) PVPricing(pvk models.PVKey) (*models.PV, error) {
	pricing, ok := otc.Pricing[pvk.Features()]
	if !ok {
		log.Info("Persistent Volume pricing not found for features \"" + pvk.Features() + "\"")
		log.Info("continuing with pricing for \"eu-de,vss.ssd\"")
		pricing, ok = otc.Pricing["eu-de,vss.ssd"]
		if !ok {
			log.Errorf("something went wrong, the DownloadPricing method probably didn't execute correctly")
			return &models.PV{}, nil
		}
	}

	// otc pv pricing is in the format: price per GB per month
	// this convertes that to: GB price per hour
	hourly, err := strconv.ParseFloat(pricing.PVAttributes.Price, 32)
	if err != nil {
		return &models.PV{}, err
	}
	hourly = hourly / 730

	return &models.PV{
		Cost:  fmt.Sprintf("%v", hourly),
		Class: pricing.PVAttributes.Type,
	}, nil

}

// TODO: Implement method
func (otc *OTC) GetAddresses() ([]byte, error) {
	return []byte{}, nil
}

// TODO: Implement method
func (otc *OTC) GetDisks() ([]byte, error) {
	return []byte{}, nil
}

// TODO: Implement method
func (otc *OTC) GetOrphanedResources() ([]models.OrphanedResource, error) {
	return []models.OrphanedResource{}, nil
}

// TODO: Implement method
func (otc *OTC) AllNodePricing() (interface{}, error) {
	return nil, nil
}

// TODO: Implement method
func (otc *OTC) UpdateConfig(r io.Reader, updateType string) (*models.CustomPricing, error) {
	return &models.CustomPricing{}, nil
}

// TODO: Implement method
func (otc *OTC) UpdateConfigFromConfigMap(configMap map[string]string) (*models.CustomPricing, error) {
	return &models.CustomPricing{}, nil
}

// TODO: Implement method
func (otc *OTC) GetManagementPlatform() (string, error) {
	return "", nil
}

// TODO: Implement method
func (otc *OTC) GetLocalStorageQuery(start, end time.Duration, isPVC, isDeleted bool) string {
	return ""
}

// TODO: Implement method
func (otc *OTC) ApplyReservedInstancePricing(nodes map[string]*models.Node) {
}

func (otc *OTC) ServiceAccountStatus() *models.ServiceAccountStatus {
	return &models.ServiceAccountStatus{
		Checks: []*models.ServiceAccountCheck{},
	}
}

// TODO: Implement method
func (otc *OTC) PricingSourceStatus() map[string]*models.PricingSource {
	return map[string]*models.PricingSource{}
}

// TODO: Implement method
func (otc *OTC) ClusterManagementPricing() (string, float64, error) {
	return "", 0.0, nil
}

func (otc *OTC) CombinedDiscountForNode(nodeType string, reservedInstance bool, defaultDiscount, negotiatedDiscount float64) float64 {
	return 1.0 - ((1.0 - defaultDiscount) * (1.0 - negotiatedDiscount))
}

// Regions retrieved from https://www.open-telekom-cloud.com/de/business-navigator/hochverfuegbare-rechenzentren
var otcRegions = []string{
	"eu-de",
	"eu-nl",
}

func (otc *OTC) Regions() []string {
	regionOverrides := env.GetRegionOverrideList()
	if len(regionOverrides) > 0 {
		log.Debugf("Overriding OTC regions with configured region list: %+v", regionOverrides)
		return regionOverrides
	}
	return otcRegions
}

// PricingSourceSummary returns the pricing source summary for the provider.
// The summary represents what was _parsed_ from the pricing source, not what
// was returned from the relevant API.
func (otc *OTC) PricingSourceSummary() interface{} {
	// encode the pricing source summary as a JSON string
	return otc.Pricing
}
