package alibaba

import (
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aliyun/alibaba-cloud-sdk-go/sdk"
	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/auth/credentials"
	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/auth/signers"
	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/requests"
	"github.com/opencost/opencost/core/pkg/env"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/fileutil"
	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/core/pkg/util/stringutil"
	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/cloud/utils"
	"github.com/opencost/opencost/pkg/clustercache"

	ocenv "github.com/opencost/opencost/pkg/env"
	"golang.org/x/exp/slices"
)

const (
	ALIBABA_ECS_PRODUCT_CODE                   = "ecs"
	ALIBABA_ECS_VERSION                        = "2014-05-26"
	ALIBABA_ECS_DOMAIN                         = "ecs.aliyuncs.com"
	ALIBABA_DESCRIBE_PRICE_API_ACTION          = "DescribePrice"
	ALIBABA_DESCRIBE_DISK_API_ACTION           = "DescribeDisks"
	ALIBABA_INSTANCE_RESOURCE_TYPE             = "instance"
	ALIBABA_DISK_RESOURCE_TYPE                 = "disk"
	ALIBABA_PAY_AS_YOU_GO_BILLING              = "Pay-As-You-Go"
	ALIBABA_SUBSCRIPTION_BILLING               = "Subscription"
	ALIBABA_PREEMPTIBLE_BILLING                = "Preemptible"
	ALIBABA_OPTIMIZE_KEYWORD                   = "optimize"
	ALIBABA_NON_OPTIMIZE_KEYWORD               = "nonoptimize"
	ALIBABA_HOUR_PRICE_UNIT                    = "Hour"
	ALIBABA_MONTH_PRICE_UNIT                   = "Month"
	ALIBABA_YEAR_PRICE_UNIT                    = "Year"
	ALIBABA_UNKNOWN_INSTANCE_FAMILY_TYPE       = "unknown"
	ALIBABA_NOT_SUPPORTED_INSTANCE_FAMILY_TYPE = "unsupported"
	ALIBABA_DISK_CLOUD_ESSD_CATEGORY           = "cloud_essd"
	ALIBABA_DISK_CLOUD_CATEGORY                = "cloud"
	ALIBABA_DATA_DISK_CATEGORY                 = "data"
	ALIBABA_SYSTEM_DISK_CATEGORY               = "system"
	ALIBABA_DATA_DISK_PREFIX                   = "DataDisk"
	ALIBABA_PV_CLOUD_DISK_TYPE                 = "CloudDisk"
	ALIBABA_PV_NAS_TYPE                        = "NAS"
	ALIBABA_PV_OSS_TYPE                        = "OSS"
	ALIBABA_DEFAULT_DATADISK_SIZE              = "2000"
	ALIBABA_DISK_TOPOLOGY_REGION_LABEL         = "topology.diskplugin.csi.alibabacloud.com/region"
	ALIBABA_DISK_TOPOLOGY_ZONE_LABEL           = "topology.diskplugin.csi.alibabacloud.com/zone"
)

var (
	// Regular expression to get the numerical value of PV suffix with GiB from *v1.PersistentVolume.
	sizeRegEx = regexp.MustCompile("(.*?)Gi")
)

// Variable to keep track of instance families that fail in DescribePrice API due improper defaulting of systemDisk if the information is not available
var alibabaDefaultToCloudEssd = []string{"g6e", "r6e"}

var alibabaRegions = []string{
	"cn-qingdao",
	"cn-beijing",
	"cn-zhangjiakou",
	"cn-huhehaote",
	"cn-wulanchabu",
	"cn-hangzhou",
	"cn-shanghai",
	"cn-nanjing",
	"cn-shenzhen",
	"cn-heyuan",
	"cn-guangzhou",
	"cn-fuzhou",
	"cn-wuhan-lr",
	"cn-chengdu",
	"cn-hongkong",
	"ap-northeast-1",
	"ap-northeast-2",
	"ap-southeast-1",
	"ap-southeast-2",
	"ap-southeast-3",
	"ap-southeast-6",
	"ap-southeast-5",
	"ap-south-1",
	"ap-southeast-7",
	"us-east-1",
	"us-west-1",
	"eu-west-1",
	"me-east-1",
	"me-central-1",
	"eu-central-1",
}

// AlibabaInfo contains configuration for Alibaba's CUR integration
// Deprecated: v1.104 Use BOAConfiguration instead
type AlibabaInfo struct {
	AlibabaClusterRegion    string `json:"ClusterRegion"`
	AlibabaServiceKeyName   string `json:"serviceKeyName"`
	AlibabaServiceKeySecret string `json:"serviceKeySecret"`
	AlibabaAccountID        string `json:"accountID"`
}

// IsEmpty returns true if all fields in config are empty, false if not.
func (ai *AlibabaInfo) IsEmpty() bool {
	return ai.AlibabaClusterRegion == "" &&
		ai.AlibabaServiceKeyName == "" &&
		ai.AlibabaServiceKeySecret == "" &&
		ai.AlibabaAccountID == ""
}

// AlibabaAccessKey holds Alibaba credentials parsing from the service-key.json file.
// Deprecated: v1.104 Use AccessKey instead
type AlibabaAccessKey struct {
	AccessKeyID     string `json:"alibaba_access_key_id"`
	SecretAccessKey string `json:"alibaba_secret_access_key"`
}

// Slim Version of k8s disk assigned to a node or PV.
type SlimK8sDisk struct {
	DiskType         string
	RegionID         string
	PriceUnit        string
	SizeInGiB        string
	DiskCategory     string
	PerformanceLevel string
	ProviderID       string
	StorageClass     string
}

func NewSlimK8sDisk(diskType, regionID, priceUnit, diskCategory, performanceLevel, providerID, storageClass, sizeInGiB string) *SlimK8sDisk {
	return &SlimK8sDisk{
		DiskType:         diskType,
		RegionID:         regionID,
		PriceUnit:        priceUnit,
		SizeInGiB:        sizeInGiB,
		DiskCategory:     diskCategory,
		PerformanceLevel: performanceLevel,
		ProviderID:       providerID,
		StorageClass:     storageClass,
	}
}

// Slim version of a k8s v1.node just to pass along the object of this struct instead of constant getting the labels from within v1.Node & unit testing.
type SlimK8sNode struct {
	InstanceType       string
	RegionID           string
	PriceUnit          string
	MemorySizeInKiB    string // TO-DO : Possible to convert to float?
	IsIoOptimized      bool
	OSType             string
	ProviderID         string
	SystemDisk         *SlimK8sDisk
	InstanceTypeFamily string // Bug in DescribePrice, doesn't default to enhanced type correctly and you get an error in DescribePrice to get around need the family of the InstanceType.
}

func NewSlimK8sNode(instanceType, regionID, priceUnit, memorySizeInKiB, osType, providerID, instanceTypeFamily string, isIOOptimized bool, systemDiskInfo *SlimK8sDisk) *SlimK8sNode {
	return &SlimK8sNode{
		InstanceType:       instanceType,
		RegionID:           regionID,
		PriceUnit:          priceUnit,
		MemorySizeInKiB:    memorySizeInKiB,
		IsIoOptimized:      isIOOptimized,
		OSType:             osType,
		SystemDisk:         systemDiskInfo,
		ProviderID:         providerID,
		InstanceTypeFamily: instanceTypeFamily,
	}
}

// AlibabaNodeAttributes represents metadata about the Node in its pricing information.
// Basic Attributes needed atleast to get the key, Some attributes from k8s Node response
// be populated directly into *Node object.
type AlibabaNodeAttributes struct {
	// InstanceType represents the type of instance.
	InstanceType string `json:"instanceType"`
	// MemorySizeInKiB represents the size of memory of instance.
	MemorySizeInKiB string `json:"memorySizeInKiB"`
	// IsIoOptimized represents the if instance is I/O optimized.
	IsIoOptimized bool `json:"isIoOptimized"`
	// OSType represents the OS installed in the Instance.
	OSType string `json:"osType"`
	// SystemDiskCategory represents the exact category of the system disk attached to the node.
	SystemDiskCategory string `json:"systemDiskCategory"`
	// SystemDiskSizeInGiB represents the size of the system disk attached to the node.
	SystemDiskSizeInGiB string `json:"systemDiskSizeInGiB"`
	// SystemDiskPerformanceLevel represents the performance level of the system disk attached to the node.
	SystemDiskPerformanceLevel string `json:"systemPerformanceLevel"`
}

func NewAlibabaNodeAttributes(node *SlimK8sNode) *AlibabaNodeAttributes {
	if node == nil {
		return nil
	}
	var diskCategory, sizeInGiB, performanceLevel string
	if node.SystemDisk != nil {
		diskCategory = node.SystemDisk.DiskCategory
		sizeInGiB = node.SystemDisk.SizeInGiB
		performanceLevel = node.SystemDisk.PerformanceLevel
	}
	return &AlibabaNodeAttributes{
		InstanceType:               node.InstanceType,
		MemorySizeInKiB:            node.MemorySizeInKiB,
		IsIoOptimized:              node.IsIoOptimized,
		OSType:                     node.OSType,
		SystemDiskCategory:         diskCategory,
		SystemDiskSizeInGiB:        sizeInGiB,
		SystemDiskPerformanceLevel: performanceLevel,
	}
}

// AlibabaPVAttributes represents metadata the PV in its pricing information.
// Basic Attributes needed atleast to get the keys. Some attributes from k8s PV response
// be populated directly into *PV object.
type AlibabaPVAttributes struct {
	// PVType can be Cloud Disk, NetWork Attached Storage(NAS) or Object Storage Service (OSS).
	// Represents the way the PV was attached
	PVType string `json:"pvType"`
	// PVSubType represent the sub category of PVType. This is Data in case of Cloud Disk.
	PVSubType string `json:"pvSubType"`
	// Example for PVCategory with cloudDisk PVType are cloud, cloud_efficiency, cloud_ssd,
	// ephemeral_ssd and cloud_essd. If not present returns empty.
	PVCategory string `json:"pvCategory"`
	// Example for PerformanceLevel with cloudDisk PVType are PL0,PL1,PL2 &PL3. If not present returns empty.
	PVPerformanceLevel string `json:"performanceLevel"`
	// The Size of the PV in terms of GiB
	SizeInGiB string `json:"sizeInGiB"`
}

// TO-Do: next iteration of Alibaba provider support NetWork Attached Storage(NAS) and Object Storage Service (OSS type PVs).
// Currently defaulting to cloudDisk with provision to add work in future.
func NewAlibabaPVAttributes(disk *SlimK8sDisk) *AlibabaPVAttributes {
	if disk == nil {
		return nil
	}
	return &AlibabaPVAttributes{
		PVType:             ALIBABA_PV_CLOUD_DISK_TYPE,
		PVSubType:          disk.DiskType,
		PVCategory:         disk.DiskCategory,
		PVPerformanceLevel: disk.PerformanceLevel,
		SizeInGiB:          disk.SizeInGiB,
	}
}

// Stage 1 support will be Pay-As-You-Go with HourlyPrice equal to TradePrice with PriceUnit as Hour
// TO-DO: Subscription and Premptible support, Information can be gathered from describing instance for subscription type
// and spotprice can be gather from DescribeSpotPriceHistory API.
// TO-DO: how would you calculate hourly price for subscription type, is it PRICE_YEARLY/HOURS_IN_THE_YEAR|MONTH?
type AlibabaPricingDetails struct {
	// Represents hourly price for the given Alibaba cloud Product.
	HourlyPrice float32 `json:"hourlyPrice"`
	// Represents the unit in which Alibaba Product is billed can be Hour, Month or Year based on the billingMethod.
	PriceUnit string `json:"priceUnit"`
	// Original Price paid to acquire the Alibaba Product.
	TradePrice float32 `json:"tradePrice"`
	// Represents the currency unit of the price for billing Alibaba Product.
	CurrencyCode string `json:"currencyCode"`
}

func NewAlibabaPricingDetails(hourlyPrice float32, priceUnit string, tradePrice float32, currencyCode string) *AlibabaPricingDetails {
	return &AlibabaPricingDetails{
		HourlyPrice:  hourlyPrice,
		PriceUnit:    priceUnit,
		TradePrice:   tradePrice,
		CurrencyCode: currencyCode,
	}
}

// AlibabaPricingTerms can have three types of supported billing method Pay-As-You-Go, Subscription and Premptible
type AlibabaPricingTerms struct {
	BillingMethod  string                 `json:"billingMethod"`
	PricingDetails *AlibabaPricingDetails `json:"pricingDetails"`
}

func NewAlibabaPricingTerms(billingMethod string, pricingDetails *AlibabaPricingDetails) *AlibabaPricingTerms {
	return &AlibabaPricingTerms{
		BillingMethod:  billingMethod,
		PricingDetails: pricingDetails,
	}
}

// Alibaba Pricing struct carry the Attributes and pricing information for Node or PV
type AlibabaPricing struct {
	NodeAttributes *AlibabaNodeAttributes
	PVAttributes   *AlibabaPVAttributes
	PricingTerms   *AlibabaPricingTerms
	Node           *models.Node
	PV             *models.PV
}

// Alibaba cloud's Provider struct
type Alibaba struct {
	// Data to store Alibaba cloud's pricing struct, key in the map represents exact match to
	// node.features() or pv.features for easy lookup
	Pricing map[string]*AlibabaPricing
	// Lock Needed to provide thread safe
	DownloadPricingDataLock sync.RWMutex
	Clientset               clustercache.ClusterCache
	Config                  models.ProviderConfig
	ServiceAccountChecks    *models.ServiceAccountChecks
	ClusterAccountId        string
	ClusterRegion           string

	// The following fields are unexported because of avoiding any leak of secrets of these keys.
	// Alibaba Access key used specifically in signer interface used to sign API calls
	accessKey *credentials.AccessKeyCredential
	// Map of regionID to sdk.client to call API for that region
	clients map[string]*sdk.Client
}

// GetAlibabaAccessKey return the Access Key used to interact with the Alibaba cloud, if not set it
// set it first by looking at env variables else load it from secret files.
func (alibaba *Alibaba) GetAlibabaAccessKey() (*credentials.AccessKeyCredential, error) {
	if alibaba.accessKeyisLoaded() {
		return alibaba.accessKey, nil
	}

	config, err := alibaba.GetConfig()
	if err != nil {
		return nil, fmt.Errorf("error getting the default config for Alibaba Cloud provider: %w", err)
	}

	if config.AlibabaServiceKeyName == "" {
		config.AlibabaServiceKeyName = ocenv.GetAlibabaAccessKeyID()
	}
	if config.AlibabaServiceKeySecret == "" {
		config.AlibabaServiceKeySecret = ocenv.GetAlibabaAccessKeySecret()
	}

	if config.AlibabaServiceKeyName == "" && config.AlibabaServiceKeySecret == "" {
		log.Debugf("missing service key values for Alibaba cloud integration attempting to use service account integration")
		err := alibaba.loadAlibabaAuthSecretAndSetEnv(true)
		if err != nil {
			return nil, fmt.Errorf("unable to set the Alibaba Cloud key/secret from config file %w", err)
		}
		config.AlibabaServiceKeyName = ocenv.GetAlibabaAccessKeyID()
		config.AlibabaServiceKeySecret = ocenv.GetAlibabaAccessKeySecret()
	}

	if config.AlibabaServiceKeyName == "" && config.AlibabaServiceKeySecret == "" {
		return nil, fmt.Errorf("failed to get the access key for the current alibaba account")
	}

	// At this point either user is using the alibaba key and secret from secret passed in helm config if not he will use the secret that is passed in custom pricing
	// There's no check at this time for if the custom pricing key and secret is valid and that's on the user else there will be errors recorded.
	// Key and secret passed in config will supersede key and secret passed while installing Closed source helm chart.
	alibaba.accessKey = &credentials.AccessKeyCredential{AccessKeyId: config.AlibabaServiceKeyName, AccessKeySecret: config.AlibabaServiceKeySecret}

	return alibaba.accessKey, nil
}

func (alibaba *Alibaba) GetAlibabaCloudInfo() (*AlibabaInfo, error) {
	config, err := alibaba.GetConfig()
	if err != nil {
		return nil, fmt.Errorf("could not retrieve AlibabaCloudInfo %s", err)
	}

	aak, err := alibaba.GetAlibabaAccessKey()
	if err != nil {
		return nil, err
	}

	return &AlibabaInfo{
		AlibabaClusterRegion:    config.AlibabaClusterRegion,
		AlibabaServiceKeyName:   aak.AccessKeyId,
		AlibabaServiceKeySecret: aak.AccessKeySecret,
		AlibabaAccountID:        config.ProjectID,
	}, nil
}

// DownloadPricingData satisfies the provider interface and downloads the prices for Node instances and PVs.
func (alibaba *Alibaba) DownloadPricingData() error {
	alibaba.DownloadPricingDataLock.Lock()
	defer alibaba.DownloadPricingDataLock.Unlock()

	var aak *credentials.AccessKeyCredential
	var err error

	if !alibaba.accessKeyisLoaded() {
		aak, err = alibaba.GetAlibabaAccessKey()
		if err != nil {
			return fmt.Errorf("unable to get the access key information: %w", err)
		}
	} else {
		aak = alibaba.accessKey
	}

	c, err := alibaba.Config.GetCustomPricingData()
	if err != nil {
		return fmt.Errorf("error downloading default pricing data: %w", err)
	}

	// Get all the nodes from Alibaba cluster.
	nodeList := alibaba.Clientset.GetAllNodes()

	var client *sdk.Client
	var signer *signers.AccessKeySigner
	var ok bool
	var lookupKey string
	alibaba.clients = make(map[string]*sdk.Client)
	alibaba.Pricing = make(map[string]*AlibabaPricing)
	for _, node := range nodeList {
		pricingObj := &AlibabaPricing{}
		slimK8sNode := generateSlimK8sNodeFromV1Node(node)

		if client, ok = alibaba.clients[slimK8sNode.RegionID]; !ok {
			client, err = sdk.NewClientWithAccessKey(slimK8sNode.RegionID, aak.AccessKeyId, aak.AccessKeySecret)
			if err != nil {
				return fmt.Errorf("unable to initiate alibaba cloud sdk client for region %s : %w", slimK8sNode.RegionID, err)
			}
			alibaba.clients[slimK8sNode.RegionID] = client
		}
		signer = signers.NewAccessKeySigner(aak)

		// Adjust the system Disk information of a Node by retrieving the details of associated disk. If unable to retrieve set it to empty
		// system disk to pass through and use defaults with Alibaba pricing API.
		instanceID := getInstanceIDFromProviderID(slimK8sNode.ProviderID)
		slimK8sNode.SystemDisk = getSystemDiskInfoOfANode(instanceID, slimK8sNode.RegionID, client, signer)

		lookupKey, err = determineKeyForPricing(slimK8sNode)
		if err != nil {
			return fmt.Errorf("unable to determine key for pricing: %w", err)
		}

		if _, ok := alibaba.Pricing[lookupKey]; ok {
			log.Debugf("Pricing information for node with same features %s already exists hence skipping", lookupKey)
			continue
		}

		pricingObj, err = processDescribePriceAndCreateAlibabaPricing(client, slimK8sNode, signer, c)

		if err != nil {
			return fmt.Errorf("failed to create pricing information for node with type %s with error: %w", slimK8sNode.InstanceType, err)
		}
		alibaba.Pricing[lookupKey] = pricingObj
	}

	// set the first occurrence of region from the node
	if alibaba.ClusterRegion == "" {
		for _, node := range nodeList {
			if regionID, ok := node.Labels["topology.kubernetes.io/region"]; ok {
				alibaba.ClusterRegion = regionID
				break
			}
		}
	}

	// PV pricing for only Cloud Disk for now.
	// TO-DO: Support both NAS(Network Attached storage) and OSS(Object Storage Service) type PVs

	pvList := alibaba.Clientset.GetAllPersistentVolumes()

	for _, pv := range pvList {
		pvRegion := determinePVRegion(pv)
		if pvRegion == "" {
			pvRegion = alibaba.ClusterRegion
		}
		pricingObj := &AlibabaPricing{}
		slimK8sDisk := generateSlimK8sDiskFromV1PV(pv, pvRegion)
		lookupKey, err = determineKeyForPricing(slimK8sDisk)
		if err != nil {
			return fmt.Errorf("unable to determine key for pricing: %w", err)
		}
		if _, ok := alibaba.Pricing[lookupKey]; ok {
			log.Debugf("Pricing information for pv with same features %s already exists hence skipping", lookupKey)
			continue
		}
		if client, ok = alibaba.clients[slimK8sDisk.RegionID]; !ok {
			client, err = sdk.NewClientWithAccessKey(slimK8sDisk.RegionID, aak.AccessKeyId, aak.AccessKeySecret)
			if err != nil {
				return fmt.Errorf("unable to initiate alibaba cloud sdk client for region %s : %w", slimK8sDisk.RegionID, err)
			}
			alibaba.clients[slimK8sDisk.RegionID] = client
		}
		signer = signers.NewAccessKeySigner(aak)
		pricingObj, err = processDescribePriceAndCreateAlibabaPricing(client, slimK8sDisk, signer, c)
		if err != nil {
			return fmt.Errorf("failed to create pricing information for pv with category %s with error: %w", slimK8sDisk.DiskCategory, err)
		}
		alibaba.Pricing[lookupKey] = pricingObj
	}

	return nil
}

// AllNodePricing returns all the pricing data for all nodes and pvs
func (alibaba *Alibaba) AllNodePricing() (interface{}, error) {
	alibaba.DownloadPricingDataLock.RLock()
	defer alibaba.DownloadPricingDataLock.RUnlock()
	return alibaba.Pricing, nil
}

// NodePricing gives pricing information of a specific node given by the key
func (alibaba *Alibaba) NodePricing(key models.Key) (*models.Node, models.PricingMetadata, error) {
	alibaba.DownloadPricingDataLock.RLock()
	defer alibaba.DownloadPricingDataLock.RUnlock()

	// Get node features for the key
	keyFeature := key.Features()

	meta := models.PricingMetadata{}

	pricing, ok := alibaba.Pricing[keyFeature]
	if !ok {
		keys := make([]string, 0, len(alibaba.Pricing))
		for k := range alibaba.Pricing {
			keys = append(keys, k)
		}
		kf := key.(*AlibabaNodeKey)
		// Try to look up pricing with no disk attached
		pricing, ok = alibaba.Pricing[kf.FeaturesWithOtherDisk("")]
		if !ok {
			log.Errorf("Node pricing information not found for node with feature: %s . Existing keys are: %+v", keyFeature, keys)
			return nil, meta, fmt.Errorf("Node pricing information not found for node with feature: %s letting it use default values", keyFeature)
		}
	}

	log.Debugf("returning the node price for the node with feature: %s", keyFeature)
	returnNode := pricing.Node

	return returnNode, meta, nil
}

// PVPricing gives a pricing information of a specific PV given by PVkey
func (alibaba *Alibaba) PVPricing(pvk models.PVKey) (*models.PV, error) {
	alibaba.DownloadPricingDataLock.RLock()
	defer alibaba.DownloadPricingDataLock.RUnlock()

	keyFeature := pvk.Features()

	pricing, ok := alibaba.Pricing[keyFeature]

	if !ok {
		log.Debugf("Persistent Volume pricing not found for PV with feature: %s", keyFeature)
		return nil, fmt.Errorf("Persistent Volume pricing not found for PV with feature: %s letting it use default values", keyFeature)
	}

	log.Debugf("returning the PV price for the node with feature: %s", keyFeature)
	return pricing.PV, nil
}

// Inter zone and Inter region network cost are defaulted based on https://www.alibabacloud.com/help/en/cloud-data-transmission/latest/cross-region-data-transfers
// Internet cost is default based on https://www.alibabacloud.com/help/en/elastic-compute-service/latest/public-bandwidth to $0.123
func (alibaba *Alibaba) NetworkPricing() (*models.Network, error) {
	cpricing, err := alibaba.Config.GetCustomPricingData()
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

// Alibaba loadbalancer has three different types https://www.alibabacloud.com/product/server-load-balancer,
// defaulted price to classic load balancer https://www.alibabacloud.com/help/en/server-load-balancer/latest/pay-as-you-go.
func (alibaba *Alibaba) LoadBalancerPricing() (*models.LoadBalancer, error) {
	cpricing, err := alibaba.Config.GetCustomPricingData()
	if err != nil {
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

func (alibaba *Alibaba) GetConfig() (*models.CustomPricing, error) {
	c, err := alibaba.Config.GetCustomPricingData()
	if err != nil {
		return nil, err
	}
	if c.Discount == "" {
		c.Discount = "0%"
	}
	if c.NegotiatedDiscount == "" {
		c.NegotiatedDiscount = "0%"
	}
	if c.ShareTenancyCosts == "" {
		c.ShareTenancyCosts = models.DefaultShareTenancyCost
	}

	return c, nil
}

// Load once and cache the result (even on failure). This is an install time secret, so
// we don't expect the secret to change. If it does, however, we can force reload using
// the input parameter.
func (alibaba *Alibaba) loadAlibabaAuthSecretAndSetEnv(force bool) error {
	if !force && alibaba.accessKeyisLoaded() {
		return nil
	}

	exists, err := fileutil.FileExists(models.AuthSecretPath)
	if !exists || err != nil {
		return fmt.Errorf("failed to locate service account file: %s with err: %w", models.AuthSecretPath, err)
	}

	result, err := os.ReadFile(models.AuthSecretPath)
	if err != nil {
		return fmt.Errorf("failed to read service account file: %s with err: %w", models.AuthSecretPath, err)
	}

	var ak *AlibabaAccessKey
	err = json.Unmarshal(result, &ak)
	if err != nil {
		return fmt.Errorf("failed to unmarshall access key id and access key secret with err: %w", err)
	}

	err = env.Set(ocenv.AlibabaAccessKeyIDEnvVar, ak.AccessKeyID)
	if err != nil {
		return fmt.Errorf("failed to set environment variable: %s with err: %w", ocenv.AlibabaAccessKeyIDEnvVar, err)
	}
	err = env.Set(ocenv.AlibabaAccessKeySecretEnvVar, ak.SecretAccessKey)
	if err != nil {
		return fmt.Errorf("failed to set environment variable: %s with err: %w", ocenv.AlibabaAccessKeySecretEnvVar, err)
	}

	alibaba.accessKey = &credentials.AccessKeyCredential{
		AccessKeyId:     ak.AccessKeyID,
		AccessKeySecret: ak.SecretAccessKey,
	}
	return nil
}

// Regions returns a current supported list of Alibaba regions
func (alibaba *Alibaba) Regions() []string {

	regionOverrides := ocenv.GetRegionOverrideList()

	if len(regionOverrides) > 0 {
		log.Debugf("Overriding Alibaba regions with configured region list: %+v", regionOverrides)
		return regionOverrides
	}

	return alibabaRegions
}

// ClusterInfo returns information about Alibaba Cloud cluster, as provided by metadata.
func (alibaba *Alibaba) ClusterInfo() (map[string]string, error) {

	c, err := alibaba.GetConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to getConfig with err: %w", err)
	}

	var clusterName string
	if c.ClusterName != "" {
		clusterName = c.ClusterName
	}

	// Set it to environment clusterID if not set at this point
	if clusterName == "" {
		clusterName = ocenv.GetClusterID()
	}

	m := make(map[string]string)
	m["name"] = clusterName
	m["provider"] = opencost.AlibabaProvider
	m["project"] = alibaba.ClusterAccountId
	m["region"] = alibaba.ClusterRegion
	m["id"] = ocenv.GetClusterID()
	return m, nil
}

// Will look at this in Next PR if needed
func (alibaba *Alibaba) GetAddresses() ([]byte, error) {
	return nil, nil
}

// Will look at this in Next PR if needed
func (alibaba *Alibaba) GetDisks() ([]byte, error) {
	return nil, nil
}

func (alibaba *Alibaba) GetOrphanedResources() ([]models.OrphanedResource, error) {
	return nil, errors.New("not implemented")
}

func (alibaba *Alibaba) UpdateConfig(r io.Reader, updateType string) (*models.CustomPricing, error) {
	return alibaba.Config.Update(func(c *models.CustomPricing) error {
		if updateType != "" {
			return fmt.Errorf("UpdateConfig for Alibaba Provider doesn't support updateType %s at this time", updateType)

		} else {
			a := make(map[string]interface{})
			err := json.NewDecoder(r).Decode(&a)
			if err != nil {
				return err
			}
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
		}

		if ocenv.IsRemoteEnabled() {
			err := utils.UpdateClusterMeta(ocenv.GetClusterID(), c.ClusterName)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (alibaba *Alibaba) UpdateConfigFromConfigMap(cm map[string]string) (*models.CustomPricing, error) {
	return alibaba.Config.UpdateFromMap(cm)
}

// Will look at this in Next PR if needed
func (alibaba *Alibaba) GetManagementPlatform() (string, error) {
	return "", nil
}

// Will look at this in Next PR if needed
func (alibaba *Alibaba) GetLocalStorageQuery(window, offset time.Duration, rate bool, used bool) string {
	return ""
}

// Will look at this in Next PR if needed
func (alibaba *Alibaba) ApplyReservedInstancePricing(nodes map[string]*models.Node) {

}

// Will look at this in Next PR if needed
func (alibaba *Alibaba) ServiceAccountStatus() *models.ServiceAccountStatus {
	return &models.ServiceAccountStatus{}
}

// Will look at this in Next PR if needed
func (alibaba *Alibaba) PricingSourceStatus() map[string]*models.PricingSource {
	return map[string]*models.PricingSource{}
}

// Will look at this in Next PR if needed
func (alibaba *Alibaba) ClusterManagementPricing() (string, float64, error) {
	return "", 0.0, nil
}

// Will look at this in Next PR if needed
func (alibaba *Alibaba) CombinedDiscountForNode(string, bool, float64, float64) float64 {
	return 0.0
}

func (alibaba *Alibaba) accessKeyisLoaded() bool {
	if alibaba.accessKey == nil {
		return false
	}
	if alibaba.accessKey.AccessKeyId == "" {
		return false
	}
	if alibaba.accessKey.AccessKeySecret == "" {
		return false
	}
	return true
}

type AlibabaNodeKey struct {
	ProviderID                 string
	RegionID                   string
	InstanceType               string
	OSType                     string
	OptimizedKeyword           string //If IsIoOptimized is true use the word optimize in the Node key and if its not optimized use the word nonoptimize
	SystemDiskCategory         string
	SystemDiskSizeInGiB        string
	SystemDiskPerformanceLevel string
}

func NewAlibabaNodeKey(node *SlimK8sNode, optimizedKeyword, systemDiskCategory, systemDiskSizeInGiB, systemDiskPerfromanceLevel string) *AlibabaNodeKey {
	var providerID, regionID, instanceType, osType string
	if node != nil {
		providerID = node.ProviderID
		regionID = node.RegionID
		instanceType = node.InstanceType
		osType = node.OSType
	}
	return &AlibabaNodeKey{
		ProviderID:                 providerID,
		RegionID:                   regionID,
		InstanceType:               instanceType,
		OSType:                     osType,
		OptimizedKeyword:           optimizedKeyword,
		SystemDiskCategory:         systemDiskCategory,
		SystemDiskSizeInGiB:        systemDiskSizeInGiB,
		SystemDiskPerformanceLevel: systemDiskPerfromanceLevel,
	}
}

func (alibabaNodeKey *AlibabaNodeKey) ID() string {
	return alibabaNodeKey.ProviderID
}

func (alibabaNodeKey *AlibabaNodeKey) Features() string {
	keyLookup := stringutil.DeleteEmptyStringsFromArray([]string{alibabaNodeKey.RegionID, alibabaNodeKey.InstanceType, alibabaNodeKey.OSType,
		alibabaNodeKey.OptimizedKeyword, alibabaNodeKey.SystemDiskCategory, alibabaNodeKey.SystemDiskSizeInGiB, alibabaNodeKey.SystemDiskPerformanceLevel})
	return strings.Join(keyLookup, "::")
}

func (alibabaNodeKey *AlibabaNodeKey) FeaturesWithOtherDisk(overrideDiskCategory string) string {
	keyLookup := stringutil.DeleteEmptyStringsFromArray([]string{alibabaNodeKey.RegionID, alibabaNodeKey.InstanceType, alibabaNodeKey.OSType,
		alibabaNodeKey.OptimizedKeyword, overrideDiskCategory, alibabaNodeKey.SystemDiskSizeInGiB, alibabaNodeKey.SystemDiskPerformanceLevel})
	return strings.Join(keyLookup, "::")
}

func (alibabaNodeKey *AlibabaNodeKey) GPUType() string {
	return ""
}

func (alibabaNodeKey *AlibabaNodeKey) GPUCount() int {
	return 0
}

// Get's the key for the k8s node input
func (alibaba *Alibaba) GetKey(mapValue map[string]string, node *clustercache.Node) models.Key {
	slimK8sNode := generateSlimK8sNodeFromV1Node(node)

	var aak *credentials.AccessKeyCredential
	var err error
	var ok bool
	var client *sdk.Client
	var signer *signers.AccessKeySigner

	optimizedKeyword := ""
	if slimK8sNode.IsIoOptimized {
		optimizedKeyword = ALIBABA_OPTIMIZE_KEYWORD
	} else {
		optimizedKeyword = ALIBABA_NON_OPTIMIZE_KEYWORD
	}

	var diskCategory, diskSizeInGiB, diskPerformanceLevel string

	if !alibaba.accessKeyisLoaded() {
		aak, err = alibaba.GetAlibabaAccessKey()
		if err != nil {
			log.Warnf("unable to set the signer for node with providerID %s to retrieve the key skipping SystemDisk Retrieval with err: %v", slimK8sNode.ProviderID, err)
			return NewAlibabaNodeKey(slimK8sNode, optimizedKeyword, diskCategory, diskSizeInGiB, diskPerformanceLevel)
		}
	} else {
		aak = alibaba.accessKey
	}

	signer = signers.NewAccessKeySigner(aak)

	if aak == nil {
		log.Warnf("unable to retrieve the Alibaba API keys for node with providerID %s hence skipping SystemDisk Retrieval", slimK8sNode.ProviderID)
		return NewAlibabaNodeKey(slimK8sNode, optimizedKeyword, diskCategory, diskSizeInGiB, diskPerformanceLevel)
	}

	if client, ok = alibaba.clients[slimK8sNode.RegionID]; !ok {
		client, err = sdk.NewClientWithAccessKey(slimK8sNode.RegionID, aak.AccessKeyId, aak.AccessKeySecret)
		if err != nil {
			log.Warnf("unable to set the client  for node with providerID %s to retrieve the key skipping SystemDisk Retrieval with err: %v", slimK8sNode.ProviderID, err)
			return NewAlibabaNodeKey(slimK8sNode, optimizedKeyword, diskCategory, diskSizeInGiB, diskPerformanceLevel)
		}
		alibaba.clients[slimK8sNode.RegionID] = client
	}

	instanceID := getInstanceIDFromProviderID(slimK8sNode.ProviderID)
	slimK8sNode.SystemDisk = getSystemDiskInfoOfANode(instanceID, slimK8sNode.RegionID, client, signer)

	if slimK8sNode.SystemDisk != nil {
		diskCategory = slimK8sNode.SystemDisk.DiskCategory
		diskSizeInGiB = slimK8sNode.SystemDisk.SizeInGiB
		diskPerformanceLevel = slimK8sNode.SystemDisk.PerformanceLevel
	}
	return NewAlibabaNodeKey(slimK8sNode, optimizedKeyword, diskCategory, diskSizeInGiB, diskPerformanceLevel)
}

type AlibabaPVKey struct {
	ProviderID        string
	RegionID          string
	PVType            string
	PVSubType         string
	PVCategory        string
	PVPerformaceLevel string
	StorageClassName  string
	SizeInGiB         string
}

func (alibaba *Alibaba) GetPVKey(pv *clustercache.PersistentVolume, parameters map[string]string, defaultRegion string) models.PVKey {
	regionID := defaultRegion
	// If default Region is not passed default it to cluster region ID.
	if defaultRegion == "" {
		regionID = alibaba.ClusterRegion
	}
	slimK8sDisk := generateSlimK8sDiskFromV1PV(pv, defaultRegion)
	return &AlibabaPVKey{
		ProviderID:        slimK8sDisk.ProviderID,
		RegionID:          regionID,
		PVType:            ALIBABA_PV_CLOUD_DISK_TYPE,
		PVSubType:         slimK8sDisk.DiskType,
		PVCategory:        slimK8sDisk.DiskCategory,
		PVPerformaceLevel: slimK8sDisk.PerformanceLevel,
		StorageClassName:  pv.Spec.StorageClassName,
		SizeInGiB:         slimK8sDisk.SizeInGiB,
	}
}

func (alibabaPVKey *AlibabaPVKey) Features() string {
	keyLookup := stringutil.DeleteEmptyStringsFromArray([]string{alibabaPVKey.RegionID, alibabaPVKey.PVSubType, alibabaPVKey.PVCategory, alibabaPVKey.PVPerformaceLevel, alibabaPVKey.SizeInGiB})
	return strings.Join(keyLookup, "::")
}

func (alibabaPVKey *AlibabaPVKey) ID() string {
	return alibabaPVKey.ProviderID
}

// Get storage class information for PV.
func (alibabaPVKey *AlibabaPVKey) GetStorageClass() string {
	return alibabaPVKey.StorageClassName
}

// Helper functions for alibabaprovider.go

// createDescribePriceACSRequest creates the HTTP GET request for the required resources' Price information,
// When supporting subscription and Premptible resources this HTTP call needs to be modified with PriceUnit information
// When supporting different new type of instances like Compute Optimized, Memory Optimized etc make sure you add the instance type
// in unit test and check if it works or not to create the ack request and processDescribePriceAndCreateAlibabaPricing function
// else more parameters need to be pulled from kubernetes node response or gather information from elsewhere and function modified.
func createDescribePriceACSRequest(i interface{}) (*requests.CommonRequest, error) {
	request := requests.NewCommonRequest()
	request.Method = requests.GET
	request.Product = ALIBABA_ECS_PRODUCT_CODE
	request.Domain = ALIBABA_ECS_DOMAIN
	request.Version = ALIBABA_ECS_VERSION
	request.Scheme = requests.HTTPS
	request.ApiName = ALIBABA_DESCRIBE_PRICE_API_ACTION
	switch i.(type) {
	case *SlimK8sNode:
		node := i.(*SlimK8sNode)
		request.QueryParams["RegionId"] = node.RegionID
		request.QueryParams["ResourceType"] = ALIBABA_INSTANCE_RESOURCE_TYPE
		request.QueryParams["InstanceType"] = node.InstanceType
		request.QueryParams["PriceUnit"] = node.PriceUnit
		if node.SystemDisk != nil {
			// Only if the required information is present it should be overridden else default it via the API
			if node.SystemDisk.DiskCategory != "" {
				request.QueryParams["SystemDisk.Category"] = node.SystemDisk.DiskCategory
			}
			if node.SystemDisk.SizeInGiB != "" {
				request.QueryParams["SystemDisk.Size"] = node.SystemDisk.SizeInGiB
			}
			if node.SystemDisk.PerformanceLevel != "" {
				request.QueryParams["SystemDisk.PerformanceLevel"] = node.SystemDisk.PerformanceLevel
			}
		} else {
			// When the system disk information is not available, and the instance family is g6e or r6e,
			// or the instance generation is 6 or above, the default disk category in DescribePrice should be cloud_essd.
			if slices.Contains(alibabaDefaultToCloudEssd, node.InstanceTypeFamily) || getInstanceFamilyGenerationFromType(node.InstanceType) > 6 {
				request.QueryParams["SystemDisk.Category"] = ALIBABA_DISK_CLOUD_ESSD_CATEGORY
			}
		}
		request.TransToAcsRequest()
		return request, nil
	case *SlimK8sDisk:
		disk := i.(*SlimK8sDisk)
		request.QueryParams["RegionId"] = disk.RegionID
		request.QueryParams["PriceUnit"] = disk.PriceUnit
		request.QueryParams["ResourceType"] = ALIBABA_DISK_RESOURCE_TYPE
		request.QueryParams[fmt.Sprintf("%s.%d.Size", ALIBABA_DATA_DISK_PREFIX, 1)] = disk.SizeInGiB
		request.QueryParams[fmt.Sprintf("%s.%d.Category", ALIBABA_DATA_DISK_PREFIX, 1)] = disk.DiskCategory
		// Performance level defaults to PL1 if not present in volume attribute.
		if disk.PerformanceLevel != "" {
			request.QueryParams[fmt.Sprintf("%s.%d.PerformanceLevel", ALIBABA_DATA_DISK_PREFIX, 1)] = disk.PerformanceLevel
		}
		request.TransToAcsRequest()
		return request, nil
	default:
		return nil, fmt.Errorf("unsupported ECS type (%T) for DescribePrice at this time", i)
	}
}

// createDescribeDisksCSRequest creates the HTTP GET Request to map the system disk to the InstanceID
func createDescribeDisksACSRequest(instanceID, regionID, diskType string) (*requests.CommonRequest, error) {
	request := requests.NewCommonRequest()
	request.Method = requests.GET
	request.Product = ALIBABA_ECS_PRODUCT_CODE
	request.Domain = ALIBABA_ECS_DOMAIN
	request.Version = ALIBABA_ECS_VERSION
	request.Scheme = requests.HTTPS
	request.ApiName = ALIBABA_DESCRIBE_DISK_API_ACTION
	request.QueryParams["RegionId"] = regionID
	request.QueryParams["InstanceId"] = instanceID
	request.QueryParams["DiskType"] = diskType
	request.TransToAcsRequest()
	return request, nil
}

// determineKeyForPricing generate a unique key from SlimK8sNode object that is constructed from v1.Node object and
// SlimK8sDisk that is constructed from v1.PersistentVolume.
func determineKeyForPricing(i interface{}) (string, error) {
	if i == nil {
		return "", fmt.Errorf("nil component passed to determine key")
	}
	switch i.(type) {
	case *SlimK8sNode:
		node := i.(*SlimK8sNode)
		var diskCategory, diskSizeInGiB, diskPerformanceLevel string
		if node.SystemDisk != nil {
			diskCategory = node.SystemDisk.DiskCategory
			diskSizeInGiB = node.SystemDisk.SizeInGiB
			diskPerformanceLevel = node.SystemDisk.PerformanceLevel
		}
		if node.IsIoOptimized {
			keyLookup := stringutil.DeleteEmptyStringsFromArray([]string{node.RegionID, node.InstanceType, node.OSType, ALIBABA_OPTIMIZE_KEYWORD, diskCategory, diskSizeInGiB, diskPerformanceLevel})
			return strings.Join(keyLookup, "::"), nil
		} else {
			keyLookup := stringutil.DeleteEmptyStringsFromArray([]string{node.RegionID, node.InstanceType, node.OSType, ALIBABA_NON_OPTIMIZE_KEYWORD, diskCategory, diskSizeInGiB, diskPerformanceLevel})
			return strings.Join(keyLookup, "::"), nil
		}
	case *SlimK8sDisk:
		disk := i.(*SlimK8sDisk)
		keyLookup := stringutil.DeleteEmptyStringsFromArray([]string{disk.RegionID, disk.DiskType, disk.DiskCategory, disk.PerformanceLevel, disk.SizeInGiB})
		return strings.Join(keyLookup, "::"), nil
	default:
		return "", fmt.Errorf("unsupported ECS type (%T) at this time", i)
	}
}

// Below structs are used to unmarshal json response of Alibaba cloud's API DescribePrice
type Price struct {
	OriginalPrice             float32 `json:"OriginalPrice"`
	ReservedInstanceHourPrice float32 `json:"ReservedInstanceHourPrice"`
	DiscountPrice             float32 `json:"DiscountPrice"`
	Currency                  string  `json:"Currency"`
	TradePrice                float32 `json:"TradePrice"`
}

type PriceInfo struct {
	Price Price `json:"Price"`
}

type DescribePriceResponse struct {
	RequestId string    `json:"RequestId"`
	PriceInfo PriceInfo `json:"PriceInfo"`
}

// processDescribePriceAndCreateAlibabaPricing processes the DescribePrice API and generates the pricing information for alibaba node resource and alibaba pv resource that's backed by cloud disk.
func processDescribePriceAndCreateAlibabaPricing(client *sdk.Client, i interface{}, signer *signers.AccessKeySigner, custom *models.CustomPricing) (pricing *AlibabaPricing, err error) {
	pricing = &AlibabaPricing{}
	var response DescribePriceResponse

	if i == nil {
		return nil, fmt.Errorf("nil component passed to process the pricing information")
	}
	switch i.(type) {
	case *SlimK8sNode:
		node := i.(*SlimK8sNode)
		req, err := createDescribePriceACSRequest(node)
		if err != nil {
			return nil, err
		}
		resp, err := client.ProcessCommonRequestWithSigner(req, signer)
		pricing.NodeAttributes = NewAlibabaNodeAttributes(node)
		if err != nil || resp.GetHttpStatus() != 200 {
			// Try again but default the disk to something else
			return nil, fmt.Errorf("unable to fetch information for node with InstanceType: %v", node.InstanceType)
		} else {
			// This is where population of Pricing happens
			err = json.Unmarshal(resp.GetHttpContentBytes(), &response)
			if err != nil {
				return nil, fmt.Errorf("unable to unmarshall json response to custom struct with err: %w", err)
			}
			// TO-DO : Ask in PR How to get the defaults is it equal to AWS/GCP defaults? And what needs to be returned
			pricing.Node = &models.Node{
				Cost:         fmt.Sprintf("%f", response.PriceInfo.Price.TradePrice),
				BaseCPUPrice: custom.CPU,
				BaseRAMPrice: custom.RAM,
				BaseGPUPrice: custom.GPU,
			}
			// TO-DO : Currently with Pay-As-You-go Offering TradePrice = HourlyPrice , When support happens to other type HourlyPrice Need to be determined.
			pricing.PricingTerms = NewAlibabaPricingTerms(ALIBABA_PAY_AS_YOU_GO_BILLING, NewAlibabaPricingDetails(response.PriceInfo.Price.TradePrice, ALIBABA_HOUR_PRICE_UNIT, response.PriceInfo.Price.TradePrice, response.PriceInfo.Price.Currency))
		}
	case *SlimK8sDisk:
		disk := i.(*SlimK8sDisk)
		req, err := createDescribePriceACSRequest(disk)
		if err != nil {
			return nil, err
		}
		resp, err := client.ProcessCommonRequestWithSigner(req, signer)
		if err != nil || resp.GetHttpStatus() != 200 {
			return nil, fmt.Errorf("unable to fetch information for disk with DiskType: %v with err: %w", disk.DiskCategory, err)
		} else {
			// This is where population of Pricing happens
			err = json.Unmarshal(resp.GetHttpContentBytes(), &response)
			if err != nil {
				return nil, fmt.Errorf("unable to unmarshall json response to custom struct with err: %w", err)
			}
			pricing.PVAttributes = NewAlibabaPVAttributes(disk)
			pricing.PV = &models.PV{
				Cost: fmt.Sprintf("%f", response.PriceInfo.Price.TradePrice),
			}
			// TO-DO : Disk has support for Hour and Month but pricing API is failing for month for disk(Research why?) and same challenge as node pricing no prepaid/postpaid distinction in v1.PersistentVolume object have to look at APIs for th information.
			pricing.PricingTerms = NewAlibabaPricingTerms(ALIBABA_PAY_AS_YOU_GO_BILLING, NewAlibabaPricingDetails(response.PriceInfo.Price.TradePrice, ALIBABA_HOUR_PRICE_UNIT, response.PriceInfo.Price.TradePrice, response.PriceInfo.Price.Currency))
		}
	default:
		return nil, fmt.Errorf("unsupported ECS Pricing component of type (%T) at this time", i)
	}

	return pricing, nil
}

// This function is to get the InstanceFamily from the InstanceType , convention followed in
// instance type is ecs.[FamilyName].[DifferentSize], it gets the familyName , if it is unable to get it
// it lists the instance family name as Unknown.
func getInstanceFamilyFromType(instanceType string) string {
	splitinstanceType := strings.Split(instanceType, ".")
	if len(splitinstanceType) != 3 {
		log.Warnf("unable to find the family of the instance type %s, returning its family type unknown", instanceType)
		return ALIBABA_UNKNOWN_INSTANCE_FAMILY_TYPE
	}
	return splitinstanceType[1]
}

// This function is used to obtain the generation of the instance family from the InstanceType,
// because when the generation is higher than or equal to 7, the instance disk type will not support cloud_efficiency.
// In such cases, when calling the DescribePrice interface, the system disk type will default to cloud_essd.
func getInstanceFamilyGenerationFromType(instanceType string) int {
	// FamilyName format: g7ne or g7 or r7 or r6e,
	familyName := getInstanceFamilyFromType(instanceType)
	re := regexp.MustCompile(`(\d+)`)
	match := re.FindString(familyName)
	if match != "" {
		generation, err := strconv.Atoi(match)
		if err != nil {
			log.Errorf("unable to convert the generation of the instance type %s to integer", instanceType)
		} else {
			return generation
		}
	}
	log.Warnf("unable to find the generation of the instance type %s,", instanceType)
	return -1
}

// getInstanceIDFromProviderID returns the instance ID associated with the Node. A *v1.Node providerID in Alibaba cloud
// is of <REGION-ID>.<INSTANCE-ID>. This function returns the Instance ID for the given ProviderID. if its unable to interpret
// it defaults to empty string.
func getInstanceIDFromProviderID(providerID string) string {
	if providerID == "" {
		return ""
	}
	splitStrings := strings.Split(providerID, ".")
	if len(splitStrings) < 2 {
		return ""
	}
	return splitStrings[1]
}

type Disk struct {
	Category         string `json:"Category"`
	Size             int    `json:"Size"`
	PerformanceLevel string `json:"PerformanceLevel"`
	Type             string `json:"Type"`
	RegionId         string `json:"RegionId"`
	DiskId           string `json:"DiskId"`
	DiskChargeType   string `json:"DiskChargeType"`
}

type Disks struct {
	Disk []*Disk `json:"Disk"`
}

type DescribeDiskResponse struct {
	TotalCount int    `json:"TotalCount"`
	Disks      *Disks `json:"Disks"`
}

// getSystemDiskInfoOfANode gets the relevant System disk information associated with the Node given by the instanceID
// in form of a SlimK8sDisk with only relevant information that can adjust the node pricing. If any error occurs return
// an empty disk to not impact any default set at the price retrieval of the node.
func getSystemDiskInfoOfANode(instanceID, regionID string, client *sdk.Client, signer *signers.AccessKeySigner) (systemDisk *SlimK8sDisk) {
	systemDisk = &SlimK8sDisk{}
	var response DescribeDiskResponse
	// if instanceID is empty string return an empty k8s
	if instanceID == "" {
		return
	}
	req, err := createDescribeDisksACSRequest(instanceID, regionID, ALIBABA_SYSTEM_DISK_CATEGORY)
	// if any error occurs return an empty disk to not impact default pricing.
	if err != nil {
		log.Warnf("Unable to create Describe Disk Request with err: %v for node with InstanceID: %s, hence defaulting it to an empty system disk to pass through to defaults", err, instanceID)
		return
	}

	resp, err := client.ProcessCommonRequestWithSigner(req, signer)
	if err != nil || resp.GetHttpStatus() != 200 {
		log.Warnf("Unable to process Describe Disk request with err: %v and errcode: %d for the node with InstanceID: %s, hence defaulting it to an empty system disk to pass through to defaults", err, resp.GetHttpStatus(), instanceID)
		return
	} else {
		// This is where population of Pricing happens
		err = json.Unmarshal(resp.GetHttpContentBytes(), &response)
		if err != nil {
			log.Warnf("Unable to unmarshall Describe Disk response with err: %v for the node with InstanceID: %s, hence defaulting it to an empty system disk to pass through to defaults", err, instanceID)
			return
		}
		// Every instance should only have one system disk per Alibaba Cloud documentation https://www.alibabacloud.com/help/en/elastic-compute-service/latest/block-storage-overview-disks,
		// if TotalCount is not 1 just return empty and let it not impact default pricing.
		if response.TotalCount != 1 {
			log.Warnf("Total count of system disk for node with InstanceID: %s is not 1, hence defaulting it to an empty system disk to pass through to defaults", instanceID)
			return
		}

		if response.Disks == nil {
			log.Warnf("Disks information missing for node with InstanceID: %s, hence defaulting it to an empty system disk to pass through to defaults", instanceID)
			return
		}

		if len(response.Disks.Disk) < 1 {
			log.Warnf("Total number of system disk for node with InstanceID: %s is less than 1, hence defaulting it to an empty system disk to pass through to defaults", instanceID)
			return
		}

		// TO-DO: When supporting Subscription type disk, you can leverge the disk.DiskChargeType here to map it to subscription type.
		systemDisk := response.Disks.Disk[0]
		return NewSlimK8sDisk(systemDisk.Type, systemDisk.RegionId, ALIBABA_HOUR_PRICE_UNIT, systemDisk.Category, systemDisk.PerformanceLevel, systemDisk.DiskId, "", fmt.Sprintf("%d", systemDisk.Size))
	}
}

// generateSlimK8sNodeFromV1Node generates SlimK8sNode struct from v1.Node to fetch pricing information and call alibaba API.
func generateSlimK8sNodeFromV1Node(node *clustercache.Node) *SlimK8sNode {
	var regionID, osType, instanceType, providerID, priceUnit, instanceFamily string
	var memorySizeInKiB string // TO-DO: try to convert it into float
	var ok, IsIoOptimized bool
	if regionID, ok = node.Labels["topology.kubernetes.io/region"]; !ok {
		// HIGHLY UNLIKELY THAT THIS LABEL WONT BE THERE.
		log.Debugf("No RegionID label for the node: %s", node.Name)
	}
	if osType, ok = node.Labels["beta.kubernetes.io/os"]; !ok {
		// HIGHLY UNLIKELY THAT THIS LABEL WONT BE THERE.
		log.Debugf("OS type undetected for the node: %s", node.Name)
	}
	if instanceType, ok = node.Labels["node.kubernetes.io/instance-type"]; !ok {
		// HIGHLY UNLIKELY THAT THIS LABEL WONT BE THERE.
		log.Debugf("Instance Type undetected for the node: %s", node.Name)
	}

	instanceFamily = getInstanceFamilyFromType(instanceType)
	memorySizeInKiB = fmt.Sprintf("%s", node.Status.Capacity.Memory())
	providerID = node.SpecProviderID // Alibaba Cloud provider doesnt follow convention of prefix with cloud provider name

	// Looking at current Instance offering , all of the Instances seem to be I/O optimized - https://www.alibabacloud.com/help/en/elastic-compute-service/latest/instance-family
	// Basic price Json has it as part of the key so defaulting to true.
	IsIoOptimized = true
	priceUnit = ALIBABA_HOUR_PRICE_UNIT

	systemDisk := &SlimK8sDisk{}
	return NewSlimK8sNode(instanceType, regionID, priceUnit, memorySizeInKiB, osType, providerID, instanceFamily, IsIoOptimized, systemDisk)
}

// getNumericalValueFromResourceQuantity returns the numericalValue of the resourceQuantity
// An example is: 20Gi returns to 20. If any error occurs it returns the default value used in describePrice API which is 2000.
func getNumericalValueFromResourceQuantity(quantity string) (value string) {
	// defaulting when any panic or empty string occurs.
	defer func() {
		log.Debugf("unable to determine the size of the PV so defaulting the size to %s", ALIBABA_DEFAULT_DATADISK_SIZE)
		if err := recover(); err != nil {
			value = ALIBABA_DEFAULT_DATADISK_SIZE
		}
		if value == "" {
			value = ALIBABA_DEFAULT_DATADISK_SIZE
		}
	}()
	res := sizeRegEx.FindAllStringSubmatch(quantity, 1)
	value = res[0][1]
	return
}

// generateSlimK8sDiskFromV1PV function generates SlimK8sDisk from v1.PersistentVolume
// to generate slim disk type that can be used to fetch pricing information for Data disk type.
func generateSlimK8sDiskFromV1PV(pv *clustercache.PersistentVolume, regionID string) *SlimK8sDisk {

	// All PVs are data disks while local disk are categorized as system disk
	diskType := ALIBABA_DATA_DISK_CATEGORY

	//TO-DO: Disk supports month and hour prices , defaulting to hour
	priceUnit := ALIBABA_HOUR_PRICE_UNIT

	sizeQuantity := fmt.Sprintf("%s", pv.Spec.Capacity.Storage())

	// res := sizeRegEx.FindAllStringSubmatch(sizeQuantity, 1)

	sizeInGiB := getNumericalValueFromResourceQuantity(sizeQuantity)

	providerID := ""
	if pv.Spec.CSI != nil {
		providerID = pv.Spec.CSI.VolumeHandle
	} else {
		providerID = pv.Name // Looks like pv name is same as providerID in Alibaba k8s cluster
	}

	// Performance level being empty string gets defaulted in describePrice to PL1.
	performanceLevel := ""
	diskCategory := ""
	if pv.Spec.CSI != nil {
		if val, ok := pv.Spec.CSI.VolumeAttributes["performanceLevel"]; ok {
			performanceLevel = val
		}
		if val, ok := pv.Spec.CSI.VolumeAttributes["type"]; ok {
			diskCategory = val
		}
	}

	// Highly unlikely that label pv.Spec.CSI.VolumeAttributes["type"] doesn't exist but if occurred default to cloud (most basic disk type)
	if diskCategory == "" {
		diskCategory = ALIBABA_DISK_CLOUD_CATEGORY
	}

	return NewSlimK8sDisk(diskType, regionID, priceUnit, diskCategory, performanceLevel, providerID, pv.Spec.StorageClassName, sizeInGiB)
}

// determinePVRegion determines associated region for a particular PV based on the following priority, which can be changed and any other path to determine region can be added!
// if topology.diskplugin.csi.alibabacloud.com/region label/annotation is passed during PV creation return that as the PV region.
// if topology.diskplugin.csi.alibabacloud.com/zone label/annotation is passed during PV creation determine the region based on this pv label.
// if neither of the above label/annotation is present check node affinity for the zone affinity and determine the region based on this zone.
// if nether of the above yields a region , return empty string to default it to cluster region.
func determinePVRegion(pv *clustercache.PersistentVolume) string {
	// if "topology.diskplugin.csi.alibabacloud.com/region" is present as a label or annotation return that as the PV region
	if val, ok := pv.Labels[ALIBABA_DISK_TOPOLOGY_REGION_LABEL]; ok {
		log.Debugf("determinePVRegion returned a region value of: %s through label: %s for PV name: %s", val, ALIBABA_DISK_TOPOLOGY_REGION_LABEL, pv.Name)
		return val
	}
	if val, ok := pv.Annotations[ALIBABA_DISK_TOPOLOGY_REGION_LABEL]; ok {
		log.Debugf("determinePVRegion returned a region value of: %s through annotation: %s for PV name: %s", val, ALIBABA_DISK_TOPOLOGY_REGION_LABEL, pv.Name)
		return val
	}

	// if "topology.diskplugin.csi.alibabacloud.com/zone" is present as a label or annotation set it as the PV zone before looking at node affinity to determine the region PV belongs too
	var pvZone string

	if val, ok := pv.Labels[ALIBABA_DISK_TOPOLOGY_ZONE_LABEL]; ok {
		log.Debugf("determinePVRegion will set zone value to: %s through label: %s for PV name: %s", val, ALIBABA_DISK_TOPOLOGY_ZONE_LABEL, pv.Name)
		pvZone = val
	}

	if pvZone == "" {
		if val, ok := pv.Annotations[ALIBABA_DISK_TOPOLOGY_ZONE_LABEL]; ok {
			log.Debugf("determinePVRegion will set zone value to: %s through annotation: %s for PV name: %s", val, ALIBABA_DISK_TOPOLOGY_ZONE_LABEL, pv.Name)
			pvZone = val
		}
	}

	if pvZone == "" {
		// zone and regionID labels are optional in Alibaba PV creation, while PV through UI creation put's a zone PV is associated with and the region
		// can be determined from this information. If pv is provision via yaml and the block is missing that's the only time it gets defaulted to ClusterRegion.
		if pv.Spec.NodeAffinity != nil {
			nodeAffinity := pv.Spec.NodeAffinity
			if nodeAffinity.Required != nil && nodeAffinity.Required.NodeSelectorTerms != nil {
				for _, nodeSelectorTerm := range nodeAffinity.Required.NodeSelectorTerms {
					matchExpression := nodeSelectorTerm.MatchExpressions
					for _, nodeSelectorRequirement := range matchExpression {
						if nodeSelectorRequirement.Key == ALIBABA_DISK_TOPOLOGY_ZONE_LABEL {
							log.Debugf("determinePVRegion will set zone value to: %s through node affinity label: %s for PV name: %s", nodeSelectorRequirement.Values[0], ALIBABA_DISK_TOPOLOGY_ZONE_LABEL, pv.Name)
							pvZone = nodeSelectorRequirement.Values[0]
						}
					}
				}
			}
		}
	}

	regionOverrides := ocenv.GetRegionOverrideList()
	regions := alibabaRegions

	if len(regionOverrides) > 0 {
		regions = regionOverrides
	}

	for _, region := range regions {
		if strings.Contains(pvZone, region) {
			log.Debugf("determinePVRegion determined region of %s through zone affiliation of the PV %s\n", region, pvZone)
			return region
		}
	}
	return ""
}

// PricingSourceSummary returns the pricing source summary for the provider.
// The summary represents what was _parsed_ from the pricing source, not
// everything that was _available_ in the pricing source.
func (a *Alibaba) PricingSourceSummary() interface{} {
	return a.Pricing
}
