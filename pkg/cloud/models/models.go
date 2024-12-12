package models

import (
	"fmt"
	"io"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/microcosm-cc/bluemonday"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/pkg/clustercache"
	"github.com/opencost/opencost/pkg/config"
)

var (
	sanitizePolicy = bluemonday.UGCPolicy()
)

const (
	AuthSecretPath                 = "/var/secrets/service-key.json"
	StorageConfigSecretPath        = "/var/azure-storage-config/azure-storage-config.json"
	DefaultShareTenancyCost        = "true"
	KarpenterCapacityTypeLabel     = "karpenter.sh/capacity-type"
	KarpenterCapacitySpotTypeValue = "spot"
)

// ReservedInstanceData keeps record of resources on a node should be
// priced at reserved rates
type ReservedInstanceData struct {
	ReservedCPU int64   `json:"reservedCPU"`
	ReservedRAM int64   `json:"reservedRAM"`
	CPUCost     float64 `json:"CPUHourlyCost"`
	RAMCost     float64 `json:"RAMHourlyCost"`
}

// Node is the interface by which the provider and cost model communicate Node prices.
// The provider will best-effort try to fill out this struct.
type Node struct {
	Cost             string                `json:"hourlyCost"`
	VCPU             string                `json:"CPU"`
	VCPUCost         string                `json:"CPUHourlyCost"`
	RAM              string                `json:"RAM"`
	RAMBytes         string                `json:"RAMBytes"`
	RAMCost          string                `json:"RAMGBHourlyCost"`
	Storage          string                `json:"storage"`
	StorageCost      string                `json:"storageHourlyCost"`
	UsesBaseCPUPrice bool                  `json:"usesDefaultPrice"`
	BaseCPUPrice     string                `json:"baseCPUPrice"` // Used to compute an implicit RAM GB/Hr price when RAM pricing is not provided.
	BaseRAMPrice     string                `json:"baseRAMPrice"` // Used to compute an implicit RAM GB/Hr price when RAM pricing is not provided.
	BaseGPUPrice     string                `json:"baseGPUPrice"`
	UsageType        string                `json:"usageType"`
	GPU              string                `json:"gpu"` // GPU represents the number of GPU on the instance
	GPUName          string                `json:"gpuName"`
	GPUCost          string                `json:"gpuCost"`
	VGPU             string                `json:"vgpu"` // virtualized gpu-- if we are using gpu replicas
	InstanceType     string                `json:"instanceType,omitempty"`
	Region           string                `json:"region,omitempty"`
	Reserved         *ReservedInstanceData `json:"reserved,omitempty"`
	ProviderID       string                `json:"providerID,omitempty"`
	PricingType      PricingType           `json:"pricingType,omitempty"`
	ArchType         string                `json:"archType,omitempty"`
}

// IsSpot determines whether or not a Node uses spot by usage type
func (n *Node) IsSpot() bool {
	if n != nil {
		return strings.Contains(n.UsageType, "spot") || strings.Contains(n.UsageType, "emptible")
	} else {
		return false
	}
}

type OrphanedResource struct {
	Kind        string            `json:"resourceKind"`
	Region      string            `json:"region"`
	Description map[string]string `json:"description"`
	Size        *int64            `json:"diskSizeInGB,omitempty"`
	DiskName    string            `json:"diskName,omitempty"`
	Url         string            `json:"url"`
	Address     string            `json:"ipAddress,omitempty"`
	MonthlyCost *float64          `json:"monthlyCost"`
}

// PV is the interface by which the provider and cost model communicate PV prices.
// The provider will best-effort try to fill out this struct.
type PV struct {
	Cost       string            `json:"hourlyCost"`
	CostPerIO  string            `json:"costPerIOOperation"`
	Class      string            `json:"storageClass"`
	Size       string            `json:"size"`
	Region     string            `json:"region"`
	ProviderID string            `json:"providerID,omitempty"`
	Parameters map[string]string `json:"parameters"`
}

// Key represents a way for nodes to match between the k8s API and a pricing API
type Key interface {
	ID() string       // ID represents an exact match
	Features() string // Features are a comma separated string of node metadata that could match pricing
	GPUType() string  // GPUType returns "" if no GPU exists or GPUs, but the name of the GPU otherwise
	GPUCount() int    // GPUCount returns 0 if no GPU exists or GPUs, but the number of attached GPUs otherwise
}

type PVKey interface {
	Features() string
	GetStorageClass() string
	ID() string
}

// OutOfClusterAllocation represents a cloud provider cost not associated with kubernetes
type OutOfClusterAllocation struct {
	Aggregator  string  `json:"aggregator"`
	Environment string  `json:"environment"`
	Service     string  `json:"service"`
	Cost        float64 `json:"cost"`
	Cluster     string  `json:"cluster"`
}

type CustomPricing struct {
	Provider    string `json:"provider"`
	Description string `json:"description"`
	// CPU a string-encoded float describing cost per core-hour of CPU.
	CPU string `json:"CPU"`
	// CPU a string-encoded float describing cost per core-hour of CPU for spot
	// nodes.
	SpotCPU string `json:"spotCPU"`
	// RAM a string-encoded float describing cost per GiB-hour of RAM/memory.
	RAM string `json:"RAM"`
	// SpotRAM a string-encoded float describing cost per GiB-hour of RAM/memory
	// for spot nodes.
	SpotRAM string `json:"spotRAM"`
	GPU     string `json:"GPU"`
	SpotGPU string `json:"spotGPU"`
	// Storage is a string-encoded float describing cost per GB-hour of storage
	// (e.g. PV, disk) resources.
	Storage                      string `json:"storage"`
	ZoneNetworkEgress            string `json:"zoneNetworkEgress"`
	RegionNetworkEgress          string `json:"regionNetworkEgress"`
	InternetNetworkEgress        string `json:"internetNetworkEgress"`
	FirstFiveForwardingRulesCost string `json:"firstFiveForwardingRulesCost"`
	AdditionalForwardingRuleCost string `json:"additionalForwardingRuleCost"`
	LBIngressDataCost            string `json:"LBIngressDataCost"`
	SpotLabel                    string `json:"spotLabel,omitempty"`
	SpotLabelValue               string `json:"spotLabelValue,omitempty"`
	GpuLabel                     string `json:"gpuLabel,omitempty"`
	GpuLabelValue                string `json:"gpuLabelValue,omitempty"`
	ServiceKeyName               string `json:"awsServiceKeyName,omitempty"`
	ServiceKeySecret             string `json:"awsServiceKeySecret,omitempty"`
	AlibabaServiceKeyName        string `json:"alibabaServiceKeyName,omitempty"`
	AlibabaServiceKeySecret      string `json:"alibabaServiceKeySecret,omitempty"`
	AlibabaClusterRegion         string `json:"alibabaClusterRegion,omitempty"`
	SpotDataRegion               string `json:"awsSpotDataRegion,omitempty"`
	SpotDataBucket               string `json:"awsSpotDataBucket,omitempty"`
	SpotDataPrefix               string `json:"awsSpotDataPrefix,omitempty"`
	ProjectID                    string `json:"projectID,omitempty"`
	AthenaProjectID              string `json:"athenaProjectID,omitempty"`
	AthenaBucketName             string `json:"athenaBucketName"`
	AthenaRegion                 string `json:"athenaRegion"`
	AthenaDatabase               string `json:"athenaDatabase"`
	AthenaCatalog                string `json:"athenaCatalog"`
	AthenaTable                  string `json:"athenaTable"`
	AthenaWorkgroup              string `json:"athenaWorkgroup"`
	MasterPayerARN               string `json:"masterPayerARN"`
	BillingDataDataset           string `json:"billingDataDataset,omitempty"`
	CustomPricesEnabled          string `json:"customPricesEnabled"`
	DefaultIdle                  string `json:"defaultIdle"`
	AzureSubscriptionID          string `json:"azureSubscriptionID"`
	AzureClientID                string `json:"azureClientID"`
	AzureClientSecret            string `json:"azureClientSecret"`
	AzureTenantID                string `json:"azureTenantID"`
	AzureBillingRegion           string `json:"azureBillingRegion"`
	AzureBillingAccount          string `json:"azureBillingAccount"`
	AzureOfferDurableID          string `json:"azureOfferDurableID"`
	AzureStorageSubscriptionID   string `json:"azureStorageSubscriptionID"`
	AzureStorageAccount          string `json:"azureStorageAccount"`
	AzureStorageAccessKey        string `json:"azureStorageAccessKey"`
	AzureStorageContainer        string `json:"azureStorageContainer"`
	AzureContainerPath           string `json:"azureContainerPath"`
	AzureCloud                   string `json:"azureCloud"`
	CurrencyCode                 string `json:"currencyCode"`
	Discount                     string `json:"discount"`
	NegotiatedDiscount           string `json:"negotiatedDiscount"`
	SharedOverhead               string `json:"sharedOverhead"`
	ClusterName                  string `json:"clusterName"`
	ClusterAccountID             string `json:"clusterAccount,omitempty"`
	SharedNamespaces             string `json:"sharedNamespaces"`
	SharedLabelNames             string `json:"sharedLabelNames"`
	SharedLabelValues            string `json:"sharedLabelValues"`
	ShareTenancyCosts            string `json:"shareTenancyCosts"` // TODO clean up configuration so we can use a type other that string (this should be a bool, but the app panics if it's not a string)
	ReadOnly                     string `json:"readOnly"`
	EditorAccess                 string `json:"editorAccess"`
	KubecostToken                string `json:"kubecostToken"`
	GoogleAnalyticsTag           string `json:"googleAnalyticsTag"`
	ExcludeProviderID            string `json:"excludeProviderID"`
	DefaultLBPrice               string `json:"defaultLBPrice"`
}

// GetSharedOverheadCostPerMonth parses and returns a float64 representation
// of the configured monthly shared overhead cost. If the string version cannot
// be parsed into a float, an error is logged and 0.0 is returned.
func (cp *CustomPricing) GetSharedOverheadCostPerMonth() float64 {
	// Empty string should be interpreted as "no cost", i.e. 0.0
	if cp.SharedOverhead == "" {
		return 0.0
	}

	// Attempt to parse, but log and return 0.0 if that fails.
	sharedCostPerMonth, err := strconv.ParseFloat(cp.SharedOverhead, 64)
	if err != nil {
		log.Errorf("SharedOverhead: failed to parse shared overhead \"%s\": %s", cp.SharedOverhead, err)
		return 0.0
	}

	return sharedCostPerMonth
}

func sanitizeFloatString(number string, allowNaN bool) (string, error) {
	num, err := strconv.ParseFloat(number, 64)
	if err != nil {
		return "", fmt.Errorf("expected a string representing a number; got '%s'", number)
	}
	if !allowNaN && math.IsNaN(num) {
		return "", fmt.Errorf("expected a string representing a number; got 'NaN'")
	}

	// Format the numerical string we just parsed.
	return strconv.FormatFloat(num, 'f', -1, 64), nil
}

func SetCustomPricingField(obj *CustomPricing, name string, value string) error {
	structValue := reflect.ValueOf(obj).Elem()
	structFieldValue := structValue.FieldByName(name)

	if !structFieldValue.IsValid() {
		return fmt.Errorf("no such field: %s in obj", name)
	}

	if !structFieldValue.CanSet() {
		return fmt.Errorf("cannot set %s field value", name)
	}

	// If the custom pricing field is expected to be a string representation
	// of a floating point number, e.g. a resource price, then do some extra
	// validation work in order to prevent "NaN" and other invalid strings
	// from getting set here.
	switch strings.ToLower(name) {
	case "cpu", "gpu", "ram", "spotcpu", "spotgpu", "spotram", "storage", "zonenetworkegress", "regionnetworkegress", "internetnetworkegress":
		// If we are sent an empty string, ignore the key and don't change the value
		if value == "" {
			return nil
		} else {
			// Validate that "value" represents a real floating point number, and
			// set precision, bits, etc. Do not allow NaN.
			val, err := sanitizeFloatString(value, false)
			if err != nil {
				return fmt.Errorf("invalid numeric value for field '%s': %s", name, value)
			}
			value = val
		}
	default:
	}

	structFieldType := structFieldValue.Type()
	value = sanitizePolicy.Sanitize(value)
	val := reflect.ValueOf(value)
	if structFieldType != val.Type() {
		return fmt.Errorf("provided value type didn't match custom pricing field type")
	}

	structFieldValue.Set(val)
	return nil
}

type PricingSources struct {
	PricingSources map[string]*PricingSource
}

type PricingSource struct {
	Name      string `json:"name"`
	Enabled   bool   `json:"enabled"`
	Available bool   `json:"available"`
	Error     string `json:"error"`
}

type PricingType string

const (
	Api           PricingType = "api"
	Spot          PricingType = "spot"
	Reserved      PricingType = "reserved"
	SavingsPlan   PricingType = "savingsPlan"
	CsvExact      PricingType = "csvExact"
	CsvClass      PricingType = "csvClass"
	DefaultPrices PricingType = "defaultPrices"
)

type PricingMatchMetadata struct {
	TotalNodes        int                 `json:"TotalNodes"`
	PricingTypeCounts map[PricingType]int `json:"PricingType"`
}

// Provider represents a k8s provider.
type Provider interface {
	ClusterInfo() (map[string]string, error)
	GetAddresses() ([]byte, error)
	GetDisks() ([]byte, error)
	GetOrphanedResources() ([]OrphanedResource, error)
	NodePricing(Key) (*Node, PricingMetadata, error)
	PVPricing(PVKey) (*PV, error)
	NetworkPricing() (*Network, error)           // TODO: add key interface arg for dynamic price fetching
	LoadBalancerPricing() (*LoadBalancer, error) // TODO: add key interface arg for dynamic price fetching
	AllNodePricing() (interface{}, error)
	DownloadPricingData() error
	GetKey(map[string]string, *clustercache.Node) Key
	GetPVKey(*clustercache.PersistentVolume, map[string]string, string) PVKey
	UpdateConfig(r io.Reader, updateType string) (*CustomPricing, error)
	UpdateConfigFromConfigMap(map[string]string) (*CustomPricing, error)
	GetConfig() (*CustomPricing, error)
	GetManagementPlatform() (string, error)
	GetLocalStorageQuery(time.Duration, time.Duration, bool, bool) string
	ApplyReservedInstancePricing(map[string]*Node)
	ServiceAccountStatus() *ServiceAccountStatus
	PricingSourceStatus() map[string]*PricingSource
	ClusterManagementPricing() (string, float64, error)
	CombinedDiscountForNode(string, bool, float64, float64) float64
	Regions() []string
	PricingSourceSummary() interface{}
}

// ProviderConfig describes config storage common to all providers.
type ProviderConfig interface {
	ConfigFileManager() *config.ConfigFileManager
	GetCustomPricingData() (*CustomPricing, error)
	Update(func(*CustomPricing) error) (*CustomPricing, error)
	UpdateFromMap(map[string]string) (*CustomPricing, error)
}
