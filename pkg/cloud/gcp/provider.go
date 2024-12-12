package gcp

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/opencost/opencost/pkg/cloud/aws"
	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/cloud/utils"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util"
	"github.com/opencost/opencost/core/pkg/util/fileutil"
	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
	"github.com/opencost/opencost/pkg/clustercache"
	"github.com/opencost/opencost/pkg/env"
	"github.com/rs/zerolog"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/compute/metadata"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/compute/v1"
)

const GKE_GPU_TAG = "cloud.google.com/gke-accelerator"
const BigqueryUpdateType = "bigqueryupdate"
const BillingAPIURLFmt = "https://cloudbilling.googleapis.com/v1/services/6F81-5844-456A/skus?key=%s&currencyCode=%s"

const (
	GCPHourlyPublicIPCost = 0.01

	GCPMonthlyBasicDiskCost = 0.04
	GCPMonthlySSDDiskCost   = 0.17
	GCPMonthlyGP2DiskCost   = 0.1

	GKEPreemptibleLabel = "cloud.google.com/gke-preemptible"
	GKESpotLabel        = "cloud.google.com/gke-spot"
)

// List obtained by installing the `gcloud` CLI tool,
// logging into gcp account, and running command
// `gcloud compute regions list`
var gcpRegions = []string{
	"asia-east1",
	"asia-east2",
	"asia-northeast1",
	"asia-northeast2",
	"asia-northeast3",
	"asia-south1",
	"asia-south2",
	"asia-southeast1",
	"asia-southeast2",
	"australia-southeast1",
	"australia-southeast2",
	"europe-central2",
	"europe-north1",
	"europe-southwest1",
	"europe-west1",
	"europe-west10",
	"europe-west12",
	"europe-west2",
	"europe-west3",
	"europe-west4",
	"europe-west6",
	"europe-west8",
	"europe-west9",
	"me-central1",
	"me-central2",
	"me-west1",
	"northamerica-northeast1",
	"northamerica-northeast2",
	"southamerica-east1",
	"southamerica-west1",
	"us-central1",
	"us-east1",
	"us-east4",
	"us-east5",
	"us-south1",
	"us-west1",
	"us-west2",
	"us-west3",
	"us-west4",
}

var (
	nvidiaTeslaGPURegex = regexp.MustCompile("(Nvidia Tesla [^ ]+) ")
	nvidiaGPURegex      = regexp.MustCompile("(Nvidia [^ ]+) ")
	// gce://guestbook-12345/...
	//  => guestbook-12345
	gceRegex = regexp.MustCompile("gce://([^/]*)/*")
)

// GCP implements a provider interface for GCP
type GCP struct {
	Pricing                 map[string]*GCPPricing
	Clientset               clustercache.ClusterCache
	APIKey                  string
	BaseCPUPrice            string
	ProjectID               string
	BillingDataDataset      string
	DownloadPricingDataLock sync.RWMutex
	ReservedInstances       []*GCPReservedInstance
	Config                  models.ProviderConfig
	ServiceKeyProvided      bool
	ValidPricingKeys        map[string]bool
	MetadataClient          *metadata.Client
	clusterManagementPrice  float64
	ClusterRegion           string
	ClusterAccountID        string
	ClusterProjectID        string
	clusterProvisioner      string
}

type gcpAllocation struct {
	Aggregator  bigquery.NullString
	Environment bigquery.NullString
	Service     string
	Cost        float64
}

type multiKeyGCPAllocation struct {
	Keys    bigquery.NullString
	Service string
	Cost    float64
}

// GetLocalStorageQuery returns the cost of local storage for the given window. Setting rate=true
// returns hourly spend. Setting used=true only tracks used storage, not total.
func (gcp *GCP) GetLocalStorageQuery(window, offset time.Duration, rate bool, used bool) string {
	// TODO Set to the price for the appropriate storage class. It's not trivial to determine the local storage disk type
	// See https://cloud.google.com/compute/disks-image-pricing#persistentdisk
	localStorageCost := 0.04

	baseMetric := "container_fs_limit_bytes"
	if used {
		baseMetric = "container_fs_usage_bytes"
	}

	fmtOffset := timeutil.DurationToPromOffsetString(offset)

	fmtCumulativeQuery := `sum(
		sum_over_time(%s{device!="tmpfs", id="/", %s}[%s:1m]%s)
	) by (%s) / 60 / 730 / 1024 / 1024 / 1024 * %f`

	fmtMonthlyQuery := `sum(
		avg_over_time(%s{device!="tmpfs", id="/", %s}[%s:1m]%s)
	) by (%s) / 1024 / 1024 / 1024 * %f`

	fmtQuery := fmtCumulativeQuery
	if rate {
		fmtQuery = fmtMonthlyQuery
	}
	fmtWindow := timeutil.DurationString(window)

	return fmt.Sprintf(fmtQuery, baseMetric, env.GetPromClusterFilter(), fmtWindow, fmtOffset, env.GetPromClusterLabel(), localStorageCost)
}

func (gcp *GCP) GetConfig() (*models.CustomPricing, error) {
	c, err := gcp.Config.GetCustomPricingData()
	if err != nil {
		return nil, err
	}
	if c.Discount == "" {
		c.Discount = "30%"
	}
	if c.NegotiatedDiscount == "" {
		c.NegotiatedDiscount = "0%"
	}
	if c.CurrencyCode == "" {
		c.CurrencyCode = "USD"
	}
	if c.ShareTenancyCosts == "" {
		c.ShareTenancyCosts = models.DefaultShareTenancyCost
	}
	return c, nil
}

// BigQueryConfig contain the required config and credentials to access OOC resources for GCP
// Deprecated: v1.104 Use BigQueryConfiguration instead
type BigQueryConfig struct {
	ProjectID          string            `json:"projectID"`
	BillingDataDataset string            `json:"billingDataDataset"`
	Key                map[string]string `json:"key"`
}

// IsEmpty returns true if all fields in config are empty, false if not.
func (bqc *BigQueryConfig) IsEmpty() bool {
	return bqc.ProjectID == "" &&
		bqc.BillingDataDataset == "" &&
		(bqc.Key == nil || len(bqc.Key) == 0)
}
func (gcp *GCP) GetManagementPlatform() (string, error) {
	nodes := gcp.Clientset.GetAllNodes()

	if len(nodes) > 0 {
		n := nodes[0]
		version := n.Status.NodeInfo.KubeletVersion
		if strings.Contains(version, "gke") {
			return "gke", nil
		}
	}
	return "", nil
}

// Attempts to load a GCP auth secret and copy the contents to the key file.
func (*GCP) loadGCPAuthSecret() {
	path := env.GetConfigPathWithDefault("/models/")

	keyPath := path + "key.json"
	keyExists, _ := fileutil.FileExists(keyPath)
	if keyExists {
		log.Info("GCP Auth Key already exists, no need to load from secret")
		return
	}

	exists, err := fileutil.FileExists(models.AuthSecretPath)
	if !exists || err != nil {
		errMessage := "Secret does not exist"
		if err != nil {
			errMessage = err.Error()
		}

		log.Warnf("Failed to load auth secret, or was not mounted: %s", errMessage)
		return
	}

	result, err := os.ReadFile(models.AuthSecretPath)
	if err != nil {
		log.Warnf("Failed to load auth secret, or was not mounted: %s", err.Error())
		return
	}

	err = os.WriteFile(keyPath, result, 0644)
	if err != nil {
		log.Warnf("Failed to copy auth secret to %s: %s", keyPath, err.Error())
	}
}

func (gcp *GCP) UpdateConfigFromConfigMap(a map[string]string) (*models.CustomPricing, error) {
	return gcp.Config.UpdateFromMap(a)
}

func (gcp *GCP) UpdateConfig(r io.Reader, updateType string) (*models.CustomPricing, error) {
	return gcp.Config.Update(func(c *models.CustomPricing) error {
		if updateType == BigqueryUpdateType {
			a := BigQueryConfig{}
			err := json.NewDecoder(r).Decode(&a)
			if err != nil {
				return err
			}

			c.ProjectID = a.ProjectID
			c.BillingDataDataset = a.BillingDataDataset

			if len(a.Key) > 0 {
				j, err := json.Marshal(a.Key)
				if err != nil {
					return err
				}

				path := env.GetConfigPathWithDefault("/models/")

				keyPath := path + "key.json"
				err = os.WriteFile(keyPath, j, 0644)
				if err != nil {
					return err
				}
				gcp.ServiceKeyProvided = true
			}
		} else if updateType == aws.AthenaInfoUpdateType {
			a := aws.AwsAthenaInfo{}
			err := json.NewDecoder(r).Decode(&a)
			if err != nil {
				return err
			}
			c.AthenaBucketName = a.AthenaBucketName
			c.AthenaRegion = a.AthenaRegion
			c.AthenaDatabase = a.AthenaDatabase
			c.AthenaCatalog = a.AthenaCatalog
			c.AthenaTable = a.AthenaTable
			c.AthenaWorkgroup = a.AthenaWorkgroup
			c.ServiceKeyName = a.ServiceKeyName
			c.ServiceKeySecret = a.ServiceKeySecret
			c.AthenaProjectID = a.AccountID
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
						return err
					}
				} else {
					return fmt.Errorf("type error while updating config for %s", kUpper)
				}
			}
		}

		if env.IsRemoteEnabled() {
			err := utils.UpdateClusterMeta(env.GetClusterID(), c.ClusterName)
			if err != nil {
				return err
			}
		}

		return nil
	})
}

// ClusterInfo returns information on the GKE cluster, as provided by metadata.
func (gcp *GCP) ClusterInfo() (map[string]string, error) {
	remoteEnabled := env.IsRemoteEnabled()

	attribute, err := gcp.MetadataClient.InstanceAttributeValue("cluster-name")
	if err != nil {
		log.Infof("Error loading metadata cluster-name: %s", err.Error())
	}

	c, err := gcp.GetConfig()
	if err != nil {
		log.Errorf("Error opening config: %s", err.Error())
	}
	if c.ClusterName != "" {
		attribute = c.ClusterName
	}

	// Use a default name if none has been set until this point
	if attribute == "" {
		attribute = "GKE Cluster #1"
	}

	m := make(map[string]string)
	m["name"] = attribute
	m["provider"] = opencost.GCPProvider
	m["region"] = gcp.ClusterRegion
	m["account"] = gcp.ClusterAccountID
	m["project"] = gcp.ClusterProjectID
	m["provisioner"] = gcp.clusterProvisioner
	m["id"] = env.GetClusterID()
	m["remoteReadEnabled"] = strconv.FormatBool(remoteEnabled)
	return m, nil
}

func (gcp *GCP) ClusterManagementPricing() (string, float64, error) {
	return gcp.clusterProvisioner, gcp.clusterManagementPrice, nil
}

func (gcp *GCP) getAllAddresses() (*compute.AddressAggregatedList, error) {
	projID, err := gcp.MetadataClient.ProjectID()
	if err != nil {
		return nil, err
	}

	client, err := google.DefaultClient(context.TODO(),
		"https://www.googleapis.com/auth/compute.readonly")
	if err != nil {
		return nil, err
	}

	svc, err := compute.New(client)
	if err != nil {
		return nil, err
	}

	res, err := svc.Addresses.AggregatedList(projID).Do()

	if err != nil {
		return nil, err
	}

	return res, nil
}

func (gcp *GCP) GetAddresses() ([]byte, error) {
	res, err := gcp.getAllAddresses()
	if err != nil {
		return nil, err
	}

	return json.Marshal(res)
}

func (gcp *GCP) isAddressOrphaned(address *compute.Address) bool {
	// Consider address orphaned if it has 0 users
	return len(address.Users) == 0
}

func (gcp *GCP) getAllDisks() (*compute.DiskAggregatedList, error) {
	projID, err := gcp.MetadataClient.ProjectID()
	if err != nil {
		return nil, err
	}

	client, err := google.DefaultClient(context.TODO(),
		"https://www.googleapis.com/auth/compute.readonly")
	if err != nil {
		return nil, err
	}

	svc, err := compute.New(client)
	if err != nil {
		return nil, err
	}

	res, err := svc.Disks.AggregatedList(projID).Do()

	if err != nil {
		return nil, err
	}

	return res, nil
}

// GetDisks returns the GCP disks backing PVs. Useful because sometimes k8s will not clean up PVs correctly. Requires a json config in /var/configs with key region.
func (gcp *GCP) GetDisks() ([]byte, error) {
	res, err := gcp.getAllDisks()
	if err != nil {
		return nil, err
	}

	return json.Marshal(res)
}

func (gcp *GCP) isDiskOrphaned(disk *compute.Disk) (bool, error) {
	// Do not consider disk orphaned if it has more than 0 users
	if len(disk.Users) > 0 {
		return false, nil
	}

	// Do not consider disk orphaned if it was used within the last hour
	threshold := time.Now().Add(time.Duration(-1) * time.Hour)
	if disk.LastDetachTimestamp != "" {
		lastUsed, err := time.Parse(time.RFC3339, disk.LastDetachTimestamp)
		if err != nil {
			// This can return false since errors are checked before the bool
			return false, fmt.Errorf("error parsing time: %s", err)
		}
		if threshold.Before(lastUsed) {
			return false, nil
		}
	}
	return true, nil
}

func (gcp *GCP) GetOrphanedResources() ([]models.OrphanedResource, error) {
	disks, err := gcp.getAllDisks()
	if err != nil {
		return nil, err
	}

	addresses, err := gcp.getAllAddresses()
	if err != nil {
		return nil, err
	}

	var orphanedResources []models.OrphanedResource

	for _, diskList := range disks.Items {
		if len(diskList.Disks) == 0 {
			continue
		}

		for _, disk := range diskList.Disks {
			isOrphaned, err := gcp.isDiskOrphaned(disk)
			if err != nil {
				return nil, err
			}
			if isOrphaned {
				cost, err := gcp.findCostForDisk(disk)
				if err != nil {
					return nil, err
				}

				// GCP gives us description as a string formatted as a map[string]string, so we need to
				// deconstruct it back into a map[string]string to match the OR struct
				desc := map[string]string{}
				if disk.Description != "" {
					if err := json.Unmarshal([]byte(disk.Description), &desc); err != nil {
						log.Errorf("ignoring orphaned disk %s, failed to convert disk description to map: %s", disk.Name, err)
						continue
					}
				}

				// Converts https://www.googleapis.com/compute/v1/projects/xxxxx/zones/us-central1-c to us-central1-c
				zone := path.Base(disk.Zone)
				if zone == "." {
					zone = ""
				}

				or := models.OrphanedResource{
					Kind:        "disk",
					Region:      zone,
					Description: desc,
					Size:        &disk.SizeGb,
					DiskName:    disk.Name,
					Url:         disk.SelfLink,
					MonthlyCost: cost,
				}
				orphanedResources = append(orphanedResources, or)
			}
		}
	}

	for _, addressList := range addresses.Items {
		if len(addressList.Addresses) == 0 {
			continue
		}

		for _, address := range addressList.Addresses {
			if gcp.isAddressOrphaned(address) {
				//todo: use GCP pricing
				cost := GCPHourlyPublicIPCost * timeutil.HoursPerMonth

				// Converts https://www.googleapis.com/compute/v1/projects/xxxxx/regions/us-central1 to us-central1
				region := path.Base(address.Region)
				if region == "." {
					region = ""
				}

				or := models.OrphanedResource{
					Kind:   "address",
					Region: region,
					Description: map[string]string{
						"type": address.AddressType,
					},
					Address:     address.Address,
					Url:         address.SelfLink,
					MonthlyCost: &cost,
				}
				orphanedResources = append(orphanedResources, or)
			}
		}
	}

	return orphanedResources, nil
}

func (gcp *GCP) findCostForDisk(disk *compute.Disk) (*float64, error) {
	//todo: use GCP pricing struct
	price := GCPMonthlyBasicDiskCost
	if strings.Contains(disk.Type, "ssd") {
		price = GCPMonthlySSDDiskCost
	}
	if strings.Contains(disk.Type, "gp2") {
		price = GCPMonthlyGP2DiskCost
	}
	cost := price * float64(disk.SizeGb)

	// This isn't much use but I (Nick) think its could be going down the
	// right path. Disk region isnt returning anything (and if it did its
	// a url, same with type). Currently the only region stored in the
	// Pricing struct is uscentral-1, so that would need to be fixed
	// key := disk.Region + "," + disk.Type

	// priceStr := gcp.Pricing[key].PV.Cost
	// price, err := strconv.ParseFloat(priceStr, 64)
	// if err != nil {
	// 	return nil, err
	// }

	// cost := price * timeutil.HoursPerMonth * float64(disk.SizeGb)
	return &cost, nil
}

// GCPPricing represents GCP pricing data for a SKU
type GCPPricing struct {
	Name                string           `json:"name"`
	SKUID               string           `json:"skuId"`
	Description         string           `json:"description"`
	Category            *GCPResourceInfo `json:"category"`
	ServiceRegions      []string         `json:"serviceRegions"`
	PricingInfo         []*PricingInfo   `json:"pricingInfo"`
	ServiceProviderName string           `json:"serviceProviderName"`
	Node                *models.Node     `json:"node"`
	PV                  *models.PV       `json:"pv"`
}

// PricingInfo contains metadata about a cost.
type PricingInfo struct {
	Summary                string             `json:"summary"`
	PricingExpression      *PricingExpression `json:"pricingExpression"`
	CurrencyConversionRate float64            `json:"currencyConversionRate"`
	EffectiveTime          string             `json:""`
}

// PricingExpression contains metadata about a cost.
type PricingExpression struct {
	UsageUnit                string         `json:"usageUnit"`
	UsageUnitDescription     string         `json:"usageUnitDescription"`
	BaseUnit                 string         `json:"baseUnit"`
	BaseUnitConversionFactor int64          `json:"-"`
	DisplayQuantity          int            `json:"displayQuantity"`
	TieredRates              []*TieredRates `json:"tieredRates"`
}

// TieredRates contain data about variable pricing.
type TieredRates struct {
	StartUsageAmount int            `json:"startUsageAmount"`
	UnitPrice        *UnitPriceInfo `json:"unitPrice"`
}

// UnitPriceInfo contains data about the actual price being charged.
type UnitPriceInfo struct {
	CurrencyCode string  `json:"currencyCode"`
	Units        string  `json:"units"`
	Nanos        float64 `json:"nanos"`
}

// GCPResourceInfo contains metadata about the node.
type GCPResourceInfo struct {
	ServiceDisplayName string `json:"serviceDisplayName"`
	ResourceFamily     string `json:"resourceFamily"`
	ResourceGroup      string `json:"resourceGroup"`
	UsageType          string `json:"usageType"`
}

func (gcp *GCP) parsePage(r io.Reader, inputKeys map[string]models.Key, pvKeys map[string]models.PVKey) (map[string]*GCPPricing, string, error) {
	gcpPricingList := make(map[string]*GCPPricing)
	var nextPageToken string
	dec := json.NewDecoder(r)
	for {
		t, err := dec.Token()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, "", fmt.Errorf("error parsing GCP pricing page: %s", err)
		}
		if t == "error" {
			errReader := dec.Buffered()
			buf := new(strings.Builder)
			_, err = io.Copy(buf, errReader)
			if err != nil {
				return nil, "", fmt.Errorf("error respnse: could not be read %s", err)
			}

			return nil, "", fmt.Errorf("error respnse: %s", buf.String())
		}
		if t == "skus" {
			_, err := dec.Token() // consumes [
			if err != nil {
				return nil, "", err
			}
			for dec.More() {

				product := &GCPPricing{}
				err := dec.Decode(&product)
				if err != nil {
					return nil, "", err
				}

				usageType := strings.ToLower(product.Category.UsageType)
				instanceType := strings.ToLower(product.Category.ResourceGroup)

				if instanceType == "ssd" && strings.Contains(product.Description, "SSD backed") && !strings.Contains(product.Description, "Regional") { // TODO: support regional
					lastRateIndex := len(product.PricingInfo[0].PricingExpression.TieredRates) - 1
					var nanos float64
					if lastRateIndex > -1 && len(product.PricingInfo) > 0 {
						nanos = product.PricingInfo[0].PricingExpression.TieredRates[lastRateIndex].UnitPrice.Nanos
					} else {
						continue
					}
					hourlyPrice := (nanos * math.Pow10(-9)) / 730

					for _, sr := range product.ServiceRegions {
						region := sr
						candidateKey := region + "," + "ssd"
						if _, ok := pvKeys[candidateKey]; ok {
							product.PV = &models.PV{
								Cost: strconv.FormatFloat(hourlyPrice, 'f', -1, 64),
							}
							gcpPricingList[candidateKey] = product
							continue
						}
					}
					continue
				} else if instanceType == "ssd" && strings.Contains(product.Description, "SSD backed") && strings.Contains(product.Description, "Regional") { // TODO: support regional
					lastRateIndex := len(product.PricingInfo[0].PricingExpression.TieredRates) - 1
					var nanos float64
					if lastRateIndex > -1 && len(product.PricingInfo) > 0 {
						nanos = product.PricingInfo[0].PricingExpression.TieredRates[lastRateIndex].UnitPrice.Nanos
					} else {
						continue
					}
					hourlyPrice := (nanos * math.Pow10(-9)) / 730

					for _, sr := range product.ServiceRegions {
						region := sr
						candidateKey := region + "," + "ssd" + "," + "regional"
						if _, ok := pvKeys[candidateKey]; ok {
							product.PV = &models.PV{
								Cost: strconv.FormatFloat(hourlyPrice, 'f', -1, 64),
							}
							gcpPricingList[candidateKey] = product
							continue
						}
					}
					continue
				} else if instanceType == "pdstandard" && !strings.Contains(product.Description, "Regional") { // TODO: support regional
					lastRateIndex := len(product.PricingInfo[0].PricingExpression.TieredRates) - 1
					var nanos float64
					if lastRateIndex > -1 && len(product.PricingInfo) > 0 {
						nanos = product.PricingInfo[0].PricingExpression.TieredRates[lastRateIndex].UnitPrice.Nanos
					} else {
						continue
					}
					hourlyPrice := (nanos * math.Pow10(-9)) / 730
					for _, sr := range product.ServiceRegions {
						region := sr
						candidateKey := region + "," + "pdstandard"
						if _, ok := pvKeys[candidateKey]; ok {
							product.PV = &models.PV{
								Cost: strconv.FormatFloat(hourlyPrice, 'f', -1, 64),
							}
							gcpPricingList[candidateKey] = product
							continue
						}
					}
					continue
				} else if instanceType == "pdstandard" && strings.Contains(product.Description, "Regional") { // TODO: support regional
					lastRateIndex := len(product.PricingInfo[0].PricingExpression.TieredRates) - 1
					var nanos float64
					if lastRateIndex > -1 && len(product.PricingInfo) > 0 {
						nanos = product.PricingInfo[0].PricingExpression.TieredRates[lastRateIndex].UnitPrice.Nanos
					} else {
						continue
					}
					hourlyPrice := (nanos * math.Pow10(-9)) / 730
					for _, sr := range product.ServiceRegions {
						region := sr
						candidateKey := region + "," + "pdstandard" + "," + "regional"
						if _, ok := pvKeys[candidateKey]; ok {
							product.PV = &models.PV{
								Cost: strconv.FormatFloat(hourlyPrice, 'f', -1, 64),
							}
							gcpPricingList[candidateKey] = product
							continue
						}
					}
					continue
				}

				if (instanceType == "ram" || instanceType == "cpu") && strings.Contains(strings.ToUpper(product.Description), "CUSTOM") {
					instanceType = "custom"
				}

				if (instanceType == "ram" || instanceType == "cpu") && strings.Contains(strings.ToUpper(product.Description), "N2") && !strings.Contains(strings.ToUpper(product.Description), "PREMIUM") {
					if (instanceType == "ram" || instanceType == "cpu") && strings.Contains(strings.ToUpper(product.Description), "N2D AMD") {
						instanceType = "n2dstandard"
					} else {
						instanceType = "n2standard"
					}
				}

				if (instanceType == "ram" || instanceType == "cpu") && strings.Contains(strings.ToUpper(product.Description), "A2 INSTANCE") {
					instanceType = "a2"
				}

				if (instanceType == "ram" || instanceType == "cpu") && strings.Contains(strings.ToUpper(product.Description), "COMPUTE OPTIMIZED") {
					instanceType = "c2standard"
				}

				if (instanceType == "ram" || instanceType == "cpu") && strings.Contains(strings.ToUpper(product.Description), "E2 INSTANCE") {
					instanceType = "e2"
				}
				partialCPUMap := make(map[string]float64)
				partialCPUMap["e2micro"] = 0.25
				partialCPUMap["e2small"] = 0.5
				partialCPUMap["e2medium"] = 1

				if (instanceType == "ram" || instanceType == "cpu") && strings.Contains(strings.ToUpper(product.Description), "T2D AMD") {
					instanceType = "t2dstandard"
				}
				if (instanceType == "ram" || instanceType == "cpu") && strings.Contains(strings.ToUpper(product.Description), "T2A ARM") {
					instanceType = "t2astandard"
				}

				var gpuType string
				for matchnum, group := range nvidiaTeslaGPURegex.FindStringSubmatch(product.Description) {
					if matchnum == 1 {
						gpuType = strings.ToLower(strings.Join(strings.Split(group, " "), "-"))
						log.Debugf("GCP Billing API: GPU type found: '%s'", gpuType)
					}
				}

				// If a 'Nvidia Tesla' is not found, try 'Nvidia'
				if gpuType == "" {
					for matchnum, group := range nvidiaGPURegex.FindStringSubmatch(product.Description) {
						if matchnum == 1 {
							gpuType = strings.ToLower(strings.Join(strings.Split(group, " "), "-"))
							log.Debugf("GCP Billing API: GPU type found: '%s'", gpuType)
						}
					}
				}

				candidateKeys := []string{}
				if gcp.ValidPricingKeys == nil {
					gcp.ValidPricingKeys = make(map[string]bool)
				}

				for _, region := range product.ServiceRegions {
					switch instanceType {
					case "e2":
						candidateKeys = append(candidateKeys, region+","+"e2micro"+","+usageType)
						candidateKeys = append(candidateKeys, region+","+"e2small"+","+usageType)
						candidateKeys = append(candidateKeys, region+","+"e2medium"+","+usageType)
						candidateKeys = append(candidateKeys, region+","+"e2standard"+","+usageType)
						candidateKeys = append(candidateKeys, region+","+"e2custom"+","+usageType)
					case "a2":
						candidateKeys = append(candidateKeys, region+","+"a2highgpu"+","+usageType)
						candidateKeys = append(candidateKeys, region+","+"a2megagpu"+","+usageType)
						candidateKeys = append(candidateKeys, region+","+"a2ultragpu"+","+usageType)
					default:
						candidateKey := region + "," + instanceType + "," + usageType
						candidateKeys = append(candidateKeys, candidateKey)
					}
				}

				for _, candidateKey := range candidateKeys {
					instanceType = strings.Split(candidateKey, ",")[1] // we may have overridden this while generating candidate keys
					region := strings.Split(candidateKey, ",")[0]
					candidateKeyGPU := candidateKey + ",gpu"
					gcp.ValidPricingKeys[candidateKey] = true
					gcp.ValidPricingKeys[candidateKeyGPU] = true
					if gpuType != "" {
						lastRateIndex := len(product.PricingInfo[0].PricingExpression.TieredRates) - 1
						var nanos float64
						var unitsBaseCurrency int
						if lastRateIndex > -1 && len(product.PricingInfo) > 0 {
							nanos = product.PricingInfo[0].PricingExpression.TieredRates[lastRateIndex].UnitPrice.Nanos
							unitsBaseCurrency, err = strconv.Atoi(product.PricingInfo[0].PricingExpression.TieredRates[lastRateIndex].UnitPrice.Units)
							if err != nil {
								return nil, "", fmt.Errorf("error parsing base unit price for gpu: %w", err)
							}
						} else {
							continue
						}

						// as per https://cloud.google.com/billing/v1/how-tos/catalog-api
						// the hourly price is the whole currency price + the fractional currency price
						hourlyPrice := (nanos * math.Pow10(-9)) + float64(unitsBaseCurrency)

						// GPUs with an hourly price of 0 are reserved versions of GPUs
						// (E.g., SKU "2013-37B4-22EA")
						// and are excluded from cost computations
						if hourlyPrice == 0 {
							log.Debugf("GCP Billing API: excluding reserved GPU SKU #%s", product.SKUID)
							continue
						}

						for k, key := range inputKeys {
							if key.GPUType() == gpuType+","+usageType {
								if region == strings.Split(k, ",")[0] {
									matchedKey := key.Features()
									log.Debugf("GCP Billing API: matched GPU to node: %s: %s", matchedKey, product.Description)
									if pl, ok := gcpPricingList[matchedKey]; ok {
										pl.Node.GPUName = gpuType
										pl.Node.GPUCost = strconv.FormatFloat(hourlyPrice, 'f', -1, 64)
										pl.Node.GPU = "1"
									} else {
										product.Node = &models.Node{
											GPUName: gpuType,
											GPUCost: strconv.FormatFloat(hourlyPrice, 'f', -1, 64),
											GPU:     "1",
										}
										gcpPricingList[matchedKey] = product
									}
								}
							}
						}
					} else {
						_, ok := inputKeys[candidateKey]
						_, ok2 := inputKeys[candidateKeyGPU]
						if ok || ok2 {
							var nanos float64
							var unitsBaseCurrency int
							if len(product.PricingInfo) > 0 {
								lastRateIndex := len(product.PricingInfo[0].PricingExpression.TieredRates) - 1
								if lastRateIndex >= 0 {
									nanos = product.PricingInfo[0].PricingExpression.TieredRates[lastRateIndex].UnitPrice.Nanos
									unitsBaseCurrency, err = strconv.Atoi(product.PricingInfo[0].PricingExpression.TieredRates[lastRateIndex].UnitPrice.Units)
									if err != nil {
										return nil, "", fmt.Errorf("error parsing base unit price for instance: %w", err)
									}
								} else {
									continue
								}
							} else {
								continue
							}

							// as per https://cloud.google.com/billing/v1/how-tos/catalog-api
							// the hourly price is the whole currency price + the fractional currency price
							hourlyPrice := (nanos * math.Pow10(-9)) + float64(unitsBaseCurrency)

							if hourlyPrice == 0 {
								continue
							} else if strings.Contains(strings.ToUpper(product.Description), "RAM") {
								if instanceType == "custom" {
									log.Debugf("GCP Billing API: RAM custom sku '%s'", product.Name)
								}
								if _, ok := gcpPricingList[candidateKey]; ok {
									log.Debugf("GCP Billing API: key '%s': RAM price: %f", candidateKey, hourlyPrice)
									gcpPricingList[candidateKey].Node.RAMCost = strconv.FormatFloat(hourlyPrice, 'f', -1, 64)
								} else {
									log.Debugf("GCP Billing API: key '%s': RAM price: %f", candidateKey, hourlyPrice)
									pricing := &GCPPricing{}
									pricing.Node = &models.Node{
										RAMCost: strconv.FormatFloat(hourlyPrice, 'f', -1, 64),
									}
									partialCPU, pcok := partialCPUMap[instanceType]
									if pcok {
										pricing.Node.VCPU = fmt.Sprintf("%f", partialCPU)
									}
									pricing.Node.UsageType = usageType
									gcpPricingList[candidateKey] = pricing
								}
								if _, ok := gcpPricingList[candidateKeyGPU]; ok {
									log.Debugf("GCP Billing API: key '%s': RAM price: %f", candidateKeyGPU, hourlyPrice)
									gcpPricingList[candidateKeyGPU].Node.RAMCost = strconv.FormatFloat(hourlyPrice, 'f', -1, 64)
								} else {
									log.Debugf("GCP Billing API: key '%s': RAM price: %f", candidateKeyGPU, hourlyPrice)
									pricing := &GCPPricing{}
									pricing.Node = &models.Node{
										RAMCost: strconv.FormatFloat(hourlyPrice, 'f', -1, 64),
									}
									partialCPU, pcok := partialCPUMap[instanceType]
									if pcok {
										pricing.Node.VCPU = fmt.Sprintf("%f", partialCPU)
									}
									pricing.Node.UsageType = usageType
									gcpPricingList[candidateKeyGPU] = pricing
								}
							} else {
								if _, ok := gcpPricingList[candidateKey]; ok {
									log.Debugf("GCP Billing API: key '%s': CPU price: %f", candidateKey, hourlyPrice)
									gcpPricingList[candidateKey].Node.VCPUCost = strconv.FormatFloat(hourlyPrice, 'f', -1, 64)
								} else {
									log.Debugf("GCP Billing API: key '%s': CPU price: %f", candidateKey, hourlyPrice)
									pricing := &GCPPricing{}
									pricing.Node = &models.Node{
										VCPUCost: strconv.FormatFloat(hourlyPrice, 'f', -1, 64),
									}
									partialCPU, pcok := partialCPUMap[instanceType]
									if pcok {
										pricing.Node.VCPU = fmt.Sprintf("%f", partialCPU)
									}
									pricing.Node.UsageType = usageType
									gcpPricingList[candidateKey] = pricing
								}
								if _, ok := gcpPricingList[candidateKeyGPU]; ok {
									log.Debugf("GCP Billing API: key '%s': CPU price: %f", candidateKeyGPU, hourlyPrice)
									gcpPricingList[candidateKeyGPU].Node.VCPUCost = strconv.FormatFloat(hourlyPrice, 'f', -1, 64)
								} else {
									log.Debugf("GCP Billing API: key '%s': CPU price: %f", candidateKeyGPU, hourlyPrice)
									pricing := &GCPPricing{}
									pricing.Node = &models.Node{
										VCPUCost: strconv.FormatFloat(hourlyPrice, 'f', -1, 64),
									}
									partialCPU, pcok := partialCPUMap[instanceType]
									if pcok {
										pricing.Node.VCPU = fmt.Sprintf("%f", partialCPU)
									}
									pricing.Node.UsageType = usageType
									gcpPricingList[candidateKeyGPU] = pricing
								}
							}
						}
					}
				}
			}
		}
		if t == "nextPageToken" {
			pageToken, err := dec.Token()
			if err != nil {
				log.Errorf("Error parsing nextpage token: " + err.Error())
				return nil, "", err
			}
			if pageToken.(string) != "" {
				nextPageToken = pageToken.(string)
			} else {
				nextPageToken = "done"
			}
		}
	}
	return gcpPricingList, nextPageToken, nil
}

func (gcp *GCP) getBillingAPIURL(apiKey, currencyCode string) string {
	return fmt.Sprintf(BillingAPIURLFmt, apiKey, currencyCode)
}

func (gcp *GCP) parsePages(inputKeys map[string]models.Key, pvKeys map[string]models.PVKey) (map[string]*GCPPricing, error) {
	var pages []map[string]*GCPPricing
	c, err := gcp.GetConfig()
	if err != nil {
		return nil, err
	}

	url := gcp.getBillingAPIURL(gcp.APIKey, c.CurrencyCode)

	var parsePagesHelper func(string) error
	parsePagesHelper = func(pageToken string) error {
		if pageToken == "done" {
			return nil
		} else if pageToken != "" {
			url = url + "&pageToken=" + pageToken
		}
		resp, err := http.Get(url)
		if err != nil {
			return err
		}
		page, token, err := gcp.parsePage(resp.Body, inputKeys, pvKeys)
		if err != nil {
			return err
		}
		pages = append(pages, page)
		return parsePagesHelper(token)
	}
	err = parsePagesHelper("")
	if err != nil {
		return nil, err
	}
	returnPages := make(map[string]*GCPPricing)
	for _, page := range pages {
		for k, v := range page {
			if val, ok := returnPages[k]; ok { //keys may need to be merged
				if val.Node != nil {
					if val.Node.VCPUCost == "" {
						val.Node.VCPUCost = v.Node.VCPUCost
					}
					if val.Node.RAMCost == "" {
						val.Node.RAMCost = v.Node.RAMCost
					}
					if val.Node.GPUCost == "" {
						val.Node.GPUCost = v.Node.GPUCost
						val.Node.GPU = v.Node.GPU
						val.Node.GPUName = v.Node.GPUName
					}
				}
				if val.PV != nil {
					if val.PV.Cost == "" {
						val.PV.Cost = v.PV.Cost
					}
				}
			} else {
				returnPages[k] = v
			}
		}
	}

	log.Debugf("ALL PAGES: %+v", returnPages)
	for k, v := range returnPages {
		if v.Node != nil {
			log.Debugf("Returned Page: %s : %+v", k, v.Node)
		}
		if v.PV != nil {
			log.Debugf("Returned Page: %s : %+v", k, v.PV)
		}
	}
	return returnPages, err
}

// DownloadPricingData fetches data from the GCP Pricing API. Requires a key-- a kubecost key is provided for quickstart, but should be replaced by a users.
func (gcp *GCP) DownloadPricingData() error {
	gcp.DownloadPricingDataLock.Lock()
	defer gcp.DownloadPricingDataLock.Unlock()
	c, err := gcp.Config.GetCustomPricingData()
	if err != nil {
		log.Errorf("Error downloading default pricing data: %s", err.Error())
		return err
	}
	gcp.loadGCPAuthSecret()

	gcp.BaseCPUPrice = c.CPU
	gcp.ProjectID = c.ProjectID
	gcp.BillingDataDataset = c.BillingDataDataset

	nodeList := gcp.Clientset.GetAllNodes()
	inputkeys := make(map[string]models.Key)

	defaultRegion := "" // Sometimes, PVs may be missing the region label. In that case assume that they are in the same region as the nodes
	for _, n := range nodeList {
		labels := n.Labels
		if _, ok := labels["cloud.google.com/gke-nodepool"]; ok { // The node is part of a GKE nodepool, so you're paying a cluster management cost
			gcp.clusterManagementPrice = 0.10
			gcp.clusterProvisioner = "GKE"
		}
		r, _ := util.GetRegion(labels)
		if r != "" {
			defaultRegion = r
		}
		key := gcp.GetKey(labels, n)
		inputkeys[key.Features()] = key
	}

	pvList := gcp.Clientset.GetAllPersistentVolumes()
	storageClasses := gcp.Clientset.GetAllStorageClasses()
	storageClassMap := make(map[string]map[string]string)
	for _, storageClass := range storageClasses {
		params := storageClass.Parameters
		storageClassMap[storageClass.Name] = params
		if storageClass.Annotations["storageclass.kubernetes.io/is-default-class"] == "true" || storageClass.Annotations["storageclass.beta.kubernetes.io/is-default-class"] == "true" {
			storageClassMap["default"] = params
			storageClassMap[""] = params
		}
	}

	pvkeys := make(map[string]models.PVKey)
	for _, pv := range pvList {
		params, ok := storageClassMap[pv.Spec.StorageClassName]
		if !ok {
			log.DedupedWarningf(5, "Unable to find params for storageClassName %s", pv.Name)
			continue
		}
		key := gcp.GetPVKey(pv, params, defaultRegion)
		pvkeys[key.Features()] = key
	}

	reserved, err := gcp.getReservedInstances()
	if err != nil {
		log.Errorf("Failed to lookup reserved instance data: %s", err.Error())
	} else {
		gcp.ReservedInstances = reserved

		if zerolog.GlobalLevel() <= zerolog.DebugLevel {
			log.Debugf("Found %d reserved instances", len(reserved))
			for _, r := range reserved {
				log.Debugf("%s", r)
			}
		}
	}

	pages, err := gcp.parsePages(inputkeys, pvkeys)

	if err != nil {
		return err
	}
	gcp.Pricing = pages
	return nil
}

func (gcp *GCP) PVPricing(pvk models.PVKey) (*models.PV, error) {
	gcp.DownloadPricingDataLock.RLock()
	defer gcp.DownloadPricingDataLock.RUnlock()
	pricing, ok := gcp.Pricing[pvk.Features()]
	if !ok {
		log.Debugf("Persistent Volume pricing not found for %s: %s", pvk.GetStorageClass(), pvk.Features())
		return &models.PV{}, nil
	}
	return pricing.PV, nil
}

// Stubbed NetworkPricing for GCP. Pull directly from gcp.json for now
func (gcp *GCP) NetworkPricing() (*models.Network, error) {
	cpricing, err := gcp.Config.GetCustomPricingData()
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

func (gcp *GCP) LoadBalancerPricing() (*models.LoadBalancer, error) {
	fffrc := 0.025
	afrc := 0.010
	lbidc := 0.008

	numForwardingRules := 1.0
	dataIngressGB := 0.0

	var totalCost float64
	if numForwardingRules < 5 {
		totalCost = fffrc*numForwardingRules + lbidc*dataIngressGB
	} else {
		totalCost = fffrc*5 + afrc*(numForwardingRules-5) + lbidc*dataIngressGB
	}
	return &models.LoadBalancer{
		Cost: totalCost,
	}, nil
}

const (
	GCPReservedInstanceResourceTypeRAM string = "MEMORY"
	GCPReservedInstanceResourceTypeCPU string = "VCPU"
	GCPReservedInstanceStatusActive    string = "ACTIVE"
	GCPReservedInstancePlanOneYear     string = "TWELVE_MONTH"
	GCPReservedInstancePlanThreeYear   string = "THIRTY_SIX_MONTH"
)

type GCPReservedInstancePlan struct {
	Name    string
	CPUCost float64
	RAMCost float64
}

type GCPReservedInstance struct {
	ReservedRAM int64
	ReservedCPU int64
	Plan        *GCPReservedInstancePlan
	StartDate   time.Time
	EndDate     time.Time
	Region      string
}

func (r *GCPReservedInstance) String() string {
	return fmt.Sprintf("[CPU: %d, RAM: %d, Region: %s, Start: %s, End: %s]", r.ReservedCPU, r.ReservedRAM, r.Region, r.StartDate.String(), r.EndDate.String())
}

type GCPReservedCounter struct {
	RemainingCPU int64
	RemainingRAM int64
	Instance     *GCPReservedInstance
}

func newReservedCounter(instance *GCPReservedInstance) *GCPReservedCounter {
	return &GCPReservedCounter{
		RemainingCPU: instance.ReservedCPU,
		RemainingRAM: instance.ReservedRAM,
		Instance:     instance,
	}
}

// Two available Reservation plans for GCP, 1-year and 3-year
var gcpReservedInstancePlans map[string]*GCPReservedInstancePlan = map[string]*GCPReservedInstancePlan{
	GCPReservedInstancePlanOneYear: {
		Name:    GCPReservedInstancePlanOneYear,
		CPUCost: 0.019915,
		RAMCost: 0.002669,
	},
	GCPReservedInstancePlanThreeYear: {
		Name:    GCPReservedInstancePlanThreeYear,
		CPUCost: 0.014225,
		RAMCost: 0.001907,
	},
}

func (gcp *GCP) ApplyReservedInstancePricing(nodes map[string]*models.Node) {
	numReserved := len(gcp.ReservedInstances)

	// Early return if no reserved instance data loaded
	if numReserved == 0 {
		log.Debug("[Reserved] No Reserved Instances")
		return
	}

	now := time.Now()

	counters := make(map[string][]*GCPReservedCounter)
	for _, r := range gcp.ReservedInstances {
		if now.Before(r.StartDate) || now.After(r.EndDate) {
			log.Infof("[Reserved] Skipped Reserved Instance due to dates")
			continue
		}

		_, ok := counters[r.Region]
		counter := newReservedCounter(r)
		if !ok {
			counters[r.Region] = []*GCPReservedCounter{counter}
		} else {
			counters[r.Region] = append(counters[r.Region], counter)
		}
	}

	gcpNodes := make(map[string]*clustercache.Node)
	currentNodes := gcp.Clientset.GetAllNodes()

	// Create a node name -> node map
	for _, gcpNode := range currentNodes {
		gcpNodes[gcpNode.Name] = gcpNode
	}

	// go through all provider nodes using k8s nodes for region
	for nodeName, node := range nodes {
		// Reset reserved allocation to prevent double allocation
		node.Reserved = nil

		kNode, ok := gcpNodes[nodeName]
		if !ok {
			log.Debugf("[Reserved] Could not find K8s Node with name: %s", nodeName)
			continue
		}

		nodeRegion, ok := util.GetRegion(kNode.Labels)
		if !ok {
			log.Debug("[Reserved] Could not find node region")
			continue
		}

		reservedCounters, ok := counters[nodeRegion]
		if !ok {
			log.Debugf("[Reserved] Could not find counters for region: %s", nodeRegion)
			continue
		}

		node.Reserved = &models.ReservedInstanceData{
			ReservedCPU: 0,
			ReservedRAM: 0,
		}

		for _, reservedCounter := range reservedCounters {
			if reservedCounter.RemainingCPU != 0 {
				nodeCPU, _ := strconv.ParseInt(node.VCPU, 10, 64)
				nodeCPU -= node.Reserved.ReservedCPU
				node.Reserved.CPUCost = reservedCounter.Instance.Plan.CPUCost

				if reservedCounter.RemainingCPU >= nodeCPU {
					reservedCounter.RemainingCPU -= nodeCPU
					node.Reserved.ReservedCPU += nodeCPU
				} else {
					node.Reserved.ReservedCPU += reservedCounter.RemainingCPU
					reservedCounter.RemainingCPU = 0
				}
			}

			if reservedCounter.RemainingRAM != 0 {
				nodeRAMF, _ := strconv.ParseFloat(node.RAMBytes, 64)
				nodeRAM := int64(nodeRAMF)
				nodeRAM -= node.Reserved.ReservedRAM
				node.Reserved.RAMCost = reservedCounter.Instance.Plan.RAMCost

				if reservedCounter.RemainingRAM >= nodeRAM {
					reservedCounter.RemainingRAM -= nodeRAM
					node.Reserved.ReservedRAM += nodeRAM
				} else {
					node.Reserved.ReservedRAM += reservedCounter.RemainingRAM
					reservedCounter.RemainingRAM = 0
				}
			}
		}
	}
}

func (gcp *GCP) getReservedInstances() ([]*GCPReservedInstance, error) {
	var results []*GCPReservedInstance

	ctx := context.Background()
	computeService, err := compute.NewService(ctx)
	if err != nil {
		return nil, err
	}

	commitments, err := computeService.RegionCommitments.AggregatedList(gcp.ProjectID).Do()
	if err != nil {
		return nil, err
	}

	for regionKey, commitList := range commitments.Items {
		for _, commit := range commitList.Commitments {
			if commit.Status != GCPReservedInstanceStatusActive {
				continue
			}

			var vcpu int64 = 0
			var ram int64 = 0
			for _, resource := range commit.Resources {
				switch resource.Type {
				case GCPReservedInstanceResourceTypeRAM:
					ram = resource.Amount * 1024 * 1024
				case GCPReservedInstanceResourceTypeCPU:
					vcpu = resource.Amount
				default:
					log.Debugf("Failed to handle resource type: %s", resource.Type)
				}
			}

			var region string
			regionStr := strings.Split(regionKey, "/")
			if len(regionStr) == 2 {
				region = regionStr[1]
			}

			timeLayout := "2006-01-02T15:04:05Z07:00"
			startTime, err := time.Parse(timeLayout, commit.StartTimestamp)
			if err != nil {
				log.Warnf("Failed to parse start date: %s", commit.StartTimestamp)
				continue
			}

			endTime, err := time.Parse(timeLayout, commit.EndTimestamp)
			if err != nil {
				log.Warnf("Failed to parse end date: %s", commit.EndTimestamp)
				continue
			}

			// Look for a plan based on the name. Default to One Year if it fails
			plan, ok := gcpReservedInstancePlans[commit.Plan]
			if !ok {
				plan = gcpReservedInstancePlans[GCPReservedInstancePlanOneYear]
			}

			results = append(results, &GCPReservedInstance{
				Region:      region,
				ReservedRAM: ram,
				ReservedCPU: vcpu,
				Plan:        plan,
				StartDate:   startTime,
				EndDate:     endTime,
			})
		}
	}

	return results, nil
}

type pvKey struct {
	ProviderID             string
	Labels                 map[string]string
	StorageClass           string
	StorageClassParameters map[string]string
	DefaultRegion          string
}

func (key *pvKey) ID() string {
	return key.ProviderID
}

func (key *pvKey) GetStorageClass() string {
	return key.StorageClass
}

func (gcp *GCP) GetPVKey(pv *clustercache.PersistentVolume, parameters map[string]string, defaultRegion string) models.PVKey {
	providerID := ""
	if pv.Spec.GCEPersistentDisk != nil {
		providerID = pv.Spec.GCEPersistentDisk.PDName
	}
	return &pvKey{
		ProviderID:             providerID,
		Labels:                 pv.Labels,
		StorageClass:           pv.Spec.StorageClassName,
		StorageClassParameters: parameters,
		DefaultRegion:          defaultRegion,
	}
}

func (key *pvKey) Features() string {
	// TODO: regional cluster pricing.
	storageClass := key.StorageClassParameters["type"]
	if storageClass == "pd-ssd" {
		storageClass = "ssd"
	} else if storageClass == "pd-standard" {
		storageClass = "pdstandard"
	}
	replicationType := ""
	if rt, ok := key.StorageClassParameters["replication-type"]; ok {
		if rt == "regional-pd" {
			replicationType = ",regional"
		}
	}
	region, _ := util.GetRegion(key.Labels)
	if region == "" {
		region = key.DefaultRegion
	}
	return region + "," + storageClass + replicationType
}

type gcpKey struct {
	Labels map[string]string
}

func (gcp *GCP) GetKey(labels map[string]string, n *clustercache.Node) models.Key {
	return &gcpKey{
		Labels: labels,
	}
}

func (gcp *gcpKey) ID() string {
	return ""
}

func (k *gcpKey) GPUCount() int {
	return 0
}

func (gcp *gcpKey) GPUType() string {
	if t, ok := gcp.Labels[GKE_GPU_TAG]; ok {
		usageType := getUsageType(gcp.Labels)
		log.Debugf("GPU of type: \"%s\" found", t)
		return t + "," + usageType
	}
	return ""
}

func parseGCPInstanceTypeLabel(it string) string {
	var instanceType string

	splitByDash := strings.Split(it, "-")

	// GKE nodes are labeled with the GCP instance type, but users can deploy on GCP
	// with tools like K3s, whose instance type labels will be "k3s". This logic
	// avoids a panic in the slice operation then there are no dashes (-) in the
	// instance type label value.
	if len(splitByDash) < 2 {
		instanceType = "unknown"
	} else {
		instanceType = strings.ToLower(strings.Join(splitByDash[:2], ""))
		if instanceType == "n1highmem" || instanceType == "n1highcpu" {
			instanceType = "n1standard" // These are priced the same. TODO: support n1ultrahighmem
		} else if instanceType == "n2highmem" || instanceType == "n2highcpu" {
			instanceType = "n2standard"
		} else if instanceType == "e2highmem" || instanceType == "e2highcpu" {
			instanceType = "e2standard"
		} else if instanceType == "n2dhighmem" || instanceType == "n2dhighcpu" {
			instanceType = "n2dstandard"
		} else if strings.HasPrefix(instanceType, "custom") {
			instanceType = "custom" // The suffix of custom does not matter
		}
	}

	return instanceType
}

// GetKey maps node labels to information needed to retrieve pricing data
func (gcp *gcpKey) Features() string {
	var instanceType string
	it, _ := util.GetInstanceType(gcp.Labels)
	if it == "" {
		log.DedupedErrorf(1, "Missing or Unknown 'node.kubernetes.io/instance-type' node label")
		instanceType = "unknown"
	} else {
		instanceType = parseGCPInstanceTypeLabel(it)
	}

	r, _ := util.GetRegion(gcp.Labels)
	region := strings.ToLower(r)
	usageType := getUsageType(gcp.Labels)

	if _, ok := gcp.Labels[GKE_GPU_TAG]; ok {
		return region + "," + instanceType + "," + usageType + "," + "gpu"
	}

	return region + "," + instanceType + "," + usageType
}

// AllNodePricing returns the GCP pricing objects stored
func (gcp *GCP) AllNodePricing() (interface{}, error) {
	gcp.DownloadPricingDataLock.RLock()
	defer gcp.DownloadPricingDataLock.RUnlock()
	return gcp.Pricing, nil
}

func (gcp *GCP) getPricing(key models.Key) (*GCPPricing, bool) {
	gcp.DownloadPricingDataLock.RLock()
	defer gcp.DownloadPricingDataLock.RUnlock()
	n, ok := gcp.Pricing[key.Features()]
	return n, ok
}
func (gcp *GCP) isValidPricingKey(key models.Key) bool {
	gcp.DownloadPricingDataLock.RLock()
	defer gcp.DownloadPricingDataLock.RUnlock()
	_, ok := gcp.ValidPricingKeys[key.Features()]
	return ok
}

// NodePricing returns GCP pricing data for a single node
func (gcp *GCP) NodePricing(key models.Key) (*models.Node, models.PricingMetadata, error) {
	meta := models.PricingMetadata{}

	c, err := gcp.Config.GetCustomPricingData()
	if err != nil {
		meta.Warnings = append(meta.Warnings, fmt.Sprintf("failed to detect currency: %s", err))
	} else {
		meta.Currency = c.CurrencyCode
	}

	if n, ok := gcp.getPricing(key); ok {
		log.Debugf("Returning pricing for node %s: %+v from SKU %s", key, n.Node, n.Name)

		// Add pricing URL, but redact the key (hence, "***"")
		meta.Source = fmt.Sprintf("Downloaded pricing from %s", gcp.getBillingAPIURL("***", c.CurrencyCode))

		n.Node.BaseCPUPrice = gcp.BaseCPUPrice

		return n.Node, meta, nil
	} else if ok := gcp.isValidPricingKey(key); ok {
		meta.Warnings = append(meta.Warnings, fmt.Sprintf("No pricing found, but key is valid: %s", key.Features()))

		err := gcp.DownloadPricingData()
		if err != nil {
			log.Warnf("no pricing data found for %s", key.Features())

			meta.Warnings = append(meta.Warnings, "Failed to download pricing data")

			return nil, meta, fmt.Errorf("failed to download pricing data: %w", err)
		}
		if n, ok := gcp.getPricing(key); ok {
			log.Debugf("Returning pricing for node %s: %+v from SKU %s", key, n.Node, n.Name)

			// Add pricing URL, but redact the key (hence, "***"")
			meta.Source = fmt.Sprintf("Downloaded pricing from %s", gcp.getBillingAPIURL("***", c.CurrencyCode))

			n.Node.BaseCPUPrice = gcp.BaseCPUPrice

			return n.Node, meta, nil
		}

		log.Warnf("no pricing data found for %s", key.Features())

		meta.Warnings = append(meta.Warnings, "Failed to find pricing after downloading data, but key is valid")

		return nil, meta, fmt.Errorf("failed to find pricing data: %s", key.Features())
	}

	meta.Warnings = append(meta.Warnings, fmt.Sprintf("No pricing found, and key is not valid: %s", key.Features()))

	return nil, meta, fmt.Errorf("no pricing data found for %s", key.Features())
}

func (gcp *GCP) ServiceAccountStatus() *models.ServiceAccountStatus {
	return &models.ServiceAccountStatus{
		Checks: []*models.ServiceAccountCheck{},
	}
}

func (gcp *GCP) PricingSourceStatus() map[string]*models.PricingSource {
	return make(map[string]*models.PricingSource)
}

func (gcp *GCP) CombinedDiscountForNode(instanceType string, isPreemptible bool, defaultDiscount, negotiatedDiscount float64) float64 {
	class := strings.Split(instanceType, "-")[0]
	return 1.0 - ((1.0 - sustainedUseDiscount(class, defaultDiscount, isPreemptible)) * (1.0 - negotiatedDiscount))
}

func (gcp *GCP) Regions() []string {

	regionOverrides := env.GetRegionOverrideList()

	if len(regionOverrides) > 0 {
		log.Debugf("Overriding GCP regions with configured region list: %+v", regionOverrides)
		return regionOverrides
	}

	return gcpRegions
}

func sustainedUseDiscount(class string, defaultDiscount float64, isPreemptible bool) float64 {
	if isPreemptible {
		return 0.0
	}
	discount := defaultDiscount
	switch class {
	case "e2", "f1", "g1":
		discount = 0.0
	case "n2", "n2d":
		discount = 0.2
	}
	return discount
}

func ParseGCPProjectID(id string) string {
	// gce://guestbook-12345/...
	//  => guestbook-12345
	match := gceRegex.FindStringSubmatch(id)
	if len(match) >= 2 {
		return match[1]
	}
	// Return empty string if an account could not be parsed from provided string
	return ""
}

func getUsageType(labels map[string]string) string {
	if t, ok := labels[GKEPreemptibleLabel]; ok && t == "true" {
		return "preemptible"
	} else if t, ok := labels[GKESpotLabel]; ok && t == "true" {
		// https://cloud.google.com/kubernetes-engine/docs/concepts/spot-vms
		return "preemptible"
	} else if t, ok := labels[models.KarpenterCapacityTypeLabel]; ok && t == models.KarpenterCapacitySpotTypeValue {
		return "preemptible"
	}
	return "ondemand"
}

// PricingSourceSummary returns the pricing source summary for the provider.
// The summary represents what was _parsed_ from the pricing source, not
// everything that was _available_ in the pricing source.
func (gcp *GCP) PricingSourceSummary() interface{} {
	return gcp.Pricing
}
