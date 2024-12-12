package carbon

import (
	"embed"
	"encoding/csv"
	"fmt"
	"strconv"
	"strings"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util"
)

//go:embed carbonlookupdata.csv
var f embed.FS

type carbonLookupKeyDisk struct {
	provider string
	region   string
}

type carbonLookupKeyNode struct {
	provider     string
	region       string
	instanceType string
}

var carbonLookupNode map[carbonLookupKeyNode]float64
var carbonLookupDisk map[carbonLookupKeyDisk]float64

// Opencost does not build network types
var carbonValidInstanceTypes map[string]string
var carbonValidRegions map[string]string

func init() {

	carbonData, err := f.ReadFile("carbonlookupdata.csv")
	if err != nil {
		log.Errorf("Error getting content of carbon lookup file: %s", err)
		return
	}

	reader := csv.NewReader(strings.NewReader(string(carbonData)))

	// skip header
	_, err = reader.Read()
	if err != nil {
		log.Errorf("Error reading carbon lookup data: %s", err)
		return
	}

	dat, err := reader.ReadAll()
	if err != nil {
		log.Errorf("Error reading carbon lookup data: %s", err)
		return
	}

	carbonLookupNode = make(map[carbonLookupKeyNode]float64)
	carbonLookupDisk = make(map[carbonLookupKeyDisk]float64)

	carbonValidInstanceTypes = make(map[string]string)
	carbonValidRegions = make(map[string]string)

	for _, carbonItem := range dat {

		if coeff, err := strconv.ParseFloat(carbonItem[5], 64); err != nil {

			panic(fmt.Errorf("error setting up carbon lookup table: malformed carbon cost '%s'", carbonItem[5]))

		} else {

			provider := carbonItem[0]
			region := carbonItem[1]
			instanceType := carbonItem[2]
			assetType := carbonItem[3]

			switch assetType {
			case "Node":
				carbonLookupNode[carbonLookupKeyNode{
					provider:     provider,
					region:       region,
					instanceType: instanceType,
				}] = coeff
			case "Disk":
				carbonLookupDisk[carbonLookupKeyDisk{
					provider: provider,
					region:   region,
				}] = coeff
			}

			carbonValidInstanceTypes[instanceType] = provider
			carbonValidRegions[region] = provider

		}

	}

}

type CarbonRow struct {
	Co2e float64 `json:"co2e"`
}

func RelateCarbonAssets(as *opencost.AssetSet) (map[string]CarbonRow, error) {

	res := make(map[string]CarbonRow)

	for key, asset := range as.Assets {

		// If no valid region, default to per-provider calculated average
		region, _ := util.GetRegion(asset.GetLabels())
		if _, ok := carbonValidRegions[region]; !ok {
			region = "average-region"
		}

		// If no valid instance type, also default to per-provider calculated average
		instanceType, _ := util.GetInstanceType(asset.GetLabels())
		if _, ok := carbonValidInstanceTypes[instanceType]; !ok {
			region = "average-region"
		}

		provider := getProviderFromProviderID(asset.GetProperties().ProviderID)

		// If we're not able to parse the provider id, try to fetch the provider from the carbon data
		if provider == "" && region != "average-region" {
			provider = carbonValidRegions[region]
		} else {
			if asset.Type() == opencost.NodeAssetType || asset.Type() == opencost.DiskAssetType {
				log.DedupedErrorf(10, "Cannot infer region information for asset '%s'", asset.GetProperties().ProviderID)
			}
		}

		var carbonCoeff float64
		switch asset.Type() {
		case opencost.NodeAssetType:
			carbonCoeff = carbonLookupNode[carbonLookupKeyNode{
				provider:     provider,
				region:       region,
				instanceType: instanceType,
			}]
		case opencost.DiskAssetType:
			carbonCoeff = carbonLookupDisk[carbonLookupKeyDisk{
				provider: provider,
				region:   region,
			}]
		}

		res[key] = CarbonRow{
			Co2e: carbonCoeff * asset.Minutes() / 60,
		}

	}

	return res, nil

}

func getProviderFromProviderID(providerid string) string {

	if strings.HasPrefix(providerid, "gke") {
		return opencost.GCPProvider
	} else if strings.HasPrefix(providerid, "i-") {
		return opencost.AWSProvider
	} else if strings.HasPrefix(providerid, "azure") {
		return opencost.AzureProvider
	}

	return ""

}
