package opencost

////////////////////////////////////////////////////////////////////////////////
// NOTE: If you add fields to _any_ struct that is serialized by bingen, please
// make sure to add those fields to the END of the struct definition. This is
// required for backwards-compatibility. So:
//
// type Foo struct {
//     ExistingField1 string
//     ExistingField2 int
// }
//
// becomes:
//
// type Foo struct {
//     ExistingField1 string
//     ExistingField2 int
//     NewField       float64 // @bingen: <- annotation ref: bingen README
// }
//
////////////////////////////////////////////////////////////////////////////////

// Default Version Set (uses -version flag passed) includes shared resources
// @bingen:generate:Window
// @bingen:generate:Coverage
// @bingen:generate:CoverageSet

// Asset Version Set: Includes Asset pipeline specific resources
// @bingen:set[name=Assets,version=21]
// @bingen:generate:Any
// @bingen:generate:Asset
// @bingen:generate:AssetLabels
// @bingen:generate:AssetProperties
// @bingen:generate:AssetProperty
// @bingen:generate[stringtable,preprocess,postprocess]:AssetSet
// @bingen:generate:AssetSetRange
// @bingen:generate:Breakdown
// @bingen:generate:Cloud
// @bingen:generate:ClusterManagement
// @bingen:generate:Disk
// @bingen:generate:LoadBalancer
// @bingen:generate:Network
// @bingen:generate:Node
// @bingen:generate:NodeOverhead
// @bingen:generate:SharedAsset
// @bingen:end

// Allocation Version Set: Includes Allocation pipeline specific resources
// @bingen:set[name=Allocation,version=23]
// @bingen:generate[migrate]:Allocation
// @bingen:generate[stringtable]:AllocationSet
// @bingen:generate:AllocationSetRange
// @bingen:generate:AllocationProperties
// @bingen:generate:AllocationProperty
// @bingen:generate:AllocationLabels
// @bingen:generate:AllocationAnnotations
// @bingen:generate:RawAllocationOnlyData
// @bingen:generate:PVAllocations
// @bingen:generate:PVKey
// @bingen:generate:PVAllocation
// @bingen:generate:LbAllocations
// @bingen:generate:LbAllocation
// @bingen:generate:GPUAllocation
// @bingen:end

// @bingen:set[name=CloudCost,version=3]
// @bingen:generate:CloudCost
// @bingen:generate:CostMetric
// @bingen:generate[stringtable]:CloudCostSet
// @bingen:generate:CloudCostSetRange
// @bingen:generate:CloudCostProperties
// @bingen:generate:CloudCostLabels
// @bingen:end

//go:generate bingen -package=opencost -version=17 -buffer=github.com/opencost/opencost/core/pkg/util
