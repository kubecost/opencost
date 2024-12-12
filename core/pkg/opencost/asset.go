package opencost

import (
	"encoding"
	"fmt"
	"math"
	"strings"
	"time"

	filter21 "github.com/opencost/opencost/core/pkg/filter"
	"github.com/opencost/opencost/core/pkg/filter/ast"
	"github.com/opencost/opencost/core/pkg/filter/matcher"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/core/pkg/util/promutil"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
)

// UndefinedKey is used in composing Asset group keys if the group does not have that property defined.
// E.g. if aggregating on Cluster, Assets in the AssetSet where Asset has no cluster will be grouped under key "__undefined__"
const UndefinedKey = "__undefined__"

// LocalStorageClass is used to assign storage class of local disks.
const LocalStorageClass = "__local__"

// UnknownStorageClass is used to assign storage class of persistent volume whose information is unable to be traced.
const UnknownStorageClass = "__unknown__"

// Asset defines an entity within a cluster that has a defined cost over a
// given period of time.
type Asset interface {
	// Type identifies the kind of Asset, which must always exist and should
	// be defined by the underlying type implementing the interface.
	Type() AssetType

	// GetProperties are a map of predefined traits, which may or may not exist,
	// but must conform to the AssetProperty schema
	GetProperties() *AssetProperties
	SetProperties(*AssetProperties)

	// GetLabels are a map of undefined string-to-string values
	GetLabels() AssetLabels
	SetLabels(AssetLabels)

	// Monetary values
	GetAdjustment() float64
	SetAdjustment(float64)
	TotalCost() float64

	// Temporal values
	GetStart() time.Time
	GetEnd() time.Time
	SetStartEnd(time.Time, time.Time)
	GetWindow() Window
	SetWindow(Window)
	ExpandWindow(Window)
	Minutes() float64

	// Operations and comparisons
	Add(Asset) Asset
	Clone() Asset
	Equal(Asset) bool
	SanitizeNaN()

	// Representations
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
	json.Marshaler
	fmt.Stringer
}

// AssetToExternalAllocation converts the given asset to an Allocation, given
// the Properties to use to aggregate, and the mapping from Allocation property
// to Asset label. For example, consider this asset:
//
// CURRENT: Asset ETL stores its data ALREADY MAPPED from label to k8s concept. This isn't ideal-- see the TODO.
//
//	  Cloud {
//		   TotalCost: 10.00,
//		   Labels{
//	      "kubernetes_namespace":"monitoring",
//		     "env":"prod"
//		   }
//	  }
//
// Given the following parameters, we expect to return:
//
//  1. single-prop full match
//     aggregateBy = ["namespace"]
//     => Allocation{Name: "monitoring", ExternalCost: 10.00, TotalCost: 10.00}, nil
//
//  2. multi-prop full match
//     aggregateBy = ["namespace", "label:env"]
//     allocationPropertyLabels = {"namespace":"kubernetes_namespace"}
//     => Allocation{Name: "monitoring/env=prod", ExternalCost: 10.00, TotalCost: 10.00}, nil
//
//  3. multi-prop partial match
//     aggregateBy = ["namespace", "label:foo"]
//     => Allocation{Name: "monitoring/__unallocated__", ExternalCost: 10.00, TotalCost: 10.00}, nil
//
//  4. no match
//     aggregateBy = ["cluster"]
//     => nil, err
//
// TODO:
//
//	  Cloud {
//		   TotalCost: 10.00,
//		   Labels{
//	      "kubernetes_namespace":"monitoring",
//		     "env":"prod"
//		   }
//	  }
//
// Given the following parameters, we expect to return:
//
//  1. single-prop full match
//     aggregateBy = ["namespace"]
//     allocationPropertyLabels = {"namespace":"kubernetes_namespace"}
//     => Allocation{Name: "monitoring", ExternalCost: 10.00, TotalCost: 10.00}, nil
//
//  2. multi-prop full match
//     aggregateBy = ["namespace", "label:env"]
//     allocationPropertyLabels = {"namespace":"kubernetes_namespace"}
//     => Allocation{Name: "monitoring/env=prod", ExternalCost: 10.00, TotalCost: 10.00}, nil
//
//  3. multi-prop partial match
//     aggregateBy = ["namespace", "label:foo"]
//     allocationPropertyLabels = {"namespace":"kubernetes_namespace"}
//     => Allocation{Name: "monitoring/__unallocated__", ExternalCost: 10.00, TotalCost: 10.00}, nil
//
//  4. no match
//     aggregateBy = ["cluster"]
//     allocationPropertyLabels = {"namespace":"kubernetes_namespace"}
//     => nil, err
//
// (See asset_test.go for assertions of these examples and more.)
func AssetToExternalAllocation(asset Asset, aggregateBy []string, labelConfig *LabelConfig) (*Allocation, error) {
	if asset == nil {
		return nil, fmt.Errorf("asset is nil")
	}

	// Use default label config if one is not provided.
	if labelConfig == nil {
		labelConfig = NewLabelConfig()
	}

	// names will collect the slash-separated names accrued by iterating over
	// aggregateBy and checking the relevant labels.
	names := []string{}

	// match records whether or not a match was found in the Asset labels,
	// such that is can genuinely be turned into an external Allocation.
	match := false

	// props records the relevant Properties to set on the resultant Allocation
	props := AllocationProperties{}

	// For each aggregation parameter, try to find a match in the asset's
	// labels, using the labelConfig to translate. For an aggregation parameter
	// defined by a label (e.g. "label:app") this is simple: look for the label
	// and use it (e.g. if "app" is a defined label on the asset, then use its
	// value). For an aggregation parameter defined by a non-label property
	// (e.g. "namespace") this requires using the labelConfig to look up the
	// label name associated with that property and to use the value under that
	// label, if set (e.g. if the aggregation property is "namespace" and the
	// labelConfig is configured with "namespace_external_label" => "kubens"
	// and the asset has label "kubens":"kubecost", then file the asset as an
	// external cost under "kubecost").
	for _, aggBy := range aggregateBy {
		name := labelConfig.GetExternalAllocationName(asset.GetLabels(), aggBy)

		if name == "" {
			// No matching label has been defined in the cost-analyzer label config
			// relating to the given aggregateBy property.
			names = append(names, UnallocatedSuffix)
			continue
		} else {
			names = append(names, name)
			match = true

			// Default labels to an empty map, if necessary
			if props.Labels == nil {
				props.Labels = map[string]string{}
			}

			// Set the corresponding property on props
			switch aggBy {
			case AllocationClusterProp:
				props.Cluster = name
			case AllocationNodeProp:
				props.Node = name
			case AllocationNamespaceProp:
				props.Namespace = name
			case AllocationControllerKindProp:
				props.ControllerKind = name
			case AllocationControllerProp:
				props.Controller = name
			case AllocationPodProp:
				props.Pod = name
			case AllocationContainerProp:
				props.Container = name
			case AllocationServiceProp:
				props.Services = []string{name}
			case AllocationDeploymentProp:
				props.Controller = name
				props.ControllerKind = "deployment"
			case AllocationStatefulSetProp:
				props.Controller = name
				props.ControllerKind = "statefulset"
			case AllocationDaemonSetProp:
				props.Controller = name
				props.ControllerKind = "daemonset"
			case AllocationDepartmentProp:
				props.Labels[labelConfig.DepartmentLabel] = name
			case AllocationEnvironmentProp:
				props.Labels[labelConfig.EnvironmentLabel] = name
			case AllocationOwnerProp:
				props.Labels[labelConfig.OwnerLabel] = name
			case AllocationProductProp:
				props.Labels[labelConfig.ProductLabel] = name
			case AllocationTeamProp:
				props.Labels[labelConfig.TeamLabel] = name
			default:
				if strings.HasPrefix(aggBy, "label:") {
					// Set the corresponding label in props
					labelName := strings.TrimPrefix(aggBy, "label:")
					labelValue := strings.TrimPrefix(name, labelName+"=")
					props.Labels[labelName] = labelValue
				}
			}
		}
	}

	// If not a signle aggregation property generated a matching label property,
	// then consider the asset ineligible to be treated as an external allocation.
	if !match {
		return nil, fmt.Errorf("asset does not qualify as an external allocation")
	}

	// Use naming to label as an external allocation. See IsExternal() for more.
	names = append(names, ExternalSuffix)

	// TODO: external allocation: efficiency?
	// TODO: external allocation: resource totals?
	return &Allocation{
		Name:         strings.Join(names, "/"),
		Properties:   &props,
		Window:       asset.GetWindow().Clone(),
		Start:        asset.GetStart(),
		End:          asset.GetEnd(),
		ExternalCost: asset.TotalCost(),
	}, nil
}

// key is used to determine uniqueness of an Asset, for instance during Insert
// to determine if two Assets should be combined. Passing `nil` `aggregateBy` indicates
// that all available `AssetProperty` keys should be used. Passing empty `aggregateBy` indicates that
// no key should be used (e.g. to aggregate all assets). Passing one or more `aggregateBy`
// values will key by only those values.
// Valid values of `aggregateBy` elements are strings which are an `AssetProperty`, and strings prefixed
// with `"label:"`.
func key(a Asset, aggregateBy []string, labelConfig *LabelConfig) (string, error) {
	var buffer strings.Builder

	if labelConfig == nil {
		labelConfig = NewLabelConfig()
	}

	if aggregateBy == nil {
		aggregateBy = []string{
			string(AssetProviderProp),
			string(AssetAccountProp),
			string(AssetProjectProp),
			string(AssetCategoryProp),
			string(AssetClusterProp),
			string(AssetTypeProp),
			string(AssetServiceProp),
			string(AssetProviderIDProp),
			string(AssetNameProp),
		}
	}

	for i, s := range aggregateBy {
		key := ""
		switch true {
		case s == string(AssetProviderProp):
			key = a.GetProperties().Provider
		case s == string(AssetAccountProp):
			key = a.GetProperties().Account
		case s == string(AssetProjectProp):
			key = a.GetProperties().Project
		case s == string(AssetClusterProp):
			key = a.GetProperties().Cluster
		case s == string(AssetCategoryProp):
			key = a.GetProperties().Category
		case s == string(AssetTypeProp):
			key = a.Type().String()
		case s == string(AssetServiceProp):
			key = a.GetProperties().Service
		case s == string(AssetProviderIDProp):
			key = a.GetProperties().ProviderID
		case s == string(AssetNameProp):
			key = a.GetProperties().Name
		case s == string(AssetDepartmentProp):
			key = getKeyFromLabelConfig(a, labelConfig, labelConfig.DepartmentExternalLabel)
		case s == string(AssetEnvironmentProp):
			key = getKeyFromLabelConfig(a, labelConfig, labelConfig.EnvironmentExternalLabel)
		case s == string(AssetOwnerProp):
			key = getKeyFromLabelConfig(a, labelConfig, labelConfig.OwnerExternalLabel)
		case s == string(AssetProductProp):
			key = getKeyFromLabelConfig(a, labelConfig, labelConfig.ProductExternalLabel)
		case s == string(AssetTeamProp):
			key = getKeyFromLabelConfig(a, labelConfig, labelConfig.TeamExternalLabel)
		case strings.HasPrefix(s, "label:"):
			if labelKey := strings.TrimPrefix(s, "label:"); labelKey != "" {
				labelVal := a.GetLabels()[labelKey]
				if labelVal == "" {
					key = "__undefined__"
				} else {
					key = fmt.Sprintf("%s=%s", labelKey, labelVal)
				}
			} else {
				// Don't allow aggregating on label ""
				return "", fmt.Errorf("attempted to aggregate on invalid key: %s", s)
			}
		default:
			return "", fmt.Errorf("attempted to aggregate on invalid key: %s", s)
		}

		if key != "" {
			buffer.WriteString(key)
		} else {
			buffer.WriteString(UndefinedKey)
		}
		if i != (len(aggregateBy) - 1) {
			buffer.WriteString("/")
		}
	}
	return buffer.String(), nil
}

func getKeyFromLabelConfig(a Asset, labelConfig *LabelConfig, label string) string {
	labels := a.GetLabels()
	if labels == nil {
		return UnallocatedSuffix
	} else {
		key := UnallocatedSuffix
		labelNames := strings.Split(label, ",")
		for _, labelName := range labelNames {
			name := labelConfig.Sanitize(labelName)
			if labelValue, ok := labels[name]; ok {
				key = labelValue
				break
			}
		}
		return key
	}
}

func GetAssetKey(a Asset, aggregateBy []string) (string, error) {
	return key(a, aggregateBy, nil)
}

func toString(a Asset) string {
	return fmt.Sprintf("%s{%s}%s=%.2f", a.Type().String(), a.GetProperties(), a.GetWindow(), a.TotalCost())
}

// AssetLabels is a schema-free mapping of key/value pairs that can be
// attributed to an Asset as a flexible a
type AssetLabels map[string]string

// Clone returns a cloned map of labels
func (al AssetLabels) Clone() AssetLabels {
	clone := make(AssetLabels, len(al))

	for label, value := range al {
		clone[label] = value
	}

	return clone
}

// Equal returns true only if the two set of labels are exact matches
func (al AssetLabels) Equal(that AssetLabels) bool {
	if len(al) != len(that) {
		return false
	}

	for label, value := range al {
		if thatValue, ok := that[label]; !ok || thatValue != value {
			return false
		}
	}

	return true
}

// Merge retains only the labels shared with the given AssetLabels
func (al AssetLabels) Merge(that AssetLabels) AssetLabels {
	result := AssetLabels{}

	for label, value := range al {
		if thatValue, ok := that[label]; ok && thatValue == value {
			result[label] = value
		}
	}

	return result
}

// Append joins AssetLabels with a given map of labels
func (al AssetLabels) Append(newLabels map[string]string, overwrite bool) {
	if len(newLabels) == 0 {
		return
	}

	for label, value := range newLabels {
		if _, ok := al[label]; ok {
			if overwrite {
				al[label] = value
			}
		} else {
			al[label] = value
		}
	}
}

// AssetType identifies a type of Asset
type AssetType int

const (
	// AnyAssetType describes the Any AssetType
	AnyAssetType AssetType = iota

	// CloudAssetType describes the Cloud AssetType
	CloudAssetType

	// ClusterManagementAssetType describes the ClusterManagement AssetType
	ClusterManagementAssetType

	// DiskAssetType describes the Disk AssetType
	DiskAssetType

	// LoadBalancerAssetType describes the LoadBalancer AssetType
	LoadBalancerAssetType

	// NetworkAssetType describes the Network AssetType
	NetworkAssetType

	// NodeAssetType describes the Node AssetType
	NodeAssetType

	// SharedAssetType describes the Shared AssetType
	SharedAssetType
)

// ParseAssetType attempts to parse the given string into an AssetType
func ParseAssetType(text string) (AssetType, error) {
	switch strings.TrimSpace(strings.ToLower(text)) {
	case "cloud":
		return CloudAssetType, nil
	case "clustermanagement":
		return ClusterManagementAssetType, nil
	case "disk":
		return DiskAssetType, nil
	case "loadbalancer":
		return LoadBalancerAssetType, nil
	case "network":
		return NetworkAssetType, nil
	case "node":
		return NodeAssetType, nil
	case "shared":
		return SharedAssetType, nil
	}
	return AnyAssetType, fmt.Errorf("invalid asset type: %s", text)
}

// String converts the given AssetType to a string
func (at AssetType) String() string {
	return [...]string{
		"Asset",
		"Cloud",
		"ClusterManagement",
		"Disk",
		"LoadBalancer",
		"Network",
		"Node",
		"Shared",
	}[at]
}

// Any is the most general Asset, which is usually created as a result of
// adding two Assets of different types.
type Any struct {
	Labels     AssetLabels
	Properties *AssetProperties
	Start      time.Time
	End        time.Time
	Window     Window
	Adjustment float64
	Cost       float64
}

// NewAsset creates a new Any-type Asset for the given period of time
func NewAsset(start, end time.Time, window Window) *Any {
	return &Any{
		Labels:     AssetLabels{},
		Properties: &AssetProperties{},
		Start:      start,
		End:        end,
		Window:     window.Clone(),
	}
}

// Type returns the Asset's type
func (a *Any) Type() AssetType {
	return AnyAssetType
}

// Properties returns the Asset's Properties
func (a *Any) GetProperties() *AssetProperties {
	return a.Properties
}

// SetProperties sets the Asset's Properties
func (a *Any) SetProperties(props *AssetProperties) {
	a.Properties = props
}

// Labels returns the Asset's labels
func (a *Any) GetLabels() AssetLabels {
	return a.Labels
}

// SetLabels sets the Asset's labels
func (a *Any) SetLabels(labels AssetLabels) {
	a.Labels = labels
}

// Adjustment returns the Asset's cost adjustment
func (a *Any) GetAdjustment() float64 {
	return a.Adjustment
}

// SetAdjustment sets the Asset's cost adjustment
func (a *Any) SetAdjustment(adj float64) {
	a.Adjustment = adj
}

// TotalCost returns the Asset's TotalCost
func (a *Any) TotalCost() float64 {
	return a.Cost + a.Adjustment
}

// Start returns the Asset's start time within the window
func (a *Any) GetStart() time.Time {
	return a.Start
}

// End returns the Asset's end time within the window
func (a *Any) GetEnd() time.Time {
	return a.End
}

// Minutes returns the number of minutes the Asset was active within the window
func (a *Any) Minutes() float64 {
	return a.End.Sub(a.Start).Minutes()
}

// Window returns the Asset's window
func (a *Any) GetWindow() Window {
	return a.Window
}

func (a *Any) SetWindow(window Window) {
	a.Window = window
}

// ExpandWindow expands the Asset's window by the given window
func (a *Any) ExpandWindow(window Window) {
	a.Window = a.Window.Expand(window)
}

// SetStartEnd sets the Asset's Start and End fields
func (a *Any) SetStartEnd(start, end time.Time) {
	if a.Window.Contains(start) {
		a.Start = start
	} else {
		log.Warnf("Any.SetStartEnd: start %s not in %s", start, a.Window)
	}

	if a.Window.Contains(end) {
		a.End = end
	} else {
		log.Warnf("Any.SetStartEnd: end %s not in %s", end, a.Window)
	}
}

// Add sums the Asset with the given Asset to produce a new Asset, maintaining
// as much relevant information as possible (i.e. type, Properties, labels).
func (a *Any) Add(that Asset) Asset {
	this := a.Clone().(*Any)

	props := a.Properties.Merge(that.GetProperties())
	labels := a.Labels.Merge(that.GetLabels())

	start := a.Start
	if that.GetStart().Before(start) {
		start = that.GetStart()
	}
	end := a.End
	if that.GetEnd().After(end) {
		end = that.GetEnd()
	}
	window := a.Window.Expand(that.GetWindow())

	this.Start = start
	this.End = end
	this.Window = window
	this.SetProperties(props)
	this.SetLabels(labels)
	this.Adjustment += that.GetAdjustment()
	this.Cost += (that.TotalCost() - that.GetAdjustment())

	return this
}

// Clone returns a cloned instance of the Asset
func (a *Any) Clone() Asset {
	return &Any{
		Labels:     a.Labels.Clone(),
		Properties: a.Properties.Clone(),
		Start:      a.Start,
		End:        a.End,
		Window:     a.Window.Clone(),
		Adjustment: a.Adjustment,
		Cost:       a.Cost,
	}
}

// Equal returns true if the given Asset is an exact match of the receiver
func (a *Any) Equal(that Asset) bool {
	t, ok := that.(*Any)
	if !ok {
		return false
	}

	if !a.Labels.Equal(t.Labels) {
		return false
	}
	if !a.Properties.Equal(t.Properties) {
		return false
	}

	if !a.Start.Equal(t.Start) {
		return false
	}
	if !a.End.Equal(t.End) {
		return false
	}
	if !a.Window.Equal(t.Window) {
		return false
	}

	if a.Cost != t.Cost {
		return false
	}

	return true
}

// String implements fmt.Stringer
func (a *Any) String() string {
	return toString(a)
}

func (a *Any) SanitizeNaN() {
	if a == nil {
		return
	}
	if math.IsNaN(a.Adjustment) {
		log.DedupedWarningf(5, "Any: Unexpected NaN found for Adjustment: labels:%v, window:%s, properties:%s", a.Labels, a.Window.String(), a.Properties.String())
		a.Adjustment = 0
	}
	if math.IsNaN(a.Cost) {
		log.DedupedWarningf(5, "Any: Unexpected NaN found for Cost: name:%v, window:%s, properties:%s", a.Labels, a.Window.String(), a.Properties.String())
		a.Cost = 0
	}
}

// Cloud describes a cloud asset
type Cloud struct {
	Labels     AssetLabels
	Properties *AssetProperties
	Start      time.Time
	End        time.Time
	Window     Window
	Adjustment float64
	Cost       float64
	Credit     float64 // Credit is a negative value representing dollars credited back to a given line-item
}

// NewCloud returns a new Cloud Asset
func NewCloud(category, providerID string, start, end time.Time, window Window) *Cloud {
	properties := &AssetProperties{
		Category:   category,
		ProviderID: providerID,
	}

	return &Cloud{
		Labels:     AssetLabels{},
		Properties: properties,
		Start:      start,
		End:        end,
		Window:     window.Clone(),
	}
}

// Type returns the AssetType
func (ca *Cloud) Type() AssetType {
	return CloudAssetType
}

// Properties returns the AssetProperties
func (ca *Cloud) GetProperties() *AssetProperties {
	return ca.Properties
}

// SetProperties sets the Asset's Properties
func (ca *Cloud) SetProperties(props *AssetProperties) {
	ca.Properties = props
}

// Labels returns the AssetLabels
func (ca *Cloud) GetLabels() AssetLabels {
	return ca.Labels
}

// SetLabels sets the Asset's labels
func (ca *Cloud) SetLabels(labels AssetLabels) {
	ca.Labels = labels
}

// Adjustment returns the Asset's adjustment value
func (ca *Cloud) GetAdjustment() float64 {
	return ca.Adjustment
}

// SetAdjustment sets the Asset's adjustment value
func (ca *Cloud) SetAdjustment(adj float64) {
	ca.Adjustment = adj
}

// TotalCost returns the Asset's total cost
func (ca *Cloud) TotalCost() float64 {
	return ca.Cost + ca.Adjustment + ca.Credit
}

// Start returns the Asset's precise start time within the window
func (ca *Cloud) GetStart() time.Time {
	return ca.Start
}

// End returns the Asset's precise end time within the window
func (ca *Cloud) GetEnd() time.Time {
	return ca.End
}

// Minutes returns the number of Minutes the Asset ran
func (ca *Cloud) Minutes() float64 {
	return ca.End.Sub(ca.Start).Minutes()
}

// Window returns the window within which the Asset ran
func (ca *Cloud) GetWindow() Window {
	return ca.Window
}

func (ca *Cloud) SetWindow(window Window) {
	ca.Window = window
}

// ExpandWindow expands the Asset's window by the given window
func (ca *Cloud) ExpandWindow(window Window) {
	ca.Window = ca.Window.Expand(window)
}

// SetStartEnd sets the Asset's Start and End fields
func (ca *Cloud) SetStartEnd(start, end time.Time) {
	if ca.Window.Contains(start) {
		ca.Start = start
	} else {
		log.Warnf("Cloud.SetStartEnd: start %s not in %s", start, ca.Window)
	}

	if ca.Window.Contains(end) {
		ca.End = end
	} else {
		log.Warnf("Cloud.SetStartEnd: end %s not in %s", end, ca.Window)
	}
}

// Add sums the Asset with the given Asset to produce a new Asset, maintaining
// as much relevant information as possible (i.e. type, Properties, labels).
func (ca *Cloud) Add(a Asset) Asset {
	// Cloud + Cloud = Cloud
	if that, ok := a.(*Cloud); ok {
		this := ca.Clone().(*Cloud)
		this.add(that)
		return this
	}

	props := ca.Properties.Merge(a.GetProperties())
	labels := ca.Labels.Merge(a.GetLabels())

	start := ca.Start
	if a.GetStart().Before(start) {
		start = a.GetStart()
	}
	end := ca.End
	if a.GetEnd().After(end) {
		end = a.GetEnd()
	}
	window := ca.Window.Expand(a.GetWindow())

	// Cloud + !Cloud = Any
	any := NewAsset(start, end, window)
	any.SetProperties(props)
	any.SetLabels(labels)
	any.Adjustment = ca.Adjustment + a.GetAdjustment()
	any.Cost = (ca.TotalCost() - ca.Adjustment) + (a.TotalCost() - a.GetAdjustment())

	return any
}

func (ca *Cloud) add(that *Cloud) {
	if ca == nil {
		ca = that
		return
	}

	props := ca.Properties.Merge(that.Properties)
	labels := ca.Labels.Merge(that.Labels)

	start := ca.Start
	if that.Start.Before(start) {
		start = that.Start
	}
	end := ca.End
	if that.End.After(end) {
		end = that.End
	}
	window := ca.Window.Expand(that.Window)

	ca.Start = start
	ca.End = end
	ca.Window = window
	ca.SetProperties(props)
	ca.SetLabels(labels)
	ca.Adjustment += that.Adjustment
	ca.Cost += that.Cost
	ca.Credit += that.Credit
}

// Clone returns a cloned instance of the Asset
func (ca *Cloud) Clone() Asset {
	return &Cloud{
		Labels:     ca.Labels.Clone(),
		Properties: ca.Properties.Clone(),
		Start:      ca.Start,
		End:        ca.End,
		Window:     ca.Window.Clone(),
		Adjustment: ca.Adjustment,
		Cost:       ca.Cost,
		Credit:     ca.Credit,
	}
}

// Equal returns true if the given Asset precisely equals the Asset
func (ca *Cloud) Equal(a Asset) bool {
	that, ok := a.(*Cloud)
	if !ok {
		return false
	}

	if !ca.Labels.Equal(that.Labels) {
		return false
	}
	if !ca.Properties.Equal(that.Properties) {
		return false
	}
	if !ca.Start.Equal(that.Start) {
		return false
	}
	if !ca.End.Equal(that.End) {
		return false
	}
	if !ca.Window.Equal(that.Window) {
		return false
	}
	if ca.Adjustment != that.Adjustment {
		return false
	}
	if ca.Cost != that.Cost {
		return false
	}
	if ca.Credit != that.Credit {
		return false
	}

	return true
}

// String implements fmt.Stringer
func (ca *Cloud) String() string {
	return toString(ca)
}

func (ca *Cloud) SanitizeNaN() {
	if ca == nil {
		return
	}
	if math.IsNaN(ca.Adjustment) {
		log.DedupedWarningf(5, "Cloud: Unexpected NaN found for Adjustment: labels:%v, window:%s, properties:%s", ca.Labels, ca.Window.String(), ca.Properties.String())
		ca.Adjustment = 0
	}
	if math.IsNaN(ca.Cost) {
		log.DedupedWarningf(5, "Cloud: Unexpected NaN found for Cost: name:%v, window:%s, properties:%s", ca.Labels, ca.Window.String(), ca.Properties.String())
		ca.Cost = 0
	}
	if math.IsNaN(ca.Credit) {
		log.DedupedWarningf(5, "Cloud: Unexpected NaN found for Credit: name:%v, window:%s, properties:%s", ca.Labels, ca.Window.String(), ca.Properties.String())
		ca.Credit = 0
	}
}

// ClusterManagement describes a provider's cluster management fee
type ClusterManagement struct {
	Labels     AssetLabels
	Properties *AssetProperties
	Window     Window
	Cost       float64
	Adjustment float64 // @bingen:field[version=16]
}

// NewClusterManagement creates and returns a new ClusterManagement instance
func NewClusterManagement(provider, cluster string, window Window) *ClusterManagement {
	properties := &AssetProperties{
		Category: ManagementCategory,
		Provider: ParseProvider(provider),
		Cluster:  cluster,
		Service:  KubernetesService,
	}

	return &ClusterManagement{
		Labels:     AssetLabels{},
		Properties: properties,
		Window:     window.Clone(),
	}
}

// Type returns the Asset's type
func (cm *ClusterManagement) Type() AssetType {
	return ClusterManagementAssetType
}

// Properties returns the Asset's Properties
func (cm *ClusterManagement) GetProperties() *AssetProperties {
	return cm.Properties
}

// SetProperties sets the Asset's Properties
func (cm *ClusterManagement) SetProperties(props *AssetProperties) {
	cm.Properties = props
}

// Labels returns the Asset's labels
func (cm *ClusterManagement) GetLabels() AssetLabels {
	return cm.Labels
}

// SetLabels sets the Asset's Properties
func (cm *ClusterManagement) SetLabels(props AssetLabels) {
	cm.Labels = props
}

// Adjustment does not apply to ClusterManagement
func (cm *ClusterManagement) GetAdjustment() float64 {
	return cm.Adjustment
}

// SetAdjustment does not apply to ClusterManagement
func (cm *ClusterManagement) SetAdjustment(adj float64) {
	cm.Adjustment = adj
}

// TotalCost returns the Asset's total cost
func (cm *ClusterManagement) TotalCost() float64 {
	return cm.Cost + cm.Adjustment
}

// Start returns the Asset's precise start time within the window
func (cm *ClusterManagement) GetStart() time.Time {
	return *cm.Window.Start()
}

// End returns the Asset's precise end time within the window
func (cm *ClusterManagement) GetEnd() time.Time {
	return *cm.Window.End()
}

// Minutes returns the number of minutes the Asset ran
func (cm *ClusterManagement) Minutes() float64 {
	return cm.Window.Minutes()
}

// Window return the Asset's window
func (cm *ClusterManagement) GetWindow() Window {
	return cm.Window
}

func (cm *ClusterManagement) SetWindow(window Window) {
	cm.Window = window
}

// ExpandWindow expands the Asset's window by the given window
func (cm *ClusterManagement) ExpandWindow(window Window) {
	cm.Window = cm.Window.Expand(window)
}

// SetStartEnd sets the Asset's Start and End fields (not applicable here)
func (cm *ClusterManagement) SetStartEnd(start, end time.Time) {
	return
}

// Add sums the Asset with the given Asset to produce a new Asset, maintaining
// as much relevant information as possible (i.e. type, Properties, labels).
func (cm *ClusterManagement) Add(a Asset) Asset {
	// ClusterManagement + ClusterManagement = ClusterManagement
	if that, ok := a.(*ClusterManagement); ok {
		this := cm.Clone().(*ClusterManagement)
		this.add(that)
		return this
	}

	props := cm.Properties.Merge(a.GetProperties())
	labels := cm.Labels.Merge(a.GetLabels())

	start := cm.GetStart()
	if a.GetStart().Before(start) {
		start = a.GetStart()
	}
	end := cm.GetEnd()
	if a.GetEnd().After(end) {
		end = a.GetEnd()
	}
	window := cm.Window.Expand(a.GetWindow())

	// ClusterManagement + !ClusterManagement = Any
	any := NewAsset(start, end, window)
	any.SetProperties(props)
	any.SetLabels(labels)
	any.Adjustment = cm.Adjustment + a.GetAdjustment()
	any.Cost = (cm.TotalCost() - cm.Adjustment) + (a.TotalCost() - a.GetAdjustment())

	return any
}

func (cm *ClusterManagement) add(that *ClusterManagement) {
	if cm == nil {
		cm = that
		return
	}

	props := cm.Properties.Merge(that.Properties)
	labels := cm.Labels.Merge(that.Labels)
	window := cm.Window.Expand(that.Window)

	cm.Window = window
	cm.SetProperties(props)
	cm.SetLabels(labels)
	cm.Adjustment += that.Adjustment
	cm.Cost += that.Cost
}

// Clone returns a cloned instance of the Asset
func (cm *ClusterManagement) Clone() Asset {
	return &ClusterManagement{
		Labels:     cm.Labels.Clone(),
		Properties: cm.Properties.Clone(),
		Window:     cm.Window.Clone(),
		Adjustment: cm.Adjustment,
		Cost:       cm.Cost,
	}
}

// Equal returns true if the given Asset exactly matches the Asset
func (cm *ClusterManagement) Equal(a Asset) bool {
	that, ok := a.(*ClusterManagement)
	if !ok {
		return false
	}

	if !cm.Labels.Equal(that.Labels) {
		return false
	}
	if !cm.Properties.Equal(that.Properties) {
		return false
	}

	if !cm.Window.Equal(that.Window) {
		return false
	}

	if cm.Adjustment != that.Adjustment {
		return false
	}

	if cm.Cost != that.Cost {
		return false
	}

	return true
}

// String implements fmt.Stringer
func (cm *ClusterManagement) String() string {
	return toString(cm)
}

func (cm *ClusterManagement) SanitizeNaN() {
	if cm == nil {
		return
	}
	if math.IsNaN(cm.Adjustment) {
		log.DedupedWarningf(5, "ClusterManagement: Unexpected NaN found for Adjustment: labels:%v, window:%s, properties:%s", cm.Labels, cm.Window.String(), cm.Properties.String())
		cm.Adjustment = 0
	}
	if math.IsNaN(cm.Cost) {
		log.DedupedWarningf(5, "ClusterManagement: Unexpected NaN found for Cost: name:%v, window:%s, properties:%s", cm.Labels, cm.Window.String(), cm.Properties.String())
		cm.Cost = 0
	}
}

// Disk represents an in-cluster disk Asset
type Disk struct {
	Labels         AssetLabels
	Properties     *AssetProperties
	Start          time.Time
	End            time.Time
	Window         Window
	Adjustment     float64
	Cost           float64
	ByteHours      float64
	Local          float64
	Breakdown      *Breakdown
	StorageClass   string   // @bingen:field[version=17]
	ByteHoursUsed  *float64 // @bingen:field[version=18]
	ByteUsageMax   *float64 // @bingen:field[version=18]
	VolumeName     string   // @bingen:field[version=18]
	ClaimName      string   // @bingen:field[version=18]
	ClaimNamespace string   // @bingen:field[version=18]
}

// NewDisk creates and returns a new Disk Asset
func NewDisk(name, cluster, providerID string, start, end time.Time, window Window) *Disk {
	properties := &AssetProperties{
		Category:   StorageCategory,
		Name:       name,
		Cluster:    cluster,
		ProviderID: providerID,
		Service:    KubernetesService,
	}

	return &Disk{
		Labels:     AssetLabels{},
		Properties: properties,
		Start:      start,
		End:        end,
		Window:     window,
		Breakdown:  &Breakdown{},
	}
}

// Type returns the AssetType of the Asset
func (d *Disk) Type() AssetType {
	return DiskAssetType
}

// Properties returns the Asset's Properties
func (d *Disk) GetProperties() *AssetProperties {
	return d.Properties
}

// SetProperties sets the Asset's Properties
func (d *Disk) SetProperties(props *AssetProperties) {
	d.Properties = props
}

// Labels returns the Asset's labels
func (d *Disk) GetLabels() AssetLabels {
	return d.Labels
}

// SetLabels sets the Asset's labels
func (d *Disk) SetLabels(labels AssetLabels) {
	d.Labels = labels
}

// Adjustment returns the Asset's cost adjustment
func (d *Disk) GetAdjustment() float64 {
	return d.Adjustment
}

// SetAdjustment sets the Asset's cost adjustment
func (d *Disk) SetAdjustment(adj float64) {
	d.Adjustment = adj
}

// TotalCost returns the Asset's total cost
func (d *Disk) TotalCost() float64 {
	return d.Cost + d.Adjustment
}

// Start returns the precise start time of the Asset within the window
func (d *Disk) GetStart() time.Time {
	return d.Start
}

// End returns the precise start time of the Asset within the window
func (d *Disk) GetEnd() time.Time {
	return d.End
}

// Minutes returns the number of minutes the Asset ran
func (d *Disk) Minutes() float64 {
	diskMins := d.End.Sub(d.Start).Minutes()
	windowMins := d.Window.Minutes()

	if diskMins > windowMins {
		log.Warnf("Asset ETL: Disk.Minutes exceeds window: %.2f > %.2f", diskMins, windowMins)
		diskMins = windowMins
	}

	if diskMins < 0 {
		diskMins = 0
	}

	return diskMins
}

// Window returns the window within which the Asset
func (d *Disk) GetWindow() Window {
	return d.Window
}

func (d *Disk) SetWindow(window Window) {
	d.Window = window
}

// ExpandWindow expands the Asset's window by the given window
func (d *Disk) ExpandWindow(window Window) {
	d.Window = d.Window.Expand(window)
}

// SetStartEnd sets the Asset's Start and End fields
func (d *Disk) SetStartEnd(start, end time.Time) {
	if d.Window.Contains(start) {
		d.Start = start
	} else {
		log.Warnf("Disk.SetStartEnd: start %s not in %s", start, d.Window)
	}

	if d.Window.Contains(end) {
		d.End = end
	} else {
		log.Warnf("Disk.SetStartEnd: end %s not in %s", end, d.Window)
	}
}

// Add sums the Asset with the given Asset to produce a new Asset, maintaining
// as much relevant information as possible (i.e. type, Properties, labels).
func (d *Disk) Add(a Asset) Asset {
	// Disk + Disk = Disk
	if that, ok := a.(*Disk); ok {
		this := d.Clone().(*Disk)
		this.add(that)
		return this
	}

	props := d.Properties.Merge(a.GetProperties())
	labels := d.Labels.Merge(a.GetLabels())

	start := d.Start
	if a.GetStart().Before(start) {
		start = a.GetStart()
	}
	end := d.End
	if a.GetEnd().After(end) {
		end = a.GetEnd()
	}
	window := d.Window.Expand(a.GetWindow())

	// Disk + !Disk = Any
	any := NewAsset(start, end, window)
	any.SetProperties(props)
	any.SetLabels(labels)
	any.Adjustment = d.Adjustment + a.GetAdjustment()
	any.Cost = (d.TotalCost() - d.Adjustment) + (a.TotalCost() - a.GetAdjustment())

	return any
}

func (d *Disk) add(that *Disk) {
	if d == nil {
		d = that
		return
	}

	props := d.Properties.Merge(that.Properties)
	labels := d.Labels.Merge(that.Labels)
	d.SetProperties(props)
	d.SetLabels(labels)

	start := d.Start
	if that.Start.Before(start) {
		start = that.Start
	}
	end := d.End
	if that.End.After(end) {
		end = that.End
	}
	window := d.Window.Expand(that.Window)
	d.Start = start
	d.End = end
	d.Window = window

	totalCost := d.Cost + that.Cost
	if totalCost > 0.0 {
		d.Breakdown.Idle = (d.Breakdown.Idle*d.Cost + that.Breakdown.Idle*that.Cost) / totalCost
		d.Breakdown.Other = (d.Breakdown.Other*d.Cost + that.Breakdown.Other*that.Cost) / totalCost
		d.Breakdown.System = (d.Breakdown.System*d.Cost + that.Breakdown.System*that.Cost) / totalCost
		d.Breakdown.User = (d.Breakdown.User*d.Cost + that.Breakdown.User*that.Cost) / totalCost

		d.Local = (d.TotalCost()*d.Local + that.TotalCost()*that.Local) / (d.TotalCost() + that.TotalCost())
	} else {
		d.Local = (d.Local + that.Local) / 2.0
	}

	d.Adjustment += that.Adjustment
	d.Cost += that.Cost

	d.ByteHours += that.ByteHours

	if d.ByteHoursUsed == nil && that.ByteHoursUsed != nil {
		copy := *that.ByteHoursUsed
		d.ByteHoursUsed = &copy
	} else if d.ByteHoursUsed != nil && that.ByteHoursUsed == nil {
		// do nothing
	} else if d.ByteHoursUsed != nil && that.ByteHoursUsed != nil {
		sum := *d.ByteHoursUsed
		sum += *that.ByteHoursUsed
		d.ByteHoursUsed = &sum
	}

	// We have to nil out the max because we don't know if we're
	// aggregating across time our properties. See RawAllocationOnly on
	// Allocation for further reference.
	d.ByteUsageMax = nil

	// If storage class don't match default it to empty storage class
	if d.StorageClass != that.StorageClass {
		d.StorageClass = ""
	}

	if d.VolumeName != that.VolumeName {
		d.VolumeName = ""
	}
	if d.ClaimName != that.ClaimName {
		d.ClaimName = ""
	}
	if d.ClaimNamespace != that.ClaimNamespace {
		d.ClaimNamespace = ""
	}
}

// Clone returns a cloned instance of the Asset
func (d *Disk) Clone() Asset {
	var max *float64
	if d.ByteUsageMax != nil {
		copied := *d.ByteUsageMax
		max = &copied
	}
	var byteHoursUsed *float64
	if d.ByteHoursUsed != nil {
		copied := *d.ByteHoursUsed
		byteHoursUsed = &copied
	}

	return &Disk{
		Properties:     d.Properties.Clone(),
		Labels:         d.Labels.Clone(),
		Start:          d.Start,
		End:            d.End,
		Window:         d.Window.Clone(),
		Adjustment:     d.Adjustment,
		Cost:           d.Cost,
		ByteHours:      d.ByteHours,
		ByteHoursUsed:  byteHoursUsed,
		ByteUsageMax:   max,
		Local:          d.Local,
		Breakdown:      d.Breakdown.Clone(),
		StorageClass:   d.StorageClass,
		VolumeName:     d.VolumeName,
		ClaimName:      d.ClaimName,
		ClaimNamespace: d.ClaimNamespace,
	}
}

// Equal returns true if the two Assets match exactly
func (d *Disk) Equal(a Asset) bool {
	that, ok := a.(*Disk)
	if !ok {
		return false
	}

	if !d.Labels.Equal(that.Labels) {
		return false
	}
	if !d.Properties.Equal(that.Properties) {
		return false
	}
	if !d.Start.Equal(that.Start) {
		return false
	}
	if !d.End.Equal(that.End) {
		return false
	}
	if !d.Window.Equal(that.Window) {
		return false
	}
	if d.Adjustment != that.Adjustment {
		return false
	}
	if d.Cost != that.Cost {
		return false
	}
	if d.ByteHours != that.ByteHours {
		return false
	}
	if d.ByteHoursUsed != nil && that.ByteHoursUsed == nil {
		return false
	}
	if d.ByteHoursUsed == nil && that.ByteHoursUsed != nil {
		return false
	}
	if (d.ByteHoursUsed != nil && that.ByteHoursUsed != nil) && *d.ByteHoursUsed != *that.ByteHoursUsed {
		return false
	}
	if d.ByteUsageMax != nil && that.ByteUsageMax == nil {
		return false
	}
	if d.ByteUsageMax == nil && that.ByteUsageMax != nil {
		return false
	}
	if (d.ByteUsageMax != nil && that.ByteUsageMax != nil) && *d.ByteUsageMax != *that.ByteUsageMax {
		return false
	}
	if d.Local != that.Local {
		return false
	}
	if !d.Breakdown.Equal(that.Breakdown) {
		return false
	}
	if d.StorageClass != that.StorageClass {
		return false
	}
	if d.VolumeName != that.VolumeName {
		return false
	}
	if d.ClaimName != that.ClaimName {
		return false
	}
	if d.ClaimNamespace != that.ClaimNamespace {
		return false
	}

	return true
}

// String implements fmt.Stringer
func (d *Disk) String() string {
	return toString(d)
}

// Bytes returns the number of bytes belonging to the disk. This could be
// fractional because it's the number of byte*hours divided by the number of
// hours running; e.g. the sum of a 100GiB disk running for the first 10 hours
// and a 30GiB disk running for the last 20 hours of the same 24-hour window
// would produce:
//
//	(100*10 + 30*20) / 24 = 66.667GiB
//
// However, any number of disks running for the full span of a window will
// report the actual number of bytes of the static disk; e.g. the above
// scenario for one entire 24-hour window:
//
//	(100*24 + 30*24) / 24 = (100 + 30) = 130GiB
func (d *Disk) Bytes() float64 {
	// [b*hr]*([min/hr]*[1/min]) = [b*hr]/[hr] = b
	return d.ByteHours * (60.0 / d.Minutes())
}

func (d *Disk) SanitizeNaN() {
	if d == nil {
		return
	}
	if math.IsNaN(d.Adjustment) {
		log.DedupedWarningf(5, "Disk: Unexpected NaN found for Adjustment: labels:%v, window:%s, properties:%s", d.Labels, d.Window.String(), d.Properties.String())
		d.Adjustment = 0
	}
	if math.IsNaN(d.Cost) {
		log.DedupedWarningf(5, "Disk: Unexpected NaN found for Cost: labels:%v, window:%s, properties:%s", d.Labels, d.Window.String(), d.Properties.String())
		d.Cost = 0
	}
	if math.IsNaN(d.ByteHours) {
		log.DedupedWarningf(5, "Disk: Unexpected NaN found for ByteHours: labels:%v, window:%s, properties:%s", d.Labels, d.Window.String(), d.Properties.String())
		d.ByteHours = 0
	}
	if math.IsNaN(d.Local) {
		log.DedupedWarningf(5, "Disk: Unexpected NaN found for Local: labels:%v, window:%s, properties:%s", d.Labels, d.Window.String(), d.Properties.String())
		d.Local = 0
	}
	if d.ByteHoursUsed != nil && math.IsNaN(*d.ByteHoursUsed) {
		log.DedupedWarningf(5, "Disk: Unexpected NaN found for ByteHoursUsed: labels:%v, window:%s, properties:%s", d.Labels, d.Window.String(), d.Properties.String())
		f := 0.0
		d.ByteHoursUsed = &f
	}
	if d.ByteUsageMax != nil && math.IsNaN(*d.ByteUsageMax) {
		log.DedupedWarningf(5, "Disk: Unexpected NaN found for ByteUsageMax: labels:%v, window:%s, properties:%s", d.Labels, d.Window.String(), d.Properties.String())
		f := 0.0
		d.ByteUsageMax = &f
	}

	d.Breakdown.SanitizeNaN()
}

// Breakdown describes a resource's use as a percentage of various usage types
type Breakdown struct {
	Idle   float64 `json:"idle"`
	Other  float64 `json:"other"`
	System float64 `json:"system"`
	User   float64 `json:"user"`
}

func (b *Breakdown) SanitizeNaN() {
	if math.IsNaN(b.Idle) {
		log.DedupedWarningf(5, "Breakdown: Unexpected NaN found for Idle")
		b.Idle = 0
	}
	if math.IsNaN(b.Other) {
		log.DedupedWarningf(5, "Breakdown: Unexpected NaN found for Other")
		b.Other = 0
	}
	if math.IsNaN(b.System) {
		log.DedupedWarningf(5, "Breakdown: Unexpected NaN found for System")
		b.System = 0
	}
	if math.IsNaN(b.User) {
		log.DedupedWarningf(5, "Breakdown: Unexpected NaN found for User")
		b.User = 0
	}
}

// Clone returns a cloned instance of the Breakdown
func (b *Breakdown) Clone() *Breakdown {
	if b == nil {
		return nil
	}

	return &Breakdown{
		Idle:   b.Idle,
		Other:  b.Other,
		System: b.System,
		User:   b.User,
	}
}

// Equal returns true if the two Breakdowns are exact matches
func (b *Breakdown) Equal(that *Breakdown) bool {
	if b == nil && that == nil {
		return true
	}

	if b == nil || that == nil {
		return false
	}

	if b.Idle != that.Idle {
		return false
	}
	if b.Other != that.Other {
		return false
	}
	if b.System != that.System {
		return false
	}
	if b.User != that.User {
		return false
	}

	return true
}

// Network is an Asset representing a single node's network costs
type Network struct {
	Properties *AssetProperties
	Labels     AssetLabels
	Start      time.Time
	End        time.Time
	Window     Window
	Adjustment float64
	Cost       float64
}

// NewNetwork creates and returns a new Network Asset
func NewNetwork(name, cluster, providerID string, start, end time.Time, window Window) *Network {
	properties := &AssetProperties{
		Category:   NetworkCategory,
		Name:       name,
		Cluster:    cluster,
		ProviderID: providerID,
		Service:    KubernetesService,
	}

	return &Network{
		Properties: properties,
		Labels:     AssetLabels{},
		Start:      start,
		End:        end,
		Window:     window.Clone(),
	}
}

// Type returns the AssetType of the Asset
func (n *Network) Type() AssetType {
	return NetworkAssetType
}

// Properties returns the Asset's Properties
func (n *Network) GetProperties() *AssetProperties {
	return n.Properties
}

// SetProperties sets the Asset's Properties
func (n *Network) SetProperties(props *AssetProperties) {
	n.Properties = props
}

// Labels returns the Asset's labels
func (n *Network) GetLabels() AssetLabels {
	return n.Labels
}

// SetLabels sets the Asset's labels
func (n *Network) SetLabels(labels AssetLabels) {
	n.Labels = labels
}

// Adjustment returns the Asset's cost adjustment
func (n *Network) GetAdjustment() float64 {
	return n.Adjustment
}

// SetAdjustment sets the Asset's cost adjustment
func (n *Network) SetAdjustment(adj float64) {
	n.Adjustment = adj
}

// TotalCost returns the Asset's total cost
func (n *Network) TotalCost() float64 {
	return n.Cost + n.Adjustment
}

// Start returns the precise start time of the Asset within the window
func (n *Network) GetStart() time.Time {
	return n.Start
}

// End returns the precise end time of the Asset within the window
func (n *Network) GetEnd() time.Time {
	return n.End
}

// Minutes returns the number of minutes the Asset ran within the window
func (n *Network) Minutes() float64 {
	netMins := n.End.Sub(n.Start).Minutes()
	windowMins := n.Window.Minutes()

	if netMins > windowMins {
		log.Warnf("Asset ETL: Network.Minutes exceeds window: %.2f > %.2f", netMins, windowMins)
		netMins = windowMins
	}

	if netMins < 0 {
		netMins = 0
	}

	return netMins
}

// Window returns the window within which the Asset ran
func (n *Network) GetWindow() Window {
	return n.Window
}

func (n *Network) SetWindow(window Window) {
	n.Window = window
}

// ExpandWindow expands the Asset's window by the given window
func (n *Network) ExpandWindow(window Window) {
	n.Window = n.Window.Expand(window)
}

// SetStartEnd sets the Asset's Start and End fields
func (n *Network) SetStartEnd(start, end time.Time) {
	if n.Window.Contains(start) {
		n.Start = start
	} else {
		log.Warnf("Disk.SetStartEnd: start %s not in %s", start, n.Window)
	}

	if n.Window.Contains(end) {
		n.End = end
	} else {
		log.Warnf("Disk.SetStartEnd: end %s not in %s", end, n.Window)
	}
}

// Add sums the Asset with the given Asset to produce a new Asset, maintaining
// as much relevant information as possible (i.e. type, Properties, labels).
func (n *Network) Add(a Asset) Asset {
	// Network + Network = Network
	if that, ok := a.(*Network); ok {
		this := n.Clone().(*Network)
		this.add(that)
		return this
	}

	props := n.Properties.Merge(a.GetProperties())
	labels := n.Labels.Merge(a.GetLabels())

	start := n.Start
	if a.GetStart().Before(start) {
		start = a.GetStart()
	}
	end := n.End
	if a.GetEnd().After(end) {
		end = a.GetEnd()
	}
	window := n.Window.Expand(a.GetWindow())

	// Network + !Network = Any
	any := NewAsset(start, end, window)
	any.SetProperties(props)
	any.SetLabels(labels)
	any.Adjustment = n.Adjustment + a.GetAdjustment()
	any.Cost = (n.TotalCost() - n.Adjustment) + (a.TotalCost() - a.GetAdjustment())

	return any
}

func (n *Network) add(that *Network) {
	if n == nil {
		n = that
		return
	}

	props := n.Properties.Merge(that.Properties)
	labels := n.Labels.Merge(that.Labels)
	n.SetProperties(props)
	n.SetLabels(labels)

	start := n.Start
	if that.Start.Before(start) {
		start = that.Start
	}
	end := n.End
	if that.End.After(end) {
		end = that.End
	}
	window := n.Window.Expand(that.Window)
	n.Start = start
	n.End = end
	n.Window = window

	n.Cost += that.Cost
	n.Adjustment += that.Adjustment
}

// Clone returns a deep copy of the given Network
func (n *Network) Clone() Asset {
	if n == nil {
		return nil
	}

	return &Network{
		Properties: n.Properties.Clone(),
		Labels:     n.Labels.Clone(),
		Start:      n.Start,
		End:        n.End,
		Window:     n.Window.Clone(),
		Adjustment: n.Adjustment,
		Cost:       n.Cost,
	}
}

// Equal returns true if the tow Assets match exactly
func (n *Network) Equal(a Asset) bool {
	that, ok := a.(*Network)
	if !ok {
		return false
	}

	if !n.Labels.Equal(that.Labels) {
		return false
	}
	if !n.Properties.Equal(that.Properties) {
		return false
	}

	if !n.Start.Equal(that.Start) {
		return false
	}
	if !n.End.Equal(that.End) {
		return false
	}
	if !n.Window.Equal(that.Window) {
		return false
	}

	if n.Adjustment != that.Adjustment {
		return false
	}
	if n.Cost != that.Cost {
		return false
	}

	return true
}

// String implements fmt.Stringer
func (n *Network) String() string {
	return toString(n)
}

func (n *Network) SanitizeNaN() {
	if n == nil {
		return
	}
	if math.IsNaN(n.Adjustment) {
		log.DedupedWarningf(5, "Network: Unexpected NaN found for Adjustment: labels:%v, window:%s, properties:%s", n.Labels, n.Window.String(), n.Properties.String())
		n.Adjustment = 0
	}
	if math.IsNaN(n.Cost) {
		log.DedupedWarningf(5, "Network: Unexpected NaN found for Cost: labels:%v, window:%s, properties:%s", n.Labels, n.Window.String(), n.Properties.String())
		n.Cost = 0
	}
}

// NodeOverhead represents the delta between the allocatable resources
// of the node and the node nameplate capacity
type NodeOverhead struct {
	CpuOverheadFraction  float64
	RamOverheadFraction  float64
	OverheadCostFraction float64
}

func (n *NodeOverhead) SanitizeNaN() {
	if math.IsNaN(n.CpuOverheadFraction) {
		log.DedupedWarningf(5, "NodeOverhead: Unexpected NaN found for CpuOverheadFraction")
		n.CpuOverheadFraction = 0
	}
	if math.IsNaN(n.RamOverheadFraction) {
		log.DedupedWarningf(5, "NodeOverhead: Unexpected NaN found for RamOverheadFraction")
		n.RamOverheadFraction = 0
	}
	if math.IsNaN(n.OverheadCostFraction) {
		log.DedupedWarningf(5, "NodeOverhead: Unexpected NaN found for OverheadCostFraction")
		n.OverheadCostFraction = 0
	}
}

func (n *NodeOverhead) Equal(other *NodeOverhead) bool {
	if n == nil && other != nil {
		return false
	}
	if n != nil && other == nil {
		return false
	}
	if n == nil && other == nil {
		return true
	}

	// This is okay because everything in NodeOverhead is a value type.
	return *n == *other
}

func (n *NodeOverhead) Clone() *NodeOverhead {
	if n == nil {
		return nil
	}
	return &NodeOverhead{
		CpuOverheadFraction:  n.CpuOverheadFraction,
		RamOverheadFraction:  n.RamOverheadFraction,
		OverheadCostFraction: n.OverheadCostFraction,
	}
}

// Node is an Asset representing a single node in a cluster
type Node struct {
	Properties   *AssetProperties
	Labels       AssetLabels
	Start        time.Time
	End          time.Time
	Window       Window
	Adjustment   float64
	NodeType     string
	CPUCoreHours float64
	RAMByteHours float64
	GPUHours     float64
	CPUBreakdown *Breakdown
	RAMBreakdown *Breakdown
	CPUCost      float64
	GPUCost      float64
	GPUCount     float64
	RAMCost      float64
	Discount     float64
	Preemptible  float64
	Overhead     *NodeOverhead // @bingen:field[version=19]
}

// NewNode creates and returns a new Node Asset
func NewNode(name, cluster, providerID string, start, end time.Time, window Window) *Node {
	properties := &AssetProperties{
		Category:   ComputeCategory,
		Name:       name,
		Cluster:    cluster,
		ProviderID: providerID,
		Service:    KubernetesService,
	}

	return &Node{
		Properties:   properties,
		Labels:       AssetLabels{},
		Start:        start,
		End:          end,
		Window:       window.Clone(),
		CPUBreakdown: &Breakdown{},
		RAMBreakdown: &Breakdown{},
	}
}

// Type returns the AssetType of the Asset
func (n *Node) Type() AssetType {
	return NodeAssetType
}

// Properties returns the Asset's Properties
func (n *Node) GetProperties() *AssetProperties {
	return n.Properties
}

// SetProperties sets the Asset's Properties
func (n *Node) SetProperties(props *AssetProperties) {
	n.Properties = props
}

// Labels returns the Asset's labels
func (n *Node) GetLabels() AssetLabels {
	return n.Labels
}

// SetLabels sets the Asset's labels
func (n *Node) SetLabels(labels AssetLabels) {
	n.Labels = labels
}

// Adjustment returns the Asset's cost adjustment
func (n *Node) GetAdjustment() float64 {
	return n.Adjustment
}

// SetAdjustment sets the Asset's cost adjustment
func (n *Node) SetAdjustment(adj float64) {
	n.Adjustment = adj
}

// TotalCost returns the Asset's total cost
func (n *Node) TotalCost() float64 {
	return ((n.CPUCost + n.RAMCost) * (1.0 - n.Discount)) + n.GPUCost + n.Adjustment
}

// Start returns the precise start time of the Asset within the window
func (n *Node) GetStart() time.Time {
	return n.Start
}

// End returns the precise end time of the Asset within the window
func (n *Node) GetEnd() time.Time {
	return n.End
}

// Minutes returns the number of minutes the Asset ran within the window
func (n *Node) Minutes() float64 {
	nodeMins := n.End.Sub(n.Start).Minutes()
	windowMins := n.Window.Minutes()

	if nodeMins > windowMins {
		log.Warnf("Asset ETL: Node.Minutes exceeds window: %.2f > %.2f", nodeMins, windowMins)
		nodeMins = windowMins
	}

	if nodeMins < 0 {
		nodeMins = 0
	}

	return nodeMins
}

// Window returns the window within which the Asset ran
func (n *Node) GetWindow() Window {
	return n.Window
}

func (n *Node) SetWindow(window Window) {
	n.Window = window
}

// ExpandWindow expands the Asset's window by the given window
func (n *Node) ExpandWindow(window Window) {
	n.Window = n.Window.Expand(window)
}

// SetStartEnd sets the Asset's Start and End fields
func (n *Node) SetStartEnd(start, end time.Time) {
	if n.Window.Contains(start) {
		n.Start = start
	} else {
		log.Warnf("Disk.SetStartEnd: start %s not in %s", start, n.Window)
	}

	if n.Window.Contains(end) {
		n.End = end
	} else {
		log.Warnf("Disk.SetStartEnd: end %s not in %s", end, n.Window)
	}
}

// Add sums the Asset with the given Asset to produce a new Asset, maintaining
// as much relevant information as possible (i.e. type, Properties, labels).
func (n *Node) Add(a Asset) Asset {
	// Node + Node = Node
	if that, ok := a.(*Node); ok {
		this := n.Clone().(*Node)
		this.add(that)
		return this
	}

	props := n.Properties.Merge(a.GetProperties())
	labels := n.Labels.Merge(a.GetLabels())

	start := n.Start
	if a.GetStart().Before(start) {
		start = a.GetStart()
	}
	end := n.End
	if a.GetEnd().After(end) {
		end = a.GetEnd()
	}
	window := n.Window.Expand(a.GetWindow())

	// Node + !Node = Any
	any := NewAsset(start, end, window)
	any.SetProperties(props)
	any.SetLabels(labels)
	any.Adjustment = n.Adjustment + a.GetAdjustment()
	any.Cost = (n.TotalCost() - n.Adjustment) + (a.TotalCost() - a.GetAdjustment())

	return any
}

func (n *Node) add(that *Node) {
	if n == nil {
		n = that
		return
	}

	props := n.Properties.Merge(that.Properties)
	labels := n.Labels.Merge(that.Labels)
	n.SetProperties(props)
	n.SetLabels(labels)

	if n.NodeType != that.NodeType {
		n.NodeType = ""
	}

	start := n.Start
	if that.Start.Before(start) {
		start = that.Start
	}
	end := n.End
	if that.End.After(end) {
		end = that.End
	}
	window := n.Window.Expand(that.Window)
	n.Start = start
	n.End = end
	n.Window = window

	// Order of operations for node costs is:
	//   Discount(CPU + RAM) + GPU + Adjustment

	// Combining discounts, then involves weighting each discount by each
	// respective (CPU + RAM) cost. Combining preemptible, on the other
	// hand, is done with all three (but not Adjustment, which can change
	// without triggering a re-computation of Preemtible).

	disc := (n.CPUCost+n.RAMCost)*(1.0-n.Discount) + (that.CPUCost+that.RAMCost)*(1.0-that.Discount)
	nonDisc := (n.CPUCost + n.RAMCost) + (that.CPUCost + that.RAMCost)
	if nonDisc > 0 {
		n.Discount = 1.0 - (disc / nonDisc)
	} else {
		n.Discount = (n.Discount + that.Discount) / 2.0
	}

	nNoAdj := n.TotalCost() - n.Adjustment
	thatNoAdj := that.TotalCost() - that.Adjustment
	if (nNoAdj + thatNoAdj) > 0 {
		n.Preemptible = (nNoAdj*n.Preemptible + thatNoAdj*that.Preemptible) / (nNoAdj + thatNoAdj)
	} else {
		n.Preemptible = (n.Preemptible + that.Preemptible) / 2.0
	}

	totalCPUCost := n.CPUCost + that.CPUCost
	if totalCPUCost > 0.0 {
		n.CPUBreakdown.Idle = (n.CPUBreakdown.Idle*n.CPUCost + that.CPUBreakdown.Idle*that.CPUCost) / totalCPUCost
		n.CPUBreakdown.Other = (n.CPUBreakdown.Other*n.CPUCost + that.CPUBreakdown.Other*that.CPUCost) / totalCPUCost
		n.CPUBreakdown.System = (n.CPUBreakdown.System*n.CPUCost + that.CPUBreakdown.System*that.CPUCost) / totalCPUCost
		n.CPUBreakdown.User = (n.CPUBreakdown.User*n.CPUCost + that.CPUBreakdown.User*that.CPUCost) / totalCPUCost
	}

	totalRAMCost := n.RAMCost + that.RAMCost
	if totalRAMCost > 0.0 {
		n.RAMBreakdown.Idle = (n.RAMBreakdown.Idle*n.RAMCost + that.RAMBreakdown.Idle*that.RAMCost) / totalRAMCost
		n.RAMBreakdown.Other = (n.RAMBreakdown.Other*n.RAMCost + that.RAMBreakdown.Other*that.RAMCost) / totalRAMCost
		n.RAMBreakdown.System = (n.RAMBreakdown.System*n.RAMCost + that.RAMBreakdown.System*that.RAMCost) / totalRAMCost
		n.RAMBreakdown.User = (n.RAMBreakdown.User*n.RAMCost + that.RAMBreakdown.User*that.RAMCost) / totalRAMCost
	}

	// These calculations have to happen before the mutable fields of n they
	// depend on (cpu cost, ram cost) are mutated with post-add totals.
	if n.Overhead != nil && that.Overhead != nil {
		n.Overhead.RamOverheadFraction = (n.Overhead.RamOverheadFraction*n.RAMCost + that.Overhead.RamOverheadFraction*that.RAMCost) / totalRAMCost
		n.Overhead.CpuOverheadFraction = (n.Overhead.CpuOverheadFraction*n.CPUCost + that.Overhead.CpuOverheadFraction*that.CPUCost) / totalCPUCost
	} else {
		n.Overhead = nil
	}

	n.CPUCoreHours += that.CPUCoreHours
	n.RAMByteHours += that.RAMByteHours
	n.GPUHours += that.GPUHours

	n.CPUCost += that.CPUCost
	n.GPUCost += that.GPUCost
	n.RAMCost += that.RAMCost
	n.Adjustment += that.Adjustment

	// The cost-weighted overhead is calculated after the node is totaled
	// because the cost-weighted overhead is based on post-add data.
	if n.Overhead != nil {
		n.Overhead.OverheadCostFraction = ((n.Overhead.CpuOverheadFraction * n.CPUCost) + (n.Overhead.RamOverheadFraction * n.RAMCost)) / n.TotalCost()
	}
}

// Clone returns a deep copy of the given Node
func (n *Node) Clone() Asset {
	if n == nil {
		return nil
	}

	return &Node{
		Properties:   n.Properties.Clone(),
		Labels:       n.Labels.Clone(),
		Start:        n.Start,
		End:          n.End,
		Window:       n.Window.Clone(),
		Adjustment:   n.Adjustment,
		NodeType:     n.NodeType,
		CPUCoreHours: n.CPUCoreHours,
		RAMByteHours: n.RAMByteHours,
		GPUHours:     n.GPUHours,
		CPUBreakdown: n.CPUBreakdown.Clone(),
		RAMBreakdown: n.RAMBreakdown.Clone(),
		CPUCost:      n.CPUCost,
		GPUCost:      n.GPUCost,
		GPUCount:     n.GPUCount,
		RAMCost:      n.RAMCost,
		Preemptible:  n.Preemptible,
		Overhead:     n.Overhead.Clone(),
		Discount:     n.Discount,
	}
}

// Equal returns true if the tow Assets match exactly
func (n *Node) Equal(a Asset) bool {
	that, ok := a.(*Node)
	if !ok {
		return false
	}

	if !n.Labels.Equal(that.Labels) {
		return false
	}
	if !n.Properties.Equal(that.Properties) {
		return false
	}
	if !n.Start.Equal(that.Start) {
		return false
	}
	if !n.End.Equal(that.End) {
		return false
	}
	if !n.Window.Equal(that.Window) {
		return false
	}
	if n.Adjustment != that.Adjustment {
		return false
	}
	if n.NodeType != that.NodeType {
		return false
	}
	if n.CPUCoreHours != that.CPUCoreHours {
		return false
	}
	if n.RAMByteHours != that.RAMByteHours {
		return false
	}
	if n.GPUHours != that.GPUHours {
		return false
	}
	if !n.CPUBreakdown.Equal(that.CPUBreakdown) {
		return false
	}
	if !n.RAMBreakdown.Equal(that.RAMBreakdown) {
		return false
	}
	if n.CPUCost != that.CPUCost {
		return false
	}
	if n.GPUCost != that.GPUCost {
		return false
	}
	if n.RAMCost != that.RAMCost {
		return false
	}
	if n.Discount != that.Discount {
		return false
	}
	if n.Preemptible != that.Preemptible {
		return false
	}
	if !n.Overhead.Equal(that.Overhead) {
		return false
	}

	return true
}

// String implements fmt.Stringer
func (n *Node) String() string {
	return toString(n)
}

// IsPreemptible returns true if the node is 100% preemptible. It's possible
// to be "partially preemptible" by adding a preemptible node with a
// non-preemptible node.
func (n *Node) IsPreemptible() bool {
	return n.Preemptible == 1.0
}

// CPUCores returns the number of cores belonging to the node. This could be
// fractional because it's the number of core*hours divided by the number of
// hours running; e.g. the sum of a 4-core node running for the first 10 hours
// and a 3-core node running for the last 20 hours of the same 24-hour window
// would produce:
//
//	(4*10 + 3*20) / 24 = 4.167 cores
//
// However, any number of cores running for the full span of a window will
// report the actual number of cores of the static node; e.g. the above
// scenario for one entire 24-hour window:
//
//	(4*24 + 3*24) / 24 = (4 + 3) = 7 cores
func (n *Node) CPUCores() float64 {
	// [core*hr]*([min/hr]*[1/min]) = [core*hr]/[hr] = core
	return n.CPUCoreHours * (60.0 / n.Minutes())
}

// RAMBytes returns the amount of RAM belonging to the node. This could be
// fractional because it's the number of byte*hours divided by the number of
// hours running; e.g. the sum of a 12GiB-RAM node running for the first 10 hours
// and a 16GiB-RAM node running for the last 20 hours of the same 24-hour window
// would produce:
//
//	(12*10 + 16*20) / 24 = 18.333GiB RAM
//
// However, any number of bytes running for the full span of a window will
// report the actual number of bytes of the static node; e.g. the above
// scenario for one entire 24-hour window:
//
//	(12*24 + 16*24) / 24 = (12 + 16) = 28GiB RAM
func (n *Node) RAMBytes() float64 {
	// [b*hr]*([min/hr]*[1/min]) = [b*hr]/[hr] = b
	return n.RAMByteHours * (60.0 / n.Minutes())
}

// GPUs returns the amount of GPUs belonging to the node. This could be
// fractional because it's the number of gpu*hours divided by the number of
// hours running; e.g. the sum of a 2 gpu node running for the first 10 hours
// and a 1 gpu node running for the last 20 hours of the same 24-hour window
// would produce:
//
//	(2*10 + 1*20) / 24 = 1.667 GPUs
//
// However, any number of GPUs running for the full span of a window will
// report the actual number of GPUs of the static node; e.g. the above
// scenario for one entire 24-hour window:
//
//	(2*24 + 1*24) / 24 = (2 + 1) = 3 GPUs
func (n *Node) GPUs() float64 {
	// [b*hr]*([min/hr]*[1/min]) = [b*hr]/[hr] = b
	return n.GPUHours * (60.0 / n.Minutes())
}

func (n *Node) SanitizeNaN() {
	if math.IsNaN(n.Adjustment) {
		log.DedupedWarningf(5, "Node: Unexpected NaN found for Adjustment: labels:%v, window:%s, properties:%s", n.Labels, n.Window.String(), n.Properties.String())
		n.Adjustment = 0
	}
	if math.IsNaN(n.CPUCoreHours) {
		log.DedupedWarningf(5, "Node: Unexpected NaN found for CPUCoreHours: labels:%v, window:%s, properties:%s", n.Labels, n.Window.String(), n.Properties.String())
		n.CPUCoreHours = 0
	}
	if math.IsNaN(n.RAMByteHours) {
		log.DedupedWarningf(5, "Node: Unexpected NaN found for RAMByteHours: labels:%v, window:%s, properties:%s", n.Labels, n.Window.String(), n.Properties.String())
		n.RAMByteHours = 0
	}
	if math.IsNaN(n.GPUHours) {
		log.DedupedWarningf(5, "Node: Unexpected NaN found for GPUHours: labels:%v, window:%s, properties:%s", n.Labels, n.Window.String(), n.Properties.String())
		n.GPUHours = 0
	}
	if math.IsNaN(n.CPUCost) {
		log.DedupedWarningf(5, "Node: Unexpected NaN found for CPUCost: labels:%v, window:%s, properties:%s", n.Labels, n.Window.String(), n.Properties.String())
		n.CPUCost = 0
	}
	if math.IsNaN(n.GPUCost) {
		log.DedupedWarningf(5, "Node: Unexpected NaN found for GPUCost: labels:%v, window:%s, properties:%s", n.Labels, n.Window.String(), n.Properties.String())
		n.GPUCost = 0
	}
	if math.IsNaN(n.GPUCount) {
		log.DedupedWarningf(5, "Node: Unexpected NaN found for GPUCount: labels:%v, window:%s, properties:%s", n.Labels, n.Window.String(), n.Properties.String())
		n.GPUCount = 0
	}
	if math.IsNaN(n.RAMCost) {
		log.DedupedWarningf(5, "Node: Unexpected NaN found for RAMCost: labels:%v, window:%s, properties:%s", n.Labels, n.Window.String(), n.Properties.String())
		n.RAMCost = 0
	}
	if math.IsNaN(n.Discount) {
		log.DedupedWarningf(5, "Node: Unexpected NaN found for Discount: labels:%v, window:%s, properties:%s", n.Labels, n.Window.String(), n.Properties.String())
		n.Discount = 0
	}
	if math.IsNaN(n.Preemptible) {
		log.DedupedWarningf(5, "Node: Unexpected NaN found for Preemptible: labels:%v, window:%s, properties:%s", n.Labels, n.Window.String(), n.Properties.String())
		n.Preemptible = 0
	}

	if n.CPUBreakdown != nil {
		n.CPUBreakdown.SanitizeNaN()
	}
	if n.RAMBreakdown != nil {
		n.RAMBreakdown.SanitizeNaN()
	}

	if n.Overhead != nil {
		n.Overhead.SanitizeNaN()
	}
}

// LoadBalancer is an Asset representing a single load balancer in a cluster
// TODO: add GB of ingress processed, numForwardingRules once we start recording those to prometheus metric
type LoadBalancer struct {
	Properties *AssetProperties
	Labels     AssetLabels
	Start      time.Time
	End        time.Time
	Window     Window
	Adjustment float64
	Cost       float64
	Private    bool   // @bingen:field[version=20]
	Ip         string // @bingen:field[version=21]
}

// NewLoadBalancer instantiates and returns a new LoadBalancer
func NewLoadBalancer(name, cluster, providerID string, start, end time.Time, window Window, private bool, ip string) *LoadBalancer {
	properties := &AssetProperties{
		Category:   NetworkCategory,
		Name:       name,
		Cluster:    cluster,
		ProviderID: providerID,
		Service:    KubernetesService,
	}

	return &LoadBalancer{
		Properties: properties,
		Labels:     AssetLabels{},
		Start:      start,
		End:        end,
		Window:     window,
		Private:    private,
		Ip:         ip,
	}
}

// Type returns the AssetType of the Asset
func (lb *LoadBalancer) Type() AssetType {
	return LoadBalancerAssetType
}

// Properties returns the Asset's Properties
func (lb *LoadBalancer) GetProperties() *AssetProperties {
	return lb.Properties
}

// SetProperties sets the Asset's Properties
func (lb *LoadBalancer) SetProperties(props *AssetProperties) {
	lb.Properties = props
}

// Labels returns the Asset's labels
func (lb *LoadBalancer) GetLabels() AssetLabels {
	return lb.Labels
}

// SetLabels sets the Asset's labels
func (lb *LoadBalancer) SetLabels(labels AssetLabels) {
	lb.Labels = labels
}

// Adjustment returns the Asset's cost adjustment
func (lb *LoadBalancer) GetAdjustment() float64 {
	return lb.Adjustment
}

// SetAdjustment sets the Asset's cost adjustment
func (lb *LoadBalancer) SetAdjustment(adj float64) {
	lb.Adjustment = adj
}

// TotalCost returns the total cost of the Asset
func (lb *LoadBalancer) TotalCost() float64 {
	return lb.Cost + lb.Adjustment
}

// Start returns the preceise start point of the Asset within the window
func (lb *LoadBalancer) GetStart() time.Time {
	return lb.Start
}

// End returns the preceise end point of the Asset within the window
func (lb *LoadBalancer) GetEnd() time.Time {
	return lb.End
}

// Minutes returns the number of minutes the Asset ran within the window
func (lb *LoadBalancer) Minutes() float64 {
	return lb.End.Sub(lb.Start).Minutes()
}

// Window returns the window within which the Asset ran
func (lb *LoadBalancer) GetWindow() Window {
	return lb.Window
}

func (lb *LoadBalancer) SetWindow(window Window) {
	lb.Window = window
}

// ExpandWindow expands the Asset's window by the given window
func (lb *LoadBalancer) ExpandWindow(w Window) {
	lb.Window = lb.Window.Expand(w)
}

// SetStartEnd sets the Asset's Start and End fields
func (lb *LoadBalancer) SetStartEnd(start, end time.Time) {
	if lb.Window.Contains(start) {
		lb.Start = start
	} else {
		log.Warnf("Disk.SetStartEnd: start %s not in %s", start, lb.Window)
	}

	if lb.Window.Contains(end) {
		lb.End = end
	} else {
		log.Warnf("Disk.SetStartEnd: end %s not in %s", end, lb.Window)
	}
}

// Add sums the Asset with the given Asset to produce a new Asset, maintaining
// as much relevant information as possible (i.e. type, Properties, labels).
func (lb *LoadBalancer) Add(a Asset) Asset {
	// LoadBalancer + LoadBalancer = LoadBalancer
	if that, ok := a.(*LoadBalancer); ok {
		this := lb.Clone().(*LoadBalancer)
		this.add(that)
		return this
	}

	props := lb.GetProperties().Merge(a.GetProperties())
	labels := lb.Labels.Merge(a.GetLabels())

	start := lb.Start
	if a.GetStart().Before(start) {
		start = a.GetStart()
	}
	end := lb.End
	if a.GetEnd().After(end) {
		end = a.GetEnd()
	}
	window := lb.Window.Expand(a.GetWindow())

	// LoadBalancer + !LoadBalancer = Any
	any := NewAsset(start, end, window)
	any.SetProperties(props)
	any.SetLabels(labels)
	any.Adjustment = lb.Adjustment + a.GetAdjustment()
	any.Cost = (lb.TotalCost() - lb.Adjustment) + (a.TotalCost() - a.GetAdjustment())

	return any
}

func (lb *LoadBalancer) add(that *LoadBalancer) {
	if lb == nil {
		lb = that
		return
	}

	props := lb.Properties.Merge(that.GetProperties())
	labels := lb.Labels.Merge(that.GetLabels())
	lb.SetProperties(props)
	lb.SetLabels(labels)

	start := lb.Start
	if that.Start.Before(start) {
		start = that.Start
	}
	end := lb.End
	if that.End.After(end) {
		end = that.End
	}
	window := lb.Window.Expand(that.Window)
	lb.Start = start
	lb.End = end
	lb.Window = window

	lb.Cost += that.Cost
	lb.Adjustment += that.Adjustment

	if lb.Ip != that.Ip {
		//TODO: should we add to an array here or just ignore?
		log.DedupedWarningf(5, "LoadBalancer add: load balancer ip fields (%s and %s) do not match. ignoring...", lb.Ip, that.Ip)
	}
}

// Clone returns a cloned instance of the given Asset
func (lb *LoadBalancer) Clone() Asset {
	return &LoadBalancer{
		Properties: lb.Properties.Clone(),
		Labels:     lb.Labels.Clone(),
		Start:      lb.Start,
		End:        lb.End,
		Window:     lb.Window.Clone(),
		Adjustment: lb.Adjustment,
		Cost:       lb.Cost,
		Private:    lb.Private,
		Ip:         lb.Ip,
	}
}

// Equal returns true if the two Assets match precisely
func (lb *LoadBalancer) Equal(a Asset) bool {
	that, ok := a.(*LoadBalancer)
	if !ok {
		return false
	}

	if !lb.Labels.Equal(that.Labels) {
		return false
	}
	if !lb.Properties.Equal(that.Properties) {
		return false
	}
	if !lb.Start.Equal(that.Start) {
		return false
	}
	if !lb.End.Equal(that.End) {
		return false
	}
	if !lb.Window.Equal(that.Window) {
		return false
	}
	if lb.Adjustment != that.Adjustment {
		return false
	}
	if lb.Cost != that.Cost {
		return false
	}
	if lb.Private != that.Private {
		return false
	}
	if lb.Ip != that.Ip {
		return false
	}

	return true
}

// String implements fmt.Stringer
func (lb *LoadBalancer) String() string {
	return toString(lb)
}

func (lb *LoadBalancer) SanitizeNaN() {
	if lb == nil {
		return
	}
	if math.IsNaN(lb.Adjustment) {
		log.DedupedWarningf(5, "LoadBalancer: Unexpected NaN found for Adjustment: labels:%v, window:%s, properties:%s", lb.Labels, lb.Window.String(), lb.Properties.String())
		lb.Adjustment = 0
	}
	if math.IsNaN(lb.Cost) {
		log.DedupedWarningf(5, "LoadBalancer: Unexpected NaN found for Cost: labels:%v, window:%s, properties:%s", lb.Labels, lb.Window.String(), lb.Properties.String())
		lb.Cost = 0
	}
}

// SharedAsset is an Asset representing a shared cost
type SharedAsset struct {
	Properties *AssetProperties
	Labels     AssetLabels
	Window     Window
	Cost       float64
}

// NewSharedAsset creates and returns a new SharedAsset
func NewSharedAsset(name string, window Window) *SharedAsset {
	properties := &AssetProperties{
		Name:     name,
		Category: SharedCategory,
		Service:  OtherCategory,
	}

	return &SharedAsset{
		Properties: properties,
		Labels:     AssetLabels{},
		Window:     window.Clone(),
	}
}

// Type returns the AssetType of the Asset
func (sa *SharedAsset) Type() AssetType {
	return SharedAssetType
}

// Properties returns the Asset's Properties
func (sa *SharedAsset) GetProperties() *AssetProperties {
	return sa.Properties
}

// SetProperties sets the Asset's Properties
func (sa *SharedAsset) SetProperties(props *AssetProperties) {
	sa.Properties = props
}

// Labels returns the Asset's labels
func (sa *SharedAsset) GetLabels() AssetLabels {
	return sa.Labels
}

// SetLabels sets the Asset's labels
func (sa *SharedAsset) SetLabels(labels AssetLabels) {
	sa.Labels = labels
}

// Adjustment is not relevant to SharedAsset, but required to implement Asset
func (sa *SharedAsset) GetAdjustment() float64 {
	return 0.0
}

// SetAdjustment is not relevant to SharedAsset, but required to implement Asset
func (sa *SharedAsset) SetAdjustment(float64) {
	return
}

// TotalCost returns the Asset's total cost
func (sa *SharedAsset) TotalCost() float64 {
	return sa.Cost
}

// Start returns the start time of the Asset
func (sa *SharedAsset) GetStart() time.Time {
	return *sa.Window.start
}

// End returns the end time of the Asset
func (sa *SharedAsset) GetEnd() time.Time {
	return *sa.Window.end
}

// Minutes returns the number of minutes the SharedAsset ran within the window
func (sa *SharedAsset) Minutes() float64 {
	return sa.Window.Minutes()
}

// Window returns the window within the SharedAsset ran
func (sa *SharedAsset) GetWindow() Window {
	return sa.Window
}

func (sa *SharedAsset) SetWindow(window Window) {
	sa.Window = window
}

// ExpandWindow expands the Asset's window
func (sa *SharedAsset) ExpandWindow(w Window) {
	sa.Window = sa.Window.Expand(w)
}

// SetStartEnd sets the Asset's Start and End fields (not applicable here)
func (sa *SharedAsset) SetStartEnd(start, end time.Time) {
	return
}

// Add sums the Asset with the given Asset to produce a new Asset, maintaining
// as much relevant information as possible (i.e. type, Properties, labels).
func (sa *SharedAsset) Add(a Asset) Asset {
	// SharedAsset + SharedAsset = SharedAsset
	if that, ok := a.(*SharedAsset); ok {
		this := sa.Clone().(*SharedAsset)
		this.add(that)
		return this
	}

	props := sa.Properties.Merge(a.GetProperties())
	labels := sa.Labels.Merge(a.GetLabels())

	start := sa.GetStart()
	if a.GetStart().Before(start) {
		start = a.GetStart()
	}
	end := sa.GetEnd()
	if a.GetEnd().After(end) {
		end = a.GetEnd()
	}
	window := sa.Window.Expand(a.GetWindow())

	// SharedAsset + !SharedAsset = Any
	any := NewAsset(start, end, window)
	any.SetProperties(props)
	any.SetLabels(labels)
	any.Adjustment = sa.GetAdjustment() + a.GetAdjustment()
	any.Cost = (sa.TotalCost() - sa.GetAdjustment()) + (a.TotalCost() - a.GetAdjustment())

	return any
}

func (sa *SharedAsset) add(that *SharedAsset) {
	if sa == nil {
		sa = that
		return
	}

	props := sa.Properties.Merge(that.GetProperties())
	labels := sa.Labels.Merge(that.GetLabels())
	sa.SetProperties(props)
	sa.SetLabels(labels)

	window := sa.Window.Expand(that.Window)
	sa.Window = window

	sa.Cost += that.Cost
}

// Clone returns a deep copy of the given SharedAsset
func (sa *SharedAsset) Clone() Asset {
	if sa == nil {
		return nil
	}

	return &SharedAsset{
		Properties: sa.Properties.Clone(),
		Labels:     sa.Labels.Clone(),
		Window:     sa.Window.Clone(),
		Cost:       sa.Cost,
	}
}

// Equal returns true if the two Assets are exact matches
func (sa *SharedAsset) Equal(a Asset) bool {
	that, ok := a.(*SharedAsset)
	if !ok {
		return false
	}

	if !sa.Labels.Equal(that.Labels) {
		return false
	}
	if !sa.Properties.Equal(that.Properties) {
		return false
	}
	if !sa.Window.Equal(that.Window) {
		return false
	}
	if sa.Cost != that.Cost {
		return false
	}

	return true
}

// String implements fmt.Stringer
func (sa *SharedAsset) String() string {
	return toString(sa)
}

func (sa *SharedAsset) SanitizeNaN() {
	if sa == nil {
		return
	}
	if math.IsNaN(sa.Cost) {
		log.DedupedWarningf(5, "SharedAsset: Unexpected NaN found for Cost: labels:%v, window:%s, properties:%s", sa.Labels, sa.Window.String(), sa.Properties.String())
		sa.Cost = 0
	}
}

// This type exists because only the assets map of AssetSet is marshaled to
// json, which makes it impossible to recreate an AssetSet struct. Thus,
// the type when unmarshaling a marshaled AssetSet,is AssetSetResponse
type AssetSetResponse struct {
	Assets map[string]Asset
}

// AssetSet stores a set of Assets, each with a unique name, that share
// a window. An AssetSet is mutable, so treat it like a threadsafe map.
type AssetSet struct {
	AggregationKeys   []string
	Assets            map[string]Asset
	Any               map[string]*Any               //@bingen:field[ignore]
	Cloud             map[string]*Cloud             //@bingen:field[ignore]
	ClusterManagement map[string]*ClusterManagement //@bingen:field[ignore]
	Disks             map[string]*Disk              //@bingen:field[ignore]
	Network           map[string]*Network           //@bingen:field[ignore]
	Nodes             map[string]*Node              //@bingen:field[ignore]
	LoadBalancers     map[string]*LoadBalancer      //@bingen:field[ignore]
	SharedAssets      map[string]*SharedAsset       //@bingen:field[ignore]
	FromSource        string                        // stores the name of the source used to compute the data
	Window            Window
	Warnings          []string
	Errors            []string
}

// This methid is executed before marshalling the AssetSet binary.
func preProcessAssetSet(assetSet *AssetSet) {
	length := len(assetSet.Any) + len(assetSet.Cloud) + len(assetSet.ClusterManagement) + len(assetSet.Disks) +
		len(assetSet.Network) + len(assetSet.Nodes) + len(assetSet.LoadBalancers) + len(assetSet.SharedAssets)

	if length != len(assetSet.Assets) {
		log.Warnf("AssetSet concrete Asset maps are out of sync with AssetSet.Assets map.")
	}
}

// This method is executed after unmarshalling the AssetSet binary.
func postProcessAssetSet(assetSet *AssetSet) {
	for key, as := range assetSet.Assets {
		addToConcreteMap(assetSet, key, as)
	}
}

// addToConcreteMap adds the Asset to the correct concrete map in the AssetSet. This is used
// in the postProcessAssetSet method as well as AssetSet.Insert().
func addToConcreteMap(assetSet *AssetSet, key string, as Asset) {
	switch asset := as.(type) {
	case *Any:
		if assetSet.Any == nil {
			assetSet.Any = make(map[string]*Any)
		}
		assetSet.Any[key] = asset

	case *Cloud:
		if assetSet.Cloud == nil {
			assetSet.Cloud = make(map[string]*Cloud)
		}
		assetSet.Cloud[key] = asset

	case *ClusterManagement:
		if assetSet.ClusterManagement == nil {
			assetSet.ClusterManagement = make(map[string]*ClusterManagement)
		}
		assetSet.ClusterManagement[key] = asset

	case *Disk:
		if assetSet.Disks == nil {
			assetSet.Disks = make(map[string]*Disk)
		}
		assetSet.Disks[key] = asset

	case *Network:
		if assetSet.Network == nil {
			assetSet.Network = make(map[string]*Network)
		}
		assetSet.Network[key] = asset

	case *Node:
		if assetSet.Nodes == nil {
			assetSet.Nodes = make(map[string]*Node)
		}
		assetSet.Nodes[key] = asset

	case *LoadBalancer:
		if assetSet.LoadBalancers == nil {
			assetSet.LoadBalancers = make(map[string]*LoadBalancer)
		}
		assetSet.LoadBalancers[key] = asset

	case *SharedAsset:
		if assetSet.SharedAssets == nil {
			assetSet.SharedAssets = make(map[string]*SharedAsset)
		}
		assetSet.SharedAssets[key] = asset
	}
}

// removeFromConcreteMap will remove an Asset from the AssetSet's concrete type mapping for the Asset and key.
func removeFromConcreteMap(assetSet *AssetSet, key string, as Asset) {
	switch as.(type) {
	case *Any:
		delete(assetSet.Any, key)

	case *Cloud:
		delete(assetSet.Cloud, key)

	case *ClusterManagement:
		delete(assetSet.ClusterManagement, key)

	case *Disk:
		delete(assetSet.Disks, key)

	case *Network:
		delete(assetSet.Network, key)

	case *Node:
		delete(assetSet.Nodes, key)

	case *LoadBalancer:
		delete(assetSet.LoadBalancers, key)

	case *SharedAsset:
		delete(assetSet.SharedAssets, key)
	}
}

// NewAssetSet instantiates a new AssetSet and, optionally, inserts
// the given list of Assets
func NewAssetSet(start, end time.Time, assets ...Asset) *AssetSet {
	as := &AssetSet{
		Assets: map[string]Asset{},
		Window: NewWindow(&start, &end),
	}

	for _, a := range assets {
		as.Insert(a, nil)
	}

	return as
}

// AggregateBy aggregates the Assets in the AssetSet by the given list of
// AssetProperties, such that each asset is binned by a key determined by its
// relevant property values.
func (as *AssetSet) AggregateBy(aggregateBy []string, opts *AssetAggregationOptions) error {
	if opts == nil {
		opts = &AssetAggregationOptions{}
	}

	if as.IsEmpty() && len(opts.SharedHourlyCosts) == 0 {
		return nil
	}

	var filter AssetMatcher
	if opts.Filter == nil {
		filter = &matcher.AllPass[Asset]{}
	} else {
		compiler := NewAssetMatchCompiler()
		var err error
		filter, err = compiler.Compile(opts.Filter)
		if err != nil {
			return fmt.Errorf("compiling filter '%s': %w", ast.ToPreOrderShortString(opts.Filter), err)
		}
	}
	if filter == nil {
		return fmt.Errorf("unexpected nil filter")
	}

	aggSet := NewAssetSet(as.Start(), as.End())
	aggSet.AggregationKeys = aggregateBy

	// Compute hours of the given AssetSet, and if it ends in the future,
	// adjust the hours accordingly
	hours := as.Window.Minutes() / 60.0
	diff := time.Since(as.End())
	if diff < 0.0 {
		hours += diff.Hours()
	}

	// Insert a shared asset for each shared cost
	for name, hourlyCost := range opts.SharedHourlyCosts {
		sa := NewSharedAsset(name, as.Window.Clone())
		sa.Cost = hourlyCost * hours

		// Insert shared asset if it passes filter
		if filter.Matches(sa) {
			err := aggSet.Insert(sa, opts.LabelConfig)
			if err != nil {
				return err
			}
		}
	}

	// Delete the Assets that don't pass each filter
	for key, asset := range as.Assets {
		if !filter.Matches(asset) {
			delete(as.Assets, key)
		}
	}

	// Insert each asset into the new set, which will be keyed by the `aggregateBy`
	// on aggSet, resulting in aggregation.
	for _, asset := range as.Assets {
		err := aggSet.Insert(asset, opts.LabelConfig)
		if err != nil {
			return err
		}
	}

	// Assign the aggregated values back to the original set
	as.Assets = aggSet.Assets
	as.AggregationKeys = aggregateBy

	return nil
}

// Clone returns a new AssetSet with a deep copy of the given
// AssetSet's assets.
func (as *AssetSet) Clone() *AssetSet {
	if as == nil {
		return nil
	}

	var aggregateBy []string
	if as.AggregationKeys != nil {
		aggregateBy = append(aggregateBy, as.AggregationKeys...)
	}

	s := as.Start()
	e := as.End()

	var errors []string
	var warnings []string

	if as.Errors != nil {
		errors = make([]string, len(as.Errors))
		copy(errors, as.Errors)
	} else {
		errors = nil
	}

	if as.Warnings != nil {
		warnings := make([]string, len(as.Warnings))
		copy(warnings, as.Warnings)
	} else {
		warnings = nil
	}

	var anyMap map[string]*Any
	if as.Any != nil {
		anyMap = make(map[string]*Any, len(as.Any))
	}
	var cloudMap map[string]*Cloud
	if as.Cloud != nil {
		cloudMap = make(map[string]*Cloud, len(as.Cloud))
	}
	var clusterManagementMap map[string]*ClusterManagement
	if as.ClusterManagement != nil {
		clusterManagementMap = make(map[string]*ClusterManagement, len(as.ClusterManagement))
	}
	var disksMap map[string]*Disk
	if as.Disks != nil {
		disksMap = make(map[string]*Disk, len(as.Disks))
	}
	var networkMap map[string]*Network
	if as.Network != nil {
		networkMap = make(map[string]*Network, len(as.Network))
	}
	var nodesMap map[string]*Node
	if as.Nodes != nil {
		nodesMap = make(map[string]*Node, len(as.Nodes))
	}
	var loadBalancersMap map[string]*LoadBalancer
	if as.LoadBalancers != nil {
		loadBalancersMap = make(map[string]*LoadBalancer, len(as.LoadBalancers))
	}

	var sharedAssetsMap map[string]*SharedAsset
	if as.SharedAssets != nil {
		sharedAssetsMap = make(map[string]*SharedAsset, len(as.SharedAssets))
	}

	assetSet := &AssetSet{
		Window:            NewWindow(&s, &e),
		AggregationKeys:   aggregateBy,
		Assets:            make(map[string]Asset, len(as.Assets)),
		Any:               anyMap,
		Cloud:             cloudMap,
		ClusterManagement: clusterManagementMap,
		Disks:             disksMap,
		Network:           networkMap,
		Nodes:             nodesMap,
		LoadBalancers:     loadBalancersMap,
		SharedAssets:      sharedAssetsMap,
		Errors:            errors,
		Warnings:          warnings,
	}

	for k, v := range as.Assets {
		newAsset := v.Clone()
		assetSet.Assets[k] = newAsset
		addToConcreteMap(assetSet, k, newAsset)
	}

	return assetSet
}

// End returns the end time of the AssetSet's window
func (as *AssetSet) End() time.Time {
	return *as.Window.End()
}

// FindMatch attempts to find a match in the AssetSet for the given Asset on
// the provided Properties and labels. If a match is not found, FindMatch
// returns nil and a Not Found error.
func (as *AssetSet) FindMatch(query Asset, aggregateBy []string, labelConfig *LabelConfig) (Asset, error) {
	matchKey, err := key(query, aggregateBy, labelConfig)
	if err != nil {
		return nil, err
	}
	for _, asset := range as.Assets {
		if k, err := key(asset, aggregateBy, labelConfig); err != nil {
			return nil, err
		} else if k == matchKey {
			return asset, nil
		}
	}

	return nil, fmt.Errorf("Asset not found to match %s on %v", query, aggregateBy)
}

// ReconciliationMatch attempts to find an exact match in the AssetSet on
// (Category, ProviderID). If a match is found, it returns the Asset with the
// intent to adjust it. If no match exists, it attempts to find one on only
// (ProviderID). If that match is found, it returns the Asset with the intent
// to insert the associated Cloud cost.
func (as *AssetSet) ReconciliationMatch(query Asset) (Asset, bool, error) {
	// Full match means matching on (Category, ProviderID)
	fullMatchProps := []string{string(AssetCategoryProp), string(AssetProviderIDProp)}
	fullMatchKey, err := key(query, fullMatchProps, nil)

	// This should never happen because we are using enumerated Properties,
	// but the check is here in case that changes
	if err != nil {
		return nil, false, err
	}

	// Partial match means matching only on (ProviderID)
	providerIDMatchProps := []string{string(AssetProviderIDProp)}
	providerIDMatchKey, err := key(query, providerIDMatchProps, nil)

	// This should never happen because we are using enumerated Properties,
	// but the check is here in case that changes
	if err != nil {
		return nil, false, err
	}

	var providerIDMatch Asset
	for _, asset := range as.Assets {
		// Ignore cloud assets when looking for reconciliation matches
		if asset.Type() == CloudAssetType {
			continue
		}
		if k, err := key(asset, fullMatchProps, nil); err != nil {
			return nil, false, err
		} else if k == fullMatchKey {
			log.DedupedInfof(10, "Asset ETL: Reconciliation[rcnw]: ReconcileRange Match: %s", fullMatchKey)
			return asset, true, nil
		}
		if k, err := key(asset, providerIDMatchProps, nil); err != nil {
			return nil, false, err
		} else if k == providerIDMatchKey {
			// Found a partial match. Save it until after all other options
			// have been checked for full matches.
			providerIDMatch = asset
		}
	}

	// No full match was found, so return partial match, if found.
	if providerIDMatch != nil {
		return providerIDMatch, false, nil
	}

	return nil, false, fmt.Errorf("Asset not found to match %s", query)
}

// ReconciliationMatchMap returns a map of the calling AssetSet's Assets, by provider id and category. This data structure
// allows for reconciliation matching to be done in constant time and prevents duplicate reconciliation.
func (as *AssetSet) ReconciliationMatchMap() map[string]map[string]Asset {
	matchMap := make(map[string]map[string]Asset)

	if as == nil {
		return matchMap
	}

	for _, asset := range as.Assets {
		if asset == nil {
			continue
		}
		props := asset.GetProperties()
		// Ignore assets that cannot be matched when looking for reconciliation matches
		if props == nil || props.ProviderID == "" {
			continue
		}

		// we can't guarantee case in providerID for Azure provider to have map working for all providers,
		// lower casing providerID  while creating reconciliation map
		providerID := strings.ToLower(props.ProviderID)
		if _, ok := matchMap[providerID]; !ok {
			matchMap[providerID] = make(map[string]Asset)
		}

		// Check if a match is already in the map
		if duplicateAsset, ok := matchMap[providerID][props.Category]; ok {
			log.DedupedWarningf(5, "duplicate asset found when reconciling for %s", props.ProviderID)
			// if one asset already has adjustment use that one
			if duplicateAsset.GetAdjustment() == 0 && asset.GetAdjustment() != 0 {
				matchMap[providerID][props.Category] = asset
			} else if duplicateAsset.GetAdjustment() != 0 && asset.GetAdjustment() == 0 {
				matchMap[providerID][props.Category] = duplicateAsset
				// otherwise use the one with the higher cost
			} else if duplicateAsset.TotalCost() < asset.TotalCost() {
				matchMap[providerID][props.Category] = asset
			}
		} else {
			matchMap[providerID][props.Category] = asset
		}

	}
	return matchMap
}

// Get returns the Asset in the AssetSet at the given key, or nil and false
// if no Asset exists for the given key
func (as *AssetSet) Get(key string) (Asset, bool) {
	if a, ok := as.Assets[key]; ok {
		return a, true
	}
	return nil, false
}

// Insert inserts the given Asset into the AssetSet, using the AssetSet's
// configured Properties to determine the key under which the Asset will
// be inserted.
func (as *AssetSet) Insert(asset Asset, labelConfig *LabelConfig) error {
	if as == nil {
		return fmt.Errorf("cannot Insert into nil AssetSet")
	}

	if as.Assets == nil {
		as.Assets = map[string]Asset{}
	}

	// need a label config

	// Determine key into which to Insert the Asset.
	k, err := key(asset, as.AggregationKeys, labelConfig)
	if err != nil {
		return err
	}

	// Add the given Asset to the existing entry, if there is one;
	// otherwise just set directly into assets
	if _, ok := as.Assets[k]; !ok {
		as.Assets[k] = asset

		// insert the asset into it's type specific map as well
		addToConcreteMap(as, k, asset)
	} else {
		// possibly creates a new asset type, so we need to update the
		// concrete type mappings
		newAsset := as.Assets[k].Add(asset)
		removeFromConcreteMap(as, k, as.Assets[k])

		// overwrite the existing asset with the new one, and re-add the new
		// asset to the concrete type mappings
		as.Assets[k] = newAsset
		addToConcreteMap(as, k, newAsset)
	}
	// Expand the window, just to be safe. It's possible that the asset will
	// be set into the map without expanding it to the AssetSet's window.
	as.Assets[k].ExpandWindow(as.Window)

	return nil
}

// IsEmpty returns true if the AssetSet is nil, or if it contains
// zero assets.
func (as *AssetSet) IsEmpty() bool {
	return as == nil || len(as.Assets) == 0
}

func (as *AssetSet) Length() int {
	if as == nil {
		return 0
	}

	return len(as.Assets)
}

func (as *AssetSet) GetWindow() Window {
	return as.Window
}

// Resolution returns the AssetSet's window duration
func (as *AssetSet) Resolution() time.Duration {
	return as.Window.Duration()
}

func (as *AssetSet) Set(asset Asset, aggregateBy []string, labelConfig *LabelConfig) error {
	if as.IsEmpty() {
		as.Assets = map[string]Asset{}
	}

	// Expand the window to match the AssetSet, then set it
	asset.ExpandWindow(as.Window)
	k, err := key(asset, aggregateBy, labelConfig)
	if err != nil {
		return err
	}

	as.Assets[k] = asset
	addToConcreteMap(as, k, asset)

	return nil
}

func (as *AssetSet) Start() time.Time {
	return *as.Window.Start()
}

func (as *AssetSet) TotalCost() float64 {
	tc := 0.0

	for _, a := range as.Assets {
		tc += a.TotalCost()
	}

	return tc
}

func (as *AssetSet) UTCOffset() time.Duration {
	_, zone := as.Start().Zone()
	return time.Duration(zone) * time.Second
}

func (as *AssetSet) accumulate(that *AssetSet) (*AssetSet, error) {
	if as == nil {
		return that, nil
	}

	if that == nil {
		return as, nil
	}

	// In the case of an AssetSetRange with empty entries, we may end up with
	// an incoming `as` without an `aggregateBy`, even though we are tring to
	// aggregate here. This handles that case by assigning the correct `aggregateBy`.
	if !sameContents(as.AggregationKeys, that.AggregationKeys) {
		if len(as.AggregationKeys) == 0 {
			as.AggregationKeys = that.AggregationKeys
		}
	}

	// Set start, end to min(start), max(end)
	start := as.Start()
	end := as.End()

	if that.Start().Before(start) {
		start = that.Start()
	}

	if that.End().After(end) {
		end = that.End()
	}

	if as.IsEmpty() && that.IsEmpty() {
		return NewAssetSet(start, end), nil
	}

	acc := NewAssetSet(start, end)
	acc.AggregationKeys = as.AggregationKeys

	for _, asset := range as.Assets {
		err := acc.Insert(asset, nil)
		if err != nil {
			return nil, err
		}
	}

	for _, asset := range that.Assets {
		err := acc.Insert(asset, nil)
		if err != nil {
			return nil, err
		}
	}

	return acc, nil
}

func (as *AssetSet) SanitizeNaN() {
	for _, a := range as.Assets {
		a.SanitizeNaN()
	}

	for _, a := range as.Any {
		a.SanitizeNaN()
	}

	for _, c := range as.Cloud {
		c.SanitizeNaN()
	}

	for _, cm := range as.ClusterManagement {
		cm.SanitizeNaN()
	}

	for _, d := range as.Disks {
		d.SanitizeNaN()
	}

	for _, n := range as.Network {
		n.SanitizeNaN()
	}

	for _, n := range as.Nodes {
		n.SanitizeNaN()
	}

	for _, lb := range as.LoadBalancers {
		lb.SanitizeNaN()
	}

	for _, sa := range as.SharedAssets {
		sa.SanitizeNaN()
	}
}

type DiffKind string

const (
	DiffAdded   DiffKind = "added"
	DiffRemoved          = "removed"
	DiffChanged          = "changed"
)

// Diff stores an object and a string that denotes whether that object was
// added or removed from a set of those objects
type Diff[T any] struct {
	Before T
	After  T
	Kind   DiffKind
}

// DiffAsset takes two AssetSets and returns a map of keys to Diffs by checking
// the keys of each AssetSet. If a key is not found or is found with a different total cost,
// a Diff is generated and added to the map. A found asset will only be added to the map if the new
// total cost is greater than ratioCostChange * the old total cost
func DiffAsset(before, after *AssetSet, ratioCostChange float64) (map[string]Diff[Asset], error) {
	if ratioCostChange < 0.0 {
		return nil, fmt.Errorf("Percent cost change cannot be less than 0")
	}

	changedItems := map[string]Diff[Asset]{}

	for assetKey1, asset1 := range before.Assets {
		if asset2, ok := after.Assets[assetKey1]; !ok {
			d := Diff[Asset]{asset1, nil, DiffRemoved}
			changedItems[assetKey1] = d
		} else if math.Abs(asset1.TotalCost()-asset2.TotalCost()) > ratioCostChange*asset1.TotalCost() { //check if either value exceeds the other by more than pctCostChange
			d := Diff[Asset]{asset1, asset2, DiffChanged}
			changedItems[assetKey1] = d
		}
	}

	for assetKey2, asset2 := range after.Assets {
		if _, ok := before.Assets[assetKey2]; !ok {
			d := Diff[Asset]{nil, asset2, DiffAdded}
			changedItems[assetKey2] = d
		}
	}

	return changedItems, nil
}

// AssetSetRange is a thread-safe slice of AssetSets. It is meant to
// be used such that the AssetSets held are consecutive and coherent with
// respect to using the same aggregation Properties, UTC offset, and
// resolution. However these rules are not necessarily enforced, so use wisely.
type AssetSetRange struct {
	Assets    []*AssetSet
	FromStore string // stores the name of the store used to retrieve the data
}

// NewAssetSetRange instantiates a new range composed of the given
// AssetSets in the order provided.
func NewAssetSetRange(assets ...*AssetSet) *AssetSetRange {
	return &AssetSetRange{
		Assets: assets,
	}
}

func (asr *AssetSetRange) Accumulate(accumulateBy AccumulateOption) (*AssetSetRange, error) {
	switch accumulateBy {
	case AccumulateOptionNone:
		return asr.accumulateByNone()
	case AccumulateOptionAll:
		return asr.accumulateByAll()
	case AccumulateOptionHour:
		return asr.accumulateByHour()
	case AccumulateOptionDay:
		return asr.accumulateByDay()
	case AccumulateOptionWeek:
		return asr.accumulateByWeek()
	case AccumulateOptionMonth:
		return asr.accumulateByMonth()
	default:
		// ideally, this should never happen
		return nil, fmt.Errorf("unexpected error, invalid accumulateByType: %s", accumulateBy)
	}
}

func (asr *AssetSetRange) accumulateByNone() (*AssetSetRange, error) {
	return asr.clone(), nil
}

func (asr *AssetSetRange) accumulateByAll() (*AssetSetRange, error) {
	var err error
	var as *AssetSet
	as, err = asr.newAccumulation()
	if err != nil {
		return nil, fmt.Errorf("error accumulating all:%s", err)
	}

	accumulated := NewAssetSetRange(as)
	return accumulated, nil
}

func (asr *AssetSetRange) accumulateByHour() (*AssetSetRange, error) {
	// ensure that the asset sets have a 1-hour window, if a set exists
	if len(asr.Assets) > 0 && asr.Assets[0].Window.Duration() != time.Hour {
		return nil, fmt.Errorf("window duration must equal 1 hour; got:%s", asr.Assets[0].Window.Duration())
	}

	return asr.clone(), nil
}

func (asr *AssetSetRange) accumulateByDay() (*AssetSetRange, error) {
	// if the asset set window is 1-day, just return the existing asset set range
	if len(asr.Assets) > 0 && asr.Assets[0].Window.Duration() == time.Hour*24 {
		return asr, nil
	}

	var toAccumulate *AssetSetRange
	result := NewAssetSetRange()
	for i, as := range asr.Assets {

		if as.Window.Duration() != time.Hour {
			return nil, fmt.Errorf("window duration must equal 1 hour; got:%s", as.Window.Duration())
		}

		hour := as.Window.Start().Hour()

		if toAccumulate == nil {
			toAccumulate = NewAssetSetRange()
			as = as.Clone()
		}

		toAccumulate.Append(as)
		asAccumulated, err := toAccumulate.accumulate()
		if err != nil {
			return nil, fmt.Errorf("error accumulating result: %s", err)
		}
		toAccumulate = NewAssetSetRange(asAccumulated)

		if hour == 23 || i == len(asr.Assets)-1 {
			if length := len(toAccumulate.Assets); length != 1 {
				return nil, fmt.Errorf("failed accumulation, detected %d sets instead of 1", length)
			}
			result.Append(toAccumulate.Assets[0])
			toAccumulate = nil
		}
	}
	return result, nil
}

func (asr *AssetSetRange) accumulateByMonth() (*AssetSetRange, error) {
	var toAccumulate *AssetSetRange
	result := NewAssetSetRange()
	for i, as := range asr.Assets {
		if as.Window.Duration() != time.Hour*24 {
			return nil, fmt.Errorf("window duration must equal 24 hours; got:%s", as.Window.Duration())
		}

		_, month, _ := as.Window.Start().Date()
		_, nextDayMonth, _ := as.Window.Start().Add(time.Hour * 24).Date()

		if toAccumulate == nil {
			toAccumulate = NewAssetSetRange()
			as = as.Clone()
		}

		toAccumulate.Append(as)
		asAccumulated, err := toAccumulate.accumulate()
		if err != nil {
			return nil, fmt.Errorf("error accumulating result: %s", err)
		}
		toAccumulate = NewAssetSetRange(asAccumulated)

		// either the month has ended, or there are no more asset sets
		if month != nextDayMonth || i == len(asr.Assets)-1 {
			if length := len(toAccumulate.Assets); length != 1 {
				return nil, fmt.Errorf("failed accumulation, detected %d sets instead of 1", length)
			}
			result.Append(toAccumulate.Assets[0])
			toAccumulate = nil
		}
	}
	return result, nil
}

func (asr *AssetSetRange) accumulateByWeek() (*AssetSetRange, error) {
	if len(asr.Assets) > 0 && asr.Assets[0].Window.Duration() == timeutil.Week {
		return asr, nil
	}

	var toAccumulate *AssetSetRange
	result := NewAssetSetRange()
	for i, as := range asr.Assets {
		if as.Window.Duration() != time.Hour*24 {
			return nil, fmt.Errorf("window duration must equal 24 hours; got:%s", as.Window.Duration())
		}

		dayOfWeek := as.Window.Start().Weekday()

		if toAccumulate == nil {
			toAccumulate = NewAssetSetRange()
			as = as.Clone()
		}

		toAccumulate.Append(as)
		asAccumulated, err := toAccumulate.accumulate()
		if err != nil {
			return nil, fmt.Errorf("error accumulating result: %s", err)
		}
		toAccumulate = NewAssetSetRange(asAccumulated)

		// current assumption is the week always ends on Saturday, or there are no more asset sets
		if dayOfWeek == time.Saturday || i == len(asr.Assets)-1 {
			if length := len(toAccumulate.Assets); length != 1 {
				return nil, fmt.Errorf("failed accumulation, detected %d sets instead of 1", length)
			}
			result.Append(toAccumulate.Assets[0])
			toAccumulate = nil
		}
	}
	return result, nil
}

func (asr *AssetSetRange) AccumulateToAssetSet() (*AssetSet, error) {
	return asr.accumulate()
}

// Accumulate sums each AssetSet in the given range, returning a single cumulative
// AssetSet for the entire range.
func (asr *AssetSetRange) accumulate() (*AssetSet, error) {
	var assetSet *AssetSet
	var err error

	for _, as := range asr.Assets {
		assetSet, err = assetSet.accumulate(as)
		if err != nil {
			return nil, err
		}
	}

	return assetSet, nil
}

// NewAccumulation clones the first available AssetSet to use as the data structure to
// accumulate the remaining data. This leaves the original AssetSetRange intact.
func (asr *AssetSetRange) newAccumulation() (*AssetSet, error) {
	var assetSet *AssetSet
	var err error

	if asr == nil {
		return nil, fmt.Errorf("nil AssetSetRange in accumulation")
	}

	if len(asr.Assets) == 0 {
		return nil, fmt.Errorf("AssetSetRange has empty AssetSet in accumulation")
	}

	for _, as := range asr.Assets {
		if assetSet == nil {
			assetSet = as.Clone()
			continue
		}

		// copy if non-nil
		var assetSetCopy *AssetSet = nil
		if as != nil {
			assetSetCopy = as.Clone()
		}

		// nil is acceptable to pass to accumulate
		assetSet, err = assetSet.accumulate(assetSetCopy)
		if err != nil {
			return nil, err
		}
	}

	return assetSet, nil
}

type AssetAggregationOptions struct {
	SharedHourlyCosts map[string]float64
	Filter            filter21.Filter
	LabelConfig       *LabelConfig
}

func (asr *AssetSetRange) AggregateBy(aggregateBy []string, opts *AssetAggregationOptions) error {
	aggRange := &AssetSetRange{Assets: []*AssetSet{}}

	for _, as := range asr.Assets {
		err := as.AggregateBy(aggregateBy, opts)
		if err != nil {
			return err
		}

		aggRange.Assets = append(aggRange.Assets, as)
	}

	asr.Assets = aggRange.Assets

	return nil
}

func (asr *AssetSetRange) Append(that *AssetSet) {
	asr.Assets = append(asr.Assets, that)
}

// Get provides bounds checked access into the AssetSetRange's AssetSets.
func (asr *AssetSetRange) Get(i int) (*AssetSet, error) {
	if i < 0 || i >= len(asr.Assets) {
		return nil, fmt.Errorf("AssetSetRange: index out of range: %d", i)
	}

	return asr.Assets[i], nil
}

// Length returns the total number of AssetSets in the range.
func (asr *AssetSetRange) Length() int {
	if asr == nil {
		return 0
	}

	return len(asr.Assets)
}

// InsertRange merges the given AssetSetRange into the receiving one by
// lining up sets with matching windows, then inserting each asset from
// the given ASR into the respective set in the receiving ASR. If the given
// ASR contains an AssetSetRange from a window that does not exist in the
// receiving ASR, then an error is returned. However, the given ASR does not
// need to cover the full range of the receiver.
func (asr *AssetSetRange) InsertRange(that *AssetSetRange) error {
	if asr == nil {
		return fmt.Errorf("cannot insert range into nil AssetSetRange")
	}

	// keys maps window to index in asr
	keys := map[string]int{}
	for i, as := range asr.Assets {
		if as == nil {
			continue
		}
		keys[as.Window.String()] = i
	}

	// Nothing to merge, so simply return
	if len(keys) == 0 {
		return nil
	}

	var err error
	var as *AssetSet
	for _, thatAS := range that.Assets {
		if thatAS == nil || err != nil {
			continue
		}

		// Find matching AssetSet in asr
		i, ok := keys[thatAS.Window.String()]
		if !ok {
			err = fmt.Errorf("cannot merge AssetSet into window that does not exist: %s", thatAS.Window.String())
			continue
		}
		as, err = asr.Get(i)
		if err != nil {
			err = fmt.Errorf("AssetSetRange index does not exist: %d", i)
			continue
		}

		// Insert each Asset from the given set
		for _, asset := range thatAS.Assets {
			err = as.Insert(asset, nil)
			if err != nil {
				err = fmt.Errorf("error inserting asset: %s", err)
				continue
			}
		}
	}

	// err might be nil
	return err
}

func (asr *AssetSetRange) GetWarnings() []string {
	warnings := []string{}

	for _, as := range asr.Assets {
		if len(as.Warnings) > 0 {
			warnings = append(warnings, as.Warnings...)
		}
	}

	return warnings
}

func (asr *AssetSetRange) HasWarnings() bool {
	for _, as := range asr.Assets {
		if len(as.Warnings) > 0 {
			return true
		}
	}

	return false
}

// IsEmpty returns false if AssetSetRange contains a single AssetSet that is not empty
func (asr *AssetSetRange) IsEmpty() bool {
	if asr == nil || asr.Length() == 0 {
		return true
	}

	for _, asset := range asr.Assets {
		if !asset.IsEmpty() {
			return false
		}
	}

	return true
}

func (asr *AssetSetRange) MarshalJSON() ([]byte, error) {
	if asr == nil {
		return json.Marshal([]*AssetSet{})
	}

	return json.Marshal(asr.Assets)
}

// As with AssetSet, AssetSetRange does not serialize all its fields,
// making it impossible to reconstruct the AssetSetRange from its json.
// Therefore, the type a marshaled AssetSetRange unmarshals to is
// AssetSetRangeResponse
type AssetSetRangeResponse struct {
	Assets []*AssetSetResponse
}

func (asr *AssetSetRange) UTCOffset() time.Duration {
	if asr.Length() == 0 {
		return 0
	}

	as, err := asr.Get(0)
	if err != nil {
		return 0
	}
	return as.UTCOffset()
}

// Window returns the full window that the AssetSetRange spans, from the
// start of the first AssetSet to the end of the last one.
func (asr *AssetSetRange) Window() Window {
	if asr == nil || asr.Length() == 0 {
		return NewWindow(nil, nil)
	}

	start := asr.Assets[0].Start()
	end := asr.Assets[asr.Length()-1].End()

	return NewWindow(&start, &end)
}

// Start returns the earliest start of all Assets in the AssetSetRange.
// It returns an error if there are no assets
func (asr *AssetSetRange) Start() (time.Time, error) {
	start := time.Time{}
	if asr == nil {
		return start, fmt.Errorf("had no data to compute a start from")
	}

	firstStartNotSet := true
	for _, as := range asr.Assets {
		for _, a := range as.Assets {
			if firstStartNotSet {
				start = a.GetStart()
				firstStartNotSet = false
			}
			if a.GetStart().Before(start) {
				start = a.GetStart()
			}
		}
	}

	if firstStartNotSet {
		return start, fmt.Errorf("had no data to compute a start from")
	}

	return start, nil
}

// End returns the latest end of all Assets in the AssetSetRange.
// It returns an error if there are no assets.
func (asr *AssetSetRange) End() (time.Time, error) {
	end := time.Time{}
	if asr == nil {
		return end, fmt.Errorf("had no data to compute an end from")
	}

	firstEndNotSet := true
	for _, as := range asr.Assets {
		for _, a := range as.Assets {
			if firstEndNotSet {
				end = a.GetEnd()
				firstEndNotSet = false
			}
			if a.GetEnd().After(end) {
				end = a.GetEnd()
			}
		}
	}

	if firstEndNotSet {
		return end, fmt.Errorf("had no data to compute an end from")
	}

	return end, nil
}

// Each iterates over all AssetSets in the AssetSetRange and returns the start and end over
// the entire range
func (asr *AssetSetRange) StartAndEnd() (time.Time, time.Time, error) {
	start := time.Time{}
	end := time.Time{}

	if asr == nil {
		return start, end, fmt.Errorf("had no data to compute a start and end from")
	}

	firstStartNotSet := true
	firstEndNotSet := true

	for _, as := range asr.Assets {
		for _, a := range as.Assets {
			if firstStartNotSet {
				start = a.GetStart()
				firstStartNotSet = false
			}
			if a.GetStart().Before(start) {
				start = a.GetStart()
			}
			if firstEndNotSet {
				end = a.GetEnd()
				firstEndNotSet = false
			}
			if a.GetEnd().After(end) {
				end = a.GetEnd()
			}
		}
	}

	if firstStartNotSet {
		return start, end, fmt.Errorf("had no data to compute a start from")
	}

	if firstEndNotSet {
		return start, end, fmt.Errorf("had no data to compute an end from")
	}

	return start, end, nil
}

// Minutes returns the duration, in minutes, between the earliest start
// and the latest end of all assets in the AssetSetRange.
func (asr *AssetSetRange) Minutes() float64 {
	if asr == nil {
		return 0
	}

	start, end, err := asr.StartAndEnd()
	if err != nil {
		return 0
	}

	duration := end.Sub(start)

	return duration.Minutes()
}

// TotalCost returns the AssetSetRange's total cost
func (asr *AssetSetRange) TotalCost() float64 {
	if asr == nil {
		return 0.0
	}

	tc := 0.0
	for _, as := range asr.Assets {
		tc += as.TotalCost()
	}

	return tc
}

func (asr *AssetSetRange) clone() *AssetSetRange {
	asrClone := NewAssetSetRange()
	asrClone.FromStore = asr.FromStore
	for _, as := range asr.Assets {
		asClone := as.Clone()
		asrClone.Append(asClone)
	}

	return asrClone
}

// This is a helper type. The Asset API returns a json which cannot be natively
// unmarshaled into any Asset struct. Therefore, this struct IN COMBINATION WITH
// DESERIALIZATION LOGIC DEFINED IN asset_json.go can unmarshal a json directly
// from an Assets API query
type AssetAPIResponse struct {
	Code int                   `json:"code"`
	Data AssetSetRangeResponse `json:"data"`
}

// Returns true if string slices a and b contain all of the same strings, in any order.
func sameContents(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !contains(b, a[i]) {
			return false
		}
	}
	return true
}

func contains(slice []string, item string) bool {
	for _, element := range slice {
		if element == item {
			return true
		}
	}
	return false
}

func GetNodePoolName(provider string, labels map[string]string) string {

	switch provider {
	case AzureProvider:
		return getPoolNameHelper(AKSNodepoolLabel, labels)
	case AWSProvider:
		return getPoolNameHelper(EKSNodepoolLabel, labels)
	case GCPProvider:
		return getPoolNameHelper(GKENodePoolLabel, labels)
	default:
		log.Warnf("node pool name not supported for this provider")
		return ""
	}
}

func getPoolNameHelper(label string, labels map[string]string) string {
	sanitizedLabel := promutil.SanitizeLabelName(label)
	if poolName, found := labels[sanitizedLabel]; found {
		return poolName
	} else {
		log.Warnf("unable to derive node pool name from node labels")
		return ""
	}
}
