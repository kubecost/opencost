package opencost

import (
	"errors"
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
)

// CloudCost represents a CUR line item, identifying a cloud resource and
// its cost over some period of time.
type CloudCost struct {
	Properties       *CloudCostProperties `json:"properties"`
	Window           Window               `json:"window"`
	ListCost         CostMetric           `json:"listCost"`
	NetCost          CostMetric           `json:"netCost"`
	AmortizedNetCost CostMetric           `json:"amortizedNetCost"`
	InvoicedCost     CostMetric           `json:"invoicedCost"`
	AmortizedCost    CostMetric           `json:"amortizedCost"`
}

// NewCloudCost instantiates a new CloudCost
func NewCloudCost(start, end time.Time, ccProperties *CloudCostProperties, kubernetesPercent, listCost, netCost, amortizedNetCost, invoicedCost, amortizedCost float64) *CloudCost {
	return &CloudCost{
		Properties: ccProperties,
		Window:     NewWindow(&start, &end),
		ListCost: CostMetric{
			Cost:              listCost,
			KubernetesPercent: kubernetesPercent,
		},
		NetCost: CostMetric{
			Cost:              netCost,
			KubernetesPercent: kubernetesPercent,
		},
		AmortizedNetCost: CostMetric{
			Cost:              amortizedNetCost,
			KubernetesPercent: kubernetesPercent,
		},
		InvoicedCost: CostMetric{
			Cost:              invoicedCost,
			KubernetesPercent: kubernetesPercent,
		},
		AmortizedCost: CostMetric{
			Cost:              amortizedCost,
			KubernetesPercent: kubernetesPercent,
		},
	}
}

func (cc *CloudCost) Clone() *CloudCost {
	return &CloudCost{
		Properties:       cc.Properties.Clone(),
		Window:           cc.Window.Clone(),
		ListCost:         cc.ListCost.Clone(),
		NetCost:          cc.NetCost.Clone(),
		AmortizedNetCost: cc.AmortizedNetCost.Clone(),
		InvoicedCost:     cc.InvoicedCost.Clone(),
		AmortizedCost:    cc.AmortizedCost.Clone(),
	}
}

func (cc *CloudCost) Equal(that *CloudCost) bool {
	if that == nil {
		return false
	}

	return cc.Properties.Equal(that.Properties) &&
		cc.Window.Equal(that.Window) &&
		cc.ListCost.Equal(that.ListCost) &&
		cc.NetCost.Equal(that.NetCost) &&
		cc.AmortizedNetCost.Equal(that.AmortizedNetCost) &&
		cc.InvoicedCost.Equal(that.InvoicedCost) &&
		cc.AmortizedCost.Equal(that.AmortizedCost)
}

func (cc *CloudCost) add(that *CloudCost) {
	if cc == nil {
		log.Warnf("cannot add to nil CloudCost")
		return
	}

	// Preserve properties of cloud cost  that are matching between the two CloudCost
	cc.Properties = cc.Properties.Intersection(that.Properties)

	cc.ListCost = cc.ListCost.add(that.ListCost)
	cc.NetCost = cc.NetCost.add(that.NetCost)
	cc.AmortizedNetCost = cc.AmortizedNetCost.add(that.AmortizedNetCost)
	cc.InvoicedCost = cc.InvoicedCost.add(that.InvoicedCost)
	cc.AmortizedCost = cc.AmortizedCost.add(that.AmortizedCost)

	cc.Window = cc.Window.Expand(that.Window)
}

func (cc *CloudCost) StringProperty(prop string) (string, error) {
	if cc == nil {
		return "", nil
	}

	switch prop {
	case CloudCostInvoiceEntityIDProp:
		return cc.Properties.InvoiceEntityID, nil
	case CloudCostInvoiceEntityNameProp:
		return cc.Properties.InvoiceEntityName, nil
	case CloudCostAccountIDProp:
		return cc.Properties.AccountID, nil
	case CloudCostAccountNameProp:
		return cc.Properties.AccountName, nil
	case CloudCostRegionIDProp:
		return cc.Properties.RegionID, nil
	case CloudCostAvailabilityZoneProp:
		return cc.Properties.AvailabilityZone, nil
	case CloudCostProviderProp:
		return cc.Properties.Provider, nil
	case CloudCostProviderIDProp:
		return cc.Properties.ProviderID, nil
	case CloudCostServiceProp:
		return cc.Properties.Service, nil
	case CloudCostCategoryProp:
		return cc.Properties.Category, nil
	default:
		return "", fmt.Errorf("invalid property name: %s", prop)
	}
}

func (cc *CloudCost) StringMapProperty(property string) (map[string]string, error) {
	switch property {
	case CloudCostLabelProp:
		if cc.Properties == nil {
			return nil, nil
		}
		return cc.Properties.Labels, nil

	default:
		return nil, fmt.Errorf("CloudCost: StringMapProperty: invalid property name: %s", property)
	}
}

func (cc *CloudCost) GetCostMetric(costMetricName CostMetricName) (CostMetric, error) {
	switch costMetricName {
	case CostMetricListCost:
		return cc.ListCost, nil
	case CostMetricNetCost:
		return cc.NetCost, nil
	case CostMetricAmortizedNetCost:
		return cc.AmortizedNetCost, nil
	case CostMetricInvoicedCost:
		return cc.InvoicedCost, nil
	case CostMetricAmortizedCost:
		return cc.AmortizedCost, nil
	}
	return CostMetric{}, fmt.Errorf("invalid Cost Metric: %s", costMetricName)
}

// WeightCostMetrics weights all the cost metrics with the given weightedAverage
func (cc *CloudCost) WeightCostMetrics(weightedAverge float64) {
	cc.ListCost.Cost *= weightedAverge
	cc.NetCost.Cost *= weightedAverge
	cc.AmortizedNetCost.Cost *= weightedAverge
	cc.InvoicedCost.Cost *= weightedAverge
	cc.AmortizedCost.Cost *= weightedAverge
}

// CloudCostSet follows the established set pattern of windowed data types. It has addition metadata types that can be
// used to preserve data consistency and be used for validation.
// - Integration is the ID for the integration that a CloudCostSet was sourced from, this value is cleared if when a
// set is joined with another with a different key
// - AggregationProperties is set by the Aggregate function and ensures that any additional inserts are keyed correctly
type CloudCostSet struct {
	CloudCosts            map[string]*CloudCost `json:"cloudCosts"`
	Window                Window                `json:"window"`
	Integration           string                `json:"-"`
	AggregationProperties []string              `json:"aggregationProperties"`
}

// NewCloudCostSet instantiates a new CloudCostSet and, optionally, inserts
// the given list of CloudCosts
func NewCloudCostSet(start, end time.Time, cloudCosts ...*CloudCost) *CloudCostSet {
	ccs := &CloudCostSet{
		CloudCosts: map[string]*CloudCost{},
		Window:     NewWindow(&start, &end),
	}

	for _, cc := range cloudCosts {
		ccs.Insert(cc)
	}

	return ccs
}

func (ccs *CloudCostSet) Aggregate(props []string) (*CloudCostSet, error) {
	if ccs == nil {
		return nil, errors.New("cannot aggregate a nil CloudCostSet")
	}

	if ccs.Window.IsOpen() {
		return nil, fmt.Errorf("cannot aggregate a CloudCostSet with an open window: %s", ccs.Window)
	}

	// Create a new result set, with the given aggregation property
	result := ccs.cloneSet()
	result.AggregationProperties = props

	// Insert clones of each item in the set, keyed by the given property.
	// The underlying insert logic will add binned items together.
	for name, cc := range ccs.CloudCosts {
		ccClone := cc.Clone()
		err := result.Insert(ccClone)
		if err != nil {
			return nil, fmt.Errorf("error aggregating %s by %v: %s", name, props, err)
		}
	}

	return result, nil
}

func (ccs *CloudCostSet) Accumulate(that *CloudCostSet) (*CloudCostSet, error) {
	if ccs.IsEmpty() {
		return that.Clone(), nil
	}
	acc := ccs.Clone()
	err := acc.accumulateInto(that)
	if err == nil {
		return nil, err
	}
	return acc, nil
}

// accumulateInto accumulates a the arg CloudCostSet Into the receiver
func (ccs *CloudCostSet) accumulateInto(that *CloudCostSet) error {
	if ccs == nil {
		return fmt.Errorf("CloudCost: cannot accumulate into nil set")
	}

	if that.IsEmpty() {
		return nil
	}

	if ccs.Integration != that.Integration {
		ccs.Integration = ""
	}

	ccs.Window.Expand(that.Window)

	for _, cc := range that.CloudCosts {
		err := ccs.Insert(cc)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ccs *CloudCostSet) Equal(that *CloudCostSet) bool {
	if ccs.Integration != that.Integration {
		return false
	}

	if !ccs.Window.Equal(that.Window) {
		return false
	}

	// Check Aggregation Properties, slice order is grounds for inequality
	if len(ccs.AggregationProperties) != len(that.AggregationProperties) {
		return false
	}
	for i, prop := range ccs.AggregationProperties {
		if that.AggregationProperties[i] != prop {
			return false
		}
	}

	if len(ccs.CloudCosts) != len(that.CloudCosts) {
		return false
	}

	for k, cc := range ccs.CloudCosts {
		if tcc, ok := that.CloudCosts[k]; !ok || !cc.Equal(tcc) {
			return false
		}
	}

	return true
}

// Insert adds a CloudCost to a CloudCostSet using its AggregationProperties and LabelConfig
// to determine the key where it will be inserted
func (ccs *CloudCostSet) Insert(cc *CloudCost) error {
	if ccs == nil {
		return fmt.Errorf("cannot insert into nil CloudCostSet")
	}

	if cc == nil {
		return fmt.Errorf("cannot insert nil CloudCost into CloudCostSet")
	}

	if ccs.CloudCosts == nil {
		ccs.CloudCosts = map[string]*CloudCost{}
	}

	// If the Aggregation properties is not set the returned key will be a hash of the properties values
	ccKey := cc.Properties.GenerateKey(ccs.AggregationProperties)

	// Add the given CloudCost to the existing entry, if there is one;
	// otherwise just set directly into allocations
	if _, ok := ccs.CloudCosts[ccKey]; !ok {
		ccs.CloudCosts[ccKey] = cc.Clone()
	} else {
		ccs.CloudCosts[ccKey].add(cc)
	}

	return nil
}

func (ccs *CloudCostSet) Clone() *CloudCostSet {
	cloudCosts := make(map[string]*CloudCost, len(ccs.CloudCosts))
	for k, v := range ccs.CloudCosts {
		cloudCosts[k] = v.Clone()
	}

	cloneCCS := ccs.cloneSet()
	cloneCCS.CloudCosts = cloudCosts

	return cloneCCS
}

// cloneSet creates a copy of the receiver without any of its CloudCosts
func (ccs *CloudCostSet) cloneSet() *CloudCostSet {
	var aggProps []string
	if ccs.AggregationProperties != nil {
		aggProps = make([]string, len(ccs.AggregationProperties))
		for i, v := range ccs.AggregationProperties {
			aggProps[i] = v
		}
	}
	return &CloudCostSet{
		CloudCosts:            make(map[string]*CloudCost),
		Integration:           ccs.Integration,
		AggregationProperties: aggProps,
		Window:                ccs.Window.Clone(),
	}
}

func (ccs *CloudCostSet) IsEmpty() bool {
	if ccs == nil {
		return true
	}

	if len(ccs.CloudCosts) == 0 {
		return true
	}

	return false
}

func (ccs *CloudCostSet) Length() int {
	if ccs == nil {
		return 0
	}
	return len(ccs.CloudCosts)
}

func (ccs *CloudCostSet) GetWindow() Window {
	return ccs.Window
}

func (ccs *CloudCostSet) Merge(that *CloudCostSet) (*CloudCostSet, error) {
	if ccs == nil {
		return nil, fmt.Errorf("cannot merge nil CloudCostSets")
	}

	if that.IsEmpty() {
		return ccs.Clone(), nil
	}

	if !ccs.Window.Equal(that.Window) {
		return nil, fmt.Errorf("cannot merge CloudCostSets with different windows")
	}

	result := ccs.cloneSet()
	// clear integration if it is not equal
	if ccs.Integration != that.Integration {
		result.Integration = ""
	}

	for _, cc := range ccs.CloudCosts {
		result.Insert(cc)
	}

	for _, cc := range that.CloudCosts {
		result.Insert(cc)
	}

	return result, nil
}

type CloudCostSetRange struct {
	CloudCostSets []*CloudCostSet `json:"sets"`
	Window        Window          `json:"window"`
}

// NewCloudCostSetRange create a CloudCostSetRange containing CloudCostSets with windows of equal duration
// the duration between start and end must be divisible by the window duration argument
func NewCloudCostSetRange(start time.Time, end time.Time, accumOpt AccumulateOption, integration string) (*CloudCostSetRange, error) {
	windows, err := NewClosedWindow(start.UTC(), end.UTC()).GetAccumulateWindows(accumOpt)
	if err != nil {
		return nil, err
	}

	// Build slice of CloudCostSet to cover the range
	cloudCostItemSets := make([]*CloudCostSet, len(windows))
	for i, w := range windows {
		ccs := NewCloudCostSet(*w.Start(), *w.End())
		ccs.Integration = integration
		cloudCostItemSets[i] = ccs
	}
	return &CloudCostSetRange{
		CloudCostSets: cloudCostItemSets,
	}, nil
}

func (ccsr *CloudCostSetRange) Clone() *CloudCostSetRange {
	ccsSlice := make([]*CloudCostSet, len(ccsr.CloudCostSets))
	for i, ccs := range ccsr.CloudCostSets {
		ccsSlice[i] = ccs.Clone()
	}
	return &CloudCostSetRange{
		CloudCostSets: ccsSlice,
	}
}

func (ccsr *CloudCostSetRange) IsEmpty() bool {
	for _, ccs := range ccsr.CloudCostSets {
		if !ccs.IsEmpty() {
			return false
		}
	}
	return true
}

// accumulate sums each CloudCostSet in the given range, returning a single cumulative
// CloudCostSet for the entire range.
func (ccsr *CloudCostSetRange) AccumulateAll() (*CloudCostSet, error) {
	var cloudCostSet *CloudCostSet
	var err error

	if ccsr == nil {
		return nil, fmt.Errorf("nil CloudCostSetRange in accumulation")
	}

	if len(ccsr.CloudCostSets) == 0 {
		return nil, fmt.Errorf("CloudCostSetRange has empty CloudCostSet in accumulation")
	}

	for _, ccs := range ccsr.CloudCostSets {
		if cloudCostSet == nil {
			cloudCostSet = ccs.Clone()
			continue
		}
		err = cloudCostSet.accumulateInto(ccs)
		if err != nil {
			return nil, err
		}
	}

	return cloudCostSet, nil
}

// Accumulate sums CloudCostSets based on the AccumulateOption (calendar week or calendar month).
// The accumulated set is determined by the start of the window of the allocation set.
func (ccsr *CloudCostSetRange) Accumulate(accumulateBy AccumulateOption) (*CloudCostSetRange, error) {
	switch accumulateBy {
	case AccumulateOptionNone:
		return ccsr.accumulateByNone()
	case AccumulateOptionAll:
		return ccsr.accumulateByAll()
	case AccumulateOptionHour:
		return ccsr.accumulateByHour()
	case AccumulateOptionDay:
		return ccsr.accumulateByDay()
	case AccumulateOptionWeek:
		return ccsr.accumulateByWeek()
	case AccumulateOptionMonth:
		return ccsr.accumulateByMonth()
	default:
		// ideally, this should never happen
		return nil, fmt.Errorf("unexpected error, invalid accumulateByType: %s", accumulateBy)
	}
}

func (ccsr *CloudCostSetRange) accumulateByAll() (*CloudCostSetRange, error) {

	ccs, err := ccsr.AccumulateAll()
	if err != nil {
		return nil, fmt.Errorf("error accumulating all:%s", err)
	}

	accumulated := &CloudCostSetRange{
		CloudCostSets: []*CloudCostSet{ccs},
	}
	return accumulated, nil
}

func (ccsr *CloudCostSetRange) accumulateByNone() (*CloudCostSetRange, error) {
	return ccsr.Clone(), nil
}
func (ccsr *CloudCostSetRange) accumulateByHour() (*CloudCostSetRange, error) {
	// ensure that the summary allocation sets have a 1-hour window, if a set exists
	if len(ccsr.CloudCostSets) > 0 && ccsr.CloudCostSets[0].Window.Duration() != time.Hour {
		return nil, fmt.Errorf("window duration must equal 1 hour; got:%s", ccsr.CloudCostSets[0].Window.Duration())
	}

	return ccsr.Clone(), nil
}

func (ccsr *CloudCostSetRange) accumulateByDay() (*CloudCostSetRange, error) {
	// if the allocation set window is 1-day, just return the existing allocation set range
	if len(ccsr.CloudCostSets) > 0 && ccsr.CloudCostSets[0].Window.Duration() == time.Hour*24 {
		return ccsr, nil
	}

	var toAccumulate *CloudCostSetRange
	result := &CloudCostSetRange{}
	for i, ccs := range ccsr.CloudCostSets {

		if ccs.Window.Duration() != time.Hour {
			return nil, fmt.Errorf("window duration must equal 1 hour; got:%s", ccs.Window.Duration())
		}

		hour := ccs.Window.Start().Hour()

		if toAccumulate == nil {
			toAccumulate = &CloudCostSetRange{}
			ccs = ccs.Clone()
		}

		toAccumulate.Append(ccs)
		accumulated, err := toAccumulate.accumulateByAll()
		if err != nil {
			return nil, fmt.Errorf("error accumulating result: %s", err)
		}
		toAccumulate = accumulated

		if hour == 23 || i == len(ccsr.CloudCostSets)-1 {
			if length := len(toAccumulate.CloudCostSets); length != 1 {
				return nil, fmt.Errorf("failed accumulation, detected %d sets instead of 1", length)
			}
			result.Append(toAccumulate.CloudCostSets[0])
			toAccumulate = nil
		}
	}
	return result, nil
}

func (ccsr *CloudCostSetRange) accumulateByWeek() (*CloudCostSetRange, error) {
	if len(ccsr.CloudCostSets) > 0 && ccsr.CloudCostSets[0].Window.Duration() == timeutil.Week {
		return ccsr, nil
	}

	var toAccumulate *CloudCostSetRange
	result := &CloudCostSetRange{}
	for i, css := range ccsr.CloudCostSets {
		if css.Window.Duration() != time.Hour*24 {
			return nil, fmt.Errorf("window duration must equal 24 hours; got:%s", css.Window.Duration())
		}

		dayOfWeek := css.Window.Start().Weekday()

		if toAccumulate == nil {
			toAccumulate = &CloudCostSetRange{}
			css = css.Clone()
		}

		toAccumulate.Append(css)
		accumulated, err := toAccumulate.accumulateByAll()
		if err != nil {
			return nil, fmt.Errorf("error accumulating result: %s", err)
		}
		toAccumulate = accumulated

		// current assumption is the week always ends on Saturday, or there are no more allocation sets
		if dayOfWeek == time.Saturday || i == len(ccsr.CloudCostSets)-1 {
			if length := len(toAccumulate.CloudCostSets); length != 1 {
				return nil, fmt.Errorf("failed accumulation, detected %d sets instead of 1", length)
			}
			result.Append(toAccumulate.CloudCostSets[0])
			toAccumulate = nil
		}
	}
	return result, nil
}

func (ccsr *CloudCostSetRange) accumulateByMonth() (*CloudCostSetRange, error) {
	var toAccumulate *CloudCostSetRange
	result := &CloudCostSetRange{}
	for i, css := range ccsr.CloudCostSets {
		if css.Window.Duration() != time.Hour*24 {
			return nil, fmt.Errorf("window duration must equal 24 hours; got:%s", css.Window.Duration())
		}

		_, month, _ := css.Window.Start().Date()
		_, nextDayMonth, _ := css.Window.Start().Add(time.Hour * 24).Date()

		if toAccumulate == nil {
			toAccumulate = &CloudCostSetRange{}
			css = css.Clone()
		}

		toAccumulate.Append(css)
		accumulated, err := toAccumulate.accumulateByAll()
		if err != nil {
			return nil, fmt.Errorf("error accumulating result: %s", err)
		}
		toAccumulate = accumulated

		// either the month has ended, or there are no more allocation sets
		if month != nextDayMonth || i == len(ccsr.CloudCostSets)-1 {
			if length := len(toAccumulate.CloudCostSets); length != 1 {
				return nil, fmt.Errorf("failed accumulation, detected %d sets instead of 1", length)
			}
			result.Append(toAccumulate.CloudCostSets[0])
			toAccumulate = nil
		}
	}
	return result, nil
}

// Append appends the given CloudCostSet to the end of the range. It does not
// validate whether or not that violates window continuity.
func (ccsr *CloudCostSetRange) Append(that *CloudCostSet) {
	ccsr.CloudCostSets = append(ccsr.CloudCostSets, that)
}

// LoadCloudCost loads CloudCosts into existing CloudCostSets of the CloudCostSetRange.
// This function service to aggregate and distribute costs over predefined windows
// are accumulated here so that the resulting CloudCost with the 1d window has the correct price for the entire day.
// If all or a portion of the window of the CloudCost is outside of the windows of the existing CloudCostSets,
// that portion of the CloudCost's cost will not be inserted
func (ccsr *CloudCostSetRange) LoadCloudCost(cloudCost *CloudCost) {
	window := cloudCost.Window
	if window.IsOpen() {
		log.Errorf("CloudCostSetRange: LoadCloudCost: invalid window %s", window.String())
		return
	}

	totalPct := 0.0

	// Distribute cost of the current item across one or more CloudCosts in
	// across each relevant CloudCostSet. Stop when the end of the current
	// block reaches the item's end time or the end of the range.
	for _, ccs := range ccsr.CloudCostSets {
		setWindow := ccs.Window

		// get percent of item window contained in set window
		pct := setWindow.GetPercentInWindow(window)
		if pct == 0 {
			continue
		}

		cc := cloudCost
		// If the current set Window only contains a portion of the CloudCost Window, insert costs relative to that portion
		if pct < 1.0 {
			cc = &CloudCost{
				Properties:       cloudCost.Properties,
				Window:           window.Contract(setWindow),
				ListCost:         cloudCost.ListCost.percent(pct),
				NetCost:          cloudCost.NetCost.percent(pct),
				AmortizedNetCost: cloudCost.AmortizedNetCost.percent(pct),
				InvoicedCost:     cloudCost.InvoicedCost.percent(pct),
				AmortizedCost:    cloudCost.AmortizedCost.percent(pct),
			}
		}

		err := ccs.Insert(cc)
		if err != nil {
			log.Errorf("CloudCostSetRange: LoadCloudCost: failed to load CloudCost with window %s: %s", setWindow.String(), err.Error())
		}

		// If all cost has been inserted, then there is no need to check later days in the range
		totalPct += pct
		if totalPct >= 1.0 {
			return
		}
	}
}
