package opencost

import (
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/filter"
)

// AllocationQuerier interface defining api for requesting Allocation data
type AllocationQuerier interface {
	QueryAllocation(start, end time.Time, opts *AllocationQueryOptions) (*AllocationSetRange, error)
}

// SummaryAllocationQuerier interface defining api for requesting SummaryAllocation data
type SummaryAllocationQuerier interface {
	QuerySummaryAllocation(start, end time.Time, opts *AllocationQueryOptions) (*SummaryAllocationSetRange, error)
}

// AssetQuerier interface defining api for requesting Asset data
type AssetQuerier interface {
	QueryAsset(start, end time.Time, opts *AssetQueryOptions) (*AssetSetRange, error)
}

// AllocationQueryOptions defines optional parameters for querying an Allocation Store
type AllocationQueryOptions struct {
	Accumulate              AccumulateOption
	AggregateBy             []string
	Compute                 bool
	DisableAggregatedStores bool
	Filter                  filter.Filter
	IdleByNode              bool
	IncludeExternal         bool
	IncludeIdle             bool
	LabelConfig             *LabelConfig
	MergeUnallocated        bool
	Reconcile               bool
	ReconcileNetwork        bool
	ShareFuncs              []AllocationMatchFunc
	SharedHourlyCosts       map[string]float64
	ShareIdle               string
	ShareSplit              string
	ShareTenancyCosts       bool
	SplitIdle               bool
	Step                    time.Duration
}

type AccumulateOption string

const (
	AccumulateOptionNone    AccumulateOption = ""
	AccumulateOptionAll     AccumulateOption = "all"
	AccumulateOptionHour    AccumulateOption = "hour"
	AccumulateOptionDay     AccumulateOption = "day"
	AccumulateOptionWeek    AccumulateOption = "week"
	AccumulateOptionMonth   AccumulateOption = "month"
	AccumulateOptionQuarter AccumulateOption = "quarter"
)

// ParseAccumulate converts a string to an AccumulateOption
func ParseAccumulate(acc string) AccumulateOption {
	var opt AccumulateOption
	switch strings.ToLower(acc) {
	case "quarter":
		opt = AccumulateOptionQuarter
	case "month":
		opt = AccumulateOptionMonth
	case "week":
		opt = AccumulateOptionWeek
	case "day":
		opt = AccumulateOptionDay
	case "hour":
		opt = AccumulateOptionHour
	case "true":
		opt = AccumulateOptionAll
	default:
		opt = AccumulateOptionNone
	}
	return opt
}

// AssetQueryOptions defines optional parameters for querying an Asset Store
type AssetQueryOptions struct {
	Accumulate              bool
	AggregateBy             []string
	Compute                 bool
	DisableAdjustments      bool
	DisableAggregatedStores bool
	Filter                  filter.Filter
	IncludeCloud            bool
	SharedHourlyCosts       map[string]float64
	Step                    time.Duration
	LabelConfig             *LabelConfig
}
