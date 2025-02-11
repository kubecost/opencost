//go:build !incubating

package costmodel

import (
	"time"

	"github.com/opencost/opencost/core/pkg/source"
)

// These implementations are placeholders to allow conditional compilation of
// incubating features to be enabled specifically without introducing conflicts
// with the existing codebase. Since go only supports file scoped conditional
// compilation, we need to define these no-op functions in a separate file.

// Once a change is approved to move from incubation to a feature, the methods
// defined here can be moved to the calling file and the build tag removed.

// ExtendedNodeQueryResults is a place holder data type for the incubating
// feature for extending the node details that can be returned with allocation
// data
type extendedNodeQueryResults struct{}

// queryExtendedNodeData is a place holder function for the incubating feature
func queryExtendedNodeData(grp *source.QueryGroup, ds source.OpenCostDataSource, start, end time.Time) (*extendedNodeQueryResults, error) {
	return &extendedNodeQueryResults{}, nil
}

// applyExtendedNodeData is a place holder function for the incubating feature
// which appends additional node data to the given node map
func applyExtendedNodeData(nodeMap map[nodeKey]*nodePricing, results *extendedNodeQueryResults) {
}

// nodePricing describes the resource costs associated with a given node,
// as well as the source of the information (e.g. prometheus, custom)
type nodePricing struct {
	Name            string
	NodeType        string
	ProviderID      string
	Preemptible     bool
	CostPerCPUHr    float64
	CostPerRAMGiBHr float64
	CostPerGPUHr    float64
	Discount        float64
	Source          string
}
