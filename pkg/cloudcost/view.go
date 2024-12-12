package cloudcost

import (
	"time"

	"github.com/opencost/opencost/core/pkg/util/mathutil"
)

// View serves data to the Cloud Cost front end, in the
// structure it requires (i.e. a graph and a table).
type View struct {
	GraphData  ViewGraphData `json:"graphData"`
	TableTotal *ViewTableRow `json:"tableTotal"`
	TableRows  ViewTableRows `json:"tableRows"`
}

type ViewTableRows []*ViewTableRow

func (vtrs ViewTableRows) Equal(that ViewTableRows) bool {
	if len(vtrs) != len(that) {
		return false
	}

	for i := 0; i < len(vtrs); i++ {
		if !vtrs[i].Equal(that[i]) {
			return false
		}
	}

	return true
}

type ViewTableRow struct {
	Name              string            `json:"name"`
	Labels            map[string]string `json:"labels"`
	KubernetesPercent float64           `json:"kubernetesPercent"`
	Cost              float64           `json:"cost"`
}

func (vtr *ViewTableRow) Equal(that *ViewTableRow) bool {
	if vtr.Name != that.Name {
		return false
	}

	if len(vtr.Labels) != len(that.Labels) {
		return false
	}

	for key, value := range vtr.Labels {
		thatValue, ok := that.Labels[key]
		if !ok {
			return false
		}
		if value != thatValue {
			return false
		}
	}

	if !mathutil.Approximately(vtr.KubernetesPercent, that.KubernetesPercent) {
		return false
	}

	if !mathutil.Approximately(vtr.Cost, that.Cost) {
		return false
	}

	return true
}

type ViewGraphData []*ViewGraphDataSet

func (vgd ViewGraphData) Equal(that ViewGraphData) bool {
	if len(vgd) != len(that) {
		return false
	}

	for i := 0; i < len(vgd); i++ {
		if !vgd[i].Equal(that[i]) {
			return false
		}
	}

	return true
}

type ViewGraphDataSet struct {
	Start time.Time              `json:"start"`
	End   time.Time              `json:"end"`
	Items []ViewGraphDataSetItem `json:"items"`
}

// NOTE: does not compare start and end times, just that the items are equal
func (vgds *ViewGraphDataSet) Equal(that *ViewGraphDataSet) bool {
	if len(vgds.Items) != len(that.Items) {
		return false
	}

	for i := 0; i < len(vgds.Items); i++ {
		if !vgds.Items[i].Equal(that.Items[i]) {
			return false
		}
	}

	return true
}

type ViewGraphDataSetItem struct {
	Name  string  `json:"name"`
	Value float64 `json:"value"`
}

func (vgdsi ViewGraphDataSetItem) Equal(that ViewGraphDataSetItem) bool {
	if vgdsi.Name != that.Name {
		return false
	}

	if !mathutil.Approximately(vgdsi.Value, that.Value) {
		return false
	}

	return true
}

type ViewTotals struct {
	NumResults int           `json:"numResults"`
	Combined   *ViewTableRow `json:"combined"`
}
