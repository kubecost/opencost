package opencost

import (
	"fmt"
	"strings"
)

// CostMetricName a string type that acts as an enumeration of possible CostMetric options
type CostMetricName string

const (
	CostMetricNone             CostMetricName = ""
	CostMetricListCost         CostMetricName = "listCost"
	CostMetricNetCost          CostMetricName = "netCost"
	CostMetricAmortizedNetCost CostMetricName = "amortizedNetCost"
	CostMetricInvoicedCost     CostMetricName = "invoicedCost"
	CostMetricAmortizedCost    CostMetricName = "amortizedCost"
)

// ParseCostMetricName provides a resilient way to parse one of the enumerated CostMetricName types from a string
// or throws an error if it is not able to.
func ParseCostMetricName(costMetric string) (CostMetricName, error) {
	switch strings.ToLower(costMetric) {
	case strings.ToLower(string(CostMetricListCost)):
		return CostMetricListCost, nil
	case strings.ToLower(string(CostMetricAmortizedCost)):
		return CostMetricAmortizedCost, nil
	case strings.ToLower(string(CostMetricAmortizedNetCost)):
		return CostMetricAmortizedNetCost, nil
	case strings.ToLower(string(CostMetricNetCost)):
		return CostMetricNetCost, nil
	case strings.ToLower(string(CostMetricInvoicedCost)):
		return CostMetricInvoicedCost, nil
	}
	return CostMetricNone, fmt.Errorf("failed to parse a valid CostMetricName from '%s'", costMetric)
}

// CostMetric is a container for values associated with a specific accounting method
type CostMetric struct {
	Cost              float64 `json:"cost"`
	KubernetesPercent float64 `json:"kubernetesPercent"`
}

func (cm CostMetric) Equal(that CostMetric) bool {
	return cm.Cost == that.Cost && cm.KubernetesPercent == that.KubernetesPercent
}

func (cm CostMetric) Clone() CostMetric {
	return CostMetric{
		Cost:              cm.Cost,
		KubernetesPercent: cm.KubernetesPercent,
	}
}

func (cm CostMetric) add(that CostMetric) CostMetric {
	// Compute KubernetesPercent for sum
	k8sPct := 0.0
	sumCost := cm.Cost + that.Cost
	if sumCost > 0.0 {
		thisK8sCost := cm.Cost * cm.KubernetesPercent
		thatK8sCost := that.Cost * that.KubernetesPercent
		k8sPct = (thisK8sCost + thatK8sCost) / sumCost
	}

	return CostMetric{
		Cost:              sumCost,
		KubernetesPercent: k8sPct,
	}
}

// percent returns the product of the given percent and the cost, KubernetesPercent remains the same
func (cm CostMetric) percent(pct float64) CostMetric {
	return CostMetric{
		Cost:              cm.Cost * pct,
		KubernetesPercent: cm.KubernetesPercent,
	}
}
