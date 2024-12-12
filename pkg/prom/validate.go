package prom

import (
	"fmt"

	"github.com/opencost/opencost/pkg/env"

	prometheus "github.com/prometheus/client_golang/api"
)

var (
	prometheusValidateQuery string = "up"
	thanosValidateQuery     string = fmt.Sprintf("up offset %s", env.GetThanosOffset())
)

// PrometheusMetadata represents a validation result for prometheus/thanos running
// opencost.
type PrometheusMetadata struct {
	Running            bool `json:"running"`
	KubecostDataExists bool `json:"kubecostDataExists"`
}

// Validate tells the model what data prometheus has on it.
func Validate(cli prometheus.Client) (*PrometheusMetadata, error) {
	if IsThanos(cli) {
		return validate(cli, thanosValidateQuery)
	}

	return validate(cli, prometheusValidateQuery)
}

// validate executes the prometheus query against the provided client.
func validate(cli prometheus.Client, q string) (*PrometheusMetadata, error) {
	ctx := NewContext(cli)

	resUp, _, err := ctx.QuerySync(q)
	if err != nil {
		return &PrometheusMetadata{
			Running:            false,
			KubecostDataExists: false,
		}, err
	}

	if len(resUp) == 0 {
		return &PrometheusMetadata{
			Running:            false,
			KubecostDataExists: false,
		}, fmt.Errorf("no running jobs on Prometheus at %s", ctx.QueryURL().Path)
	}

	for _, result := range resUp {
		job, err := result.GetString("job")
		if err != nil {
			return &PrometheusMetadata{
				Running:            false,
				KubecostDataExists: false,
			}, fmt.Errorf("up query does not have job names")
		}

		if job == "kubecost" {
			return &PrometheusMetadata{
				Running:            true,
				KubecostDataExists: true,
			}, err
		}
	}

	return &PrometheusMetadata{
		Running:            true,
		KubecostDataExists: false,
	}, nil
}
