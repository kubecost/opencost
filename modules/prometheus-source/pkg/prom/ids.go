package prom

import (
	"strings"

	prometheus "github.com/prometheus/client_golang/api"
)

const (
	// PrometheusClientID is the identifier used when creating the client that
	// targets prometheus. This can be used to check a specific client instance
	// by calling prom.IsClientID(client, prom.PrometheusClientID)
	PrometheusClientID string = "Prometheus"

	// ThanosClientID is the identifier used when creating the client that
	// targets thanos. This can be used to check a specific client instance
	// by calling prom.IsClientID(client, prom.ThanosClientID)
	ThanosClientID string = "Thanos"
)

// identityClient provides an interface for extracting an indentifer from the client objects
type identityClient interface {
	ID() string
}

// IsClientID returns true if the client has an identifier of the specific type.
func IsClientID(cli prometheus.Client, id string) bool {
	if cli == nil {
		return false
	}

	if idClient, ok := cli.(identityClient); ok {
		return strings.EqualFold(idClient.ID(), id)
	}

	return false
}

// IsPrometheus returns true if the client provided is used to target prometheus
func IsPrometheus(cli prometheus.Client) bool {
	return IsClientID(cli, PrometheusClientID)
}

// IsThanos returns true if the client provided is used to target thanos
func IsThanos(cli prometheus.Client) bool {
	return IsClientID(cli, ThanosClientID)
}
