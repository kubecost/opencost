package gcp

import (
	"regexp"
	"strings"

	"github.com/opencost/opencost/core/pkg/opencost"
)

func IsK8s(labels map[string]string) bool {
	if _, ok := labels["goog-gke-volume"]; ok {
		return true
	}

	if _, ok := labels["goog-gke-node"]; ok {
		return true
	}

	if _, ok := labels["goog-k8s-cluster-name"]; ok {
		return true
	}

	return false
}

var parseProviderIDRx = regexp.MustCompile("^.+\\/(.+)?") // Capture "gke-cluster-3-default-pool-xxxx-yy" from "projects/###/instances/gke-cluster-3-default-pool-xxxx-yy"

func ParseProviderID(id string) string {
	match := parseProviderIDRx.FindStringSubmatch(id)
	if len(match) == 0 {
		return id
	}
	return match[len(match)-1]
}

func SelectCategory(service, description string) string {
	s := strings.ToLower(service)
	d := strings.ToLower(description)

	// Network descriptions
	if strings.Contains(d, "download") {
		return opencost.NetworkCategory
	}
	if strings.Contains(d, "network") {
		return opencost.NetworkCategory
	}
	if strings.Contains(d, "ingress") {
		return opencost.NetworkCategory
	}
	if strings.Contains(d, "egress") {
		return opencost.NetworkCategory
	}
	if strings.Contains(d, "static ip") {
		return opencost.NetworkCategory
	}
	if strings.Contains(d, "external ip") {
		return opencost.NetworkCategory
	}
	if strings.Contains(d, "load balanced") {
		return opencost.NetworkCategory
	}
	if strings.Contains(d, "licensing fee") {
		return opencost.OtherCategory
	}

	// Storage Descriptions
	if strings.Contains(d, "storage") {
		return opencost.StorageCategory
	}
	if strings.Contains(d, "pd capacity") {
		return opencost.StorageCategory
	}
	if strings.Contains(d, "pd iops") {
		return opencost.StorageCategory
	}
	if strings.Contains(d, "pd snapshot") {
		return opencost.StorageCategory
	}

	// Service Defaults
	if strings.Contains(s, "storage") {
		return opencost.StorageCategory
	}
	if strings.Contains(s, "compute") {
		return opencost.ComputeCategory
	}
	if strings.Contains(s, "sql") {
		return opencost.StorageCategory
	}
	if strings.Contains(s, "bigquery") {
		return opencost.StorageCategory
	}
	if strings.Contains(s, "kubernetes") {
		return opencost.ManagementCategory
	} else if strings.Contains(s, "pub/sub") {
		return opencost.NetworkCategory
	}

	return opencost.OtherCategory
}
