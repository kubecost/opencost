package util

import (
	v1 "k8s.io/api/core/v1"
)

// See https://kubernetes.io/docs/reference/labels-annotations-taints/

func GetZone(labels map[string]string) (string, bool) {
	if _, ok := labels[v1.LabelTopologyZone]; ok { // Label as of 1.17
		return labels[v1.LabelTopologyZone], true
	} else if _, ok := labels[v1.LabelZoneFailureDomain]; ok { // deprecated label
		return labels[v1.LabelZoneFailureDomain], true
	} else {
		return "", false
	}
}

func GetRegion(labels map[string]string) (string, bool) {
	if _, ok := labels[v1.LabelTopologyRegion]; ok { // Label as of 1.17
		return labels[v1.LabelTopologyRegion], true
	} else if _, ok := labels[v1.LabelZoneRegion]; ok { // deprecated label
		return labels[v1.LabelZoneRegion], true
	} else {
		return "", false
	}
}

func GetInstanceType(labels map[string]string) (string, bool) {
	if _, ok := labels[v1.LabelInstanceTypeStable]; ok {
		return labels[v1.LabelInstanceTypeStable], true
	} else if _, ok := labels[v1.LabelInstanceType]; ok {
		return labels[v1.LabelInstanceType], true
	} else {
		return "", false
	}
}

func GetOperatingSystem(labels map[string]string) (string, bool) {
	if _, ok := labels[v1.LabelOSStable]; ok {
		return labels[v1.LabelOSStable], true
	} else if _, ok := labels["beta.kubernetes.io/os"]; ok {
		return labels["beta.kubernetes.io/os"], true
	} else {
		return "", false
	}
}

func GetArchType(labels map[string]string) (string, bool) {
	if _, ok := labels[v1.LabelArchStable]; ok {
		return labels[v1.LabelArchStable], true
	} else if _, ok := labels["beta.kubernetes.io/arch"]; ok {
		return labels["beta.kubernetes.io/arch"], true
	} else {
		return "", false
	}
}
