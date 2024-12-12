package opencost

import (
	"fmt"
	"strings"

	"github.com/opencost/opencost/core/pkg/util/cloudutil"
	"github.com/opencost/opencost/core/pkg/util/promutil"
)

// LabelConfig is a port of type AnalyzerConfig. We need to be more thoughtful
// about design at some point, but this is a stop-gap measure, which is required
// because AnalyzerConfig is defined in package main, so it can't be imported.
type LabelConfig struct {
	DepartmentLabel          string `json:"department_label"`
	EnvironmentLabel         string `json:"environment_label"`
	OwnerLabel               string `json:"owner_label"`
	ProductLabel             string `json:"product_label"`
	TeamLabel                string `json:"team_label"`
	ClusterExternalLabel     string `json:"cluster_external_label"`
	NamespaceExternalLabel   string `json:"namespace_external_label"`
	ControllerExternalLabel  string `json:"controller_external_label"`
	DaemonsetExternalLabel   string `json:"daemonset_external_label"`
	DeploymentExternalLabel  string `json:"deployment_external_label"`
	StatefulsetExternalLabel string `json:"statefulset_external_label"`
	ServiceExternalLabel     string `json:"service_external_label"`
	PodExternalLabel         string `json:"pod_external_label"`
	DepartmentExternalLabel  string `json:"department_external_label"`
	EnvironmentExternalLabel string `json:"environment_external_label"`
	OwnerExternalLabel       string `json:"owner_external_label"`
	ProductExternalLabel     string `json:"product_external_label"`
	TeamExternalLabel        string `json:"team_external_label"`
}

// NewLabelConfig creates a new LabelConfig instance with default values.
func NewLabelConfig() *LabelConfig {
	return &LabelConfig{
		DepartmentLabel:          "department",
		EnvironmentLabel:         "env",
		OwnerLabel:               "owner",
		ProductLabel:             "app",
		TeamLabel:                "team",
		ClusterExternalLabel:     "kubernetes_cluster",
		NamespaceExternalLabel:   "kubernetes_namespace",
		ControllerExternalLabel:  "kubernetes_controller",
		DaemonsetExternalLabel:   "kubernetes_daemonset",
		DeploymentExternalLabel:  "kubernetes_deployment",
		StatefulsetExternalLabel: "kubernetes_statefulset",
		ServiceExternalLabel:     "kubernetes_service",
		PodExternalLabel:         "kubernetes_pod",
		DepartmentExternalLabel:  "kubernetes_label_department",
		EnvironmentExternalLabel: "kubernetes_label_env",
		OwnerExternalLabel:       "kubernetes_label_owner",
		ProductExternalLabel:     "kubernetes_label_app",
		TeamExternalLabel:        "kubernetes_label_team",
	}
}

// Map returns the config as a basic string map, with default values if not set
func (lc *LabelConfig) Map() map[string]string {
	// Start with default values
	m := map[string]string{
		"department_label":           "department",
		"environment_label":          "env",
		"owner_label":                "owner",
		"product_label":              "app",
		"team_label":                 "team",
		"cluster_external_label":     "kubernetes_cluster",
		"namespace_external_label":   "kubernetes_namespace",
		"controller_external_label":  "kubernetes_controller",
		"daemonset_external_label":   "kubernetes_daemonset",
		"deployment_external_label":  "kubernetes_deployment",
		"statefulset_external_label": "kubernetes_statefulset",
		"service_external_label":     "kubernetes_service",
		"pod_external_label":         "kubernetes_pod",
		"department_external_label":  "kubernetes_label_department",
		"environment_external_label": "kubernetes_label_env",
		"owner_external_label":       "kubernetes_label_owner",
		"product_external_label":     "kubernetes_label_app",
		"team_external_label":        "kubernetes_label_team",
	}

	if lc == nil {
		return m
	}

	if lc.DepartmentLabel != "" {
		m["department_label"] = lc.DepartmentLabel
	}

	if lc.EnvironmentLabel != "" {
		m["environment_label"] = lc.EnvironmentLabel
	}

	if lc.OwnerLabel != "" {
		m["owner_label"] = lc.OwnerLabel
	}

	if lc.ProductLabel != "" {
		m["product_label"] = lc.ProductLabel
	}

	if lc.TeamLabel != "" {
		m["team_label"] = lc.TeamLabel
	}

	if lc.ClusterExternalLabel != "" {
		m["cluster_external_label"] = lc.ClusterExternalLabel
	}

	if lc.NamespaceExternalLabel != "" {
		m["namespace_external_label"] = lc.NamespaceExternalLabel
	}

	if lc.ControllerExternalLabel != "" {
		m["controller_external_label"] = lc.ControllerExternalLabel
	}

	if lc.DaemonsetExternalLabel != "" {
		m["daemonset_external_label"] = lc.DaemonsetExternalLabel
	}

	if lc.DeploymentExternalLabel != "" {
		m["deployment_external_label"] = lc.DeploymentExternalLabel
	}

	if lc.StatefulsetExternalLabel != "" {
		m["statefulset_external_label"] = lc.StatefulsetExternalLabel
	}

	if lc.ServiceExternalLabel != "" {
		m["service_external_label"] = lc.ServiceExternalLabel
	}

	if lc.PodExternalLabel != "" {
		m["pod_external_label"] = lc.PodExternalLabel
	}

	if lc.DepartmentExternalLabel != "" {
		m["department_external_label"] = lc.DepartmentExternalLabel
	} else if lc.DepartmentLabel != "" {
		m["department_external_label"] = "kubernetes_label_" + lc.DepartmentLabel
	}

	if lc.EnvironmentExternalLabel != "" {
		m["environment_external_label"] = lc.EnvironmentExternalLabel
	} else if lc.EnvironmentLabel != "" {
		m["environment_external_label"] = "kubernetes_label_" + lc.EnvironmentLabel
	}

	if lc.OwnerExternalLabel != "" {
		m["owner_external_label"] = lc.OwnerExternalLabel
	} else if lc.OwnerLabel != "" {
		m["owner_external_label"] = "kubernetes_label_" + lc.OwnerLabel
	}

	if lc.ProductExternalLabel != "" {
		m["product_external_label"] = lc.ProductExternalLabel
	} else if lc.ProductLabel != "" {
		m["product_external_label"] = "kubernetes_label_" + lc.ProductLabel
	}

	if lc.TeamExternalLabel != "" {
		m["team_external_label"] = lc.TeamExternalLabel
	} else if lc.TeamLabel != "" {
		m["team_external_label"] = "kubernetes_label_" + lc.TeamLabel
	}

	return m
}

// Sanitize returns a sanitized version of the given string, which converts
// all illegal characters to underscores. Illegal characters are those that
// Prometheus does not support; i.e. [^a-zA-Z0-9_]
func (lc *LabelConfig) Sanitize(label string) string {
	return promutil.SanitizeLabelName(strings.TrimSpace(label))
}

// GetExternalAllocationName derives an external allocation name from a set of
// labels, given an aggregation property. If the aggregation property is,
// itself, a label (e.g. label:app) then this function looks for a
// corresponding value under "app" and returns "app=thatvalue". If the
// aggregation property is not a label but a Kubernetes concept
// (e.g. namespace) then this function first finds the "external label"
// configured to represent it (e.g. NamespaceExternalLabel: "kubens") and uses
// that label to determine an external allocation name. If no label value can
// be found, return an empty string.
func (lc *LabelConfig) GetExternalAllocationName(labels map[string]string, aggregateBy string) string {
	labelNames := []string{}
	aggByLabel := false

	// Determine if the aggregation property is, itself, a label or not. If
	// not, determine the label associated with the given aggregation property.
	if strings.HasPrefix(aggregateBy, "label:") {
		labelNames = append(labelNames, promutil.SanitizeLabelName(strings.TrimPrefix(aggregateBy, "label:")))
		aggByLabel = true
	} else {
		// If lc is nil, use a default LabelConfig to do a best-effort match
		if lc == nil {
			lc = NewLabelConfig()
		}

		switch strings.ToLower(aggregateBy) {
		case AllocationClusterProp:
			labelNames = strings.Split(lc.ClusterExternalLabel, ",")
		case AllocationControllerProp:
			labelNames = strings.Split(lc.ControllerExternalLabel, ",")
		case AllocationNamespaceProp:
			labelNames = strings.Split(lc.NamespaceExternalLabel, ",")
		case AllocationPodProp:
			labelNames = strings.Split(lc.PodExternalLabel, ",")
		case AllocationServiceProp:
			labelNames = strings.Split(lc.ServiceExternalLabel, ",")
		case AllocationDeploymentProp:
			labelNames = strings.Split(lc.DeploymentExternalLabel, ",")
		case AllocationStatefulSetProp:
			labelNames = strings.Split(lc.StatefulsetExternalLabel, ",")
		case AllocationDaemonSetProp:
			labelNames = strings.Split(lc.DaemonsetExternalLabel, ",")
		case AllocationDepartmentProp:
			labelNames = strings.Split(lc.DepartmentExternalLabel, ",")
		case AllocationEnvironmentProp:
			labelNames = strings.Split(lc.EnvironmentExternalLabel, ",")
		case AllocationOwnerProp:
			labelNames = strings.Split(lc.OwnerExternalLabel, ",")
		case AllocationProductProp:
			labelNames = strings.Split(lc.ProductExternalLabel, ",")
		case AllocationTeamProp:
			labelNames = strings.Split(lc.TeamExternalLabel, ",")
		}

		for i, labelName := range labelNames {
			labelNames[i] = promutil.SanitizeLabelName(strings.TrimSpace(labelName))
		}
	}

	// No label is set for the given aggregation property.
	if len(labelNames) == 0 {
		return ""
	}

	// The relevant label is not present in the set of labels provided.
	labelName := ""
	labelValue := ""
	for _, ln := range labelNames {
		if lv, ok := labels[ln]; ok {
			// Match found for given label
			labelName = ln
			labelValue = lv
			break
		} else {
			// Convert the label name to a format compatible with AWS Glue and
			// Athena column naming and check again. If not found after that,
			// then consider the label not present.
			ln = cloudutil.ConvertToGlueColumnFormat(ln)
			if lv, ok = labels[ln]; ok {
				// Match found for given label after converting to AWS format
				labelName = ln
				labelValue = lv
				break
			}
		}
	}

	// No match found
	if labelName == "" {
		return ""
	}

	// When aggregating by some label (i.e. not by a Kubernetes concept),
	// prepend the label value with the label name (e.g. "app=cost-analyzer").
	// This step is not necessary for Kubernetes concepts (e.g. for namespace,
	// we do not need "namespace=kubecost"; just "kubecost" will do).
	if aggByLabel {
		return fmt.Sprintf("%s=%s", labelName, labelValue)
	}

	return labelValue
}
