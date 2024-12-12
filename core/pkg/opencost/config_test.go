package opencost

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/util/cloudutil"
)

func TestLabelConfig_Map(t *testing.T) {
	var m map[string]string
	var lc *LabelConfig

	m = lc.Map()
	if len(m) != 18 {
		t.Fatalf("Map: expected length %d; got length %d", 18, len(m))
	}
	if val, ok := m["deployment_external_label"]; !ok || val != "kubernetes_deployment" {
		t.Fatalf("Map: expected %s; got %s", "kubernetes_deployment", val)
	}
	if val, ok := m["namespace_external_label"]; !ok || val != "kubernetes_namespace" {
		t.Fatalf("Map: expected %s; got %s", "kubernetes_namespace", val)
	}

	lc = &LabelConfig{
		DaemonsetExternalLabel: "kubernetes_ds",
	}
	m = lc.Map()
	if len(m) != 18 {
		t.Fatalf("Map: expected length %d; got length %d", 18, len(m))
	}
	if val, ok := m["daemonset_external_label"]; !ok || val != "kubernetes_ds" {
		t.Fatalf("Map: expected %s; got %s", "kubernetes_ds", val)
	}
	if val, ok := m["namespace_external_label"]; !ok || val != "kubernetes_namespace" {
		t.Fatalf("Map: expected %s; got %s", "kubernetes_namespace", val)
	}
}

func TestLabelConfig_GetExternalAllocationName(t *testing.T) {
	// Make sure that AWS's Glue/Athena column formatting is supported
	glueFormattedLabel := cloudutil.ConvertToGlueColumnFormat("___Non_&GlueFormattedLabel___&")

	labels := map[string]string{
		"kubens":                      "kubecost-staging",
		"kubeowner":                   "kubecost-owner",
		"env":                         "env1",
		"app":                         "app1",
		"team":                        "team1",
		glueFormattedLabel:            "glue",
		"prom_sanitization_test":      "pass",
		"kubernetes_cluster":          "cluster-one",
		"kubernetes_namespace":        "kubecost",
		"kubernetes_controller":       "kubecost-controller",
		"kubernetes_daemonset":        "kubecost-daemonset",
		"kubernetes_deployment":       "kubecost-deployment",
		"kubernetes_statefulset":      "kubecost-statefulset",
		"kubernetes_service":          "kubecost-service",
		"kubernetes_pod":              "kubecost-cost-analyzer-abc123",
		"kubernetes_label_department": "kubecost-department",
		"kubernetes_label_env":        "kubecost-env",
		"kubernetes_label_owner":      "kubecost-owner",
		"kubernetes_label_app":        "kubecost-app",
		"kubernetes_label_team":       "kubecost-team",
	}

	testCases := []struct {
		aggBy    string
		expected string
	}{
		{"label:env", "env=env1"},
		{"label:app", "app=app1"},
		{"label:Non__GlueFormattedLabel", "non_glue_formatted_label=glue"},
		{"cluster", "cluster-one"},
		{"namespace", "kubecost"},
		{"controller", "kubecost-controller"},
		{"daemonset", "kubecost-daemonset"},
		{"deployment", "kubecost-deployment"},
		{"statefulset", "kubecost-statefulset"},
		{"service", "kubecost-service"},
		{"pod", "kubecost-cost-analyzer-abc123"},
		{"pod", "kubecost-cost-analyzer-abc123"},
		{"notathing", ""},
		{"", ""},
	}

	var lc *LabelConfig

	// If lc is nil, everything should still work off of defaults.
	for _, tc := range testCases {
		actual := lc.GetExternalAllocationName(labels, tc.aggBy)
		if actual != tc.expected {
			t.Fatalf("GetExternalAllocationName failed; expected '%s'; got '%s'", tc.expected, actual)
		}
	}

	// If lc is default, everything should work, just like the nil.
	lc = NewLabelConfig()
	for _, tc := range testCases {
		actual := lc.GetExternalAllocationName(labels, tc.aggBy)
		if actual != tc.expected {
			t.Fatalf("GetExternalAllocationName failed; expected '%s'; got '%s'", tc.expected, actual)
		}
	}

	// Change the external label for namespace and confirm it still works
	lc.NamespaceExternalLabel = "kubens"
	lc.ServiceExternalLabel = "prom/sanitization-test"
	lc.PodExternalLabel = "Non__GlueFormattedLabel"
	lc.OwnerExternalLabel = "kubeowner"
	lc.DepartmentExternalLabel = "doesntexist,env"
	lc.TeamExternalLabel = "team,env"

	// TODO how is e.g. OwnerExternalLabel supposed to work?

	testCases = []struct {
		aggBy    string
		expected string
	}{
		{"namespace", "kubecost-staging"},
		{"service", "pass"},
		{"pod", "glue"},
		{"owner", "kubecost-owner"},
		{"department", "env1"},
		{"team", "team1"},
	}
	for _, tc := range testCases {
		actual := lc.GetExternalAllocationName(labels, tc.aggBy)
		if actual != tc.expected {
			t.Fatalf("GetExternalAllocationName failed; expected '%s'; got '%s'", tc.expected, actual)
		}
	}
}

func TestLabelConfig_Sanitize(t *testing.T) {
	testCases := []struct {
		label    string
		expected string
	}{
		{"", ""},
		{"simple", "simple"},
		{"prom/sanitization-test", "prom_sanitization_test"},
		{" prom/sanitization-test$  ", "prom_sanitization_test_"},
	}

	lc := NewLabelConfig()
	for _, tc := range testCases {
		actual := lc.Sanitize(tc.label)
		if actual != tc.expected {
			t.Fatalf("Sanitize failed; expected '%s'; got '%s'", tc.expected, actual)
		}
	}
}
