package promutil

import (
	"fmt"
	"reflect"
	"testing"
)

func checkSlice(s1, s2 []string) error {
	if len(s1) != len(s2) {
		return fmt.Errorf("len(s1) [%d] != len(s2) [%d]", len(s1), len(s2))
	}

	for i := 0; i < len(s1); i++ {
		if s1[i] != s2[i] {
			return fmt.Errorf("At Index: %d. Different Values %s (s1) != %s (s2)", i, s1[i], s2[i])
		}
	}
	return nil
}

func TestEmptyKubeLabelsToPromLabels(t *testing.T) {
	labels, values := KubeLabelsToLabels(nil)

	if len(labels) != 0 {
		t.Errorf("Labels length is non-zero\n")
	}
	if len(values) != 0 {
		t.Errorf("Values length is non-zero\n")
	}

	labels, values = KubeLabelsToLabels(map[string]string{})

	if len(labels) != 0 {
		t.Errorf("Labels length is non-zero\n")
	}
	if len(values) != 0 {
		t.Errorf("Values length is non-zero\n")
	}
}

func TestKubeLabelsToPromLabels(t *testing.T) {
	var expectedLabels []string = []string{
		"label_app",
		"label_chart",
		"label_control_plane",
		"label_gatekeeper_sh_operation",
		"label_heritage",
		"label_pod_template_hash",
		"label_release",
	}
	var expectedValues []string = []string{
		"gatekeeper",
		"gatekeeper",
		"audit-controller",
		"audit",
		"Helm",
		"5599859cd4",
		"gatekeeper",
	}

	kubeLabels := map[string]string{
		"app":                     "gatekeeper",
		"chart":                   "gatekeeper",
		"control-plane":           "audit-controller",
		"gatekeeper.sh/operation": "audit",
		"heritage":                "Helm",
		"pod-template-hash":       "5599859cd4",
		"release":                 "gatekeeper",
	}

	labels, values := KubePrependQualifierToLabels(kubeLabels, "label_")
	l2, v2 := KubeLabelsToLabels(kubeLabels)

	// Check to make sure we get expected labels and values returned
	err := checkSlice(labels, expectedLabels)
	if err != nil {
		t.Errorf("%s", err)
	}
	err = checkSlice(values, expectedValues)
	if err != nil {
		t.Errorf("%s", err)
	}

	// Check to make sure the helper function returns what the prependqualifier func
	// returns
	err = checkSlice(l2, labels)
	if err != nil {
		t.Errorf("%s", err)
	}

	err = checkSlice(v2, values)
	if err != nil {
		t.Errorf("%s", err)
	}
}

func TestSanitizeLabels(t *testing.T) {
	type testCase struct {
		in  map[string]string
		exp map[string]string
	}

	tcs := map[string]testCase{
		"empty labels": {
			in:  map[string]string{},
			exp: map[string]string{},
		},
		"no op": {
			in: map[string]string{
				"foo": "bar",
				"baz": "loo",
			},
			exp: map[string]string{
				"foo": "bar",
				"baz": "loo",
			},
		},
		"modification, no collisions": {
			in: map[string]string{
				"foo-foo":   "bar",
				"baz---baz": "loo",
			},
			exp: map[string]string{
				"foo_foo":   "bar",
				"baz___baz": "loo",
			},
		},
		"modification, one collision": {
			in: map[string]string{
				"foo-foo":   "bar",
				"foo+foo":   "bar",
				"baz---baz": "loo",
			},
			exp: map[string]string{
				"foo_foo":   "bar",
				"baz___baz": "loo",
			},
		},
		"modification, all collisions": {
			in: map[string]string{
				"foo-foo": "bar",
				"foo+foo": "bar",
				"foo_foo": "bar",
			},
			exp: map[string]string{
				"foo_foo": "bar",
			},
		},
	}

	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			act := SanitizeLabels(tc.in)
			if !reflect.DeepEqual(tc.exp, act) {
				t.Errorf("sanitizing labels failed for case %s: %+v != %+v", name, tc.exp, act)
			}
		})
	}
}
