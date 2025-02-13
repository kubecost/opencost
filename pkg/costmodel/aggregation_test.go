package costmodel

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/opencost"
)

func TestParseAggregationProperties_Default(t *testing.T) {
	got, err := ParseAggregationProperties([]string{})
	expected := []string{
		opencost.AllocationClusterProp,
		opencost.AllocationNodeProp,
		opencost.AllocationNamespaceProp,
		opencost.AllocationPodProp,
		opencost.AllocationContainerProp,
	}

	if err != nil {
		t.Fatalf("TestParseAggregationPropertiesDefault: unexpected error: %s", err)
	}

	if len(expected) != len(got) {
		t.Fatalf("TestParseAggregationPropertiesDefault: expected length of %d, got: %d", len(expected), len(got))
	}

	for i := range got {
		if got[i] != expected[i] {
			t.Fatalf("TestParseAggregationPropertiesDefault: expected[i] should be %s, got[i]:%s", expected[i], got[i])
		}
	}
}

func TestParseAggregationProperties_All(t *testing.T) {
	got, err := ParseAggregationProperties([]string{"all"})

	if err != nil {
		t.Fatalf("TestParseAggregationPropertiesDefault: unexpected error: %s", err)
	}

	if len(got) != 0 {
		t.Fatalf("TestParseAggregationPropertiesDefault: expected length of 0, got: %d", len(got))
	}
}
