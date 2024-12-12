package costmodel

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_filterOutLocalPVs(t *testing.T) {
	testCases := []struct {
		name     string
		input    map[DiskIdentifier]*Disk
		expected map[DiskIdentifier]*Disk
	}{
		{
			name: "Filter out local PVs",
			input: map[DiskIdentifier]*Disk{
				{Cluster: "cluster1", Name: "pv1"}:              &Disk{Name: "pv1"},
				{Cluster: "cluster1", Name: "local-pv-123"}:     &Disk{Name: "local-pv-123"},
				{Cluster: "cluster2", Name: "pv2"}:              &Disk{Name: "pv2"},
				{Cluster: "cluster2", Name: "local-pv-456"}:     &Disk{Name: "local-pv-456"},
				{Cluster: "cluster3", Name: "not-local-pv-789"}: &Disk{Name: "not-local-pv-789"},
			},
			expected: map[DiskIdentifier]*Disk{
				{Cluster: "cluster1", Name: "pv1"}:              &Disk{Name: "pv1"},
				{Cluster: "cluster2", Name: "pv2"}:              &Disk{Name: "pv2"},
				{Cluster: "cluster3", Name: "not-local-pv-789"}: &Disk{Name: "not-local-pv-789"},
			},
		},
		{
			name: "No local PVs to filter",
			input: map[DiskIdentifier]*Disk{
				{Cluster: "cluster1", Name: "pv1"}: &Disk{Name: "pv1"},
				{Cluster: "cluster2", Name: "pv2"}: &Disk{Name: "pv2"},
			},
			expected: map[DiskIdentifier]*Disk{
				{Cluster: "cluster1", Name: "pv1"}: &Disk{Name: "pv1"},
				{Cluster: "cluster2", Name: "pv2"}: &Disk{Name: "pv2"},
			},
		},
		{
			name:     "Empty input",
			input:    map[DiskIdentifier]*Disk{},
			expected: map[DiskIdentifier]*Disk{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := filterOutLocalPVs(tc.input)
			assert.Equal(t, tc.expected, result)
		})
	}
}
