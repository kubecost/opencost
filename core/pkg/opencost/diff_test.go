package opencost

import (
	"reflect"
	"testing"
	"time"
)

func TestDiff(t *testing.T) {

	start := time.Now().AddDate(0, 0, -1)
	end := time.Now()
	window1 := NewWindow(&start, &end)

	node1 := NewNode("node1", "cluster1", "123abc", start, end, window1)
	node1.CPUCost = 10
	node1b := node1.Clone().(*Node)
	node1b.CPUCost = 20
	node1Key, _ := key(node1, nil, nil)
	node2 := NewNode("node2", "cluster1", "123abc", start, end, window1)
	node2.CPUCost = 100
	node2b := node2.Clone().(*Node)
	node2b.CPUCost = 105
	node2Key, _ := key(node2, nil, nil)
	node3 := NewNode("node3", "cluster1", "123abc", start, end, window1)
	node3Key, _ := key(node3, nil, nil)
	node4 := NewNode("node4", "cluster1", "123abc", start, end, window1)
	node4Key, _ := key(node4, nil, nil)
	disk1 := NewDisk("disk1", "cluster1", "123abc", start, end, window1)
	disk1Key, _ := key(disk1, nil, nil)
	disk2 := NewDisk("disk2", "cluster1", "123abc", start, end, window1)
	disk2Key, _ := key(disk2, nil, nil)

	cases := map[string]struct {
		inputAssetsBefore []Asset
		inputAssetsAfter  []Asset
		costChangeRatio   float64
		expected          map[string]Diff[Asset]
	}{
		"added node": {
			inputAssetsBefore: []Asset{node1, node2},
			inputAssetsAfter:  []Asset{node1, node2, node3},
			expected:          map[string]Diff[Asset]{node3Key: {nil, node3, DiffAdded}},
		},
		"multiple adds": {
			inputAssetsBefore: []Asset{node1, node2},
			inputAssetsAfter:  []Asset{node1, node2, node3, node4},
			expected:          map[string]Diff[Asset]{node3Key: {nil, node3, DiffAdded}, node4Key: {nil, node4, DiffAdded}},
		},
		"removed node": {
			inputAssetsBefore: []Asset{node1, node2},
			inputAssetsAfter:  []Asset{node2},
			expected:          map[string]Diff[Asset]{node1Key: {node1, nil, DiffRemoved}},
		},
		"multiple removes": {
			inputAssetsBefore: []Asset{node1, node2, node3},
			inputAssetsAfter:  []Asset{node2},
			expected:          map[string]Diff[Asset]{node1Key: {node1, nil, DiffRemoved}, node3Key: {node3, nil, DiffRemoved}},
		},
		"remove all": {
			inputAssetsBefore: []Asset{node1, node2},
			inputAssetsAfter:  []Asset{},
			expected:          map[string]Diff[Asset]{node1Key: {node1, nil, DiffRemoved}, node2Key: {node2, nil, DiffRemoved}},
		},
		"add and remove": {
			inputAssetsBefore: []Asset{node1, node2},
			inputAssetsAfter:  []Asset{node2, node3},
			expected:          map[string]Diff[Asset]{node1Key: {node1, nil, DiffRemoved}, node3Key: {nil, node3, DiffAdded}},
		},
		"no change": {
			inputAssetsBefore: []Asset{node1, node2},
			inputAssetsAfter:  []Asset{node1, node2},
			expected:          map[string]Diff[Asset]{},
		},
		"order switch": {
			inputAssetsBefore: []Asset{node2, node1},
			inputAssetsAfter:  []Asset{node1, node2},
			expected:          map[string]Diff[Asset]{},
		},
		"disk add": {
			inputAssetsBefore: []Asset{disk1, node1},
			inputAssetsAfter:  []Asset{disk1, node1, disk2},
			expected:          map[string]Diff[Asset]{disk2Key: {nil, disk2, DiffAdded}},
		},
		"disk and node add": {
			inputAssetsBefore: []Asset{disk1, node1},
			inputAssetsAfter:  []Asset{disk1, node1, disk2, node2},
			expected:          map[string]Diff[Asset]{disk2Key: {nil, disk2, DiffAdded}, node2Key: {nil, node2, DiffAdded}},
		},
		"disk and node removed": {
			inputAssetsBefore: []Asset{disk1, node1, disk2, node2},
			inputAssetsAfter:  []Asset{disk2, node2},
			expected:          map[string]Diff[Asset]{disk1Key: {disk1, nil, DiffRemoved}, node1Key: {node1, nil, DiffRemoved}},
		},
		"cost change more than 10%": {
			inputAssetsBefore: []Asset{node1},
			inputAssetsAfter:  []Asset{node1b},
			costChangeRatio:   0.1,
			expected:          map[string]Diff[Asset]{node1Key: {node1, node1b, DiffChanged}},
		},
		"cost change less than 10%": {
			inputAssetsBefore: []Asset{node2},
			inputAssetsAfter:  []Asset{node2b},
			costChangeRatio:   0.1,
			expected:          map[string]Diff[Asset]{},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			as1 := NewAssetSet(start, end, tc.inputAssetsBefore...)

			as2 := NewAssetSet(start, end, tc.inputAssetsAfter...)

			result, err := DiffAsset(as1.Clone(), as2.Clone(), tc.costChangeRatio)

			if err != nil {
				t.Fatalf("error; got %s", err)
			}

			if !reflect.DeepEqual(result, tc.expected) {
				t.Fatalf("expected %+v; got %+v", tc.expected, result)
			}

		})
	}

}
