package clusters

import "fmt"

// MapToClusterInfo returns a ClusterInfo using parsed data from a string map. If
// parsing the map fails for id and/or name, an error is returned.
func MapToClusterInfo(info map[string]string) (*ClusterInfo, error) {
	var id string
	var name string

	if i, ok := info[ClusterInfoIdKey]; ok {
		id = i
	} else {
		return nil, fmt.Errorf("cluster info missing id")
	}
	if n, ok := info[ClusterInfoNameKey]; ok {
		name = n
	} else {
		name = id
	}

	var clusterProfile string
	var provider string
	var account string
	var project string
	var region string
	var provisioner string

	if cp, ok := info[ClusterInfoProfileKey]; ok {
		clusterProfile = cp
	}

	if pvdr, ok := info[ClusterInfoProviderKey]; ok {
		provider = pvdr
	}

	if acct, ok := info[ClusterInfoAccountKey]; ok {
		account = acct
	}

	if proj, ok := info[ClusterInfoProjectKey]; ok {
		project = proj
	}

	if reg, ok := info[ClusterInfoRegionKey]; ok {
		region = reg
	}

	if pvsr, ok := info[ClusterInfoProvisionerKey]; ok {
		provisioner = pvsr
	}

	return &ClusterInfo{
		ID:          id,
		Name:        name,
		Profile:     clusterProfile,
		Provider:    provider,
		Account:     account,
		Project:     project,
		Region:      region,
		Provisioner: provisioner,
	}, nil
}
