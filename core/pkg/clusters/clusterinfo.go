package clusters

// The following constants are used as keys into the cluster info map data structure
const (
	ClusterInfoIdKey               = "id"
	ClusterInfoNameKey             = "name"
	ClusterInfoProviderKey         = "provider"
	ClusterInfoProjectKey          = "project"
	ClusterInfoAccountKey          = "account"
	ClusterInfoRegionKey           = "region"
	ClusterInfoProvisionerKey      = "provisioner"
	ClusterInfoProfileKey          = "clusterProfile"
	ClusterInfoLogCollectionKey    = "logCollection"
	ClusterInfoProductAnalyticsKey = "productAnalytics"
	ClusterInfoErrorReportingKey   = "errorReporting"
	ClusterInfoValuesReportingKey  = "valuesReporting"
	ClusterInfoThanosEnabledKey    = "thanosEnabled"
	ClusterInfoThanosOffsetKey     = "thanosOffset"
	ClusterInfoVersionKey          = "version"
)

// ClusterInfo holds attributes of Cluster from metrics pulled from Prometheus
type ClusterInfo struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Profile     string `json:"profile"`
	Provider    string `json:"provider"`
	Account     string `json:"account"`
	Project     string `json:"project"`
	Region      string `json:"region"`
	Provisioner string `json:"provisioner"`
}

// Clone creates a copy of ClusterInfo and returns it
func (ci *ClusterInfo) Clone() *ClusterInfo {
	if ci == nil {
		return nil
	}

	return &ClusterInfo{
		ID:          ci.ID,
		Name:        ci.Name,
		Profile:     ci.Profile,
		Provider:    ci.Provider,
		Account:     ci.Account,
		Project:     ci.Project,
		Region:      ci.Region,
		Provisioner: ci.Provisioner,
	}
}

type ClusterMap interface {
	// GetClusterIDs returns a slice containing all of the cluster identifiers.
	GetClusterIDs() []string

	// AsMap returns the cluster map as a standard go map
	AsMap() map[string]*ClusterInfo

	// InfoFor returns the ClusterInfo entry for the provided clusterID or nil if it
	// doesn't exist
	InfoFor(clusterID string) *ClusterInfo

	// NameFor returns the name of the cluster provided the clusterID.
	NameFor(clusterID string) string

	// NameIDFor returns an identifier in the format "<clusterName>/<clusterID>" if the cluster has an
	// assigned name. Otherwise, just the clusterID is returned.
	NameIDFor(clusterID string) string
}

// ClusterInfoProvider is a contract which is capable of performing cluster info lookups.
type ClusterInfoProvider interface {
	// GetClusterInfo returns a string map containing the local/remote connected cluster info
	GetClusterInfo() map[string]string
}
