package clustercache

import (
	"sync"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/pkg/config"
	"golang.org/x/exp/slices"
)

// ClusterImporter is an implementation of ClusterCache which leverages a backing configuration file
// as it's source of the cluster data.
type ClusterImporter struct {
	source          *config.ConfigFile
	sourceHandlerID config.HandlerID
	dataLock        *sync.Mutex
	data            *clusterEncoding
}

// Creates a new ClusterCache implementation which uses an import process to provide cluster data
func NewClusterImporter(source *config.ConfigFile) ClusterCache {
	return &ClusterImporter{
		source:   source,
		dataLock: new(sync.Mutex),
		data:     new(clusterEncoding),
	}
}

// onImportSourceChanged handles the source data updating
func (ci *ClusterImporter) onImportSourceChanged(changeType config.ChangeType, data []byte) {
	if changeType == config.ChangeTypeDeleted {
		ci.dataLock.Lock()
		ci.data = new(clusterEncoding)
		ci.dataLock.Unlock()
		return
	}

	ci.update(data)
}

// update replaces the underlying cluster data with the provided new data if it decodes
func (ci *ClusterImporter) update(data []byte) {
	ce := new(clusterEncoding)
	err := json.Unmarshal(data, ce)
	if err != nil {
		log.Warnf("Failed to unmarshal cluster during import: %s", err)
		return
	}

	ci.dataLock.Lock()
	ci.data = ce
	ci.dataLock.Unlock()
}

// Run starts the watcher processes
func (ci *ClusterImporter) Run() {
	if ci.source == nil {
		log.Errorf("ClusterImporter source does not exist, not running")
		return
	}

	exists, err := ci.source.Exists()
	if err != nil {
		log.Errorf("Failed to import source for cluster: %s", err)
		return
	}

	if exists {
		data, err := ci.source.Read()
		if err != nil {
			log.Warnf("Failed to import cluster: %s", err)
		} else {
			ci.update(data)
		}
	}

	ci.sourceHandlerID = ci.source.AddChangeHandler(ci.onImportSourceChanged)
}

// Stops the watcher processes
func (ci *ClusterImporter) Stop() {
	if ci.sourceHandlerID != "" {
		ci.source.RemoveChangeHandler(ci.sourceHandlerID)
		ci.sourceHandlerID = ""
	}
}

// GetAllNamespaces returns all the cached namespaces
func (ci *ClusterImporter) GetAllNamespaces() []*Namespace {
	ci.dataLock.Lock()
	defer ci.dataLock.Unlock()

	return slices.Clone(ci.data.Namespaces)
}

// GetAllNodes returns all the cached nodes
func (ci *ClusterImporter) GetAllNodes() []*Node {
	ci.dataLock.Lock()
	defer ci.dataLock.Unlock()

	return slices.Clone(ci.data.Nodes)
}

// GetAllPods returns all the cached pods
func (ci *ClusterImporter) GetAllPods() []*Pod {
	ci.dataLock.Lock()
	defer ci.dataLock.Unlock()

	return slices.Clone(ci.data.Pods)
}

// GetAllServices returns all the cached services
func (ci *ClusterImporter) GetAllServices() []*Service {
	ci.dataLock.Lock()
	defer ci.dataLock.Unlock()

	return slices.Clone(ci.data.Services)
}

// GetAllDaemonSets returns all the cached DaemonSets
func (ci *ClusterImporter) GetAllDaemonSets() []*DaemonSet {
	ci.dataLock.Lock()
	defer ci.dataLock.Unlock()

	return slices.Clone(ci.data.DaemonSets)
}

// GetAllDeployments returns all the cached deployments
func (ci *ClusterImporter) GetAllDeployments() []*Deployment {
	ci.dataLock.Lock()
	defer ci.dataLock.Unlock()

	return slices.Clone(ci.data.Deployments)
}

// GetAllStatfulSets returns all the cached StatefulSets
func (ci *ClusterImporter) GetAllStatefulSets() []*StatefulSet {
	ci.dataLock.Lock()
	defer ci.dataLock.Unlock()

	return slices.Clone(ci.data.StatefulSets)
}

// GetAllReplicaSets returns all the cached ReplicaSets
func (ci *ClusterImporter) GetAllReplicaSets() []*ReplicaSet {
	ci.dataLock.Lock()
	defer ci.dataLock.Unlock()

	return slices.Clone(ci.data.ReplicaSets)
}

// GetAllPersistentVolumes returns all the cached persistent volumes
func (ci *ClusterImporter) GetAllPersistentVolumes() []*PersistentVolume {
	ci.dataLock.Lock()
	defer ci.dataLock.Unlock()

	return slices.Clone(ci.data.PersistentVolumes)
}

// GetAllPersistentVolumeClaims returns all the cached persistent volume claims
func (ci *ClusterImporter) GetAllPersistentVolumeClaims() []*PersistentVolumeClaim {
	ci.dataLock.Lock()
	defer ci.dataLock.Unlock()

	return slices.Clone(ci.data.PersistentVolumeClaims)
}

// GetAllStorageClasses returns all the cached storage classes
func (ci *ClusterImporter) GetAllStorageClasses() []*StorageClass {
	ci.dataLock.Lock()
	defer ci.dataLock.Unlock()

	return slices.Clone(ci.data.StorageClasses)
}

// GetAllJobs returns all the cached jobs
func (ci *ClusterImporter) GetAllJobs() []*Job {
	ci.dataLock.Lock()
	defer ci.dataLock.Unlock()

	return slices.Clone(ci.data.Jobs)
}

// GetAllPodDisruptionBudgets returns all cached pod disruption budgets
func (ci *ClusterImporter) GetAllPodDisruptionBudgets() []*PodDisruptionBudget {
	ci.dataLock.Lock()
	defer ci.dataLock.Unlock()

	return slices.Clone(ci.data.PodDisruptionBudgets)
}

func (ci *ClusterImporter) GetAllReplicationControllers() []*ReplicationController {
	ci.dataLock.Lock()
	defer ci.dataLock.Unlock()

	return slices.Clone(ci.data.ReplicationControllers)
}
