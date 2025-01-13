package clustercache

import (
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util/atomic"
	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/pkg/config"
)

// clusterEncoding is used to represent the cluster objects in the encoded states.
type clusterEncoding struct {
	Namespaces             []*Namespace             `json:"namespaces,omitempty"`
	Nodes                  []*Node                  `json:"nodes,omitempty"`
	Pods                   []*Pod                   `json:"pods,omitempty"`
	Services               []*Service               `json:"services,omitempty"`
	DaemonSets             []*DaemonSet             `json:"daemonSets,omitempty"`
	Deployments            []*Deployment            `json:"deployments,omitempty"`
	StatefulSets           []*StatefulSet           `json:"statefulSets,omitempty"`
	ReplicaSets            []*ReplicaSet            `json:"replicaSets,omitempty"`
	PersistentVolumes      []*PersistentVolume      `json:"persistentVolumes,omitempty"`
	PersistentVolumeClaims []*PersistentVolumeClaim `json:"persistentVolumeClaims,omitempty"`
	StorageClasses         []*StorageClass          `json:"storageClasses,omitempty"`
	Jobs                   []*Job                   `json:"jobs,omitempty"`
	PodDisruptionBudgets   []*PodDisruptionBudget   `json:"podDisruptionBudgets,omitempty"`
	ReplicationControllers []*ReplicationController `json:"replicationController,omitempty"`
}

// ClusterExporter manages and runs an file export process which dumps the local kubernetes cluster to a target location.
type ClusterExporter struct {
	cluster  ClusterCache
	target   *config.ConfigFile
	interval time.Duration
	runState atomic.AtomicRunState
}

// NewClusterExporter creates a new ClusterExporter instance for exporting the kubernetes cluster.
func NewClusterExporter(cluster ClusterCache, target *config.ConfigFile, interval time.Duration) *ClusterExporter {
	return &ClusterExporter{
		cluster:  cluster,
		target:   target,
		interval: interval,
	}
}

// Run starts the automated process of running Export on a specific interval.
func (ce *ClusterExporter) Run() {
	// in the event there is a race that occurs between Run() and Stop(), we
	// ensure that we wait for the reset to occur before starting again
	ce.runState.WaitForReset()

	if !ce.runState.Start() {
		log.Warnf("ClusterExporter already running")
		return
	}

	go func() {
		for {
			err := ce.Export()
			if err != nil {
				log.Warnf("Failed to export cluster: %s", err)
			}

			select {
			case <-time.After(ce.interval):
			case <-ce.runState.OnStop():
				ce.runState.Reset()
				return
			}
		}
	}()
}

// Stop halts the Cluster export on an interval
func (ce *ClusterExporter) Stop() {
	ce.runState.Stop()
}

// Export stores the cluster cache data into a PODO, marshals as JSON, and saves it to the
// target location.
func (ce *ClusterExporter) Export() error {
	c := ce.cluster
	encoding := &clusterEncoding{
		Namespaces:             c.GetAllNamespaces(),
		Nodes:                  c.GetAllNodes(),
		Pods:                   c.GetAllPods(),
		Services:               c.GetAllServices(),
		DaemonSets:             c.GetAllDaemonSets(),
		Deployments:            c.GetAllDeployments(),
		StatefulSets:           c.GetAllStatefulSets(),
		ReplicaSets:            c.GetAllReplicaSets(),
		PersistentVolumes:      c.GetAllPersistentVolumes(),
		PersistentVolumeClaims: c.GetAllPersistentVolumeClaims(),
		StorageClasses:         c.GetAllStorageClasses(),
		Jobs:                   c.GetAllJobs(),
		PodDisruptionBudgets:   c.GetAllPodDisruptionBudgets(),
		ReplicationControllers: c.GetAllReplicationControllers(),
	}

	data, err := json.Marshal(encoding)
	if err != nil {
		return err
	}

	return ce.target.Write(data)
}
