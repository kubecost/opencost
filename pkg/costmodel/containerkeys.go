package costmodel

import (
	"errors"
	"strings"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/pkg/clustercache"
	"github.com/opencost/opencost/pkg/env"
)

var (
	// Static KeyTuple Errors
	NewKeyTupleErr = errors.New("NewKeyTuple() Provided key not containing exactly 3 components.")

	// Static Errors for ContainerMetric creation
	InvalidKeyErr      error = errors.New("Not a valid key")
	NoContainerErr     error = errors.New("Prometheus vector does not have container name")
	NoContainerNameErr error = errors.New("Prometheus vector does not have string container name")
	NoPodErr           error = errors.New("Prometheus vector does not have pod name")
	NoPodNameErr       error = errors.New("Prometheus vector does not have string pod name")
	NoNamespaceErr     error = errors.New("Prometheus vector does not have namespace")
	NoNamespaceNameErr error = errors.New("Prometheus vector does not have string namespace")
	NoNodeNameErr      error = errors.New("Prometheus vector does not have string node")
	NoClusterIDErr     error = errors.New("Prometheus vector does not have string cluster id")
)

//--------------------------------------------------------------------------
//  KeyTuple
//--------------------------------------------------------------------------

// KeyTuple contains is a utility which parses Namespace, Key, and ClusterID from a
// comma delimitted string.
type KeyTuple struct {
	key    string
	kIndex int
	cIndex int
}

// Namespace returns the the namespace from the string key.
func (kt *KeyTuple) Namespace() string {
	return kt.key[0 : kt.kIndex-1]
}

// Key returns the identifier from the string key.
func (kt *KeyTuple) Key() string {
	return kt.key[kt.kIndex : kt.cIndex-1]
}

// ClusterID returns the cluster identifier from the string key.
func (kt *KeyTuple) ClusterID() string {
	return kt.key[kt.cIndex:]
}

// NewKeyTuple creates a new KeyTuple instance by determining the exact indices of each tuple
// entry. When each component is requested, a string slice is returned using the boundaries.
func NewKeyTuple(key string) (*KeyTuple, error) {
	kIndex := strings.IndexRune(key, ',')
	if kIndex < 0 {
		return nil, NewKeyTupleErr
	}
	kIndex += 1

	subIndex := strings.IndexRune(key[kIndex:], ',')
	if subIndex < 0 {
		return nil, NewKeyTupleErr
	}
	cIndex := kIndex + subIndex + 1

	if strings.ContainsRune(key[cIndex:], ',') {
		return nil, NewKeyTupleErr
	}

	return &KeyTuple{
		key:    key,
		kIndex: kIndex,
		cIndex: cIndex,
	}, nil
}

//--------------------------------------------------------------------------
//  ContainerMetric
//--------------------------------------------------------------------------

// ContainerMetric contains a set of identifiers specific to a kubernetes container including
// a unique string key
type ContainerMetric struct {
	Namespace     string
	PodName       string
	ContainerName string
	NodeName      string
	ClusterID     string
	key           string
}

// Key returns a unique string key that can be used in map[string]interface{}
func (c *ContainerMetric) Key() string {
	return c.key
}

// containerMetricKey creates a unique string key, a comma delimitted list of the provided
// parameters.
func containerMetricKey(ns, podName, containerName, nodeName, clusterID string) string {
	return ns + "," + podName + "," + containerName + "," + nodeName + "," + clusterID
}

// NewContainerMetricFromKey creates a new ContainerMetric instance using a provided comma delimitted
// string key.
func NewContainerMetricFromKey(key string) (*ContainerMetric, error) {
	s := strings.Split(key, ",")
	if len(s) == 5 {
		return &ContainerMetric{
			Namespace:     s[0],
			PodName:       s[1],
			ContainerName: s[2],
			NodeName:      s[3],
			ClusterID:     s[4],
			key:           key,
		}, nil
	}
	return nil, InvalidKeyErr
}

// NewContainerMetricFromValues creates a new ContainerMetric instance using the provided string parameters.
func NewContainerMetricFromValues(ns, podName, containerName, nodeName, clusterId string) *ContainerMetric {
	return &ContainerMetric{
		Namespace:     ns,
		PodName:       podName,
		ContainerName: containerName,
		NodeName:      nodeName,
		ClusterID:     clusterId,
		key:           containerMetricKey(ns, podName, containerName, nodeName, clusterId),
	}
}

// NewContainerMetricsFromPod creates a slice of ContainerMetric instances for each container in the
// provided Pod.
func NewContainerMetricsFromPod(pod *clustercache.Pod, clusterID string) ([]*ContainerMetric, error) {
	podName := pod.Name
	ns := pod.Namespace
	node := pod.Spec.NodeName

	var cs []*ContainerMetric
	for _, container := range pod.Spec.Containers {
		containerName := container.Name
		cs = append(cs, &ContainerMetric{
			Namespace:     ns,
			PodName:       podName,
			ContainerName: containerName,
			NodeName:      node,
			ClusterID:     clusterID,
			key:           containerMetricKey(ns, podName, containerName, node, clusterID),
		})
	}
	return cs, nil
}

// NewContainerMetricFromPrometheus accepts the metrics map from a QueryResult and returns a new ContainerMetric
// instance
func NewContainerMetricFromPrometheus(metrics map[string]interface{}, defaultClusterID string) (*ContainerMetric, error) {
	// TODO: Can we use *prom.QueryResult.GetString() here?
	cName, ok := metrics["container_name"]
	if !ok {
		return nil, NoContainerErr
	}
	containerName, ok := cName.(string)
	if !ok {
		return nil, NoContainerNameErr
	}
	pName, ok := metrics["pod_name"]
	if !ok {
		return nil, NoPodErr
	}
	podName, ok := pName.(string)
	if !ok {
		return nil, NoPodNameErr
	}
	ns, ok := metrics["namespace"]
	if !ok {
		return nil, NoNamespaceErr
	}
	namespace, ok := ns.(string)
	if !ok {
		return nil, NoNamespaceNameErr
	}
	node, ok := metrics["node"]
	if !ok {
		log.Debugf("Prometheus vector does not have node name")
		node = ""
	}
	nodeName, ok := node.(string)
	if !ok {
		return nil, NoNodeNameErr
	}
	cid, ok := metrics[env.GetPromClusterLabel()]
	if !ok {
		log.Debugf("Prometheus vector does not have cluster id")
		cid = defaultClusterID
	}
	clusterID, ok := cid.(string)
	if !ok {
		return nil, NoClusterIDErr
	}

	return &ContainerMetric{
		ContainerName: containerName,
		PodName:       podName,
		Namespace:     namespace,
		NodeName:      nodeName,
		ClusterID:     clusterID,
		key:           containerMetricKey(namespace, podName, containerName, nodeName, clusterID),
	}, nil
}
