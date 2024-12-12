package costmodel

import (
	"fmt"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/pkg/env"
	"github.com/opencost/opencost/pkg/prom"
)

type containerKey struct {
	Cluster   string
	Namespace string
	Pod       string
	Container string
}

func (k containerKey) String() string {
	return fmt.Sprintf("%s/%s/%s/%s", k.Cluster, k.Namespace, k.Pod, k.Container)
}

func newContainerKey(cluster, namespace, pod, container string) containerKey {
	return containerKey{
		Cluster:   cluster,
		Namespace: namespace,
		Pod:       pod,
		Container: container,
	}
}

// resultContainerKey converts a Prometheus query result to a containerKey by
// looking up values associated with the given label names. For example,
// passing "cluster_id" for clusterLabel will use the value of the label
// "cluster_id" as the containerKey's Cluster field. If a given field does not
// exist on the result, an error is returned. (The only exception to that is
// clusterLabel, which we expect may not exist, but has a default value.)
func resultContainerKey(res *prom.QueryResult, clusterLabel, namespaceLabel, podLabel, containerLabel string) (containerKey, error) {
	key := containerKey{}

	cluster, err := res.GetString(clusterLabel)
	if err != nil {
		cluster = env.GetClusterID()
	}
	key.Cluster = cluster

	namespace, err := res.GetString(namespaceLabel)
	if err != nil {
		return key, err
	}
	key.Namespace = namespace

	pod, err := res.GetString(podLabel)
	if err != nil {
		return key, err
	}
	key.Pod = pod

	container, err := res.GetString(containerLabel)
	if err != nil {
		return key, err
	}
	key.Container = container

	return key, nil
}

type podKey struct {
	namespaceKey
	Pod string
}

func (k podKey) String() string {
	return fmt.Sprintf("%s/%s/%s", k.Cluster, k.Namespace, k.Pod)
}

func newPodKey(cluster, namespace, pod string) podKey {
	return podKey{
		namespaceKey: namespaceKey{
			Cluster:   cluster,
			Namespace: namespace,
		},
		Pod: pod,
	}
}

// getUnmountedPodKey while certain Unmounted costs can have a namespace, all unmounted costs for a single cluster will be represented by the same asset
func getUnmountedPodKey(cluster string) podKey {
	return newPodKey(cluster, opencost.UnmountedSuffix, opencost.UnmountedSuffix)
}

// resultPodKey converts a Prometheus query result to a podKey by looking
// up values associated with the given label names. For example, passing
// "cluster_id" for clusterLabel will use the value of the label "cluster_id"
// as the podKey's Cluster field. If a given field does not exist on the
// result, an error is returned. (The only exception to that is clusterLabel,
// which we expect may not exist, but has a default value.)
func resultPodKey(res *prom.QueryResult, clusterLabel, namespaceLabel string) (podKey, error) {
	key := podKey{}

	cluster, err := res.GetString(clusterLabel)
	if err != nil {
		cluster = env.GetClusterID()
	}
	key.Cluster = cluster

	namespace, err := res.GetString(namespaceLabel)
	if err != nil {
		return key, err
	}
	key.Namespace = namespace

	pod, err := res.GetString("pod")
	if pod == "" || err != nil {
		pod, err = res.GetString("pod_name")
		if err != nil {
			return key, err
		}
	}
	key.Pod = pod

	return key, nil
}

type namespaceKey struct {
	Cluster   string
	Namespace string
}

func (k namespaceKey) String() string {
	return fmt.Sprintf("%s/%s", k.Cluster, k.Namespace)
}

func newNamespaceKey(cluster, namespace string) namespaceKey {
	return namespaceKey{
		Cluster:   cluster,
		Namespace: namespace,
	}
}

// resultNamespaceKey converts a Prometheus query result to a namespaceKey by
// looking up values associated with the given label names. For example,
// passing "cluster_id" for clusterLabel will use the value of the label
// "cluster_id" as the namespaceKey's Cluster field. If a given field does not
// exist on the result, an error is returned. (The only exception to that is
// clusterLabel, which we expect may not exist, but has a default value.)
func resultNamespaceKey(res *prom.QueryResult, clusterLabel, namespaceLabel string) (namespaceKey, error) {
	key := namespaceKey{}

	cluster, err := res.GetString(clusterLabel)
	if err != nil {
		cluster = env.GetClusterID()
	}
	key.Cluster = cluster

	namespace, err := res.GetString(namespaceLabel)
	if err != nil {
		return key, err
	}
	key.Namespace = namespace

	return key, nil
}

type controllerKey struct {
	Cluster        string
	Namespace      string
	ControllerKind string
	Controller     string
}

func (k controllerKey) String() string {
	return fmt.Sprintf("%s/%s/%s/%s", k.Cluster, k.Namespace, k.ControllerKind, k.Controller)
}

func newControllerKey(cluster, namespace, controllerKind, controller string) controllerKey {
	return controllerKey{
		Cluster:        cluster,
		Namespace:      namespace,
		ControllerKind: controllerKind,
		Controller:     controller,
	}
}

// resultControllerKey converts a Prometheus query result to a controllerKey by
// looking up values associated with the given label names. For example,
// passing "cluster_id" for clusterLabel will use the value of the label
// "cluster_id" as the controllerKey's Cluster field. If a given field does not
// exist on the result, an error is returned. (The only exception to that is
// clusterLabel, which we expect may not exist, but has a default value.)
func resultControllerKey(controllerKind string, res *prom.QueryResult, clusterLabel, namespaceLabel, controllerLabel string) (controllerKey, error) {
	key := controllerKey{}

	cluster, err := res.GetString(clusterLabel)
	if err != nil {
		cluster = env.GetClusterID()
	}
	key.Cluster = cluster

	namespace, err := res.GetString(namespaceLabel)
	if err != nil {
		return key, err
	}
	key.Namespace = namespace

	controller, err := res.GetString(controllerLabel)
	if err != nil {
		return key, err
	}
	key.Controller = controller

	key.ControllerKind = controllerKind

	return key, nil
}

// resultDeploymentKey creates a controllerKey for a Deployment.
// (See resultControllerKey for more.)
func resultDeploymentKey(res *prom.QueryResult, clusterLabel, namespaceLabel, controllerLabel string) (controllerKey, error) {
	return resultControllerKey("deployment", res, clusterLabel, namespaceLabel, controllerLabel)
}

// resultStatefulSetKey creates a controllerKey for a StatefulSet.
// (See resultControllerKey for more.)
func resultStatefulSetKey(res *prom.QueryResult, clusterLabel, namespaceLabel, controllerLabel string) (controllerKey, error) {
	return resultControllerKey("statefulset", res, clusterLabel, namespaceLabel, controllerLabel)
}

// resultDaemonSetKey creates a controllerKey for a DaemonSet.
// (See resultControllerKey for more.)
func resultDaemonSetKey(res *prom.QueryResult, clusterLabel, namespaceLabel, controllerLabel string) (controllerKey, error) {
	return resultControllerKey("daemonset", res, clusterLabel, namespaceLabel, controllerLabel)
}

// resultJobKey creates a controllerKey for a Job.
// (See resultControllerKey for more.)
func resultJobKey(res *prom.QueryResult, clusterLabel, namespaceLabel, controllerLabel string) (controllerKey, error) {
	return resultControllerKey("job", res, clusterLabel, namespaceLabel, controllerLabel)
}

// resultReplicaSetKey creates a controllerKey for a Job.
// (See resultControllerKey for more.)
func resultReplicaSetKey(res *prom.QueryResult, clusterLabel, namespaceLabel, controllerLabel string) (controllerKey, error) {
	return resultControllerKey("replicaset", res, clusterLabel, namespaceLabel, controllerLabel)
}

// resultReplicaSetRolloutKey creates a controllerKey for a Job.
// (See resultControllerKey for more.)
func resultReplicaSetRolloutKey(res *prom.QueryResult, clusterLabel, namespaceLabel, controllerLabel string) (controllerKey, error) {
	return resultControllerKey("rollout", res, clusterLabel, namespaceLabel, controllerLabel)
}

type serviceKey struct {
	Cluster   string
	Namespace string
	Service   string
}

func (k serviceKey) String() string {
	return fmt.Sprintf("%s/%s/%s", k.Cluster, k.Namespace, k.Service)
}

func newServiceKey(cluster, namespace, service string) serviceKey {
	return serviceKey{
		Cluster:   cluster,
		Namespace: namespace,
		Service:   service,
	}
}

// resultServiceKey converts a Prometheus query result to a serviceKey by
// looking up values associated with the given label names. For example,
// passing "cluster_id" for clusterLabel will use the value of the label
// "cluster_id" as the serviceKey's Cluster field. If a given field does not
// exist on the result, an error is returned. (The only exception to that is
// clusterLabel, which we expect may not exist, but has a default value.)
func resultServiceKey(res *prom.QueryResult, clusterLabel, namespaceLabel, serviceLabel string) (serviceKey, error) {
	key := serviceKey{}

	cluster, err := res.GetString(clusterLabel)
	if err != nil {
		cluster = env.GetClusterID()
	}
	key.Cluster = cluster

	namespace, err := res.GetString(namespaceLabel)
	if err != nil {
		return key, err
	}
	key.Namespace = namespace

	service, err := res.GetString(serviceLabel)
	if err != nil {
		return key, err
	}
	key.Service = service

	return key, nil
}

type nodeKey struct {
	Cluster string
	Node    string
}

func (k nodeKey) String() string {
	return fmt.Sprintf("%s/%s", k.Cluster, k.Node)
}

func newNodeKey(cluster, node string) nodeKey {
	return nodeKey{
		Cluster: cluster,
		Node:    node,
	}
}

// resultNodeKey converts a Prometheus query result to a nodeKey by
// looking up values associated with the given label names. For example,
// passing "cluster_id" for clusterLabel will use the value of the label
// "cluster_id" as the nodeKey's Cluster field. If a given field does not
// exist on the result, an error is returned. (The only exception to that is
// clusterLabel, which we expect may not exist, but has a default value.)
func resultNodeKey(res *prom.QueryResult, clusterLabel, nodeLabel string) (nodeKey, error) {
	key := nodeKey{}

	cluster, err := res.GetString(clusterLabel)
	if err != nil {
		cluster = env.GetClusterID()
	}
	key.Cluster = cluster

	node, err := res.GetString(nodeLabel)
	if err != nil {
		return key, err
	}
	key.Node = node

	return key, nil
}

type pvcKey struct {
	Cluster               string
	Namespace             string
	PersistentVolumeClaim string
}

func (k pvcKey) String() string {
	return fmt.Sprintf("%s/%s/%s", k.Cluster, k.Namespace, k.PersistentVolumeClaim)
}

func newPVCKey(cluster, namespace, persistentVolumeClaim string) pvcKey {
	return pvcKey{
		Cluster:               cluster,
		Namespace:             namespace,
		PersistentVolumeClaim: persistentVolumeClaim,
	}
}

// resultPVCKey converts a Prometheus query result to a pvcKey by
// looking up values associated with the given label names. For example,
// passing "cluster_id" for clusterLabel will use the value of the label
// "cluster_id" as the pvcKey's Cluster field. If a given field does not
// exist on the result, an error is returned. (The only exception to that is
// clusterLabel, which we expect may not exist, but has a default value.)
func resultPVCKey(res *prom.QueryResult, clusterLabel, namespaceLabel, pvcLabel string) (pvcKey, error) {
	key := pvcKey{}

	cluster, err := res.GetString(clusterLabel)
	if err != nil {
		cluster = env.GetClusterID()
	}
	key.Cluster = cluster

	namespace, err := res.GetString(namespaceLabel)
	if err != nil {
		return key, err
	}
	key.Namespace = namespace

	pvc, err := res.GetString(pvcLabel)
	if err != nil {
		return key, err
	}
	key.PersistentVolumeClaim = pvc

	return key, nil
}

type pvKey struct {
	Cluster          string
	PersistentVolume string
}

func (k pvKey) String() string {
	return fmt.Sprintf("%s/%s", k.Cluster, k.PersistentVolume)
}

func newPVKey(cluster, persistentVolume string) pvKey {
	return pvKey{
		Cluster:          cluster,
		PersistentVolume: persistentVolume,
	}
}

// resultPVKey converts a Prometheus query result to a pvKey by
// looking up values associated with the given label names. For example,
// passing "cluster_id" for clusterLabel will use the value of the label
// "cluster_id" as the pvKey's Cluster field. If a given field does not
// exist on the result, an error is returned. (The only exception to that is
// clusterLabel, which we expect may not exist, but has a default value.)
func resultPVKey(res *prom.QueryResult, clusterLabel, persistentVolumeLabel string) (pvKey, error) {
	key := pvKey{}

	cluster, err := res.GetString(clusterLabel)
	if err != nil {
		cluster = env.GetClusterID()
	}
	key.Cluster = cluster

	persistentVolume, err := res.GetString(persistentVolumeLabel)
	if err != nil {
		return key, err
	}
	key.PersistentVolume = persistentVolume

	return key, nil
}
