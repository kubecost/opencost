package clustercache

import (
	"context"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	stv1 "k8s.io/api/storage/v1"
	"k8s.io/client-go/kubernetes"
)

type KubernetesClusterCacheV2 struct {
	namespaceStore             *GenericStore[*v1.Namespace, *Namespace]
	nodeStore                  *GenericStore[*v1.Node, *Node]
	podStore                   *GenericStore[*v1.Pod, *Pod]
	serviceStore               *GenericStore[*v1.Service, *Service]
	daemonSetStore             *GenericStore[*appsv1.DaemonSet, *DaemonSet]
	deploymentStore            *GenericStore[*appsv1.Deployment, *Deployment]
	statefulSetStore           *GenericStore[*appsv1.StatefulSet, *StatefulSet]
	persistentVolumeStore      *GenericStore[*v1.PersistentVolume, *PersistentVolume]
	persistentVolumeClaimStore *GenericStore[*v1.PersistentVolumeClaim, *PersistentVolumeClaim]
	storageClassStore          *GenericStore[*stv1.StorageClass, *StorageClass]
	jobStore                   *GenericStore[*batchv1.Job, *Job]
	replicationControllerStore *GenericStore[*v1.ReplicationController, *ReplicationController]
	replicaSetStore            *GenericStore[*appsv1.ReplicaSet, *ReplicaSet]
	pdbStore                   *GenericStore[*policyv1.PodDisruptionBudget, *PodDisruptionBudget]
}

func NewKubernetesClusterCacheV2(clientset kubernetes.Interface) *KubernetesClusterCacheV2 {
	ctx := context.TODO()
	return &KubernetesClusterCacheV2{
		namespaceStore:             CreateStoreAndWatch(ctx, clientset.CoreV1().RESTClient(), "namespaces", transformNamespace),
		nodeStore:                  CreateStoreAndWatch(ctx, clientset.CoreV1().RESTClient(), "nodes", transformNode),
		persistentVolumeClaimStore: CreateStoreAndWatch(ctx, clientset.CoreV1().RESTClient(), "persistentvolumeclaims", transformPersistentVolumeClaim),
		persistentVolumeStore:      CreateStoreAndWatch(ctx, clientset.CoreV1().RESTClient(), "persistentvolumes", transformPersistentVolume),
		podStore:                   CreateStoreAndWatch(ctx, clientset.CoreV1().RESTClient(), "pods", transformPod),
		replicationControllerStore: CreateStoreAndWatch(ctx, clientset.CoreV1().RESTClient(), "replicationcontrollers", transformReplicationController),
		serviceStore:               CreateStoreAndWatch(ctx, clientset.CoreV1().RESTClient(), "services", transformService),
		daemonSetStore:             CreateStoreAndWatch(ctx, clientset.AppsV1().RESTClient(), "daemonsets", transformDaemonSet),
		deploymentStore:            CreateStoreAndWatch(ctx, clientset.AppsV1().RESTClient(), "deployments", transformDeployment),
		replicaSetStore:            CreateStoreAndWatch(ctx, clientset.AppsV1().RESTClient(), "replicasets", transformReplicaSet),
		statefulSetStore:           CreateStoreAndWatch(ctx, clientset.AppsV1().RESTClient(), "statefulsets", transformStatefulSet),
		storageClassStore:          CreateStoreAndWatch(ctx, clientset.StorageV1().RESTClient(), "storageclasses", transformStorageClass),
		jobStore:                   CreateStoreAndWatch(ctx, clientset.BatchV1().RESTClient(), "jobs", transformJob),
		pdbStore:                   CreateStoreAndWatch(ctx, clientset.PolicyV1().RESTClient(), "poddisruptionbudgets", transformPodDisruptionBudget),
	}
}

func (kcc *KubernetesClusterCacheV2) Run() {
}

func (kcc *KubernetesClusterCacheV2) Stop() {
}

func (kcc *KubernetesClusterCacheV2) GetAllNamespaces() []*Namespace {
	return kcc.namespaceStore.GetAll()
}

func (kcc *KubernetesClusterCacheV2) GetAllNodes() []*Node {
	return kcc.nodeStore.GetAll()
}

func (kcc *KubernetesClusterCacheV2) GetAllPods() []*Pod {
	return kcc.podStore.GetAll()
}

func (kcc *KubernetesClusterCacheV2) GetAllServices() []*Service {
	return kcc.serviceStore.GetAll()
}

func (kcc *KubernetesClusterCacheV2) GetAllDaemonSets() []*DaemonSet {
	return kcc.daemonSetStore.GetAll()
}

func (kcc *KubernetesClusterCacheV2) GetAllDeployments() []*Deployment {
	return kcc.deploymentStore.GetAll()
}

func (kcc *KubernetesClusterCacheV2) GetAllStatefulSets() []*StatefulSet {
	return kcc.statefulSetStore.GetAll()
}

func (kcc *KubernetesClusterCacheV2) GetAllPersistentVolumes() []*PersistentVolume {
	return kcc.persistentVolumeStore.GetAll()
}

func (kcc *KubernetesClusterCacheV2) GetAllPersistentVolumeClaims() []*PersistentVolumeClaim {
	return kcc.persistentVolumeClaimStore.GetAll()
}

func (kcc *KubernetesClusterCacheV2) GetAllStorageClasses() []*StorageClass {
	return kcc.storageClassStore.GetAll()
}

func (kcc *KubernetesClusterCacheV2) GetAllJobs() []*Job {
	return kcc.jobStore.GetAll()
}

func (kcc *KubernetesClusterCacheV2) GetAllReplicationControllers() []*ReplicationController {
	return kcc.replicationControllerStore.GetAll()
}

func (kcc *KubernetesClusterCacheV2) GetAllReplicaSets() []*ReplicaSet {
	return kcc.replicaSetStore.GetAll()
}

func (kcc *KubernetesClusterCacheV2) GetAllPodDisruptionBudgets() []*PodDisruptionBudget {
	return kcc.pdbStore.GetAll()
}
