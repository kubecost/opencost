package clustercache

import (
	"sync"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/pkg/env"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	stv1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/kubernetes"
)

type Namespace struct {
	Name        string
	Labels      map[string]string
	Annotations map[string]string
}

func transformNamespace(input *v1.Namespace) *Namespace {
	return &Namespace{
		Name:        input.Name,
		Annotations: input.Annotations,
		Labels:      input.Labels,
	}
}

type Pod struct {
	UID             types.UID
	Name            string
	Namespace       string
	Labels          map[string]string
	Annotations     map[string]string
	OwnerReferences []metav1.OwnerReference
	Status          PodStatus
	Spec            PodSpec
}

type PodStatus struct {
	Phase             v1.PodPhase
	ContainerStatuses []v1.ContainerStatus
}

type PodSpec struct {
	NodeName   string
	Containers []Container
	Volumes    []v1.Volume
}

type Container struct {
	Name      string
	Resources v1.ResourceRequirements
}

func transformPodContainer(input v1.Container) Container {
	return Container{
		Name:      input.Name,
		Resources: input.Resources,
	}
}

func transformPodStatus(input v1.PodStatus) PodStatus {
	return PodStatus{
		Phase:             input.Phase,
		ContainerStatuses: input.ContainerStatuses,
	}
}

func transformPodSpec(input v1.PodSpec) PodSpec {
	containers := make([]Container, len(input.Containers))
	for i, container := range input.Containers {
		containers[i] = transformPodContainer(container)
	}
	return PodSpec{
		NodeName:   input.NodeName,
		Containers: containers,
		Volumes:    input.Volumes,
	}

}

func transformPod(input *v1.Pod) *Pod {
	return &Pod{
		UID:             input.UID,
		Name:            input.Name,
		Namespace:       input.Namespace,
		Labels:          input.Labels,
		Annotations:     input.Annotations,
		OwnerReferences: input.OwnerReferences,
		Spec:            transformPodSpec(input.Spec),
		Status:          transformPodStatus(input.Status),
	}
}

type Node struct {
	Name           string
	Labels         map[string]string
	Annotations    map[string]string
	Status         v1.NodeStatus
	SpecProviderID string
}

func transformNode(input *v1.Node) *Node {
	return &Node{
		Name:           input.Name,
		Labels:         input.Labels,
		Annotations:    input.Annotations,
		Status:         input.Status,
		SpecProviderID: input.Spec.ProviderID,
	}
}

type Service struct {
	Name      string
	Namespace string
	Selector  map[string]string
	Type      v1.ServiceType
	Status    v1.ServiceStatus
}

func transformService(input *v1.Service) *Service {
	return &Service{
		Name:      input.Name,
		Namespace: input.Namespace,
		Selector:  input.Spec.Selector,
		Type:      input.Spec.Type,
		Status:    input.Status,
	}
}

type DaemonSet struct {
	Name           string
	Namespace      string
	Labels         map[string]string
	SpecContainers []v1.Container
}

func transformDaemonSet(input *appsv1.DaemonSet) *DaemonSet {
	return &DaemonSet{
		Name:           input.Name,
		Namespace:      input.Namespace,
		Labels:         input.Labels,
		SpecContainers: input.Spec.Template.Spec.Containers,
	}
}

type Deployment struct {
	Name                    string
	Namespace               string
	Labels                  map[string]string
	MatchLabels             map[string]string
	SpecSelector            *metav1.LabelSelector
	SpecReplicas            *int32
	StatusAvailableReplicas int32
}

func transformDeployment(input *appsv1.Deployment) *Deployment {
	return &Deployment{
		Name:                    input.Name,
		Namespace:               input.Namespace,
		Labels:                  input.Labels,
		MatchLabels:             input.Spec.Selector.MatchLabels,
		SpecReplicas:            input.Spec.Replicas,
		SpecSelector:            input.Spec.Selector,
		StatusAvailableReplicas: input.Status.AvailableReplicas,
	}
}

type StatefulSet struct {
	Name         string
	Namespace    string
	SpecSelector *metav1.LabelSelector
}

func transformStatefulSet(input *appsv1.StatefulSet) *StatefulSet {
	return &StatefulSet{
		Name:         input.Name,
		Namespace:    input.Namespace,
		SpecSelector: input.Spec.Selector,
	}
}

type PersistentVolume struct {
	Name        string
	Namespace   string
	Labels      map[string]string
	Annotations map[string]string
	Spec        v1.PersistentVolumeSpec
	Status      v1.PersistentVolumeStatus
}

func transformPersistentVolume(input *v1.PersistentVolume) *PersistentVolume {
	return &PersistentVolume{
		Name:        input.Name,
		Namespace:   input.Namespace,
		Labels:      input.Labels,
		Annotations: input.Annotations,
		Spec:        input.Spec,
		Status:      input.Status,
	}
}

// ClusterCache defines an contract for an object which caches components within a cluster, ensuring
// up to date resources using watchers
type ClusterCache interface {
	// Run starts the watcher processes
	Run()

	// Stops the watcher processes
	Stop()

	// GetAllNamespaces returns all the cached namespaces
	GetAllNamespaces() []*Namespace

	// GetAllNodes returns all the cached nodes
	GetAllNodes() []*Node

	// GetAllPods returns all the cached pods
	GetAllPods() []*Pod

	// GetAllServices returns all the cached services
	GetAllServices() []*Service

	// GetAllDaemonSets returns all the cached DaemonSets
	GetAllDaemonSets() []*DaemonSet

	// GetAllDeployments returns all the cached deployments
	GetAllDeployments() []*Deployment

	// GetAllStatfulSets returns all the cached StatefulSets
	GetAllStatefulSets() []*StatefulSet

	// GetAllPersistentVolumes returns all the cached persistent volumes
	GetAllPersistentVolumes() []*PersistentVolume

	// GetAllPersistentVolumeClaims returns all the cached persistent volume claims
	GetAllPersistentVolumeClaims() []*v1.PersistentVolumeClaim

	// GetAllStorageClasses returns all the cached storage classes
	GetAllStorageClasses() []*stv1.StorageClass

	// GetAllJobs returns all the cached jobs
	GetAllJobs() []*batchv1.Job

	// GetAllPodDisruptionBudgets returns all cached pod disruption budgets
	GetAllPodDisruptionBudgets() []*policyv1.PodDisruptionBudget
	// SetConfigMapUpdateFunc sets the configmap update function
	SetConfigMapUpdateFunc(func(interface{}))
}

// KubernetesClusterCache is the implementation of ClusterCache
type KubernetesClusterCache struct {
	client kubernetes.Interface

	namespaceWatch         WatchController
	nodeWatch              WatchController
	podWatch               WatchController
	kubecostConfigMapWatch WatchController
	serviceWatch           WatchController
	daemonsetsWatch        WatchController
	deploymentsWatch       WatchController
	statefulsetWatch       WatchController
	pvWatch                WatchController
	pvcWatch               WatchController
	storageClassWatch      WatchController
	jobsWatch              WatchController
	pdbWatch               WatchController
	stop                   chan struct{}
}

func initializeCache(wc WatchController, wg *sync.WaitGroup, cancel chan struct{}) {
	defer wg.Done()
	wc.WarmUp(cancel)
}

func NewKubernetesClusterCache(client kubernetes.Interface) ClusterCache {
	coreRestClient := client.CoreV1().RESTClient()
	appsRestClient := client.AppsV1().RESTClient()
	storageRestClient := client.StorageV1().RESTClient()
	batchClient := client.BatchV1().RESTClient()
	pdbClient := client.PolicyV1().RESTClient()

	kubecostNamespace := env.GetKubecostNamespace()
	log.Infof("NAMESPACE: %s", kubecostNamespace)

	kcc := &KubernetesClusterCache{
		client:                 client,
		namespaceWatch:         NewCachingWatcher(coreRestClient, "namespaces", &v1.Namespace{}, "", fields.Everything()),
		nodeWatch:              NewCachingWatcher(coreRestClient, "nodes", &v1.Node{}, "", fields.Everything()),
		podWatch:               NewCachingWatcher(coreRestClient, "pods", &v1.Pod{}, "", fields.Everything()),
		kubecostConfigMapWatch: NewCachingWatcher(coreRestClient, "configmaps", &v1.ConfigMap{}, kubecostNamespace, fields.Everything()),
		serviceWatch:           NewCachingWatcher(coreRestClient, "services", &v1.Service{}, "", fields.Everything()),
		daemonsetsWatch:        NewCachingWatcher(appsRestClient, "daemonsets", &appsv1.DaemonSet{}, "", fields.Everything()),
		deploymentsWatch:       NewCachingWatcher(appsRestClient, "deployments", &appsv1.Deployment{}, "", fields.Everything()),
		statefulsetWatch:       NewCachingWatcher(appsRestClient, "statefulsets", &appsv1.StatefulSet{}, "", fields.Everything()),
		pvWatch:                NewCachingWatcher(coreRestClient, "persistentvolumes", &v1.PersistentVolume{}, "", fields.Everything()),
		pvcWatch:               NewCachingWatcher(coreRestClient, "persistentvolumeclaims", &v1.PersistentVolumeClaim{}, "", fields.Everything()),
		storageClassWatch:      NewCachingWatcher(storageRestClient, "storageclasses", &stv1.StorageClass{}, "", fields.Everything()),
		jobsWatch:              NewCachingWatcher(batchClient, "jobs", &batchv1.Job{}, "", fields.Everything()),
		pdbWatch:               NewCachingWatcher(pdbClient, "poddisruptionbudgets", &policyv1.PodDisruptionBudget{}, "", fields.Everything()),
	}

	// Wait for each caching watcher to initialize
	cancel := make(chan struct{})
	var wg sync.WaitGroup
	if env.IsETLReadOnlyMode() {
		wg.Add(1)
		go initializeCache(kcc.kubecostConfigMapWatch, &wg, cancel)
	} else {
		wg.Add(13)
		go initializeCache(kcc.kubecostConfigMapWatch, &wg, cancel)
		go initializeCache(kcc.namespaceWatch, &wg, cancel)
		go initializeCache(kcc.nodeWatch, &wg, cancel)
		go initializeCache(kcc.podWatch, &wg, cancel)
		go initializeCache(kcc.serviceWatch, &wg, cancel)
		go initializeCache(kcc.daemonsetsWatch, &wg, cancel)
		go initializeCache(kcc.deploymentsWatch, &wg, cancel)
		go initializeCache(kcc.statefulsetWatch, &wg, cancel)
		go initializeCache(kcc.pvWatch, &wg, cancel)
		go initializeCache(kcc.pvcWatch, &wg, cancel)
		go initializeCache(kcc.storageClassWatch, &wg, cancel)
		go initializeCache(kcc.jobsWatch, &wg, cancel)
		go initializeCache(kcc.pdbWatch, &wg, cancel)
	}

	wg.Wait()

	log.Infof("Done waiting")

	return kcc
}

func (kcc *KubernetesClusterCache) Run() {
	if kcc.stop != nil {
		return
	}
	stopCh := make(chan struct{})

	go kcc.namespaceWatch.Run(1, stopCh)
	go kcc.nodeWatch.Run(1, stopCh)
	go kcc.podWatch.Run(1, stopCh)
	go kcc.serviceWatch.Run(1, stopCh)
	go kcc.kubecostConfigMapWatch.Run(1, stopCh)
	go kcc.daemonsetsWatch.Run(1, stopCh)
	go kcc.deploymentsWatch.Run(1, stopCh)
	go kcc.statefulsetWatch.Run(1, stopCh)
	go kcc.pvWatch.Run(1, stopCh)
	go kcc.pvcWatch.Run(1, stopCh)
	go kcc.storageClassWatch.Run(1, stopCh)
	go kcc.jobsWatch.Run(1, stopCh)
	go kcc.pdbWatch.Run(1, stopCh)

	kcc.stop = stopCh
}

func (kcc *KubernetesClusterCache) Stop() {
	if kcc.stop == nil {
		return
	}

	close(kcc.stop)
	kcc.stop = nil
}

func (kcc *KubernetesClusterCache) GetAllNamespaces() []*Namespace {
	var namespaces []*Namespace
	items := kcc.namespaceWatch.GetAll()
	for _, ns := range items {
		namespaces = append(namespaces, transformNamespace(ns.(*v1.Namespace)))
	}
	return namespaces
}

func (kcc *KubernetesClusterCache) GetAllNodes() []*Node {
	var nodes []*Node
	items := kcc.nodeWatch.GetAll()
	for _, node := range items {
		nodes = append(nodes, transformNode(node.(*v1.Node)))
	}
	return nodes
}

func (kcc *KubernetesClusterCache) GetAllPods() []*Pod {
	var pods []*Pod
	items := kcc.podWatch.GetAll()
	for _, pod := range items {
		pods = append(pods, transformPod(pod.(*v1.Pod)))
	}
	return pods
}

func (kcc *KubernetesClusterCache) GetAllServices() []*Service {
	var services []*Service
	items := kcc.serviceWatch.GetAll()
	for _, service := range items {
		services = append(services, transformService(service.(*v1.Service)))
	}
	return services
}

func (kcc *KubernetesClusterCache) GetAllDaemonSets() []*DaemonSet {
	var daemonsets []*DaemonSet
	items := kcc.daemonsetsWatch.GetAll()
	for _, daemonset := range items {
		daemonsets = append(daemonsets, transformDaemonSet(daemonset.(*appsv1.DaemonSet)))
	}
	return daemonsets
}

func (kcc *KubernetesClusterCache) GetAllDeployments() []*Deployment {
	var deployments []*Deployment
	items := kcc.deploymentsWatch.GetAll()
	for _, deployment := range items {
		deployments = append(deployments, transformDeployment(deployment.(*appsv1.Deployment)))
	}
	return deployments
}

func (kcc *KubernetesClusterCache) GetAllStatefulSets() []*StatefulSet {
	var statefulsets []*StatefulSet
	items := kcc.statefulsetWatch.GetAll()
	for _, statefulset := range items {
		statefulsets = append(statefulsets, transformStatefulSet(statefulset.(*appsv1.StatefulSet)))
	}
	return statefulsets
}

func (kcc *KubernetesClusterCache) GetAllPersistentVolumes() []*PersistentVolume {
	var pvs []*PersistentVolume
	items := kcc.pvWatch.GetAll()
	for _, pv := range items {
		pvs = append(pvs, transformPersistentVolume(pv.(*v1.PersistentVolume)))
	}
	return pvs
}

func (kcc *KubernetesClusterCache) GetAllPersistentVolumeClaims() []*v1.PersistentVolumeClaim {
	var pvcs []*v1.PersistentVolumeClaim
	items := kcc.pvcWatch.GetAll()
	for _, pvc := range items {
		pvcs = append(pvcs, pvc.(*v1.PersistentVolumeClaim))
	}
	return pvcs
}

func (kcc *KubernetesClusterCache) GetAllStorageClasses() []*stv1.StorageClass {
	var storageClasses []*stv1.StorageClass
	items := kcc.storageClassWatch.GetAll()
	for _, stc := range items {
		storageClasses = append(storageClasses, stc.(*stv1.StorageClass))
	}
	return storageClasses
}

func (kcc *KubernetesClusterCache) GetAllJobs() []*batchv1.Job {
	var jobs []*batchv1.Job
	items := kcc.jobsWatch.GetAll()
	for _, job := range items {
		jobs = append(jobs, job.(*batchv1.Job))
	}
	return jobs
}

func (kcc *KubernetesClusterCache) GetAllPodDisruptionBudgets() []*policyv1.PodDisruptionBudget {
	var pdbs []*policyv1.PodDisruptionBudget
	items := kcc.pdbWatch.GetAll()
	for _, pdb := range items {
		pdbs = append(pdbs, pdb.(*policyv1.PodDisruptionBudget))
	}
	return pdbs
}

func (kcc *KubernetesClusterCache) SetConfigMapUpdateFunc(f func(interface{})) {
	kcc.kubecostConfigMapWatch.SetUpdateHandler(f)
}
