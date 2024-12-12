package metrics

import (
	"testing"

	"github.com/opencost/opencost/pkg/clustercache"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestWhitelist(t *testing.T) {
	sampleServices := []*clustercache.Service{{
		SpecSelector: map[string]string{"servicewhitelistlabel": "foo"},
	}}
	replicaSetLabelSelector := metav1.LabelSelector{
		MatchLabels: map[string]string{"replicasetwhitelistlabel1": "bar"},
	}
	sampleReplicaSets := []*clustercache.ReplicaSet{{
		SpecSelector: &replicaSetLabelSelector,
	}}

	sampleStatefulSets := []*clustercache.StatefulSet{}

	kc := NewFakeCache(sampleReplicaSets, sampleStatefulSets, sampleServices)
	wl := map[string]bool{
		"whitelistedlabel": true,
	}
	mc := MetricsConfig{
		DisabledMetrics:    []string{},
		UseLabelsWhitelist: true,
		LabelsWhitelist:    wl,
	}
	kplc := KubePodLabelsCollector{
		KubeClusterCache: kc,
		metricsConfig:    mc,
	}
	kplc.UpdateWhitelist()
	if !kplc.labelsWhitelist["servicewhitelistlabel"] {
		t.Errorf("Missing expected label %s", "servicewhitelistlabel")
	}
	if !kplc.labelsWhitelist["replicasetwhitelistlabel1"] {
		t.Errorf("Missing expected label %s", "servicewhitelistlabel1")
	}

}

type FakeCache struct {
	clustercache.ClusterCache
	replicasets  []*clustercache.ReplicaSet
	statefulsets []*clustercache.StatefulSet
	services     []*clustercache.Service
}

func (f FakeCache) GetAllReplicaSets() []*clustercache.ReplicaSet {
	return f.replicasets
}

func (f FakeCache) GetAllStatefulSets() []*clustercache.StatefulSet {
	return f.statefulsets
}

func (f FakeCache) GetAllServices() []*clustercache.Service {
	return f.services
}

func NewFakeCache(replicasets []*clustercache.ReplicaSet, statefulsets []*clustercache.StatefulSet, services []*clustercache.Service) FakeCache {
	return FakeCache{
		replicasets:  replicasets,
		statefulsets: statefulsets,
		services:     services,
	}
}
