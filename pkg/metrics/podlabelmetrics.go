package metrics

import (
	"github.com/opencost/opencost/core/pkg/util/promutil"
	"github.com/opencost/opencost/pkg/clustercache"
	"github.com/prometheus/client_golang/prometheus"
)

//--------------------------------------------------------------------------
//  KubePodLabelsCollector
//--------------------------------------------------------------------------

// KubePodLabelsCollector is a prometheus collector that emits pod labels only
type KubePodLabelsCollector struct {
	KubeClusterCache clustercache.ClusterCache
	metricsConfig    MetricsConfig
	labelsWhitelist  map[string]bool
}

func (kpmc *KubePodLabelsCollector) SetLabelsWhiteList() {
	kpmc.labelsWhitelist = make(map[string]bool)
	for k, v := range kpmc.metricsConfig.LabelsWhitelist {
		kpmc.labelsWhitelist[k] = v
	}
}

// Describe sends the super-set of all possible descriptors of pod labels only
// collected by this Collector.
func (kpmc KubePodLabelsCollector) Describe(ch chan<- *prometheus.Desc) {
	disabledMetrics := kpmc.metricsConfig.GetDisabledMetricsMap()

	if _, disabled := disabledMetrics["kube_pod_labels"]; !disabled {
		ch <- prometheus.NewDesc("kube_pod_labels", "All labels for each pod prefixed with label_", []string{}, nil)
	}
	if _, disabled := disabledMetrics["kube_pod_owner"]; !disabled {
		ch <- prometheus.NewDesc("kube_pod_owner", "Information about the Pod's owner", []string{}, nil)
	}
}

func (kpmc *KubePodLabelsCollector) UpdateControllerSelectorsCache() {
	for _, r := range kpmc.KubeClusterCache.GetAllReplicaSets() {
		for k := range r.SpecSelector.MatchLabels {
			kpmc.labelsWhitelist[k] = true
		}
		for _, v := range r.SpecSelector.MatchExpressions {
			kpmc.labelsWhitelist[v.Key] = true
		}
	}
	for _, ss := range kpmc.KubeClusterCache.GetAllStatefulSets() {
		for k := range ss.SpecSelector.MatchLabels {
			kpmc.labelsWhitelist[k] = true
		}
		for _, v := range ss.SpecSelector.MatchExpressions {
			kpmc.labelsWhitelist[v.Key] = true
		}
	}
}

func (kpmc *KubePodLabelsCollector) UpdateServiceLabels() {
	for _, service := range kpmc.KubeClusterCache.GetAllServices() {
		// Just unroll the selector and keep all labels whose keys could match a service selector
		for k := range service.SpecSelector {
			kpmc.labelsWhitelist[k] = true
		}
	}
}

func (kpmc *KubePodLabelsCollector) UpdateWhitelist() {
	kpmc.SetLabelsWhiteList()
	kpmc.UpdateControllerSelectorsCache()
	kpmc.UpdateServiceLabels()
}

// Collect is called by the Prometheus registry when collecting metrics.
func (kpmc KubePodLabelsCollector) Collect(ch chan<- prometheus.Metric) {
	pods := kpmc.KubeClusterCache.GetAllPods()
	disabledMetrics := kpmc.metricsConfig.GetDisabledMetricsMap()

	for _, pod := range pods {
		podName := pod.Name
		podNS := pod.Namespace
		podUID := string(pod.UID)

		// Pod Labels
		if _, disabled := disabledMetrics["kube_pod_labels"]; !disabled {
			podLabels := pod.Labels
			if kpmc.metricsConfig.UseLabelsWhitelist {
				kpmc.UpdateWhitelist()
				for lname := range pod.Labels {
					if _, ok := kpmc.labelsWhitelist[lname]; !ok {
						delete(podLabels, lname)
					}
				}
			}

			labelNames, labelValues := promutil.KubePrependQualifierToLabels(promutil.SanitizeLabels(podLabels), "label_")
			ch <- newKubePodLabelsMetric("kube_pod_labels", podNS, podName, podUID, labelNames, labelValues)
		}

		// Owner References
		if _, disabled := disabledMetrics["kube_pod_owner"]; !disabled {
			for _, owner := range pod.OwnerReferences {
				ch <- newKubePodOwnerMetric("kube_pod_owner", podNS, podName, owner.Name, owner.Kind, owner.Controller != nil)
			}
		}
	}
}
