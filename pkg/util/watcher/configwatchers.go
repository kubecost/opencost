package watcher

import (
	"context"
	"sync/atomic"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/pkg/clustercache"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"

	"k8s.io/client-go/kubernetes"
)

// ConfigMapWatcher represents a single configmap watcher
type ConfigMapWatcher struct {
	ConfigMapName string
	WatchFunc     func(string, map[string]string) error
}

type ConfigMapWatchers struct {
	kubeClientset   kubernetes.Interface
	namespace       string
	watchers        map[string][]*ConfigMapWatcher
	watchController clustercache.WatchController
	started         atomic.Bool
	stop            chan struct{}
}

func NewConfigMapWatchers(kubeClientset kubernetes.Interface, namespace string, watchers ...*ConfigMapWatcher) *ConfigMapWatchers {
	var stopCh chan struct{}
	var watchController clustercache.WatchController

	if kubeClientset != nil {
		coreRestClient := kubeClientset.CoreV1().RESTClient()
		watchController = clustercache.NewCachingWatcher(coreRestClient, "configmaps", &v1.ConfigMap{}, namespace, fields.Everything())
		stopCh = make(chan struct{})

		// a bit awkward here, but since we'll mostly be deferring adding a watcher after initializing k8s,
		// we'll warmup and start the actual watcher here
		watchController.WarmUp(stopCh)
		go watchController.Run(1, stopCh)
	}

	cmw := &ConfigMapWatchers{
		kubeClientset:   kubeClientset,
		namespace:       namespace,
		watchController: watchController,
		watchers:        make(map[string][]*ConfigMapWatcher),
		stop:            stopCh,
	}

	for _, w := range watchers {
		cmw.AddWatcher(w)
	}

	return cmw
}

func (cmw *ConfigMapWatchers) AddWatcher(watcher *ConfigMapWatcher) {
	if cmw.started.Load() {
		log.Warnf("Cannot add watcher %s after starting", watcher.ConfigMapName)
		return
	}

	if watcher == nil {
		return
	}

	name := watcher.ConfigMapName
	cmw.watchers[name] = append(cmw.watchers[name], watcher)
}

func (cmw *ConfigMapWatchers) Add(configMapName string, watchFunc func(string, map[string]string) error) {
	cmw.AddWatcher(&ConfigMapWatcher{
		ConfigMapName: configMapName,
		WatchFunc:     watchFunc,
	})
}

func (cmw *ConfigMapWatchers) Watch() {
	if cmw.kubeClientset == nil {
		return
	}

	if !cmw.started.CompareAndSwap(false, true) {
		log.Warnf("Already started")
		return
	}

	watchConfigFunc := cmw.toWatchFunc()

	// We need an initial invocation because the init of the cache has happened before we had access to the provider.
	for cw := range cmw.watchers {
		configs, err := cmw.kubeClientset.CoreV1().ConfigMaps(cmw.namespace).Get(context.Background(), cw, metav1.GetOptions{})
		if err != nil {
			log.Infof("No %s configmap found at install time, using existing configs: %s", cw, err.Error())
		} else {
			log.Infof("Found configmap %s, watching...", configs.Name)
			watchConfigFunc(configs)
		}
	}

	cmw.watchController.SetUpdateHandler(watchConfigFunc)
}

func (cmw *ConfigMapWatchers) Stop() {
	if cmw.stop == nil {
		return
	}

	close(cmw.stop)
	cmw.stop = nil
}

func (cmw *ConfigMapWatchers) toWatchFunc() func(any) {
	return func(c any) {
		conf, ok := c.(*v1.ConfigMap)
		if !ok {
			return
		}

		name := conf.GetName()
		data := conf.Data
		if watchers, ok := cmw.watchers[name]; ok {
			for _, cw := range watchers {
				err := cw.WatchFunc(name, data)
				if err != nil {
					log.Infof("ERROR UPDATING %s CONFIG: %s", name, err.Error())
				}
			}
		}
	}
}
