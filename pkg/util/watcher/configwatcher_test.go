package watcher

import (
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	TestConfigMapName          = "test-config"
	AlternateTestConfigMapName = "alternate-test-config"
	TestDataProperty           = "test-prop"
)

func newTestWatcher(t *testing.T, configMapName string, instanceName string, didRun *bool) *ConfigMapWatcher {
	return &ConfigMapWatcher{
		ConfigMapName: configMapName,
		WatchFunc: func(cmn string, data map[string]string) error {
			t.Logf("ConfigMapWatcher[%s] triggered for ConfigMap: %s, data[\"test\"] = %s\n", instanceName, cmn, data[TestDataProperty])
			*didRun = true
			return nil
		},
	}
}

func newConfigMap(configMapName string, dataValue string) *v1.ConfigMap {
	return &v1.ConfigMap{
		Data: map[string]string{
			TestDataProperty: dataValue,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: configMapName,
		},
	}
}

func TestConfigWatcherSingleHandler(t *testing.T) {
	// Test that a single watcher added for the configmap test-config is executed when
	// triggered
	var didRun bool = false

	w := NewConfigMapWatchers(nil, "", newTestWatcher(t, TestConfigMapName, "single", &didRun))
	f := w.toWatchFunc()

	// Execute watch func with a 'test-config' configmap
	f(newConfigMap(TestConfigMapName, "testing 1 2 3"))

	if !didRun {
		t.Errorf("Failed to run configmap handler for 'single'\n")
	}
}

func TestConfigWatcherMultipleHandlers(t *testing.T) {
	// Test that adding two different configmap watchers aren't both triggered on a configmap update
	var firstDidRun bool = false
	var secondDidRun bool = false

	w := NewConfigMapWatchers(
		nil,
		"",
		newTestWatcher(t, TestConfigMapName, "single", &firstDidRun),
		newTestWatcher(t, AlternateTestConfigMapName, "alternate", &secondDidRun))

	f := w.toWatchFunc()

	// Execute watch func with a 'alternate-test-config' configmap
	f(newConfigMap(AlternateTestConfigMapName, "oof!"))

	// Assert that first did not run
	if firstDidRun {
		t.Errorf("Executed alternate-test-config map change, but test-config handler, 'single' executed!\n")
	}

	if !secondDidRun {
		t.Errorf("Failed to run configmap handler for 'alternate'\n")
	}
}

func TestConfigWatcherMultipleHandlersForSameConfig(t *testing.T) {
	// Test that adding two different configmap watchers for the same configmap are both triggered
	var firstDidRun bool = false
	var secondDidRun bool = false
	var thirdDidRun bool = false

	w := NewConfigMapWatchers(
		nil,
		"",
		newTestWatcher(t, TestConfigMapName, "first", &firstDidRun),
		newTestWatcher(t, AlternateTestConfigMapName, "alternate", &secondDidRun),
		// third watcher watches for the same configmap as "first"
		newTestWatcher(t, TestConfigMapName, "third", &thirdDidRun),
	)

	f := w.toWatchFunc()

	// Execute watch func with a 'test-config' configmap
	f(newConfigMap(TestConfigMapName, "double trouble"))

	// Assert that second did not run
	if secondDidRun {
		t.Errorf("Executed test-config map change, first handler, 'single', executed!\n")
	}

	if !firstDidRun {
		t.Errorf("Failed to run configmap handler for 'first'\n")
	}
	if !thirdDidRun {
		t.Errorf("Failed to run configmap handler for 'third'\n")
	}
}

func TestConfigMapWatcherWithAdd(t *testing.T) {
	// Test that adding two different configmap watchers for the same configmap are both triggered
	// when using Add() and AddWatcher()
	var firstDidRun bool = false
	var secondDidRun bool = false
	var thirdDidRun bool = false

	a, b, c := newTestWatcher(t, TestConfigMapName, "first", &firstDidRun),
		newTestWatcher(t, AlternateTestConfigMapName, "alternate", &secondDidRun),
		// third watcher watches for the same configmap as "first"
		newTestWatcher(t, TestConfigMapName, "third", &thirdDidRun)

	w := NewConfigMapWatchers(nil, "")
	w.AddWatcher(a)
	w.AddWatcher(b)
	w.Add(c.ConfigMapName, c.WatchFunc)

	f := w.toWatchFunc()

	// Execute watch func with a 'test-config' configmap
	f(newConfigMap(TestConfigMapName, "double trouble"))

	// Assert that second did not run
	if secondDidRun {
		t.Errorf("Executed test-config map change, first handler, 'single', executed!\n")
	}

	if !firstDidRun {
		t.Errorf("Failed to run configmap handler for 'first'\n")
	}
	if !thirdDidRun {
		t.Errorf("Failed to run configmap handler for 'third'\n")
	}
}
