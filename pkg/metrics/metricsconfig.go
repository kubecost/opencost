package metrics

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"sync"

	"github.com/opencost/opencost/pkg/env"
	"github.com/opencost/opencost/pkg/util/watcher"
)

var (
	metricsConfigLock = new(sync.Mutex)
	metricsFilePath   = path.Join(env.GetCostAnalyzerVolumeMountPath(), "metrics.json")
)

type MetricsConfig struct {
	DisabledMetrics    []string        `json:"disabledMetrics"`
	UseLabelsWhitelist bool            `json:"useLabelsWhitelist,omitempty"`
	LabelsWhitelist    map[string]bool `json:"labelsWhiteList,omitempty"`
}

// Gets map of disabled metrics to empty structs
func (mc MetricsConfig) GetDisabledMetricsMap() map[string]struct{} {
	disabledMetricsMap := make(map[string]struct{})

	for i := range mc.DisabledMetrics {
		disabledMetricsMap[mc.DisabledMetrics[i]] = struct{}{}
	}

	return disabledMetricsMap
}

// Unmarshals metrics.json to a MetricsConfig struct
func GetMetricsConfig() (*MetricsConfig, error) {
	metricsConfigLock.Lock()
	defer metricsConfigLock.Unlock()
	mc := &MetricsConfig{}
	body, err := os.ReadFile(metricsFilePath)
	if os.IsNotExist(err) {

		return mc, nil
	} else if err != nil {
		return mc, fmt.Errorf("error reading metrics config file: %s", err)
	}

	err = json.Unmarshal(body, mc)
	if err != nil {
		return mc, fmt.Errorf("error decoding metrics config: %s", err)
	}

	return mc, nil
}

// Writes MetricsConfig struct to json file
func UpdateMetricsConfig(mc *MetricsConfig) (*MetricsConfig, error) {
	metricsConfigLock.Lock()
	defer metricsConfigLock.Unlock()

	mcb, err := json.Marshal(mc)
	if err != nil {
		return nil, fmt.Errorf("error encoding metrics config struct: %s", err)
	}

	err = os.WriteFile(metricsFilePath, mcb, 0644)
	if err != nil {
		return nil, fmt.Errorf("error writing to metrics config file: %s", err)
	}

	return mc, nil
}

// Updates metric config file from configmap
func UpdateMetricsConfigFromConfigmap(data map[string]string) error {

	mc := &MetricsConfig{}
	key := "metrics.json"

	cdata, ok := data[key]
	if !ok {
		return fmt.Errorf("error finding metrics config data")
	}

	err := json.Unmarshal([]byte(cdata), &mc)
	if err != nil {
		return fmt.Errorf("failed to unmarshal metrics configs: %s", err)
	}

	_, err = UpdateMetricsConfig(mc)
	if err != nil {
		return err
	}

	return nil

}

// Returns ConfigMapWatcher for metrics configuration configmap
func GetMetricsConfigWatcher() *watcher.ConfigMapWatcher {
	return &watcher.ConfigMapWatcher{
		ConfigMapName: env.GetMetricsConfigmapName(),
		WatchFunc: func(name string, data map[string]string) error {
			err := UpdateMetricsConfigFromConfigmap(data)
			return err
		},
	}
}
