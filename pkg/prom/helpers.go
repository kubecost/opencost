package prom

import (
	"context"
	"fmt"
	"time"

	prometheus "github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"

	"gopkg.in/yaml.v2"
)

const PrometheusTroubleshootingURL = "https://www.opencost.io/docs/integrations/prometheus"

// ScrapeConfig is the minimalized view of a prometheus scrape configuration
type ScrapeConfig struct {
	JobName        string `yaml:"job_name,omitempty"`
	ScrapeInterval string `yaml:"scrape_interval,omitempty"`
}

// PrometheusConfig is the minimalized view of a prometheus configuration
type PrometheusConfig struct {
	ScrapeConfigs []ScrapeConfig `yaml:"scrape_configs,omitempty"`
}

// GetPrometheusConfig uses the provided yaml string to parse the minimalized prometheus config
func GetPrometheusConfig(pcfg string) (PrometheusConfig, error) {
	var promCfg PrometheusConfig
	err := yaml.Unmarshal([]byte(pcfg), &promCfg)
	return promCfg, err
}

// ScrapeIntervalFor uses the provided prometheus client to locate a scrape interval for a specific job name
func ScrapeIntervalFor(client prometheus.Client, jobName string) (time.Duration, error) {
	api := v1.NewAPI(client)
	promConfig, err := api.Config(context.Background())
	if err != nil {
		return 0, err
	}

	cfg, err := GetPrometheusConfig(promConfig.YAML)
	if err != nil {
		return 0, err
	}

	for _, sc := range cfg.ScrapeConfigs {
		if sc.JobName == jobName {
			if sc.ScrapeInterval != "" {
				si := sc.ScrapeInterval
				sid, err := time.ParseDuration(si)
				if err != nil {
					return 0, fmt.Errorf("Error parsing scrape config for %s", sc.JobName)
				} else {
					return sid, nil
				}
			}
		}
	}

	return 0, fmt.Errorf("Failed to locate scrape config for %s", jobName)
}
