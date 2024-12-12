package customcost

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
)

func TestPipelineService(t *testing.T) {
	// establish temporary test assets dir

	dir := t.TempDir()

	err := os.MkdirAll(dir+"/config", 0777)
	if err != nil {
		t.Fatalf("error creating temp config dir: %v", err)
	}

	err = os.MkdirAll(dir+"/executable", 0777)
	if err != nil {
		t.Fatalf("error creating temp exec dir: %v", err)
	}
	// write DD secrets to config files
	// write config file to temp dir
	writeDDConfig(dir+"/config", t)

	// set env vars for plugin config and executable
	err = os.Setenv("PLUGIN_CONFIG_DIR", dir+"/config")
	if err != nil {
		t.Fatalf("error setting config dir env var: %v", err)
	}
	err = os.Setenv("PLUGIN_EXECUTABLE_DIR", dir+"/executable")
	if err != nil {
		t.Fatalf("error setting config dir env var: %v", err)
	}

	// download amd plugin, store in tmp executable dir
	downloadLatestPluginExec(dir+"/executable", t)

	// set up repos
	hourlyRepo := NewMemoryRepository()
	dailyRepo := NewMemoryRepository()
	// set up ingestor config
	config := DefaultIngestorConfiguration()

	config.DailyDuration = 7 * timeutil.Day
	config.HourlyDuration = 16 * time.Hour
	pipeline, err := NewPipelineService(hourlyRepo, dailyRepo, config)
	if err != nil {
		t.Fatalf("error starting pipeline: %v", err)
	}

	// wait until coverage is complete, then stop ingestor
	ingestionComplete := false
	maxLoops := 10
	loopCount := 0
	for !ingestionComplete && loopCount < maxLoops {
		status := pipeline.Status()
		log.Debugf("got status: %v", status)
		coverageHourly, foundHourly := status.CoverageHourly["datadog"]
		coverageDaily, foundDaily := status.CoverageDaily["datadog"]
		if foundHourly && foundDaily {
			// check coverage
			minTime := time.Now().UTC().Add(-6 * timeutil.Day)
			maxTime := time.Now().UTC().Add(-3 * time.Hour)
			if coverageDaily.Start().Before(minTime) && coverageHourly.End().After(maxTime) {
				log.Infof("good coverage, breaking out of loop")
				ingestionComplete = true
				break
			} else {
				log.Infof("coverage not within range. Looking for coverage %v to %v, but current coverage is %v/%v", minTime, maxTime, coverageDaily, coverageHourly)
			}
		} else {
			log.Debugf("no coverage info ready yet for datadog")
		}
		log.Infof("sleeping 10s...")
		time.Sleep(10 * time.Second)
		loopCount++
	}

	if !ingestionComplete {
		t.Fatal("ingestor never completed within allocated time")
	}

	pipeline.hourlyIngestor.Stop()
	pipeline.dailyIngestor.Stop()

	// inspect data from yesterday
	targetTime := time.Now().UTC().Add(-1 * timeutil.Day).Truncate(timeutil.Day)
	log.Infof("querying for data with window start: %v", targetTime)
	// check for presence of hosts in DD response
	ddCosts, err := dailyRepo.Get(targetTime, "datadog")
	if err != nil {
		t.Fatalf("error getting results for targetTime")
	}
	foundInfraHosts := false
	for _, cost := range ddCosts.Costs {
		if cost.ResourceType == "infra_hosts" {
			foundInfraHosts = true
		}
	}

	if !foundInfraHosts {
		t.Fatal("expecting infra_hosts costs in daily response")
	}

	// query data from 4 hours ago (hourly)
	targetTime = time.Now().UTC().Add(-13 * time.Hour).Truncate(time.Hour)
	log.Infof("querying for data with window start: %v", targetTime)
	// check for presence of hosts in DD response
	ddCosts, err = hourlyRepo.Get(targetTime, "datadog")
	if err != nil {
		t.Fatalf("error getting results for targetTime")
	}
	foundInfraHosts = false
	for _, cost := range ddCosts.Costs {
		if cost.ResourceType == "infra_hosts" {
			foundInfraHosts = true
		}
	}

	if !foundInfraHosts {
		t.Fatal("expecting infra_hosts costs in hourly response")
	}
}

func downloadLatestPluginExec(dirName string, t *testing.T) {
	ddPluginURL := "https://github.com/opencost/opencost-plugins/releases/download/v0.0.3/datadog.ocplugin." + runtime.GOOS + "." + runtime.GOARCH
	out, err := os.OpenFile(dirName+"/datadog.ocplugin."+runtime.GOOS+"."+runtime.GOARCH, 0755|os.O_CREATE, 0755)
	if err != nil {
		t.Fatalf("error creating executable file: %v", err)
	}
	resp, err := http.Get(ddPluginURL)
	if err != nil {
		t.Fatalf("error downloading: %v", err)
	}
	defer resp.Body.Close()
	defer out.Close()

	_, err = io.Copy(out, resp.Body)
	if err != nil {
		t.Fatalf("error copying: %v", err)
	}
}

func writeDDConfig(pluginConfigDir string, t *testing.T) {
	// read necessary env vars. If any are missing, log warning and skip test
	ddSite := os.Getenv("DD_SITE")
	ddApiKey := os.Getenv("DD_API_KEY")
	ddAppKey := os.Getenv("DD_APPLICATION_KEY")

	if ddSite == "" {
		log.Warnf("DD_SITE undefined, this needs to have the URL of your DD instance, skipping test")
		t.Skip()
		return
	}

	if ddApiKey == "" {
		log.Warnf("DD_API_KEY undefined, skipping test")
		t.Skip()
		return
	}

	if ddAppKey == "" {
		log.Warnf("DD_APPLICATION_KEY undefined, skipping test")
		t.Skip()
		return
	}

	// write out config to temp file using contents of env vars
	ddConf := fmt.Sprintf(`{"datadog_site": "%s", "datadog_api_key": "%s", "datadog_app_key": "%s"}`, ddSite, ddApiKey, ddAppKey)

	// set up custom cost request
	file, err := os.CreateTemp(pluginConfigDir, "datadog_config.json")
	if err != nil {
		t.Fatalf("could not create temp config dir: %v", err)
	}

	err = os.WriteFile(file.Name(), []byte(ddConf), 0777)
	if err != nil {
		t.Fatalf("could not write file: %v", err)
	}
}
