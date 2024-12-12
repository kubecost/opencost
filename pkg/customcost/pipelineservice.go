package customcost

import (
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path"
	"runtime"
	"strings"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
	"github.com/julienschmidt/httprouter"
	"github.com/opencost/opencost/core/pkg/log"
	ocplugin "github.com/opencost/opencost/core/pkg/plugin"
	proto "github.com/opencost/opencost/core/pkg/protocol"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
)

var protocol = proto.HTTP()

const execFmt = `%s/%s.ocplugin.%s.%s`

// PipelineService exposes CustomCost pipeline controls and diagnostics endpoints
type PipelineService struct {
	hourlyIngestor, dailyIngestor *CustomCostIngestor
	hourlyStore, dailyStore       Repository
	domains                       []string
}

func getRegisteredPlugins(configDir string, execDir string) (map[string]*plugin.Client, error) {

	pluginNames := map[string]string{}
	// scan plugin config directory for all file names
	configFiles, err := os.ReadDir(configDir)
	if err != nil {
		log.Errorf("error reading files in directory %s: %v", configDir, err)
	}

	// list of plugins that we must run are the strings before _
	for _, file := range configFiles {
		// skip hidden files and directories
		if strings.HasPrefix(file.Name(), ".") || file.IsDir() {
			continue
		}

		log.Tracef("parsing config file name: %s", file.Name())
		fileParts := strings.Split(file.Name(), "_")

		if len(fileParts) != 2 || fileParts[1] == "_config.json" {
			return nil, fmt.Errorf("plugin config file name %s invalid. Config files must have the form <plugin name>_config.json", file.Name())
		}

		pluginNames[fileParts[0]] = path.Join(configDir, file.Name())
	}

	if len(pluginNames) == 0 {
		log.Infof("no plugins detected.")
		return nil, nil
	}

	log.Infof("requiring plugins matching your architecture: " + runtime.GOARCH)
	configs := map[string]*plugin.ClientConfig{}
	// set up the client config
	for name, config := range pluginNames {
		file := fmt.Sprintf(execFmt, execDir, name, runtime.GOOS, runtime.GOARCH)
		log.Debugf("looking for file: %s", file)
		if _, err := os.Stat(file); err != nil {
			msg := fmt.Sprintf("error reading executable for %s plugin. Plugin executables must be in %s and have name format <plugin name>.ocplugin.<os>.<opencost binary archtecture (arm64 or amd64)>", name, execDir)
			log.Errorf(msg)
			return nil, fmt.Errorf(msg)
		}

		var handshakeConfig = plugin.HandshakeConfig{
			ProtocolVersion:  1,
			MagicCookieKey:   "PLUGIN_NAME",
			MagicCookieValue: name,
		}

		logger := hclog.New(&hclog.LoggerOptions{
			Name:   "plugin[" + name + "]",
			Output: os.Stdout,
			Level:  hclog.Debug,
		})

		// pluginMap is the map of plugins we can dispense.
		var pluginMap = map[string]plugin.Plugin{
			"CustomCostSource": &ocplugin.CustomCostPlugin{},
		}
		configs[name] = &plugin.ClientConfig{
			HandshakeConfig:  handshakeConfig,
			Plugins:          pluginMap,
			Cmd:              exec.Command(fmt.Sprintf(execFmt, execDir, name, runtime.GOOS, runtime.GOARCH), config),
			Logger:           logger,
			AllowedProtocols: []plugin.Protocol{plugin.ProtocolGRPC},
		}
	}

	plugins := map[string]*plugin.Client{}

	for name, config := range configs {
		client := plugin.NewClient(config)

		// add the connected, initialized client to the ma
		plugins[name] = client
	}

	return plugins, nil
}

// NewPipelineService is a constructor for a PipelineService
func NewPipelineService(hourlyrepo, dailyrepo Repository, ingConf CustomCostIngestorConfig) (*PipelineService, error) {
	registeredPlugins, err := getRegisteredPlugins(ingConf.PluginConfigDir, ingConf.PluginExecutableDir)
	if err != nil {
		log.Errorf("error getting registered plugins: %v", err)
		return nil, fmt.Errorf("error getting registered plugins: %v", err)
	}

	hourlyIngestor, err := NewCustomCostIngestor(&ingConf, hourlyrepo, registeredPlugins, time.Hour)
	if err != nil {
		return nil, err
	}

	hourlyIngestor.Start(false)

	dailyIngestor, err := NewCustomCostIngestor(&ingConf, dailyrepo, registeredPlugins, timeutil.Day)
	if err != nil {
		return nil, err
	}

	dailyIngestor.Start(false)

	var domains []string
	for domain, _ := range registeredPlugins {
		domains = append(domains, domain)
	}

	return &PipelineService{
		hourlyIngestor: hourlyIngestor,
		hourlyStore:    hourlyrepo,
		dailyStore:     dailyrepo,
		dailyIngestor:  dailyIngestor,
		domains:        domains,
	}, nil
}

// Status gives a combined view of the state of configs and the ingestor status
func (dp *PipelineService) Status() Status {

	// Pull config status from the config controller
	ingstatusHourly := dp.hourlyIngestor.Status()

	// Pull config status from the config controller
	ingstatusDaily := dp.dailyIngestor.Status()

	// These are the statuses
	return Status{
		CoverageDaily:     ingstatusDaily.Coverage,
		CoverageHourly:    ingstatusHourly.Coverage,
		RefreshRateHourly: ingstatusHourly.RefreshRate.String(),
		RefreshRateDaily:  ingstatusDaily.RefreshRate.String(),
		Domains:           dp.domains,
	}

}

// GetCustomCostRebuildHandler creates a handler from a http request which initiates a rebuild of custom cost pipeline, if a
// domain is provided then it only rebuilds the specified billing domain
func (s *PipelineService) GetCustomCostRebuildHandler() func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	// If pipeline Service is nil, always return 501
	if s == nil {
		return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
			http.Error(w, "Custom Cost Pipeline Service is nil", http.StatusNotImplemented)
		}
	}
	if s.dailyIngestor == nil || s.hourlyIngestor == nil {
		return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
			http.Error(w, "Custom Cost Pipeline Service Ingestion Manager is nil", http.StatusNotImplemented)
		}
	}
	// Return valid handler func
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		w.Header().Set("Content-Type", "application/json")

		commit := r.URL.Query().Get("commit") == "true" || r.URL.Query().Get("commit") == "1"

		if !commit {
			protocol.WriteData(w, "Pass parameter 'commit=true' to confirm Custom Cost rebuild")
			return
		}

		domain := r.URL.Query().Get("domain")

		err := s.hourlyIngestor.Rebuild(domain)
		if err != nil {
			log.Errorf("error rebuilding hourly ingestor")
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		err = s.dailyIngestor.Rebuild(domain)
		if err != nil {
			log.Errorf("error rebuilding daily ingestor")
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		protocol.WriteData(w, fmt.Sprintf("Rebuilding Custom Cost For Domain %s", domain))
	}
}

// GetCustomCostStatusHandler creates a handler from a http request which returns the custom cost ingestor status
func (s *PipelineService) GetCustomCostStatusHandler() func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	if s == nil {
		resultStatus := Status{
			Enabled: false,
		}
		return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
			protocol.WriteData(w, resultStatus)
		}
	}
	if s.hourlyIngestor == nil || s.dailyIngestor == nil {
		return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
			http.Error(w, "Custom Cost Pipeline Service Ingestor is nil", http.StatusNotImplemented)
		}
	}

	// Return valid handler func
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		w.Header().Set("Content-Type", "application/json")
		stat := s.Status()
		stat.Enabled = true
		protocol.WriteData(w, stat)
	}
}
