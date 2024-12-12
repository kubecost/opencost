package clusters

import (
	"encoding/base64"
	"fmt"
	"os"
	"strings"

	"github.com/google/uuid"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util/fileutil"
	"github.com/opencost/opencost/core/pkg/util/json"

	"sigs.k8s.io/yaml"
)

// The details key used to provide auth information
const DetailsAuthKey = "auth"

// Authentication Information
type ClusterConfigEntryAuth struct {
	// The type of authentication provider to use
	Type string `yaml:"type"`

	// Data expressed as a secret
	SecretName string `yaml:"secretName,omitempty"`

	// Any data specifically needed by the auth provider
	Data string `yaml:"data,omitempty"`

	// User and Password as a possible input
	User string `yaml:"user,omitempty"`
	Pass string `yaml:"pass,omitempty"`
}

// Cluster definition from a configuration yaml
type ClusterConfigEntry struct {
	Name    string                  `yaml:"name"`
	Address string                  `yaml:"address"`
	Auth    *ClusterConfigEntryAuth `yaml:"auth,omitempty"`
	Details map[string]interface{}  `yaml:"details,omitempty"`
}

// ClusterDefinition
type ClusterDefinition struct {
	ID      string                 `json:"id,omitempty"`
	Name    string                 `json:"name"`
	Address string                 `json:"address"`
	Details map[string]interface{} `json:"details,omitempty"`
}

// ClusterStorage interface defines an implementation prototype for a storage responsible
// for ClusterDefinition instances
type ClusterStorage interface {
	// Add only if the key does not exist
	AddIfNotExists(key string, cluster []byte) error

	// Adds the encoded cluster to storage if it doesn't exist. Otherwise, update the existing
	// value with the provided.
	AddOrUpdate(key string, cluster []byte) error

	// Removes a key from the cluster storage
	Remove(key string) error

	// Iterates through all key/values for the storage and calls the handler func. If a handler returns
	// an error, the iteration stops.
	Each(handler func(string, []byte) error) error

	// Closes the backing storage
	Close() error
}

// ClusterManager provides an implementation
type ClusterManager struct {
	storage ClusterStorage
	// cache   map[string]*ClusterDefinition
}

// Creates a new ClusterManager instance using the provided storage
func NewClusterManager(storage ClusterStorage) *ClusterManager {
	return &ClusterManager{
		storage: storage,
	}
}

// Creates a new ClusterManager instance using the provided storage and populates a
// yaml configured list of clusters
func NewConfiguredClusterManager(storage ClusterStorage, config string) *ClusterManager {
	clusterManager := NewClusterManager(storage)

	exists, err := fileutil.FileExists(config)
	if !exists {
		if err != nil {
			log.Errorf("Failed to load config file: %s. Error: %s", config, err.Error())
		}
		return clusterManager
	}

	data, err := os.ReadFile(config)
	if err != nil {
		return clusterManager
	}

	var entries []*ClusterConfigEntry
	err = yaml.Unmarshal(data, &entries)
	if err != nil {
		return clusterManager
	}

	for _, entry := range entries {
		details := entry.Details
		if details == nil {
			details = make(map[string]interface{})
		}

		if entry.Auth != nil {
			authData, err := getAuth(entry.Auth)
			if err != nil {
				log.Errorf("%s", err)
			} else {
				details[DetailsAuthKey] = authData
			}
		}

		clusterManager.Add(ClusterDefinition{
			ID:      entry.Name,
			Name:    entry.Name,
			Address: entry.Address,
			Details: details,
		})
	}

	return clusterManager
}

// Add Adds a cluster definition, but will not update an existing entry.
func (cm *ClusterManager) Add(cluster ClusterDefinition) (*ClusterDefinition, error) {
	// First time add
	if cluster.ID == "" {
		cluster.ID = uuid.New().String()
	}

	data, err := json.Marshal(cluster)
	if err != nil {
		return nil, err
	}

	err = cm.storage.AddIfNotExists(cluster.ID, data)
	if err != nil {
		return nil, err
	}

	return &cluster, nil
}

// AddOrUpdate will add the cluster definition if it doesn't exist, or update the existing definition
// if it does exist.
func (cm *ClusterManager) AddOrUpdate(cluster ClusterDefinition) (*ClusterDefinition, error) {
	// First time add
	if cluster.ID == "" {
		cluster.ID = uuid.New().String()
	}

	data, err := json.Marshal(cluster)
	if err != nil {
		return nil, err
	}

	err = cm.storage.AddOrUpdate(cluster.ID, data)
	if err != nil {
		return nil, err
	}

	return &cluster, nil
}

// Remove will remove a cluster definition by id.
func (cm *ClusterManager) Remove(id string) error {
	return cm.storage.Remove(id)
}

// GetAll will return all of the cluster definitions
func (cm *ClusterManager) GetAll() []*ClusterDefinition {
	clusters := []*ClusterDefinition{}

	err := cm.storage.Each(func(key string, cluster []byte) error {
		var cd ClusterDefinition
		err := json.Unmarshal(cluster, &cd)
		if err != nil {
			log.Errorf("Failed to unmarshal json cluster definition for key: %s", key)
			return nil
		}

		clusters = append(clusters, &cd)
		return nil
	})

	if err != nil {
		log.Infof("[Error] Failed to load list of clusters: %s", err.Error())
	}

	return clusters
}

// Close will close the backing database
func (cm *ClusterManager) Close() error {
	return cm.storage.Close()
}

func toBasicAuth(user, pass string) string {
	return base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", user, pass)))
}

func fileFromSecret(secretName string) string {
	return fmt.Sprintf("/var/secrets/%s/auth", secretName)
}

func fromSecret(secretName string) (string, error) {
	file := fileFromSecret(secretName)
	exists, err := fileutil.FileExists(file)
	if !exists || err != nil {
		return "", fmt.Errorf("Failed to locate secret: %s", file)
	}

	data, err := os.ReadFile(file)
	if err != nil {
		return "", fmt.Errorf("Failed to load secret: %s", file)
	}

	return base64.StdEncoding.EncodeToString(data), nil
}

func getAuth(auth *ClusterConfigEntryAuth) (string, error) {
	// We only support basic auth currently
	if !strings.EqualFold(auth.Type, "basic") {
		return "", fmt.Errorf("Authentication Type: '%s' is not supported", auth.Type)
	}

	if auth.SecretName != "" {
		return fromSecret(auth.SecretName)
	}

	if auth.Data != "" {
		return auth.Data, nil
	}

	if auth.User != "" && auth.Pass != "" {
		return toBasicAuth(auth.User, auth.Pass), nil
	}

	return "", fmt.Errorf("No valid basic auth parameters provided.")
}
