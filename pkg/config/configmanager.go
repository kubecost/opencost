package config

import (
	"sync"

	"github.com/opencost/opencost/pkg/storage"
)

//--------------------------------------------------------------------------
//  ConfigFileManagerOpts
//--------------------------------------------------------------------------

// ConfigFileManagerOpts describes how to configure the ConfigFileManager for
// serving configuration files
type ConfigFileManagerOpts struct {
	// BucketStoreConfig is the local file location for the configuration used to
	// write and read configuration data to/from the bucket. The format of this
	// configuration file should be compatible with storage.NewBucketStorage
	BucketStoreConfig string

	// LocalConfigPath provides a backup location for storing the configuration
	// files
	LocalConfigPath string
}

// IsBucketStorageEnabled returns true if bucket storage is enabled.
func (cfmo *ConfigFileManagerOpts) IsBucketStorageEnabled() bool {
	return cfmo.BucketStoreConfig != ""
}

// DefaultConfigFileManagerOpts returns the default configuration options for the
// config file manager
func DefaultConfigFileManagerOpts() *ConfigFileManagerOpts {
	return &ConfigFileManagerOpts{
		BucketStoreConfig: "",
		LocalConfigPath:   "/",
	}
}

//--------------------------------------------------------------------------
//  ConfigFileManager
//--------------------------------------------------------------------------

// ConfigFileManager is a fascade for a central API used to create and watch
// config files.
type ConfigFileManager struct {
	lock  *sync.Mutex
	store *storage.FileStorage
	files map[string]*ConfigFile
}

// NewConfigFileManager creates a new backing storage and configuration file manager
func NewConfigFileManager(opts *ConfigFileManagerOpts) *ConfigFileManager {
	if opts == nil {
		opts = DefaultConfigFileManagerOpts()
	}

	return &ConfigFileManager{
		lock:  new(sync.Mutex),
		store: storage.NewFileStorage(opts.LocalConfigPath),
		files: make(map[string]*ConfigFile),
	}
}

// ConfigFileAt returns an existing configuration file for the provided path if it exists. Otherwise,
// a new instance is created and returned. Note that the path does not have to exist in order for the
// instance to be created. It can exist as a potential file path on the storage, and be written to
// later
func (cfm *ConfigFileManager) ConfigFileAt(path string) *ConfigFile {
	cfm.lock.Lock()
	defer cfm.lock.Unlock()
	if cf, ok := cfm.files[path]; ok {
		return cf
	}

	cf := NewConfigFile(cfm.store, path)
	cfm.files[path] = cf
	return cf
}
