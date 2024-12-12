package config

import (
	"errors"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util/atomic"
	"github.com/opencost/opencost/pkg/storage"
)

// HandlerID is a unique identifier assigned to a provided ConfigChangedHandler. This is used to remove a handler
// from the ConfigFile when it is no longer needed.
type HandlerID string

//--------------------------------------------------------------------------
//  ChangeType
//--------------------------------------------------------------------------

// ChangeType is used to specifically categorize the change that was made on a ConfigFile
type ChangeType string

// ChangeType constants contain the different types of updates passed through the ConfigChangedHandler
const (
	ChangeTypeCreated  ChangeType = "created"
	ChangeTypeModified ChangeType = "modified"
	ChangeTypeDeleted  ChangeType = "deleted"
)

//--------------------------------------------------------------------------
//  ConfigChangedHandler
//--------------------------------------------------------------------------

// ConfigChangedHandler is the func handler used to receive change updates about the
// config file. Both ChangeTypeCreated and ChangeTypeModified yield a valid []byte, while
// ChangeTypeDeleted yields a nil []byte.
type ConfigChangedHandler func(ChangeType, []byte)

//--------------------------------------------------------------------------
//  ConfigFile
//--------------------------------------------------------------------------

// DefaultHandlerPriority is used as the priority for any handlers added via AddChangeHandler
const DefaultHandlerPriority int = 1000

// NoBackingStore error is used when the config file's backing storage is missing
var NoBackingStore error = errors.New("Backing storage does not exist.")

// ConfigFile is representation of a configuration file that can be written to, read, and watched
// for updates
type ConfigFile struct {
	store      storage.Storage
	file       string
	dataLock   *sync.Mutex
	data       []byte
	watchLock  *sync.Mutex
	watchers   []*pHandler
	runState   atomic.AtomicRunState
	lastChange time.Time
}

// NewConfigFile creates a new ConfigFile instance using a specific storage.Storage and path relative
// to the storage.
func NewConfigFile(store storage.Storage, file string) *ConfigFile {
	return &ConfigFile{
		store:     store,
		file:      file,
		dataLock:  new(sync.Mutex),
		data:      nil,
		watchLock: new(sync.Mutex),
	}
}

// Path returns the fully qualified path of the config file.
func (cf *ConfigFile) Path() string {
	if cf.store == nil {
		return cf.file
	}

	return cf.store.FullPath(cf.file)
}

// Write will write the binary data to the file.
func (cf *ConfigFile) Write(data []byte) error {
	if cf.store == nil {
		return NoBackingStore
	}

	e := cf.store.Write(cf.file, data)
	// update cache on successful write
	if e == nil {
		cf.dataLock.Lock()
		cf.data = data
		cf.dataLock.Unlock()
	}
	return e
}

// Read will read the binary data from the file and return it. If an error is returned,
// the byte array will be nil.
func (cf *ConfigFile) Read() ([]byte, error) {
	return cf.internalRead(false)
}

// internalRead is used to allow a forced override of data cache to refresh data
func (cf *ConfigFile) internalRead(force bool) ([]byte, error) {
	if cf.store == nil {
		return nil, NoBackingStore
	}

	cf.dataLock.Lock()
	defer cf.dataLock.Unlock()
	if !force {
		if cf.data != nil {
			return cf.data, nil
		}
	}

	d, e := cf.store.Read(cf.file)
	if e != nil {
		return nil, e
	}
	cf.data = d
	return cf.data, nil
}

// Stat returns the StorageStats for the file.
func (cf *ConfigFile) Stat() (*storage.StorageInfo, error) {
	if cf.store == nil {
		return nil, NoBackingStore
	}

	return cf.store.Stat(cf.file)
}

// Exists returns true if the file exist. If an error other than a NotExist error is returned,
// the result will be false with the provided error.
func (cf *ConfigFile) Exists() (bool, error) {
	if cf.store == nil {
		return false, NoBackingStore
	}

	return cf.store.Exists(cf.file)
}

// Delete removes the file from storage permanently.
func (cf *ConfigFile) Delete() error {
	if cf.store == nil {
		return NoBackingStore
	}

	e := cf.store.Remove(cf.file)

	// on removal, clear data cache
	if e == nil {
		cf.dataLock.Lock()
		cf.data = nil
		cf.dataLock.Unlock()
	}
	return e
}

// Refresh allows external callers to force reload the config file from internal storage. This is
// particularly useful when there exist no change listeners on the config, which would prevent the
// data cache from automatically updating on change
func (cf *ConfigFile) Refresh() ([]byte, error) {
	return cf.internalRead(true)
}

// AddChangeHandler accepts a ConfigChangedHandler function which will be called whenever the implementation
// detects that a change has been made. A unique HandlerID is returned that can be used to remove the handler
// if necessary.
func (cf *ConfigFile) AddChangeHandler(handler ConfigChangedHandler) HandlerID {
	return cf.AddPriorityChangeHandler(handler, DefaultHandlerPriority)
}

// AddPriorityChangeHandler allows adding a config change handler with a specific priority. By default,
// any handlers added via AddChangeHandler have a default priority of 1000. The lower the priority, the
// sooner in the handler execution it will be called.
func (cf *ConfigFile) AddPriorityChangeHandler(handler ConfigChangedHandler, priority int) HandlerID {
	cf.watchLock.Lock()
	defer cf.watchLock.Unlock()

	h := &pHandler{
		id:       HandlerID(uuid.NewString()),
		handler:  handler,
		priority: priority,
	}

	cf.watchers = append(cf.watchers, h)

	// create the actual file watcher once we have at least one active watcher func registered
	if len(cf.watchers) == 1 {
		cf.runWatcher()
	}

	return h.id
}

// RemoveChangeHandler removes the change handler with the provided identifier if it exists. True
// is returned if the handler was removed (it existed), false otherwise.
func (cf *ConfigFile) RemoveChangeHandler(id HandlerID) bool {
	cf.watchLock.Lock()
	defer cf.watchLock.Unlock()

	for i := range cf.watchers {
		if cf.watchers[i] != nil && cf.watchers[i].id == id {
			copy(cf.watchers[i:], cf.watchers[i+1:])
			cf.watchers[len(cf.watchers)-1] = nil
			cf.watchers = cf.watchers[:len(cf.watchers)-1]

			// stop watching the file for changes if there are no more external watchers
			if len(cf.watchers) == 0 {
				cf.stopWatcher()
			}

			return true
		}
	}
	return false
}

// RemoveAllHandlers removes all added handlers
func (cf *ConfigFile) RemoveAllHandlers() {
	cf.watchLock.Lock()
	defer cf.watchLock.Unlock()

	cf.watchers = nil

	cf.stopWatcher()
}

// runWatcher creates a go routine which will poll the stat of a storage target on a specific
// interval and dispatch created, modified, and deleted events for that file.
func (cf *ConfigFile) runWatcher() {
	// we wait for a reset on the run state prior to starting, which
	// will only block iff the run state is in the process of stopping
	cf.runState.WaitForReset()

	// if start fails after waiting for a reset, it means that another thread
	// beat this thread to the start
	if !cf.runState.Start() {
		log.Warnf("Run watcher already running for file: %s", cf.file)
		return
	}

	go func() {
		first := true

		var last time.Time
		var exists bool

		for {
			// Each iteration, check for the stop trigger, or wait 10 seconds
			select {
			case <-cf.runState.OnStop():
				cf.runState.Reset()
				return
			case <-time.After(10 * time.Second):
			}

			// Query stat on the file, on errors other than exists,
			// we'll need to log the error, and perhaps limit the retries
			st, err := cf.Stat()
			if err != nil && !os.IsNotExist(err) {
				log.Errorf("Storage Stat Error: %s", err)
				continue
			}

			// On first iteration, set exists and last modification time (if applicable)
			// and flip flag
			if first {
				exists = !os.IsNotExist(err)
				if exists {
					last = st.ModTime
				}
				first = false
				continue
			}

			// File does not exist in storage, need to check to see if that is different
			// from last state check
			if os.IsNotExist(err) {
				// check to see if the file has gone from exists to !exists
				if exists {
					exists = false
					cf.onFileChange(ChangeTypeDeleted, nil)
				}
				continue
			}

			// check to see if the file has gone from !exists to exists
			if !exists {
				data, err := cf.internalRead(true)
				if err != nil {
					log.Warnf("Read() Error: %s\n", err)
					continue
				}
				exists = true
				last = st.ModTime
				cf.onFileChange(ChangeTypeCreated, data)
				continue
			}

			mtime := st.ModTime
			if mtime != last {
				last = mtime
				data, err := cf.internalRead(true)
				if err != nil {
					log.Errorf("Read() Error: %s\n", err)
					continue
				}
				cf.onFileChange(ChangeTypeModified, data)
			}
		}
	}()
}

// stopWatcher closes the stop channel, returning from the runWatcher go routine. Allows us
// to remove any polling stat checks on files when there are no change handlers.
func (cf *ConfigFile) stopWatcher() {
	cf.runState.Stop()
}

// onFileChange is internally called when the core watcher recognizes a change in the ConfigFile. This
// method dispatches that change to all added watchers
func (cf *ConfigFile) onFileChange(changeType ChangeType, newData []byte) {
	// On change, we copy out the handlers to a separate slice for processing for a few reasons:
	// 1. We don't want to lock while executing the handlers
	// 2. Handlers may want to operate on the ConfigFile instance, which would result in a deadlock
	// 3. Allows us to implement priority sorting outside of the lock as well
	cf.watchLock.Lock()
	if len(cf.watchers) == 0 {
		cf.watchLock.Unlock()
		return
	}

	toNotify := make([]*pHandler, len(cf.watchers))
	copy(toNotify, cf.watchers)
	cf.watchLock.Unlock()

	sort.SliceStable(toNotify, func(i, j int) bool {
		return toNotify[i].priority < toNotify[j].priority
	})

	for _, handler := range toNotify {
		handler.handler(changeType, newData)
	}
}

//--------------------------------------------------------------------------
//  pHandler
//--------------------------------------------------------------------------

// pHandler is a wrapper type used to assign a ConfigChangedHandler a unique identifier and priority.
type pHandler struct {
	id       HandlerID
	handler  ConfigChangedHandler
	priority int
}
