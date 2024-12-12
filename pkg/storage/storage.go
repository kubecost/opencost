package storage

import (
	"os"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/pkg/errors"
)

// DirDelim is the delimiter used to model a directory structure in an object store bucket.
const DirDelim = "/"

// DoesNotExistError is used as a generic error to return when a target path does not
// exist in storage. Equivalent to os.ErrorNotExist such that it will work with os.IsNotExist(err)
var DoesNotExistError = os.ErrNotExist

// StorageInfo is a data object containing basic information about the path in storage.
type StorageInfo struct {
	Name    string    // base name of the file
	Size    int64     // length in bytes for regular files
	ModTime time.Time // modification time
}

// Storage provides an API for storing binary data
type Storage interface {
	// StorageType returns a string identifier for the type of storage used by the implementation.
	StorageType() StorageType

	// FullPath returns the storage working path combined with the path provided
	FullPath(path string) string

	// Stat returns the StorageStats for the specific path.
	Stat(path string) (*StorageInfo, error)

	// Read uses the relative path of the storage combined with the provided path to
	// read the contents.
	Read(path string) ([]byte, error)

	// Write uses the relative path of the storage combined with the provided path
	// to write a new file or overwrite an existing file.
	Write(path string, data []byte) error

	// Remove uses the relative path of the storage combined with the provided path to
	// remove a file from storage permanently.
	Remove(path string) error

	// Exists uses the relative path of the storage combined with the provided path to
	// determine if the file exists.
	Exists(path string) (bool, error)

	// List uses the relative path of the storage combined with the provided path to return
	// storage information for the files.
	List(path string) ([]*StorageInfo, error)

	// ListDirectories uses the relative path of the storage combined with the provided path
	// to return storage information for only directories contained along the path. This
	// functions as List, but returns storage information for only directories.
	ListDirectories(path string) ([]*StorageInfo, error)
}

// Validate uses the provided storage implementation to write a test file to the store, followed by a removal.
func Validate(storage Storage) error {
	const testPath = "tmp/test.txt"
	const testContent = "test"

	log.Debug("validating storage")

	// attempt to read a path
	_, err := storage.Exists(testPath)
	if err != nil {
		return errors.Wrap(err, "Failed to check if path exists")
	}

	// attempt to write a path
	err = storage.Write(testPath, []byte(testContent))
	if err != nil {
		return errors.Wrap(err, "Failed to write data to storage")
	}

	// attempt to read the path
	data, err := storage.Read(testPath)
	if err != nil {
		return errors.Wrap(err, "Failed to read data from storage")
	}
	if string(data) != testContent {
		return errors.New("Failed to read the expected data from storage")
	}

	// delete the path
	err = storage.Remove(testPath)
	if err != nil {
		return errors.Wrap(err, "Failed to remove data from storage")
	}

	return nil
}

// IsNotExist returns true if the error provided from a storage object is DoesNotExist
func IsNotExist(err error) bool {
	if err == nil {
		return false
	}

	return err.Error() == DoesNotExistError.Error()
}
