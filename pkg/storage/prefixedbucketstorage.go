package storage

// Partially forked from Thanos prefixed bucket for prefix handling
// Licensed under the Apache License 2.0
// https://github.com/thanos-io/objstore/blob/main/prefixed_bucket.go

import (
	"strings"

	"github.com/pkg/errors"
)

type PrefixedBucketStorage struct {
	storage Storage
	prefix  string
}

func NewPrefixedBucketStorage(storage Storage, prefix string) (Storage, error) {
	if validPrefix(prefix) {
		return &PrefixedBucketStorage{
			storage: storage,
			prefix:  strings.Trim(prefix, DirDelim),
		}, nil
	}

	return storage, errors.Errorf("failed to create prefixed storage: invalid prefix '%s'", prefix)
}

func validPrefix(prefix string) bool {
	prefix = strings.Replace(prefix, "/", "", -1)
	return len(prefix) > 0
}

func conditionalPrefix(prefix, name string) string {
	if len(name) > 0 && !strings.HasPrefix(name, prefix) {
		return withPrefix(prefix, name)
	}

	return name
}

func withPrefix(prefix, name string) string {
	return prefix + DirDelim + name
}

func (pbs *PrefixedBucketStorage) StorageType() StorageType {
	return pbs.storage.StorageType()
}

// FullPath returns the storage working path combined with the path provided.
func (pbs *PrefixedBucketStorage) FullPath(name string) string {
	return pbs.storage.FullPath(conditionalPrefix(pbs.prefix, name))
}

// Exists checks if the given object exists.
func (pbs *PrefixedBucketStorage) Exists(name string) (bool, error) {
	return pbs.storage.Exists(conditionalPrefix(pbs.prefix, name))
}

// List returns storage information for files on the provided path.
func (pbs *PrefixedBucketStorage) List(path string) ([]*StorageInfo, error) {
	return pbs.storage.List(conditionalPrefix(pbs.prefix, path))
}

// ListDirectories returns storage information for only directories on the provided path.
func (pbs *PrefixedBucketStorage) ListDirectories(path string) ([]*StorageInfo, error) {
	return pbs.storage.ListDirectories(conditionalPrefix(pbs.prefix, path))
}

// Read returns a reader for the given object name.
func (pbs *PrefixedBucketStorage) Read(name string) ([]byte, error) {
	return pbs.storage.Read(conditionalPrefix(pbs.prefix, name))
}

// Remove deletes the object with the given name.
func (pbs *PrefixedBucketStorage) Remove(name string) error {
	return pbs.storage.Remove(conditionalPrefix(pbs.prefix, name))
}

// Stat returns information about the specified object.
func (pbs *PrefixedBucketStorage) Stat(name string) (*StorageInfo, error) {
	return pbs.storage.Stat(conditionalPrefix(pbs.prefix, name))
}

// Write uploads the contents of the reader as an object into the bucket.
func (pbs *PrefixedBucketStorage) Write(name string, data []byte) error {
	return pbs.storage.Write(conditionalPrefix(pbs.prefix, name), data)
}
