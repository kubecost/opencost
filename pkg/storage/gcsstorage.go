package storage

// Fork from Thanos GCS Bucket support to reuse configuration options
// Licensed under the Apache License 2.0.
// https://github.com/thanos-io/thanos/blob/main/pkg/objstore/gcs/gcs.go

import (
	"context"
	"io"
	"strings"

	gcs "cloud.google.com/go/storage"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/pkg/errors"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"gopkg.in/yaml.v2"
)

// Config stores the configuration for gcs bucket.
type GCSConfig struct {
	Bucket         string `yaml:"bucket"`
	ServiceAccount string `yaml:"service_account"`
}

// GCSStorage is a storage.Storage implementation for Google Cloud Storage.
type GCSStorage struct {
	name   string
	bucket *gcs.BucketHandle
	client *gcs.Client
}

// NewGCSStorage creates a new GCSStorage instance using the provided GCS configuration.
func NewGCSStorage(conf []byte) (*GCSStorage, error) {
	var gc GCSConfig
	if err := yaml.Unmarshal(conf, &gc); err != nil {
		return nil, err
	}

	return NewGCSStorageWith(gc)
}

// NewGCSStorageWith creates a new GCSStorage instance using the provided GCS configuration.
func NewGCSStorageWith(gc GCSConfig) (*GCSStorage, error) {
	if gc.Bucket == "" {
		return nil, errors.New("missing Google Cloud Storage bucket name for stored blocks")
	}

	ctx := context.Background()
	var opts []option.ClientOption

	// If ServiceAccount is provided, use them in GCS client, otherwise fallback to Google default logic.
	if gc.ServiceAccount != "" {
		credentials, err := google.CredentialsFromJSON(ctx, []byte(gc.ServiceAccount), gcs.ScopeFullControl)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create credentials from JSON")
		}
		opts = append(opts, option.WithCredentials(credentials))
	}

	gcsClient, err := gcs.NewClient(ctx, opts...)
	if err != nil {
		return nil, err
	}

	return &GCSStorage{
		name:   gc.Bucket,
		bucket: gcsClient.Bucket(gc.Bucket),
		client: gcsClient,
	}, nil
}

// Name returns the bucket name for gcs.
func (gs *GCSStorage) Name() string {
	return gs.name
}

// StorageType returns a string identifier for the type of storage used by the implementation.
func (gs *GCSStorage) StorageType() StorageType {
	return StorageTypeBucketGCS
}

// FullPath returns the storage working path combined with the path provided
func (gs *GCSStorage) FullPath(name string) string {
	name = trimLeading(name)

	return name
}

// Stat returns the StorageStats for the specific path.
func (gs *GCSStorage) Stat(name string) (*StorageInfo, error) {
	name = trimLeading(name)
	//log.Debugf("GCSStorage::Stat(%s)", name)]

	ctx := context.Background()
	attrs, err := gs.bucket.Object(name).Attrs(ctx)
	if err != nil {
		if gs.isDoesNotExist(err) {
			return nil, DoesNotExistError
		}
		return nil, err
	}

	return &StorageInfo{
		Name:    trimName(attrs.Name),
		Size:    attrs.Size,
		ModTime: attrs.Updated,
	}, nil
}

// isDoesNotExist returns true if the error matches resource not exists errors.
func (gs *GCSStorage) isDoesNotExist(err error) bool {
	msg := err.Error()
	return msg == gcs.ErrBucketNotExist.Error() || msg == gcs.ErrObjectNotExist.Error()
}

// Read uses the relative path of the storage combined with the provided path to
// read the contents.
func (gs *GCSStorage) Read(name string) ([]byte, error) {
	name = trimLeading(name)
	log.Debugf("GCSStorage::Read(%s)", name)

	ctx := context.Background()
	reader, err := gs.bucket.Object(name).NewReader(ctx)
	if err != nil {
		return nil, err
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// Write uses the relative path of the storage combined with the provided path
// to write a new file or overwrite an existing file.
func (gs *GCSStorage) Write(name string, data []byte) error {
	name = trimLeading(name)
	log.Debugf("GCSStorage::Write(%s)", name)

	ctx := context.Background()

	writer := gs.bucket.Object(name).NewWriter(ctx)
	// Set chunksize to 0 to write files in one go. This prevents chunking of
	// upload into multiple parts, which requires additional memory for buffering
	// the sub-parts. To remain consistent with other storage implementations,
	// we would rather attempt to lower cost fast upload and fast-fail.
	writer.ChunkSize = 0

	// Write the data to GCS object
	if _, err := writer.Write(data); err != nil {
		return errors.Wrap(err, "upload gcs object")
	}

	// NOTE: Sometimes errors don't arrive during Write(), so we must also check
	// NOTE: the error returned by Close().
	if err := writer.Close(); err != nil {
		return errors.Wrap(err, "upload gcs object")
	}
	return nil
}

// Remove uses the relative path of the storage combined with the provided path to
// remove a file from storage permanently.
func (gs *GCSStorage) Remove(name string) error {
	name = trimLeading(name)

	log.Debugf("GCSStorage::Remove(%s)", name)
	ctx := context.Background()

	return gs.bucket.Object(name).Delete(ctx)
}

// Exists uses the relative path of the storage combined with the provided path to
// determine if the file exists.
func (gs *GCSStorage) Exists(name string) (bool, error) {
	name = trimLeading(name)
	//log.Debugf("GCSStorage::Exists(%s)", name)

	ctx := context.Background()
	_, err := gs.bucket.Object(name).Attrs(ctx)
	if err != nil {
		if gs.isDoesNotExist(err) {
			return false, nil
		}
		return false, errors.Wrap(err, "stat gcs object")
	}

	return true, nil
}

// List uses the relative path of the storage combined with the provided path to return
// storage information for the files.
func (gs *GCSStorage) List(path string) ([]*StorageInfo, error) {
	path = trimLeading(path)

	log.Debugf("GCSStorage::List(%s)", path)
	ctx := context.Background()

	// Ensure the object name actually ends with a dir suffix. Otherwise we'll just iterate the
	// object itself as one prefix item.
	if path != "" {
		path = strings.TrimSuffix(path, DirDelim) + DirDelim
	}

	it := gs.bucket.Objects(ctx, &gcs.Query{
		Prefix:    path,
		Delimiter: DirDelim,
	})

	// iterate over the objects at the path, collect storage info
	var stats []*StorageInfo
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, errors.Wrap(err, "list gcs objects")
		}

		// ignore the root path directory
		if attrs.Name == path {
			continue
		}

		stats = append(stats, &StorageInfo{
			Name:    trimName(attrs.Name),
			Size:    attrs.Size,
			ModTime: attrs.Updated,
		})
	}

	return stats, nil
}

func (gs *GCSStorage) ListDirectories(path string) ([]*StorageInfo, error) {
	path = trimLeading(path)

	log.Debugf("GCSStorage::List(%s)", path)
	ctx := context.Background()

	// Ensure the object name actually ends with a dir suffix. Otherwise we'll just iterate the
	// object itself as one prefix item.
	if path != "" {
		path = strings.TrimSuffix(path, DirDelim) + DirDelim
	}

	it := gs.bucket.Objects(ctx, &gcs.Query{
		Prefix:    path,
		Delimiter: DirDelim,
	})

	// iterate over the objects at the path, collect storage info
	var stats []*StorageInfo
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, errors.Wrap(err, "list gcs objects")
		}

		// ignore the root path directory
		if attrs.Name == path {
			continue
		}

		// We filter directories using DirDelim, so a nameless entry is a dir
		// See gcs.ObjectAttrs Prefix property
		if attrs.Name == "" {
			stats = append(stats, &StorageInfo{
				Name:    attrs.Prefix,
				Size:    attrs.Size,
				ModTime: attrs.Updated,
			})
		}

	}

	return stats, nil
}
