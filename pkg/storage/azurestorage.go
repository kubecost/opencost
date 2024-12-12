package storage

// Fork from Thanos Azure Storage Bucket support to reuse configuration options
// Licensed under the Apache License 2.0
// https://github.com/thanos-io/objstore/blob/main/providers/azure/azure.go

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/opencost/opencost/core/pkg/log"

	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"gopkg.in/yaml.v2"
)

const (
	azureDefaultEndpoint = "blob.core.windows.net"
)

// Set default retry values to default Azure values. 0 = use Default Azure.
var defaultAzureConfig = AzureConfig{
	PipelineConfig: PipelineConfig{
		MaxTries:      0,
		TryTimeout:    0,
		RetryDelay:    0,
		MaxRetryDelay: 0,
	},
	ReaderConfig: ReaderConfig{
		MaxRetryRequests: 0,
	},
	HTTPConfig: AzureHTTPConfig{
		IdleConnTimeout:       model.Duration(90 * time.Second),
		ResponseHeaderTimeout: model.Duration(2 * time.Minute),
		TLSHandshakeTimeout:   model.Duration(10 * time.Second),
		ExpectContinueTimeout: model.Duration(1 * time.Second),
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   100,
		MaxConnsPerHost:       0,
		DisableCompression:    false,
	},
}

// AzureConfig Azure storage configuration.
type AzureConfig struct {
	StorageAccountName      string          `yaml:"storage_account"`
	StorageAccountKey       string          `yaml:"storage_account_key"`
	StorageConnectionString string          `yaml:"storage_connection_string"`
	ContainerName           string          `yaml:"container"`
	Endpoint                string          `yaml:"endpoint"`
	MaxRetries              int             `yaml:"max_retries"`
	MSIResource             string          `yaml:"msi_resource"`
	UserAssignedID          string          `yaml:"user_assigned_id"`
	PipelineConfig          PipelineConfig  `yaml:"pipeline_config"`
	ReaderConfig            ReaderConfig    `yaml:"reader_config"`
	HTTPConfig              AzureHTTPConfig `yaml:"http_config"`
}

type ReaderConfig struct {
	MaxRetryRequests int `yaml:"max_retry_requests"`
}

type PipelineConfig struct {
	MaxTries      int32          `yaml:"max_tries"`
	TryTimeout    model.Duration `yaml:"try_timeout"`
	RetryDelay    model.Duration `yaml:"retry_delay"`
	MaxRetryDelay model.Duration `yaml:"max_retry_delay"`
}

type AzureHTTPConfig struct {
	IdleConnTimeout       model.Duration `yaml:"idle_conn_timeout"`
	ResponseHeaderTimeout model.Duration `yaml:"response_header_timeout"`
	InsecureSkipVerify    bool           `yaml:"insecure_skip_verify"`

	TLSHandshakeTimeout   model.Duration `yaml:"tls_handshake_timeout"`
	ExpectContinueTimeout model.Duration `yaml:"expect_continue_timeout"`
	MaxIdleConns          int            `yaml:"max_idle_conns"`
	MaxIdleConnsPerHost   int            `yaml:"max_idle_conns_per_host"`
	MaxConnsPerHost       int            `yaml:"max_conns_per_host"`
	DisableCompression    bool           `yaml:"disable_compression"`

	TLSConfig TLSConfig `yaml:"tls_config"`
}

// AzureStorage implements the storage.Storage interface against Azure APIs.
type AzureStorage struct {
	name            string
	containerClient *container.Client
	config          *AzureConfig
}

// Validate checks to see if any of the config options are set.
func (conf *AzureConfig) validate() error {
	var errMsg []string
	if conf.UserAssignedID != "" && conf.StorageAccountKey != "" {
		errMsg = append(errMsg, "user_assigned_id cannot be set when using storage_account_key authentication")
	}

	if conf.UserAssignedID != "" && conf.StorageConnectionString != "" {
		errMsg = append(errMsg, "user_assigned_id cannot be set when using storage_connection_string authentication")
	}

	if conf.StorageAccountKey != "" && conf.StorageConnectionString != "" {
		errMsg = append(errMsg, "storage_account_key and storage_connection_string cannot both be set")
	}

	if conf.StorageAccountName == "" {
		errMsg = append(errMsg, "storage_account_name is required but not configured")
	}

	if conf.ContainerName == "" {
		errMsg = append(errMsg, "no container specified")
	}

	if conf.PipelineConfig.MaxTries < 0 {
		errMsg = append(errMsg, "The value of max_tries must be greater than or equal to 0 in the config file")
	}

	if conf.ReaderConfig.MaxRetryRequests < 0 {
		errMsg = append(errMsg, "The value of max_retry_requests must be greater than or equal to 0 in the config file")
	}

	if len(errMsg) > 0 {
		return errors.New(strings.Join(errMsg, ", "))
	}

	return nil
}

// parseAzureConfig unmarshals a buffer into a Config with default values.
func parseAzureConfig(conf []byte) (AzureConfig, error) {
	config := defaultAzureConfig
	if err := yaml.UnmarshalStrict(conf, &config); err != nil {
		return AzureConfig{}, err
	}

	// If we don't have config specific retry values but we do have the generic MaxRetries.
	// This is for backwards compatibility but also ease of configuration.
	if config.MaxRetries > 0 {
		if config.PipelineConfig.MaxTries == 0 {
			config.PipelineConfig.MaxTries = int32(config.MaxRetries)
		}
		if config.ReaderConfig.MaxRetryRequests == 0 {
			config.ReaderConfig.MaxRetryRequests = config.MaxRetries
		}
	}

	return config, nil
}

// NewAzureStorage returns a new Storage using the provided Azure config.
func NewAzureStorage(azureConfig []byte) (*AzureStorage, error) {
	log.Debugf("Creating new Azure Bucket Connection")

	conf, err := parseAzureConfig(azureConfig)
	if err != nil {
		return nil, fmt.Errorf("error parsing azure storage config: %w", err)
	}

	return NewAzureStorageWith(conf)
}

// NewAzureStorageWith returns a new Storage using the provided Azure config struct.
func NewAzureStorageWith(conf AzureConfig) (*AzureStorage, error) {
	if err := conf.validate(); err != nil {
		return nil, fmt.Errorf("error validating azure storage config: %w", err)
	}

	containerClient, err := getContainerClient(conf)
	if err != nil {
		return nil, fmt.Errorf("error retrieving container client: %w", err)
	}

	// Check if storage account container already exists, and create one if it does not.
	ctx := context.Background()
	_, err = containerClient.GetProperties(ctx, &container.GetPropertiesOptions{})
	if err != nil {
		if !bloberror.HasCode(err, bloberror.ContainerNotFound) {
			return nil, err
		}
		_, err := containerClient.Create(ctx, nil)
		if err != nil {
			return nil, errors.Wrapf(err, "error creating Azure blob container: %s", conf.ContainerName)
		}
		log.Infof("Azure blob container successfully created %s", conf.ContainerName)
	}

	return &AzureStorage{
		name:            conf.ContainerName,
		containerClient: containerClient,
		config:          &conf,
	}, nil
}

// Name returns the bucket name for azure storage.
func (as *AzureStorage) Name() string {
	return as.name
}

// StorageType returns a string identifier for the type of storage used by the implementation.
func (as *AzureStorage) StorageType() StorageType {
	return StorageTypeBucketAzure
}

// FullPath returns the storage working path combined with the path provided
func (as *AzureStorage) FullPath(name string) string {
	name = trimLeading(name)

	return name
}

// Stat returns the StorageStats for the specific path.
func (b *AzureStorage) Stat(name string) (*StorageInfo, error) {
	name = trimLeading(name)
	ctx := context.Background()
	blobClient := b.containerClient.NewBlobClient(name)
	props, err := blobClient.GetProperties(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("error retrieving blob properties: %w", err)
	}

	return &StorageInfo{
		Name:    trimName(name),
		Size:    *props.ContentLength,
		ModTime: *props.LastModified,
	}, nil
}

// Read uses the relative path of the storage combined with the provided path to
// read the contents.
func (b *AzureStorage) Read(name string) ([]byte, error) {
	name = trimLeading(name)
	ctx := context.Background()

	log.Debugf("AzureStorage::Read(%s)", name)

	downloadResponse, err := b.containerClient.NewBlobClient(name).DownloadStream(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("AzureStorage: Read: failed to download %w", err)
	}
	// NOTE: automatically retries are performed if the connection fails
	retryReader := downloadResponse.NewRetryReader(ctx, &azblob.RetryReaderOptions{
		MaxRetries: int32(b.config.ReaderConfig.MaxRetryRequests),
	})
	defer retryReader.Close()

	// read the body into a buffer
	downloadedData := bytes.Buffer{}

	_, err = downloadedData.ReadFrom(retryReader)
	if err != nil {
		return nil, fmt.Errorf("AzureStorage: Read: failed to read downloaded data %w", err)
	}

	return downloadedData.Bytes(), nil
}

// Write uses the relative path of the storage combined with the provided path
// to write a new file or overwrite an existing file.
func (b *AzureStorage) Write(name string, data []byte) error {
	name = trimLeading(name)
	ctx := context.Background()

	log.Debugf("AzureStorage::Write(%s)", name)

	r := bytes.NewReader(data)
	blobClient := b.containerClient.NewBlockBlobClient(name)
	opts := &blockblob.UploadStreamOptions{
		BlockSize:   3 * 1024 * 1024,
		Concurrency: 4,
	}
	if _, err := blobClient.UploadStream(ctx, r, opts); err != nil {
		return errors.Wrapf(err, "cannot upload Azure blob, address: %s", name)
	}
	return nil
}

// Remove uses the relative path of the storage combined with the provided path to
// remove a file from storage permanently.
func (b *AzureStorage) Remove(name string) error {
	name = trimLeading(name)

	log.Debugf("AzureStorage::Remove(%s)", name)
	ctx := context.Background()

	blobClient := b.containerClient.NewBlobClient(name)
	opt := &blob.DeleteOptions{
		DeleteSnapshots: to.Ptr(blob.DeleteSnapshotsOptionTypeInclude),
	}
	if _, err := blobClient.Delete(ctx, opt); err != nil {
		return errors.Wrapf(err, "error deleting blob, address: %s", name)
	}
	return nil
}

// Exists uses the relative path of the storage combined with the provided path to
// determine if the file exists.
func (b *AzureStorage) Exists(name string) (bool, error) {
	name = trimLeading(name)
	ctx := context.Background()
	blobClient := b.containerClient.NewBlobClient(name)
	if _, err := blobClient.GetProperties(ctx, nil); err != nil {
		if b.IsObjNotFoundErr(err) {
			return false, nil
		}
		return false, errors.Wrapf(err, "cannot get properties for Azure blob, address: %s", name)
	}
	return true, nil
}

// List uses the relative path of the storage combined with the provided path to return
// storage information for the files.
func (b *AzureStorage) List(path string) ([]*StorageInfo, error) {
	path = trimLeading(path)

	log.Debugf("AzureStorage::List(%s)", path)
	ctx := context.Background()

	// Ensure the object name actually ends with a dir suffix. Otherwise we'll just iterate the
	// object itself as one prefix item.
	if path != "" {
		path = strings.TrimSuffix(path, DirDelim) + DirDelim
	}

	var stats []*StorageInfo
	list := b.containerClient.NewListBlobsHierarchyPager(DirDelim, &container.ListBlobsHierarchyOptions{
		Prefix: &path,
	})
	for list.More() {
		page, err := list.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve page: %s", err)
		}
		segment := page.ListBlobsHierarchySegmentResponse.Segment
		if segment == nil {
			continue
		}
		for _, blob := range segment.BlobItems {
			if blob.Name == nil {
				continue
			}
			if blob.Properties == nil {
				continue
			}
			stats = append(stats, &StorageInfo{
				Name:    trimName(*blob.Name),
				Size:    *blob.Properties.ContentLength,
				ModTime: *blob.Properties.LastModified,
			})
		}
	}

	return stats, nil
}

func (b *AzureStorage) ListDirectories(path string) ([]*StorageInfo, error) {
	path = trimLeading(path)

	log.Debugf("AzureStorage::ListDirectories(%s)", path)
	ctx := context.Background()

	// Ensure the object name actually ends with a dir suffix. Otherwise we'll just iterate the
	// object itself as one prefix item.
	if path != "" {
		path = strings.TrimSuffix(path, DirDelim) + DirDelim
	}

	var stats []*StorageInfo
	list := b.containerClient.NewListBlobsHierarchyPager(DirDelim, &container.ListBlobsHierarchyOptions{
		Prefix: &path,
	})
	for list.More() {
		page, err := list.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve page: %s", err)
		}
		segment := page.ListBlobsHierarchySegmentResponse.Segment
		if segment == nil {
			continue
		}
		for _, dir := range segment.BlobPrefixes {
			if dir.Name == nil {
				continue
			}

			stats = append(stats, &StorageInfo{
				Name: *dir.Name,
			})
		}
	}

	return stats, nil
}

// IsObjNotFoundErr returns true if error means that object is not found. Relevant to Get operations.
func (b *AzureStorage) IsObjNotFoundErr(err error) bool {
	if err == nil {
		return false
	}
	return bloberror.HasCode(err, bloberror.BlobNotFound) || bloberror.HasCode(err, bloberror.InvalidURI)
}

// IsAccessDeniedErr returns true if access to object is denied.
func (b *AzureStorage) IsAccessDeniedErr(err error) bool {
	if err == nil {
		return false
	}
	return bloberror.HasCode(err, bloberror.AuthorizationPermissionMismatch) || bloberror.HasCode(err, bloberror.InsufficientAccountPermissions)
}

func DefaultAzureTransport(config AzureConfig) (*http.Transport, error) {
	tlsConfig, err := NewTLSConfig(&config.HTTPConfig.TLSConfig)
	if err != nil {
		return nil, fmt.Errorf("error creating TLS config: %w", err)
	}

	if config.HTTPConfig.InsecureSkipVerify {
		tlsConfig.InsecureSkipVerify = true
	}
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,

		MaxIdleConns:          config.HTTPConfig.MaxIdleConns,
		MaxIdleConnsPerHost:   config.HTTPConfig.MaxIdleConnsPerHost,
		IdleConnTimeout:       time.Duration(config.HTTPConfig.IdleConnTimeout),
		MaxConnsPerHost:       config.HTTPConfig.MaxConnsPerHost,
		TLSHandshakeTimeout:   time.Duration(config.HTTPConfig.TLSHandshakeTimeout),
		ExpectContinueTimeout: time.Duration(config.HTTPConfig.ExpectContinueTimeout),

		ResponseHeaderTimeout: time.Duration(config.HTTPConfig.ResponseHeaderTimeout),
		DisableCompression:    config.HTTPConfig.DisableCompression,
		TLSClientConfig:       tlsConfig,
	}, nil
}

func getContainerClient(conf AzureConfig) (*container.Client, error) {
	dt, err := DefaultAzureTransport(conf)
	if err != nil {
		return nil, fmt.Errorf("error creating default transport: %w", err)
	}
	opt := &container.ClientOptions{
		ClientOptions: azcore.ClientOptions{
			Retry: policy.RetryOptions{
				MaxRetries:    conf.PipelineConfig.MaxTries,
				TryTimeout:    time.Duration(conf.PipelineConfig.TryTimeout),
				RetryDelay:    time.Duration(conf.PipelineConfig.RetryDelay),
				MaxRetryDelay: time.Duration(conf.PipelineConfig.MaxRetryDelay),
			},
			Telemetry: policy.TelemetryOptions{
				ApplicationID: "Thanos",
			},
			Transport: &http.Client{Transport: dt},
		},
	}

	// Use connection string if set
	if conf.StorageConnectionString != "" {
		containerClient, err := container.NewClientFromConnectionString(conf.StorageConnectionString, conf.ContainerName, opt)
		if err != nil {
			return nil, fmt.Errorf("error creating client from connection string: %w", err)
		}
		return containerClient, nil
	}

	if conf.Endpoint == "" {
		conf.Endpoint = "blob.core.windows.net"
	}

	containerURL := fmt.Sprintf("https://%s.%s/%s", conf.StorageAccountName, conf.Endpoint, conf.ContainerName)

	// Use shared keys if set
	if conf.StorageAccountKey != "" {
		cred, err := container.NewSharedKeyCredential(conf.StorageAccountName, conf.StorageAccountKey)
		if err != nil {
			return nil, fmt.Errorf("error getting shared key credential: %w", err)
		}
		containerClient, err := container.NewClientWithSharedKeyCredential(containerURL, cred, opt)
		if err != nil {
			return nil, fmt.Errorf("error creating client with shared key credential: %w", err)
		}
		return containerClient, nil
	}

	// Otherwise use a token credential
	var cred azcore.TokenCredential

	// Use Managed Identity Credential if a user assigned ID is set
	if conf.UserAssignedID != "" {
		msiOpt := &azidentity.ManagedIdentityCredentialOptions{}
		msiOpt.ID = azidentity.ClientID(conf.UserAssignedID)
		cred, err = azidentity.NewManagedIdentityCredential(msiOpt)
	} else {
		// Otherwise use Default Azure Credential
		cred, err = azidentity.NewDefaultAzureCredential(nil)
	}

	if err != nil {
		return nil, fmt.Errorf("error creating token credential: %w", err)
	}

	containerClient, err := container.NewClient(containerURL, cred, opt)
	if err != nil {
		return nil, fmt.Errorf("error creating client from token credential: %w", err)
	}

	return containerClient, nil
}
