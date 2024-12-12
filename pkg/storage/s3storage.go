package storage

// Fork from Thanos S3 Bucket support to reuse configuration options
// Licensed under the Apache License 2.0
// https://github.com/thanos-io/thanos/blob/main/pkg/objstore/s3/s3.go

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/log"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/encrypt"
	"github.com/pkg/errors"

	"gopkg.in/yaml.v2"
)

type ctxKey int

const (
	// SSEKMS is the name of the SSE-KMS method for objectstore encryption.
	SSEKMS = "SSE-KMS"

	// SSEC is the name of the SSE-C method for objstore encryption.
	SSEC = "SSE-C"

	// SSES3 is the name of the SSE-S3 method for objstore encryption.
	SSES3 = "SSE-S3"

	// sseConfigKey is the context key to override SSE config. This feature is used by downstream
	// projects (eg. Cortex) to inject custom SSE config on a per-request basis. Future work or
	// refactoring can introduce breaking changes as far as the functionality is preserved.
	// NOTE: we're using a context value only because it's a very specific S3 option. If SSE will
	// be available to wider set of backends we should probably add a variadic option to Get() and Upload().
	sseConfigKey = ctxKey(0)
)

var defaultS3Config = S3Config{
	PutUserMetadata: map[string]string{},
	HTTPConfig: S3HTTPConfig{
		IdleConnTimeout:       time.Duration(90 * time.Second),
		ResponseHeaderTimeout: time.Duration(2 * time.Minute),
		TLSHandshakeTimeout:   time.Duration(10 * time.Second),
		ExpectContinueTimeout: time.Duration(1 * time.Second),
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   100,
		MaxConnsPerHost:       0,
	},
	PartSize: 1024 * 1024 * 64, // 64MB.
}

// Config stores the configuration for s3 bucket.
type S3Config struct {
	Bucket             string            `yaml:"bucket"`
	Endpoint           string            `yaml:"endpoint"`
	Region             string            `yaml:"region"`
	AWSSDKAuth         bool              `yaml:"aws_sdk_auth"`
	AccessKey          string            `yaml:"access_key"`
	Insecure           bool              `yaml:"insecure"`
	SignatureV2        bool              `yaml:"signature_version2"`
	SecretKey          string            `yaml:"secret_key"`
	PutUserMetadata    map[string]string `yaml:"put_user_metadata"`
	HTTPConfig         S3HTTPConfig      `yaml:"http_config"`
	TraceConfig        TraceConfig       `yaml:"trace"`
	ListObjectsVersion string            `yaml:"list_objects_version"`
	// PartSize used for multipart upload. Only used if uploaded object size is known and larger than configured PartSize.
	// NOTE we need to make sure this number does not produce more parts than 10 000.
	PartSize    uint64    `yaml:"part_size"`
	SSEConfig   SSEConfig `yaml:"sse_config"`
	STSEndpoint string    `yaml:"sts_endpoint"`
}

// SSEConfig deals with the configuration of SSE for Minio. The following options are valid:
// kmsencryptioncontext == https://docs.aws.amazon.com/kms/latest/developerguide/services-s3.html#s3-encryption-context
type SSEConfig struct {
	Type                 string            `yaml:"type"`
	KMSKeyID             string            `yaml:"kms_key_id"`
	KMSEncryptionContext map[string]string `yaml:"kms_encryption_context"`
	EncryptionKey        string            `yaml:"encryption_key"`
}

type TraceConfig struct {
	Enable bool `yaml:"enable"`
}

// HTTPConfig stores the http.Transport configuration for the s3 minio client.
type S3HTTPConfig struct {
	IdleConnTimeout       time.Duration `yaml:"idle_conn_timeout"`
	ResponseHeaderTimeout time.Duration `yaml:"response_header_timeout"`
	InsecureSkipVerify    bool          `yaml:"insecure_skip_verify"`

	TLSHandshakeTimeout   time.Duration `yaml:"tls_handshake_timeout"`
	ExpectContinueTimeout time.Duration `yaml:"expect_continue_timeout"`
	MaxIdleConns          int           `yaml:"max_idle_conns"`
	MaxIdleConnsPerHost   int           `yaml:"max_idle_conns_per_host"`
	MaxConnsPerHost       int           `yaml:"max_conns_per_host"`

	// Allow upstream callers to inject a round tripper
	Transport http.RoundTripper `yaml:"-"`

	TLSConfig TLSConfig `yaml:"tls_config"`
}

// DefaultTransport - this default transport is based on the Minio
// DefaultTransport up until the following commit:
// https://githus3.com/minio/minio-go/commit/008c7aa71fc17e11bf980c209a4f8c4d687fc884
// The values have since diverged.
func DefaultS3Transport(config S3Config) (*http.Transport, error) {
	tlsConfig, err := NewTLSConfig(&config.HTTPConfig.TLSConfig)
	if err != nil {
		return nil, err
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
		// A custom ResponseHeaderTimeout was introduced
		// to cover cases where the tcp connection works but
		// the server never answers. Defaults to 2 minutes.
		ResponseHeaderTimeout: time.Duration(config.HTTPConfig.ResponseHeaderTimeout),
		// Set this value so that the underlying transport round-tripper
		// doesn't try to auto decode the body of objects with
		// content-encoding set to `gzip`.
		//
		// Refer: https://golang.org/src/net/http/transport.go?h=roundTrip#L1843.
		DisableCompression: true,
		// #nosec It's up to the user to decide on TLS configs
		TLSClientConfig: tlsConfig,
	}, nil
}

// S3Storage provides storage via S3
type S3Storage struct {
	name            string
	client          *minio.Client
	defaultSSE      encrypt.ServerSide
	putUserMetadata map[string]string
	partSize        uint64
	listObjectsV1   bool
}

// parseConfig unmarshals a buffer into a Config with default HTTPConfig values.
func parseS3Config(conf []byte) (S3Config, error) {
	config := defaultS3Config
	if err := yaml.Unmarshal(conf, &config); err != nil {
		return S3Config{}, err
	}

	return config, nil
}

// NewBucket returns a new Bucket using the provided s3 config values.
func NewS3Storage(conf []byte) (*S3Storage, error) {
	config, err := parseS3Config(conf)
	if err != nil {
		return nil, err
	}

	return NewS3StorageWith(config)
}

// NewBucketWithConfig returns a new Bucket using the provided s3 config values.
func NewS3StorageWith(config S3Config) (*S3Storage, error) {
	var chain []credentials.Provider

	wrapCredentialsProvider := func(p credentials.Provider) credentials.Provider { return p }
	if config.SignatureV2 {
		wrapCredentialsProvider = func(p credentials.Provider) credentials.Provider {
			return &overrideSignerType{Provider: p, signerType: credentials.SignatureV2}
		}
	}

	if err := validate(config); err != nil {
		return nil, err
	}

	if config.AWSSDKAuth {
		chain = []credentials.Provider{
			wrapCredentialsProvider(&awsAuth{Region: config.Region}),
		}
	} else if config.AccessKey != "" {
		chain = []credentials.Provider{wrapCredentialsProvider(&credentials.Static{
			Value: credentials.Value{
				AccessKeyID:     config.AccessKey,
				SecretAccessKey: config.SecretKey,
				SignerType:      credentials.SignatureV4,
			},
		})}
	} else {
		chain = []credentials.Provider{
			wrapCredentialsProvider(&credentials.EnvAWS{}),
			wrapCredentialsProvider(&credentials.FileAWSCredentials{}),
			wrapCredentialsProvider(&credentials.IAM{
				Client: &http.Client{
					Transport: http.DefaultTransport,
				},
				Endpoint: config.STSEndpoint,
			}),
		}
	}

	// Check if a roundtripper has been set in the config
	// otherwise build the default transport.
	var rt http.RoundTripper
	if config.HTTPConfig.Transport != nil {
		rt = config.HTTPConfig.Transport
	} else {
		var err error
		rt, err = DefaultS3Transport(config)
		if err != nil {
			return nil, err
		}
	}

	client, err := minio.New(config.Endpoint, &minio.Options{
		Creds:     credentials.NewChainCredentials(chain),
		Secure:    !config.Insecure,
		Region:    config.Region,
		Transport: rt,
	})
	if err != nil {
		return nil, errors.Wrap(err, "initialize s3 client")
	}

	var sse encrypt.ServerSide
	if config.SSEConfig.Type != "" {
		switch config.SSEConfig.Type {
		case SSEKMS:
			// If the KMSEncryptionContext is a nil map the header that is
			// constructed by the encrypt.ServerSide object will be base64
			// encoded "nil" which is not accepted by AWS.
			if config.SSEConfig.KMSEncryptionContext == nil {
				config.SSEConfig.KMSEncryptionContext = make(map[string]string)
			}
			sse, err = encrypt.NewSSEKMS(config.SSEConfig.KMSKeyID, config.SSEConfig.KMSEncryptionContext)
			if err != nil {
				return nil, errors.Wrap(err, "initialize s3 client SSE-KMS")
			}

		case SSEC:
			key, err := os.ReadFile(config.SSEConfig.EncryptionKey)
			if err != nil {
				return nil, err
			}

			sse, err = encrypt.NewSSEC(key)
			if err != nil {
				return nil, errors.Wrap(err, "initialize s3 client SSE-C")
			}

		case SSES3:
			sse = encrypt.NewSSE()

		default:
			sseErrMsg := errors.Errorf("Unsupported type %q was provided. Supported types are SSE-S3, SSE-KMS, SSE-C", config.SSEConfig.Type)
			return nil, errors.Wrap(sseErrMsg, "Initialize s3 client SSE Config")
		}
	}

	if config.ListObjectsVersion != "" && config.ListObjectsVersion != "v1" && config.ListObjectsVersion != "v2" {
		return nil, errors.Errorf("Initialize s3 client list objects version: Unsupported version %q was provided. Supported values are v1, v2", config.ListObjectsVersion)
	}

	bkt := &S3Storage{
		name:            config.Bucket,
		client:          client,
		defaultSSE:      sse,
		putUserMetadata: config.PutUserMetadata,
		partSize:        config.PartSize,
		listObjectsV1:   config.ListObjectsVersion == "v1",
	}
	return bkt, nil
}

// Name returns the bucket name for s3.
func (s3 *S3Storage) Name() string {
	return s3.name
}

// StorageType returns a string identifier for the type of storage used by the implementation.
func (s3 *S3Storage) StorageType() StorageType {
	return StorageTypeBucketS3
}

// validate checks to see the config options are set.
func validate(conf S3Config) error {
	if conf.Endpoint == "" {
		return errors.New("no s3 endpoint in config file")
	}
	if conf.AWSSDKAuth && conf.AccessKey != "" {
		return errors.New("aws_sdk_auth and access_key are mutually exclusive configurations")
	}
	if conf.AccessKey == "" && conf.SecretKey != "" {
		return errors.New("no s3 acccess_key specified while secret_key is present in config file; either both should be present in config or envvars/IAM should be used.")
	}

	if conf.AccessKey != "" && conf.SecretKey == "" {
		return errors.New("no s3 secret_key specified while access_key is present in config file; either both should be present in config or envvars/IAM should be used.")
	}

	if conf.SSEConfig.Type == SSEC && conf.SSEConfig.EncryptionKey == "" {
		return errors.New("encryption_key must be set if sse_config.type is set to 'SSE-C'")
	}

	if conf.SSEConfig.Type == SSEKMS && conf.SSEConfig.KMSKeyID == "" {
		return errors.New("kms_key_id must be set if sse_config.type is set to 'SSE-KMS'")
	}

	return nil
}

// FullPath returns the storage working path combined with the path provided
func (s3 *S3Storage) FullPath(name string) string {
	name = trimLeading(name)

	return name
}

// Get returns a reader for the given object name.
func (s3 *S3Storage) Read(name string) ([]byte, error) {
	name = trimLeading(name)

	log.Tracef("S3Storage::Read(%s)", name)
	ctx := context.Background()

	return s3.getRange(ctx, name, 0, -1)

}

// Exists checks if the given object exists.
func (s3 *S3Storage) Exists(name string) (bool, error) {
	name = trimLeading(name)
	log.Tracef("S3Storage::Exists(%s)", name)

	ctx := context.Background()

	_, err := s3.client.StatObject(ctx, s3.name, name, minio.StatObjectOptions{})
	if err != nil {
		if s3.isDoesNotExist(err) {
			return false, nil
		}
		return false, errors.Wrap(err, "stat s3 object")
	}

	return true, nil
}

// Upload the contents of the reader as an object into the bucket.
func (s3 *S3Storage) Write(name string, data []byte) error {
	name = trimLeading(name)

	log.Tracef("S3Storage::Write(%s)", name)

	ctx := context.Background()
	sse, err := s3.getServerSideEncryption(ctx)
	if err != nil {
		return err
	}

	var size int64 = int64(len(data))

	// Set partSize to 0 to write files in one go. This prevents chunking of
	// upload into multiple parts, which requires additional memory for buffering
	// the sub-parts. To remain consistent with other storage implementations,
	// we would rather attempt to lower cost fast upload and fast-fail.
	var partSize uint64 = 0

	r := bytes.NewReader(data)
	_, err = s3.client.PutObject(ctx, s3.name, name, r, int64(size), minio.PutObjectOptions{
		PartSize:             partSize,
		ServerSideEncryption: sse,
		UserMetadata:         s3.putUserMetadata,
	})

	if err != nil {
		return errors.Wrap(err, "upload s3 object")
	}

	return nil
}

// Attributes returns information about the specified object.
func (s3 *S3Storage) Stat(name string) (*StorageInfo, error) {
	name = trimLeading(name)

	log.Tracef("S3Storage::Stat(%s)", name)
	ctx := context.Background()

	objInfo, err := s3.client.StatObject(ctx, s3.name, name, minio.StatObjectOptions{})
	if err != nil {
		if s3.isDoesNotExist(err) {
			return nil, DoesNotExistError
		}
		return nil, err
	}

	return &StorageInfo{
		Name:    trimName(name),
		Size:    objInfo.Size,
		ModTime: objInfo.LastModified,
	}, nil
}

// Delete removes the object with the given name.
func (s3 *S3Storage) Remove(name string) error {
	name = trimLeading(name)

	log.Tracef("S3Storage::Remove(%s)", name)
	ctx := context.Background()

	return s3.client.RemoveObject(ctx, s3.name, name, minio.RemoveObjectOptions{})
}

func (s3 *S3Storage) List(path string) ([]*StorageInfo, error) {
	path = trimLeading(path)

	log.Tracef("S3Storage::List(%s)", path)
	ctx := context.Background()

	// Ensure the object name actually ends with a dir suffix. Otherwise we'll just iterate the
	// object itself as one prefix item.
	if path != "" {
		path = strings.TrimSuffix(path, DirDelim) + DirDelim
	}

	opts := minio.ListObjectsOptions{
		Prefix:    path,
		Recursive: false,
		UseV1:     s3.listObjectsV1,
	}

	var stats []*StorageInfo
	for object := range s3.client.ListObjects(ctx, s3.name, opts) {
		// Catch the error when failed to list objects.
		if object.Err != nil {
			return nil, object.Err
		}

		// The s3 client can also return the directory itself in the ListObjects call above.
		if object.Key == path {
			continue
		}

		name := trimName(object.Key)

		// This sometimes happens with empty buckets.
		if name == "" {
			continue
		}

		stats = append(stats, &StorageInfo{
			Name:    name,
			Size:    object.Size,
			ModTime: object.LastModified,
		})
	}

	return stats, nil
}

func (s3 *S3Storage) ListDirectories(path string) ([]*StorageInfo, error) {
	path = trimLeading(path)

	log.Tracef("S3Storage::List(%s)", path)
	ctx := context.Background()

	if path != "" {
		path = strings.TrimSuffix(path, DirDelim) + DirDelim
	}

	opts := minio.ListObjectsOptions{
		Prefix:    path,
		Recursive: false,
		UseV1:     s3.listObjectsV1,
	}

	var stats []*StorageInfo
	for object := range s3.client.ListObjects(ctx, s3.name, opts) {

		if object.Err != nil {
			return nil, object.Err
		}

		if object.Key == "" {
			continue
		}

		if object.Key == path {
			continue
		}

		// If trim removes the entire name, it's a directory, ergo we list it
		if trimName(object.Key) == "" {
			stats = append(stats, &StorageInfo{
				Name:    object.Key,
				Size:    object.Size,
				ModTime: object.LastModified,
			})
		}

	}

	return stats, nil
}

// getServerSideEncryption returns the SSE to use.
func (s3 *S3Storage) getServerSideEncryption(ctx context.Context) (encrypt.ServerSide, error) {
	if value := ctx.Value(sseConfigKey); value != nil {
		if sse, ok := value.(encrypt.ServerSide); ok {
			return sse, nil
		}
		return nil, errors.New("invalid SSE config override provided in the context")
	}

	return s3.defaultSSE, nil
}

// isDoesNotExist returns true if error means that object key is not found.
func (s3 *S3Storage) isDoesNotExist(err error) bool {
	return minio.ToErrorResponse(errors.Cause(err)).Code == "NoSuchKey"
}

// isObjNotFound returns true if the error means that the object was not found
func (s3 *S3Storage) isObjNotFound(err error) bool {
	return minio.ToErrorResponse(errors.Cause(err)).Code == "NotFoundObject"
}

func (s3 *S3Storage) getRange(ctx context.Context, name string, off, length int64) ([]byte, error) {
	sse, err := s3.getServerSideEncryption(ctx)
	if err != nil {
		return nil, err
	}

	opts := &minio.GetObjectOptions{ServerSideEncryption: sse}
	if length != -1 {
		if err := opts.SetRange(off, off+length-1); err != nil {
			return nil, err
		}
	} else if off > 0 {
		if err := opts.SetRange(off, 0); err != nil {
			return nil, err
		}
	}
	r, err := s3.client.GetObject(ctx, s3.name, name, *opts)
	if err != nil {
		if s3.isObjNotFound(err) {
			return nil, DoesNotExistError
		}
		return nil, err
	}
	defer r.Close()

	// NotFoundObject error is revealed only after first Read. This does the initial GetRequest. Prefetch this here
	// for convenience.
	if _, err := r.Read(nil); err != nil {
		if s3.isObjNotFound(err) {
			return nil, DoesNotExistError
		}

		return nil, errors.Wrap(err, "Read from S3 failed")
	}

	return io.ReadAll(r)
}

// awsAuth retrieves credentials from the aws-sdk-go.
type awsAuth struct {
	Region string
	creds  aws.Credentials
}

// Retrieve retrieves the keys from the environment.
func (a *awsAuth) Retrieve() (credentials.Value, error) {
	cfg, err := awsconfig.LoadDefaultConfig(context.TODO(), awsconfig.WithRegion(a.Region))
	if err != nil {
		return credentials.Value{}, errors.Wrap(err, "load AWS SDK config")
	}

	creds, err := cfg.Credentials.Retrieve(context.TODO())
	if err != nil {
		return credentials.Value{}, errors.Wrap(err, "retrieve AWS SDK credentials")
	}

	a.creds = creds

	return credentials.Value{
		AccessKeyID:     creds.AccessKeyID,
		SecretAccessKey: creds.SecretAccessKey,
		SessionToken:    creds.SessionToken,
		SignerType:      credentials.SignatureV4,
	}, nil
}

// IsExpired returns if the credentials have been retrieved.
func (a *awsAuth) IsExpired() bool {
	return a.creds.Expired()
}

type overrideSignerType struct {
	credentials.Provider
	signerType credentials.SignatureType
}

func (s *overrideSignerType) Retrieve() (credentials.Value, error) {
	v, err := s.Provider.Retrieve()
	if err != nil {
		return v, err
	}
	if !v.SignerType.IsAnonymous() {
		v.SignerType = s.signerType
	}
	return v, nil
}
