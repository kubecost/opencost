package storage

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/util/json"
)

func assert(t *testing.T, condition bool, msg string) {
	if !condition {
		t.Error(msg)
	}
}

func assertEq[T comparable](t *testing.T, got, expected T) {
	if got != expected {
		t.Errorf("Failed Equality Assertion:\n  Got: %v\n  Exp: %v", got, expected)
	}
}

func TestStorageTypes(t *testing.T) {
	fileType := StorageTypeFile
	s3Type := StorageTypeBucketS3
	gcsType := StorageTypeBucketGCS
	azureType := StorageTypeBucketAzure

	assert(t, fileType.BackendType() == "file", "StorageTypeFile.BackendType() should return 'file'")
	assert(t, s3Type.BackendType() == "bucket", "StorageTypeBucketS3.BackendType() should return 'bucket'")
	assert(t, gcsType.BackendType() == "bucket", "StorageTypeBucketGCS.BackendType() should return 'bucket'")
	assert(t, azureType.BackendType() == "bucket", "StorageTypeBucketAzure.BackendType() should return 'bucket'")

	assert(t, fileType.ProviderType() == "", "StorageTypeFile.ProviderType() should return ''")
	assert(t, s3Type.ProviderType() == "s3", "StorageTypeBucketS3.ProviderType() should return 's3'")
	assert(t, gcsType.ProviderType() == "gcs", "StorageTypeBucketGCS.ProviderType() should return 'gcs'")
	assert(t, azureType.ProviderType() == "azure", "StorageTypeBucketAzure.ProviderType() should return 'azure'")

	assert(t, fileType.IsFileStorage(), "StorageTypeFile.IsFileStorage() should return true")
	assert(t, s3Type.IsBucketStorage(), "StorageTypeBucketS3.IsBucketStorage() should return true")
	assert(t, gcsType.IsBucketStorage(), "StorageTypeBucketGCS.IsBucketStorage() should return true")
	assert(t, azureType.IsBucketStorage(), "StorageTypeBucketAzure.IsBucketStorage() should return true")
}

func TestJSONEncodeStorageType(t *testing.T) {
	fileType := StorageTypeFile
	s3Type := StorageTypeBucketS3
	gcsType := StorageTypeBucketGCS
	azureType := StorageTypeBucketAzure

	var data []byte
	var err error

	data, err = json.Marshal(fileType)
	if err != nil {
		t.Error(err)
	}
	assert(t, string(data) == `{"backendType":"file"}`, "json.Marshal(StorageTypeFile) should return '\"file\"'")

	data, err = json.Marshal(s3Type)
	if err != nil {
		t.Error(err)
	}
	assert(t, string(data) == `{"backendType":"bucket","providerType":"s3"}`, "json.Marshal(StorageTypeBucketS3) should return '\"bucket|s3\"'")

	data, err = json.Marshal(gcsType)
	if err != nil {
		t.Error(err)
	}
	assert(t, string(data) == `{"backendType":"bucket","providerType":"gcs"}`, "json.Marshal(StorageTypeBucketGCS) should return '\"bucket|gcs\"'")

	data, err = json.Marshal(azureType)
	if err != nil {
		t.Error(err)
	}
	assert(t, string(data) == `{"backendType":"bucket","providerType":"azure"}`, "json.Marshal(StorageTypeBucketAzure) should return '\"bucket|azure\"'")
}

func TestJSONDecodeStorageType(t *testing.T) {
	fileType := StorageTypeFile
	s3Type := StorageTypeBucketS3
	gcsType := StorageTypeBucketGCS
	azureType := StorageTypeBucketAzure

	var st StorageType

	data := []byte(`{"backendType":"file"}`)
	err := json.Unmarshal(data, &st)
	if err != nil {
		t.Error(err)
	}
	assert(t, st == fileType, "json.Unmarshal() should return StorageTypeFile")

	data = []byte(`{"backendType":"bucket","providerType":"s3"}`)
	err = json.Unmarshal(data, &st)
	if err != nil {
		t.Error(err)
	}
	assert(t, st == s3Type, "json.Unmarshal() should return StorageTypeBucketS3")

	data = []byte(`{"backendType":"bucket","providerType":"gcs"}`)
	err = json.Unmarshal(data, &st)
	if err != nil {
		t.Error(err)
	}
	assert(t, st == gcsType, "json.Unmarshal() should return StorageTypeBucketGCS")

	data = []byte(`{"backendType":"bucket","providerType":"azure"}`)
	err = json.Unmarshal(data, &st)
	if err != nil {
		t.Error(err)
	}
	assert(t, st == azureType, "json.Unmarshal() should return StorageTypeBucketAzure")
}

type TestWrapper struct {
	Foo         string      `json:"foo"`
	Prop        int         `json:"prop"`
	StorageType StorageType `json:"storageType"`
}

func TestJSONEncodeStorageTypeWrapped(t *testing.T) {
	tw := TestWrapper{
		Foo:         "bar",
		Prop:        42,
		StorageType: StorageTypeFile,
	}

	var data []byte
	var err error

	data, err = json.Marshal(tw)
	if err != nil {
		t.Error(err)
	}
	assertEq(t, string(data), `{"foo":"bar","prop":42,"storageType":{"backendType":"file"}}`)

	tw = TestWrapper{
		Foo:         "bar",
		Prop:        42,
		StorageType: StorageTypeBucketS3,
	}

	data, err = json.Marshal(tw)
	if err != nil {
		t.Error(err)
	}
	assertEq(t, string(data), `{"foo":"bar","prop":42,"storageType":{"backendType":"bucket","providerType":"s3"}}`)
}

func TestJSONDecodeStorageTypeWrapped(t *testing.T) {
	tw := TestWrapper{
		Foo:         "bar",
		Prop:        42,
		StorageType: StorageTypeFile,
	}

	var stw TestWrapper

	data := []byte(`{"foo":"bar","prop":42,"storageType":{"backendType":"file"}}`)
	err := json.Unmarshal(data, &stw)
	if err != nil {
		t.Error(err)
	}
	assertEq(t, stw.Foo, tw.Foo)
	assertEq(t, stw.Prop, tw.Prop)
	assertEq(t, stw.StorageType, tw.StorageType)

	tw = TestWrapper{
		Foo:         "bar",
		Prop:        42,
		StorageType: StorageTypeBucketS3,
	}

	data = []byte(`{"foo":"bar","prop":42,"storageType":{"backendType":"bucket","providerType":"s3"}}`)
	err = json.Unmarshal(data, &stw)
	if err != nil {
		t.Error(err)
	}
	assertEq(t, stw.Foo, tw.Foo)
	assertEq(t, stw.Prop, tw.Prop)
	assertEq(t, stw.StorageType, tw.StorageType)
}
