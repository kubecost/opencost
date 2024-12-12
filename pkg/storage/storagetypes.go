package storage

import (
	"strings"

	"github.com/opencost/opencost/core/pkg/util/json"
)

/*
 NOTE: This format is to provide monitoring a simple way to identify the storage type with
 NOTE: minimal changes to the Storage interface. It's not very robust, so use caution if
 NOTE: leveraging this type in other systems.
*/

// StorageType is a string identifier for the type of storage used by a Storage implementation.
// The string format is "backend|provider" where backend is the represents the generic storage
// facility, and the provider is the specific implementation.
type StorageType string

const (
	StorageTypeFile        StorageType = "file"
	StorageTypeBucketS3    StorageType = "bucket|s3"
	StorageTypeBucketGCS   StorageType = "bucket|gcs"
	StorageTypeBucketAzure StorageType = "bucket|azure"
)

// jsonIR is a json intermediate representation of a StorageType
type jsonIR struct {
	BackendType  string `json:"backendType"`
	ProviderType string `json:"providerType,omitempty"`
}

// MarshalJSON implements the json.Marshaler interface for encoding a StorageType.
func (st StorageType) MarshalJSON() ([]byte, error) {
	return json.Marshal(jsonIR{
		BackendType:  st.BackendType(),
		ProviderType: st.ProviderType(),
	})
}

// UnmarshalJSON implements the json.Unmarshaler interface for decoding a StorageType.
func (st *StorageType) UnmarshalJSON(data []byte) error {
	var ir jsonIR
	err := json.Unmarshal(data, &ir)
	if err != nil {
		return err
	}

	str := ir.BackendType
	if ir.ProviderType != "" {
		str += "|" + ir.ProviderType
	}

	*st = StorageType(str)
	return nil
}

// IsFileStorage returns true if the StorageType is a file storage type.
func (st StorageType) IsFileStorage() bool {
	return st.BackendType() == "file"
}

// IsBucketStorage returns true if the StorageType is a bucket storage type.
func (st StorageType) IsBucketStorage() bool {
	return st.BackendType() == "bucket"
}

// BackendType returns the backend type if applicable for the storage type.
func (st StorageType) BackendType() string {
	index := strings.Index(string(st), "|")
	if index > 0 {
		return string(st)[:index]
	}
	return string(st)
}

// ProviderType returns the provider type if applicable for the storage type.
func (st StorageType) ProviderType() string {
	index := strings.Index(string(st), "|")

	if index > 0 && index < len(string(st))-1 {
		return string(st)[index+1:]
	}

	return ""
}
