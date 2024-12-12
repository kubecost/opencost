package alibaba

import (
	"fmt"

	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/auth"
	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/auth/credentials"
	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/pkg/cloud"
)

const AccessKeyAuthorizerType = "AlibabaAccessKey"

// Authorizer provide *bssopenapi.Client for Alibaba cloud BOS for Billing related SDK calls
type Authorizer interface {
	cloud.Authorizer
	GetCredentials() (auth.Credential, error)
}

// SelectAuthorizerByType is an implementation of AuthorizerSelectorFn and acts as a register for Authorizer types
func SelectAuthorizerByType(typeStr string) (Authorizer, error) {
	switch typeStr {
	case AccessKeyAuthorizerType:
		return &AccessKey{}, nil
	default:
		return nil, fmt.Errorf("alibaba: provider authorizer type '%s' is not valid", typeStr)
	}
}

// AccessKey holds Alibaba credentials parsing from the service-key.json file.
type AccessKey struct {
	AccessKeyID     string `json:"accessKeyID"`
	AccessKeySecret string `json:"accessKeySecret"`
}

// MarshalJSON custom json marshalling functions, sets properties as tagged in struct and sets the authorizer type property
func (ak *AccessKey) MarshalJSON() ([]byte, error) {
	fmap := make(map[string]any, 3)
	fmap[cloud.AuthorizerTypeProperty] = AccessKeyAuthorizerType
	fmap["accessKeyID"] = ak.AccessKeyID
	fmap["accessKeySecret"] = ak.AccessKeySecret
	return json.Marshal(fmap)
}

func (ak *AccessKey) Validate() error {
	if ak.AccessKeyID == "" {
		return fmt.Errorf("AccessKey: missing Access key ID")
	}
	if ak.AccessKeySecret == "" {
		return fmt.Errorf("AccessKey: missing Access Key secret")
	}
	return nil
}

func (ak *AccessKey) Equals(config cloud.Config) bool {
	if config == nil {
		return false
	}
	thatConfig, ok := config.(*AccessKey)
	if !ok {
		return false
	}

	if ak.AccessKeyID != thatConfig.AccessKeyID {
		return false
	}
	if ak.AccessKeySecret != thatConfig.AccessKeySecret {
		return false
	}
	return true
}

func (ak *AccessKey) Sanitize() cloud.Config {
	return &AccessKey{
		AccessKeyID:     ak.AccessKeyID,
		AccessKeySecret: cloud.Redacted,
	}
}

// GetCredentials creates a credentials object to authorize the use of service sdk calls
func (ak *AccessKey) GetCredentials() (auth.Credential, error) {
	err := ak.Validate()
	if err != nil {
		return nil, err
	}
	return &credentials.AccessKeyCredential{AccessKeyId: ak.AccessKeyID, AccessKeySecret: ak.AccessKeySecret}, nil
}
