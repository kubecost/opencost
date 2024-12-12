package aws

import (
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/pkg/cloud"
)

type S3Configuration struct {
	Bucket     string     `json:"bucket"`
	Region     string     `json:"region"`
	Account    string     `json:"account"`
	Authorizer Authorizer `json:"authorizer"`
}

func (s3c *S3Configuration) Validate() error {
	// Validate Authorizer
	if s3c.Authorizer == nil {
		return fmt.Errorf("S3Configuration: missing Authorizer")
	}

	err := s3c.Authorizer.Validate()
	if err != nil {
		return fmt.Errorf("S3Configuration: %s", err)
	}

	// Validate base properties
	if s3c.Bucket == "" {
		return fmt.Errorf("S3Configuration: missing bucket")
	}

	if s3c.Region == "" {
		return fmt.Errorf("S3Configuration: missing region")
	}

	if s3c.Account == "" {
		return fmt.Errorf("S3Configuration: missing account")
	}

	return nil
}

func (s3c *S3Configuration) Equals(config cloud.Config) bool {
	if config == nil {
		return false
	}
	thatConfig, ok := config.(*S3Configuration)
	if !ok {
		return false
	}

	if s3c.Authorizer != nil {
		if !s3c.Authorizer.Equals(thatConfig.Authorizer) {
			return false
		}
	} else {
		if thatConfig.Authorizer != nil {
			return false
		}
	}

	if s3c.Bucket != thatConfig.Bucket {
		return false
	}

	if s3c.Region != thatConfig.Region {
		return false
	}

	if s3c.Account != thatConfig.Account {
		return false
	}

	return true
}

func (s3c *S3Configuration) Sanitize() cloud.Config {
	return &S3Configuration{
		Bucket:     s3c.Bucket,
		Region:     s3c.Region,
		Account:    s3c.Account,
		Authorizer: s3c.Authorizer.Sanitize().(Authorizer),
	}
}

func (s3c *S3Configuration) Key() string {
	return fmt.Sprintf("%s/%s", s3c.Account, s3c.Bucket)
}

func (s3c *S3Configuration) Provider() string {
	return opencost.AWSProvider
}

func (s3c *S3Configuration) UnmarshalJSON(b []byte) error {
	var f interface{}
	err := json.Unmarshal(b, &f)
	if err != nil {
		return err
	}

	fmap := f.(map[string]interface{})

	bucket, err := cloud.GetInterfaceValue[string](fmap, "bucket")
	if err != nil {
		return fmt.Errorf("S3Configuration: UnmarshalJSON: %s", err.Error())
	}
	s3c.Bucket = bucket

	region, err := cloud.GetInterfaceValue[string](fmap, "region")
	if err != nil {
		return fmt.Errorf("S3Configuration: UnmarshalJSON: %s", err.Error())
	}
	s3c.Region = region

	account, err := cloud.GetInterfaceValue[string](fmap, "account")
	if err != nil {
		return fmt.Errorf("S3Configuration: UnmarshalJSON: %s", err.Error())
	}
	s3c.Account = account

	authAny, ok := fmap["authorizer"]
	if !ok {
		return fmt.Errorf("S3Configuration: UnmarshalJSON: missing authorizer")
	}
	authorizer, err := cloud.AuthorizerFromInterface(authAny, SelectAuthorizerByType)
	if err != nil {
		return fmt.Errorf("S3Configuration: UnmarshalJSON: %s", err.Error())
	}
	s3c.Authorizer = authorizer

	return nil
}

func (s3c *S3Configuration) CreateAWSConfig() (aws.Config, error) {
	return s3c.Authorizer.CreateAWSConfig(s3c.Region)
}
