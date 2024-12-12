package aws

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/pkg/cloud"
)

func TestAuthorizerJSON_Sanitize(t *testing.T) {

	testCases := map[string]struct {
		input    Authorizer
		expected Authorizer
	}{
		"Access Key": {
			input: &AccessKey{
				ID:     "ID",
				Secret: "Secret",
			},
			expected: &AccessKey{
				ID:     "ID",
				Secret: cloud.Redacted,
			},
		},
		"Service Account": {
			input:    &ServiceAccount{},
			expected: &ServiceAccount{},
		},
		"Master Payer Access Key": {
			input: &AssumeRole{
				Authorizer: &AccessKey{
					ID:     "ID",
					Secret: "Secret",
				},
				RoleARN: "role arn",
			},
			expected: &AssumeRole{
				Authorizer: &AccessKey{
					ID:     "ID",
					Secret: cloud.Redacted,
				},
				RoleARN: "role arn",
			},
		},
		"Master Payer Service Account": {
			input: &AssumeRole{
				Authorizer: &ServiceAccount{},
				RoleARN:    "role arn",
			},
			expected: &AssumeRole{
				Authorizer: &ServiceAccount{},
				RoleARN:    "role arn",
			},
		},
		"Google Web Identity": {
			input: &WebIdentity{
				RoleARN:          "role arn",
				IdentityProvider: "Google",
				TokenRetriever: &GoogleIDTokenRetriever{
					Aud: "aud",
				},
			},
			expected: &WebIdentity{
				RoleARN:          "role arn",
				IdentityProvider: "Google",
				TokenRetriever: &GoogleIDTokenRetriever{
					Aud: "aud",
				},
			},
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			// Convert to AuthorizerJSON for sanitization
			sanitizedAuthorizer := tc.input.Sanitize()

			if !tc.expected.Equals(sanitizedAuthorizer) {
				t.Error("Authorizer was not as expected after Sanitization")
			}

		})
	}
}

func TestAuthorizerJSON_Encode(t *testing.T) {

	testCases := map[string]struct {
		authorizer Authorizer
	}{
		"Access Key": {
			authorizer: &AccessKey{
				ID:     "ID",
				Secret: "Secret",
			},
		},
		"Service Account": {
			authorizer: &ServiceAccount{},
		},
		"Master Payer Access Key": {
			authorizer: &AssumeRole{
				Authorizer: &AccessKey{
					ID:     "ID",
					Secret: "Secret",
				},
				RoleARN: "role arn",
			},
		},
		"Master Payer Service Account": {
			authorizer: &AssumeRole{
				Authorizer: &ServiceAccount{},
				RoleARN:    "role arn",
			},
		},
		"Google Web Identity": {
			authorizer: &WebIdentity{
				RoleARN:          "role arn",
				IdentityProvider: "Google",
				TokenRetriever: &GoogleIDTokenRetriever{
					Aud: "aud",
				},
			},
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {

			b, err := tc.authorizer.MarshalJSON()
			if err != nil {
				t.Errorf("Failed to Marshal Authorizer: %s", err)
			}

			var f interface{}
			err = json.Unmarshal(b, &f)
			if err != nil {
				t.Errorf("Failed to Unmarshal Authorizer: %s", err)
			}

			authorizer, err := cloud.AuthorizerFromInterface(f, SelectAuthorizerByType)
			if err != nil {
				t.Errorf("Failed to Unmarshal Authorizer: %s", err)
			}

			if !tc.authorizer.Equals(authorizer) {
				t.Error("Authorizer was not as expected after Sanitization")
			}

		})
	}
}
