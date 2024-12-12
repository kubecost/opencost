package aws

import (
	"context"
	"fmt"

	"golang.org/x/oauth2/google"
	"google.golang.org/api/idtoken"
	"google.golang.org/api/option"
)

type IDTokenRetriever interface {
	GetIdentityToken() ([]byte, error)
	Validate() error
	Sanitize() IDTokenRetriever
	Equals(IDTokenRetriever) bool
}

type GoogleIDTokenRetriever struct {
	Aud string `json:"aud"`
}

func (gitr GoogleIDTokenRetriever) GetIdentityToken() ([]byte, error) {
	ctx := context.Background()
	res := []byte{}

	credentials, err := google.FindDefaultCredentials(ctx)
	if err != nil {
		return res, fmt.Errorf("failed to find default credentials: %v", err)
	}

	ts, err := idtoken.NewTokenSource(ctx, gitr.Aud, option.WithCredentials(credentials))
	if err != nil {
		return res, fmt.Errorf("failed to create ID token source: %w", err)
	}

	t, err := ts.Token()
	if err != nil {
		return res, fmt.Errorf("failed to receive ID token from metadata server: %w", err)
	}

	return []byte(t.AccessToken), nil
}

func (gitr GoogleIDTokenRetriever) Validate() error {
	if gitr.Aud == "" {
		return fmt.Errorf("GoogleIDTokenRetriever: missing audience configuration")
	}

	return nil
}

func (gitr GoogleIDTokenRetriever) Equals(other IDTokenRetriever) bool {
	that, ok := other.(*GoogleIDTokenRetriever)
	if !ok {
		return false
	}

	if gitr.Aud != that.Aud {
		return false
	}

	return true
}

func (gitr GoogleIDTokenRetriever) Sanitize() IDTokenRetriever {
	return &GoogleIDTokenRetriever{
		Aud: gitr.Aud,
	}
}
