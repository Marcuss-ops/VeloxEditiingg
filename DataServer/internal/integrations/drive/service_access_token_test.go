package drive

import (
	"context"
	"errors"
	"testing"
)

func TestGetTokenPrefersEphemeralAccessToken(t *testing.T) {
	service := &Service{}
	token, err := service.getToken(WithAccessToken(context.Background(), "ephemeral-access"))
	if err != nil {
		t.Fatal(err)
	}
	if token.AccessToken != "ephemeral-access" || token.RefreshToken != "" {
		t.Fatalf("token = %#v; refresh token must not be present", token)
	}
}

func TestGetTokenWithoutCredentialIsAuthenticationError(t *testing.T) {
	service := &Service{}
	_, err := service.getToken(context.Background())
	if err == nil || !errors.Is(err, ErrNotAuthenticated) {
		t.Fatalf("getToken() error = %v, want ErrNotAuthenticated", err)
	}
}
