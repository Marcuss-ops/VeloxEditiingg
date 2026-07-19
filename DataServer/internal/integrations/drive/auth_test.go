package drive

import "testing"

func TestTokenUnmarshalAcceptsPipelineGenAccessToken(t *testing.T) {
	var token Token
	if err := token.UnmarshalJSON([]byte(`{"access_token":"present","refresh_token":"refresh","expiry":"2099-01-01T00:00:00Z"}`)); err != nil {
		t.Fatalf("unmarshal token: %v", err)
	}
	if token.AccessToken != "present" {
		t.Fatalf("access token = %q, want compatibility value", token.AccessToken)
	}
	if token.RefreshToken != "refresh" {
		t.Fatalf("refresh token was not preserved")
	}
}
