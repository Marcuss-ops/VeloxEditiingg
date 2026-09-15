package artifacts

import (
	"encoding/hex"
	"strings"
	"testing"
)

func TestEarlyUploadTokenIsScopedAndConstantFormat(t *testing.T) {
	secret := hex.EncodeToString([]byte(strings.Repeat("k", 32)))
	token, err := EarlyUploadToken(secret, "upload-1")
	if err != nil {
		t.Fatalf("EarlyUploadToken: %v", err)
	}
	if len(token) != 64 {
		t.Fatalf("token length=%d, want 64 hex characters", len(token))
	}
	if !VerifyEarlyUploadToken(secret, "upload-1", token) {
		t.Fatal("token did not verify for its upload")
	}
	if VerifyEarlyUploadToken(secret, "upload-2", token) {
		t.Fatal("token verified for a different upload")
	}
	otherSecret := hex.EncodeToString([]byte(strings.Repeat("j", 32)))
	if VerifyEarlyUploadToken(otherSecret, "upload-1", token) {
		t.Fatal("token verified with a different secret")
	}
}

func TestEarlyUploadTokenRejectsInvalidSecretsAndIDs(t *testing.T) {
	if _, err := EarlyUploadToken("not-hex", "upload-1"); err == nil {
		t.Fatal("invalid secret was accepted")
	}
	if _, err := EarlyUploadToken(hex.EncodeToString([]byte("short")), "upload-1"); err == nil {
		t.Fatal("short secret was accepted")
	}
	secret := hex.EncodeToString([]byte(strings.Repeat("k", 32)))
	if _, err := EarlyUploadToken(secret, " "); err == nil {
		t.Fatal("empty upload ID was accepted")
	}
}
