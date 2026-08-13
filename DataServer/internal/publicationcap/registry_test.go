package publicationcap

import (
	"strings"
	"testing"
)

func TestRegistryRejectsUnsupportedAndInvalidMetadata(t *testing.T) {
	r := DefaultRegistry()
	// Social platform capabilities are owned by the Social API, not Velox:
	// any provider outside the owned set must fail closed as unknown.
	if err := r.Validate("youtube", Metadata{HasLocalizations: true, HasCaptions: true}); err == nil {
		t.Fatal("youtube capability must be rejected as unknown")
	}
	if err := r.Validate("facebook", Metadata{}); err == nil {
		t.Fatal("facebook capability must be rejected as unknown")
	}
	if err := r.Validate("tiktok", Metadata{}); err == nil {
		t.Fatal("tiktok capability must be rejected as unknown")
	}
	if err := r.Validate("google_drive", Metadata{HasLocalizations: true}); err == nil {
		t.Fatal("unsupported localization was accepted")
	}
	if err := r.Validate("drive", Metadata{Title: strings.Repeat("x", 256)}); err == nil {
		t.Fatal("oversized title was accepted")
	}
	if err := r.Validate("drive", Metadata{Title: "ok", Description: "ok"}); err != nil {
		t.Fatalf("owned provider with valid metadata rejected: %v", err)
	}
}
