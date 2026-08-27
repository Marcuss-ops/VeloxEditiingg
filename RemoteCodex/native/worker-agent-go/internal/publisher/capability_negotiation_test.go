package publisher

import (
	"context"
	"testing"
)

type capabilityTestTransport struct {
	progressive bool
	uploads     int
}

func (t *capabilityTestTransport) ID() string { return "test" }
func (t *capabilityTestTransport) Capabilities() CapabilitySet {
	if t.progressive {
		return CapabilitySet{"artifact.progressive-upload.v1"}
	}
	return nil
}
func (t *capabilityTestTransport) Upload(context.Context, UploadRequest) (*UploadResult, error) {
	t.uploads++
	return &UploadResult{UploadID: "v1", UploadedBytes: 1}, nil
}

func TestSupportsProgressiveRequiresCapabilityAndInterface(t *testing.T) {
	if SupportsProgressive(&capabilityTestTransport{progressive: true}) {
		t.Fatal("advertising-only transport must not negotiate progressive upload")
	}
	if SupportsProgressive(&capabilityTestTransport{}) {
		t.Fatal("transport without capability must not negotiate progressive upload")
	}
}
