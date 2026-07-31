package publication

import (
	"errors"
	"testing"
)

type testProvider struct {
	name string
}

func (p testProvider) Name() string { return p.name }

func (p testProvider) Validate(ResolvedPublication) error { return nil }

func TestProviderRegistryIsImmutableAndSorted(t *testing.T) {
	empty := NewProviderRegistry()
	withYouTube, err := empty.Register(testProvider{name: "youtube"})
	if err != nil {
		t.Fatal(err)
	}
	withDrive, err := withYouTube.Register(testProvider{name: "drive"})
	if err != nil {
		t.Fatal(err)
	}

	if empty.Len() != 0 || withYouTube.Len() != 1 || withDrive.Len() != 2 {
		t.Fatalf("registry lengths = %d, %d, %d", empty.Len(), withYouTube.Len(), withDrive.Len())
	}
	if got := withDrive.Names(); len(got) != 2 || got[0] != "drive" || got[1] != "youtube" {
		t.Fatalf("names = %#v, want sorted provider names", got)
	}
	if !withDrive.Has("youtube") || withDrive.Has("missing") {
		t.Fatal("provider presence lookup is incorrect")
	}
}

func TestProviderRegistryRejectsInvalidAndDuplicateProviders(t *testing.T) {
	registry := NewProviderRegistry()
	if _, err := registry.Register(nil); !errors.Is(err, ErrInvalidProvider) {
		t.Fatalf("nil provider error = %v", err)
	}
	if _, err := registry.Register(testProvider{name: "bad/name"}); !errors.Is(err, ErrInvalidProvider) {
		t.Fatalf("invalid name error = %v", err)
	}
	registry, err := registry.Register(testProvider{name: "youtube"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Register(testProvider{name: "youtube"}); !errors.Is(err, ErrProviderAlreadyRegistered) {
		t.Fatalf("duplicate error = %v", err)
	}
}

func TestProviderRegistryResolveMissing(t *testing.T) {
	_, err := NewProviderRegistry().Resolve("youtube")
	if !errors.Is(err, ErrProviderNotFound) {
		t.Fatalf("missing provider error = %v", err)
	}
}
