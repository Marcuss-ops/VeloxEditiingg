package app

import (
	"testing"

	"velox-server/internal/config"
)

func TestYouTubeModule_Name(t *testing.T) {
	m := NewYouTubeModule(&config.Config{}, "", nil)
	if m.Name() != "youtube" {
		t.Errorf("expected 'youtube', got %q", m.Name())
	}
}

func TestYouTubeModule_NilAccessors(t *testing.T) {
	m := NewYouTubeModule(&config.Config{}, "", nil)

	if m.Handlers() != nil {
		t.Error("Handlers() should be nil before RegisterRoutes")
	}
	if m.Manager() != nil {
		t.Error("Manager() should be nil before RegisterRoutes")
	}
	if m.Service() != nil {
		t.Error("Service() should be nil before RegisterRoutes")
	}
}

func TestNewYouTubeModule_Nilsafe(t *testing.T) {
	m := NewYouTubeModule(nil, "", nil)
	if m == nil {
		t.Fatal("New should return non-nil module")
	}
	if m.Name() != "youtube" {
		t.Errorf("expected 'youtube', got %q", m.Name())
	}
}
