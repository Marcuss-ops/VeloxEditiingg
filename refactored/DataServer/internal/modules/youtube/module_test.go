package youtube

import (
	"testing"

	"velox-server/internal/config"
)

func TestModule_Name(t *testing.T) {
	m := New(&config.Config{}, "", nil)
	if m.Name() != "youtube" {
		t.Errorf("expected 'youtube', got %q", m.Name())
	}
}

func TestModule_NilAccessors(t *testing.T) {
	m := New(&config.Config{}, "", nil)

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

func TestNew_Nilsafe(t *testing.T) {
	m := New(nil, "", nil)
	if m == nil {
		t.Fatal("New should return non-nil module")
	}
	if m.Name() != "youtube" {
		t.Errorf("expected 'youtube', got %q", m.Name())
	}
}
