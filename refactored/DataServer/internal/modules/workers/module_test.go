package workers

import (
	"testing"

	"github.com/gin-gonic/gin"

	"velox-server/internal/config"
)

func TestModule_Name(t *testing.T) {
	m := New(&config.Config{}, nil, nil, nil, nil)
	if m.Name() != "workers" {
		t.Errorf("expected 'workers', got %q", m.Name())
	}
}

func TestModule_RegisterRoutes_NilLifecycle(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	m := New(&config.Config{}, nil, nil, nil, nil)

	// Should not panic with nil lifecycle
	m.RegisterRoutes(r)
}

func TestModule_RegisterRoutes_WithLifecycle(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	m := New(&config.Config{}, nil, nil, nil, nil)
	m.RegisterRoutes(r)
}

func TestNew_Nilsafe(t *testing.T) {
	m := New(nil, nil, nil, nil, nil)
	if m == nil {
		t.Fatal("New should return non-nil module")
	}
	if m.Name() != "workers" {
		t.Errorf("expected 'workers', got %q", m.Name())
	}
}
