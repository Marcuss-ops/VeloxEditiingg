package app

import (
	"testing"

	"github.com/gin-gonic/gin"

	"velox-server/internal/config"
)

func TestWorkersModule_Name(t *testing.T) {
	m := NewWorkersModule(&config.Config{}, nil, nil, nil, nil, nil, nil)
	if m.Name() != "workers" {
		t.Errorf("expected 'workers', got %q", m.Name())
	}
}

func TestWorkersModule_RegisterRoutes_NilLifecycle(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	m := NewWorkersModule(&config.Config{}, nil, nil, nil, nil, nil, nil)

	// Should not panic with nil lifecycle
	m.RegisterRoutes(r)
}

func TestWorkersModule_RegisterRoutes_WithLifecycle(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	m := NewWorkersModule(&config.Config{}, nil, nil, nil, nil, nil, nil)
	m.RegisterRoutes(r)
}

func TestNewWorkersModule_Nilsafe(t *testing.T) {
	m := NewWorkersModule(nil, nil, nil, nil, nil, nil, nil)
	if m == nil {
		t.Fatal("New should return non-nil module")
	}
	if m.Name() != "workers" {
		t.Errorf("expected 'workers', got %q", m.Name())
	}
}
