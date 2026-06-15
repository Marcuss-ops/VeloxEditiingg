package app

import (
	"testing"

	"github.com/gin-gonic/gin"
)

// TestModule is a simple module for testing.
type TestModule struct {
	name   string
	routes []string
}

func NewTestModule(name string) *TestModule {
	return &TestModule{name: name}
}

func (m *TestModule) Name() string {
	return m.name
}

func (m *TestModule) RegisterRoutes(r *gin.Engine) {
	m.routes = append(m.routes, "registered")
}

func TestRegistry_Register(t *testing.T) {
	r := NewRegistry()
	m := NewTestModule("test")

	r.Register(m)

	if r.Len() != 1 {
		t.Errorf("expected 1 module, got %d", r.Len())
	}

	names := r.List()
	if len(names) != 1 || names[0] != "test" {
		t.Errorf("expected [test], got %v", names)
	}
}

func TestRegistry_RegisterRoutes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := NewRegistry()
	router := gin.New()

	m := NewTestModule("test")
	r.Register(m)

	r.RegisterRoutes(router)

	// Check that routes were registered
	if len(m.routes) != 1 {
		t.Errorf("expected 1 route registration, got %d", len(m.routes))
	}
}
