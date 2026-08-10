package app

import (
	"net/http"
	"sync"

	"github.com/gin-gonic/gin"
)

// ReadinessCheck is a function that returns nil when the dependency
// is healthy, or an error describing what's wrong.
type ReadinessCheck func() error

// HealthModule provides health and readiness endpoints.
//
// /health returns 200 as long as the process is alive (no dependency
// checks — this is the Kubernetes liveness probe).
//
// /ready returns 200 only when all registered ReadinessChecks pass.
// A 503 is returned with per-check details when any dependency is
// not yet available (Kubernetes readiness probe).
type HealthModule struct {
	mu         sync.RWMutex
	booted     bool
	checks     []namedCheck
	capability []namedCapability
}

type namedCheck struct {
	name  string
	check ReadinessCheck
}

type namedCapability struct {
	name  string
	state func() string
}

// NewHealthModule creates a new health module.
func NewHealthModule() *HealthModule {
	return &HealthModule{}
}

// Name returns the module identifier.
func (m *HealthModule) Name() string {
	return "health"
}

// RegisterRoutes registers health check endpoints.
func (m *HealthModule) RegisterRoutes(r *gin.Engine) {
	r.GET("/health", m.health)
	r.GET("/api/health", m.health)
	r.GET("/ready", m.ready)
	r.GET("/api/ready", m.ready)
	// Canonical /health/ready alias used by operators and canary checks.
	r.GET("/health/ready", m.ready)
	r.GET("/api/health/ready", m.ready)
}

// AddReadinessCheck registers a named readiness check.
// Checks are evaluated in registration order on every /ready request.
func (m *HealthModule) AddReadinessCheck(name string, check ReadinessCheck) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.checks = append(m.checks, namedCheck{name: name, check: check})
}

// AddReadinessCapability exposes a non-failing capability state in the
// readiness payload. This distinguishes intentional DISABLED capabilities
// from a missing or silently healthy-looking service.
func (m *HealthModule) AddReadinessCapability(name string, state func() string) {
	if state == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.capability = append(m.capability, namedCapability{name: name, state: state})
}

// MarkReady signals that bootstrap is complete and /ready should
// evaluate registered checks (instead of returning 503 unconditionally).
func (m *HealthModule) MarkReady() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.booted = true
}

func (m *HealthModule) health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "healthy"})
}

func (m *HealthModule) ready(c *gin.Context) {
	m.mu.RLock()
	isReady := m.booted
	checks := make([]namedCheck, len(m.checks))
	copy(checks, m.checks)
	capability := make([]namedCapability, len(m.capability))
	copy(capability, m.capability)
	m.mu.RUnlock()

	if !isReady {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status":  "not_ready",
			"message": "bootstrap not yet complete",
		})
		return
	}

	var failures []gin.H
	allOK := true
	for _, nc := range checks {
		if err := nc.check(); err != nil {
			allOK = false
			failures = append(failures, gin.H{
				"check": nc.name,
				"error": err.Error(),
			})
		}
	}

	capabilityStates := make(map[string]string, len(capability))
	for _, item := range capability {
		capabilityStates[item.name] = item.state()
	}

	if !allOK {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status":       "not_ready",
			"failures":     failures,
			"capabilities": capabilityStates,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":       "ready",
		"checks":       len(checks),
		"capabilities": capabilityStates,
	})
}
