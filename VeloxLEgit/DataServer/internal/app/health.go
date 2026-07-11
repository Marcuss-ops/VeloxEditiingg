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
	mu     sync.RWMutex
	booted bool
	checks []namedCheck
}

type namedCheck struct {
	name  string
	check ReadinessCheck
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
}

// AddReadinessCheck registers a named readiness check.
// Checks are evaluated in registration order on every /ready request.
func (m *HealthModule) AddReadinessCheck(name string, check ReadinessCheck) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.checks = append(m.checks, namedCheck{name: name, check: check})
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

	if !allOK {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status":   "not_ready",
			"failures": failures,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status": "ready",
		"checks": len(checks),
	})
}
