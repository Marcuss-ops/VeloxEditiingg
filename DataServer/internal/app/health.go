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
	runtime    *RuntimeInfo
}

type namedCheck struct {
	name  string
	check ReadinessCheck
}

type namedCapability struct {
	name  string
	state func() string
}

// RuntimeInfo is the build and transport identity exposed by the health
// endpoints. It is supplied by the composition root so this module never
// reads process environment or build metadata itself.
type RuntimeInfo struct {
	Version   string   `json:"version,omitempty"`
	BuildTime string   `json:"build_time,omitempty"`
	Commit    string   `json:"commit,omitempty"`
	GRPC      GRPCInfo `json:"grpc"`
}

// GRPCInfo makes the worker control transport observable independently from
// the HTTP listener. A healthy REST endpoint is not sufficient when workers
// depend on the gRPC control stream.
type GRPCInfo struct {
	Configured bool `json:"configured"`
	Port       int  `json:"port"`
	Started    bool `json:"started"`
}

// NewHealthModule creates a new health module.
func NewHealthModule() *HealthModule {
	return &HealthModule{}
}

// SetRuntimeInfo installs the bootstrap identity exposed by the health
// endpoints. Bootstrap calls this before marking the module ready.
func (m *HealthModule) SetRuntimeInfo(info RuntimeInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()
	copy := info
	m.runtime = &copy
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

// CapabilityNames returns the names of all registered capability state
// exposures, in registration order. Used by tests to pin the capability
// contract (AGENTS.md §6): every exposure must be paired with a
// fail-closed readiness gate.
func (m *HealthModule) CapabilityNames() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	names := make([]string, len(m.capability))
	for i, item := range m.capability {
		names[i] = item.name
	}
	return names
}

// CheckNames returns the names of all registered readiness checks, in
// registration order.
func (m *HealthModule) CheckNames() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	names := make([]string, len(m.checks))
	for i, item := range m.checks {
		names[i] = item.name
	}
	return names
}

// MarkReady signals that bootstrap is complete and /ready should
// evaluate registered checks (instead of returning 503 unconditionally).
func (m *HealthModule) MarkReady() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.booted = true
}

func (m *HealthModule) health(c *gin.Context) {
	c.JSON(http.StatusOK, m.withRuntime(gin.H{"status": "healthy"}))
}

func (m *HealthModule) ready(c *gin.Context) {
	m.mu.RLock()
	isReady := m.booted
	checks := make([]namedCheck, len(m.checks))
	copy(checks, m.checks)
	capability := make([]namedCapability, len(m.capability))
	copy(capability, m.capability)
	var runtime *RuntimeInfo
	if m.runtime != nil {
		copy := *m.runtime
		runtime = &copy
	}
	m.mu.RUnlock()

	if !isReady {
		c.JSON(http.StatusServiceUnavailable, m.withRuntime(gin.H{
			"status":  "not_ready",
			"message": "bootstrap not yet complete",
		}))
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
		payload := gin.H{
			"status":       "not_ready",
			"failures":     failures,
			"capabilities": capabilityStates,
		}
		if runtime != nil {
			addRuntime(payload, *runtime)
		}
		c.JSON(http.StatusServiceUnavailable, payload)
		return
	}

	payload := gin.H{
		"status":       "ready",
		"checks":       len(checks),
		"capabilities": capabilityStates,
	}
	if runtime != nil {
		addRuntime(payload, *runtime)
	}
	c.JSON(http.StatusOK, payload)
}

func (m *HealthModule) withRuntime(payload gin.H) gin.H {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.runtime != nil {
		addRuntime(payload, *m.runtime)
	}
	return payload
}

func addRuntime(payload gin.H, info RuntimeInfo) {
	payload["version"] = info.Version
	payload["build_time"] = info.BuildTime
	payload["commit"] = info.Commit
	payload["grpc"] = info.GRPC
}
