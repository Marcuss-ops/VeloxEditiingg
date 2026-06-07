// Package api provides endpoint adaptation for legacy and new API modes.
package api

import "velox-worker-agent/pkg/config"

// EndpointSet represents a set of API endpoints for a specific API version.
type EndpointSet struct {
	RegisterWorker   string
	UnregisterWorker string
	Heartbeat        string
	GetJob           string
	SubmitResult     string
	HealthCheck      string
	GetCommands      string
	AckCommand       string
	UpdateStatus     string
}

// endpointSets defines the endpoint paths for each API mode.
var endpointSets = map[config.APIMode]EndpointSet{
	config.APIModeNewAPI: {
		RegisterWorker:   "/api/workers/register",
		UnregisterWorker: "/api/workers/unregister",
		Heartbeat:        "/api/workers/heartbeat",
		GetJob:           "/api/jobs/get",
		SubmitResult:     "/api/jobs/result",
		HealthCheck:      "/health",
		GetCommands:      "/api/v1/worker/commands",
		AckCommand:       "/api/workers/commands/ack",
		UpdateStatus:     "/api/workers/status",
	},
	config.APIModeLegacyV1: {
		RegisterWorker:   "/api/v1/workers/register",
		UnregisterWorker: "/api/v1/workers/unregister",
		Heartbeat:        "/api/v1/workers/heartbeat",
		GetJob:           "/api/v1/queue/job",
		SubmitResult:     "/api/v1/jobs/result",
		HealthCheck:      "/api/v1/health",
		GetCommands:      "/api/v1/workers/commands",
		AckCommand:       "/api/v1/workers/commands/ack",
		UpdateStatus:     "/api/v1/workers/status",
	},
}

// EndpointAdapter provides API endpoint resolution based on the configured mode.
type EndpointAdapter struct {
	mode      config.APIMode
	endpoints EndpointSet
}

// NewEndpointAdapter creates a new adapter for the given API mode.
func NewEndpointAdapter(mode config.APIMode) *EndpointAdapter {
	// Default to new API if mode is empty or invalid
	if mode == "" {
		mode = config.APIModeNewAPI
	}

	endpoints, ok := endpointSets[mode]
	if !ok {
		// Fallback to new API for unknown modes
		endpoints = endpointSets[config.APIModeNewAPI]
	}

	return &EndpointAdapter{
		mode:      mode,
		endpoints: endpoints,
	}
}

// Mode returns the current API mode.
func (a *EndpointAdapter) Mode() config.APIMode {
	return a.mode
}

// Endpoints returns the current endpoint set.
func (a *EndpointAdapter) Endpoints() EndpointSet {
	return a.endpoints
}

// RegisterWorker returns the endpoint for worker registration.
func (a *EndpointAdapter) RegisterWorker() string {
	return a.endpoints.RegisterWorker
}

// UnregisterWorker returns the endpoint for worker unregistration.
func (a *EndpointAdapter) UnregisterWorker() string {
	return a.endpoints.UnregisterWorker
}

// Heartbeat returns the endpoint for heartbeat messages.
func (a *EndpointAdapter) Heartbeat() string {
	return a.endpoints.Heartbeat
}

// GetJob returns the endpoint for job retrieval.
func (a *EndpointAdapter) GetJob() string {
	return a.endpoints.GetJob
}

// SubmitResult returns the endpoint for job result submission.
func (a *EndpointAdapter) SubmitResult() string {
	return a.endpoints.SubmitResult
}

// HealthCheck returns the endpoint for health checks.
func (a *EndpointAdapter) HealthCheck() string {
	return a.endpoints.HealthCheck
}

// IsLegacy returns true if the adapter is using legacy v1 endpoints.
func (a *EndpointAdapter) IsLegacy() bool {
	return a.mode == config.APIModeLegacyV1
}

// GetCommands returns the endpoint for fetching worker commands.
func (a *EndpointAdapter) GetCommands() string {
	return a.endpoints.GetCommands
}

// AckCommand returns the endpoint for acknowledging commands.
func (a *EndpointAdapter) AckCommand() string {
	return a.endpoints.AckCommand
}

// UpdateStatus returns the endpoint for status updates.
func (a *EndpointAdapter) UpdateStatus() string {
	return a.endpoints.UpdateStatus
}
