// Package api provides endpoint adaptation for the canonical Go master API.
package api

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

// newAPIEndpoints defines the canonical endpoint paths.
var newAPIEndpoints = EndpointSet{
	RegisterWorker:   "/api/workers/register",
	UnregisterWorker: "/api/workers/unregister",
	Heartbeat:        "/api/workers/heartbeat",
	GetJob:           "/api/jobs/get",
	SubmitResult:     "/api/jobs/result",
	HealthCheck:      "/health",
	GetCommands:      "/api/workers/commands",
	AckCommand:       "/api/workers/commands/ack",
	UpdateStatus:     "/api/workers/status",
}

// EndpointAdapter provides API endpoint resolution for the canonical master API.
type EndpointAdapter struct {
	endpoints EndpointSet
}

// NewEndpointAdapter creates a new adapter for the canonical API.
func NewEndpointAdapter() *EndpointAdapter {
	return &EndpointAdapter{endpoints: newAPIEndpoints}
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
