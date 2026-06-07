package api

import (
	"testing"

	"velox-worker-agent/pkg/config"
)

func TestEndpointAdapter_NewAPI(t *testing.T) {
	adapter := NewEndpointAdapter(config.APIModeNewAPI)

	tests := []struct {
		name     string
		got      string
		expected string
	}{
		{"RegisterWorker", adapter.RegisterWorker(), "/api/workers/register"},
		{"UnregisterWorker", adapter.UnregisterWorker(), "/api/workers/unregister"},
		{"Heartbeat", adapter.Heartbeat(), "/api/workers/heartbeat"},
		{"GetJob", adapter.GetJob(), "/api/jobs/get"},
		{"GetCommands", adapter.GetCommands(), "/api/v1/worker/commands"},
		{"SubmitResult", adapter.SubmitResult(), "/api/jobs/result"},
		{"HealthCheck", adapter.HealthCheck(), "/health"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.expected {
				t.Errorf("%s = %q, want %q", tt.name, tt.got, tt.expected)
			}
		})
	}

	if adapter.Mode() != config.APIModeNewAPI {
		t.Errorf("Mode() = %q, want %q", adapter.Mode(), config.APIModeNewAPI)
	}

	if adapter.IsLegacy() {
		t.Error("IsLegacy() = true, want false")
	}
}

func TestEndpointAdapter_LegacyV1(t *testing.T) {
	adapter := NewEndpointAdapter(config.APIModeLegacyV1)

	tests := []struct {
		name     string
		got      string
		expected string
	}{
		{"RegisterWorker", adapter.RegisterWorker(), "/api/v1/workers/register"},
		{"UnregisterWorker", adapter.UnregisterWorker(), "/api/v1/workers/unregister"},
		{"Heartbeat", adapter.Heartbeat(), "/api/v1/workers/heartbeat"},
		{"GetJob", adapter.GetJob(), "/api/v1/queue/job"},
		{"SubmitResult", adapter.SubmitResult(), "/api/v1/jobs/result"},
		{"HealthCheck", adapter.HealthCheck(), "/api/v1/health"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.expected {
				t.Errorf("%s = %q, want %q", tt.name, tt.got, tt.expected)
			}
		})
	}

	if adapter.Mode() != config.APIModeLegacyV1 {
		t.Errorf("Mode() = %q, want %q", adapter.Mode(), config.APIModeLegacyV1)
	}

	if !adapter.IsLegacy() {
		t.Error("IsLegacy() = false, want true")
	}
}

func TestEndpointAdapter_EmptyMode(t *testing.T) {
	// Empty mode should default to new API
	adapter := NewEndpointAdapter("")

	if adapter.Mode() != config.APIModeNewAPI {
		t.Errorf("Mode() = %q, want %q", adapter.Mode(), config.APIModeNewAPI)
	}

	if adapter.IsLegacy() {
		t.Error("IsLegacy() = true, want false for empty mode")
	}
}

func TestEndpointAdapter_UnknownMode(t *testing.T) {
	// Unknown mode should fallback to new API
	adapter := NewEndpointAdapter("unknown_mode")

	if adapter.Mode() != "unknown_mode" {
		t.Errorf("Mode() = %q, want %q", adapter.Mode(), "unknown_mode")
	}

	// Should still return valid endpoints (fallback to new API)
	if adapter.RegisterWorker() != "/api/workers/register" {
		t.Errorf("RegisterWorker() = %q, want %q (fallback)", adapter.RegisterWorker(), "/api/workers/register")
	}
}

func TestEndpointAdapter_Endpoints(t *testing.T) {
	adapter := NewEndpointAdapter(config.APIModeNewAPI)
	endpoints := adapter.Endpoints()

	if endpoints.RegisterWorker != "/api/workers/register" {
		t.Errorf("Endpoints().RegisterWorker = %q, want %q", endpoints.RegisterWorker, "/api/workers/register")
	}

	if endpoints.GetJob != "/api/jobs/get" {
		t.Errorf("Endpoints().GetJob = %q, want %q", endpoints.GetJob, "/api/jobs/get")
	}
}
