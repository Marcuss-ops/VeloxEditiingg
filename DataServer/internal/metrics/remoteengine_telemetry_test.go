package metrics

import (
	"bytes"
	"strings"
	"testing"
)

func TestRemoteEngineTelemetry_ExportsClientCounters(t *testing.T) {
	registry := NewRegistry()
	telemetry := NewRemoteEngineTelemetry(registry)
	telemetry.RecordRequest("get_pipeline_status")
	telemetry.RecordRequest("start_pipeline")
	telemetry.RecordRetry("TRANSIENT")
	telemetry.RecordRetry("RATE_LIMIT")
	telemetry.RecordFailure("PERMANENT")
	telemetry.RecordFailure("UNKNOWN")

	var output bytes.Buffer
	if err := registry.WritePrometheus(&output); err != nil {
		t.Fatalf("WritePrometheus: %v", err)
	}
	for _, want := range []string{
		`velox_remote_engine_requests_total{method="get_pipeline_status"} 1`,
		`velox_remote_engine_requests_total{method="start_pipeline"} 1`,
		`velox_remote_engine_retries_total{class="TRANSIENT"} 1`,
		`velox_remote_engine_retries_total{class="RATE_LIMIT"} 1`,
		`velox_remote_engine_failures_total{class="PERMANENT"} 1`,
		`velox_remote_engine_failures_total{class="UNKNOWN"} 1`,
	} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("metrics output missing %q:\n%s", want, output.String())
		}
	}
}

func TestRemoteEngineTelemetry_NilRegistryIsNoop(t *testing.T) {
	telemetry := NewRemoteEngineTelemetry(nil)
	// All observers must be nil-safe (no panic, no effect).
	telemetry.RecordRequest("get_pipeline_status")
	telemetry.RecordRetry("TRANSIENT")
	telemetry.RecordFailure("PERMANENT")
}
