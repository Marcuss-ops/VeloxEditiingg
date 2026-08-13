package metrics

import (
	"bytes"
	"strings"
	"testing"
)

func TestForwardingTelemetry_ExportsRunnerCounters(t *testing.T) {
	registry := NewRegistry()
	telemetry := NewForwardingTelemetry(registry)
	telemetry.RecordClaimed(3)
	telemetry.RecordForwarded()
	telemetry.RecordFailed()
	telemetry.RecordRetried()
	telemetry.ObserveQueue(12, 340)

	var output bytes.Buffer
	if err := registry.WritePrometheus(&output); err != nil {
		t.Fatalf("WritePrometheus: %v", err)
	}
	for _, want := range []string{
		"velox_forwarding_claimed_total 3",
		"velox_forwarding_forwarded_total 1",
		"velox_forwarding_failed_total 1",
		"velox_forwarding_retried_total 1",
		"velox_forwarding_queue_depth 12",
		"velox_forwarding_oldest_pending_seconds 340",
	} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("metrics output missing %q:\n%s", want, output.String())
		}
	}
}

func TestForwardingTelemetry_NilRegistryIsNoop(t *testing.T) {
	telemetry := NewForwardingTelemetry(nil)
	// All observers must be nil-safe (no panic, no effect).
	telemetry.RecordClaimed(1)
	telemetry.RecordForwarded()
	telemetry.RecordFailed()
	telemetry.RecordRetried()
	telemetry.ObserveQueue(1, 2)
}
