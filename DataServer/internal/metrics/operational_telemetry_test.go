package metrics

import (
	"bytes"
	"strings"
	"testing"
)

func TestOperationalTelemetry_ExportsDBPoolStats(t *testing.T) {
	registry := NewRegistry()
	telemetry := NewOperationalTelemetry(registry)
	telemetry.ObserveDBStats(4, 2, 2, 7, 1250)

	var output bytes.Buffer
	if err := registry.WritePrometheus(&output); err != nil {
		t.Fatalf("WritePrometheus: %v", err)
	}
	for _, want := range []string{
		"velox_db_open_connections 4",
		"velox_db_in_use_connections 2",
		"velox_db_idle_connections 2",
		"velox_db_wait_count 7",
		"velox_db_wait_duration_ms 1250",
	} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("metrics output missing %q:\n%s", want, output.String())
		}
	}
}
