package metrics

import (
	"strings"
	"testing"
	"time"
)

func TestPrefetchTelemetryIsExposedWithBoundedLabels(t *testing.T) {
	reg := NewRegistry()
	collector := NewCollector(reg)
	collector.RecordPrefetchJob("velox-worker-51", "received")
	collector.RecordPrefetchJob("velox-worker-51", "applied")
	collector.RecordPrefetchAsset("velox-worker-51", "prefetch", "hit", 4096)
	collector.RecordPrefetchAsset("velox-worker-51", "runtime_download", "miss", 2048)
	collector.RecordPrefetchFailure("velox-worker-51", "download")
	collector.RecordPrefetchDuration("velox-worker-51", 2*time.Second)

	out := dumpRegistryAll(t, reg)
	for _, want := range []string{
		`velox_prefetch_jobs_total{worker_id="velox-worker-51",outcome="received"} 1`,
		`velox_prefetch_jobs_total{worker_id="velox-worker-51",outcome="applied"} 1`,
		`velox_prefetch_bytes_total{worker_id="velox-worker-51",origin="prefetch"} 4096`,
		`velox_prefetch_cache_events_total{worker_id="velox-worker-51",origin="prefetch",result="hit"} 1`,
		`velox_prefetch_cache_events_total{worker_id="velox-worker-51",origin="runtime_download",result="miss"} 1`,
		`velox_prefetch_failures_total{worker_id="velox-worker-51",reason="download"} 1`,
		`velox_prefetch_duration_seconds_count{worker_id="velox-worker-51"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

func TestPrefetchTelemetryDropsUnboundedValues(t *testing.T) {
	reg := NewRegistry()
	collector := NewCollector(reg)
	collector.RecordPrefetchJob("worker-1", "arbitrary-error-message")
	collector.RecordPrefetchFailure("worker-1", "arbitrary-error-message")
	collector.RecordPrefetchAsset("worker-1", "arbitrary-origin", "arbitrary-result", 1)

	out := dumpRegistryAll(t, reg)
	if strings.Contains(out, "arbitrary-error-message") || strings.Contains(out, "arbitrary-origin") || strings.Contains(out, "arbitrary-result") {
		t.Fatalf("unbounded prefetch label escaped into metrics:\n%s", out)
	}
}
