package metrics

import (
	"strings"
	"testing"
	"time"
)

func TestCollectorRecordsTaskOfferDispatch(t *testing.T) {
	reg := NewRegistry()
	collector := NewCollector(reg)
	collector.RecordTaskOfferDispatch("worker-1", 1500*time.Millisecond)
	collector.RecordTaskOfferDispatch("", time.Second)
	collector.RecordTaskOfferDispatch("worker-1", 0)
	out := dumpRegistryAll(t, reg)
	for _, want := range []string{
		"# TYPE velox_task_offer_dispatch_seconds histogram",
		`velox_task_offer_dispatch_seconds_bucket{worker_id="worker-1",le="1"} 0`,
		`velox_task_offer_dispatch_seconds_bucket{worker_id="worker-1",le="2"} 1`,
		`velox_task_offer_dispatch_seconds_count{worker_id="worker-1"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}
