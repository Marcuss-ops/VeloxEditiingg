package worker

import (
	"context"
	"testing"
	"time"

	"velox-worker-agent/internal/telemetry"
)

func TestRecordCacheProjectionEvent_AppendsCanonicalJournalFacts(t *testing.T) {
	recorder := telemetry.NewEventRecorder()
	ctx := telemetry.WithRecorder(context.Background(), recorder)

	recordCacheProjectionEvent(ctx, "hash_verify", 17*time.Millisecond, telemetry.StatusOK, "", 0)
	recordCacheProjectionEvent(ctx, "eviction", 0, telemetry.StatusOK, "invalid", 0)

	events := recorder.Snapshot()
	if len(events) != 2 {
		t.Fatalf("events=%d, want 2", len(events))
	}
	if events[0].Component != "worker.cache" || events[0].Action != "hash_verify" || events[0].DurationMS != 17 {
		t.Fatalf("hash event=%+v", events[0])
	}
	if events[1].Component != "worker.cache" || events[1].Action != "eviction" || events[1].MetadataJSON != `{"reason":"invalid"}` {
		t.Fatalf("eviction event=%+v", events[1])
	}
}
