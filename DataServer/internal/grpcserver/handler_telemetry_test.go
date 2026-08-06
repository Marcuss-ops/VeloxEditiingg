// Package grpcserver / handler_telemetry_test.go
//
// Asserts that handleHeartbeat ingests the typed WorkerTelemetrySnapshot
// from the heartbeat Extra map, gates it (out-of-sequence / stale / worker
// mismatch / unsupported schema) and surfaces accepted fields onto the
// metrics sink and the session.
package grpcserver

import (
	"fmt"
	"testing"
	"time"

	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"

	"google.golang.org/protobuf/types/known/structpb"
)

// telemetryExtra builds a heartbeat Extra map carrying a typed
// telemetry_snapshot block (JSON-shaped, as the worker's structpb
// round-trip would produce: numbers as float64).
func telemetryExtra(snap controltransport.WorkerTelemetrySnapshot) *structpb.Struct {
	block := snap.AsMap()
	// AsMap emits Go ints; the worker serializes through JSON → structpb, so
	// coerce to the JSON shape for the master's FromMap.
	coerced := make(map[string]interface{}, len(block))
	for k, v := range block {
		coerced[k] = v
	}
	extra, err := structpb.NewStruct(map[string]interface{}{
		controltransport.TelemetrySnapshotExtraKey: coerced,
	})
	if err != nil {
		panic(fmt.Sprintf("build telemetry extra: %v", err))
	}
	return extra
}

func TestHandleHeartbeat_TelemetryAcceptedAndSurfacedToSink(t *testing.T) {
	sink := &recordableSink{}
	h := minimalHandlerForHeartbeat(t, sink)

	now := time.Now().UTC()
	h.handleHeartbeat("w-tel", "s-tel", &pb.Heartbeat{
		WorkerName: "w-tel",
		Resources:  &pb.WorkerResourceCounters{CpuUtilizationRatio: 0.5},
		Extra: telemetryExtra(controltransport.WorkerTelemetrySnapshot{
			WorkerID:       "w-tel",
			Sequence:       1,
			CapturedAt:     now,
			ActiveLeases:   2,
			DownloadQueue:  3,
			CacheBytes:     4096,
			CacheHitTotal:  100,
			CacheMissTotal: 20,
			RenderActive:   1,
			DiskFreeBytes:  1 << 30,
			SchemaVersion:  controltransport.TelemetrySnapshotSchemaVersion,
		}),
	})

	if sink.calls != 1 {
		t.Fatalf("sink calls=%d, want 1", sink.calls)
	}
	if sink.lastSnap == nil {
		t.Fatal("sink snapshot nil")
	}
	// Cache bytes from the typed snapshot must overlay the proto-derived
	// snapshot (previously hardcoded 0 — the PR-3 gap this task closes).
	if sink.lastSnap.CacheBytesUsed != 4096 {
		t.Errorf("CacheBytesUsed=%d, want 4096 (typed snapshot source)", sink.lastSnap.CacheBytesUsed)
	}
}

func TestHandleHeartbeat_TelemetryRejectsOutOfSequence(t *testing.T) {
	sink := &recordableSink{}
	h := minimalHandlerForHeartbeat(t, sink)
	now := time.Now().UTC()

	// Register a real session so the per-session gate persists between beats.
	sess := &workerSession{workerID: "w-oos", sessionID: "s-oos"}
	h.mu.Lock()
	h.sessions["s-oos"] = sess
	h.workerSessions["w-oos"] = "s-oos"
	h.mu.Unlock()

	base := func(seq uint64) *pb.Heartbeat {
		return &pb.Heartbeat{
			WorkerName: "w-oos",
			Resources:  &pb.WorkerResourceCounters{CpuUtilizationRatio: 0.5},
			Extra: telemetryExtra(controltransport.WorkerTelemetrySnapshot{
				WorkerID:      "w-oos",
				Sequence:      seq,
				CapturedAt:    now,
				CacheBytes:    100,
				SchemaVersion: controltransport.TelemetrySnapshotSchemaVersion,
			}),
		}
	}

	h.handleHeartbeat("w-oos", "s-oos", base(5))
	if got := sess.telemetry(); got == nil || got.Sequence != 5 {
		t.Fatalf("first snapshot not accepted: %+v", got)
	}

	// Replayed sequence must be rejected and must NOT overwrite the session
	// snapshot (sequence 3 < 5).
	h.handleHeartbeat("w-oos", "s-oos", base(3))
	if got := sess.telemetry(); got == nil || got.Sequence != 5 {
		t.Fatalf("out-of-order snapshot overwrote session: %+v", got)
	}

	// A fresh monotonic snapshot is still accepted afterwards.
	h.handleHeartbeat("w-oos", "s-oos", base(6))
	if got := sess.telemetry(); got == nil || got.Sequence != 6 {
		t.Fatalf("post-reject monotonic snapshot not accepted: %+v", got)
	}
}

func TestHandleHeartbeat_TelemetryRejectsStale(t *testing.T) {
	h := minimalHandlerForHeartbeat(t, nil)
	now := time.Now().UTC()

	sess := &workerSession{workerID: "w-stale", sessionID: "s-stale"}
	h.mu.Lock()
	h.sessions["s-stale"] = sess
	h.workerSessions["w-stale"] = "s-stale"
	h.mu.Unlock()

	h.handleHeartbeat("w-stale", "s-stale", &pb.Heartbeat{
		WorkerName: "w-stale",
		Resources:  &pb.WorkerResourceCounters{},
		Extra: telemetryExtra(controltransport.WorkerTelemetrySnapshot{
			WorkerID:      "w-stale",
			Sequence:      1,
			CapturedAt:    now.Add(-2 * controltransport.DefaultTelemetrySnapshotMaxAge),
			SchemaVersion: controltransport.TelemetrySnapshotSchemaVersion,
		}),
	})
	if got := sess.telemetry(); got != nil {
		t.Fatalf("stale snapshot accepted: %+v", got)
	}
}

func TestHandleHeartbeat_TelemetryRejectsWorkerMismatch(t *testing.T) {
	h := minimalHandlerForHeartbeat(t, nil)
	now := time.Now().UTC()

	sess := &workerSession{workerID: "w-sess", sessionID: "s-sess"}
	h.mu.Lock()
	h.sessions["s-sess"] = sess
	h.workerSessions["w-sess"] = "s-sess"
	h.mu.Unlock()

	h.handleHeartbeat("w-sess", "s-sess", &pb.Heartbeat{
		WorkerName: "w-sess",
		Resources:  &pb.WorkerResourceCounters{},
		Extra: telemetryExtra(controltransport.WorkerTelemetrySnapshot{
			WorkerID:      "w-other", // does not match the session
			Sequence:      1,
			CapturedAt:    now,
			SchemaVersion: controltransport.TelemetrySnapshotSchemaVersion,
		}),
	})
	if got := sess.telemetry(); got != nil {
		t.Fatalf("mismatched-worker snapshot accepted: %+v", got)
	}
}

func TestHandleHeartbeat_TelemetryRejectsUnsupportedSchema(t *testing.T) {
	h := minimalHandlerForHeartbeat(t, nil)
	now := time.Now().UTC()

	sess := &workerSession{workerID: "w-schema", sessionID: "s-schema"}
	h.mu.Lock()
	h.sessions["s-schema"] = sess
	h.workerSessions["w-schema"] = "s-schema"
	h.mu.Unlock()

	h.handleHeartbeat("w-schema", "s-schema", &pb.Heartbeat{
		WorkerName: "w-schema",
		Resources:  &pb.WorkerResourceCounters{},
		Extra: telemetryExtra(controltransport.WorkerTelemetrySnapshot{
			WorkerID:      "w-schema",
			Sequence:      1,
			CapturedAt:    now,
			SchemaVersion: 999,
		}),
	})
	if got := sess.telemetry(); got != nil {
		t.Fatalf("unsupported-schema snapshot accepted: %+v", got)
	}
}

func TestHandleHeartbeat_TelemetryAbsentIsNoop(t *testing.T) {
	sink := &recordableSink{}
	h := minimalHandlerForHeartbeat(t, sink)
	h.handleHeartbeat("w-none", "s-none", &pb.Heartbeat{
		WorkerName: "w-none",
		Resources:  &pb.WorkerResourceCounters{CpuUtilizationRatio: 0.5},
	})
	if sink.calls != 1 {
		t.Fatalf("sink calls=%d, want 1 (absent snapshot must not suppress resources)", sink.calls)
	}
	if sink.lastSnap.CacheBytesUsed != 0 {
		t.Errorf("CacheBytesUsed=%d, want 0 (no typed snapshot)", sink.lastSnap.CacheBytesUsed)
	}
}
