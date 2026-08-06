package controltransport

import (
	"testing"
	"time"
)

func TestTelemetrySnapshot_AsMapRoundTrip(t *testing.T) {
	captured := time.Date(2026, 8, 6, 10, 11, 12, 123456000, time.UTC)
	in := WorkerTelemetrySnapshot{
		WorkerID:        "w-1",
		Sequence:        7,
		CapturedAt:      captured,
		ActiveLeases:    3,
		DownloadQueue:   2,
		CacheBytes:      4096,
		CacheHitTotal:   120,
		CacheMissTotal:  40,
		RenderActive:    1,
		DiskFreeBytes:   1 << 30,
		SchemaVersion:   TelemetrySnapshotSchemaVersion,
		SoftwareRelease: ReleaseIdentity{ImageDigest: "sha256:" + "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2c3", SourceCommit: "deadbeef"},
	}

	out, ok := TelemetrySnapshotFromMap(in.AsMap())
	if !ok {
		t.Fatal("FromMap(AsMap()) returned ok=false")
	}
	if out.WorkerID != in.WorkerID || out.Sequence != in.Sequence {
		t.Errorf("identity drift: got worker=%s seq=%d want worker=%s seq=%d", out.WorkerID, out.Sequence, in.WorkerID, in.Sequence)
	}
	if !out.CapturedAt.Equal(captured) {
		t.Errorf("CapturedAt drift: got %v want %v", out.CapturedAt, captured)
	}
	if out.ActiveLeases != 3 || out.DownloadQueue != 2 || out.RenderActive != 1 {
		t.Errorf("state counters drift: leases=%d queue=%d render=%d", out.ActiveLeases, out.DownloadQueue, out.RenderActive)
	}
	if out.CacheBytes != 4096 || out.CacheHitTotal != 120 || out.CacheMissTotal != 40 {
		t.Errorf("cache counters drift: bytes=%d hit=%d miss=%d", out.CacheBytes, out.CacheHitTotal, out.CacheMissTotal)
	}
	if out.DiskFreeBytes != 1<<30 {
		t.Errorf("DiskFreeBytes drift: got %d", out.DiskFreeBytes)
	}
	if out.SchemaVersion != TelemetrySnapshotSchemaVersion {
		t.Errorf("SchemaVersion drift: got %d want %d", out.SchemaVersion, TelemetrySnapshotSchemaVersion)
	}
	if out.SoftwareRelease.ImageDigest != in.SoftwareRelease.ImageDigest || out.SoftwareRelease.SourceCommit != "deadbeef" {
		t.Errorf("SoftwareRelease drift: %+v", out.SoftwareRelease)
	}
}

func TestTelemetrySnapshot_FromMapToleratesJSONShapes(t *testing.T) {
	// structpb / JSON round-trips coerce numbers to float64. FromMap must
	// accept those shapes without losing precision.
	in := map[string]interface{}{
		"worker_id":        "w-json",
		"sequence":         float64(99),
		"captured_at":      "2026-08-06T10:11:12.123456Z",
		"active_leases":    float64(4),
		"download_queue":   float64(1),
		"cache_bytes":      float64(8192),
		"cache_hit_total":  float64(200),
		"cache_miss_total": float64(50),
		"render_active":    float64(2),
		"disk_free_bytes":  float64(1 << 20),
		"schema_version":   float64(1),
	}
	snap, ok := TelemetrySnapshotFromMap(in)
	if !ok {
		t.Fatal("FromMap(JSON shapes) returned ok=false")
	}
	if snap.Sequence != 99 || snap.ActiveLeases != 4 || snap.DownloadQueue != 1 {
		t.Errorf("JSON numeric drift: seq=%d leases=%d queue=%d", snap.Sequence, snap.ActiveLeases, snap.DownloadQueue)
	}
	if snap.CacheBytes != 8192 || snap.CacheHitTotal != 200 || snap.CacheMissTotal != 50 || snap.DiskFreeBytes != 1<<20 {
		t.Errorf("JSON cache/disk drift: %+v", snap)
	}
	if snap.SchemaVersion != 1 || snap.RenderActive != 2 {
		t.Errorf("JSON schema/render drift: schema=%d render=%d", snap.SchemaVersion, snap.RenderActive)
	}
}

func TestTelemetrySnapshot_FromMapAbsentBlock(t *testing.T) {
	if _, ok := TelemetrySnapshotFromMap(nil); ok {
		t.Fatal("nil block must decode as absent")
	}
	if _, ok := TelemetrySnapshotFromMap(map[string]interface{}{"other": "key"}); ok {
		t.Fatal("block without worker_id must decode as absent")
	}
}

func TestTelemetrySnapshot_Validate(t *testing.T) {
	base := WorkerTelemetrySnapshot{
		WorkerID:      "w-v",
		Sequence:      1,
		CapturedAt:    time.Now().UTC(),
		SchemaVersion: TelemetrySnapshotSchemaVersion,
	}
	if err := base.Validate(); err != nil {
		t.Fatalf("valid snapshot rejected: %v", err)
	}
	cases := []struct {
		name   string
		mutate func(*WorkerTelemetrySnapshot)
	}{
		{"empty worker", func(s *WorkerTelemetrySnapshot) { s.WorkerID = "" }},
		{"bad schema", func(s *WorkerTelemetrySnapshot) { s.SchemaVersion = 999 }},
		{"zero sequence", func(s *WorkerTelemetrySnapshot) { s.Sequence = 0 }},
		{"zero captured", func(s *WorkerTelemetrySnapshot) { s.CapturedAt = time.Time{} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := base
			tc.mutate(&s)
			if err := s.Validate(); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestTelemetryGate_AcceptsMonotonicSequence(t *testing.T) {
	g := NewTelemetryGate("w-gate", 0)
	now := time.Now().UTC()
	if reason := g.Accept(WorkerTelemetrySnapshot{WorkerID: "w-gate", Sequence: 1, CapturedAt: now, SchemaVersion: 1}, now); reason != TelemetryRejectNone {
		t.Fatalf("first snapshot rejected: %s", reason)
	}
	if reason := g.Accept(WorkerTelemetrySnapshot{WorkerID: "w-gate", Sequence: 2, CapturedAt: now, SchemaVersion: 1}, now); reason != TelemetryRejectNone {
		t.Fatalf("monotonic snapshot rejected: %s", reason)
	}
}

func TestTelemetryGate_RejectsZeroSequence(t *testing.T) {
	g := NewTelemetryGate("w-zero", 0)
	now := time.Now().UTC()
	snap := WorkerTelemetrySnapshot{WorkerID: "w-zero", Sequence: 0, CapturedAt: now, SchemaVersion: 1}
	if reason := g.Accept(snap, now); reason != TelemetryRejectOutOfSequence {
		t.Fatalf("zero sequence reason=%s want out_of_sequence", reason)
	}
	// The rejected zero-sequence snapshot must not advance the baseline: a
	// valid first sequence is still accepted.
	snap.Sequence = 1
	if reason := g.Accept(snap, now); reason != TelemetryRejectNone {
		t.Fatalf("post-zero-reject valid sequence rejected: %s", reason)
	}
}

func TestTelemetryGate_RejectsOutOfSequence(t *testing.T) {
	g := NewTelemetryGate("w-gate", 0)
	now := time.Now().UTC()
	if reason := g.Accept(WorkerTelemetrySnapshot{WorkerID: "w-gate", Sequence: 10, CapturedAt: now, SchemaVersion: 1}, now); reason != TelemetryRejectNone {
		t.Fatalf("baseline snapshot rejected: %s", reason)
	}
	// Equal sequence must be rejected (replay).
	if reason := g.Accept(WorkerTelemetrySnapshot{WorkerID: "w-gate", Sequence: 10, CapturedAt: now, SchemaVersion: 1}, now); reason != TelemetryRejectOutOfSequence {
		t.Fatalf("duplicate sequence reason=%s want out_of_sequence", reason)
	}
	// Lower sequence must be rejected (reorder).
	if reason := g.Accept(WorkerTelemetrySnapshot{WorkerID: "w-gate", Sequence: 9, CapturedAt: now, SchemaVersion: 1}, now); reason != TelemetryRejectOutOfSequence {
		t.Fatalf("regressed sequence reason=%s want out_of_sequence", reason)
	}
	// A rejected snapshot must not poison the baseline: the next higher
	// sequence is still accepted.
	if reason := g.Accept(WorkerTelemetrySnapshot{WorkerID: "w-gate", Sequence: 11, CapturedAt: now, SchemaVersion: 1}, now); reason != TelemetryRejectNone {
		t.Fatalf("post-reject monotonic snapshot rejected: %s", reason)
	}
}

func TestTelemetryGate_RejectsStale(t *testing.T) {
	g := NewTelemetryGate("w-gate", 0)
	now := time.Now().UTC()
	old := WorkerTelemetrySnapshot{WorkerID: "w-gate", Sequence: 1, CapturedAt: now.Add(-DefaultTelemetrySnapshotMaxAge - time.Second), SchemaVersion: 1}
	if reason := g.Accept(old, now); reason != TelemetryRejectStale {
		t.Fatalf("stale snapshot reason=%s want stale", reason)
	}
	// Future-dated beyond skew is also rejected.
	future := WorkerTelemetrySnapshot{WorkerID: "w-gate", Sequence: 2, CapturedAt: now.Add(DefaultTelemetrySnapshotMaxAge + time.Second), SchemaVersion: 1}
	if reason := g.Accept(future, now); reason != TelemetryRejectStale {
		t.Fatalf("future-dated snapshot reason=%s want stale", reason)
	}
}

func TestTelemetryGate_RejectsWorkerMismatch(t *testing.T) {
	g := NewTelemetryGate("w-session", 0)
	now := time.Now().UTC()
	snap := WorkerTelemetrySnapshot{WorkerID: "w-other", Sequence: 1, CapturedAt: now, SchemaVersion: 1}
	if reason := g.Accept(snap, now); reason != TelemetryRejectWorkerMismatch {
		t.Fatalf("mismatched worker reason=%s want worker_mismatch", reason)
	}
}

func TestTelemetryGate_RejectsUnsupportedSchema(t *testing.T) {
	g := NewTelemetryGate("w-schema", 0)
	now := time.Now().UTC()
	snap := WorkerTelemetrySnapshot{WorkerID: "w-schema", Sequence: 1, CapturedAt: now, SchemaVersion: 999}
	if reason := g.Accept(snap, now); reason != TelemetryRejectUnsupportedSchema {
		t.Fatalf("unsupported schema reason=%s want unsupported_schema", reason)
	}
	// The rejected schema must not advance the baseline: the same sequence
	// with the correct schema is accepted.
	snap.SchemaVersion = TelemetrySnapshotSchemaVersion
	if reason := g.Accept(snap, now); reason != TelemetryRejectNone {
		t.Fatalf("post-reject valid schema rejected: %s", reason)
	}
}
