package controltransport

// telemetry_snapshot.go — the canonical typed worker telemetry snapshot.
//
// WorkerTelemetrySnapshot is the single typed payload through which a worker
// reports its operational state to the master: sequence (replay/ordering
// guard), capture timestamp, active leases, download queue depth, cache
// accounting, active renders, disk free bytes and the release certificate.
//
// The wire vehicle is the heartbeat Extra map (JSON-shaped, like the
// release_identity capabilities block): the worker emits it under
// TelemetrySnapshotExtraKey and the master parses + gates it. Free-form
// heartbeat metrics (extra["metrics"], extra["resources"]) remain for
// backward-compatible read paths, but the typed snapshot is the canonical
// state source for the fields it carries.

import (
	"fmt"
	"time"
)

// TelemetrySnapshotExtraKey is the heartbeat-Extra map key under which the
// worker publishes its typed telemetry snapshot.
const TelemetrySnapshotExtraKey = "telemetry_snapshot"

// TelemetrySnapshotSchemaVersion is the current wire schema. The master
// rejects snapshots carrying any other value (TelemetryRejectUnsupportedSchema);
// bump it on any backward-incompatible field/shape change.
const TelemetrySnapshotSchemaVersion = 1

// DefaultTelemetrySnapshotMaxAge is the staleness window used by
// NewTelemetryGate when the caller supplies a non-positive threshold.
// It exceeds the worker heartbeat cadence (15s busy / 60s idle) by a wide
// margin while still catching a snapshot that never left the worker.
const DefaultTelemetrySnapshotMaxAge = 5 * time.Minute

// WorkerTelemetrySnapshot is the canonical worker operational state report.
//
// Field provenance:
//   - ActiveLeases   — len(worker.activeTaskLeases) (task-native lease store)
//   - DownloadQueue  — asset download manager queued transfers
//   - CacheBytes     — content-addressed cache bytes used
//   - CacheHitTotal  — cumulative cache hits (monotonic)
//   - CacheMissTotal — cumulative cache misses (monotonic)
//   - RenderActive   — len(worker.activeTasks)
//   - DiskFreeBytes  — sampler statvfs free bytes
//   - SoftwareRelease— the single release certificate (shared/controltransport)
type WorkerTelemetrySnapshot struct {
	WorkerID        string
	Sequence        uint64
	CapturedAt      time.Time
	ActiveLeases    int
	DownloadQueue   int
	CacheBytes      int64
	CacheHitTotal   uint64
	CacheMissTotal  uint64
	RenderActive    int
	DiskFreeBytes   int64
	SoftwareRelease ReleaseIdentity
	SchemaVersion   int
}

// Validate reports a non-nil error when the snapshot cannot be admitted:
// missing worker identity, zero/invalid schema, zero sequence or a zero
// capture timestamp. It does NOT perform the stateful gate checks
// (sequence monotonicity / staleness) — those belong to TelemetryGate.
func (s WorkerTelemetrySnapshot) Validate() error {
	if s.WorkerID == "" {
		return fmt.Errorf("telemetry snapshot: empty worker_id")
	}
	if s.SchemaVersion != TelemetrySnapshotSchemaVersion {
		return fmt.Errorf("telemetry snapshot: unsupported schema_version %d (supported: %d)",
			s.SchemaVersion, TelemetrySnapshotSchemaVersion)
	}
	if s.Sequence == 0 {
		return fmt.Errorf("telemetry snapshot: zero sequence")
	}
	if s.CapturedAt.IsZero() {
		return fmt.Errorf("telemetry snapshot: zero captured_at")
	}
	return nil
}

// AsMap renders the snapshot as a JSON-shaped map suitable for embedding in
// the heartbeat Extra structpb (numbers are emitted as JSON numbers; the
// master's FromMap accepts the float64 shapes that a JSON round-trip
// produces). SoftwareRelease rides as a nested block when non-empty,
// reusing the canonical ReleaseIdentity capabilities block shape.
func (s WorkerTelemetrySnapshot) AsMap() map[string]interface{} {
	m := map[string]interface{}{
		"schema_version":   s.SchemaVersion,
		"worker_id":        s.WorkerID,
		"sequence":         s.Sequence,
		"captured_at":      s.CapturedAt.UTC().Format(time.RFC3339Nano),
		"active_leases":    s.ActiveLeases,
		"download_queue":   s.DownloadQueue,
		"cache_bytes":      s.CacheBytes,
		"cache_hit_total":  s.CacheHitTotal,
		"cache_miss_total": s.CacheMissTotal,
		"render_active":    s.RenderActive,
		"disk_free_bytes":  s.DiskFreeBytes,
	}
	if !s.SoftwareRelease.IsEmpty() {
		m["software_release"] = s.SoftwareRelease.AsCapabilitiesBlock()
	}
	return m
}

// TelemetrySnapshotFromMap decodes a heartbeat-Extra telemetry_snapshot block
// (or nil) into a WorkerTelemetrySnapshot. It tolerates the JSON round-trip
// shapes (float64/int32/int64 for numbers) that structpb produces. The bool
// is false when the block is absent or not an object map.
func TelemetrySnapshotFromMap(m map[string]interface{}) (WorkerTelemetrySnapshot, bool) {
	if m == nil {
		return WorkerTelemetrySnapshot{}, false
	}
	snap := WorkerTelemetrySnapshot{
		WorkerID:      stringValue(m["worker_id"]),
		Sequence:      uint64Value(m["sequence"]),
		CapturedAt:    timeValue(m["captured_at"]),
		SchemaVersion: intValue(m["schema_version"]),
	}
	// Schema defaults to the current version when absent so a heartbeat that
	// omits the field still round-trips (the gate still rejects a snapshot
	// whose explicit schema is unsupported).
	if snap.SchemaVersion == 0 {
		snap.SchemaVersion = TelemetrySnapshotSchemaVersion
	}
	// Presence of the worker_id key distinguishes "block present" from "block
	// absent" even for a degenerate all-zero snapshot.
	if _, ok := m["worker_id"]; !ok {
		return WorkerTelemetrySnapshot{}, false
	}
	snap.ActiveLeases = intValue(m["active_leases"])
	snap.DownloadQueue = intValue(m["download_queue"])
	snap.CacheBytes = int64Value(m["cache_bytes"])
	snap.CacheHitTotal = uint64Value(m["cache_hit_total"])
	snap.CacheMissTotal = uint64Value(m["cache_miss_total"])
	snap.RenderActive = intValue(m["render_active"])
	snap.DiskFreeBytes = int64Value(m["disk_free_bytes"])
	if block, ok := m["software_release"].(map[string]interface{}); ok {
		// ReleaseIdentityFromCapabilities reads the canonical block under
		// CapabilityReleaseIdentityKey; wrap the stored block so the same
		// decoder is reused for both the capabilities map and this snapshot.
		if ri, found := ReleaseIdentityFromCapabilities(map[string]interface{}{CapabilityReleaseIdentityKey: block}); found {
			snap.SoftwareRelease = ri
		}
	}
	return snap, true
}

func stringValue(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func timeValue(v interface{}) time.Time {
	s, ok := v.(string)
	if !ok {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}
	}
	return t
}

func intValue(v interface{}) int {
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	case int32:
		return int(n)
	case int64:
		return int(n)
	case uint64:
		return int(n)
	}
	return 0
}

func int64Value(v interface{}) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case int:
		return int64(n)
	case int32:
		return int64(n)
	case int64:
		return n
	case uint64:
		return int64(n)
	}
	return 0
}

func uint64Value(v interface{}) uint64 {
	switch n := v.(type) {
	case float64:
		if n < 0 {
			return 0
		}
		return uint64(n)
	case int:
		if n < 0 {
			return 0
		}
		return uint64(n)
	case int32:
		if n < 0 {
			return 0
		}
		return uint64(n)
	case int64:
		if n < 0 {
			return 0
		}
		return uint64(n)
	case uint64:
		return n
	}
	return 0
}

// TelemetryRejectReason classifies why a telemetry snapshot was rejected.
// The zero value means the snapshot was accepted.
type TelemetryRejectReason string

const (
	TelemetryRejectNone              TelemetryRejectReason = ""
	TelemetryRejectOutOfSequence     TelemetryRejectReason = "out_of_sequence"
	TelemetryRejectStale             TelemetryRejectReason = "stale"
	TelemetryRejectWorkerMismatch    TelemetryRejectReason = "worker_mismatch"
	TelemetryRejectUnsupportedSchema TelemetryRejectReason = "unsupported_schema"
)

// String returns the wire-safe rejection reason.
func (r TelemetryRejectReason) String() string { return string(r) }

// TelemetryGate enforces the admission contract for a single worker's
// telemetry snapshots:
//
//   - worker identity must match the session (TelemetryRejectWorkerMismatch)
//   - schema version must equal TelemetrySnapshotSchemaVersion
//     (TelemetryRejectUnsupportedSchema)
//   - CapturedAt must be within maxStale of the reference clock
//     (TelemetryRejectStale)
//   - Sequence must be strictly monotonic per worker lifetime
//     (TelemetryRejectOutOfSequence)
//
// The gate is stateful (it remembers the last accepted sequence) and is NOT
// safe for concurrent use — the heartbeat stream is single-writer per
// worker, so the master holds one gate per session and touches it only from
// the stream goroutine.
type TelemetryGate struct {
	workerID string
	maxStale time.Duration
	lastSeq  uint64
}

// NewTelemetryGate returns a gate bound to one worker identity. maxStale <= 0
// selects DefaultTelemetrySnapshotMaxAge.
func NewTelemetryGate(workerID string, maxStale time.Duration) *TelemetryGate {
	if maxStale <= 0 {
		maxStale = DefaultTelemetrySnapshotMaxAge
	}
	return &TelemetryGate{workerID: workerID, maxStale: maxStale}
}

// Accept evaluates one snapshot against the gate contract. On acceptance it
// records the sequence for the next evaluation and returns
// TelemetryRejectNone; otherwise it returns the classifying reason WITHOUT
// advancing the sequence (a rejected snapshot never poisons the baseline).
func (g *TelemetryGate) Accept(snap WorkerTelemetrySnapshot, now time.Time) TelemetryRejectReason {
	if snap.WorkerID != g.workerID {
		return TelemetryRejectWorkerMismatch
	}
	if snap.SchemaVersion != TelemetrySnapshotSchemaVersion {
		return TelemetryRejectUnsupportedSchema
	}
	if snap.CapturedAt.IsZero() {
		return TelemetryRejectStale
	}
	age := now.Sub(snap.CapturedAt)
	if age < 0 {
		// Future-dated snapshot: tolerate small clock skew, reject the rest.
		if -age > g.maxStale {
			return TelemetryRejectStale
		}
	} else if age > g.maxStale {
		return TelemetryRejectStale
	}
	if g.lastSeq > 0 && snap.Sequence <= g.lastSeq {
		return TelemetryRejectOutOfSequence
	}
	g.lastSeq = snap.Sequence
	return TelemetryRejectNone
}
