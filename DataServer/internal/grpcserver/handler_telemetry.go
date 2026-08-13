// Package grpcserver / handler_telemetry.go
//
// Canonical typed worker telemetry admission. The worker publishes a
// WorkerTelemetrySnapshot (shared/controltransport/telemetry_snapshot.go)
// on every heartbeat; this file parses it out of the heartbeat Extra map,
// runs it through the per-session TelemetryGate (sequence monotonicity,
// staleness, worker identity, schema version) and — on acceptance —
// surfaces the typed fields as the state source for the metrics sink and
// the session.
//
// Free-form heartbeat metrics (extra["metrics"], extra["resources"]) remain
// readable for backward compatibility, but the typed snapshot is the
// canonical source for the fields it carries: cache bytes, active leases,
// download queue depth, render active, disk free and the release
// certificate.
package grpcserver

import (
	"time"

	"velox-server/internal/logging"
	velmetrics "velox-server/internal/metrics"
	"velox-shared/controltransport"
)

// ingestTelemetrySnapshot extracts the telemetry_snapshot block from the
// heartbeat Extra map, gates it against the worker's session and returns the
// accepted snapshot (nil when the block is absent or rejected).
//
// Rejections are logged with their classifying reason (out_of_sequence,
// stale, worker_mismatch, unsupported_schema) and NEVER surface to the
// metrics sink — a replayed/out-of-order snapshot must not overwrite newer
// state with older numbers.
//
// sess may be nil in lightweight handler tests; the gate then degrades to a
// per-call throwaway validator (sequence checks still run against the
// previous sequence seen in THIS call, i.e. effectively accepting the first
// snapshot). Production always passes the real session.
func ingestTelemetrySnapshot(workerID string, sess *workerSession, extra map[string]interface{}) *controltransport.WorkerTelemetrySnapshot {
	raw, ok := extra[controltransport.TelemetrySnapshotExtraKey]
	if !ok {
		return nil
	}
	block, ok := raw.(map[string]interface{})
	if !ok {
		return nil
	}
	snap, ok := controltransport.TelemetrySnapshotFromMap(block)
	if !ok {
		return nil
	}
	// Shape validation BEFORE the gate: an empty worker_id, zero sequence or
	// zero captured_at is malformed regardless of the gate's per-session
	// state (the gate re-checks these defensively, but Validate() gives the
	// reject reason precedence and keeps the gate contract tight).
	if err := snap.Validate(); err != nil {
		logGRPCf(ctxForTaskSession(sess), logging.LevelWarn, logging.CodeGRPCTelemetryRejected, "[GRPC] telemetry snapshot from worker %s invalid: %v", workerID, err)
		return nil
	}

	var reason controltransport.TelemetryRejectReason
	if sess != nil {
		reason = sess.acceptTelemetry(snap, time.Now().UTC())
	} else {
		g := controltransport.NewTelemetryGate(workerID, 0)
		reason = g.Accept(snap, time.Now().UTC())
	}
	if reason != controltransport.TelemetryRejectNone {
		logGRPCf(ctxForTaskSession(sess), logging.LevelWarn, logging.CodeGRPCTelemetryRejected, "[GRPC] telemetry snapshot from worker %s rejected (%s): seq=%d captured_at=%v schema=%d worker=%q", workerID, reason, snap.Sequence, snap.CapturedAt, snap.SchemaVersion, snap.WorkerID)
		return nil
	}
	return &snap
}

// applyTelemetryToResourceSnapshot overlays the accepted typed snapshot's
// fields onto the ResourceSnapshot the heartbeat already decoded from the
// typed proto counters. The typed snapshot is authoritative for the fields
// it carries (cache bytes today; leases/queue/render ride on the session via
// telemetry()); the proto counters stay authoritative for CPU/RAM/disk/net.
func applyTelemetryToResourceSnapshot(snap *velmetrics.ResourceSnapshot, telemetry *controltransport.WorkerTelemetrySnapshot) {
	if snap == nil || telemetry == nil {
		return
	}
	if telemetry.CacheBytes >= 0 {
		snap.CacheBytesUsed = telemetry.CacheBytes
	}
}
