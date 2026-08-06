package worker

// telemetry_snapshot.go — worker-side assembly of the canonical typed
// WorkerTelemetrySnapshot (shared/controltransport/telemetry_snapshot.go).
//
// The snapshot is the typed replacement for the free-form heartbeat metrics
// as the source of worker operational state: active leases come from the
// task-native lease map, download queue depth from the canonical download
// manager, cache counters from the persistent content-addressed cache,
// render active from the active-task map, disk free from the resource
// sampler, and the release certificate from release_identity.go. The master
// gates every snapshot (sequence / staleness / worker identity / schema)
// before accepting it.

import (
	"time"

	"velox-shared/controltransport"
)

// buildTelemetrySnapshot assembles one canonical WorkerTelemetrySnapshot for
// the current heartbeat. Sequence is monotonically increasing per process
// (rejected by the master when it regresses); CapturedAt is wall-clock UTC.
//
// Nil-safe by construction: an uninitialized assetManager, cache, sampler or
// release certificate degrades to the zero value for that field rather than
// aborting the heartbeat.
func (w *Worker) buildTelemetrySnapshot() controltransport.WorkerTelemetrySnapshot {
	snap := controltransport.WorkerTelemetrySnapshot{
		WorkerID:      w.config.WorkerID,
		Sequence:      w.telemetrySeq.Add(1),
		CapturedAt:    time.Now().UTC(),
		SchemaVersion: controltransport.TelemetrySnapshotSchemaVersion,
	}

	// Active leases: the task-native lease map is the worker-side mirror of
	// the master's lease store; the master derives authoritative admission
	// from its own store, but this counter is the honest worker-side view.
	w.activeTaskLeasesMu.RLock()
	snap.ActiveLeases = len(w.activeTaskLeases)
	w.activeTaskLeasesMu.RUnlock()

	// Download queue depth from the canonical AssetDownloadManager. The
	// manager caches its last operational projection (refreshOperational) so
	// this read is lock-free and always current without subscribing.
	if mgr := w.assetManager; mgr != nil {
		snap.DownloadQueue = mgr.LatestOperational().QueuedTransfers
	}

	// Cache accounting from the persistent content-addressed cache. Only the
	// cache wired via WithCache reports real counters; the noop fallback
	// stays zero.
	if c := w.cache; c != nil {
		stats := c.Stats()
		snap.CacheBytes = stats.BytesUsed
		if stats.Hits >= 0 {
			snap.CacheHitTotal = uint64(stats.Hits)
		}
		if stats.Misses >= 0 {
			snap.CacheMissTotal = uint64(stats.Misses)
		}
	}

	// Render active: the active-task map is the authoritative worker-side
	// in-flight render counter (one entry per dispatched task).
	w.activeTasksMu.RLock()
	snap.RenderActive = len(w.activeTasks)
	w.activeTasksMu.RUnlock()

	// Disk free bytes from the resource sampler's latest snapshot. The
	// sampler publishes on its own cadence; if it hasn't ticked yet the
	// field stays zero (same wire contract the master already tolerates).
	if s := w.sampler; s != nil {
		if latest := s.Latest(); latest != nil {
			snap.DiskFreeBytes = latest.DiskFreeBytes
		}
	}

	// Release certificate: the single typed source for version evidence.
	snap.SoftwareRelease = w.loadReleaseIdentity()

	return snap
}
