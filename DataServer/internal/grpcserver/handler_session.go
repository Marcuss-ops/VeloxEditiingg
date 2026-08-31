// Package grpcserver / handler_session.go
//
// workerSession and outboundMessage types plus helpers for placement,
// capabilities and executor tracking. Extracted from handler.go to keep
// the core types file focused.
package grpcserver

import (
	"context"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"velox-server/internal/placement"
	"velox-server/internal/taskgraph"
	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"

	"google.golang.org/grpc"
)

// outboundMessage wraps a protobuf envelope with optional callbacks
// for the sessionWriter. OnSent is called after a successful stream.Send;
// nil means no callback. This enables #1 fix: commands are marked delivered
// only after the real network write, not after safeSend puts them in the
// in-memory channel.
type outboundMessage struct {
	Envelope *pb.MasterToWorkerEnvelope
	OnSent   func() // Called after successful stream.Send; nil if not needed
}

// workerSession tracks a single worker's gRPC stream connection.
type workerSession struct {
	workerID         string
	sessionID        string
	workerSnapshotID string
	stream           grpc.BidiStreamingServer[pb.WorkerToMasterEnvelope, pb.MasterToWorkerEnvelope]
	done             chan struct{}
	doneOnce         sync.Once          // P0 #6: prevents double-close on session teardown/reconnect
	cancel           context.CancelFunc // cancels the session context to terminate old goroutines

	// gRPC request context (carries trace context via otelgrpc).
	// Scorecard v2 / Step 15c: handlers use this instead of context.Background()
	// so spans have proper parent-child trace relationships.
	ctx context.Context

	// Serialized output: all stream.Send() calls go through sendCh → sessionWriter.
	// No other goroutine may call stream.Send() directly.
	sendCh chan *outboundMessage

	// writerErr is a small (cap 1) channel used by sessionWriter to signal
	// a stream.Send() failure back to the Stream() main loop. Phase 4.2
	// requirement: a network-level send error MUST terminate the session,
	// otherwise pending offers can be left orphaned silently. The main loop
	// reads writerErr inside its select and triggers a teardown on receipt.
	writerErr chan error // Job offering synchronization (Issue 4 fix).
	// placementNotify is the coalescing wake-up channel for push placement.
	// It is installed when the session starts and is deliberately never closed;
	// session teardown cancels the notifier context instead.
	placementNotify chan struct{}
	// PR #4: replaced pendingOffer (job-based) with pendingTaskOffer (task-based).
	pendingTaskOffer   *taskgraph.TaskWithSpec // TaskOffer sent, awaiting TaskAccepted/TaskRejected
	pendingTaskOfferAt time.Time               // when the offer was put on the wire
	claimMu            sync.Mutex              // serializes the claim+send+set flow; also guards pendingTaskOffer r/w

	// Worker capacity limit from the canonical capability report. Current
	// occupancy is never stored on the session: placement reads active leases
	// from the master task store before admission.
	maxParallelJobs      atomic.Int32
	activeExecutionSlots atomic.Int32

	// Sequence numbers for replay protection (Issue 7 fix).
	lastRecvSeq int64 // last received sequence number from worker

	// Placement snapshot fields: typed executor map, capability map,
	// and their revision counter. Populated at Hello time and updated
	// on heartbeat-driven re-advertisement. The placement snapshot is
	// built from these fields under RLock so the snapshot is always
	// consistent without blocking the main message loop.
	// placementMu makes the executor registry and named capability set one
	// generation. The individual locks remain for narrow legacy readers, but
	// placement snapshots must never observe one field before the other.
	placementMu sync.RWMutex
	executorsMu sync.RWMutex
	executors   controltransport.ExecutorRegistry

	capabilitiesMu   sync.RWMutex
	capabilities     controltransport.CapabilitySet
	assetCacheKeysMu sync.RWMutex
	assetCacheKeys   map[string]struct{}

	capabilityRevision atomic.Uint64

	ready    atomic.Bool
	draining atomic.Bool

	lastHeartbeatUnix atomic.Int64

	// Warm-placement resource facts. They are updated from the canonical
	// capability/telemetry snapshots and remain hints until the shared
	// selector applies its fail-closed admission gates.
	capacityAuthoritative atomic.Bool
	diskAuthoritative     atomic.Bool
	freeDiskBytes         atomic.Uint64
	estimatedAvailableMS  atomic.Int64
	networkMbpsBits       atomic.Uint64
	loadRatioBits         atomic.Uint64

	// Per-phase slot limits from the CapacityScorecard. When non-zero the
	// placement matcher uses these instead of the flat maxParallelJobs.
	renderSlots    atomic.Int32
	prefetchSlots  atomic.Int32
	publisherSlots atomic.Int32
	// Per-phase active counts from the lease store (phase-aware hydration).
	activeRender                atomic.Int32
	activePrefetch              atomic.Int32
	activePublisher             atomic.Int32
	phaseOccupancyAuthoritative atomic.Bool
	// capacityCooldownUntilUnixNano suppresses re-offers after the worker
	// explicitly reports capacity_full.
	capacityCooldownUntilUnixNano atomic.Int64

	// Version correlation (Step 4 / Velox Metrics Center): software
	// versions reported by the worker via heartbeat, stored on the
	// session so they can be stamped on task_attempts at report time.
	gitSHA        atomic.Value // string
	workerVersion atomic.Value // string
	engineVersion atomic.Value // string
	ffmpegVersion atomic.Value // string

	// Canonical typed telemetry snapshot admission (telemetry_snapshot.go).
	// The gate enforces sequence monotonicity + staleness + worker identity
	// + schema per session: a reconnecting worker mints a fresh session and
	// therefore a fresh baseline, matching the worker's per-process sequence
	// counter. lastTelemetry holds the most recent ACCEPTED snapshot for
	// admin-API / placement projections.
	telemetryMu   sync.Mutex
	telemetryGate *controltransport.TelemetryGate
	lastTelemetry atomic.Value // *controltransport.WorkerTelemetrySnapshot
}

// placementSnapshot builds an immutable WorkerSnapshot from the in-memory
// session state. The snapshot is consistent at a single instant (executors
// and capabilities read under their respective RLock). The caller must
// NOT hold any session mutex when calling this method.
func (s *workerSession) placementSnapshot(workerID string) placement.WorkerSnapshot {
	s.placementMu.RLock()
	defer s.placementMu.RUnlock()

	s.executorsMu.RLock()
	executorRegistry := s.executors
	s.executorsMu.RUnlock()

	s.capabilitiesMu.RLock()
	caps := append(controltransport.CapabilitySet(nil), s.capabilities...)
	for i := range caps {
		caps[i] = strings.TrimSpace(caps[i])
	}
	s.capabilitiesMu.RUnlock()
	s.assetCacheKeysMu.RLock()
	assetKeys := make(map[string]struct{}, len(s.assetCacheKeys))
	for key := range s.assetCacheKeys {
		assetKeys[key] = struct{}{}
	}
	s.assetCacheKeysMu.RUnlock()

	return placement.WorkerSnapshot{
		WorkerID:        workerID,
		SessionID:       s.sessionID,
		Ready:           s.ready.Load(),
		Draining:        s.draining.Load(),
		SessionAlive:    true,
		MaxParallelJobs: int(s.maxParallelJobs.Load()),
		// The normal dispatch path replaces ActiveJobs with the authoritative
		// lease-store projection before Matcher.Select. Warm placement uses
		// the latest accepted worker telemetry as its availability estimate.
		ActiveJobs: int(s.activeExecutionSlots.Load()),
		// Per-phase slots from the CapacityScorecard. When non-zero the
		// matcher uses these instead of the flat MaxParallelJobs limit.
		RenderSlots:           int(s.renderSlots.Load()),
		PrefetchSlots:         int(s.prefetchSlots.Load()),
		PublisherSlots:        int(s.publisherSlots.Load()),
		ActiveRender:          int(s.activeRender.Load()),
		ActivePrefetch:        int(s.activePrefetch.Load()),
		ActivePublisher:       int(s.activePublisher.Load()),
		CapacityAuthoritative: s.capacityAuthoritative.Load(),
		DiskAuthoritative:     s.diskAuthoritative.Load(),
		FreeDiskBytes:         s.freeDiskBytes.Load(),
		EstimatedAvailableMS:  s.estimatedAvailableMS.Load(),
		NetworkMbps:           math.Float64frombits(s.networkMbpsBits.Load()),
		LoadRatio:             math.Float64frombits(s.loadRatioBits.Load()),
		ExecutorRegistry:      executorRegistry,
		Capabilities:          caps,
		CachedAssetKeys:       assetKeys,
		CapabilityRevision:    s.capabilityRevision.Load(),
		LastHeartbeat: time.Unix(
			s.lastHeartbeatUnix.Load(),
			0,
		).UTC(),
	}
}

// signalPlacement wakes the session's push placer after a lifecycle change
// may have made a READY task dispatchable. The wake-up is coalesced and does
// not perform placement inline, preserving the single claimMu-serialized
// check/select/claim/send path.
func (s *workerSession) signalPlacement() {
	if s == nil {
		return
	}
	signalTaskOffers(s.placementNotify)
}

func (s *workerSession) setActiveExecutionSlots(value int) {
	if value < 0 {
		value = 0
	}
	s.activeExecutionSlots.Store(int32(value))
}

func (s *workerSession) setCapacityCooldown(until time.Time) {
	if s == nil {
		return
	}
	s.capacityCooldownUntilUnixNano.Store(until.UnixNano())
}

func (s *workerSession) capacityCooldownActive(now time.Time) bool {
	if s == nil {
		return false
	}
	return s.capacityCooldownUntilUnixNano.Load() > now.UnixNano()
}

func (s *workerSession) setCapacityAuthoritative(value bool) {
	s.capacityAuthoritative.Store(value)
}

func (s *workerSession) updatePlacementResources(freeDiskBytes int64, diskAuthoritative bool, estimatedAvailableMS int64, networkMbps, loadRatio float64) {
	if freeDiskBytes < 0 {
		freeDiskBytes = 0
	}
	if estimatedAvailableMS < 0 {
		estimatedAvailableMS = 0
	}
	if math.IsNaN(networkMbps) || networkMbps < 0 {
		networkMbps = 0
	}
	if math.IsNaN(loadRatio) || loadRatio < 0 {
		loadRatio = 0
	}
	if loadRatio > 1 {
		loadRatio = 1
	}
	s.diskAuthoritative.Store(diskAuthoritative)
	s.freeDiskBytes.Store(uint64(freeDiskBytes))
	s.estimatedAvailableMS.Store(estimatedAvailableMS)
	s.networkMbpsBits.Store(math.Float64bits(networkMbps))
	s.loadRatioBits.Store(math.Float64bits(loadRatio))
}

// setPerPhaseSlots updates the per-phase slot limits from the CapacityScorecard.
// Zero values mean "not computed" — the matcher falls back to flat FreeSlots.
func (s *workerSession) setPerPhaseSlots(renderSlots, prefetchSlots, publisherSlots int) {
	s.renderSlots.Store(int32(renderSlots))
	s.prefetchSlots.Store(int32(prefetchSlots))
	s.publisherSlots.Store(int32(publisherSlots))
}

// setActivePhaseCounts updates the per-phase active counts from the lease store.
func (s *workerSession) setActivePhaseCounts(activeRender, activePrefetch, activePublisher int) {
	s.activeRender.Store(int32(activeRender))
	s.activePrefetch.Store(int32(activePrefetch))
	s.activePublisher.Store(int32(activePublisher))
	s.phaseOccupancyAuthoritative.Store(true)
}

func (s *workerSession) replaceAssetCacheKeys(keys []string) {
	set := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		if strings.TrimSpace(key) != "" {
			set[strings.TrimSpace(key)] = struct{}{}
		}
	}
	s.assetCacheKeysMu.Lock()
	s.assetCacheKeys = set
	s.assetCacheKeysMu.Unlock()
}

// replaceCapabilities atomically replaces the session's executor and
// capability maps with the parsed values from the Hello handshake.
// It bumps the capability revision so any pending claim that was
// built from a stale snapshot can be detected by the fencing check.
func (s *workerSession) replaceCapabilities(
	executors controltransport.ExecutorRegistry,
	capabilities controltransport.CapabilitySet,
) {
	s.placementMu.Lock()
	defer s.placementMu.Unlock()

	s.executorsMu.Lock()
	s.executors = executors
	s.executorsMu.Unlock()
	s.capabilitiesMu.Lock()
	s.capabilities = capabilities
	s.capabilitiesMu.Unlock()
	s.capabilityRevision.Add(1)
}

func (s *workerSession) replaceExecutorRegistry(executors controltransport.ExecutorRegistry) {
	s.placementMu.Lock()
	defer s.placementMu.Unlock()

	s.executorsMu.Lock()
	s.executors = executors
	s.executorsMu.Unlock()
	s.capabilityRevision.Add(1)
}

// acceptTelemetry runs one WorkerTelemetrySnapshot through the per-session
// admission gate. Returns the reject reason (TelemetryRejectNone == accepted
// AND recorded as the session's last accepted snapshot). The gate is created
// lazily on first use, bound to the session's worker identity; the session is
// single-writer (its stream goroutine), so no cross-goroutine locking is
// needed beyond the lazily-created gate itself.
func (s *workerSession) acceptTelemetry(snap controltransport.WorkerTelemetrySnapshot, now time.Time) controltransport.TelemetryRejectReason {
	s.telemetryMu.Lock()
	defer s.telemetryMu.Unlock()
	if s.telemetryGate == nil {
		s.telemetryGate = controltransport.NewTelemetryGate(s.workerID, 0)
	}
	reason := s.telemetryGate.Accept(snap, now)
	if reason == controltransport.TelemetryRejectNone {
		copy := snap
		s.lastTelemetry.Store(&copy)
	}
	return reason
}

// telemetry returns the last ACCEPTED telemetry snapshot, or nil when no
// snapshot has been admitted on this session yet. Callers must treat the
// returned pointer as immutable.
func (s *workerSession) telemetry() *controltransport.WorkerTelemetrySnapshot {
	v := s.lastTelemetry.Load()
	if v == nil {
		return nil
	}
	return v.(*controltransport.WorkerTelemetrySnapshot)
}

// maxParallelJobsFromCapabilities reads the worker's capacity from the
// SINGLE canonical wire representation: capabilities.host.max_parallel_jobs
// (worker CapabilityReport.AsMap). The legacy top-level
// capabilities.max_parallel_jobs mirror was removed together with its
// reader: the protocol-version gate (controltransport.IsSupportedProtocol)
// admits only v3 workers, and current workers always publish the host
// block. A top-level-only legacy shape now reads as 0 on purpose.
func maxParallelJobsFromCapabilities(capsMap map[string]interface{}) int {
	if capsMap == nil {
		return 0
	}
	host, ok := capsMap["host"].(map[string]interface{})
	if !ok {
		return 0
	}
	mpj, ok := host["max_parallel_jobs"]
	if !ok {
		return 0
	}
	switch v := mpj.(type) {
	case float64:
		return int(v)
	case int:
		return v
	case int32:
		return int(v)
	case int64:
		return int(v)
	}
	return 0
}

// invalidateExecutor removes a single executor key from the session's
// executor map and bumps the capability revision. Called when the
// worker rejects a task with reason="unsupported_executor" — the
// placement snapshot said the worker supports this executor, but the
// worker disagrees. Invalidating prevents further offers of the same
// incompatible executor until the next Hello re-advertises it.
func (s *workerSession) invalidateExecutor(key placement.ExecutorKey) {
	s.placementMu.Lock()
	defer s.placementMu.Unlock()

	s.executorsMu.Lock()
	s.executors = s.executors.Without(key.ID, key.Version)
	s.executorsMu.Unlock()

	s.capabilityRevision.Add(1)
}
