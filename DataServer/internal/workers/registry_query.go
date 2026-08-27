package workers

import (
	"context"
	"time"

	"velox-server/internal/logging"
	"velox-server/internal/store"
	"velox-shared/identity"
)

// ------------------------------------------------------------------
// Connection status (CONNECTED / STALE / DISCONNECTED / DRAINING)
// ------------------------------------------------------------------
//
// These thresholds are the single source of truth for the canonical
// state derivation surfaced by `/api/v1/workers/:worker_id` and the
// admin worker list endpoint. They MUST be the only values used by
// dashboards and the dispatcher.
//
// Note: the previously-paired handler-side `heartbeatStaleThreshold`
// const + `computeStatusLegacy` heartbeat-only fallback (formerly in
// DataServer/internal/handlers/server/api/workers_handler.go) have been
// removed. `sanitizeWorker` now trusts `Worker.ConnectionStatus`
// directly, and ConnectionStatus always returns one of the four enum
// strings on every read path, so no heartbeat-only fallback is needed.
//
// DRAINING overrides fresh-heartbeat semantics: a draining worker is
// still "alive" enough that operators should NOT see it bumped to
// DISCONNECTED purely on heartbeat age while it is gracefully
// finishing in-flight work.
const (
	StatusConnected    = "CONNECTED"
	StatusStale        = "STALE"
	StatusDisconnected = "DISCONNECTED"
	StatusDraining     = "DRAINING"

	// ConnectionStaleThreshold — heartbeat older than this demotes a
	// session-active worker from CONNECTED to STALE. Idle workers publish
	// every 60s (heartbeat_idle in RemoteCodex heartbeat_intervals.go), so
	// the read model must allow more than one idle interval plus normal
	// scheduling/network jitter. Compile-time alias of the canonical
	// store-side constant so the persist-side mirror and the read-time
	// derivation share one source of truth (DefaultStaleThreshold in
	// store/store_worker_heartbeat.go).
	ConnectionStaleThreshold = store.DefaultStaleThreshold

	// ConnectionDisconnectedThreshold — heartbeat older than this
	// bumps a worker to DISCONNECTED regardless of session state.
	// Matches the default `CleanupStaleWorkers` window so the read
	// model and the eviction loop agree on what "abandoned" means.
	// Compile-time alias of the canonical store-side constant
	// (DefaultPartitionThreshold).
	ConnectionDisconnectedThreshold = store.DefaultPartitionThreshold
)

// ConnectionStatus is the canonical state-derivation helper. Pure
// function — no I/O, no DB — so handlers, tests, and dashboards share
// the same logic. Callers supply `now` to keep the result deterministic
// in tests; production callers pass `time.Now().UTC()`.
//
// Rules (canonical, in evaluation order):
//  1. drain=true                                    → DRAINING
//  2. !sessionActive OR (now - lastHB) ≥ 5min OR   → DISCONNECTED
//     lastHB unparseable OR lastHB == ""
//  3. sessionActive AND (now - lastHB) ≥ 150s      → STALE
//  4. sessionActive AND (now - lastHB) < 150s      → CONNECTED
func ConnectionStatus(sessionActive bool, lastHB string, drain bool, now time.Time) string {
	// Projection of the canonical typed dimensions (worker_state.go):
	// the 4-state wire vocabulary is derived from ConnectionState +
	// SchedulingState, never from ad-hoc string checks.
	cs := DeriveConnectionState(sessionActive, lastHB, now)
	ss := DeriveSchedulingState(drain, false, false, 0)
	return cs.WireStatus(ss)
}

// GetWorker returns a single worker's info by ID, with SessionActive +
// ConnectionStatus hydrated from SQLite (worker_sessions) at read time.
// Returns nil if the worker is not registered or has been revoked.
func (r *Registry) GetWorker(ctx context.Context, workerID string) *Worker {
	workerID = identity.NormalizeWorkerID(workerID)
	r.mu.RLock()
	info, ok := r.inMem[identity.ParseWorkerID(workerID)]
	r.mu.RUnlock()
	if !ok {
		return nil
	}
	r.hydrate(ctx, &info, time.Now().UTC())
	return &info
}

// List returns every registered, non-revoked worker with SessionActive +
// ConnectionStatus populated. Bulk-fetches active session state via
// `dbStore.GetActiveSessionsByWorkerIDs` to avoid N+1 queries.
func (r *Registry) List(ctx context.Context) []Worker {
	ids, infos := r.snapshotRegistered(func(id string, w Worker) bool { return true })
	if len(infos) == 0 {
		return infos
	}
	r.hydrateBulk(ctx, ids, infos, time.Now().UTC())
	return infos
}

// StatusSnapshot returns (registered, live) where both lists have
// SessionActive + ConnectionStatus populated. Registered excludes
// revoked entries; live filters by heartbeat freshness plus session
// active.
func (r *Registry) StatusSnapshot(ctx context.Context, timeout time.Duration) (registered []Worker, live []Worker) {
	registered = r.List(ctx)
	live = r.GetActiveWorkers(ctx, timeout)
	return
}

// GetStaleWorkers returns registered workers that are not currently
// "live" (no recent heartbeat). ConnectionStatus is populated so
// dashboards can disambiguate "STALE" from outright DISCONNECTED.
func (r *Registry) GetStaleWorkers(ctx context.Context, timeout time.Duration) []Worker {
	registered := r.List(ctx)
	live := r.GetActiveWorkers(ctx, timeout)
	if len(registered) == 0 {
		return nil
	}
	liveSet := make(map[identity.WorkerID]struct{}, len(live))
	for _, w := range live {
		liveSet[w.WorkerID] = struct{}{}
	}
	out := make([]Worker, 0, len(registered))
	for _, w := range registered {
		if _, ok := liveSet[w.WorkerID]; ok {
			continue
		}
		out = append(out, w)
	}
	return out
}

// GetWorkersByGroup returns all workers in a specific group with
// SessionActive + ConnectionStatus hydrated.
func (r *Registry) GetWorkersByGroup(ctx context.Context, group string) []Worker {
	now := time.Now().UTC()
	r.mu.RLock()
	var result []Worker
	for _, w := range r.inMem {
		if w.WorkerGroup == group {
			result = append(result, w)
		}
	}
	r.mu.RUnlock()
	if len(result) == 0 {
		return result
	}
	ids := make([]string, len(result))
	for i, w := range result {
		ids[i] = w.WorkerID.String()
	}
	r.hydrateBulk(ctx, ids, result, now)
	return result
}

// HasAtLeastOneLive (RW-PROD-004 §3 A7) is the master-side readiness
// helper for the worker-side /health/ready migration. Returns true iff
// at least ONE registered worker is currently live within
// HasAtLeastOneLiveTimeout (150s). The semantics match GetActiveWorkers
// (last heartbeat within timeout + session active) but the consumer
// is the master's own /health/readiness subsystem, NOT the operator
// dashboards.
// Why a single tuple instead of a count:
//
//   - Dashboards already iterate GetActiveWorkers; a separate count
//     function would be a third code path to maintain against drift.
//   - Dashboards want per-worker ConnectionStatus (CONNECTED / STALE /
//     DRAINING / DISCONNECTED enum); this helper collapses to a bool so
//     the read-model semantics are not conflated with the
//     readiness-pane semantics.
//   - The master-side readiness pane only ever asks "is the fleet
//     non-empty AND live" — a yes/no answer is the canonical gate
//     (operators run on a one-shift boundary; a stuttering dashboard
//     is worse than a hard fail-closed gate).
//
// The flag VELOX_REQUIRE_LIVE_WORKERS (A8) is the operator opt-in
// that enables this gate at server boot — the helper is unconditionally
// safe to call (returns false when nothing live) but the readiness
// check is OPT-IN to keep production deployments that occasionally
// run with zero live workers (e.g. a 6 AM scheduled drain window)
// from spuriously reporting not_ready.
func (r *Registry) HasAtLeastOneLive(ctx context.Context) bool {
	if r == nil {
		return false
	}
	return len(r.GetActiveWorkers(ctx, HasAtLeastOneLiveTimeout)) >= 1
}

// HasAtLeastOneLiveTimeout is the canonical freshness window for the
// master-side readiness gate. Matches ConnectionStaleThreshold (150s)
// so a fresh heartbeat keeps the gate satisfied while a stale one
// drops the gate in lockstep with operator dashboards.
const HasAtLeastOneLiveTimeout = ConnectionStaleThreshold

// GetActiveWorkers returns workers that have a recent heartbeat AND a
// live session. ConnectionStatus is populated; downstream consumers
// may filter further on the enum.
func (r *Registry) GetActiveWorkers(ctx context.Context, timeout time.Duration) []Worker {
	now := time.Now().UTC()
	r.mu.RLock()
	var result []Worker
	for _, w := range r.inMem {
		if r.revoked[w.WorkerID] {
			continue
		}
		if w.LastHB == "" {
			continue
		}
		t, err := time.Parse(time.RFC3339, w.LastHB)
		if err != nil || now.Sub(t.UTC()) >= timeout {
			continue
		}
		result = append(result, w)
	}
	r.mu.RUnlock()
	if len(result) == 0 {
		return result
	}
	ids := make([]string, len(result))
	for i, w := range result {
		ids[i] = w.WorkerID.String()
	}
	r.hydrateBulk(ctx, ids, result, now)
	return result
}

// ------------------------------------------------------------------
// Hydrate plumbing (private)
// ------------------------------------------------------------------
//
// The cost-aware eligibility methods (GetSchedulableWorkers /
// GetEligibleWorkers) and the stale eviction loop
// (CleanupStaleWorkers) live in registry_eligibility.go.

// snapshotRegistered iterates the in-memory map under RLock, skipping
// revoked entries, and returns parallel slices of (workerID, info)
// for callers that bulk-hydrate. Keeps the locking pattern tight: one
// RLock acquisition for the snapshot, then drops the lock before any
// per-worker DB round-trip.
func (r *Registry) snapshotRegistered(keep func(workerID string, w Worker) bool) ([]string, []Worker) {
	r.mu.RLock()
	ids := make([]string, 0, len(r.inMem))
	infos := make([]Worker, 0, len(r.inMem))
	for id, w := range r.inMem {
		if r.revoked[id] {
			continue
		}
		if !keep(id.String(), w) {
			continue
		}
		ids = append(ids, id.String())
		infos = append(infos, w)
	}
	r.mu.RUnlock()
	return ids, infos
}

// hydrateBulk fetches the active-session set for the given workerIDs in
// ONE DB query (when dbStore is wired), then mutates each info in place
// with SessionActive + ConnectionStatus derived from canonical helpers.
func (r *Registry) hydrateBulk(ctx context.Context, ids []string, infos []Worker, now time.Time) {
	if len(ids) == 0 {
		return
	}
	if r.dbStore == nil {
		for i := range infos {
			ConnectionStatusForInfo(&infos[i], false, now)
			infos[i].Capacity = deriveWorkerCapacityWithAuthority(infos[i].DeclaredMaxSlots, 0, false)
		}
		return
	}
	sessionMap, err := r.dbStore.GetActiveSessionsByWorkerIDs(ids)
	if err != nil {
		// Be conservative on DB error: treat the entire batch as
		// DISCONNECTED callers can detect via ConnectionStatus field.
		// We log once per call rather than per-worker (the str matches
		// pre-read-model behavior — the read model never blocked the
		// caller on session state).
		registryLog.WarnWithMsg(logging.CodeRegistryLoadSessionsQueryFail,
			"Workers session query failed; demoting fleet to conservative (DISCONNECTED) state",
			map[string]interface{}{"err": err.Error(), "count": len(ids)})
		sessionMap = map[string]bool{}
	}
	capacityMap, capacityErr := r.dbStore.GetWorkerCapacities(ctx, ids, now.Format(time.RFC3339))
	if capacityErr != nil {
		registryLog.WarnWithMsg(logging.CodeRegistryLoadSessionsQueryFail,
			"Workers lease capacity query failed; marking capacity non-authoritative",
			map[string]interface{}{"err": capacityErr.Error(), "count": len(ids)})
		capacityMap = map[string]store.WorkerCapacityRow{}
	}
	// Load persisted scorecards so per-phase slot limits are available
	// even when the worker has no active heartbeat session.
	scorecardMap := map[string]*store.ScorecardRow{}
	if scErr := (func() error {
		sc, scErr := r.dbStore.GetScorecardsBulk(ctx, ids)
		if scErr != nil {
			return scErr
		}
		scorecardMap = sc
		return nil
	})(); scErr != nil {
		registryLog.WarnWithMsg(logging.CodeRegistryLoadSessionsQueryFail,
			"Workers scorecard query failed; per-phase slots will be empty",
			map[string]interface{}{"err": scErr.Error(), "count": len(ids)})
	}
	for i := range infos {
		active := sessionMap[infos[i].WorkerID.String()]
		ConnectionStatusForInfo(&infos[i], active, now)
		row := capacityMap[infos[i].WorkerID.String()]
		infos[i].Capacity = deriveWorkerCapacityWithAuthority(
			infos[i].DeclaredMaxSlots,
			row.ActiveSlots,
			capacityErr == nil,
		)
		// Overlay persisted scorecard per-phase slot limits.
		if sc := scorecardMap[infos[i].WorkerID.String()]; sc != nil {
			infos[i].Capacity.RenderSlots = sc.RenderSlots
			infos[i].Capacity.PrefetchSlots = sc.PrefetchSlots
			infos[i].Capacity.PublisherSlots = sc.PublisherSlots
			infos[i].Capacity.LimitingResource = sc.LimitingResource
		}
		if capacityErr == nil {
			applyLeaseCapacityState(&infos[i], row.ActiveSlots, now)
		} else {
			applyCapacityUnavailableState(&infos[i], now)
		}
	}
}

// hydrate updates a SINGLE Worker with SessionActive +
// ConnectionStatus. Used by GetWorker (which avoids the bulk query to
// keep the per-worker path cheap).
func (r *Registry) hydrate(ctx context.Context, info *Worker, now time.Time) {
	if info == nil {
		return
	}
	if r.dbStore == nil {
		ConnectionStatusForInfo(info, false, now)
		info.Capacity = deriveWorkerCapacityWithAuthority(info.DeclaredMaxSlots, 0, false)
		return
	}
	active, err := r.dbStore.IsSessionActive(info.WorkerID.String())
	if err != nil {
		registryLog.WarnWithMsg(logging.CodeRegistryLoadSessionQueryFail,
			"worker session query failed; treating worker as DISCONNECTED",
			map[string]interface{}{"worker_id": info.WorkerID.String(), "err": err.Error()})
		active = false
	}
	ConnectionStatusForInfo(info, active, now)
	if r.dbStore != nil {
		row, capErr := r.dbStore.GetWorkerCapacity(ctx, info.WorkerID.String(), now.Format(time.RFC3339))
		if capErr != nil {
			registryLog.WarnWithMsg(logging.CodeRegistryLoadSessionsQueryFail,
				"Worker lease capacity query failed; marking capacity non-authoritative",
				map[string]interface{}{"worker_id": info.WorkerID.String(), "err": capErr.Error()})
		}
		info.Capacity = deriveWorkerCapacityWithAuthority(
			info.DeclaredMaxSlots,
			row.ActiveSlots,
			capErr == nil,
		)
		if capErr == nil {
			applyLeaseCapacityState(info, row.ActiveSlots, now)
		} else {
			applyCapacityUnavailableState(info, now)
		}
	}
}

// Reason constants for non-CONNECTED states (RW-PROD-005 A2).
// Consumed by ConnectionReason() and exposed in Worker.Reason.
const (
	ReasonDrain           = "drain"
	ReasonDetachedSession = "detached_session"
	ReasonHeartbeatStale  = "heartbeat_stale"
)

// ConnectionReason maps the canonical state-derivation inputs to the
// 3-element Reason taxonomy. Pure function — no I/O. Callers supply
// (sessionActive, drain, lastHB, now) so the mapping is testable
// without DB plumbing.
//
// Precedence (RW-PROD-005 A2):
//  1. drain=true                                   → "drain"
//  2. session_active == false                      → "detached_session"
//  3. lastHB empty/unparseable OR                  → "heartbeat_stale"
//     now - lastHB >= ConnectionStaleThreshold
//  4. fresh (connected)                            → ""
func ConnectionReason(sessionActive bool, drain bool, lastHB string, now time.Time) string {
	if drain {
		return ReasonDrain
	}
	if !sessionActive {
		return ReasonDetachedSession
	}
	if lastHB == "" {
		return ReasonHeartbeatStale
	}
	t, err := time.Parse(time.RFC3339, lastHB)
	if err != nil {
		return ReasonHeartbeatStale
	}
	if now.Sub(t.UTC()) >= ConnectionStaleThreshold {
		return ReasonHeartbeatStale
	}
	return ""
}

// ConnectionStatusForInfo mutates `info` to set SessionActive,
// ConnectionStatus, and Reason from the supplied session_active signal.
// Pure logic — no DB calls — so tests can drive it directly.
// ConnectionStatusForInfo is the compatibility-boundary adapter for the
// legacy connection_status/reason fields. It also hydrates the canonical
// independent state dimensions so every read path observes the same tuple.
// New consumers must read ConnectionState, SchedulingState, DeploymentState,
// and HealthState rather than reconstructing state from legacy strings.
// applyCapacityUnavailableState makes a lease-store outage visible in the
// canonical projection. Admission is already fail-closed; the read model
// must not claim the worker is healthy and available at the same time.
func applyCapacityUnavailableState(info *Worker, now time.Time) {
	if info == nil {
		return
	}
	// Preserve the operator-owned scheduling dimension (drain/quarantine)
	// rather than fabricating DRAINING on a storage outage. Admission is
	// independently fail-closed by Capacity.Authoritative in eligibility.
	info.HealthState = HealthDegraded
	info.Health = WorkerHealthDegraded
}

// applyLeaseCapacityState refreshes the canonical scheduling and health
// projections after the authoritative lease count has been hydrated.
// Heartbeat metrics are intentionally absent from this path.
func applyLeaseCapacityState(info *Worker, activeSlots int, now time.Time) {
	if info == nil {
		return
	}
	info.SchedulingState = DeriveSchedulingState(info.Drain, info.Quarantined, info.Resuming, activeSlots)
	info.HealthState = DeriveHealthState(info.ConnectionState, info.SchedulingState, info.DeploymentState, time.Time{}, now)
	info.Health = projectHealth9(info.ConnectionState, info.SchedulingState, info.DeploymentState, time.Time{}, now)
}

func ConnectionStatusForInfo(info *Worker, sessionActive bool, now time.Time) {
	if info == nil {
		return
	}
	info.SessionActive = sessionActive
	info.ConnectionState = DeriveConnectionState(sessionActive, info.LastHB, now)
	// Occupancy is owned by the lease-store capacity projection. The
	// compatibility adapter does not infer scheduling state from heartbeat
	// metrics, because those counters are diagnostic telemetry only.
	info.SchedulingState = DeriveSchedulingState(info.Drain, info.Quarantined, info.Resuming, 0)
	info.HealthState = DeriveHealthState(
		info.ConnectionState,
		info.SchedulingState,
		info.DeploymentState,
		time.Time{},
		now,
	)

	// Controlled compatibility projection. These legacy fields are output
	// only; no state decision may be made from their previous values.
	info.ConnectionStatus = info.ConnectionState.WireStatus(info.SchedulingState)
	info.Reason = ConnectionReason(sessionActive, info.Drain, info.LastHB, now)
	info.Health = projectHealth9(
		info.ConnectionState,
		info.SchedulingState,
		info.DeploymentState,
		time.Time{},
		now,
	)
}
