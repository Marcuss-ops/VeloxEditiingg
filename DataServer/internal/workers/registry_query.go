package workers

import (
	"context"
	"time"

	"velox-server/internal/costmodel"
	"velox-server/internal/logging"
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
// removed. `sanitizeWorker` now trusts `WorkerInfo.ConnectionStatus`
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
	// every 60s, so the read model must allow more than one idle interval
	// plus normal scheduling/network jitter. 150s is 2.5x the idle period.
	ConnectionStaleThreshold = 150 * time.Second

	// ConnectionDisconnectedThreshold — heartbeat older than this
	// bumps a worker to DISCONNECTED regardless of session state.
	// Matches the default `CleanupStaleWorkers` window so the read
	// model and the eviction loop agree on what "abandoned" means.
	ConnectionDisconnectedThreshold = 5 * time.Minute
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
	if drain {
		return StatusDraining
	}
	if !sessionActive {
		return StatusDisconnected
	}
	if lastHB == "" {
		return StatusDisconnected
	}
	t, err := time.Parse(time.RFC3339, lastHB)
	if err != nil {
		return StatusDisconnected
	}
	age := now.Sub(t.UTC())
	switch {
	case age >= ConnectionDisconnectedThreshold:
		return StatusDisconnected
	case age >= ConnectionStaleThreshold:
		return StatusStale
	default:
		return StatusConnected
	}
}

func (r *Registry) IsRegistered(ctx context.Context, workerID string) bool {
	workerID = identity.NormalizeWorkerID(workerID)
	r.mu.RLock()
	_, ok := r.inMem[workerID]
	r.mu.RUnlock()
	return ok
}

// GetWorker returns a single worker's info by ID, with SessionActive +
// ConnectionStatus hydrated from SQLite (worker_sessions) at read time.
// Returns nil if the worker is not registered or has been revoked.
func (r *Registry) GetWorker(ctx context.Context, workerID string) *WorkerInfo {
	workerID = identity.NormalizeWorkerID(workerID)
	r.mu.RLock()
	info, ok := r.inMem[workerID]
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
func (r *Registry) List(ctx context.Context) []WorkerInfo {
	ids, infos := r.snapshotRegistered(func(id string, w WorkerInfo) bool { return true })
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
func (r *Registry) StatusSnapshot(ctx context.Context, timeout time.Duration) (registered []WorkerInfo, live []WorkerInfo) {
	registered = r.List(ctx)
	live = r.GetActiveWorkers(ctx, timeout)
	return
}

// GetStaleWorkers returns registered workers that are not currently
// "live" (no recent heartbeat). ConnectionStatus is populated so
// dashboards can disambiguate "STALE" from outright DISCONNECTED.
func (r *Registry) GetStaleWorkers(ctx context.Context, timeout time.Duration) []WorkerInfo {
	registered := r.List(ctx)
	live := r.GetActiveWorkers(ctx, timeout)
	if len(registered) == 0 {
		return nil
	}
	liveSet := make(map[string]struct{}, len(live))
	for _, w := range live {
		liveSet[w.WorkerID] = struct{}{}
	}
	out := make([]WorkerInfo, 0, len(registered))
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
func (r *Registry) GetWorkersByGroup(ctx context.Context, group string) []WorkerInfo {
	now := time.Now().UTC()
	r.mu.RLock()
	var result []WorkerInfo
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
		ids[i] = w.WorkerID
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
func (r *Registry) GetActiveWorkers(ctx context.Context, timeout time.Duration) []WorkerInfo {
	now := time.Now().UTC()
	r.mu.RLock()
	var result []WorkerInfo
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
		ids[i] = w.WorkerID
	}
	r.hydrateBulk(ctx, ids, result, now)
	return result
}

// GetSchedulableWorkers returns workers that can accept new jobs.
// It routes through GetEligibleWorkers with default permissive
// requirements so dispatcher callers use the canonical costmodel path.
func (r *Registry) GetSchedulableWorkers(ctx context.Context) []WorkerInfo {
	return r.GetEligibleWorkers(ctx, costmodel.DefaultRequirements())
}

// GetEligibleWorkers is the canonical cost-aware eligibility entry point.
// It builds a WorkerProfile from each registered worker and accepts only
// profiles that costmodel.Score marks eligible. Drain, offline and capacity
// exclusions live in the costmodel path, not in ad-hoc registry filters.
func (r *Registry) GetEligibleWorkers(ctx context.Context, req costmodel.JobRequirements) []WorkerInfo {
	now := time.Now().UTC()
	r.mu.RLock()
	var result []WorkerInfo
	for _, w := range r.inMem {
		if r.revoked[w.WorkerID] {
			continue
		}
		profile := costmodel.BuildWorkerProfile(
			w.WorkerID,
			w.Schedulable,
			w.Drain,
			w.Status,
			0, 0,
			w.Capabilities,
		)
		c, _ := costmodel.Score(profile, req)
		if !c.Eligible {
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
		ids[i] = w.WorkerID
	}
	r.hydrateBulk(ctx, ids, result, now)
	return result
}

// CleanupStaleWorkers removes workers that haven't sent a heartbeat in the given duration
func (r *Registry) CleanupStaleWorkers(ctx context.Context, maxAge time.Duration) int {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now().UTC()
	count := 0

	for id, w := range r.inMem {
		if w.LastHB != "" {
			t, err := time.Parse(time.RFC3339, w.LastHB)
			if err == nil && now.Sub(t.UTC()) > maxAge {
				delete(r.inMem, id)
				if r.dbStore != nil {
					if err := r.dbStore.DeleteWorker(id); err != nil {
						registryLog.ErrorWithMsg(logging.CodeRegistryDeleteStaleWorkerFail,
							"Failed to delete stale worker",
							map[string]interface{}{"worker_id": id, "err": err.Error()})
					}
				}
				count++
				registryLog.InfoWithMsg(logging.CodeRegistryStaleWorkerCleanup,
					"Cleaned up stale worker",
					map[string]interface{}{"worker_id": id, "last_seen": w.LastHB})
			}
		}
	}

	// No need for bulk save — each deletion already hits SQLite
	return count
}

// ------------------------------------------------------------------
// Hydrate plumbing (private)
// ------------------------------------------------------------------

// snapshotRegistered iterates the in-memory map under RLock, skipping
// revoked entries, and returns parallel slices of (workerID, info)
// for callers that bulk-hydrate. Keeps the locking pattern tight: one
// RLock acquisition for the snapshot, then drops the lock before any
// per-worker DB round-trip.
func (r *Registry) snapshotRegistered(keep func(workerID string, w WorkerInfo) bool) ([]string, []WorkerInfo) {
	r.mu.RLock()
	ids := make([]string, 0, len(r.inMem))
	infos := make([]WorkerInfo, 0, len(r.inMem))
	for id, w := range r.inMem {
		if r.revoked[id] {
			continue
		}
		if !keep(id, w) {
			continue
		}
		ids = append(ids, id)
		infos = append(infos, w)
	}
	r.mu.RUnlock()
	return ids, infos
}

// hydrateBulk fetches the active-session set for the given workerIDs in
// ONE DB query (when dbStore is wired), then mutates each info in place
// with SessionActive + ConnectionStatus derived from canonical helpers.
func (r *Registry) hydrateBulk(ctx context.Context, ids []string, infos []WorkerInfo, now time.Time) {
	if len(ids) == 0 {
		return
	}
	if r.dbStore == nil {
		for i := range infos {
			ConnectionStatusForInfo(&infos[i], false, now)
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
	for i := range infos {
		active := sessionMap[infos[i].WorkerID]
		ConnectionStatusForInfo(&infos[i], active, now)
	}
}

// hydrate updates a SINGLE WorkerInfo with SessionActive +
// ConnectionStatus. Used by GetWorker (which avoids the bulk query to
// keep the per-worker path cheap).
func (r *Registry) hydrate(ctx context.Context, info *WorkerInfo, now time.Time) {
	if info == nil {
		return
	}
	if r.dbStore == nil {
		ConnectionStatusForInfo(info, false, now)
		return
	}
	active, err := r.dbStore.IsSessionActive(info.WorkerID)
	if err != nil {
		registryLog.WarnWithMsg(logging.CodeRegistryLoadSessionQueryFail,
			"worker session query failed; treating worker as DISCONNECTED",
			map[string]interface{}{"worker_id": info.WorkerID, "err": err.Error()})
		active = false
	}
	ConnectionStatusForInfo(info, active, now)
}

// Reason constants for non-CONNECTED states (RW-PROD-005 A2).
// Consumed by ConnectionReason() and exposed in WorkerInfo.Reason.
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
func ConnectionStatusForInfo(info *WorkerInfo, sessionActive bool, now time.Time) {
	info.SessionActive = sessionActive
	info.ConnectionStatus = ConnectionStatus(sessionActive, info.LastHB, info.Drain, now)
	info.Reason = ConnectionReason(sessionActive, info.Drain, info.LastHB, now)
}
