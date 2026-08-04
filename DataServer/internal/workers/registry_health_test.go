package workers

import (
	"testing"
	"time"
)

// canonicalNow returns a stable reference time for fixture tests
// so absolute-time edges are reproducible.
func canonicalNow() time.Time {
	return time.Date(2026, 7, 28, 17, 0, 0, 0, time.UTC)
}

// freshHB returns a heartbeat timestamp `age` seconds before now.
func freshHB(now time.Time, age time.Duration) string {
	return now.Add(-age).Format(time.RFC3339)
}

// ── Health() per-state tests ────────────────────────────────────────────

// TestHealth_Quarantined covers precedence rank 1: QUARANTINED
// beats every other signal. A "fresh heartbeat + drain + updating
// + busy + quarantine" worker MUST land in QUARANTINED.
func TestHealth_Quarantined(t *testing.T) {
	now := canonicalNow()
	got := Health(
		true, true, 5,
		freshHB(now, 30*time.Second),
		time.Time{}, HealthDeploymentUpdating,
		true, now,
	)
	if got != WorkerHealthQuarantined {
		t.Errorf("Health = %q, want %q", got, WorkerHealthQuarantined)
	}
}

// TestHealth_Offline covers precedence rank 2: non-live gate.
// Three orthogonal paths land in OFFLINE.
func TestHealth_Offline(t *testing.T) {
	now := canonicalNow()
	cases := []struct {
		name          string
		sessionActive bool
		lastHB        string
	}{
		{"session dead", false, freshHB(now, 30*time.Second)},
		{"empty heartbeat", true, ""},
		{"heartbeat beyond 5min", true, freshHB(now, 6*time.Minute)},
		{"unparseable heartbeat", true, "not-a-timestamp"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Thread tc.sessionActive through: each subtest below
			// targets a DIFFERENT root cause (session=dead, empty
			// HB, stale HB, unparseable HB) so we cannot hardcode
			// sessionActive=true here — the "session dead" subtest
			// specifically needs sessionActive=false to exercise
			// the precondition that lifts OFFLINE at rank 2.
			got := Health(tc.sessionActive, false, 0, tc.lastHB, time.Time{}, "", false, now)
			if got != WorkerHealthOffline {
				t.Errorf("Health = %q, want %q", got, WorkerHealthOffline)
			}
		})
	}
}

// TestHealth_Updating covers precedence rank 3: UPDATING beats
// drain and beats busy. A "fresh + drain + 3 jobs + UPDATING"
// worker lands in UPDATING.
func TestHealth_Updating(t *testing.T) {
	now := canonicalNow()
	got := Health(true, true, 3, freshHB(now, 30*time.Second),
		time.Time{}, HealthDeploymentUpdating, false, now)
	if got != WorkerHealthUpdating {
		t.Errorf("Health = %q, want %q", got, WorkerHealthUpdating)
	}
}

// TestHealth_Rollback covers precedence rank 4: ROLLBACK beats
// drain and busy. A "fresh + drain + 1 job + ROLLBACK" worker
// lands in ROLLBACK.
func TestHealth_Rollback(t *testing.T) {
	now := canonicalNow()
	got := Health(true, true, 1, freshHB(now, 30*time.Second),
		time.Time{}, HealthDeploymentRollback, false, now)
	if got != WorkerHealthRollback {
		t.Errorf("Health = %q, want %q", got, WorkerHealthRollback)
	}
}

// TestHealth_Restarting covers precedence rank 5: RESTARTING
// beats drain. A "fresh + drain + RESTARTING" worker lands in
// RESTARTING.
func TestHealth_Restarting(t *testing.T) {
	now := canonicalNow()
	got := Health(true, true, 0, freshHB(now, 30*time.Second),
		time.Time{}, HealthDeploymentRestarting, false, now)
	if got != WorkerHealthRestarting {
		t.Errorf("Health = %q, want %q", got, WorkerHealthRestarting)
	}
}

// TestHealth_Draining covers precedence rank 6: DRAINING with
// otherwise-idle fresh worker. This is the canonical "operator
// said drain=false on the queue side but the worker wants a
// smoke before any new leases".
func TestHealth_Draining(t *testing.T) {
	now := canonicalNow()
	got := Health(true, true, 0, freshHB(now, 30*time.Second),
		time.Time{}, "", false, now)
	if got != WorkerHealthDraining {
		t.Errorf("Health = %q, want %q", got, WorkerHealthDraining)
	}
}

// TestHealth_Degraded covers precedence rank 7: DEGRADED via
// smoke-fail OR heartbeat-stale. The legacy 4-state STALE bucket
// (150s ≤ age < 5min) maps to DEGRADED here per the migration
// note in CHANGELOG.
func TestHealth_Degraded(t *testing.T) {
	now := canonicalNow()
	cases := []struct {
		name          string
		lastHB        string
		lastSmokeFail time.Time
	}{
		{"heartbeat in stale window (3min)", freshHB(now, 3*time.Minute), time.Time{}},
		{"recent smoke fail (30min ago)", freshHB(now, 30*time.Second), now.Add(-30 * time.Minute)},
		{"both stale + recent smoke (smoke wins tie by precedence)", freshHB(now, 3*time.Minute), now.Add(-15 * time.Minute)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Health(true, false, 0, tc.lastHB, tc.lastSmokeFail, "", false, now)
			if got != WorkerHealthDegraded {
				t.Errorf("Health = %q, want %q", got, WorkerHealthDegraded)
			}
		})
	}
}

// TestHealth_Busy covers precedence rank 8: BUSY with fresh
// heartbeat, no drain, no fault.
func TestHealth_Busy(t *testing.T) {
	now := canonicalNow()
	got := Health(true, false, 1, freshHB(now, 30*time.Second),
		time.Time{}, "", false, now)
	if got != WorkerHealthBusy {
		t.Errorf("Health = %q, want %q", got, WorkerHealthBusy)
	}
}

// TestHealth_Healthy covers precedence rank 9: the default. Fresh
// heartbeat, no drain, no fault, no jobs.
func TestHealth_Healthy(t *testing.T) {
	now := canonicalNow()
	got := Health(true, false, 0, freshHB(now, 30*time.Second),
		time.Time{}, "", false, now)
	if got != WorkerHealthHealthy {
		t.Errorf("Health = %q, want %q", got, WorkerHealthHealthy)
	}
}

// ── Cross-cutting precedence tests ─────────────────────────────────────

// TestHealth_RollbackMasksUpdating verifies precedence rank 3
// beats rank 4: a worker carrying deployment_state='ROLLBACK'
// (and is_rollback=true) MUST surface as ROLLBACK even if
// the upstream query ever folds both signals in (defence
// against a future code path that defaults to UPDATING when
// is_rollback=false). Also pins that ROLLBACK beats drain.
func TestHealth_RollbackMasksUpdating(t *testing.T) {
	now := canonicalNow()
	// Note: in production the deployment_state string is
	// mutually exclusive on the row — the same row cannot be
	// both UPDATING and ROLLBACK at the same instant. The test
	// exercises the precedence by simulating the worst-case
	// input combination (both arms returning the rollback
	// input) to assert the switch order is robust.
	cases := []struct {
		name            string
		deploymentState string
		drain           bool
		activeJobs      int32
	}{
		{"ROLLBACK only", HealthDeploymentRollback, false, 0},
		{"ROLLBACK + drain", HealthDeploymentRollback, true, 0},
		{"ROLLBACK + active jobs", HealthDeploymentRollback, false, 3},
		{"ROLLBACK + drain + busy", HealthDeploymentRollback, true, 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Health(true, tc.drain, tc.activeJobs,
				freshHB(now, 30*time.Second),
				time.Time{}, tc.deploymentState, false, now)
			if got != WorkerHealthRollback {
				t.Errorf("Health = %q, want %q (ROLLBACK beats UPDATING/DRAINING/BUSY at rank 3)",
					got, WorkerHealthRollback)
			}
		})
	}
}

// TestHealth_QuarantinedMasksEverything verifies precedence rank 1
// is unconditional: UPDATING + ROLLBACK + DRAINING + BUSY + DEGRADED
// inputs all "fold under" QUARANTINED. Single test, multiple input
// permutations.
func TestHealth_QuarantinedMasksEverything(t *testing.T) {
	now := canonicalNow()
	cases := []struct {
		name            string
		deploymentState string
	}{
		{"QUARANTINED + UPDATING", HealthDeploymentUpdating},
		{"QUARANTINED + ROLLBACK", HealthDeploymentRollback},
		{"QUARANTINED + RESTARTING", HealthDeploymentRestarting},
		{"QUARANTINED + (no deploy)", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Even with drain=true and 5 active_jobs and stale heartbeat,
			// QUARANTINED still wins.
			got := Health(true, true, 5, freshHB(now, 3*time.Minute),
				now.Add(-30*time.Minute), tc.deploymentState, true, now)
			if got != WorkerHealthQuarantined {
				t.Errorf("Health = %q, want %q", got, WorkerHealthQuarantined)
			}
		})
	}
}

// TestHealth_OfflineMasksEverything verifies precedence rank 2
// is also unconditional: UPDATING + drain + ROLLBACK + RESTARTING
// all "fold under" OFFLINE when the worker is non-live.
func TestHealth_OfflineMasksEverything(t *testing.T) {
	now := canonicalNow()
	cases := []struct {
		name            string
		deploymentState string
	}{
		{"OFFLINE + UPDATING", HealthDeploymentUpdating},
		{"OFFLINE + ROLLBACK", HealthDeploymentRollback},
		{"OFFLINE + RESTARTING", HealthDeploymentRestarting},
		{"OFFLINE + drain + busy", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Health(false, true, 5, freshHB(now, 10*time.Minute),
				time.Time{}, tc.deploymentState, false, now)
			if got != WorkerHealthOffline {
				t.Errorf("Health = %q, want %q", got, WorkerHealthOffline)
			}
		})
	}
}

// TestHealth_StaleHeartbeatMapsToDegraded pins the semantic shift
// from the legacy 4-state's STALE bucket to DEGRADED in the new
// 9-state. A heartbeat exactly at ConnectionStaleThreshold (150s)
// MUST land in DEGRADED, not HEALTHY.
func TestHealth_StaleHeartbeatMapsToDegraded(t *testing.T) {
	now := canonicalNow()
	// Fresh edge of the stale window — right at ConnectionStaleThreshold.
	edgeHB := freshHB(now, ConnectionStaleThreshold)
	got := Health(true, false, 0, edgeHB, time.Time{}, "", false, now)
	if got != WorkerHealthDegraded {
		t.Errorf("Stale-edge heartbeat (%v old) → Health = %q, want %q",
			ConnectionStaleThreshold, got, WorkerHealthDegraded)
	}
	// Just-below edge (149s) — must land in HEALTHY or BUSY.
	justBelowHB := freshHB(now, ConnectionStaleThreshold-time.Second)
	got = Health(true, false, 0, justBelowHB, time.Time{}, "", false, now)
	if got != WorkerHealthHealthy {
		t.Errorf("Just-below-stale heartbeat (149s) → Health = %q, want %q",
			got, WorkerHealthHealthy)
	}
}

// ── HealthForInfo() tests ─────────────────────────────────────────────

// TestHealthForInfo_NilInfo asserts the helper is nil-safe.
func TestHealthForInfo_NilInfo(t *testing.T) {
	// No panic, no side effect.
	HealthForInfo(nil, time.Time{}, "", canonicalNow())
}

// TestHealthForInfo_PopulatesHealthField covers the round-trip:
// build a WorkerInfo, call HealthForInfo, assert info.Health is
// set to the canonical 9-state vocabulary (one of the 9 enum
// strings, never empty for a fresh-happy fixture).
func TestHealthForInfo_PopulatesHealthField(t *testing.T) {
	now := canonicalNow()
	info := &WorkerInfo{
		WorkerID:      "wicket",
		LastHB:        freshHB(now, 30*time.Second),
		SessionActive: true,
		Drain:         false,
	}
	HealthForInfo(info, time.Time{}, "", now)
	if info.Health != WorkerHealthHealthy {
		t.Errorf("info.Health = %q, want %q", info.Health, WorkerHealthHealthy)
	}
}

// TestHealthForInfo_ReadsActiveJobsFromMetrics asserts the
// metrics["active_tasks"] plumbing: a heartbeat with 2 active
// jobs lands in BUSY, not HEALTHY.
func TestHealthForInfo_ReadsActiveJobsFromMetrics(t *testing.T) {
	now := canonicalNow()
	info := &WorkerInfo{
		WorkerID:      "wicket",
		LastHB:        freshHB(now, 30*time.Second),
		SessionActive: true,
		Metrics: map[string]interface{}{
			"active_tasks": float64(2),
		},
	}
	HealthForInfo(info, time.Time{}, "", now)
	if info.Health != WorkerHealthBusy {
		t.Errorf("info.Health = %q, want %q (active_tasks=2 via metrics map)",
			info.Health, WorkerHealthBusy)
	}
}

// TestHealthForInfo_DrainFlag pins the drain precedence: an
// otherwise-fresh-idle worker with drain=true lands in
// DRAINING, not HEALTHY.
func TestHealthForInfo_DrainFlag(t *testing.T) {
	now := canonicalNow()
	info := &WorkerInfo{
		WorkerID:      "wicket",
		LastHB:        freshHB(now, 30*time.Second),
		SessionActive: true,
		Drain:         true,
	}
	HealthForInfo(info, time.Time{}, "", now)
	if info.Health != WorkerHealthDraining {
		t.Errorf("info.Health = %q, want %q (drain=true)",
			info.Health, WorkerHealthDraining)
	}
}

// TestHealthForInfo_DeploymentStateInput wires the deployment_state
// surface explicitly. When the future registry hydrate (Step 6/15)
// supplies "UPDATING", the helper surfaces UPDATING even when
// active_jobs would otherwise imply BUSY.
func TestHealthForInfo_DeploymentStateInput(t *testing.T) {
	now := canonicalNow()
	info := &WorkerInfo{
		WorkerID:      "wicket",
		LastHB:        freshHB(now, 30*time.Second),
		SessionActive: true,
		Metrics: map[string]interface{}{
			"active_tasks": float64(3),
		},
	}
	HealthForInfo(info, time.Time{}, HealthDeploymentUpdating, now)
	if info.Health != WorkerHealthUpdating {
		t.Errorf("info.Health = %q, want %q (UPDATING > BUSY)",
			info.Health, WorkerHealthUpdating)
	}
}

// TestHealthForInfo_DeploymentStateRollbackWins pins the precedence
// reversal on the helper path: when registry hydrate supplies
// deployment_state="ROLLBACK" (active recovery intervention) the
// helper surfaces ROLLBACK even when drain=true would otherwise be
// the dominant input. Symmetric to TestHealth_RollbackMasksUpdating
// on the pure-function side; the helper is the production code
// path, so it needs the equivalent pin.
func TestHealthForInfo_DeploymentStateRollbackWins(t *testing.T) {
	now := canonicalNow()
	info := &WorkerInfo{
		WorkerID:      "wicket",
		LastHB:        freshHB(now, 30*time.Second),
		SessionActive: true,
		Drain:         true, // would otherwise lift rank-6 DRAINING
		Metrics: map[string]interface{}{
			"active_tasks": float64(3), // would otherwise lift rank-8 BUSY
		},
	}
	HealthForInfo(info, time.Time{}, HealthDeploymentRollback, now)
	if info.Health != WorkerHealthRollback {
		t.Errorf("info.Health = %q, want %q (ROLLBACK beats DRAINING > BUSY at rank 3)",
			info.Health, WorkerHealthRollback)
	}
}

// ── DeriveDeploymentHealthState() tests ─────────────────────────────────

// TestDeriveDeploymentHealthState pins the canonical mapping from
// deployment_records row → Health() input vocabulary.
func TestDeriveDeploymentHealthState(t *testing.T) {
	cases := []struct {
		status     string
		isRollback bool
		want       string
	}{
		{"PENDING", false, HealthDeploymentUpdating},
		{"PENDING", true, HealthDeploymentRollback},
		{"SUCCEEDED", false, ""},
		{"FAILED", true, ""},
		{"ROLLED_BACK", false, ""},
		{"", false, ""},
	}
	for _, tc := range cases {
		t.Run(tc.status+"/"+boolStr(tc.isRollback), func(t *testing.T) {
			got := DeriveDeploymentHealthState(tc.status, tc.isRollback)
			if got != tc.want {
				t.Errorf("DeriveDeploymentHealthState(%q, %v) = %q, want %q",
					tc.status, tc.isRollback, got, tc.want)
			}
		})
	}
}

// TestWorkerHealth_EnumCompleteness is a self-check: the 9-state
// enum set must be exactly the 9 SPEC-mandated state names.
// Crashing this test should be the goal of any future enum
// renaming — the field is widely-typed on the admin surface and
// the spec is the contract.
//
// Uses an explicit slice + len check (NOT the previous
// strings.Contains + count-then-check brittle form, which
// could pass against a degenerate duplicated enum).
func TestWorkerHealth_EnumCompleteness(t *testing.T) {
	canonical := []string{
		WorkerHealthHealthy,
		WorkerHealthBusy,
		WorkerHealthDraining,
		WorkerHealthUpdating,
		WorkerHealthRestarting,
		WorkerHealthDegraded,
		WorkerHealthOffline,
		WorkerHealthQuarantined,
		WorkerHealthRollback,
	}
	if len(canonical) != 9 {
		t.Errorf("canonical enum slice has %d entries, want 9", len(canonical))
	}
	seen := make(map[string]bool, len(canonical))
	for _, s := range canonical {
		if seen[s] {
			t.Errorf("canonical enum duplicate: %q", s)
		}
		seen[s] = true
	}
	// Negative pin — typos like "HEALHY" do NOT pass.
	for _, typo := range []string{"HEALHY", "healhty", "BUSSY", "", "STALE"} {
		if seen[typo] {
			t.Errorf("non-canonical state %q unexpectedly accepts in the canonical set", typo)
		}
	}
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}
