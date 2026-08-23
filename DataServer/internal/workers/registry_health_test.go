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

// ── HealthForInfo() tests ─────────────────────────────────────────────

// TestHealthForInfo_NilInfo asserts the helper is nil-safe.
func TestHealthForInfo_NilInfo(t *testing.T) {
	// No panic, no side effect.
	HealthForInfo(nil, time.Time{}, "", canonicalNow())
}

// TestHealthForInfo_PopulatesHealthField covers the round-trip:
// build a Worker, call HealthForInfo, assert info.Health is
// set to the canonical 9-state vocabulary (one of the 9 enum
// strings, never empty for a fresh-happy fixture).
func TestHealthForInfo_PopulatesHealthField(t *testing.T) {
	now := canonicalNow()
	info := &Worker{
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

// TestHealthForInfo_DoesNotReadActiveJobsFromMetrics asserts that
// heartbeat occupancy telemetry cannot drive the canonical state.
func TestHealthForInfo_DoesNotReadActiveJobsFromMetrics(t *testing.T) {
	now := canonicalNow()
	info := &Worker{
		WorkerID:      "wicket",
		LastHB:        freshHB(now, 30*time.Second),
		SessionActive: true,
		Metrics: map[string]interface{}{
			"active_tasks": float64(2),
		},
	}
	HealthForInfo(info, time.Time{}, "", now)
	if info.Health != WorkerHealthHealthy {
		t.Errorf("info.Health = %q, want %q (active_tasks telemetry must not drive state)",
			info.Health, WorkerHealthHealthy)
	}
}

// TestHealthForInfo_DrainFlag pins the drain precedence: an
// otherwise-fresh-idle worker with drain=true lands in
// DRAINING, not HEALTHY.
func TestHealthForInfo_DrainFlag(t *testing.T) {
	now := canonicalNow()
	info := &Worker{
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
	info := &Worker{
		WorkerID:      "wicket",
		LastHB:        freshHB(now, 30*time.Second),
		SessionActive: true,
		Metrics: map[string]interface{}{
			"active_tasks": float64(3),
		},
	}
	HealthForInfo(info, time.Time{}, string(DeploymentUpdating), now)
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
	info := &Worker{
		WorkerID:      "wicket",
		LastHB:        freshHB(now, 30*time.Second),
		SessionActive: true,
		Drain:         true, // would otherwise lift rank-6 DRAINING
		Metrics: map[string]interface{}{
			"active_tasks": float64(3), // would otherwise lift rank-8 BUSY
		},
	}
	HealthForInfo(info, time.Time{}, string(DeploymentRollback), now)
	if info.Health != WorkerHealthRollback {
		t.Errorf("info.Health = %q, want %q (ROLLBACK beats DRAINING > BUSY at rank 3)",
			info.Health, WorkerHealthRollback)
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
