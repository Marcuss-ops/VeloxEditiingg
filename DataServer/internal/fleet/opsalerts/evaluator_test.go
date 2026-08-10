package opsalerts

import (
	"testing"
	"time"
)

// evaluator_test.go — Step 16/15 verifications for the pure
// rule evaluator. The contract:
//   - nil data → no event (skip-if-missing).
//   - rule_id+severity emit a hit only when the threshold is
//     crossed in the rule's "Above" direction.
//   - dual-severity rules (disk 85/95, cert 15d/5d) emit two
//     independent hits when the value crosses both thresholds.

func TestEvaluateEmptySnapshotReturnsNil(t *testing.T) {
	if hits := evaluateSnapshot(CallCtx{Now: time.Now()}, nil); hits != nil {
		t.Fatalf("nil snapshot should produce nil hits, got %d", len(hits))
	}
	empty := &WorkerSnapshot{WorkerID: ""}
	if hits := evaluateSnapshot(CallCtx{Now: time.Now()}, empty); hits != nil {
		t.Fatalf("empty workerID snapshot should produce nil hits, got %d", len(hits))
	}
}

func TestEvaluateHeartbeatStaleFiresAbove90(t *testing.T) {
	ctx := CallCtx{Now: time.Now()}
	val := 95.0
	snap := &WorkerSnapshot{WorkerID: "w1", HeartbeatAgeSeconds: &val}
	hits := evaluateSnapshot(ctx, snap)
	if len(hits) == 0 {
		t.Fatalf("heartbeat 95s should fire heartbeat_stale CRITICAL")
	}
	found := false
	for _, h := range hits {
		if h.RuleID == RuleHeartbeatStale && h.Severity == Critical {
			found = true
		}
	}
	if !found {
		t.Fatalf("heartbeat_stale CRITICAL hit missing in %d hits", len(hits))
	}
}

func TestEvaluateHeartbeatBelow60DoesNotFire(t *testing.T) {
	ctx := CallCtx{Now: time.Now()}
	val := 60.0
	snap := &WorkerSnapshot{WorkerID: "w1", HeartbeatAgeSeconds: &val}
	hits := evaluateSnapshot(ctx, snap)
	for _, h := range hits {
		if h.RuleID == RuleHeartbeatStale {
			t.Fatalf("heartbeat 60s should NOT fire heartbeat_stale, got hit: %+v", h)
		}
	}
}

func TestEvaluateDiskDualSeverityEscalation(t *testing.T) {
	ctx := CallCtx{Now: time.Now()}
	// Disk at 92 should fire ONLY the WARNING tier (85 boundary
	// crossed; 95 not crossed).
	val92 := 92.0
	snap92 := &WorkerSnapshot{WorkerID: "w1", DiskUsedPercent: &val92}
	hits := evaluateSnapshot(ctx, snap92)
	var warn bool
	for _, h := range hits {
		if h.RuleID == RuleDiskPressure {
			if h.Severity == Warning {
				warn = true
			}
			if h.Severity == Critical {
				t.Fatalf("disk 92%% should NOT fire disk_pressure CRITICAL (only WARNING)")
			}
		}
	}
	if !warn {
		t.Fatalf("disk 92%% should fire disk_pressure WARNING")
	}
	// Disk at 96 should fire BOTH tiers.
	val96 := 96.0
	snap96 := &WorkerSnapshot{WorkerID: "w1", DiskUsedPercent: &val96}
	hits = evaluateSnapshot(ctx, snap96)
	var gotW, gotC bool
	for _, h := range hits {
		if h.RuleID == RuleDiskPressure {
			if h.Severity == Warning {
				gotW = true
			}
			if h.Severity == Critical {
				gotC = true
			}
		}
	}
	if !gotW || !gotC {
		t.Fatalf("disk 96%% should fire BOTH disk_pressure WARNING and CRITICAL, got warning=%v critical=%v", gotW, gotC)
	}
}

func TestEvaluateSkipIfMissingDisablesEachRule(t *testing.T) {
	ctx := CallCtx{Now: time.Now()}
	// Empty snapshot: zero firing events (skip-if-missing).
	hits := evaluateSnapshot(ctx, &WorkerSnapshot{WorkerID: "w1"})
	if len(hits) != 0 {
		t.Fatalf("empty snapshot should produce 0 hits (skip-if-missing), got %d: %+v", len(hits), hits)
	}
}

func TestEvaluateSmokeFailedFiresFromStatus(t *testing.T) {
	ctx := CallCtx{Now: time.Now()}
	status := "FAILED"
	snap := &WorkerSnapshot{WorkerID: "w1", LatestSmokeStatus: &status}
	hits := evaluateSnapshot(ctx, snap)
	var found bool
	for _, h := range hits {
		if h.RuleID == RuleSmokeFailed && h.Severity == Critical {
			found = true
		}
	}
	if !found {
		t.Fatalf("smoke=FAILED should fire smoke_failed CRITICAL, got hits=%v", hits)
	}
}

func TestEvaluateSmokeSucceededDoesNotFire(t *testing.T) {
	ctx := CallCtx{Now: time.Now()}
	status := "SUCCEEDED"
	snap := &WorkerSnapshot{WorkerID: "w1", LatestSmokeStatus: &status}
	hits := evaluateSnapshot(ctx, snap)
	for _, h := range hits {
		if h.RuleID == RuleSmokeFailed {
			t.Fatalf("smoke=SUCCEEDED should NOT fire smoke_failed, got hit: %+v", h)
		}
	}
}

func TestEvaluateDriveDeliveryFailedFromArtifactID(t *testing.T) {
	ctx := CallCtx{Now: time.Now()}
	status := "FAILED"
	snap := &WorkerSnapshot{
		WorkerID:                   "w1",
		LatestSmokeStatus:          &status,
		LatestSmokeArtifactDriveID: nil, // missing → drive_delivery triggers
	}
	hits := evaluateSnapshot(ctx, snap)
	var foundSmoke, foundDrive bool
	for _, h := range hits {
		if h.RuleID == RuleSmokeFailed && h.Severity == Critical {
			foundSmoke = true
		}
		if h.RuleID == RuleDriveDeliveryFailed && h.Severity == Critical {
			foundDrive = true
		}
	}
	if !foundDrive {
		t.Fatalf("missing artifact_drive_id on FAILED smoke should fire drive_delivery_failed, got hits=%v", hits)
	}
	if !foundSmoke {
		t.Fatalf("FAILED smoke should also fire smoke_failed, got hits=%v", hits)
	}
}

func TestEvaluateDeploymentRolledBackFires(t *testing.T) {
	ctx := CallCtx{Now: time.Now()}
	st := "ROLLED_BACK"
	snap := &WorkerSnapshot{WorkerID: "w1", LatestDeploymentStatus: &st}
	hits := evaluateSnapshot(ctx, snap)
	var found bool
	for _, h := range hits {
		if h.RuleID == RuleDeploymentRollback && h.Severity == Critical {
			found = true
		}
	}
	if !found {
		t.Fatalf("status=ROLLED_BACK should fire deployment_rollback CRITICAL, got hits=%v", hits)
	}
}

func TestEvaluateVersionDriftFromDigestMismatch(t *testing.T) {
	ctx := CallCtx{Now: time.Now()}
	img := "sha256:abc"
	desired := "sha256:def"
	snap := &WorkerSnapshot{WorkerID: "w1", ImageDigest: &img, DesiredVersion: &desired}
	hits := evaluateSnapshot(ctx, snap)
	var found bool
	for _, h := range hits {
		if h.RuleID == RuleVersionDrift && h.Severity == Warning {
			found = true
		}
	}
	if !found {
		t.Fatalf("image!=desired should fire version_drift WARNING, got hits=%v", hits)
	}
}

func TestEvaluateVersionMatchDoesNotFire(t *testing.T) {
	ctx := CallCtx{Now: time.Now()}
	img := "sha256:abc"
	desired := "sha256:abc"
	snap := &WorkerSnapshot{WorkerID: "w1", ImageDigest: &img, DesiredVersion: &desired}
	hits := evaluateSnapshot(ctx, snap)
	for _, h := range hits {
		if h.RuleID == RuleVersionDrift {
			t.Fatalf("image==desired should NOT fire version_drift, got hit: %+v", h)
		}
	}
}

func TestEvaluateCertDualSeverityEscalation(t *testing.T) {
	ctx := CallCtx{Now: time.Now()}
	// Cert expires in 7 days: between 5d and 15d → only WARNING.
	expAt := ctx.Now.Add(7 * 24 * time.Hour)
	snap := &WorkerSnapshot{WorkerID: "w1", CertExpiresAt: &expAt}
	hits := evaluateSnapshot(ctx, snap)
	var gotW, gotC bool
	for _, h := range hits {
		if h.RuleID == RuleCertExpiring {
			if h.Severity == Warning {
				gotW = true
			}
			if h.Severity == Critical {
				gotC = true
			}
		}
	}
	if !gotW || gotC {
		t.Fatalf("cert 7d should fire WARNING only, got warning=%v critical=%v", gotW, gotC)
	}
	// Cert expires in 3 days: <5d → CRITICAL (15d WARNING also).
	expAt = ctx.Now.Add(3 * 24 * time.Hour)
	snap = &WorkerSnapshot{WorkerID: "w1", CertExpiresAt: &expAt}
	hits = evaluateSnapshot(ctx, snap)
	gotW, gotC = false, false
	for _, h := range hits {
		if h.RuleID == RuleCertExpiring {
			if h.Severity == Warning {
				gotW = true
			}
			if h.Severity == Critical {
				gotC = true
			}
		}
	}
	if !gotW || !gotC {
		t.Fatalf("cert 3d should fire BOTH warning and critical, got warning=%v critical=%v", gotW, gotC)
	}
}
