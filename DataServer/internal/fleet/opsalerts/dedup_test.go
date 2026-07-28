package opsalerts

import (
	"testing"
	"time"
)

// dedup_test.go — Step 16/15 verifications for the in-memory
// dedup state machine. Per the user spec suppression semantics:
//
//   - INFO     → never fires (ShouldFire false).
//   - WARNING  → 5-minute window; first event in window fires,
//                subsequent events within window touch only.
//   - CRITICAL → fires immediately on every fresh event.

func TestDedupINFONeverFires(t *testing.T) {
	d := NewDedupStore()
	key := DedupKey{WorkerID: "w1", RuleID: RuleHeartbeatStale, Severity: Info}
	if d.ShouldFire(key, Info, time.Now()) {
		t.Fatalf("INFO severity must never fire (Sopprimere eventi normali)")
	}
}

func TestDedupCRITICALFiresImmediatelyEveryTime(t *testing.T) {
	d := NewDedupStore()
	key := DedupKey{WorkerID: "w1", RuleID: RuleHeartbeatStale, Severity: Critical}
	t0 := time.Now()
	// First call fires.
	if !d.ShouldFire(key, Critical, t0) {
		t.Fatalf("first CRITICAL should fire")
	}
	d.Observe(key, AlertEventHit{WorkerID: "w1", RuleID: RuleHeartbeatStale, Severity: Critical, FiredAt: t0})
	// 1 minute later, second call STILL fires (no dedup window for CRITICAL).
	t1 := t0.Add(1 * time.Minute)
	if !d.ShouldFire(key, Critical, t1) {
		t.Fatalf("CRITICAL should fire on every fresh event regardless of dedup")
	}
}

func TestDedupWARNINGInWindowDeduplicated(t *testing.T) {
	d := NewDedupStore()
	key := DedupKey{WorkerID: "w1", RuleID: RuleDiskPressure, Severity: Warning}
	t0 := time.Now()
	// First call fires.
	if !d.ShouldFire(key, Warning, t0) {
		t.Fatalf("first WARNING should fire")
	}
	d.Observe(key, AlertEventHit{WorkerID: "w1", RuleID: RuleDiskPressure, Severity: Warning, FiredAt: t0})
	// 1 minute later: in-window, should NOT fire.
	t1 := t0.Add(1 * time.Minute)
	if d.ShouldFire(key, Warning, t1) {
		t.Fatalf("WARNING within 5min window should NOT fire (dedup)")
	}
	// 6 minutes later: window elapsed, should fire.
	t2 := t0.Add(6 * time.Minute)
	if !d.ShouldFire(key, Warning, t2) {
		t.Fatalf("WARNING after 5min window should fire again")
	}
}

func TestDedupForgetRemovesKey(t *testing.T) {
	d := NewDedupStore()
	key := DedupKey{WorkerID: "w1", RuleID: RuleDiskPressure, Severity: Warning}
	d.Observe(key, AlertEventHit{WorkerID: "w1", RuleID: RuleDiskPressure, Severity: Warning, FiredAt: time.Now()})
	d.Forget(key)
	// After Forget, key is gone — ShouldFire returns true.
	if !d.ShouldFire(key, Warning, time.Now()) {
		t.Fatalf("after Forget, the key should be eligible to fire again")
	}
}

func TestDedupTouchBumpsLastSeenAt(t *testing.T) {
	d := NewDedupStore()
	key := DedupKey{WorkerID: "w1", RuleID: RuleDiskPressure, Severity: Warning}
	t0 := time.Now()
	d.Observe(key, AlertEventHit{WorkerID: "w1", RuleID: RuleDiskPressure, Severity: Warning, FiredAt: t0})
	t1 := t0.Add(30 * time.Second)
	d.Touch(key, t1, "disk=92%", "msg")
	// Still in window: ShouldFire must return false (dedup
	// window closes on LAST-seen-at, not first-seen-at).
	if d.ShouldFire(key, Warning, t1.Add(1*time.Minute)) {
		t.Fatalf("WARNING within 5min after Touch should still be deduped")
	}
}

func TestDedupIterateWorker(t *testing.T) {
	d := NewDedupStore()
	d.Observe(DedupKey{WorkerID: "w1", RuleID: RuleDiskPressure, Severity: Warning}, AlertEventHit{WorkerID: "w1", RuleID: RuleDiskPressure, Severity: Warning, FiredAt: time.Now()})
	d.Observe(DedupKey{WorkerID: "w1", RuleID: RuleDiskPressure, Severity: Critical}, AlertEventHit{WorkerID: "w1", RuleID: RuleDiskPressure, Severity: Critical, FiredAt: time.Now()})
	d.Observe(DedupKey{WorkerID: "w2", RuleID: RuleHeartbeatStale, Severity: Critical}, AlertEventHit{WorkerID: "w2", RuleID: RuleHeartbeatStale, Severity: Critical, FiredAt: time.Now()})
	keys := d.iterateWorker("w1")
	if len(keys) != 2 {
		t.Fatalf("iterateWorker w1 should return 2 keys, got %d", len(keys))
	}
	keys2 := d.iterateWorker("w2")
	if len(keys2) != 1 {
		t.Fatalf("iterateWorker w2 should return 1 key, got %d", len(keys2))
	}
}
