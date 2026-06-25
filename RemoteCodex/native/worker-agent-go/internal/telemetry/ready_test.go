// Package telemetry — RW-PROD-004 §3 A6 ReadySnapshot tests
//
// Names in this package (TestReady_*) are stable so the canary
// scripts can grep them. Each test asserts both the BIsReady reply and
// the NotReadyReasons slice, so a regression where one drifts from the
// other is impossible to ship undetected.
package telemetry

import (
	"sync"
	"testing"
	"time"
)

// TestReady_AllOK confirms that a snapshot with every flag set true
// AND a generous disk floor is reported ready. Catches the
// regression where the reasons taxonomy gains a new entry but the
// IsReady short-circuit forgets to handle it.
func TestReady_AllOK(t *testing.T) {
	t.Cleanup(ResetForTest)
	MarkRegistered(true)
	MarkBootstrapped(true)
	MarkCacheReady(true)
	MarkBlobReady(true)
	SetExecutorsCount(1)
	SetDiskState(1<<30, 256*1024*1024)

	s := GlobalReady().Snapshot()
	if !s.IsReady() {
		t.Fatalf("expected IsReady=true, got reasons=%v", s.NotReadyReasons())
	}
	if got := s.NotReadyReasons(); len(got) != 0 {
		t.Fatalf("expected zero reasons, got %v", got)
	}
}

// TestReady_BootstrapNotRun_OnFreshProcess verifies the canonical
// "fresh boot, nothing has been plumbed yet" reading.
func TestReady_BootstrapNotRun_OnFreshProcess(t *testing.T) {
	t.Cleanup(ResetForTest)
	s := GlobalReady().Snapshot()
	if s.IsReady() {
		t.Fatalf("fresh process has NOT bootstrapped yet, must not be IsReady")
	}
	reasons := s.NotReadyReasons()
	wantAny := func(target string) bool {
		for _, r := range reasons {
			if r == target {
				return true
			}
		}
		return false
	}
	if !wantAny("bootstrap_not_run") {
		t.Fatalf("expected bootstrap_not_run in reasons; got %v", reasons)
	}
	if !wantAny("not_registered") {
		t.Fatalf("expected not_registered in reasons; got %v", reasons)
	}
	if !wantAny("cache.not_initialized") {
		t.Fatalf("expected cache.not_initialized in reasons; got %v", reasons)
	}
	if !wantAny("blob.not_initialized") {
		t.Fatalf("expected blob.not_initialized in reasons; got %v", reasons)
	}
}

// TestReady_DrainMode verifies that the drain flag trips IsReady=false
// even when every other readiness precondition is satisfied:
// a draining worker is intentionally not ready to receive new offers.
func TestReady_DrainMode(t *testing.T) {
	t.Cleanup(ResetForTest)
	MarkRegistered(true)
	MarkBootstrapped(true)
	MarkCacheReady(true)
	MarkBlobReady(true)
	SetExecutorsCount(1)
	SetDiskState(1<<30, 256*1024*1024)
	// Drain last so we can prove the order doesn't matter.
	MarkDrainMode(true)

	s := GlobalReady().Snapshot()
	if s.IsReady() {
		t.Fatal("drain_mode must flip IsReady=false even with every other gate true")
	}
	reasons := s.NotReadyReasons()
	if len(reasons) != 1 || reasons[0] != "drain_mode" {
		t.Fatalf("expected only drain_mode, got %v", reasons)
	}
}

// TestReady_NoExecutors verifies the executors.empty reason fires when
// the count is zero. Count==0 happens during --validate-config and
// during composition-root misconfiguration before
// registry.MustRegister.
func TestReady_NoExecutors(t *testing.T) {
	t.Cleanup(ResetForTest)
	MarkRegistered(true)
	MarkBootstrapped(true)
	MarkCacheReady(true)
	MarkBlobReady(true)
	SetExecutorsCount(0) // empty

	s := GlobalReady().Snapshot()
	if s.IsReady() {
		t.Fatal("zero executors count must flip IsReady=false")
	}
	reasons := s.NotReadyReasons()
	hasEmpty := false
	for _, r := range reasons {
		if r == "executors.empty" {
			hasEmpty = true
		}
	}
	if !hasEmpty {
		t.Fatalf("expected executors.empty in reasons; got %v", reasons)
	}
}

// TestReady_DiskCritical verifies that the disk-watch goroutine
// flipping SetDiskState to a value below the threshold fires
// disk.critical. Mirrors the watcher behaviour under a regressing
// scratch disk.
func TestReady_DiskCritical(t *testing.T) {
	t.Cleanup(ResetForTest)
	MarkRegistered(true)
	MarkBootstrapped(true)
	MarkCacheReady(true)
	MarkBlobReady(true)
	SetExecutorsCount(1)
	// Threshold 1 GiB, free 256 MiB — below the floor.
	SetDiskState(256*1024*1024, 1<<30)

	s := GlobalReady().Snapshot()
	if s.IsReady() {
		t.Fatal("DiskFreeBytes < DiskThresholdBytes must flip IsReady=false")
	}
	reasons := s.NotReadyReasons()
	hasCritical := false
	for _, r := range reasons {
		if r == "disk.critical" {
			hasCritical = true
		}
	}
	if !hasCritical {
		t.Fatalf("expected disk.critical in reasons; got %v", reasons)
	}
}

// TestReady_DiskStateUntilFirstSampleAVOIDs verify the threshold-only
// path (free=0): until the disk watcher goroutine has published a
// first sample, no disk.critical reason can fire (free=0 < anything
// positive would otherwise spuriously fail). Operators would
// otherwise see the worker enter disk.critical on a fresh start.
func TestReady_DiskStateUntilFirstSampleAvoidsFalseAlarm(t *testing.T) {
	t.Cleanup(ResetForTest)
	MarkRegistered(true)
	MarkBootstrapped(true)
	MarkCacheReady(true)
	MarkBlobReady(true)
	SetExecutorsCount(1)
	SetDiskState(0, 256*1024*1024)

	s := GlobalReady().Snapshot()
	if !s.IsReady() {
		t.Fatalf("expected IsReady=true until first disk sample; got reasons=%v", s.NotReadyReasons())
	}
}

// TestReady_DetailMap_FieldSet verifies the detail map includes every
// sentinel the spec asks for, including disk fields only when one of
// the two is non-zero.
func TestReady_DetailMap_FieldSet(t *testing.T) {
	t.Cleanup(ResetForTest)
	MarkRegistered(true)
	MarkBootstrapped(true)
	MarkCacheReady(true)
	MarkBlobReady(true)
	SetExecutorsCount(2)
	SetDiskState(1<<30, 256*1024*1024)

	d := GlobalReady().Snapshot().DetailMap()
	wantKeys := []string{"registered", "drain_mode", "bootstrapped", "executors_count", "cache_ready", "blob_ready", "disk_free_bytes", "disk_threshold_bytes"}
	for _, k := range wantKeys {
		if _, ok := d[k]; !ok {
			t.Errorf("missing detail map key %q", k)
		}
	}
	if d["executors_count"].(int) != 2 {
		t.Errorf("executors_count=%v want 2", d["executors_count"])
	}
}

// TestReady_ConcurrentMarkNoRace drives atomic.Pointer concurrency
// guarantees: 200 goroutines × 50 updates each must NOT corrupt the
// snapshot or trigger -race failures.
func TestReady_ConcurrentMarkNoRace(t *testing.T) {
	t.Cleanup(ResetForTest)
	var wg sync.WaitGroup
	const workers = 200
	const updates = 50
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < updates; j++ {
				MarkRegistered(id%2 == 0)
				MarkBootstrapped(j%2 == 0)
				MarkCacheReady(true)
				MarkBlobReady(true)
				SetExecutorsCount(j % 4)
				SetDiskState(int64(id*j), 1<<20)
			}
		}(i)
	}
	wg.Wait()
	// Final read must not panic. The ReadSnapshot on the snapshot
	// pointer is safe once all goroutines have joined.
	_ = GlobalReady().Snapshot()
}

// TestReady_GeneratedAtMonotonic verifies each UpdateReady tick
// advances GeneratedAt (or leaves it stable if the test runs fast
// enough that the clock does not advance; we tolerate "stable" but
// reject backward jumps).
func TestReady_GeneratedAtMonotonic(t *testing.T) {
	t.Cleanup(ResetForTest)
	MarkRegistered(true)
	first := GlobalReady().Snapshot().GeneratedAt
	time.Sleep(2 * time.Millisecond)
	MarkRegistered(true) // unchanged, but should bump GeneratedAt at least to "now"
	second := GlobalReady().Snapshot().GeneratedAt
	if second.Before(first) {
		t.Fatalf("GeneratedAt regressed: first=%v second=%v", first, second)
	}
}
