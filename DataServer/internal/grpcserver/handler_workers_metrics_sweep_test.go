// Package grpcserver / handler_workers_metrics_sweep_test.go
//
// Unit tests for the lastSeenByWorker growth bounds: stale entries (no
// heartbeat within lastSeenStaleAfter) are evicted when the map exceeds its
// cap, and the cap itself is respected. Run with -race; the sweep mutates
// the package-level sync.Map alongside concurrent Snapshot writers.
package grpcserver

import (
	"strings"
	"sync"
	"testing"
	"time"
)

func resetLastSeenByWorker(t *testing.T) {
	t.Helper()
	t.Cleanup(func() {
		lastSeenByWorker.Range(func(key, _ any) bool {
			lastSeenByWorker.Delete(key)
			return true
		})
	})
}

// TestSweepLastSeenByWorker_EvictsStaleEntries seeds the map over the cap
// where every entry is long-stale and asserts a full eviction down to the cap.
func TestSweepLastSeenByWorker_EvictsStaleEntries(t *testing.T) {
	resetLastSeenByWorker(t)

	stale := time.Now().UTC().Add(-2 * lastSeenStaleAfter)
	for i := 0; i < lastSeenMaxEntries+10; i++ {
		ls := &LastSeenResources{}
		ls.mu.Lock()
		ls.lastUpdate = stale
		ls.mu.Unlock()
		lastSeenByWorker.Store(strings.Repeat("w", 8)+"-"+time.Now().Format("150405.000000000")+"-"+string(rune('a'+i%26))+string(rune('0'+i%10)), ls)
	}

	sweepLastSeenByWorker()

	size := 0
	lastSeenByWorker.Range(func(_, _ any) bool { size++; return true })
	if size > lastSeenMaxEntries {
		t.Fatalf("map size %d exceeds cap %d after stale sweep", size, lastSeenMaxEntries)
	}
}

// TestSweepLastSeenByWorker_KeepsFreshEntries seeds a mix of fresh and stale
// entries over the cap and asserts fresh (recently-heartbeating) entries
// survive pass 1; pass 2 only evicts the oldest when still over cap.
func TestSweepLastSeenByWorker_KeepsFreshEntries(t *testing.T) {
	resetLastSeenByWorker(t)

	fresh := &LastSeenResources{}
	fresh.mu.Lock()
	fresh.lastUpdate = time.Now().UTC()
	fresh.mu.Unlock()
	lastSeenByWorker.Store("fresh-worker-under-test", fresh)

	stale := time.Now().UTC().Add(-2 * lastSeenStaleAfter)
	for i := 0; i < lastSeenMaxEntries+10; i++ {
		ls := &LastSeenResources{}
		ls.mu.Lock()
		ls.lastUpdate = stale
		ls.mu.Unlock()
		lastSeenByWorker.Store(strings.Repeat("s", 8)+"-"+time.Now().Format("150405.000000000")+"-"+string(rune('a'+i%26))+string(rune('0'+i%10)), ls)
	}

	sweepLastSeenByWorker()

	if _, ok := lastSeenByWorker.Load("fresh-worker-under-test"); !ok {
		t.Fatal("fresh entry was evicted while stale entries existed")
	}
}

// TestSweepLastSeenByWorker_NoOpUnderCap asserts the sweep is a no-op when
// the map is below the cap — the common production case.
func TestSweepLastSeenByWorker_NoOpUnderCap(t *testing.T) {
	resetLastSeenByWorker(t)

	for i := 0; i < 8; i++ {
		lastSeenByWorker.Store(strings.Repeat("k", 4)+"-"+string(rune('a'+i)), &LastSeenResources{})
	}

	sweepLastSeenByWorker() // must not panic or evict

	size := 0
	lastSeenByWorker.Range(func(_, _ any) bool { size++; return true })
	if size != 8 {
		t.Fatalf("map size = %d, want 8 (sweep must be a no-op under the cap)", size)
	}
}

// TestSnapshotRecordsLastUpdate asserts Snapshot maintains lastUpdate, which
// the sweep's staleness classification depends on.
func TestSnapshotRecordsLastUpdate(t *testing.T) {
	ls := &LastSeenResources{}
	before := time.Now().UTC().Add(-time.Second)
	ls.Snapshot(1, 2, 3, 4)
	ls.mu.Lock()
	last := ls.lastUpdate
	ls.mu.Unlock()
	if last.Before(before) {
		t.Fatalf("lastUpdate = %v, want >= %v", last, before)
	}
}

// TestSweepConcurrentWithSnapshot runs the sweep concurrently with heartbeat
// snapshots; run under -race to guard the lock discipline.
func TestSweepConcurrentWithSnapshot(t *testing.T) {
	resetLastSeenByWorker(t)

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			decodeWorkerResources("sweep-race-worker", nil)
			lsIface, _ := lastSeenByWorker.Load("sweep-race-worker")
			if ls, ok := lsIface.(*LastSeenResources); ok {
				ls.Snapshot(10, 10, 0, 0)
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		sweepLastSeenByWorker()
	}()
	wg.Wait()
}
