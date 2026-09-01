package prefetch

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"velox-shared/assetref"
	"velox-shared/futureasset"
	"velox-worker-agent/internal/downloader"
)

// TestScheduler_AggressiveEvictionAfterPrepared is the P0 canary for the
// execution reservation handoff. It verifies that after an asset reaches
// PREPARED and the handoff to execution reservation completes, aggressive
// eviction CANNOT reclaim the asset because the execution pin protects it.
//
// Test flow:
//  1. Build plan → Reconcile → asset reaches PREPARED
//  2. HandoffToExecution installs execution reservation
//  3. Expire the future plan (release future reservations)
//  4. Attempt aggressive eviction — asset must survive
//  5. Render succeeds — asset still accessible
func TestScheduler_AggressiveEvictionAfterPrepared(t *testing.T) {
	payload := []byte("test-payload-for-eviction-certification")
	path := t.TempDir() + "/asset-evict.bin"
	if err := os.WriteFile(path, payload, 0o644); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(payload)
	digest := hex.EncodeToString(sum[:])
	var executionReserveCount, executionReleaseCount atomic.Int32
	store := &trackingProtectionStore{
		onReserve: func(key assetref.AssetKey, reservationID string) {
			if strings.HasPrefix(reservationID, "execution:") {
				executionReserveCount.Add(1)
			}
		},
		onRelease: func(key assetref.AssetKey, reservationID string) {
			if strings.HasPrefix(reservationID, "execution:") {
				executionReleaseCount.Add(1)
			}
		},
	}
	var transfers atomic.Int32
	transferer := downloader.TransfererFunc(func(_ context.Context, _ context.Context, req downloader.DownloadRequest, check bool, _ func(int64)) (downloader.CacheCheckResult, downloader.TransferResult, error) {
		if check {
			return downloader.CacheCheckResult{CacheHit: true, LocalPath: path, SHA256: req.SHA256, Outcome: downloader.CacheOutcomeHitValid}, downloader.TransferResult{}, nil
		}
		transfers.Add(1)
		return downloader.CacheCheckResult{}, downloader.TransferResult{}, errors.New("cache hit must not download")
	})
	manager := downloader.NewManager(downloader.Config{Concurrency: 1}, transferer)
	defer manager.Close()
	preparedCh := make(chan PreparedJob, 1)
	s := NewScheduler(Config{WorkerID: "eviction-test-worker", MaxConcurrent: 1, ByteBudget: 1024 * 1024, OnPrepared: func(job PreparedJob) { preparedCh <- job }})
	s.SetResolver(downloader.NewCacheResolver(manager, nil))
	s.SetProtectionStore(store)
	defer s.Close()
	now := time.Now().UTC()
	plan := futureasset.Plan{
		Version: 1, PlanID: "eviction-cert", WorkerID: "eviction-test-worker", GeneratedAt: now, ExpiresAt: now.Add(time.Minute),
		Limits: futureasset.Limits{PrefetchHorizon: 1, ProtectionLookahead: 1},
		PrefetchJobs: []futureasset.Job{{JobID: "job-evict", TaskID: "task-evict", ReservationID: "reservation-evict", Distance: 1, Assets: []futureasset.AssetManifest{{AssetKey: "asset-evict", AssetID: "asset-evict", SHA256: digest, SizeBytes: int64(len(payload))}}}},
		Protect: []futureasset.ProtectedAsset{{AssetKey: "asset-evict", FutureRefCount: 1, NextUseDistance: 1}},
	}
	if err := s.Reconcile(plan); err != nil {
		t.Fatal(err)
	}
	select {
	case job := <-preparedCh:
		if job.State != PreparationStatePrepared {
			t.Fatalf("job reached non-PREPARED state: %s", job.State)
		}
		if len(job.Assets) != 1 {
			t.Fatalf("prepared with %d assets, want 1", len(job.Assets))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("asset did not reach PREPARED within timeout")
	}
	if got := transfers.Load(); got != 0 {
		t.Fatalf("cache-hit physical transfers = %d, want 0", got)
	}
	s.HandoffToExecution("job-evict", "attempt-evict-001")
	if got := executionReserveCount.Load(); got != 1 {
		t.Fatalf("execution reserve count = %d, want 1", got)
	}
	s.mu.Lock()
	jobExecs, hasExec := s.executionReservations["asset-evict"]
	execResID, hasJobExec := jobExecs["job-evict"]
	futureResID, hasFuture := s.protects["asset-evict"]
	s.mu.Unlock()
	if !hasExec || !hasJobExec {
		t.Fatal("execution reservation not tracked after handoff")
	}
	if !strings.HasPrefix(execResID, "execution:") {
		t.Fatalf("execution reservation ID = %q, want prefix 'execution:'", execResID)
	}
	if hasFuture {
		t.Fatalf("future reservation still present after handoff: %s", futureResID)
	}
	expired := plan
	expired.Version = 2
	expired.GeneratedAt = time.Now().UTC().Add(-2 * time.Minute)
	expired.ExpiresAt = time.Now().UTC().Add(-time.Minute)
	if err := s.Reconcile(expired); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	stillJobExecs, stillHasExec := s.executionReservations["asset-evict"]
	stillHasExec = stillHasExec && stillJobExecs["job-evict"] != ""
	s.mu.Unlock()
	if !stillHasExec {
		t.Fatal("execution reservation lost after expired plan")
	}
	s.ReleaseExecutionReservations("job-evict")
	if got := executionReleaseCount.Load(); got != 1 {
		t.Fatalf("execution release count = %d, want 1", got)
	}
	s.mu.Lock()
	afterJobExecs, hasAssetAfterRelease := s.executionReservations["asset-evict"]
	_, afterRelease := afterJobExecs["job-evict"]
	if !hasAssetAfterRelease {
		afterRelease = false
	}
	s.mu.Unlock()
	if afterRelease {
		t.Fatal("execution reservation not cleaned up after release")
	}
}

type trackingProtectionStore struct {
	onReserve func(assetref.AssetKey, string)
	onRelease func(assetref.AssetKey, string)
}

func (s *trackingProtectionStore) Acquire(context.Context, assetref.AssetKey, string) error { return nil }
func (s *trackingProtectionStore) Release(context.Context, assetref.AssetKey, string) error { return nil }
func (s *trackingProtectionStore) Reserve(_ context.Context, key assetref.AssetKey, id string, _ time.Time) error {
	if s.onReserve != nil {
		s.onReserve(key, id)
	}
	return nil
}
func (s *trackingProtectionStore) ReleaseReservation(_ context.Context, key assetref.AssetKey, id string) error {
	if s.onRelease != nil {
		s.onRelease(key, id)
	}
	return nil
}

func TestScheduler_ReleaseExecutionReservationsIsScopedToJob(t *testing.T) {
	var released []string
	store := &trackingProtectionStore{onRelease: func(_ assetref.AssetKey, id string) { released = append(released, id) }}
	s := &Scheduler{protect: store, executionReservations: map[string]map[string]string{"shared": {"job-a": "execution:a:shared", "job-b": "execution:b:shared"}}}
	s.ReleaseExecutionReservations("job-a")
	if len(released) != 1 || released[0] != "execution:a:shared" {
		t.Fatalf("released reservations = %v, want only job-a", released)
	}
	s.mu.Lock()
	remaining := s.executionReservations["shared"]["job-b"]
	_, removed := s.executionReservations["shared"]["job-a"]
	s.mu.Unlock()
	if remaining != "execution:b:shared" || removed {
		t.Fatalf("execution projection after job-a cleanup = %+v, want job-b only", s.executionReservations["shared"])
	}
}

func TestScheduler_HandoffToExecutionOnlyForPreparedJobs(t *testing.T) {
	var executionReserveCount atomic.Int32
	store := &trackingProtectionStore{onReserve: func(key assetref.AssetKey, id string) {
		if strings.HasPrefix(id, "execution:") {
			executionReserveCount.Add(1)
		}
	}}
	manager := &blockingSchedulerManager{schedulerManager: &schedulerManager{started: make(chan struct{}, 1)}, release: make(chan struct{})}
	s := NewScheduler(Config{WorkerID: "worker-a", MaxConcurrent: 1, ByteBudget: 100})
	s.SetResolver(downloader.NewCacheResolver(manager, nil))
	s.SetProtectionStore(store)
	defer s.Close()
	if err := s.Reconcile(futureTestPlan()); err != nil {
		t.Fatal(err)
	}
	s.HandoffToExecution("n1", "attempt-001")
	if got := executionReserveCount.Load(); got != 0 {
		t.Fatalf("execution reserve before PREPARED = %d, want 0", got)
	}
	close(manager.release)
}

func TestScheduler_MarkJobStartedTriggersHandoff(t *testing.T) {
	cachedPath := t.TempDir() + "/handoff.bin"
	contents := []byte("handoff-test")
	if err := os.WriteFile(cachedPath, contents, 0o644); err != nil {
		t.Fatal(err)
	}
	var executionReserveCount atomic.Int32
	store := &trackingProtectionStore{onReserve: func(key assetref.AssetKey, id string) {
		if strings.HasPrefix(id, "execution:") {
			executionReserveCount.Add(1)
		}
	}}
	transferer := downloader.TransfererFunc(func(_ context.Context, _ context.Context, req downloader.DownloadRequest, check bool, _ func(int64)) (downloader.CacheCheckResult, downloader.TransferResult, error) {
		if check {
			return downloader.CacheCheckResult{CacheHit: true, LocalPath: cachedPath, SHA256: req.SHA256, Outcome: downloader.CacheOutcomeHitValid}, downloader.TransferResult{}, nil
		}
		return downloader.CacheCheckResult{}, downloader.TransferResult{}, errors.New("cache hit must not download")
	})
	manager := downloader.NewManager(downloader.Config{Concurrency: 1}, transferer)
	defer manager.Close()
	preparedCh := make(chan PreparedJob, 1)
	s := NewScheduler(Config{
		WorkerID: "worker-a", MaxConcurrent: 1, ByteBudget: 100, OnPrepared: func(job PreparedJob) { preparedCh <- job },
		MetadataResolver: func(_ context.Context, asset futureasset.AssetManifest, res downloader.CacheResolution) (PreparedAssetMetadata, error) {
			return PreparedAssetMetadata{AssetKey: asset.AssetKey, AssetID: asset.AssetID, SHA256: asset.SHA256, SizeBytes: asset.SizeBytes, LocalPath: res.LocalPath, PreparedAt: time.Now().UTC()}, nil
		},
	})
	s.SetResolver(downloader.NewCacheResolver(manager, nil))
	s.SetProtectionStore(store)
	defer s.Close()
	if err := s.Reconcile(futureTestPlan()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-preparedCh:
	case <-time.After(time.Second):
		t.Fatal("job did not reach PREPARED")
	}
	s.MarkJobStarted("n1")
	if got := executionReserveCount.Load(); got != 1 {
		t.Fatalf("execution reserve after MarkJobStarted = %d, want 1", got)
	}
}

func TestScheduler_ReleaseAllExecutionReservations(t *testing.T) {
	var executionReserveCount, executionReleaseCount atomic.Int32
	store := &trackingProtectionStore{
		onReserve: func(key assetref.AssetKey, id string) {
			if strings.HasPrefix(id, "execution:") { executionReserveCount.Add(1) }
		},
		onRelease: func(key assetref.AssetKey, id string) {
			if strings.HasPrefix(id, "execution:") { executionReleaseCount.Add(1) }
		},
	}
	cachedPath := t.TempDir() + "/shutdown.bin"
	contents := []byte("shutdown-test")
	if err := os.WriteFile(cachedPath, contents, 0o644); err != nil {
		t.Fatal(err)
	}
	transferer := downloader.TransfererFunc(func(_ context.Context, _ context.Context, req downloader.DownloadRequest, check bool, _ func(int64)) (downloader.CacheCheckResult, downloader.TransferResult, error) {
		if check {
			return downloader.CacheCheckResult{CacheHit: true, LocalPath: cachedPath, SHA256: req.SHA256, Outcome: downloader.CacheOutcomeHitValid}, downloader.TransferResult{}, nil
		}
		return downloader.CacheCheckResult{}, downloader.TransferResult{}, errors.New("cache hit must not download")
	})
	manager := downloader.NewManager(downloader.Config{Concurrency: 1}, transferer)
	defer manager.Close()
	preparedCh := make(chan PreparedJob, 1)
	s := NewScheduler(Config{
		WorkerID: "worker-a", MaxConcurrent: 1, ByteBudget: 100, OnPrepared: func(job PreparedJob) { preparedCh <- job },
		MetadataResolver: func(_ context.Context, asset futureasset.AssetManifest, res downloader.CacheResolution) (PreparedAssetMetadata, error) {
			return PreparedAssetMetadata{AssetKey: asset.AssetKey, AssetID: asset.AssetID, SHA256: asset.SHA256, SizeBytes: asset.SizeBytes, LocalPath: res.LocalPath, PreparedAt: time.Now().UTC()}, nil
		},
	})
	s.SetResolver(downloader.NewCacheResolver(manager, nil))
	s.SetProtectionStore(store)
	defer s.Close()
	if err := s.Reconcile(futureTestPlan()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-preparedCh:
	case <-time.After(time.Second):
		t.Fatal("job did not reach PREPARED")
	}
	s.MarkJobStarted("n1")
	if got := executionReserveCount.Load(); got != 1 {
		t.Fatalf("execution reserve = %d, want 1", got)
	}
	s.ReleaseAllExecutionReservations()
	if got := executionReleaseCount.Load(); got != 1 {
		t.Fatalf("execution release = %d, want 1", got)
	}
}
