package prefetch

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"velox-shared/assetref"
	"velox-shared/futureasset"
	"velox-worker-agent/internal/downloader"
	"velox-worker-agent/internal/workercache"
)

type schedulerManager struct {
	mu      sync.Mutex
	keys    []assetref.AssetKey
	started chan struct{}
}

type blockingSchedulerManager struct {
	*schedulerManager
	release chan struct{}
}

func (m *blockingSchedulerManager) Resolve(ctx context.Context, req downloader.DownloadRequest) (downloader.DownloadedAsset, error) {
	m.mu.Lock()
	m.keys = append(m.keys, req.AssetKey)
	m.mu.Unlock()
	select {
	case m.started <- struct{}{}:
	default:
	}
	select {
	case <-m.release:
	case <-ctx.Done():
		return downloader.DownloadedAsset{}, ctx.Err()
	}
	return downloader.DownloadedAsset{AssetKey: req.AssetKey, AssetID: req.AssetID, LocalPath: "/verified/" + string(req.AssetKey), SHA256: req.SHA256, SizeBytes: req.SizeBytes}, nil
}

type deferredProtectionStore struct {
	mu           sync.Mutex
	reserveCalls int
	releaseCalls int
	reserveErr   error
	releaseErr   error
}

func (s *deferredProtectionStore) Acquire(context.Context, assetref.AssetKey, string) error {
	return nil
}
func (s *deferredProtectionStore) Release(context.Context, assetref.AssetKey, string) error {
	return nil
}
func (s *deferredProtectionStore) Reserve(context.Context, assetref.AssetKey, string, time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reserveCalls++
	if s.reserveErr != nil {
		return s.reserveErr
	}
	if s.reserveCalls == 1 {
		return workercache.ErrNotFound
	}
	return nil
}
func (s *deferredProtectionStore) ReleaseReservation(context.Context, assetref.AssetKey, string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.releaseCalls++
	return s.releaseErr
}
func (s *deferredProtectionStore) calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reserveCalls
}

func (s *deferredProtectionStore) released() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.releaseCalls
}

func (m *schedulerManager) Resolve(_ context.Context, req downloader.DownloadRequest) (downloader.DownloadedAsset, error) {
	m.mu.Lock()
	m.keys = append(m.keys, req.AssetKey)
	m.mu.Unlock()
	if m.started != nil {
		select {
		case m.started <- struct{}{}:
		default:
		}
	}
	return downloader.DownloadedAsset{AssetKey: req.AssetKey, AssetID: req.AssetID, LocalPath: "/verified/" + string(req.AssetKey), SHA256: req.SHA256, SizeBytes: req.SizeBytes}, nil
}
func (m *schedulerManager) Snapshot(assetref.AssetKey) (downloader.DownloadSnapshot, bool) {
	return downloader.DownloadSnapshot{}, false
}
func (m *schedulerManager) Subscribe(assetref.AssetKey) (<-chan downloader.DownloadSnapshot, func()) {
	return nil, func() {}
}
func (m *schedulerManager) JobSnapshot(string) downloader.JobDownloadSnapshot {
	return downloader.JobDownloadSnapshot{}
}
func (m *schedulerManager) LatestOperational() downloader.OperationalSnapshot {
	return downloader.OperationalSnapshot{}
}

func futureTestPlan() futureasset.Plan {
	now := time.Now().UTC()
	return futureasset.Plan{Version: 1, PlanID: "p1", WorkerID: "worker-a", GeneratedAt: now, ExpiresAt: now.Add(time.Minute), Limits: futureasset.Limits{PrefetchHorizon: 3, ProtectionLookahead: 10}, PrefetchJobs: []futureasset.Job{{JobID: "n1", TaskID: "t1", ReservationID: "r1", Distance: 1, Assets: []futureasset.AssetManifest{{AssetKey: "D", AssetID: "D", SHA256: "sha-D", SizeBytes: 10}}}}, Protect: []futureasset.ProtectedAsset{{AssetKey: "D", FutureRefCount: 1, NextUseDistance: 1}}}
}

func TestScheduler_PrefetchesNPlusOneThroughCanonicalResolver(t *testing.T) {
	manager := &schedulerManager{started: make(chan struct{}, 1)}
	s := NewScheduler(Config{WorkerID: "worker-a", MaxConcurrent: 1, ByteBudget: 100})
	s.SetResolver(downloader.NewCacheResolver(manager, nil))
	if err := s.Reconcile(futureTestPlan()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-manager.started:
	case <-time.After(time.Second):
		t.Fatal("N+1 asset was not resolved")
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if len(manager.keys) != 1 || manager.keys[0] != "D" {
		t.Fatalf("resolved keys=%v, want [D]", manager.keys)
	}
}

func TestScheduler_MissingProtectionRowDoesNotAbortPrefetch(t *testing.T) {
	manager := &schedulerManager{started: make(chan struct{}, 1)}
	protections := &deferredProtectionStore{}
	s := NewScheduler(Config{WorkerID: "worker-a", MaxConcurrent: 1, ByteBudget: 100})
	s.SetResolver(downloader.NewCacheResolver(manager, nil))
	s.SetProtectionStore(protections)

	if err := s.Reconcile(futureTestPlan()); err != nil {
		t.Fatalf("Reconcile returned error for an asset not cached yet: %v", err)
	}
	select {
	case <-manager.started:
	case <-time.After(time.Second):
		t.Fatal("prefetch did not run after protection row was initially absent")
	}
	deadline := time.After(time.Second)
	for protections.calls() < 2 {
		select {
		case <-deadline:
			t.Fatalf("protection was not retried after verified resolve; calls=%d", protections.calls())
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func TestScheduler_DoesNotAdmitAssetLargerThanByteBudget(t *testing.T) {
	manager := &schedulerManager{started: make(chan struct{}, 1)}
	s := NewScheduler(Config{WorkerID: "worker-a", MaxConcurrent: 1, ByteBudget: 10})
	s.SetResolver(downloader.NewCacheResolver(manager, nil))
	plan := futureTestPlan()
	plan.PrefetchJobs[0].Assets[0].SizeBytes = 11

	if err := s.Reconcile(plan); err != nil {
		t.Fatal(err)
	}
	select {
	case <-manager.started:
		t.Fatal("oversized asset bypassed the prefetch byte budget")
	case <-time.After(100 * time.Millisecond):
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if len(manager.keys) != 0 {
		t.Fatalf("resolved keys=%v, want no resolution while asset exceeds budget", manager.keys)
	}
}

func TestScheduler_ExpiredPlanCleansRuntimeAndProtectionProjection(t *testing.T) {
	manager := &schedulerManager{started: make(chan struct{}, 1)}
	protections := &deferredProtectionStore{}
	s := NewScheduler(Config{WorkerID: "worker-a", MaxConcurrent: 1, ByteBudget: 100})
	s.SetResolver(downloader.NewCacheResolver(manager, nil))
	s.SetProtectionStore(protections)

	plan := futureTestPlan()
	if err := s.Reconcile(plan); err != nil {
		t.Fatal(err)
	}
	expired := plan
	expired.Version = 2
	expired.GeneratedAt = time.Now().UTC().Add(-2 * time.Minute)
	expired.ExpiresAt = time.Now().UTC().Add(-time.Minute)
	if err := s.Reconcile(expired); err != nil {
		t.Fatal(err)
	}
	if got := protections.released(); got != 1 {
		t.Fatalf("released protections=%d, want 1", got)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.jobs) != 0 || len(s.protects) != 0 || len(s.pendingProtects) != 0 || len(s.protectExpiries) != 0 || len(s.hints) != 0 || len(s.readyAtByJob) != 0 {
		t.Fatalf("expired scheduler projection not empty: jobs=%d protects=%d pending=%d expiries=%d hints=%d ready=%d", len(s.jobs), len(s.protects), len(s.pendingProtects), len(s.protectExpiries), len(s.hints), len(s.readyAtByJob))
	}
}

func TestScheduler_ReleaseProtectionErrorKeepsPreviousProjection(t *testing.T) {
	manager := &schedulerManager{started: make(chan struct{}, 1)}
	protections := &deferredProtectionStore{}
	s := NewScheduler(Config{WorkerID: "worker-a", MaxConcurrent: 1, ByteBudget: 100})
	s.SetResolver(downloader.NewCacheResolver(manager, nil))
	s.SetProtectionStore(protections)
	plan := futureTestPlan()
	if err := s.Reconcile(plan); err != nil {
		t.Fatal(err)
	}
	protections.mu.Lock()
	protections.releaseErr = errors.New("release unavailable")
	protections.mu.Unlock()
	next := plan
	next.Version = 2
	next.PrefetchJobs[0].Assets[0].AssetKey = "E"
	next.PrefetchJobs[0].Assets[0].AssetID = "E"
	next.PrefetchJobs[0].Assets[0].SHA256 = "sha-E"
	next.Protect[0].AssetKey = "E"
	if err := s.Reconcile(next); err == nil {
		t.Fatal("Reconcile returned nil after protection release failure")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if got := s.protects["D"]; got == "" || len(s.protects) != 1 {
		t.Fatalf("protection projection=%v, want previous D reservation only", s.protects)
	}
}

func TestScheduler_ReportsProtectionFailureAfterVerifiedResolve(t *testing.T) {
	manager := &blockingSchedulerManager{
		schedulerManager: &schedulerManager{started: make(chan struct{}, 1)},
		release:          make(chan struct{}),
	}
	protections := &deferredProtectionStore{}
	states := make(chan string, 8)
	s := NewScheduler(Config{
		WorkerID:      "worker-a",
		MaxConcurrent: 1,
		ByteBudget:    100,
		OnState: func(state string, _ futureasset.Job, _ futureasset.AssetManifest, _ error) {
			states <- state
		},
	})
	s.SetResolver(downloader.NewCacheResolver(manager, nil))
	s.SetProtectionStore(protections)
	defer s.Close()
	if err := s.Reconcile(futureTestPlan()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-manager.started:
	case <-time.After(time.Second):
		t.Fatal("prefetch did not start")
	}
	protections.mu.Lock()
	protections.reserveErr = errors.New("protection store unavailable")
	protections.mu.Unlock()
	close(manager.release)
	deadline := time.After(time.Second)
	for {
		select {
		case state := <-states:
			if state == "protection_failed" {
				return
			}
		case <-deadline:
			t.Fatal("verified resolve did not report protection failure")
		}
	}
}

func TestScheduler_CancelDetachesReferencesAndReportsWasted(t *testing.T) {
	events := make(chan string, 8)
	manager := &schedulerManager{started: make(chan struct{}, 1)}
	s := NewScheduler(Config{
		WorkerID:      "worker-a",
		MaxConcurrent: 1,
		ByteBudget:    100,
		OnState: func(state string, _ futureasset.Job, _ futureasset.AssetManifest, _ error) {
			events <- state
		},
	})
	s.SetResolver(downloader.NewCacheResolver(manager, nil))
	if err := s.Reconcile(futureTestPlan()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-manager.started:
	case <-time.After(time.Second):
		t.Fatal("prefetch did not resolve the asset")
	}
	deadline := time.After(time.Second)
	for {
		select {
		case state := <-events:
			if state == "downloaded" {
				goto downloaded
			}
		case <-deadline:
			t.Fatal("prefetch did not report the verified download")
		}
	}

downloaded:
	if !s.Cancel("n1") {
		t.Fatal("Cancel(n1) = false, want true")
	}
	deadline = time.After(time.Second)
	for {
		select {
		case state := <-events:
			if state == "wasted" {
				return
			}
		case <-deadline:
			t.Fatal("cancel did not report the unused prefetched asset as wasted")
		}
	}
}

func TestScheduler_SharedAssetUsesManagerSingleflight(t *testing.T) {
	var transfers atomic.Int32
	release := make(chan struct{})
	transferer := downloader.TransfererFunc(func(ctx context.Context, _ context.Context, req downloader.DownloadRequest, check bool, _ func(int64)) (downloader.CacheCheckResult, downloader.TransferResult, error) {
		if check {
			return downloader.CacheCheckResult{Outcome: downloader.CacheOutcomeMissNotFound}, downloader.TransferResult{}, nil
		}
		transfers.Add(1)
		select {
		case <-release:
		case <-ctx.Done():
			return downloader.CacheCheckResult{}, downloader.TransferResult{}, ctx.Err()
		}
		return downloader.CacheCheckResult{CacheHit: false}, downloader.TransferResult{LocalPath: "/verified/F", Bytes: req.SizeBytes, SHA256: req.SHA256}, nil
	})
	manager := downloader.NewManager(downloader.Config{Concurrency: 3}, transferer)
	defer manager.Close()
	s := NewScheduler(Config{WorkerID: "worker-a", MaxConcurrent: 3, ByteBudget: 100})
	s.SetResolver(downloader.NewCacheResolver(manager, nil))
	now := time.Now().UTC()
	plan := futureasset.Plan{Version: 1, PlanID: "p", WorkerID: "worker-a", GeneratedAt: now, ExpiresAt: now.Add(time.Minute), Limits: futureasset.Limits{PrefetchHorizon: 3, ProtectionLookahead: 10}, PrefetchJobs: []futureasset.Job{
		{JobID: "n1", TaskID: "t1", ReservationID: "r1", Distance: 1, Assets: []futureasset.AssetManifest{{AssetKey: "F", AssetID: "F", SHA256: "sha", SizeBytes: 10}}},
		{JobID: "n2", TaskID: "t2", ReservationID: "r2", Distance: 2, Assets: []futureasset.AssetManifest{{AssetKey: "F", AssetID: "F", SHA256: "sha", SizeBytes: 10}}},
		{JobID: "n3", TaskID: "t3", ReservationID: "r3", Distance: 3, Assets: []futureasset.AssetManifest{{AssetKey: "F", AssetID: "F", SHA256: "sha", SizeBytes: 10}}},
	}}
	if err := s.Reconcile(plan); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(time.Second)
	for transfers.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("shared transfer did not start")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if got := transfers.Load(); got != 1 {
		t.Fatalf("physical transfers=%d, want 1", got)
	}
	close(release)
}
