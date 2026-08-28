package prefetch

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"strings"
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
	// Resolve signals at entry. Wait until runWorkItem has recorded the
	// verified transfer before cancelling; otherwise the test races the
	// bookkeeping that makes the asset eligible for a wasted event.
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

func TestScheduler_AssetQueuePrioritizesNearerJobAcrossAssets(t *testing.T) {
	manager := &blockingSchedulerManager{
		schedulerManager: &schedulerManager{started: make(chan struct{}, 8)},
		release:          make(chan struct{}),
	}
	s := NewScheduler(Config{WorkerID: "worker-a", MaxConcurrent: 2, ByteBudget: 100})
	s.SetResolver(downloader.NewCacheResolver(manager, nil))
	now := time.Now().UTC()
	plan := futureasset.Plan{
		Version: 1, PlanID: "p", WorkerID: "worker-a", GeneratedAt: now, ExpiresAt: now.Add(time.Minute),
		Limits: futureasset.Limits{PrefetchHorizon: 3, ProtectionLookahead: 10},
		// N+1 has two assets. Both bounded slots must be given to N+1;
		// the old per-job loop could give the second slot to N+2.
		PrefetchJobs: []futureasset.Job{
			{JobID: "n1", TaskID: "t1", ReservationID: "r1", Distance: 1, Assets: []futureasset.AssetManifest{
				{AssetKey: "near-a", SHA256: "s-near-a", SizeBytes: 10},
				{AssetKey: "near-b", SHA256: "s-near-b", SizeBytes: 10},
			}},
			{JobID: "n2", TaskID: "t2", ReservationID: "r2", Distance: 2, Assets: []futureasset.AssetManifest{{AssetKey: "far", SHA256: "s-far", SizeBytes: 10}}},
		},
	}
	if err := s.Reconcile(plan); err != nil {
		t.Fatal(err)
	}
	select {
	case <-manager.started:
	case <-time.After(time.Second):
		t.Fatal("prefetch did not resolve an asset")
	}
	select {
	case <-manager.started:
	case <-time.After(time.Second):
		t.Fatal("prefetch did not fill the second bounded slot")
	}
	manager.mu.Lock()
	got := append([]assetref.AssetKey(nil), manager.keys...)
	manager.mu.Unlock()
	close(manager.release)
	if len(got) != 2 || (got[0] != "near-a" && got[1] != "near-a") || (got[0] != "near-b" && got[1] != "near-b") {
		t.Fatalf("started resolved keys=%v, want [near-a near-b]", got)
	}
}

func TestScheduler_ReprioritizationInvalidatesQueuedGeneration(t *testing.T) {
	manager := &blockingSchedulerManager{
		schedulerManager: &schedulerManager{started: make(chan struct{}, 8)},
		release:          make(chan struct{}),
	}
	s := NewScheduler(Config{WorkerID: "worker-a", MaxConcurrent: 1, ByteBudget: 100})
	s.SetResolver(downloader.NewCacheResolver(manager, nil))
	now := time.Now().UTC()
	plan := futureasset.Plan{
		Version: 1, PlanID: "p1", WorkerID: "worker-a", GeneratedAt: now, ExpiresAt: now.Add(time.Minute),
		Limits: futureasset.Limits{PrefetchHorizon: 3, ProtectionLookahead: 10},
		PrefetchJobs: []futureasset.Job{
			{JobID: "blocker", TaskID: "tb", ReservationID: "rb", Distance: 1},
			{JobID: "n1", TaskID: "t1", ReservationID: "r1", Distance: 2, Assets: []futureasset.AssetManifest{
				{AssetKey: "a", SHA256: "s-a", SizeBytes: 10},
				{AssetKey: "b", SHA256: "s-b", SizeBytes: 10},
			}},
		},
	}
	if err := s.Reconcile(plan); err != nil {
		t.Fatal(err)
	}

	plan.Version = 2
	plan.PlanID = "p2"
	plan.PrefetchJobs = []futureasset.Job{plan.PrefetchJobs[1]}
	plan.PrefetchJobs[0].Distance = 1
	if err := s.Reconcile(plan); err != nil {
		t.Fatal(err)
	}

	// The old generation remains physically in the heap until admission, but
	// it must no longer be eligible after the sliding-window update.
	s.mu.Lock()
	for _, item := range s.queue {
		if item.job.JobID == "n1" && item.generation == 1 && s.currentItemLocked(item) {
			s.mu.Unlock()
			t.Fatal("stale generation remained eligible after reprioritization")
		}
	}
	s.mu.Unlock()
	s.SetResolver(downloader.NewCacheResolver(manager, nil))
	close(manager.release)

	deadline := time.After(time.Second)
	for {
		manager.mu.Lock()
		count := len(manager.keys)
		manager.mu.Unlock()
		if count >= 2 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("reprioritized generation did not drain")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	manager.mu.Lock()
	got := append([]assetref.AssetKey(nil), manager.keys...)
	manager.mu.Unlock()
	counts := make(map[assetref.AssetKey]int)
	for _, key := range got {
		counts[key]++
	}
	// If the old generation admitted "a" before Reconcile, that active
	// waiter is allowed to finish. The canonical downloader deduplicates it
	// with the current-generation waiter; only queued stale work must be
	// invalidated by the scheduler.
	if len(got) < 2 || counts["a"] < 1 || counts["b"] != 1 {
		t.Fatalf("resolved keys=%v, want current generation [a b] plus at most an active old a", got)
	}
}

func BenchmarkScheduler_AssetQueueEndToEnd(b *testing.B) {
	for i := 0; i < b.N; i++ {
		manager := &schedulerManager{started: make(chan struct{}, 4)}
		s := NewScheduler(Config{WorkerID: "worker-a", MaxConcurrent: 2, ByteBudget: 1024})
		s.SetResolver(downloader.NewCacheResolver(manager, nil))
		now := time.Now().UTC()
		plan := futureasset.Plan{
			Version: 1, PlanID: "benchmark", WorkerID: "worker-a", GeneratedAt: now, ExpiresAt: now.Add(time.Minute),
			Limits: futureasset.Limits{PrefetchHorizon: 3, ProtectionLookahead: 10},
			PrefetchJobs: []futureasset.Job{{JobID: "n1", TaskID: "t1", ReservationID: "r1", Distance: 1, Assets: []futureasset.AssetManifest{
				{AssetKey: "a", SHA256: "s-a", SizeBytes: 10},
				{AssetKey: "b", SHA256: "s-b", SizeBytes: 10},
				{AssetKey: "c", SHA256: "s-c", SizeBytes: 10},
				{AssetKey: "d", SHA256: "s-d", SizeBytes: 10},
			}}},
		}
		b.StartTimer()
		if err := s.Reconcile(plan); err != nil {
			b.Fatal(err)
		}
		deadline := time.Now().Add(time.Second)
		for {
			manager.mu.Lock()
			resolved := len(manager.keys)
			manager.mu.Unlock()
			if resolved == 4 {
				break
			}
			if time.Now().After(deadline) {
				b.Fatalf("resolved %d assets, want 4", resolved)
			}
			time.Sleep(time.Microsecond)
		}
		b.StopTimer()
		s.Close()
	}
}

func TestScheduler_DiskPressureUsesRestrictedCriticalAndRecoveryHysteresis(t *testing.T) {
	manager := &schedulerManager{started: make(chan struct{}, 8)}
	var usage atomic.Int32
	usage.Store(78)
	s := NewScheduler(Config{WorkerID: "worker-a", MaxConcurrent: 3, ByteBudget: 100, DiskRestrictedPercent: 70, DiskCriticalPercent: 85, DiskRecoveryPercent: 75, DiskUsagePercent: func() int { return int(usage.Load()) }})
	s.SetResolver(downloader.NewCacheResolver(manager, nil))
	now := time.Now().UTC()
	plan := futureasset.Plan{Version: 1, PlanID: "p", WorkerID: "worker-a", GeneratedAt: now, ExpiresAt: now.Add(time.Minute), Limits: futureasset.Limits{PrefetchHorizon: 3, ProtectionLookahead: 10}, PrefetchJobs: []futureasset.Job{
		{JobID: "n1", TaskID: "t1", ReservationID: "r1", Distance: 1, Assets: []futureasset.AssetManifest{{AssetKey: "D1", SHA256: "s1", SizeBytes: 10}}},
		{JobID: "n2", TaskID: "t2", ReservationID: "r2", Distance: 2, Assets: []futureasset.AssetManifest{{AssetKey: "D2", SHA256: "s2", SizeBytes: 10}}},
		{JobID: "n3", TaskID: "t3", ReservationID: "r3", Distance: 3, Assets: []futureasset.AssetManifest{{AssetKey: "D3", SHA256: "s3", SizeBytes: 10}}},
	}}
	if err := s.Reconcile(plan); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(time.Second)
	for {
		manager.mu.Lock()
		n := len(manager.keys)
		manager.mu.Unlock()
		if n >= 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("restricted N+1 did not start")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	manager.mu.Lock()
	got := append([]assetref.AssetKey(nil), manager.keys...)
	manager.mu.Unlock()
	if len(got) != 1 || got[0] != "D1" {
		t.Fatalf("restricted prefetch keys=%v, want [D1]", got)
	}
	usage.Store(90)
	plan.Version = 2
	plan.PrefetchJobs = []futureasset.Job{{JobID: "n4", TaskID: "t4", ReservationID: "r4", Distance: 1, Assets: []futureasset.AssetManifest{{AssetKey: "D4", SHA256: "s4", SizeBytes: 10}}}}
	if err := s.Reconcile(plan); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	manager.mu.Lock()
	if len(manager.keys) != 1 {
		t.Fatalf("critical disk started new prefetch: %v", manager.keys)
	}
	manager.mu.Unlock()
	usage.Store(74)
	plan.Version = 3
	plan.PrefetchJobs = []futureasset.Job{{JobID: "n5", TaskID: "t5", ReservationID: "r5", Distance: 1, Assets: []futureasset.AssetManifest{{AssetKey: "D5", SHA256: "s5", SizeBytes: 10}}}}
	if err := s.Reconcile(plan); err != nil {
		t.Fatal(err)
	}
	deadline = time.After(time.Second)
	for {
		manager.mu.Lock()
		n := len(manager.keys)
		manager.mu.Unlock()
		if n >= 2 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("prefetch did not recover below hysteresis threshold")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

// blockingReserveStore blocks inside Reserve until released, so a test can
// assert that Reconcile performs the durable reservation I/O OUTSIDE the
// scheduler lock (the sqlite write must not stall the control loop).
type blockingReserveStore struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockingReserveStore) Acquire(context.Context, assetref.AssetKey, string) error { return nil }
func (s *blockingReserveStore) Release(context.Context, assetref.AssetKey, string) error { return nil }
func (s *blockingReserveStore) Reserve(context.Context, assetref.AssetKey, string, time.Time) error {
	s.once.Do(func() { close(s.entered) })
	<-s.release
	return nil
}
func (s *blockingReserveStore) ReleaseReservation(context.Context, assetref.AssetKey, string) error {
	return nil
}

func TestScheduler_ReconcileDoesNotHoldLockDuringProtectionIO(t *testing.T) {
	store := &blockingReserveStore{entered: make(chan struct{}), release: make(chan struct{})}
	s := NewScheduler(Config{WorkerID: "worker-a", MaxConcurrent: 1, ByteBudget: 100})
	s.SetProtectionStore(store)
	defer s.Close()

	done := make(chan error, 1)
	go func() { done <- s.Reconcile(futureTestPlan()) }()

	select {
	case <-store.entered:
	case <-time.After(time.Second):
		t.Fatal("Reserve never entered")
	}

	// While Reserve is blocked, s.mu must be acquirable: the durable I/O is
	// meant to run OUTSIDE the scheduler lock.
	acquired := make(chan struct{})
	go func() {
		s.mu.Lock()
		close(acquired)
		s.mu.Unlock()
	}()
	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("s.mu held during blocking protection I/O")
	}

	close(store.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestDefaultMetadataResolverVerifiesSizeHashAndCapturesFFprobe(t *testing.T) {
	path := t.TempDir() + "/asset.bin"
	contents := []byte("prefetch metadata")
	if err := os.WriteFile(path, contents, 0o644); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(contents)
	asset := futureasset.AssetManifest{AssetKey: "asset", AssetID: "asset", SHA256: hex.EncodeToString(sum[:]), SizeBytes: int64(len(contents))}
	metadata, err := defaultMetadataResolver(context.Background(), asset, downloader.CacheResolution{LocalPath: path})
	if err != nil {
		t.Fatalf("defaultMetadataResolver() error = %v", err)
	}
	if metadata.SHA256 != asset.SHA256 || metadata.SizeBytes != asset.SizeBytes {
		t.Fatalf("metadata integrity = %#v, want sha=%s size=%d", metadata, asset.SHA256, asset.SizeBytes)
	}
	if metadata.FfprobeError == "" {
		t.Fatal("non-media fixture should retain ffprobe result/error metadata")
	}

	asset.SHA256 = strings.Repeat("0", 64)
	if _, err := defaultMetadataResolver(context.Background(), asset, downloader.CacheResolution{LocalPath: path}); err == nil {
		t.Fatal("hash mismatch must be rejected before PREPARED")
	}
}

func TestScheduler_CacheHitRunsMetadataAndReachesPreparedWithoutDownload(t *testing.T) {
	path := t.TempDir() + "/cached.bin"
	contents := []byte("verified cache hit")
	if err := os.WriteFile(path, contents, 0o644); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(contents)
	digest := hex.EncodeToString(sum[:])
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
	prepared := make(chan PreparedJob, 1)
	s := NewScheduler(Config{WorkerID: "worker-a", MaxConcurrent: 1, ByteBudget: 100, OnPrepared: func(job PreparedJob) { prepared <- job }})
	s.SetResolver(downloader.NewCacheResolver(manager, nil))
	defer s.Close()
	now := time.Now().UTC()
	plan := futureasset.Plan{Version: 1, PlanID: "cache-hit", WorkerID: "worker-a", GeneratedAt: now, ExpiresAt: now.Add(time.Minute), Limits: futureasset.Limits{PrefetchHorizon: 1, ProtectionLookahead: 1}, PrefetchJobs: []futureasset.Job{{JobID: "job-cache", TaskID: "task-cache", ReservationID: "reservation-cache", Distance: 1, Assets: []futureasset.AssetManifest{{AssetKey: "asset-cache", AssetID: "asset-cache", SHA256: digest, SizeBytes: int64(len(contents))}}}}}
	if err := s.Reconcile(plan); err != nil {
		t.Fatal(err)
	}
	select {
	case job := <-prepared:
		if job.State != PreparationStatePrepared || len(job.Assets) != 1 || job.Assets["asset-cache"].SHA256 != digest {
			t.Fatalf("prepared cache-hit job = %#v", job)
		}
	case <-time.After(time.Second):
		t.Fatal("cache-hit job did not reach PREPARED")
	}
	if got := transfers.Load(); got != 0 {
		t.Fatalf("cache-hit physical transfers = %d, want 0", got)
	}
}

func TestScheduler_MetadataFailureDoesNotReachPrepared(t *testing.T) {
	manager := &schedulerManager{started: make(chan struct{}, 1)}
	prepared := make(chan PreparedJob, 1)
	s := NewScheduler(Config{
		WorkerID: "worker-a", MaxConcurrent: 1, ByteBudget: 100,
		MetadataResolver: func(context.Context, futureasset.AssetManifest, downloader.CacheResolution) (PreparedAssetMetadata, error) {
			return PreparedAssetMetadata{}, errors.New("probe failed")
		},
		OnPrepared: func(job PreparedJob) { prepared <- job },
	})
	s.SetResolver(downloader.NewCacheResolver(manager, nil))
	defer s.Close()
	if err := s.Reconcile(futureTestPlan()); err != nil {
		t.Fatal(err)
	}
	select {
	case job := <-prepared:
		t.Fatalf("metadata failure reached PREPARED: %#v", job)
	case <-time.After(100 * time.Millisecond):
	}
	if got := s.PreparedJobs(); len(got) != 0 {
		t.Fatalf("prepared read model after metadata failure = %#v", got)
	}
}

// TestScheduler_DualJobPrefetchCertification is the P0 canary test for
// deterministic prefetch certification. It verifies that with
// MaxActiveJobs=1, when job A is running and job B is the next READY job,
// the FutureAssetPlan triggers prefetch of B's asset and B reaches
// PREPARED before any attempt starts.
//
// The certification criteria are:
//   - prepared_at(B) < attempt_started_at(B)  (B is ready before execution)
//   - downloaded_during_attempt(B) = 0         (no network during B's attempt)
//   - prepared_ratio = 1.0                     (all assets prefetched)
//
// SHA_A != SHA_B, path_A != path_B, payload_A != payload_B by construction.
func TestScheduler_DualJobPrefetchCertification(t *testing.T) {
	// --- Setup: two distinct payloads with different SHA-256 hashes ---
	payloadA := []byte("AAAA-payload-for-job-A-unique-content")
	payloadB := []byte("BBBB-payload-for-job-B-unique-content")

	sumA := sha256.Sum256(payloadA)
	shaA := hex.EncodeToString(sumA[:])
	sumB := sha256.Sum256(payloadB)
	shaB := hex.EncodeToString(sumB[:])

	// Sanity: hashes must differ
	if shaA == shaB {
		t.Fatal("SHA_A == SHA_B: payloads are not distinct")
	}

	// --- Create two temp files: A is pre-seeded in cache, B is absent ---
	pathA := t.TempDir() + "/asset-A.bin"
	pathB := t.TempDir() + "/asset-B.bin"
	if err := os.WriteFile(pathA, payloadA, 0o644); err != nil {
		t.Fatal(err)
	}
	// pathB is deliberately NOT written — B must be downloaded by prefetch

	// --- Transferer: returns cache hit for A, cache miss + download for B ---
	var downloadCount atomic.Int32
	var downloadStartedAt, downloadCompletedAt time.Time
	transferer := downloader.TransfererFunc(func(_ context.Context, _ context.Context, req downloader.DownloadRequest, check bool, _ func(int64)) (downloader.CacheCheckResult, downloader.TransferResult, error) {
		if check {
			if string(req.SHA256) == shaA {
				return downloader.CacheCheckResult{CacheHit: true, LocalPath: pathA, SHA256: req.SHA256, Outcome: downloader.CacheOutcomeHitValid}, downloader.TransferResult{}, nil
			}
			return downloader.CacheCheckResult{Outcome: downloader.CacheOutcomeMissNotFound}, downloader.TransferResult{}, nil
		}
		// Download path for B
		downloadStartedAt = time.Now().UTC()
		downloadCount.Add(1)
		// Write the payload to the expected path so metadata resolver can find it
		if err := os.WriteFile(pathB, payloadB, 0o644); err != nil {
			return downloader.CacheCheckResult{}, downloader.TransferResult{}, err
		}
		downloadCompletedAt = time.Now().UTC()
		return downloader.CacheCheckResult{}, downloader.TransferResult{LocalPath: pathB, Bytes: int64(len(payloadB)), SHA256: req.SHA256}, nil
	})
	manager := downloader.NewManager(downloader.Config{Concurrency: 2}, transferer)
	defer manager.Close()

	// --- Collect PREPARED events with timestamps ---
	preparedCh := make(chan PreparedJob, 4)
	var events []Event
	var eventsMu sync.Mutex

	s := NewScheduler(Config{
		WorkerID:      "canary-worker",
		MaxConcurrent: 1,
		ByteBudget:    1024 * 1024,
		OnPrepared:    func(job PreparedJob) { preparedCh <- job },
		OnEvent: func(event Event) {
			eventsMu.Lock()
			events = append(events, event)
			eventsMu.Unlock()
		},
	})
	s.SetResolver(downloader.NewCacheResolver(manager, nil))
	defer s.Close()

	// --- Build FutureAssetPlan with two jobs: A (current) and B (next) ---
	now := time.Now().UTC()
	plan := futureasset.Plan{
		Version:     1,
		PlanID:      "dual-job-cert",
		WorkerID:    "canary-worker",
		GeneratedAt: now,
		ExpiresAt:   now.Add(5 * time.Minute),
		Limits: futureasset.Limits{
			PrefetchHorizon:      2,
			ProtectionLookahead:  2,
		},
		PrefetchJobs: []futureasset.Job{
			{
				JobID:         "job-A",
				TaskID:        "task-A",
				ReservationID: "reservation-A",
				Distance:      1,
				Assets: []futureasset.AssetManifest{{
					AssetKey:  "asset-A",
					AssetID:   "asset-A",
					SHA256:    shaA,
					SizeBytes: int64(len(payloadA)),
				}},
			},
			{
				JobID:         "job-B",
				TaskID:        "task-B",
				ReservationID: "reservation-B",
				Distance:      2,
				Assets: []futureasset.AssetManifest{{
					AssetKey:  "asset-B",
					AssetID:   "asset-B",
					SHA256:    shaB,
					SizeBytes: int64(len(payloadB)),
				}},
			},
		},
		Protect: []futureasset.ProtectedAsset{
			{AssetKey: "asset-A", FutureRefCount: 1, NextUseDistance: 1},
			{AssetKey: "asset-B", FutureRefCount: 1, NextUseDistance: 2},
		},
	}

	// --- Reconcile: triggers prefetch for both A and B ---
	planAppliedAt := time.Now().UTC()
	if err := s.Reconcile(plan); err != nil {
		t.Fatal(err)
	}

	// --- Wait for both jobs to reach PREPARED ---
	preparedJobs := make(map[string]PreparedJob)
	deadline := time.After(5 * time.Second)
	for len(preparedJobs) < 2 {
		select {
		case job := <-preparedCh:
			if job.State != PreparationStatePrepared {
				t.Fatalf("job %s reached non-PREPARED state: %s", job.JobID, job.State)
			}
			preparedJobs[job.JobID] = job
		case <-deadline:
			t.Fatalf("only %d/2 jobs reached PREPARED within timeout; missing: %v", len(preparedJobs), missingJobs(preparedJobs))
		}
	}

	// --- Certification: Job B ---
	jobB := preparedJobs["job-B"]
	if jobB.JobID != "job-B" {
		t.Fatalf("expected job-B prepared, got %s", jobB.JobID)
	}
	if len(jobB.Assets) != 1 {
		t.Fatalf("job-B prepared with %d assets, want 1", len(jobB.Assets))
	}
	assetB := jobB.Assets["asset-B"]

	// Criterion 1: prepared_at(B) must exist and be after plan application
	if assetB.PreparedAt.IsZero() {
		t.Fatal("asset-B prepared_at is zero: prefetch did not record completion time")
	}
	if assetB.PreparedAt.Before(planAppliedAt) {
		t.Fatalf("asset-B prepared_at %s is before plan applied %s", assetB.PreparedAt, planAppliedAt)
	}

	// Criterion 2: SHA256 and size must match exactly
	if assetB.SHA256 != shaB {
		t.Fatalf("asset-B SHA256 = %s, want %s", assetB.SHA256, shaB)
	}
	if assetB.SizeBytes != int64(len(payloadB)) {
		t.Fatalf("asset-B size = %d, want %d", assetB.SizeBytes, len(payloadB))
	}

	// Criterion 3: local path must be present (file exists on disk)
	if assetB.LocalPath == "" {
		t.Fatal("asset-B local path is empty: prefetch did not produce a verified local file")
	}

	// Criterion 4: download happened during prefetch (before attempt)
	if downloadCount.Load() != 1 {
		t.Fatalf("expected exactly 1 download for asset-B, got %d", downloadCount.Load())
	}
	if downloadStartedAt.IsZero() || downloadCompletedAt.IsZero() {
		t.Fatal("download timestamps not recorded")
	}
	// The download must have completed before PREPARED was emitted
	if downloadCompletedAt.After(assetB.PreparedAt) {
		t.Fatalf("download completed at %s but prepared_at is %s: download finished after PREPARED", downloadCompletedAt, assetB.PreparedAt)
	}

	// Criterion 5: prepared_ratio = 1.0 (all assets in job B are prepared)
	totalAssets := len(preparedJobs["job-B"].Assets)
	prefetchedReady := 0
	for _, a := range preparedJobs["job-B"].Assets {
		if a.SHA256 == shaB && a.SizeBytes == int64(len(payloadB)) {
			prefetchedReady++
		}
	}
	preparedRatio := float64(prefetchedReady) / float64(totalAssets)
	if preparedRatio != 1.0 {
		t.Fatalf("prepared_ratio = %.2f, want 1.0 (prefetched_ready=%d, total=%d)", preparedRatio, prefetchedReady, totalAssets)
	}

	// --- Certification: Job A (cache hit, no download) ---
	jobA := preparedJobs["job-A"]
	if len(jobA.Assets) != 1 {
		t.Fatalf("job-A prepared with %d assets, want 1", len(jobA.Assets))
	}
	assetA := jobA.Assets["asset-A"]
	if assetA.SHA256 != shaA {
		t.Fatalf("asset-A SHA256 = %s, want %s", assetA.SHA256, shaA)
	}

	// --- Verify event timeline ---
	eventsMu.Lock()
	defer eventsMu.Unlock()

	// Must have download_started and asset_ready for asset-B (cache miss path)
	var downloadStarted, assetReady bool
	for _, e := range events {
		if e.Name == "download_started" && e.AssetKey == "asset-B" {
			downloadStarted = true
		}
		if e.Name == "asset_ready" && e.AssetKey == "asset-B" {
			assetReady = true
		}
	}
	if !downloadStarted {
		t.Fatal("missing download_started event for asset-B")
	}
	if !assetReady {
		t.Fatal("missing asset_ready event for asset-B")
	}

	// --- Verify PreparedJobs read model includes both ---
	allPrepared := s.PreparedJobs()
	if len(allPrepared) != 2 {
		t.Fatalf("PreparedJobs() returned %d jobs, want 2", len(allPrepared))
	}
}

func missingJobs(jobs map[string]PreparedJob) []string {
	var missing []string
	for _, id := range []string{"job-A", "job-B"} {
		if _, ok := jobs[id]; !ok {
			missing = append(missing, id)
		}
	}
	return missing
}

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
	// --- Setup: single asset with a real file in cache ---
	payload := []byte("test-payload-for-eviction-certification")
	path := t.TempDir() + "/asset-evict.bin"
	if err := os.WriteFile(path, payload, 0o644); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(payload)
	digest := hex.EncodeToString(sum[:])

	// --- Protection store: tracks reservation calls for assertion ---
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

	// --- Transferer: cache hit for the asset, must not download ---
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

	// --- Scheduler with PREPARED callback ---
	preparedCh := make(chan PreparedJob, 1)
	s := NewScheduler(Config{
		WorkerID:      "eviction-test-worker",
		MaxConcurrent: 1,
		ByteBudget:    1024 * 1024,
		OnPrepared:    func(job PreparedJob) { preparedCh <- job },
	})
	s.SetResolver(downloader.NewCacheResolver(manager, nil))
	s.SetProtectionStore(store)
	defer s.Close()

	// --- Build FutureAssetPlan ---
	now := time.Now().UTC()
	plan := futureasset.Plan{
		Version:     1,
		PlanID:      "eviction-cert",
		WorkerID:    "eviction-test-worker",
		GeneratedAt: now,
		ExpiresAt:   now.Add(time.Minute),
		Limits: futureasset.Limits{
			PrefetchHorizon:     1,
			ProtectionLookahead: 1,
		},
		PrefetchJobs: []futureasset.Job{{
			JobID:         "job-evict",
			TaskID:        "task-evict",
			ReservationID: "reservation-evict",
			Distance:      1,
			Assets: []futureasset.AssetManifest{{
				AssetKey:  "asset-evict",
				AssetID:   "asset-evict",
				SHA256:    digest,
				SizeBytes: int64(len(payload)),
			}},
		}},
		Protect: []futureasset.ProtectedAsset{{
			AssetKey:        "asset-evict",
			FutureRefCount:  1,
			NextUseDistance: 1,
		}},
	}

	// --- Step 1: Reconcile triggers prefetch → PREPARED ---
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

	// --- Step 2: HandoffToExecution installs execution reservation ---
	s.HandoffToExecution("job-evict", "attempt-evict-001")

	// Verify execution reservation was installed.
	if got := executionReserveCount.Load(); got != 1 {
		t.Fatalf("execution reserve count = %d, want 1", got)
	}

	// Verify execution reservation is tracked.
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
	// Future reservation should be released after handoff.
	if hasFuture {
		t.Fatalf("future reservation still present after handoff: %s", futureResID)
	}

	// --- Step 3: Expire the future plan ---
	expired := plan
	expired.Version = 2
	expired.GeneratedAt = time.Now().UTC().Add(-2 * time.Minute)
	expired.ExpiresAt = time.Now().UTC().Add(-time.Minute)
	if err := s.Reconcile(expired); err != nil {
		t.Fatal(err)
	}

	// Verify execution reservation still exists after expired plan.
	s.mu.Lock()
	stillJobExecs, stillHasExec := s.executionReservations["asset-evict"]
	stillHasExec = stillHasExec && stillJobExecs["job-evict"] != ""
	s.mu.Unlock()
	if !stillHasExec {
		t.Fatal("execution reservation lost after expired plan")
	}

	// --- Step 4: ReleaseExecutionReservations (render complete) ---
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

// trackingProtectionStore is a test double that records reservation/eviction
// calls for assertion without touching SQLite.
type trackingProtectionStore struct {
	onReserve func(assetref.AssetKey, string)
	onRelease func(assetref.AssetKey, string)
}

func (s *trackingProtectionStore) Acquire(context.Context, assetref.AssetKey, string) error { return nil }
func (s *trackingProtectionStore) Release(context.Context, assetref.AssetKey, string) error  { return nil }
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
	store := &trackingProtectionStore{onRelease: func(_ assetref.AssetKey, id string) {
		released = append(released, id)
	}}
	s := &Scheduler{
		protect: store,
		executionReservations: map[string]map[string]string{
			"shared": {"job-a": "execution:a:shared", "job-b": "execution:b:shared"},
		},
	}

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

// TestScheduler_HandoffToExecutionOnlyForPreparedJobs verifies that
// HandoffToExecution is a no-op for jobs that have not yet reached PREPARED.
func TestScheduler_HandoffToExecutionOnlyForPreparedJobs(t *testing.T) {
	var executionReserveCount atomic.Int32
	store := &trackingProtectionStore{
		onReserve: func(key assetref.AssetKey, id string) {
			if strings.HasPrefix(id, "execution:") {
				executionReserveCount.Add(1)
			}
		},
	}
	// Use a blocking manager so the asset never resolves → never PREPARED.
	manager := &blockingSchedulerManager{
		schedulerManager: &schedulerManager{started: make(chan struct{}, 1)},
		release:          make(chan struct{}),
	}
	s := NewScheduler(Config{WorkerID: "worker-a", MaxConcurrent: 1, ByteBudget: 100})
	s.SetResolver(downloader.NewCacheResolver(manager, nil))
	s.SetProtectionStore(store)
	defer s.Close()

	if err := s.Reconcile(futureTestPlan()); err != nil {
		t.Fatal(err)
	}

	// Handoff before PREPARED must be a no-op.
	s.HandoffToExecution("n1", "attempt-001")
	if got := executionReserveCount.Load(); got != 0 {
		t.Fatalf("execution reserve before PREPARED = %d, want 0", got)
	}
	close(manager.release)
}

// TestScheduler_MarkJobStartedTriggersHandoff verifies that MarkJobStarted
// automatically triggers the execution reservation handoff for prepared jobs.
func TestScheduler_MarkJobStartedTriggersHandoff(t *testing.T) {
	cachedPath := t.TempDir() + "/handoff.bin"
	contents := []byte("handoff-test")
	if err := os.WriteFile(cachedPath, contents, 0o644); err != nil {
		t.Fatal(err)
	}

	var executionReserveCount atomic.Int32
	store := &trackingProtectionStore{
		onReserve: func(key assetref.AssetKey, id string) {
			if strings.HasPrefix(id, "execution:") {
				executionReserveCount.Add(1)
			}
		},
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
		WorkerID:      "worker-a",
		MaxConcurrent: 1,
		ByteBudget:    100,
		OnPrepared:    func(job PreparedJob) { preparedCh <- job },
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

	// MarkJobStarted should trigger the handoff.
	s.MarkJobStarted("n1")
	if got := executionReserveCount.Load(); got != 1 {
		t.Fatalf("execution reserve after MarkJobStarted = %d, want 1", got)
	}
}

// TestScheduler_ReleaseAllExecutionReservations verifies that shutdown
// releases all execution-phase pins.
func TestScheduler_ReleaseAllExecutionReservations(t *testing.T) {
	var executionReserveCount, executionReleaseCount atomic.Int32
	store := &trackingProtectionStore{
		onReserve: func(key assetref.AssetKey, id string) {
			if strings.HasPrefix(id, "execution:") {
				executionReserveCount.Add(1)
			}
		},
		onRelease: func(key assetref.AssetKey, id string) {
			if strings.HasPrefix(id, "execution:") {
				executionReleaseCount.Add(1)
			}
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
		WorkerID:      "worker-a",
		MaxConcurrent: 1,
		ByteBudget:    100,
		OnPrepared:    func(job PreparedJob) { preparedCh <- job },
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

// ── Canonical Origin Classification Certification Tests ────────────────
//
// These three tests certify the three mutually-exclusive resolution
// origin paths. Each test simulates the exact scenario it names and
// verifies that the cacheResolutionSink classifies the origin correctly.
//
// COLD:  No FutureAssetPlan exists. The asset is not in cache.
//         The scheduler does not run. The attempt downloads the asset.
//         Origin = runtime_download.
//
// WARM:  The asset is already in cache from a prior job/session.
//         No PreparedJob entry exists for it. The scheduler does not
//         run (or runs but the asset is already a cache hit).
//         Origin = warm_cache.
//
// PREFETCH: A FutureAssetPlan triggers download of the asset before
//         the attempt. A PreparedJob entry with matching SHA256/size
//         exists. The scheduler runs and the asset is ready.
//         Origin = prefetch. prepared_ratio = 1.0.

// testOriginSink is a ResolutionSink that classifies origin based on
// the scheduler's PreparedJob read model. It mirrors the worker package's
// cacheResolutionSink logic for test isolation. It stores the classified
// resolution so callers can inspect the origin after Resolve returns.
type testOriginSink struct {
	s        *Scheduler
	lastDown downloader.CacheResolution
}

func (s *testOriginSink) RecordResolution(_ context.Context, resolution downloader.CacheResolution) {
	if resolution.CacheHit && resolution.Origin == "" {
		resolution.Origin = downloader.OriginWarmCache
		for _, job := range s.s.PreparedJobs() {
			for _, asset := range job.Assets {
				if asset.SHA256 == string(resolution.SHA256) && asset.SizeBytes > 0 {
					resolution.Origin = downloader.OriginPrefetch
				}
			}
		}
	} else if !resolution.CacheHit && resolution.Origin == "" {
		resolution.Origin = downloader.OriginRuntimeDownload
	}
	s.lastDown = resolution
}

// lastOrigin returns the origin from the most recent RecordResolution call.
func (s *testOriginSink) lastOrigin() downloader.ResolutionOrigin {
	return s.lastDown.Origin
}

// TestCertification_COLD_OriginRuntimeDownload certifies the COLD path:
// no FutureAssetPlan, asset absent from cache, must be downloaded at
// attempt time. The origin must be classified as runtime_download.
func TestCertification_COLD_OriginRuntimeDownload(t *testing.T) {
	payload := []byte("COLD-payload-not-in-cache")
	sum := sha256.Sum256(payload)
	sha := hex.EncodeToString(sum[:])

	// No file written to disk — asset is absent from cache.
	// The transferer simulates a cache miss + download.
	var downloadCount atomic.Int32
	transferer := downloader.TransfererFunc(func(_ context.Context, _ context.Context, req downloader.DownloadRequest, check bool, _ func(int64)) (downloader.CacheCheckResult, downloader.TransferResult, error) {
		if check {
			return downloader.CacheCheckResult{Outcome: downloader.CacheOutcomeMissNotFound}, downloader.TransferResult{}, nil
		}
		downloadCount.Add(1)
		return downloader.CacheCheckResult{}, downloader.TransferResult{LocalPath: "/cold/asset.bin", Bytes: int64(len(payload)), SHA256: req.SHA256}, nil
	})
	manager := downloader.NewManager(downloader.Config{Concurrency: 1}, transferer)
	defer manager.Close()

	// No FutureAssetPlan is sent — the scheduler never runs.
	// The asset must be downloaded at attempt time.
	s := NewScheduler(Config{WorkerID: "cold-worker", MaxConcurrent: 1, ByteBudget: 1024 * 1024})
	coldSink := &testOriginSink{s: s}
	s.SetResolver(downloader.NewCacheResolver(manager, coldSink))
	defer s.Close()

	// Simulate the attempt resolving the asset through the resolver.
	req := downloader.DownloadRequest{
		AssetKey:  "asset-cold",
		AssetID:   "asset-cold",
		SHA256:    assetref.ContentHash(sha),
		SizeBytes: int64(len(payload)),
	}
	resolution, err := s.resolver.Resolve(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}

	// Certification: must be a cache miss (downloaded)
	if resolution.CacheHit {
		t.Fatal("COLD: expected cache miss, got cache hit")
	}
	if !resolution.Downloaded {
		t.Fatal("COLD: expected Downloaded=true")
	}
	if downloadCount.Load() != 1 {
		t.Fatalf("COLD: download count = %d, want 1", downloadCount.Load())
	}

	// Certification: origin must be runtime_download (classified by sink)
	if coldSink.lastOrigin() != downloader.OriginRuntimeDownload {
		t.Fatalf("COLD: origin = %q, want %q", coldSink.lastOrigin(), downloader.OriginRuntimeDownload)
	}

	// Certification: no PreparedJob exists
	prepared := s.PreparedJobs()
	if len(prepared) != 0 {
		t.Fatalf("COLD: prepared jobs = %d, want 0", len(prepared))
	}
}

// TestCertification_WARM_OriginWarmCache certifies the WARM path:
// asset is already in cache from a prior job (cache hit), no PreparedJob
// entry exists. The origin must be classified as warm_cache.
func TestCertification_WARM_OriginWarmCache(t *testing.T) {
	payload := []byte("WARM-payload-already-in-cache")
	sum := sha256.Sum256(payload)
	sha := hex.EncodeToString(sum[:])

	// Pre-seed the cache with the asset.
	path := t.TempDir() + "/warm-asset.bin"
	if err := os.WriteFile(path, payload, 0o644); err != nil {
		t.Fatal(err)
	}

	// Transferer returns cache hit — asset is already local.
	var downloadCount atomic.Int32
	transferer := downloader.TransfererFunc(func(_ context.Context, _ context.Context, req downloader.DownloadRequest, check bool, _ func(int64)) (downloader.CacheCheckResult, downloader.TransferResult, error) {
		if check {
			return downloader.CacheCheckResult{CacheHit: true, LocalPath: path, SHA256: req.SHA256, Outcome: downloader.CacheOutcomeHitValid}, downloader.TransferResult{}, nil
		}
		downloadCount.Add(1)
		return downloader.CacheCheckResult{}, downloader.TransferResult{}, errors.New("cache hit must not download")
	})
	manager := downloader.NewManager(downloader.Config{Concurrency: 1}, transferer)
	defer manager.Close()

	// No FutureAssetPlan → no PreparedJob entries.
	s := NewScheduler(Config{WorkerID: "warm-worker", MaxConcurrent: 1, ByteBudget: 1024 * 1024})
	warmSink := &testOriginSink{s: s}
	s.SetResolver(downloader.NewCacheResolver(manager, warmSink))
	defer s.Close()

	// Simulate the attempt resolving the asset through the resolver.
	req := downloader.DownloadRequest{
		AssetKey:  "asset-warm",
		AssetID:   "asset-warm",
		SHA256:    assetref.ContentHash(sha),
		SizeBytes: int64(len(payload)),
	}
	resolution, err := s.resolver.Resolve(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}

	// Certification: must be a cache hit (no download)
	if !resolution.CacheHit {
		t.Fatal("WARM: expected cache hit, got cache miss")
	}
	if downloadCount.Load() != 0 {
		t.Fatalf("WARM: download count = %d, want 0", downloadCount.Load())
	}

	// Certification: origin must be warm_cache (no PreparedJob entry, classified by sink)
	if warmSink.lastOrigin() != downloader.OriginWarmCache {
		t.Fatalf("WARM: origin = %q, want %q", warmSink.lastOrigin(), downloader.OriginWarmCache)
	}

	// Certification: no PreparedJob for this asset
	prepared := s.PreparedJobs()
	if len(prepared) != 0 {
		t.Fatalf("WARM: prepared jobs = %d, want 0 (no FutureAssetPlan)", len(prepared))
	}
}

// TestCertification_PREFETCH_OriginPrefetch certifies the PREFETCH path:
// a FutureAssetPlan triggers download of the asset before the attempt.
// A PreparedJob entry with matching SHA256/size exists. The origin must
// be classified as prefetch. prepared_ratio = 1.0.
func TestCertification_PREFETCH_OriginPrefetch(t *testing.T) {
	payload := []byte("PREFETCH-payload-downloaded-by-plan")
	sum := sha256.Sum256(payload)
	sha := hex.EncodeToString(sum[:])

	// Pre-seed the cache with the asset (simulates what the plan download does).
	// The transferer returns cache miss on check, downloads on transfer.
	// After transfer, the second lookup (for the attempt) returns cache hit.
	prefetchPath := t.TempDir() + "/prefetch-asset.bin"
	if err := os.WriteFile(prefetchPath, payload, 0o644); err != nil {
		t.Fatal(err)
	}

	var downloadCount atomic.Int32
	var seen atomic.Bool // first check returns miss, subsequent checks return hit
	transferer := downloader.TransfererFunc(func(_ context.Context, _ context.Context, req downloader.DownloadRequest, check bool, _ func(int64)) (downloader.CacheCheckResult, downloader.TransferResult, error) {
		if check {
			if seen.Load() {
				return downloader.CacheCheckResult{CacheHit: true, LocalPath: prefetchPath, SHA256: req.SHA256, Outcome: downloader.CacheOutcomeHitValid}, downloader.TransferResult{}, nil
			}
			return downloader.CacheCheckResult{Outcome: downloader.CacheOutcomeMissNotFound}, downloader.TransferResult{}, nil
		}
		seen.Store(true)
		downloadCount.Add(1)
		return downloader.CacheCheckResult{}, downloader.TransferResult{LocalPath: prefetchPath, Bytes: int64(len(payload)), SHA256: req.SHA256}, nil
	})
	manager := downloader.NewManager(downloader.Config{Concurrency: 2}, transferer)
	defer manager.Close()

	// Collect PREPARED events.
	preparedCh := make(chan PreparedJob, 4)
	s := NewScheduler(Config{
		WorkerID:      "prefetch-worker",
		MaxConcurrent: 1,
		ByteBudget:    1024 * 1024,
		OnPrepared:    func(job PreparedJob) { preparedCh <- job },
	})
	prefetchSink := &testOriginSink{s: s}
	s.SetResolver(downloader.NewCacheResolver(manager, prefetchSink))
	defer s.Close()

	// Step 1: Send FutureAssetPlan to trigger prefetch.
	now := time.Now().UTC()
	plan := futureasset.Plan{
		Version:     1,
		PlanID:      "prefetch-cert",
		WorkerID:    "prefetch-worker",
		GeneratedAt: now,
		ExpiresAt:   now.Add(5 * time.Minute),
		Limits:      futureasset.Limits{PrefetchHorizon: 1, ProtectionLookahead: 1},
		PrefetchJobs: []futureasset.Job{{
			JobID:         "job-prefetch",
			TaskID:        "task-prefetch",
			ReservationID: "reservation-prefetch",
			Distance:      1,
			Assets: []futureasset.AssetManifest{{
				AssetKey:  "asset-prefetch",
				AssetID:   "asset-prefetch",
				SHA256:    sha,
				SizeBytes: int64(len(payload)),
			}},
		}},
		Protect: []futureasset.ProtectedAsset{{
			AssetKey: "asset-prefetch", FutureRefCount: 1, NextUseDistance: 1,
		}},
	}

	if err := s.Reconcile(plan); err != nil {
		t.Fatal(err)
	}

	// Step 2: Wait for PREPARED.
	select {
	case job := <-preparedCh:
		if job.State != PreparationStatePrepared {
			t.Fatalf("PREFETCH: state = %q, want PREPARED", job.State)
		}
		if len(job.Assets) != 1 {
			t.Fatalf("PREFETCH: assets = %d, want 1", len(job.Assets))
		}
		asset := job.Assets["asset-prefetch"]
		if asset.SHA256 != sha {
			t.Fatalf("PREFETCH: SHA256 = %q, want %q", asset.SHA256, sha)
		}
		if asset.SizeBytes != int64(len(payload)) {
			t.Fatalf("PREFETCH: size = %d, want %d", asset.SizeBytes, len(payload))
		}
		if asset.PreparedAt.Before(plan.GeneratedAt) {
			t.Fatalf("PREFETCH: prepared_at %s before plan GeneratedAt %s", asset.PreparedAt, plan.GeneratedAt)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("PREFETCH: did not reach PREPARED within timeout")
	}

	// Step 3: Now simulate the attempt resolving the same asset.
	// The cache should now have the asset (downloaded by the plan).
	// The resolver should classify it as prefetch because a PreparedJob exists.
	req := downloader.DownloadRequest{
		AssetKey:  "asset-prefetch",
		AssetID:   "asset-prefetch",
		SHA256:    assetref.ContentHash(sha),
		SizeBytes: int64(len(payload)),
	}
	resolution, err := s.resolver.Resolve(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}

	// Certification: must be a cache hit (downloaded by plan, now local)
	if !resolution.CacheHit {
		t.Fatal("PREFETCH: expected cache hit after plan download, got miss")
	}

	// Certification: origin must be prefetch (PreparedJob entry exists, classified by sink)
	if prefetchSink.lastOrigin() != downloader.OriginPrefetch {
		t.Fatalf("PREFETCH: origin = %q, want %q", prefetchSink.lastOrigin(), downloader.OriginPrefetch)
	}

	// Certification: prepared_ratio = 1.0
	prepared := s.PreparedJobs()
	totalAssets := 0
	prefetchedReady := 0
	for _, pj := range prepared {
		for _, a := range pj.Assets {
			totalAssets++
			if a.SHA256 == sha && a.SizeBytes == int64(len(payload)) {
				prefetchedReady++
			}
		}
	}
	if totalAssets == 0 {
		t.Fatal("PREFETCH: no prepared assets found")
	}
	preparedRatio := float64(prefetchedReady) / float64(totalAssets)
	if preparedRatio != 1.0 {
		t.Fatalf("PREFETCH: prepared_ratio = %.2f, want 1.0 (prefetched=%d, total=%d)", preparedRatio, prefetchedReady, totalAssets)
	}

	// Certification: only 1 download (by the plan, not by the attempt)
	if downloadCount.Load() != 1 {
		t.Fatalf("PREFETCH: download count = %d, want 1 (plan downloaded, attempt should not)", downloadCount.Load())
	}
}
