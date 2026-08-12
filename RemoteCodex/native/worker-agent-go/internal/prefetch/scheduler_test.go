package prefetch

import (
	"context"
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
	if s.reserveCalls == 1 {
		return workercache.ErrNotFound
	}
	return nil
}
func (s *deferredProtectionStore) ReleaseReservation(context.Context, assetref.AssetKey, string) error {
	s.mu.Lock()
	s.releaseCalls++
	s.mu.Unlock()
	return nil
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
	if !s.Cancel("n1") {
		t.Fatal("Cancel(n1) = false, want true")
	}
	deadline := time.After(time.Second)
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
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("resolved keys=%v, want [a b] from current generation", got)
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
