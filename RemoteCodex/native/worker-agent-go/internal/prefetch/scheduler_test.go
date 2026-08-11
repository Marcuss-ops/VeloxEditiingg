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
)

type schedulerManager struct {
	mu      sync.Mutex
	keys    []assetref.AssetKey
	started chan struct{}
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

func TestScheduler_DiskPressureUsesRestrictedCriticalAndRecoveryHysteresis(t *testing.T) {
	manager := &schedulerManager{started: make(chan struct{}, 8)}
	usage := 78
	s := NewScheduler(Config{WorkerID: "worker-a", MaxConcurrent: 3, ByteBudget: 100, DiskRestrictedPercent: 70, DiskCriticalPercent: 85, DiskRecoveryPercent: 75, DiskUsagePercent: func() int { return usage }})
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
	usage = 90
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
	usage = 74
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
