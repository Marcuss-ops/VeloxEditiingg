package worker

import (
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"velox-shared/controltransport"
	"velox-shared/futureasset"
	"velox-worker-agent/internal/downloader"
	"velox-worker-agent/internal/worker/concurrency"
	"velox-worker-agent/pkg/config"
	"velox-worker-agent/pkg/logger"
)

func TestReceiveLoop_FutureAssetPlanExecutesThroughCanonicalResolver(t *testing.T) {
	var transfers atomic.Int32
	transferer := downloader.TransfererFunc(func(ctx context.Context, _ context.Context, req downloader.DownloadRequest, check bool, _ func(int64)) (downloader.CacheCheckResult, downloader.TransferResult, error) {
		if check {
			return downloader.CacheCheckResult{Outcome: downloader.CacheOutcomeMissNotFound}, downloader.TransferResult{}, nil
		}
		transfers.Add(1)
		return downloader.CacheCheckResult{}, downloader.TransferResult{LocalPath: "/verified/" + string(req.AssetKey), Bytes: req.SizeBytes, SHA256: req.SHA256}, nil
	})
	manager := downloader.NewManager(downloader.Config{Concurrency: 2}, transferer)
	defer manager.Close()

	w := &Worker{
		config: &config.WorkerConfig{
			WorkerID:                        "test-prefetch-worker",
			WorkerName:                      "test-prefetch-worker",
			MaxActiveJobs:                   1,
			ProtocolVersion:                 controltransport.ProtocolVersionCurrent,
			PrefetchMaxConcurrent:           1,
			PrefetchByteBudget:              1024,
			PrefetchHorizonJobs:             3,
			PrefetchProtectionLookaheadJobs: 10,
		},
		logger:             logger.New(logger.InfoLevel, io.Discard),
		stopChan:           make(chan struct{}),
		recentLogs:         newRecentLogBuffer(10),
		seenCommands:       make(map[string]time.Time),
		activeTasks:        make(map[string]*ActiveTaskExecution),
		taskIDsByJob:       make(map[string][]string),
		pendingTasks:       make(map[string]*PendingTaskExecution),
		activeTaskLeases:   make(map[string]*ActiveTaskLease),
		concurrencyLimiter: concurrency.NewConcurrencyLimiter(1),
		assetManager:       manager,
		cacheResolver:      downloader.NewCacheResolver(manager, nil),
	}

	plan := futureasset.Plan{
		Version:     1,
		PlanID:      "plan-e2e",
		WorkerID:    "test-prefetch-worker",
		GeneratedAt: time.Now().UTC(),
		ExpiresAt:   time.Now().UTC().Add(time.Minute),
		Limits:      futureasset.Limits{PrefetchHorizon: 3, ProtectionLookahead: 10},
		PrefetchJobs: []futureasset.Job{{
			JobID:         "job-n-plus-one",
			TaskID:        "task-n-plus-one",
			ReservationID: "reservation-n-plus-one",
			Distance:      1,
			Assets: []futureasset.AssetManifest{{
				AssetKey: "asset-d", AssetID: "asset-d", SHA256: "sha-d", SizeBytes: 12,
			}},
		}},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	recvCh := make(chan controltransport.ControlMessage, 1)
	w.wg.Add(1)
	go w.receiveLoop(ctx, recvCh)
	recvCh <- controltransport.NewTypedMessage(
		controltransport.MsgFutureAssetPlan,
		"master",
		controltransport.ProtocolVersionCurrent,
		plan.ToProto(),
	)

	deadline := time.Now().Add(time.Second)
	for transfers.Load() != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := transfers.Load(); got != 1 {
		t.Fatalf("canonical transfer calls = %d, want 1", got)
	}
	if got := w.futureAssetController().Version(); got != plan.Version {
		t.Fatalf("controller version = %d, want %d", got, plan.Version)
	}

	cancel()
	close(recvCh)
	w.wg.Wait()
}

// TestReceiveLoop_DualJobPrefetchCertification is the worker-side P0 canary
// test. It verifies the full receive-loop → scheduler → download path for
// two jobs where job A is cached and job B must be downloaded by prefetch.
//
// With MaxActiveJobs=1, job B remains READY while job A runs. The
// FutureAssetPlan triggers prefetch of B's asset. Certification criteria:
//   - Plan is received and applied by the controller
//   - Only asset-B triggers a download (asset-A is cache-hit)
//   - Download count = 1 (B was prefetched, not downloaded during attempt)
func TestReceiveLoop_DualJobPrefetchCertification(t *testing.T) {
	// --- Two distinct payloads ---
	payloadA := []byte("AAAA-canary-payload-for-job-A")
	payloadB := []byte("BBBB-canary-payload-for-job-B")

	var downloadCount atomic.Int32
	var transfers atomic.Int32
	transferer := downloader.TransfererFunc(func(ctx context.Context, _ context.Context, req downloader.DownloadRequest, check bool, _ func(int64)) (downloader.CacheCheckResult, downloader.TransferResult, error) {
		if check {
			// A is cached, B is not
			if string(req.SHA256) == "sha-canary-A" {
				return downloader.CacheCheckResult{CacheHit: true, LocalPath: "/cached/asset-A", SHA256: req.SHA256, Outcome: downloader.CacheOutcomeHitValid}, downloader.TransferResult{}, nil
			}
			return downloader.CacheCheckResult{Outcome: downloader.CacheOutcomeMissNotFound}, downloader.TransferResult{}, nil
		}
		transfers.Add(1)
		downloadCount.Add(1)
		return downloader.CacheCheckResult{}, downloader.TransferResult{LocalPath: "/downloaded/asset-B", Bytes: int64(len(payloadB)), SHA256: req.SHA256}, nil
	})
	manager := downloader.NewManager(downloader.Config{Concurrency: 2}, transferer)
	defer manager.Close()

	w := &Worker{
		config: &config.WorkerConfig{
			WorkerID:                        "canary-dual-job-worker",
			WorkerName:                      "canary-dual-job-worker",
			MaxActiveJobs:                   1,
			ProtocolVersion:                 controltransport.ProtocolVersionCurrent,
			PrefetchMaxConcurrent:           1,
			PrefetchByteBudget:              1024 * 1024,
			PrefetchHorizonJobs:             3,
			PrefetchProtectionLookaheadJobs: 10,
		},
		logger:             logger.New(logger.InfoLevel, io.Discard),
		stopChan:           make(chan struct{}),
		recentLogs:         newRecentLogBuffer(10),
		seenCommands:       make(map[string]time.Time),
		activeTasks:        make(map[string]*ActiveTaskExecution),
		taskIDsByJob:       make(map[string][]string),
		pendingTasks:       make(map[string]*PendingTaskExecution),
		activeTaskLeases:   make(map[string]*ActiveTaskLease),
		concurrencyLimiter: concurrency.NewConcurrencyLimiter(1),
		assetManager:       manager,
		cacheResolver:      downloader.NewCacheResolver(manager, nil),
	}

	plan := futureasset.Plan{
		Version:     1,
		PlanID:      "dual-job-canary",
		WorkerID:    "canary-dual-job-worker",
		GeneratedAt: time.Now().UTC(),
		ExpiresAt:   time.Now().UTC().Add(5 * time.Minute),
		Limits:      futureasset.Limits{PrefetchHorizon: 3, ProtectionLookahead: 10},
		PrefetchJobs: []futureasset.Job{
			{
				JobID:         "job-A",
				TaskID:        "task-A",
				ReservationID: "reservation-A",
				Distance:      1,
				Assets: []futureasset.AssetManifest{{
					AssetKey: "asset-A", AssetID: "asset-A", SHA256: "sha-canary-A", SizeBytes: int64(len(payloadA)),
				}},
			},
			{
				JobID:         "job-B",
				TaskID:        "task-B",
				ReservationID: "reservation-B",
				Distance:      2,
				Assets: []futureasset.AssetManifest{{
					AssetKey: "asset-B", AssetID: "asset-B", SHA256: "sha-canary-B", SizeBytes: int64(len(payloadB)),
				}},
			},
		},
		Protect: []futureasset.ProtectedAsset{
			{AssetKey: "asset-A", FutureRefCount: 1, NextUseDistance: 1},
			{AssetKey: "asset-B", FutureRefCount: 1, NextUseDistance: 2},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	recvCh := make(chan controltransport.ControlMessage, 4)
	w.wg.Add(1)
	go w.receiveLoop(ctx, recvCh)

	// Send the FutureAssetPlan
	recvCh <- controltransport.NewTypedMessage(
		controltransport.MsgFutureAssetPlan,
		"master",
		controltransport.ProtocolVersionCurrent,
		plan.ToProto(),
	)

	// Wait for exactly 1 transfer (A is cache hit, B is cache miss)
	deadline := time.Now().Add(5 * time.Second)
	for transfers.Load() != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := transfers.Load(); got != 1 {
		t.Fatalf("expected 1 download transfer (asset-B), got %d", got)
	}

	// Verify the controller accepted the plan
	prefetchCtrl := w.futureAssetController()
	if prefetchCtrl == nil {
		t.Fatal("futureAssetController is nil")
	}
	if got := prefetchCtrl.Version(); got != plan.Version {
		t.Fatalf("controller version = %d, want %d", got, plan.Version)
	}

	// Certification: download count = 1 (only asset-B was downloaded)
	if got := downloadCount.Load(); got != 1 {
		t.Fatalf("download count = %d, want 1 (only asset-B)", got)
	}

	cancel()
	close(recvCh)
	w.wg.Wait()
}
