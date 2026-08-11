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
