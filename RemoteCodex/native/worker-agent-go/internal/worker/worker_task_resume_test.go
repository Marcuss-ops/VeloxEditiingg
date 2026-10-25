package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"velox-shared/assetref"
	"velox-shared/contract"
	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/taskrunner"
	"velox-worker-agent/internal/worker/concurrency"
	"velox-worker-agent/internal/workercache"
	"velox-worker-agent/pkg/api"
	"velox-worker-agent/pkg/config"
	"velox-worker-agent/pkg/logger"
)

// blockUntilCanceledExecutor blocks on its context so a test can observe
// whether the task survived a session teardown (it should) or was canceled
// by worker shutdown (it should).
type blockUntilCanceledExecutor struct {
	started  chan struct{}
	canceled chan struct{}
}

func (blockUntilCanceledExecutor) Descriptor() executor.Descriptor {
	return executor.Descriptor{
		ID:            "video.assemble.copy.v1",
		Version:       1,
		ResourceClass: executor.ResourceCPU,
		TemporalMode:  executor.TemporalGlobal,
		Deterministic: true,
		Cacheable:     true,
		OutputTypes:   []string{"video/mp4"},
	}
}

func resumePlanPayload(t *testing.T) map[string]interface{} {
	t.Helper()
	const (
		timelineSHA = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	)
	videoData, audioData := resumeAssets()
	videoSHA, audioSHA := resumeSHA(videoData), resumeSHA(audioData)
	plan := &contract.CompiledRenderPlanV2{
		PlanVersion: 2, TimelineRevision: 7, TimelineSHA256: timelineSHA, DurationUS: 1_000_000,
		Output: contract.OutputContractV2{
			Container: "mp4", VideoCodec: "h264", Width: 1920, Height: 1080,
			FPSNum: 24, FPSDen: 1, PixelFormat: "yuv420p", ProfileID: contract.CanonicalVideoProfileIDV1,
			CodecProfile: "high", CodecLevel: "4.0", GOPSize: 48, ClosedGOP: true,
			TimeBaseNum: 1, TimeBaseDen: 90000,
		},
		FinalAudio: contract.FinalAudioV2{
			Mode: contract.AudioModeFinalAudioCopy, AssetID: "audio", SHA256: audioSHA, SizeBytes: int64(len(audioData)),
			Codec: "aac", SampleRateHz: 48000, Channels: 2, DurationUS: 1_000_000,
			TimelineRevision: 7, TimelineSHA256: timelineSHA,
		},
		VideoTracks: []contract.VideoTrackV2{{TrackID: "main", Segments: []contract.VideoSegmentV2{{
			SegmentID: "fragment", AssetID: "video", SHA256: videoSHA,
			TimelineStartFrame: 0, FrameCount: 24, SourceDurationUS: 1_000_000,
		}}}},
		Assets: []contract.AssetRefV2{
			{AssetID: "video", SHA256: videoSHA, SizeBytes: int64(len(videoData)), Kind: "prepared_video_fragment", MIME: "video/mp4", DurationUS: 1_000_000, Width: 1920, Height: 1080, ProfileID: contract.CanonicalVideoProfileIDV1, FrameCount: 24, TimelineRevision: 7, TimelineSHA256: timelineSHA, FirstFrameKeyframe: true, ClosedGOP: true},
			{AssetID: "audio", SHA256: audioSHA, SizeBytes: int64(len(audioData)), Kind: "final_audio", MIME: "audio/mp4", DurationUS: 1_000_000},
		},
	}
	raw, err := plan.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	return map[string]interface{}{
		contract.PayloadKeyCompiledRenderPlanJSON: string(raw),
		contract.PayloadKeyCompiledRenderPlanSHA:  contract.HashCompiledPlanV2(raw),
		"video_name":                              "resume-test",
		"job_type":                                "video.render.v2",
		"job_id":                                  "job-resume-001",
		"target_executor_id":                      "video.assemble.copy.v1@1",
	}
}

func resumeAssets() ([]byte, []byte) { return []byte("resume-video"), []byte("resume-audio") }

func resumeSHA(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func (blockUntilCanceledExecutor) Validate(_ executor.TaskSpec) error { return nil }

func (e blockUntilCanceledExecutor) Execute(ctx context.Context, _ executor.ExecutionContext, _ executor.TaskSpec) (executor.ExecutionResult, error) {
	close(e.started)
	<-ctx.Done()
	close(e.canceled)
	return executor.ExecutionResult{Status: "failed", ErrorCode: "cancelled"}, ctx.Err()
}

// TestTaskSurvivesSessionTeardownButNotWorkerStop pins the PR-master-restart
// resume contract: a task dispatched through the receive loop runs under the
// worker-lifetime task context, so a transient session teardown (master
// restart) does NOT cancel the in-flight render; only worker shutdown (Stop)
// cancels it. Regression for the observed "master restart during RUNNING →
// job CANCELLED (context canceled)" gap.
func TestTaskSurvivesSessionTeardownButNotWorkerStop(t *testing.T) {
	log := logger.New(logger.InfoLevel, io.Discard)
	videoData, audioData := resumeAssets()
	assets := map[string][]byte{"video": videoData, "audio": audioData}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assetID := r.URL.Path[len("/api/v1/agent/assets/"):]
		if data, ok := assets[assetID]; ok {
			_, _ = w.Write(data)
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()
	clipCache, err := workercache.Open(filepath.Join(t.TempDir(), "resume-cache.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = clipCache.Close() })
	for assetID, body := range assets {
		path := filepath.Join(t.TempDir(), assetID+".asset")
		if err := os.WriteFile(path, body, 0o640); err != nil {
			t.Fatal(err)
		}
		if err := clipCache.Store(context.Background(), workercache.Entry{
			AssetKey: assetref.AssetKey(assetID), ContentHash: assetref.ContentHash(resumeSHA(body)),
			LocalPath: path, SizeBytes: int64(len(body)), DownloadComplete: true,
		}); err != nil {
			t.Fatal(err)
		}
	}
	reg := executor.NewRegistry()
	blocking := blockUntilCanceledExecutor{
		started:  make(chan struct{}),
		canceled: make(chan struct{}),
	}
	if err := reg.Register(blocking); err != nil {
		t.Fatalf("register blocking executor: %v", err)
	}
	rt := &recordingTransport{}

	w := &Worker{
		config: &config.WorkerConfig{
			WorkerID:        "test-worker-resume",
			WorkerName:      "test-worker-resume",
			MasterURL:       server.URL,
			WorkDir:         t.TempDir(),
			LogLevel:        "info",
			MaxActiveJobs:   1,
			ProtocolVersion: controltransport.ProtocolVersionCurrent,
		},
		logger:              log,
		apiClient:           api.NewClient(server.URL),
		clipCache:           clipCache,
		canonicalAssetCache: workercache.NewCanonicalAssetStore(clipCache),
		transport:           rt,
		status:              StatusIdle,
		stopChan:            make(chan struct{}),
		heartbeatBackoff:    &backoffConfig{initialInterval: time.Second, maxInterval: time.Minute, multiplier: 2.0},
		seenCommands:        make(map[string]time.Time),
		recentLogs:          newRecentLogBuffer(50),
		activeTasks:         make(map[string]*ActiveTaskExecution),
		taskIDsByJob:        make(map[string][]string),
		pendingTasks:        make(map[string]*PendingTaskExecution),
		activeTaskLeases:    make(map[string]*ActiveTaskLease),
		executorRegistry:    reg,
		taskRunner:          taskrunner.NewTaskRunner(reg, log),
		concurrencyLimiter:  concurrency.NewConcurrencyLimiter(1),
		version:             "test",
	}
	wireTestReporter(w, nil)

	// Simulate Start(): derive the worker-lifetime task context.
	taskBaseCtx, taskBaseCancel := context.WithCancel(context.Background())
	defer taskBaseCancel()
	w.taskBaseCtx = taskBaseCtx
	w.taskBaseCancel = taskBaseCancel

	// Session context: canceled on a transient session teardown (master
	// restart), exactly like runSession cancels sessionCtx.
	sessionCtx, sessionCancel := context.WithCancel(context.Background())
	defer sessionCancel()

	recvCh := make(chan controltransport.ControlMessage, 2)
	w.wg.Add(1)
	go w.receiveLoop(sessionCtx, recvCh)

	recvCh <- controltransport.NewTypedMessage(
		controltransport.MsgTaskOffer,
		"master",
		controltransport.ProtocolVersionCurrent,
		&pb.TaskOffer{
			TaskId:          "task-resume-001",
			JobId:           "job-resume-001",
			AttemptId:       "attempt-resume-001",
			LeaseId:         "lease-resume-001",
			AttemptNumber:   1,
			Revision:        1,
			ExecutorId:      "video.assemble.copy.v1",
			ExecutorVersion: 1,
			TaskSpec:        mustStruct(t, resumePlanPayload(t)),
		},
	)
	recvCh <- controltransport.NewTypedMessage(
		controltransport.MsgTaskLeaseGranted,
		"master",
		controltransport.ProtocolVersionCurrent,
		&pb.TaskLeaseGranted{
			TaskId:        "task-resume-001",
			JobId:         "job-resume-001",
			AttemptId:     "attempt-resume-001",
			LeaseId:       "lease-resume-001",
			AttemptNumber: 1,
			Revision:      1,
		},
	)

	// Wait for the executor to start (render in flight).
	select {
	case <-blocking.started:
	case <-time.After(5 * time.Second):
		t.Fatal("executor never started")
	}

	// Simulate a master restart: the session is torn down.
	sessionCancel()

	// The in-flight task must NOT be canceled by the session teardown.
	select {
	case <-blocking.canceled:
		t.Fatal("task was canceled by session teardown (master restart) — it must survive for the reconnect to resume/report it")
	case <-time.After(300 * time.Millisecond):
		// OK: task survived the session teardown.
	}

	// Worker shutdown (Stop) must cancel the in-flight task.
	taskBaseCancel()
	select {
	case <-blocking.canceled:
		// OK: worker shutdown canceled the task.
	case <-time.After(5 * time.Second):
		t.Fatal("task was not canceled by worker shutdown")
	}

	close(recvCh)
	w.wg.Wait()
}

// TestTaskContextFallsBackToSessionCtx pins the legacy-fixture fallback:
// hand-built workers that never went through Start() keep the historical
// session-scoped behavior instead of nil-panicking.
func TestTaskContextFallsBackToSessionCtx(t *testing.T) {
	w := &Worker{}
	sessionCtx := context.Background()
	if got := w.taskContext(sessionCtx); got != sessionCtx {
		t.Fatalf("taskContext(nil taskBaseCtx) = %v, want session ctx", got)
	}
}
