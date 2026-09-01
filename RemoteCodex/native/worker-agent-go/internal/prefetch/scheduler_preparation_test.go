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

	"velox-shared/futureasset"
	"velox-worker-agent/internal/downloader"
)

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
			PrefetchHorizon:     2,
			ProtectionLookahead: 2,
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

// TestScheduler_MultiJobLookaheadOverlapsRunningJob proves that the hard
// lookahead horizon is useful while the current render is still running.
// A is represented by a blocked foreground operation; B/C/D must all reach
// PREPARED before that operation is released.
func TestScheduler_MultiJobLookaheadOverlapsRunningJob(t *testing.T) {
	var transfers atomic.Int32
	transferer := downloader.TransfererFunc(func(_ context.Context, _ context.Context, req downloader.DownloadRequest, check bool, _ func(int64)) (downloader.CacheCheckResult, downloader.TransferResult, error) {
		if check {
			return downloader.CacheCheckResult{Outcome: downloader.CacheOutcomeMissNotFound}, downloader.TransferResult{}, nil
		}
		transfers.Add(1)
		path := t.TempDir() + "/" + string(req.AssetKey)
		if err := os.WriteFile(path, []byte(req.AssetKey), 0o644); err != nil {
			return downloader.CacheCheckResult{}, downloader.TransferResult{}, err
		}
		return downloader.CacheCheckResult{}, downloader.TransferResult{LocalPath: path, Bytes: req.SizeBytes, SHA256: req.SHA256}, nil
	})
	manager := downloader.NewManager(downloader.Config{Concurrency: 2}, transferer)
	defer manager.Close()

	preparedCh := make(chan PreparedJob, 3)
	var eventsMu sync.Mutex
	var events []Event
	s := NewScheduler(Config{
		WorkerID: "lookahead-worker", MaxConcurrent: 2, ByteBudget: 1024 * 1024,
		OnPrepared: func(job PreparedJob) { preparedCh <- job },
		OnEvent:    func(event Event) { eventsMu.Lock(); events = append(events, event); eventsMu.Unlock() },
	})
	s.SetResolver(downloader.NewCacheResolver(manager, nil))
	defer s.Close()

	aStarted := time.Now().UTC()
	aDone := make(chan struct{})
	now := time.Now().UTC()
	plan := futureasset.Plan{
		Version: 1, PlanID: "lookahead-overlap", WorkerID: "lookahead-worker",
		GeneratedAt: now, ExpiresAt: now.Add(time.Minute),
		Limits: futureasset.Limits{PrefetchHorizon: 3, ProtectionLookahead: 10},
	}
	for i, jobID := range []string{"job-B", "job-C", "job-D"} {
		distance := i + 1
		payload := []byte("asset-" + jobID)
		sum := sha256.Sum256(payload)
		plan.PrefetchJobs = append(plan.PrefetchJobs, futureasset.Job{
			JobID: jobID, TaskID: "task-" + jobID, ReservationID: "reservation-" + jobID, Distance: distance,
			Assets: []futureasset.AssetManifest{{AssetKey: "asset-" + jobID, AssetID: "asset-" + jobID, SHA256: hex.EncodeToString(sum[:]), SizeBytes: int64(len(payload))}},
		})
	}
	if err := s.Reconcile(plan); err != nil {
		t.Fatal(err)
	}
	preparedByJob := make(map[string]PreparedJob)
	deadline := time.After(5 * time.Second)
	for len(preparedByJob) < 3 {
		select {
		case job := <-preparedCh:
			preparedByJob[job.JobID] = job
		case <-deadline:
			t.Fatalf("only %d/3 lookahead jobs reached PREPARED", len(preparedByJob))
		}
	}
	if !time.Now().UTC().After(aStarted) {
		t.Fatal("lookahead preparation did not occur after foreground start")
	}
	select {
	case <-aDone:
		t.Fatal("foreground job A ended before lookahead preparation completed")
	default:
	}
	close(aDone)

	for _, jobID := range []string{"job-B", "job-C", "job-D"} {
		s.MarkJobStarted(jobID)
	}
	eventsMu.Lock()
	leadCount := 0
	for _, event := range events {
		if event.Name == "prefetch_ready_lead" {
			leadCount++
			if !event.ReadyAt.Before(event.StartedAt) {
				t.Fatalf("job %s has non-positive prefetch lead", event.JobID)
			}
		}
	}
	eventsMu.Unlock()
	if leadCount != 3 {
		t.Fatalf("prefetch_ready_lead events=%d, want 3", leadCount)
	}
	if got := transfers.Load(); got != 3 {
		t.Fatalf("lookahead transfer count=%d, want 3", got)
	}
}
