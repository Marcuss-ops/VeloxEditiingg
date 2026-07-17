package creatorflow

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"velox-server/internal/jobs"
	"velox-server/internal/routing"
	"velox-server/internal/store"
	"velox-server/internal/taskgraph"
)

// ── Fakes ───────────────────────────────────────────────────────────────────

type fakeJobLookup struct {
	jobs map[string]*jobs.Job
}

func (f *fakeJobLookup) Get(ctx context.Context, id string) (*jobs.Job, error) {
	if f == nil {
		return nil, nil
	}
	if j, ok := f.jobs[id]; ok {
		return j, nil
	}
	return nil, nil
}

type fakeForwardingRepo struct {
	bySource map[string]*store.CreatorForwarding

	getSourceErr       error
	insertErr          error
	upsertErr          error
	markReadyErr       error
	ensureForwardedErr error

	getSourceCalls       []string
	insertCalls          []*store.CreatorForwarding
	upsertCalls          []string
	upsertPayloads       []string
	upsertHashes         []string
	markReadyCalls       []string
	markReadyPayloads    []string
	markReadyHashes      []string
	ensureForwardedCalls []string
}

func keyForSource(provider, sourceJobID, targetExecutorID string) string {
	return fmt.Sprintf("%s:%s:%s", provider, sourceJobID, targetExecutorID)
}

func (f *fakeForwardingRepo) GetCreatorForwardingBySource(ctx context.Context, provider, sourceJobID, targetExecutorID string) (*store.CreatorForwarding, error) {
	f.getSourceCalls = append(f.getSourceCalls, keyForSource(provider, sourceJobID, targetExecutorID))
	if f.getSourceErr != nil {
		return nil, f.getSourceErr
	}
	if f.bySource == nil {
		return nil, nil
	}
	return f.bySource[keyForSource(provider, sourceJobID, targetExecutorID)], nil
}

func (f *fakeForwardingRepo) InsertCreatorForwarding(ctx context.Context, cf *store.CreatorForwarding) (*store.InsertCreatorForwardingResult, error) {
	f.insertCalls = append(f.insertCalls, cf)
	if f.insertErr != nil {
		return nil, f.insertErr
	}
	return &store.InsertCreatorForwardingResult{Created: true, Forwarding: cf}, nil
}

func (f *fakeForwardingRepo) UpsertCreatorForwardingPayload(ctx context.Context, forwardingID, payloadJSON, payloadSHA256 string) error {
	f.upsertCalls = append(f.upsertCalls, forwardingID)
	f.upsertPayloads = append(f.upsertPayloads, payloadJSON)
	f.upsertHashes = append(f.upsertHashes, payloadSHA256)
	return f.upsertErr
}

func (f *fakeForwardingRepo) MarkCreatorForwardingReadySync(ctx context.Context, forwardingID, payloadJSON, payloadSHA256 string) error {
	f.markReadyCalls = append(f.markReadyCalls, forwardingID)
	f.markReadyPayloads = append(f.markReadyPayloads, payloadJSON)
	f.markReadyHashes = append(f.markReadyHashes, payloadSHA256)
	return f.markReadyErr
}

func (f *fakeForwardingRepo) EnsureForwarded(ctx context.Context, forwardingID, jobID string) error {
	f.ensureForwardedCalls = append(f.ensureForwardedCalls, fmt.Sprintf("%s:%s", forwardingID, jobID))
	return f.ensureForwardedErr
}

func (f *fakeForwardingRepo) AtomicForwardAndEnqueue(ctx context.Context, forwardingID string, job *jobs.Job, spec *taskgraph.TaskSpec, priority int) error {
	return nil
}

// ── resolver_payload.go ─────────────────────────────────────────────────────

func TestSha256HexResolver_KnownInput(t *testing.T) {
	got := sha256HexResolver([]byte("test"))
	want := "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
	if got != want {
		t.Fatalf("sha256HexResolver(\"test\") = %q, want %q", got, want)
	}
}

func TestResolverMarshalPayload(t *testing.T) {
	t.Run("nil input", func(t *testing.T) {
		jsonStr, hash := resolverMarshalPayload(nil)
		if jsonStr != "{}" {
			t.Fatalf("want JSON {}, got %q", jsonStr)
		}
		if hash == "" {
			t.Fatalf("want non-empty hash for empty payload")
		}
	})

	t.Run("simple map", func(t *testing.T) {
		jsonStr, hash := resolverMarshalPayload(map[string]interface{}{"ok": true})
		if jsonStr != `{"ok":true}` {
			t.Fatalf("want JSON {\"ok\":true}, got %q", jsonStr)
		}
		if hash == "" {
			t.Fatalf("want non-empty hash")
		}
	})

	t.Run("non-serializable value", func(t *testing.T) {
		jsonStr, hash := resolverMarshalPayload(map[string]interface{}{"bad": make(chan int)})
		if jsonStr != "" || hash != "" {
			t.Fatalf("want empty strings on marshal error, got %q %q", jsonStr, hash)
		}
	})
}

func TestBuildAndRewritePayload_InjectsForwardingKey(t *testing.T) {
	r := &Resolver{}
	reqPayload := map[string]interface{}{
		"status": "completed",
		"result": map[string]interface{}{
			"title":          "Test Video",
			"script_text":    "script content",
			"scenes_json":    `[{"text":"Scene 1","image_link":"https://example.com/scene.png"}]`,
			"voiceover_path": "https://example.com/voice.mp3",
		},
	}
	fwdKey := routing.FormatForwardingKey("remote_engine", "job-123", "scene.composite.v1")

	workerPayload, err := r.buildAndRewritePayload(reqPayload, fwdKey)
	if err != nil {
		t.Fatalf("buildAndRewritePayload: %v", err)
	}

	got, ok := workerPayload[routing.KeyForwardingKey].(string)
	if !ok || got != fwdKey.String() {
		t.Fatalf("forwarding key not injected correctly: got %v", workerPayload[routing.KeyForwardingKey])
	}
	if workerPayload["video_name"] != "Test Video" {
		t.Fatalf("want video_name from BuildPipelinePayload, got %v", workerPayload["video_name"])
	}
	if workerPayload["source"] != "pipeline_generate_with_images" {
		t.Fatalf("want source from BuildPipelinePayload, got %v", workerPayload["source"])
	}
	if _, ok := workerPayload["voiceover_paths"]; !ok {
		t.Fatalf("want voiceover_paths in worker payload")
	}
}

func TestBuildAndRewritePayload_BuildPipelinePayloadError(t *testing.T) {
	r := &Resolver{}
	// Missing voiceover_path triggers an error in BuildPipelinePayload.
	reqPayload := map[string]interface{}{
		"status": "completed",
		"result": map[string]interface{}{
			"title":       "Test Video",
			"script_text": "script content",
			"scenes_json": `[{"text":"Scene 1"}]`,
		},
	}
	fwdKey := routing.FormatForwardingKey("remote_engine", "job-123", "scene.composite.v1")
	_, err := r.buildAndRewritePayload(reqPayload, fwdKey)
	if err == nil {
		t.Fatalf("expected error when pipeline payload cannot be built")
	}
}

func TestBuildAndRewritePayload_URLRewriteBranch(t *testing.T) {
	dataDir := t.TempDir()
	r := &Resolver{dataDir: dataDir, videosDir: dataDir, masterURL: "http://master.test"}
	reqPayload := map[string]interface{}{
		"status": "completed",
		"result": map[string]interface{}{
			"title":           "Test Video",
			"script_text":     "script content",
			"scenes_json":     `[{"text":"Scene 1","image_link":"https://example.com/scene.png"}]`,
			"voiceover_paths": []string{"https://example.com/voice.mp3"},
			"total_duration_secs": 10.0,
		},
	}
	fwdKey := routing.FormatForwardingKey("remote_engine", "job-123", "scene.composite.v1")
	workerPayload, err := r.buildAndRewritePayload(reqPayload, fwdKey)
	if err != nil {
		t.Fatalf("buildAndRewritePayload: %v", err)
	}
	got, ok := workerPayload[routing.KeyForwardingKey].(string)
	if !ok || got != fwdKey.String() {
		t.Fatalf("forwarding key not injected correctly: got %v", workerPayload[routing.KeyForwardingKey])
	}
	if workerPayload["video_name"] != "Test Video" {
		t.Fatalf("want video_name from BuildPipelinePayload, got %v", workerPayload["video_name"])
	}
}

// ── resolver_idempotency.go ─────────────────────────────────────────────────

func TestBuildIdempotentResolveResponse(t *testing.T) {
	t.Run("with run id", func(t *testing.T) {
		job := &jobs.Job{
			ID:     "job-abc",
			Status: jobs.StatusPending,
			RunID:  "run-xyz",
		}
		resp := buildIdempotentResolveResponse(job)

		if resp["ok"] != true {
			t.Fatalf("want ok=true, got %v", resp["ok"])
		}
		if resp["created"] != false {
			t.Fatalf("want created=false, got %v", resp["created"])
		}
		if resp["job_id"] != "job-abc" {
			t.Fatalf("want job_id=job-abc, got %v", resp["job_id"])
		}
		if resp["status"] != "PENDING" {
			t.Fatalf("want status=PENDING, got %v", resp["status"])
		}
		if resp["job_run_id"] != "run-xyz" || resp["run_id"] != "run-xyz" {
			t.Fatalf("run ids not set correctly: %+v", resp)
		}
	})

	t.Run("without run id", func(t *testing.T) {
		job := &jobs.Job{ID: "job-no-run", Status: jobs.StatusSucceeded}
		resp := buildIdempotentResolveResponse(job)
		if resp["job_run_id"] != nil || resp["run_id"] != nil {
			t.Fatalf("run ids should be absent when RunID is empty, got %+v", resp)
		}
	})
}

func TestBuildFreshResolveResponse(t *testing.T) {
	t.Run("with run id", func(t *testing.T) {
		job := &jobs.Job{
			ID:     "job-fresh",
			Status: jobs.StatusPending,
			RunID:  "run-fresh",
		}
		resp := buildFreshResolveResponse(job)

		if resp["ok"] != true {
			t.Fatalf("want ok=true, got %v", resp["ok"])
		}
		if resp["created"] != true {
			t.Fatalf("want created=true, got %v", resp["created"])
		}
		if resp["job_id"] != "job-fresh" {
			t.Fatalf("want job_id=job-fresh, got %v", resp["job_id"])
		}
		if resp["status"] != "PENDING" {
			t.Fatalf("want status=PENDING, got %v", resp["status"])
		}
		if resp["job_run_id"] != "run-fresh" || resp["run_id"] != "run-fresh" {
			t.Fatalf("run ids not set correctly: %+v", resp)
		}
	})

	t.Run("without run id", func(t *testing.T) {
		job := &jobs.Job{ID: "job-no-run", Status: jobs.StatusPending}
		resp := buildFreshResolveResponse(job)
		if resp["job_run_id"] != nil || resp["run_id"] != nil {
			t.Fatalf("run ids should be absent when RunID is empty, got %+v", resp)
		}
	})
}

func TestCheckIdempotencyFastPath(t *testing.T) {
	t.Run("job miss", func(t *testing.T) {
		r := &Resolver{jobLookup: &fakeJobLookup{}}
		out, hit := r.checkIdempotencyFastPath(context.Background(), ResolveRequest{}, "job-1", "scene.composite.v1")
		if hit || out != nil {
			t.Fatalf("expected miss (nil, false), got (%+v, %v)", out, hit)
		}
	})

	t.Run("job hit with explicit forwarding id", func(t *testing.T) {
		repo := &fakeForwardingRepo{}
		r := &Resolver{
			jobLookup:   &fakeJobLookup{jobs: map[string]*jobs.Job{"job-1": {ID: "job-1", Status: jobs.StatusPending}}},
			forwardRepo: repo,
		}
		out, hit := r.checkIdempotencyFastPath(context.Background(), ResolveRequest{ForwardingID: "cf-1"}, "job-1", "scene.composite.v1")
		if !hit || out == nil {
			t.Fatalf("expected hit, got (%+v, %v)", out, hit)
		}
		if len(repo.ensureForwardedCalls) != 1 {
			t.Fatalf("expected EnsureForwarded call, got %v", repo.ensureForwardedCalls)
		}
	})

	t.Run("job hit with ensure forwarded error still returns output", func(t *testing.T) {
		repo := &fakeForwardingRepo{ensureForwardedErr: errors.New("transition conflict")}
		r := &Resolver{
			jobLookup:   &fakeJobLookup{jobs: map[string]*jobs.Job{"job-1": {ID: "job-1", Status: jobs.StatusPending}}},
			forwardRepo: repo,
		}
		out, hit := r.checkIdempotencyFastPath(context.Background(), ResolveRequest{ForwardingID: "cf-1"}, "job-1", "scene.composite.v1")
		if !hit || out == nil {
			t.Fatalf("expected hit despite repair error, got (%+v, %v)", out, hit)
		}
	})

	t.Run("job hit looks up forwarding by source", func(t *testing.T) {
		repo := &fakeForwardingRepo{
			bySource: map[string]*store.CreatorForwarding{
				keyForSource("remote_engine", "src-1", "scene.composite.v1"): {ForwardingID: "cf-2"},
			},
		}
		r := &Resolver{
			jobLookup:   &fakeJobLookup{jobs: map[string]*jobs.Job{"job-1": {ID: "job-1", Status: jobs.StatusPending}}},
			forwardRepo: repo,
		}
		req := ResolveRequest{SourceProvider: "remote_engine", SourceJobID: "src-1"}
		out, hit := r.checkIdempotencyFastPath(context.Background(), req, "job-1", "scene.composite.v1")
		if !hit || out == nil {
			t.Fatalf("expected hit, got (%+v, %v)", out, hit)
		}
		if out.ForwardingID != "cf-2" {
			t.Fatalf("want forwarding id cf-2, got %s", out.ForwardingID)
		}
		if len(repo.ensureForwardedCalls) != 1 {
			t.Fatalf("expected EnsureForwarded call, got %v", repo.ensureForwardedCalls)
		}
	})

	t.Run("job hit lookup error still returns output", func(t *testing.T) {
		repo := &fakeForwardingRepo{getSourceErr: errors.New("lookup failed")}
		r := &Resolver{
			jobLookup:   &fakeJobLookup{jobs: map[string]*jobs.Job{"job-1": {ID: "job-1", Status: jobs.StatusPending}}},
			forwardRepo: repo,
		}
		req := ResolveRequest{SourceProvider: "remote_engine", SourceJobID: "src-1"}
		out, hit := r.checkIdempotencyFastPath(context.Background(), req, "job-1", "scene.composite.v1")
		if !hit || out == nil {
			t.Fatalf("expected hit despite lookup error, got (%+v, %v)", out, hit)
		}
		if len(repo.ensureForwardedCalls) != 0 {
			t.Fatalf("expected no EnsureForwarded call when forwarding id unknown, got %v", repo.ensureForwardedCalls)
		}
	})
}

// ── resolver_forwarding.go ───────────────────────────────────────────────────

func TestPersistPendingRemoteForwarding(t *testing.T) {
	t.Run("missing source provider", func(t *testing.T) {
		r := &Resolver{forwardRepo: &fakeForwardingRepo{}}
		_, err := r.PersistPendingRemoteForwarding(context.Background(), "", "job-1", "scene.composite.v1")
		if err == nil {
			t.Fatalf("expected error for empty source provider")
		}
	})

	t.Run("returns existing forwarding", func(t *testing.T) {
		repo := &fakeForwardingRepo{
			bySource: map[string]*store.CreatorForwarding{
				keyForSource("remote_engine", "job-1", "scene.composite.v1"): {ForwardingID: "cf-existing"},
			},
		}
		r := &Resolver{forwardRepo: repo}
		cf, err := r.PersistPendingRemoteForwarding(context.Background(), "remote_engine", "job-1", "scene.composite.v1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cf.ForwardingID != "cf-existing" {
			t.Fatalf("want cf-existing, got %s", cf.ForwardingID)
		}
		if len(repo.insertCalls) != 0 {
			t.Fatalf("expected no insert, got %d", len(repo.insertCalls))
		}
		wantKey := keyForSource("remote_engine", "job-1", "scene.composite.v1")
		if len(repo.getSourceCalls) != 1 || repo.getSourceCalls[0] != wantKey {
			t.Fatalf("expected lookup by source %s, got %v", wantKey, repo.getSourceCalls)
		}
	})

	t.Run("lookup error other than no row", func(t *testing.T) {
		repo := &fakeForwardingRepo{getSourceErr: errors.New("db down")}
		r := &Resolver{forwardRepo: repo}
		_, err := r.PersistPendingRemoteForwarding(context.Background(), "remote_engine", "job-1", "scene.composite.v1")
		if err == nil {
			t.Fatalf("expected error from lookup failure")
		}
	})

	t.Run("creates new forwarding with default executor", func(t *testing.T) {
		repo := &fakeForwardingRepo{}
		r := &Resolver{forwardRepo: repo}
		cf, err := r.PersistPendingRemoteForwarding(context.Background(), "remote_engine", "job-new", "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cf.ForwardingID == "" {
			t.Fatalf("want non-empty forwarding id")
		}
		if cf.TargetExecutorID != "scene.composite.v1" {
			t.Fatalf("want default executor scene.composite.v1, got %s", cf.TargetExecutorID)
		}
		if len(repo.insertCalls) != 1 {
			t.Fatalf("expected one insert, got %d", len(repo.insertCalls))
		}
		if repo.insertCalls[0].SourceProvider != "remote_engine" || repo.insertCalls[0].SourceJobID != "job-new" {
			t.Fatalf("insert called with wrong source: %+v", repo.insertCalls[0])
		}
	})

	t.Run("insert error", func(t *testing.T) {
		repo := &fakeForwardingRepo{insertErr: errors.New("insert failed")}
		r := &Resolver{forwardRepo: repo}
		_, err := r.PersistPendingRemoteForwarding(context.Background(), "remote_engine", "job-new", "")
		if err == nil {
			t.Fatalf("expected error from insert failure")
		}
	})
}

func TestEnsureReadyForwarding(t *testing.T) {
	t.Run("runner path upserts payload", func(t *testing.T) {
		repo := &fakeForwardingRepo{}
		r := &Resolver{forwardRepo: repo}
		req := ResolveRequest{ForwardingID: "cf-runner"}
		forwardingID, err := r.ensureReadyForwarding(context.Background(), req, "scene.composite.v1", map[string]interface{}{"ok": true})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if forwardingID != "cf-runner" {
			t.Fatalf("want cf-runner, got %s", forwardingID)
		}
		if len(repo.upsertCalls) != 1 {
			t.Fatalf("expected one upsert, got %v", repo.upsertCalls)
		}
		if len(repo.insertCalls) != 0 {
			t.Fatalf("expected no insert on runner path")
		}
		if repo.upsertPayloads[0] == "" || repo.upsertHashes[0] == "" {
			t.Fatalf("expected non-empty payload and hash in upsert, got %q / %q", repo.upsertPayloads[0], repo.upsertHashes[0])
		}
	})

	t.Run("handler path inserts and promotes", func(t *testing.T) {
		repo := &fakeForwardingRepo{}
		r := &Resolver{forwardRepo: repo}
		req := ResolveRequest{SourceProvider: "remote_engine", SourceJobID: "job-1"}
		forwardingID, err := r.ensureReadyForwarding(context.Background(), req, "scene.composite.v1", map[string]interface{}{"ok": true})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if forwardingID == "" {
			t.Fatalf("want non-empty forwarding id")
		}
		if len(repo.insertCalls) != 1 {
			t.Fatalf("expected one insert, got %d", len(repo.insertCalls))
		}
		if len(repo.markReadyCalls) != 1 {
			t.Fatalf("expected one mark ready call, got %v", repo.markReadyCalls)
		}
		if repo.markReadyPayloads[0] == "" || repo.markReadyHashes[0] == "" {
			t.Fatalf("expected non-empty payload and hash in mark ready, got %q / %q", repo.markReadyPayloads[0], repo.markReadyHashes[0])
		}
	})

	t.Run("non-serializable payload returns error", func(t *testing.T) {
		repo := &fakeForwardingRepo{}
		r := &Resolver{forwardRepo: repo}
		req := ResolveRequest{SourceProvider: "remote_engine", SourceJobID: "job-1"}
		_, err := r.ensureReadyForwarding(context.Background(), req, "scene.composite.v1", map[string]interface{}{"bad": make(chan int)})
		if err == nil {
			t.Fatalf("expected error for non-serializable payload")
		}
	})
}
