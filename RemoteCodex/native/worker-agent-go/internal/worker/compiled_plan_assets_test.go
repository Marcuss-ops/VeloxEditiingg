package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"velox-shared/assetref"
	"velox-shared/contract"
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/runtimeassets"
	"velox-worker-agent/internal/taskrunner"
	"velox-worker-agent/internal/workercache"
	"velox-worker-agent/pkg/api"
	"velox-worker-agent/pkg/config"
	"velox-worker-agent/pkg/logger"
)

func TestResolveCompiledRenderPlanAssets_UsesCommonResolverForEveryAsset(t *testing.T) {
	assets := map[string][]byte{
		"v2-video": []byte("prepared-video-bytes"),
		"v2-audio": []byte("final-audio-bytes"),
	}
	var mu sync.Mutex
	requests := make(map[string]int)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assetID := strings.TrimPrefix(r.URL.Path, "/api/v1/agent/assets/")
		mu.Lock()
		requests[assetID]++
		mu.Unlock()
		body, ok := assets[assetID]
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}))
	defer server.Close()

	workerDir := t.TempDir()
	w := &Worker{
		config:    &config.WorkerConfig{WorkerID: "worker-v2-assets", MasterURL: server.URL, WorkDir: workerDir},
		apiClient: api.NewClient(server.URL),
	}
	payload := compiledPlanAssetPayload(t, assets)
	canonicalBefore := payload[contract.PayloadKeyCompiledRenderPlanJSON].(string)

	bindings, err := w.resolveCompiledRenderPlanAssets(context.Background(), payload)
	if err != nil {
		t.Fatalf("resolve V2 assets: %v", err)
	}
	if len(bindings) != len(assets) {
		t.Fatalf("bindings count = %d, want %d", len(bindings), len(assets))
	}
	for assetID, body := range assets {
		binding, ok := bindings[assetID]
		if !ok {
			t.Fatalf("missing binding for %s", assetID)
		}
		if binding.Path == "" || !filepath.IsAbs(binding.Path) || strings.Contains(binding.Path, "velox-asset://") {
			t.Fatalf("binding %s path = %q, want verified local path", assetID, binding.Path)
		}
		if binding.SHA256 != assetSHA(body) || binding.Size != int64(len(body)) {
			t.Fatalf("binding %s metadata = sha:%q size:%d", assetID, binding.SHA256, binding.Size)
		}
		got, err := os.ReadFile(binding.Path)
		if err != nil || string(got) != string(body) {
			t.Fatalf("binding %s bytes = %q, err=%v", assetID, got, err)
		}
	}
	for assetID := range assets {
		if requests[assetID] != 1 {
			t.Errorf("resolver request count for %s = %d, want 1", assetID, requests[assetID])
		}
	}

	if got := payload[contract.PayloadKeyCompiledRenderPlanJSON].(string); got != canonicalBefore {
		t.Fatalf("canonical plan JSON mutated during resolution")
	}
	if strings.Contains(canonicalBefore, workerDir) || strings.Contains(canonicalBefore, "local_path") {
		t.Fatal("canonical V2 plan contains a local path after resolution")
	}
}

func TestResolveCompiledRenderPlanAssets_UsesBoundedConcurrency(t *testing.T) {
	assets := map[string][]byte{
		"v2-video": []byte("asset-video"),
		"v2-audio": []byte("asset-audio"),
	}
	var mu sync.Mutex
	active, maxActive := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assetID := strings.TrimPrefix(r.URL.Path, "/api/v1/agent/assets/")
		mu.Lock()
		active++
		if active > maxActive {
			maxActive = active
		}
		mu.Unlock()
		defer func() {
			mu.Lock()
			active--
			mu.Unlock()
		}()
		time.Sleep(100 * time.Millisecond)
		body, ok := assets[assetID]
		if !ok {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write(body)
	}))
	defer server.Close()

	w := &Worker{
		config:    &config.WorkerConfig{WorkerID: "worker-v2-concurrency", MasterURL: server.URL, WorkDir: t.TempDir()},
		apiClient: api.NewClient(server.URL),
	}
	started := time.Now()
	bindings, err := w.resolveCompiledRenderPlanAssets(context.Background(), compiledPlanAssetPayload(t, assets))
	if err != nil {
		t.Fatalf("resolve V2 assets: %v", err)
	}
	if len(bindings) != len(assets) {
		t.Fatalf("bindings count = %d, want %d", len(bindings), len(assets))
	}
	mu.Lock()
	gotMaxActive := maxActive
	mu.Unlock()
	if gotMaxActive < 2 {
		t.Fatalf("maximum concurrent asset requests = %d, want at least 2", gotMaxActive)
	}
	if elapsed := time.Since(started); elapsed >= 180*time.Millisecond {
		t.Fatalf("asset resolution took %s; expected bounded parallel resolution", elapsed)
	}
}

type contextBindingProbeExecutor struct {
	descriptor executor.Descriptor
	seen       runtimeassets.Bindings
}

func (e *contextBindingProbeExecutor) Descriptor() executor.Descriptor  { return e.descriptor }
func (e *contextBindingProbeExecutor) Validate(executor.TaskSpec) error { return nil }
func (e *contextBindingProbeExecutor) Execute(ctx context.Context, _ executor.ExecutionContext, _ executor.TaskSpec) (executor.ExecutionResult, error) {
	bindings, ok := runtimeassets.FromContext(ctx)
	if !ok {
		return executor.ExecutionResult{Status: "failed", ErrorCode: "BINDINGS_NOT_ATTACHED", ErrorDetail: "runtime bindings missing"}, nil
	}
	e.seen = bindings
	return executor.ExecutionResult{Status: "succeeded"}, nil
}

func TestDispatchTaskRunner_AttachesV2BindingsToExecutionContext(t *testing.T) {
	assets := map[string][]byte{
		"v2-video": []byte("prepared-video-bytes"),
		"v2-audio": []byte("final-audio-bytes"),
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assetID := strings.TrimPrefix(r.URL.Path, "/api/v1/agent/assets/")
		body, ok := assets[assetID]
		if !ok {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write(body)
	}))
	defer server.Close()

	probe := &contextBindingProbeExecutor{descriptor: executor.Descriptor{
		ID: "render_batch", Version: 1, ResourceClass: executor.ResourceCPU, TemporalMode: executor.TemporalGlobal,
	}}
	registry := executor.NewRegistry()
	registry.MustRegister(probe)
	log := logger.New(logger.InfoLevel, os.Stderr)
	w := &Worker{
		config:      &config.WorkerConfig{WorkerID: "worker-dispatch-bindings", MasterURL: server.URL, WorkDir: t.TempDir()},
		apiClient:   api.NewClient(server.URL),
		logger:      log,
		activeTasks: make(map[string]*ActiveTaskExecution),
		taskRunner:  taskrunner.NewTaskRunner(registry, log),
	}
	cache, err := workercache.Open(filepath.Join(t.TempDir(), "dispatch-cache.db"))
	if err != nil {
		t.Fatalf("open dispatch cache: %v", err)
	}
	t.Cleanup(func() { _ = cache.Close() })
	for assetID, body := range assets {
		path := filepath.Join(t.TempDir(), assetID+".asset")
		if err := os.WriteFile(path, body, 0o640); err != nil {
			t.Fatalf("write dispatch asset %s: %v", assetID, err)
		}
		if err := cache.Store(context.Background(), workercache.Entry{
			AssetKey: assetref.AssetKey(assetID), ContentHash: assetref.ContentHash(assetSHA(body)),
			LocalPath: path, SizeBytes: int64(len(body)), DownloadComplete: true,
		}); err != nil {
			t.Fatalf("store dispatch asset %s: %v", assetID, err)
		}
	}
	w.clipCache = cache
	w.canonicalAssetCache = workercache.NewCanonicalAssetStore(cache)
	payload := compiledPlanAssetPayload(t, assets)
	canonicalBefore := payload[contract.PayloadKeyCompiledRenderPlanJSON].(string)
	shaBefore := payload[contract.PayloadKeyCompiledRenderPlanSHA].(string)
	pte := &PendingTaskExecution{
		TaskID: "task-bindings", JobID: "job-bindings", AttemptID: "attempt-bindings", ExecutorID: "render_batch", ExecutorVersion: 1,
		Spec: executor.TaskSpec{Version: 1, JobID: "job-bindings", ExecutorID: "render_batch", Payload: payload},
	}
	result, err := w.dispatchTaskRunner(context.Background(), pte)
	if err != nil {
		t.Fatalf("dispatchTaskRunner: %v", err)
	}
	if result == nil || result.Status != "succeeded" {
		t.Fatalf("dispatch report = %+v, want succeeded", result)
	}
	if len(probe.seen) != len(assets) {
		t.Fatalf("executor saw %d bindings, want %d", len(probe.seen), len(assets))
	}
	for assetID := range assets {
		if probe.seen[assetID].Path == "" {
			t.Fatalf("executor binding %s has empty runtime path", assetID)
		}
	}
	if got := pte.Spec.Payload[contract.PayloadKeyCompiledRenderPlanJSON].(string); got != canonicalBefore {
		t.Fatalf("dispatch mutated canonical V2 JSON: before=%q after=%q", canonicalBefore, got)
	}
	if got := pte.Spec.Payload[contract.PayloadKeyCompiledRenderPlanSHA].(string); got != shaBefore {
		t.Fatalf("dispatch mutated canonical V2 SHA: before=%q after=%q", shaBefore, got)
	}
}

func TestCompiledAssetIdentityNormalizesWireReference(t *testing.T) {
	local := contract.AssetRefV2{AssetID: "plan-id", AssetKey: "velox-asset://cache-id"}
	if got := compiledAssetIdentity(local); got != "cache-id" {
		t.Fatalf("compiledAssetIdentity(local wire) = %q, want cache-id", got)
	}
	if got := compiledAssetReference(local); got != "velox-asset://cache-id" {
		t.Fatalf("compiledAssetReference(local wire) = %q, want original wire", got)
	}
	drive := contract.AssetRefV2{AssetID: "plan-drive", AssetKey: "velox-drive://drive-id"}
	if got := compiledAssetIdentity(drive); got != "drive-id" {
		t.Fatalf("compiledAssetIdentity(drive wire) = %q, want drive-id", got)
	}
	if got := compiledAssetReference(drive); got != "velox-drive://drive-id" {
		t.Fatalf("compiledAssetReference(drive wire) = %q, want original wire", got)
	}
}

func TestCompiledPlanAssetKeyUsesCanonicalCacheIdentity(t *testing.T) {
	assets := map[string][]byte{
		"v2-video": []byte("prepared-video-bytes"),
		"v2-audio": []byte("final-audio-bytes"),
	}
	payload := compiledPlanAssetPayload(t, assets)
	plan, err := contract.DecodeCompiledRenderPlanV2([]byte(payload[contract.PayloadKeyCompiledRenderPlanJSON].(string)))
	if err != nil {
		t.Fatalf("decode fixture plan: %v", err)
	}
	for i := range plan.Assets {
		switch plan.Assets[i].AssetID {
		case "v2-video":
			plan.Assets[i].AssetKey = "cache-video-v2"
		case "v2-audio":
			plan.Assets[i].AssetKey = "cache-audio-v2"
		}
	}
	canonical, err := plan.CanonicalJSON()
	if err != nil {
		t.Fatalf("canonical keyed plan: %v", err)
	}
	digest := sha256.Sum256(canonical)
	payload[contract.PayloadKeyCompiledRenderPlanJSON] = string(canonical)
	payload[contract.PayloadKeyCompiledRenderPlanSHA] = hex.EncodeToString(digest[:])

	var requested []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		identity := strings.TrimPrefix(r.URL.Path, "/api/v1/agent/assets/")
		requested = append(requested, identity)
		var body []byte
		switch identity {
		case "cache-video-v2":
			body = assets["v2-video"]
		case "cache-audio-v2":
			body = assets["v2-audio"]
		default:
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write(body)
	}))
	defer server.Close()
	w := &Worker{
		config:    &config.WorkerConfig{WorkerID: "worker-v2-asset-key", MasterURL: server.URL, WorkDir: t.TempDir()},
		apiClient: api.NewClient(server.URL),
	}
	if _, err := w.resolveCompiledRenderPlanAssets(context.Background(), payload); err != nil {
		t.Fatalf("resolve keyed V2 assets: %v", err)
	}
	seen := make(map[string]bool, len(requested))
	for _, identity := range requested {
		seen[identity] = true
	}
	if len(requested) != 2 || !seen["cache-video-v2"] || !seen["cache-audio-v2"] {
		t.Fatalf("resolver identities = %v, want cache keys", requested)
	}
}

func TestResolveCompiledRenderPlanAssets_RejectsMalformedPlanBeforeResolver(t *testing.T) {
	w := &Worker{config: &config.WorkerConfig{WorkDir: t.TempDir()}}
	payload := map[string]interface{}{
		contract.PayloadKeyCompiledRenderPlanJSON: "{not-canonical-json",
	}
	if _, err := w.resolveCompiledRenderPlanAssets(context.Background(), payload); err == nil {
		t.Fatal("malformed V2 plan was accepted")
	}
}

func compiledPlanAssetPayload(t *testing.T, assets map[string][]byte) map[string]interface{} {
	t.Helper()
	const timelineSHA = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	plan := &contract.CompiledRenderPlanV2{
		PlanVersion:      contract.CompiledPlanVersionV2,
		TimelineRevision: 1,
		TimelineSHA256:   timelineSHA,
		DurationUS:       1_000_000,
		Output: contract.OutputContractV2{
			Container: "mp4", VideoCodec: "libx264", Width: 640, Height: 360, FPSNum: 30, FPSDen: 1,
		},
		FinalAudio: contract.FinalAudioV2{
			Mode: contract.AudioModeFinalAudioCopy, AssetID: "v2-audio", SHA256: assetSHA(assets["v2-audio"]),
			SizeBytes: int64(len(assets["v2-audio"])), Codec: "aac", SampleRateHz: 48_000, Channels: 2,
			DurationUS: 1_000_000, TimelineRevision: 1, TimelineSHA256: timelineSHA,
		},
		VideoTracks: []contract.VideoTrackV2{{
			TrackID: "main", Segments: []contract.VideoSegmentV2{{
				SegmentID: "v2-segment", AssetID: "v2-video", SHA256: assetSHA(assets["v2-video"]),
				TimelineStartFrame: 0, FrameCount: 30, SourceInUS: 0, SourceDurationUS: 1_000_000,
			}},
		}},
		Assets: []contract.AssetRefV2{
			{AssetID: "v2-video", SHA256: assetSHA(assets["v2-video"]), SizeBytes: int64(len(assets["v2-video"])), Kind: "video", DurationUS: 1_000_000, Width: 640, Height: 360},
			{AssetID: "v2-audio", SHA256: assetSHA(assets["v2-audio"]), SizeBytes: int64(len(assets["v2-audio"])), Kind: "final_audio", DurationUS: 1_000_000},
		},
	}
	canonical, err := plan.CanonicalJSON()
	if err != nil {
		t.Fatalf("canonical V2 plan: %v", err)
	}
	digest := sha256.Sum256(canonical)
	return map[string]interface{}{
		contract.PayloadKeyCompiledRenderPlanJSON: string(canonical),
		contract.PayloadKeyCompiledRenderPlanSHA:  hex.EncodeToString(digest[:]),
	}
}

func assetSHA(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
