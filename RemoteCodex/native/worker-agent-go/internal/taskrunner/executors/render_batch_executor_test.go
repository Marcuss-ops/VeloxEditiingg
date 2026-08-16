package executors

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"velox-shared/contract"
	"velox-shared/controltransport"
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/publisher"
	"velox-worker-agent/internal/runtimeassets"
	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/pkg/video/ffmpegrunner"
)

type batchFakeFFmpegRunner struct {
	requests []ffmpegrunner.FFmpegRequest
}

type batchLogRecord struct {
	level   string
	message string
	fields  map[string]interface{}
}

type batchRecordingLogger struct {
	records []batchLogRecord
}

func (l *batchRecordingLogger) add(level, message string, fields map[string]interface{}) {
	copyFields := make(map[string]interface{}, len(fields))
	for key, value := range fields {
		copyFields[key] = value
	}
	l.records = append(l.records, batchLogRecord{level: level, message: message, fields: copyFields})
}

func (l *batchRecordingLogger) Info(message string, fields map[string]interface{}) {
	l.add("info", message, fields)
}
func (l *batchRecordingLogger) Warn(message string, fields map[string]interface{}) {
	l.add("warn", message, fields)
}
func (l *batchRecordingLogger) Error(message string, _ error, fields map[string]interface{}) {
	l.add("error", message, fields)
}

type batchObservabilityContext struct {
	logger   *batchRecordingLogger
	recorder *telemetry.EventRecorder
}

func (c *batchObservabilityContext) Artifacts() executor.ArtifactAccess { return nil }
func (c *batchObservabilityContext) LocalCache() executor.LocalCache    { return nil }
func (c *batchObservabilityContext) Telemetry() executor.Telemetry      { return nil }
func (c *batchObservabilityContext) Resources() executor.ResourceLimits { return nil }
func (c *batchObservabilityContext) Clock() executor.Clock              { return nil }
func (c *batchObservabilityContext) Logger() executor.Logger            { return c.logger }
func (c *batchObservabilityContext) Done() <-chan struct{}              { return nil }
func (c *batchObservabilityContext) Err() error                         { return nil }
func (c *batchObservabilityContext) Recorder() *telemetry.EventRecorder {
	return c.recorder
}

func (f *batchFakeFFmpegRunner) Run(_ context.Context, req ffmpegrunner.FFmpegRequest) (ffmpegrunner.FFmpegResult, error) {
	f.requests = append(f.requests, req)
	if len(req.Args) == 0 {
		return ffmpegrunner.FFmpegResult{}, nil
	}
	output := req.Args[len(req.Args)-1]
	if err := os.MkdirAll(filepath.Dir(output), 0o750); err != nil {
		return ffmpegrunner.FFmpegResult{}, err
	}
	if err := os.WriteFile(output, []byte("fake-"+string(req.Operation)), 0o640); err != nil {
		return ffmpegrunner.FFmpegResult{}, err
	}
	return ffmpegrunner.FFmpegResult{
		Operation:          req.Operation,
		ExitCode:           0,
		ProcessWallMS:      10,
		UserCPUMs:          3,
		SystemCPUMs:        2,
		PeakRSSBytes:       100,
		ReadBytes:          1000,
		WriteBytes:         2000,
		CommandFingerprint: ffmpegrunner.Fingerprint(req),
		Parameters:         ffmpegrunner.Sanitize(req),
	}, nil
}

func batchTestPlan() *contract.CompiledRenderPlanV2 {
	const timelineSHA = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	videoSHA := batchSHA([]byte("video-data"))
	audioSHA := batchSHA([]byte("audio-data"))
	return &contract.CompiledRenderPlanV2{
		PlanVersion:      contract.CompiledPlanVersionV2,
		TimelineRevision: 7,
		TimelineSHA256:   timelineSHA,
		DurationUS:       2_000_000,
		Output: contract.OutputContractV2{
			Container: "mp4", VideoCodec: "libx264", Width: 640, Height: 360,
			FPSNum: 30, FPSDen: 1, PixelFormat: "yuv420p",
		},
		FinalAudio: contract.FinalAudioV2{
			Mode: contract.AudioModeFinalAudioCopy, AssetID: "audio-master-001",
			SHA256: audioSHA, SizeBytes: 10, Codec: "aac",
			SampleRateHz: 48_000, Channels: 2, DurationUS: 2_000_000,
			TimelineRevision: 7, TimelineSHA256: timelineSHA,
		},
		VideoTracks: []contract.VideoTrackV2{{
			TrackID: "main",
			Segments: []contract.VideoSegmentV2{{
				SegmentID: "segment-a", AssetID: "video-a", SHA256: videoSHA,
				TimelineStartFrame: 0, FrameCount: 60, SourceInUS: 0, SourceDurationUS: 2_000_000,
			}},
		}},
		Assets: []contract.AssetRefV2{
			{AssetID: "video-a", SHA256: videoSHA, SizeBytes: 10, Kind: "video", DurationUS: 2_000_000, Width: 640, Height: 360},
			{AssetID: "audio-master-001", SHA256: audioSHA, SizeBytes: 10, Kind: "final_audio", DurationUS: 2_000_000},
		},
	}
}

func batchSHA(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func batchTaskSpec(t *testing.T, jobID string) executor.TaskSpec {
	t.Helper()
	plan := batchTestPlan()
	canonical, err := plan.CanonicalJSON()
	if err != nil {
		t.Fatalf("canonical plan: %v", err)
	}
	sum := sha256.Sum256(canonical)
	return executor.TaskSpec{
		Version: 1, JobID: jobID, ExecutorID: RenderBatchID,
		Payload: map[string]interface{}{
			contract.PayloadKeyCompiledRenderPlanJSON: string(canonical),
			contract.PayloadKeyCompiledRenderPlanSHA:  hex.EncodeToString(sum[:]),
		},
	}
}

func batchBindings(t *testing.T) runtimeassets.Bindings {
	t.Helper()
	dir := t.TempDir()
	bindings := runtimeassets.Bindings{}
	for _, item := range []struct {
		id   string
		data []byte
	}{
		{"video-a", []byte("video-data")},
		{"audio-master-001", []byte("audio-data")},
	} {
		path := filepath.Join(dir, item.id+".asset")
		if err := os.WriteFile(path, item.data, 0o640); err != nil {
			t.Fatalf("write binding %s: %v", item.id, err)
		}
		bindings[item.id] = runtimeassets.Binding{AssetID: item.id, Path: path, SHA256: batchSHA(item.data), Size: int64(len(item.data))}
	}
	return bindings
}

func TestRenderBatch_DescriptorAndRegistry(t *testing.T) {
	runner := &batchFakeFFmpegRunner{}
	exec := NewRenderBatch(runner, t.TempDir())
	desc := exec.Descriptor()
	if desc.ID != RenderBatchID || desc.Version != RenderBatchVersion {
		t.Fatalf("descriptor identity = %s@%d", desc.ID, desc.Version)
	}
	if !reflect.DeepEqual(desc.InputTypes, []string{"render.compiled.v2"}) || !reflect.DeepEqual(desc.OutputTypes, []string{"video/mp4"}) {
		t.Fatalf("descriptor types = %+v", desc)
	}
	if desc.ResourceClass != executor.ResourceCPU || desc.TemporalMode != executor.TemporalGlobal || !desc.Deterministic || !desc.Cacheable {
		t.Fatalf("descriptor capabilities = %+v", desc)
	}

	reg := executor.NewRegistry()
	if err := RegisterRenderBatchExecutor(reg, runner, t.TempDir()); err != nil {
		t.Fatalf("register render_batch: %v", err)
	}
	if !reg.Has(RenderBatchID, RenderBatchVersion) {
		t.Fatal("render_batch@1 is not registered")
	}
}

func TestRenderBatch_CapabilityReportExposesDescriptor(t *testing.T) {
	reg := executor.NewRegistry()
	runner := &batchFakeFFmpegRunner{}
	if err := RegisterRenderBatchExecutor(reg, runner, t.TempDir()); err != nil {
		t.Fatalf("register render_batch: %v", err)
	}
	report := executor.BuildCapabilityReport(reg, controltransport.HostInfo{WorkerID: "worker-test"})
	if len(report.Executors) != 1 {
		t.Fatalf("capability executor count = %d, want 1", len(report.Executors))
	}
	capability := report.Executors[0]
	if capability.ID != RenderBatchID || capability.Version != RenderBatchVersion {
		t.Fatalf("capability identity = %s@%d", capability.ID, capability.Version)
	}
	if capability.ResourceClass != string(executor.ResourceCPU) || capability.TemporalMode != string(executor.TemporalGlobal) {
		t.Fatalf("capability resource/temporal = %q/%q", capability.ResourceClass, capability.TemporalMode)
	}
	if !capability.Deterministic || !capability.Cacheable {
		t.Fatalf("capability deterministic/cacheable = %t/%t", capability.Deterministic, capability.Cacheable)
	}
	if !reflect.DeepEqual(capability.OutputTypes, []string{"video/mp4"}) {
		t.Fatalf("capability output types = %#v", capability.OutputTypes)
	}
}

func TestRenderBatch_PreservesLegacyRegistryEntries(t *testing.T) {
	reg := executor.NewRegistry()
	if err := RegisterRenderPlanExecutors(reg, t.TempDir()); err != nil {
		t.Fatalf("register legacy executors: %v", err)
	}
	legacyKeys := reg.IDs()
	if len(legacyKeys) != 4 {
		t.Fatalf("legacy executor count = %d, want 4", len(legacyKeys))
	}
	if err := RegisterRenderBatchExecutor(reg, &batchFakeFFmpegRunner{}, t.TempDir()); err != nil {
		t.Fatalf("register render_batch: %v", err)
	}
	if reg.Len() != 5 {
		t.Fatalf("combined executor count = %d, want 5", reg.Len())
	}
	for _, key := range legacyKeys {
		found := false
		for _, current := range reg.IDs() {
			if current == key {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("legacy executor %q disappeared", key)
		}
	}
}

func TestRenderBatch_RejectsLegacyAndMissingBindings(t *testing.T) {
	exec := NewRenderBatch(&batchFakeFFmpegRunner{}, t.TempDir())
	legacy := executor.TaskSpec{JobID: "legacy", Payload: map[string]interface{}{"render_plan_json": "{}"}}
	if err := exec.Validate(legacy); err == nil {
		t.Fatal("render_batch accepted a legacy payload")
	}

	spec := batchTaskSpec(t, "job-missing-bindings")
	result, err := exec.Execute(context.Background(), nil, spec)
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if result.Status != "failed" || result.ErrorCode != "ASSET_BINDINGS_MISSING" {
		t.Fatalf("missing bindings result = %+v", result)
	}
}

func TestRenderBatch_VideoOnlyThenFinalAudioCopyMux(t *testing.T) {
	runner := &batchFakeFFmpegRunner{}
	outputRoot := t.TempDir()
	exec := NewRenderBatch(runner, outputRoot)
	spec := batchTaskSpec(t, "job-batch-001")
	exec.(*renderBatchExecutor).probe = func(_ context.Context, path string) (publisher.MediaProbe, error) {
		if strings.Contains(path, "audio-master-001") {
			return publisher.MediaProbe{HasAudio: true, AudioTrackCount: 1, AudioCodec: "aac", AudioSampleRateHz: 48_000, AudioChannels: 2, DurationSec: 2}, nil
		}
		return publisher.MediaProbe{HasVideo: true, VideoTrackCount: 1, HasAudio: true, AudioTrackCount: 1, AudioCodec: "aac", AudioSampleRateHz: 48_000, AudioChannels: 2, DurationSec: 2}, nil
	}
	ctx := runtimeassets.WithBindings(context.Background(), batchBindings(t))

	result, err := exec.Execute(ctx, nil, spec)
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if result.Status != "succeeded" {
		t.Fatalf("result = %+v", result)
	}
	if len(runner.requests) != 2 {
		t.Fatalf("FFmpeg calls = %d, want visual + mux", len(runner.requests))
	}
	visual, mux := runner.requests[0], runner.requests[1]
	if visual.Operation != ffmpegrunner.OperationCompose || mux.Operation != ffmpegrunner.OperationEncode {
		t.Fatalf("operations = %q then %q, want compose then encode", visual.Operation, mux.Operation)
	}
	visualArgs := strings.Join(visual.Args, " ")
	if !strings.Contains(visualArgs, "-an") {
		t.Fatalf("visual command must be video-only: %s", visualArgs)
	}
	// The V2 command preserves frame placement with overlays instead of
	// routing through the legacy concat executor.
	if !strings.Contains(visualArgs, "overlay") {
		t.Fatalf("visual command does not describe positioned rendering: %s", visualArgs)
	}
	muxArgs := strings.Join(mux.Args, " ")
	for _, required := range []string{"-c:v copy", "-c:a copy", "-map 0:v:0", "-map 1:a:0", "-movflags +faststart"} {
		if !strings.Contains(muxArgs, required) {
			t.Errorf("mux args missing %q: %s", required, muxArgs)
		}
	}
	if strings.Contains(muxArgs, "-shortest") {
		t.Error("mux must not use -shortest as a duration fix")
	}
	if got := result.Metrics["audio_mix_count"]; got != int64(0) {
		t.Errorf("audio_mix_count = %v, want 0", got)
	}
	if got := result.Metrics["audio_encode_count"]; got != int64(0) {
		t.Errorf("audio_encode_count = %v, want 0", got)
	}
	if got := result.Metrics["final_audio_copy"]; got != int64(1) {
		t.Errorf("final_audio_copy = %v, want 1", got)
	}
	if result.RawMetrics == nil {
		t.Fatal("render_batch omitted typed raw metrics")
	}
	if result.RawMetrics.CpuTimeMs != 10 || result.RawMetrics.PeakRssBytes != 100 || result.RawMetrics.DiskReadBytes != 2000 || result.RawMetrics.DiskWriteBytes != 4000 || result.RawMetrics.OutputFileSize <= 0 || result.RawMetrics.OutputSha256 == "" || !result.RawMetrics.FinalConcatStreamCopy || result.RawMetrics.ConcatMode != "stream_copy" {
		t.Fatalf("render_batch raw metrics = %+v", *result.RawMetrics)
	}
	if len(result.Outputs) != 1 || result.Outputs[0].Type != "video/mp4" || result.Outputs[0].Hash == "" {
		t.Fatalf("final output = %+v", result.Outputs)
	}
	if _, err := os.Stat(filepath.Join(outputRoot, "job-batch-001.video-only.mp4")); !os.IsNotExist(err) {
		t.Fatalf("video-only intermediate was not cleaned up: err=%v", err)
	}
}

func TestRenderBatch_EmitsPhaseMetricsAndStructuredIdentity(t *testing.T) {
	runner := &batchFakeFFmpegRunner{}
	exec := NewRenderBatch(runner, t.TempDir())
	exec.(*renderBatchExecutor).probe = func(_ context.Context, path string) (publisher.MediaProbe, error) {
		if strings.Contains(path, "audio-master-001") {
			return publisher.MediaProbe{HasAudio: true, AudioTrackCount: 1, AudioCodec: "aac", AudioSampleRateHz: 48_000, AudioChannels: 2, DurationSec: 2}, nil
		}
		return publisher.MediaProbe{HasVideo: true, VideoTrackCount: 1, HasAudio: true, AudioTrackCount: 1, AudioCodec: "aac", AudioSampleRateHz: 48_000, AudioChannels: 2, DurationSec: 2}, nil
	}
	logger := &batchRecordingLogger{}
	recorder := telemetry.NewEventRecorder()
	execCtx := &batchObservabilityContext{logger: logger, recorder: recorder}
	result, err := exec.Execute(runtimeassets.WithBindings(context.Background(), batchBindings(t)), execCtx, batchTaskSpec(t, "job-observability"))
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if result.Status != "succeeded" {
		t.Fatalf("result = %+v", result)
	}
	for _, key := range []string{"render_plan_validate_ms", "compiled_asset_resolve_ms", "final_audio_resolve_ms", "visual_execute_ms", "final_mux_ms"} {
		value, ok := result.Metrics[key].(int64)
		if !ok || value < 0 {
			t.Errorf("metric %q = %#v, want non-negative int64", key, result.Metrics[key])
		}
	}
	for _, forbidden := range []string{"plan_sha256", "timeline_sha256", "timeline_revision", "final_audio_asset_id"} {
		if _, ok := result.Metrics[forbidden]; ok {
			t.Errorf("high-cardinality identity %q leaked into metrics: %#v", forbidden, result.Metrics[forbidden])
		}
	}

	planSHA := compiledPlanSHA(batchTaskSpec(t, "job-observability"))
	timelineSHA := batchTestPlan().TimelineSHA256
	foundIdentityLog := false
	for _, record := range logger.records {
		if record.fields["plan_sha256"] == planSHA && record.fields["timeline_sha256"] == timelineSHA {
			foundIdentityLog = true
		}
		encoded, marshalErr := json.Marshal(record)
		if marshalErr != nil {
			t.Fatalf("marshal log record: %v", marshalErr)
		}
		if strings.Contains(string(encoded), "/tmp/") || strings.Contains(string(encoded), "video-only.mp4") {
			t.Errorf("structured log leaks local output path: %s", encoded)
		}
	}
	if !foundIdentityLog {
		t.Fatalf("no structured log carried both plan and timeline SHA: %#v", logger.records)
	}

	events := recorder.Snapshot()
	if len(events) != 4 {
		t.Fatalf("recorded phases = %d, want validation, asset resolution, render and mux", len(events))
	}
	wantEvents := map[string]bool{
		"worker.plan.validate":       false,
		"worker.plan.resolve_assets": false,
		"engine.render":              false,
		"engine.mux.packet_write":    false,
	}
	for _, event := range events {
		wantEvents[event.Component+"."+event.Action] = true
		if event.Status != telemetry.StatusOK {
			t.Errorf("event %s.%s status = %q, want ok", event.Component, event.Action, event.Status)
		}
		if !strings.Contains(event.MetadataJSON, planSHA) || !strings.Contains(event.MetadataJSON, timelineSHA) {
			t.Errorf("event %s.%s metadata lacks plan/timeline identity: %s", event.Component, event.Action, event.MetadataJSON)
		}
		if strings.Contains(event.MetadataJSON, "/tmp/") || strings.Contains(event.MetadataJSON, "video-only.mp4") {
			t.Errorf("event %s.%s metadata leaks local path: %s", event.Component, event.Action, event.MetadataJSON)
		}
	}
	for key, seen := range wantEvents {
		if !seen {
			t.Errorf("missing canonical render_batch event %q", key)
		}
	}
}

func TestRenderBatch_ValidationFailureStillRecordsIdentityWithoutPaths(t *testing.T) {
	logger := &batchRecordingLogger{}
	recorder := telemetry.NewEventRecorder()
	exec := NewRenderBatch(&batchFakeFFmpegRunner{}, t.TempDir())
	spec := batchTaskSpec(t, "job-invalid-observability")
	// Make the transport hash invalid while keeping the payload free of paths.
	spec.Payload[contract.PayloadKeyCompiledRenderPlanSHA] = strings.Repeat("f", 64)
	planSHA := compiledPlanSHA(spec)
	result, err := exec.Execute(context.Background(), &batchObservabilityContext{logger: logger, recorder: recorder}, spec)
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if result.Status != "failed" || result.ErrorCode != "validation_failed" {
		t.Fatalf("result = %+v, want validation_failed", result)
	}
	if _, ok := result.Metrics["render_plan_validate_ms"]; !ok {
		t.Fatalf("validation failure omitted render_plan_validate_ms: %#v", result.Metrics)
	}
	events := recorder.Snapshot()
	if len(events) != 1 || events[0].Status != telemetry.StatusFailed {
		t.Fatalf("validation events = %+v, want one failed event", events)
	}
	if !strings.Contains(events[0].MetadataJSON, planSHA) {
		t.Errorf("validation event lost original plan SHA: %s", events[0].MetadataJSON)
	}
	for _, record := range logger.records {
		encoded, marshalErr := json.Marshal(record)
		if marshalErr != nil {
			t.Fatalf("marshal log record: %v", marshalErr)
		}
		if strings.Contains(string(encoded), "/tmp/") {
			t.Errorf("validation log leaks local path: %s", encoded)
		}
	}
}

func TestRenderBatch_VideoCommandPreservesSegmentPlacementAndGaps(t *testing.T) {
	plan := batchTestPlan()
	plan.Assets = append(plan.Assets, contract.AssetRefV2{
		AssetID: "video-b", SHA256: strings.Repeat("d", 64), SizeBytes: 10,
		Kind: "video", DurationUS: 1_000_000, Width: 640, Height: 360,
	})
	plan.VideoTracks[0].Segments = append(plan.VideoTracks[0].Segments, contract.VideoSegmentV2{
		SegmentID: "segment-b", AssetID: "video-b", SHA256: strings.Repeat("d", 64),
		TimelineStartFrame: 90, FrameCount: 30, SourceInUS: 500_000, SourceDurationUS: 1_000_000,
	})
	bindings := runtimeassets.Bindings{
		"video-a": {AssetID: "video-a", Path: "/cache/video-a.mp4"},
		"video-b": {AssetID: "video-b", Path: "/cache/video-b.mp4"},
	}
	args, err := buildVideoOnlyArgs(plan, bindings, "/tmp/video-only.mp4")
	if err != nil {
		t.Fatalf("buildVideoOnlyArgs: %v", err)
	}
	joined := strings.Join(args, " ")
	if strings.Count(joined, "overlay=eof_action=pass") != 2 {
		t.Fatalf("overlay count = %d, want 2: %s", strings.Count(joined, "overlay=eof_action=pass"), joined)
	}
	if !strings.Contains(joined, "setpts=PTS-STARTPTS+3.000000/TB") {
		t.Fatalf("second segment placement was not converted from frame 90 at 30fps: %s", joined)
	}
	if !strings.Contains(joined, "trim=start=0.500000:duration=1.000000") {
		t.Fatalf("second segment source trim was not preserved: %s", joined)
	}
}

func TestRenderBatch_RejectsAudioContractMismatch(t *testing.T) {
	exec := NewRenderBatch(&batchFakeFFmpegRunner{}, t.TempDir())
	exec.(*renderBatchExecutor).probe = func(_ context.Context, _ string) (publisher.MediaProbe, error) {
		return publisher.MediaProbe{HasAudio: true, AudioTrackCount: 1, AudioCodec: "mp3", AudioSampleRateHz: 44_100, AudioChannels: 1, DurationSec: 2}, nil
	}
	result, err := exec.Execute(runtimeassets.WithBindings(context.Background(), batchBindings(t)), nil, batchTaskSpec(t, "job-audio-contract"))
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if result.Status != "failed" || result.ErrorCode != "FINAL_AUDIO_INVALID" {
		t.Fatalf("audio contract result = %+v", result)
	}
}

func TestRenderBatch_RejectsBindingIntegrityMismatch(t *testing.T) {
	exec := NewRenderBatch(&batchFakeFFmpegRunner{}, t.TempDir())
	bindings := batchBindings(t)
	binding := bindings["audio-master-001"]
	binding.SHA256 = strings.Repeat("d", 64)
	bindings["audio-master-001"] = binding
	result, err := exec.Execute(runtimeassets.WithBindings(context.Background(), bindings), nil, batchTaskSpec(t, "job-integrity"))
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if result.Status != "failed" || result.ErrorCode != "ASSET_BINDINGS_INVALID" {
		t.Fatalf("integrity result = %+v", result)
	}
}

func TestRenderBatch_RejectsOnDiskTamper(t *testing.T) {
	exec := NewRenderBatch(&batchFakeFFmpegRunner{}, t.TempDir())
	bindings := batchBindings(t)
	if err := os.WriteFile(bindings["video-a"].Path, []byte("tampered!!"), 0o640); err != nil {
		t.Fatalf("tamper asset: %v", err)
	}
	result, err := exec.Execute(runtimeassets.WithBindings(context.Background(), bindings), nil, batchTaskSpec(t, "job-tampered"))
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if result.Status != "failed" || result.ErrorCode != "ASSET_BINDINGS_INVALID" {
		t.Fatalf("tamper result = %+v", result)
	}
	if strings.Contains(result.ErrorDetail, bindings["video-a"].Path) || strings.Contains(result.ErrorDetail, batchSHA([]byte("tampered!!"))) {
		t.Errorf("tamper ErrorDetail leaks local path or full SHA: %q", result.ErrorDetail)
	}
}

func TestRenderBatch_RejectsPathTraversalJobID(t *testing.T) {
	exec := NewRenderBatch(&batchFakeFFmpegRunner{}, t.TempDir())
	bindings := batchBindings(t)
	result, err := exec.Execute(runtimeassets.WithBindings(context.Background(), bindings), nil, batchTaskSpec(t, "../escape"))
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if result.Status != "failed" || result.ErrorCode != "INVALID_JOB_ID" {
		t.Fatalf("path traversal result = %+v", result)
	}
}

func TestRenderBatch_V2PlanHasNoLocalPaths(t *testing.T) {
	spec := batchTaskSpec(t, "job-path-free")
	raw := spec.Payload[contract.PayloadKeyCompiledRenderPlanJSON].(string)
	if strings.Contains(raw, filepath.Join(t.TempDir(), "local")) {
		t.Fatal("test plan unexpectedly contains a local path")
	}
	var decoded map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		t.Fatalf("decode plan: %v", err)
	}
	if _, ok := decoded["local_path"]; ok {
		t.Fatal("V2 plan contains local_path")
	}
}

func TestRenderBatch_RoutesFinalOutputThroughStorageResolver(t *testing.T) {
	runner := &batchFakeFFmpegRunner{}
	exec := NewRenderBatch(runner, t.TempDir())
	exec.(*renderBatchExecutor).probe = func(_ context.Context, path string) (publisher.MediaProbe, error) {
		if strings.Contains(path, "audio-master-001") {
			return publisher.MediaProbe{HasAudio: true, AudioTrackCount: 1, AudioCodec: "aac", AudioSampleRateHz: 48_000, AudioChannels: 2, DurationSec: 2}, nil
		}
		return publisher.MediaProbe{HasVideo: true, VideoTrackCount: 1, HasAudio: true, AudioTrackCount: 1, AudioCodec: "aac", AudioSampleRateHz: 48_000, AudioChannels: 2, DurationSec: 2}, nil
	}
	resolver := testStagingResolver(t)
	spec := batchTaskSpec(t, "job-batch-staging")

	execCtx := &storageExecutionContext{sinkExecutionContext: &sinkExecutionContext{}, resolver: resolver}
	result, err := exec.Execute(runtimeassets.WithBindings(context.Background(), batchBindings(t)), execCtx, spec)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Status != "succeeded" {
		t.Fatalf("result = %+v", result)
	}
	if len(result.Outputs) != 1 {
		t.Fatalf("outputs = %+v", result.Outputs)
	}
	stagingDir := resolver.Config().ArtifactStaging.Dir
	if got := result.Outputs[0].URI; !strings.HasPrefix(got, stagingDir) {
		t.Errorf("final URI %q must live under tmpfs staging root %q", got, stagingDir)
	}
}
