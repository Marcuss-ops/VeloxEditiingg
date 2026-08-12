package executors

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"velox-worker-agent/internal/executor"
	"velox-worker-agent/pkg/video/ffmpegrunner"
)

// Compile-time assertion: the fake satisfies the canonical runner contract.
var _ ffmpegrunner.FFmpegRunner = (*fakeFFmpegRunner)(nil)

// sinkExecutionContext is a minimal ExecutionContext stub exposing the
// attempt-scoped FFmpeg profile sink (B2). The remaining interface
// methods are noops; the render-plan executors only consult Recorder and
// FFmpegProfiles through optional interface assertions.
type sinkExecutionContext struct {
	agg *ffmpegrunner.Aggregator
}

func (s *sinkExecutionContext) FFmpegProfiles() *ffmpegrunner.Aggregator { return s.agg }
func (s *sinkExecutionContext) Artifacts() executor.ArtifactAccess       { return nil }
func (s *sinkExecutionContext) LocalCache() executor.LocalCache          { return nil }
func (s *sinkExecutionContext) Telemetry() executor.Telemetry            { return nil }
func (s *sinkExecutionContext) Resources() executor.ResourceLimits       { return nil }
func (s *sinkExecutionContext) Clock() executor.Clock                    { return nil }
func (s *sinkExecutionContext) Logger() executor.Logger                  { return nil }
func (s *sinkExecutionContext) Done() <-chan struct{}                    { return nil }
func (s *sinkExecutionContext) Err() error                               { return nil }

// fakeFFmpegRunner records the request it received and returns a canned
// FFmpegResult, so executor tests never touch a real ffmpeg process.
type fakeFFmpegRunner struct {
	gotOperation ffmpegrunner.OperationType
	gotArgs      []string
	result       ffmpegrunner.FFmpegResult
	err          error
}

func (f *fakeFFmpegRunner) Run(_ context.Context, req ffmpegrunner.FFmpegRequest) (ffmpegrunner.FFmpegResult, error) {
	f.gotOperation = req.Operation
	f.gotArgs = append([]string(nil), req.Args...)
	return f.result, f.err
}

func TestOperationForOutputType(t *testing.T) {
	cases := []struct {
		outputType string
		want       ffmpegrunner.OperationType
	}{
		{"audio.mix", ffmpegrunner.OperationAudioMix},
		{"video.output", ffmpegrunner.OperationEncode},
		{"video.compose", ffmpegrunner.OperationCompose},
		{"unexpected.kind", ffmpegrunner.OperationCompose}, // default = segment compose
	}
	for _, tc := range cases {
		if got := operationForOutputType(tc.outputType); got != tc.want {
			t.Errorf("operationForOutputType(%q) = %q, want %q", tc.outputType, got, tc.want)
		}
	}
}

func TestFFmpegProfileMetadata_NeverLeaksRawPaths(t *testing.T) {
	secret := "/secrets/worker-cache/bearer-token-zzz.mp4"
	req := ffmpegrunner.FFmpegRequest{
		Operation: ffmpegrunner.OperationEncode,
		Args:      []string{"-i", secret, "-vf", "ass=/secrets/worker-cache/creds.ass", "-y", "/tmp/out.mp4"},
	}
	result := ffmpegrunner.FFmpegResult{
		ProcessWallMS:      123,
		ExitCode:           0,
		CommandFingerprint: ffmpegrunner.Fingerprint(req),
		Parameters:         ffmpegrunner.Sanitize(req),
	}
	metadata := ffmpegProfileMetadata(result)
	encoded, err := json.Marshal(metadata)
	if err != nil {
		t.Fatalf("marshal metadata: %v", err)
	}
	body := string(encoded)
	for _, chunk := range []string{secret, "bearer-token-zzz", "creds.ass", "/tmp/out.mp4"} {
		if strings.Contains(body, chunk) {
			t.Errorf("ffmpeg_profile metadata leaks %q: %s", chunk, body)
		}
	}
	params := metadata["parameters"].(ffmpegrunner.SanitizedParameters)
	if !reflect.DeepEqual(params.Filters, []string{"ass"}) {
		t.Errorf("parameters.filters = %v, want [ass] (path stripped)", params.Filters)
	}
	if params.InputCount != 1 {
		t.Errorf("parameters.input_count = %d, want 1", params.InputCount)
	}
}

func validRenderPlanJSON(t *testing.T, jobID string) string {
	t.Helper()
	plan := map[string]interface{}{
		"version": 1,
		"job_id":  jobID,
		"canvas":  map[string]interface{}{"width": 1920, "height": 1080, "fps": 30},
		"timeline": []interface{}{
			map[string]interface{}{
				"source":           map[string]interface{}{"type": "video", "url": "seg1.mp4"},
				"duration_seconds": 5,
			},
		},
		"audio_tracks": []interface{}{
			map[string]interface{}{"source_url": "vo.wav", "volume": 1, "role": "voiceover"},
		},
		"output_path": "out.mp4",
	}
	data, err := json.Marshal(plan)
	if err != nil {
		t.Fatalf("marshal plan: %v", err)
	}
	return string(data)
}

// seedOutputFile materializes the artifact the executor reads back after
// the (faked) ffmpeg run.
func seedOutputFile(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatalf("mkdir output dir: %v", err)
	}
	if err := os.WriteFile(path, []byte("fake-rendered-artifact"), 0o640); err != nil {
		t.Fatalf("write output file: %v", err)
	}
}

func TestEncodeExecutor_ConsumesCanonicalRunnerAndPublishesProfile(t *testing.T) {
	const jobID = "job-encode-1"
	outputRoot := t.TempDir()
	fake := &fakeFFmpegRunner{
		result: ffmpegrunner.FFmpegResult{
			Operation:      ffmpegrunner.OperationEncode,
			ProcessSpawnMS: 12, FirstOutputMS: 45, ProcessingMS: 285,
			ExitWaitMS: 3, ProcessWallMS: 340, ExitCode: 0,
			UserCPUMs: 7, SystemCPUMs: 3, PeakRSSBytes: 100,
			ReadBytes: 200, WriteBytes: 300,
			CommandFingerprint: "fp-encode", Parameters: ffmpegrunner.SanitizedParameters{Codecs: []string{"aac", "libx264"}, InputCount: 2},
		},
	}
	output := filepath.Join(outputRoot, jobID+".mp4")
	seedOutputFile(t, output)

	executorImpl := NewEncode(fake, outputRoot)
	spec := executor.TaskSpec{
		Version:    1,
		JobID:      jobID,
		ExecutorID: EncodeID,
		Payload: map[string]interface{}{
			"render_plan_json": validRenderPlanJSON(t, jobID),
			"input_path":       "/cache/worker/video.mp4",
			"audio_mix_path":   "/cache/worker/audio.wav",
		},
	}
	result, err := executorImpl.Execute(context.Background(), nil, spec)
	if err != nil {
		t.Fatalf("Execute = %v, want nil", err)
	}
	if result.Status != "succeeded" {
		t.Fatalf("Status = %q, want succeeded (%s)", result.Status, result.ErrorDetail)
	}
	if fake.gotOperation != ffmpegrunner.OperationEncode {
		t.Errorf("runner received operation %q, want encode", fake.gotOperation)
	}
	profile, ok := result.Metrics["ffmpeg_profile"].(map[string]any)
	if !ok {
		t.Fatalf("Metrics[ffmpeg_profile] missing or wrong type: %#v", result.Metrics["ffmpeg_profile"])
	}
	if profile["process_wall_ms"] != int64(340) {
		t.Errorf("ffmpeg_profile.process_wall_ms = %v, want 340", profile["process_wall_ms"])
	}
	if profile["command_fingerprint"] != "fp-encode" {
		t.Errorf("ffmpeg_profile.command_fingerprint = %v, want fp-encode", profile["command_fingerprint"])
	}
	// B2: the per-process phase breakdown must travel with the profile.
	if profile["operation"] != ffmpegrunner.OperationEncode {
		t.Errorf("ffmpeg_profile.operation = %v, want encode", profile["operation"])
	}
	if profile["first_output_ms"] != int64(45) || profile["processing_ms"] != int64(285) || profile["exit_wait_ms"] != int64(3) {
		t.Errorf("ffmpeg_profile phase trio = first %v / proc %v / exit %v, want 45/285/3",
			profile["first_output_ms"], profile["processing_ms"], profile["exit_wait_ms"])
	}
	if result.RawMetrics == nil || result.RawMetrics.CpuTimeMs != 10 || result.RawMetrics.PeakRssBytes != 100 || result.RawMetrics.DiskReadBytes != 200 || result.RawMetrics.DiskWriteBytes != 300 || result.RawMetrics.WallClockSeconds != 0.34 {
		t.Fatalf("typed FFmpeg raw metrics = %+v", result.RawMetrics)
	}
	if _, ok := profile["parameters"]; !ok {
		t.Error("ffmpeg_profile.parameters missing")
	}
}

func TestAudioMixExecutor_RunnerReceivesAudioMixOperation(t *testing.T) {
	const jobID = "job-mix-1"
	outputRoot := t.TempDir()
	fake := &fakeFFmpegRunner{result: ffmpegrunner.FFmpegResult{ExitCode: 0}}
	seedOutputFile(t, filepath.Join(outputRoot, jobID+".audio.wav"))

	executorImpl := NewAudioMix(fake, outputRoot)
	spec := executor.TaskSpec{
		Version:    1,
		JobID:      jobID,
		ExecutorID: AudioMixID,
		Payload: map[string]interface{}{
			"render_plan_json": validRenderPlanJSON(t, jobID),
		},
	}
	result, err := executorImpl.Execute(context.Background(), nil, spec)
	if err != nil {
		t.Fatalf("Execute = %v, want nil", err)
	}
	if result.Status != "succeeded" {
		t.Fatalf("Status = %q, want succeeded (%s)", result.Status, result.ErrorDetail)
	}
	if fake.gotOperation != ffmpegrunner.OperationAudioMix {
		t.Errorf("runner received operation %q, want audio_mix", fake.gotOperation)
	}
	// The canonical runner still owns the full argument vector: the audio
	// mix plan must reach it untouched (no double-sanitization in executors).
	if len(fake.gotArgs) == 0 {
		t.Error("runner received empty args")
	}
	if _, ok := result.Metrics["audio_mix_evidence"]; !ok {
		t.Error("audio_mix_evidence missing from metrics")
	}
}

func TestComposeExecutor_RunnerReceivesComposeOperation(t *testing.T) {
	const jobID = "job-compose-1"
	outputRoot := t.TempDir()
	fake := &fakeFFmpegRunner{result: ffmpegrunner.FFmpegResult{ExitCode: 0}}
	seedOutputFile(t, filepath.Join(outputRoot, jobID+".compose.mp4"))

	executorImpl := NewCompose(fake, outputRoot)
	spec := executor.TaskSpec{
		Version:    1,
		JobID:      jobID,
		ExecutorID: ComposeID,
		Payload: map[string]interface{}{
			"render_plan_json": validRenderPlanJSON(t, jobID),
		},
	}
	result, err := executorImpl.Execute(context.Background(), nil, spec)
	if err != nil {
		t.Fatalf("Execute = %v, want nil", err)
	}
	if result.Status != "succeeded" {
		t.Fatalf("Status = %q, want succeeded (%s)", result.Status, result.ErrorDetail)
	}
	if fake.gotOperation != ffmpegrunner.OperationCompose {
		t.Errorf("runner received operation %q, want compose", fake.gotOperation)
	}
	if _, ok := result.Metrics["ffmpeg_profile"]; !ok {
		t.Error("ffmpeg_profile missing from compose metrics")
	}
}

func TestEncodeExecutor_PushesProfileIntoAttemptSink(t *testing.T) {
	const jobID = "job-sink-1"
	outputRoot := t.TempDir()
	agg := ffmpegrunner.NewAggregator()
	fake := &fakeFFmpegRunner{
		result: ffmpegrunner.FFmpegResult{
			Operation: ffmpegrunner.OperationEncode,
			ExitCode:  0, ProcessWallMS: 340,
			ProcessSpawnMS: 5, FirstOutputMS: 40, ProcessingMS: 290, ExitWaitMS: 2,
		},
	}
	seedOutputFile(t, filepath.Join(outputRoot, jobID+".mp4"))

	executorImpl := NewEncode(fake, outputRoot)
	spec := executor.TaskSpec{
		Version:    1,
		JobID:      jobID,
		ExecutorID: EncodeID,
		Payload: map[string]interface{}{
			"render_plan_json": validRenderPlanJSON(t, jobID),
			"input_path":       "/cache/worker/video.mp4",
		},
	}
	result, err := executorImpl.Execute(context.Background(), &sinkExecutionContext{agg: agg}, spec)
	if err != nil {
		t.Fatalf("Execute = %v, want nil", err)
	}
	if result.Status != "succeeded" {
		t.Fatalf("Status = %q, want succeeded (%s)", result.Status, result.ErrorDetail)
	}
	if got := agg.ProcessCount(); got != 1 {
		t.Fatalf("sink ProcessCount = %d, want 1 (profile must be pushed per attempt)", got)
	}
	summary := agg.Aggregate()
	if summary.TotalProcessingMS != 290 || summary.TotalSpawnMS != 5 {
		t.Errorf("aggregate = %+v, want processing 290 / spawn 5", summary)
	}
	if _, ok := summary.Operations[string(ffmpegrunner.OperationEncode)]; !ok {
		t.Errorf("aggregate operations missing encode bucket: %+v", summary.Operations)
	}
}

func TestEncodeExecutor_RunnerFailureSurfacesExitCode(t *testing.T) {
	const jobID = "job-fail-1"
	outputRoot := t.TempDir()
	fake := &fakeFFmpegRunner{
		err: errors.New("ffmpeg run: exit status 2"),
		result: ffmpegrunner.FFmpegResult{
			ExitCode: 2, TerminatedBySignal: false,
			ProcessWallMS: 17, UserCPUMs: 4, SystemCPUMs: 1,
			PeakRSSBytes: 55, ReadBytes: 66, WriteBytes: 77,
			CommandFingerprint: "fp-fail",
		},
	}
	executorImpl := NewEncode(fake, outputRoot)
	spec := executor.TaskSpec{
		Version:    1,
		JobID:      jobID,
		ExecutorID: EncodeID,
		Payload: map[string]interface{}{
			"render_plan_json": validRenderPlanJSON(t, jobID),
			"input_path":       "/cache/worker/video.mp4",
		},
	}
	result, err := executorImpl.Execute(context.Background(), nil, spec)
	if err != nil {
		t.Fatalf("Execute = %v, want nil (failures are mapped to the result)", err)
	}
	if result.Status != "failed" {
		t.Fatalf("Status = %q, want failed", result.Status)
	}
	if result.ErrorCode != "command_failed" {
		t.Errorf("ErrorCode = %q, want command_failed", result.ErrorCode)
	}
	if !strings.Contains(result.ErrorDetail, "exit_code=2") {
		t.Errorf("ErrorDetail = %q, want exit_code=2", result.ErrorDetail)
	}
	if result.RawMetrics == nil || result.RawMetrics.CpuTimeMs != 5 || result.RawMetrics.PeakRssBytes != 55 || result.RawMetrics.DiskReadBytes != 66 || result.RawMetrics.DiskWriteBytes != 77 || result.RawMetrics.WallClockSeconds != 0.017 {
		t.Fatalf("failed FFmpeg raw metrics = %+v", result.RawMetrics)
	}
}
