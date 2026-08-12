// Package executors / render_plan_executor_commands.go
//
// The ffmpeg-driving half of the render-plan executors: shared command
// runner plumbing (runCommandExecutor, failedResult, artifactFromFile,
// planDigest, inputArgs, totalDuration, sortedInputs) and the three
// command-building executors (audio mix, compose, encode). The pure
// parse/validate + subtitle executor stay in render_plan_executor.go.
package executors

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/pkg/video/ffmpegrunner"
	"velox-worker-agent/pkg/video/plan"
)

// operationForOutputType maps the executor output kind onto the canonical
// ffmpeg phase. audio.mix → audio mix (AudioRecorder), video.compose →
// segment compose (SegmentRecorder), video.output → final mux/encode
// (MuxRecorder).
func operationForOutputType(outputType string) ffmpegrunner.OperationType {
	switch outputType {
	case "audio.mix":
		return ffmpegrunner.OperationAudioMix
	case "video.output":
		return ffmpegrunner.OperationEncode
	default:
		return ffmpegrunner.OperationCompose
	}
}

// ffmpegProfileMetadata is the SANITIZED projection of an FFmpegResult
// that may travel to the master: fingerprints, durations, CPU/RSS/I/O
// counters and safe parameters. Raw paths/tokens never leave the runner.
func ffmpegProfileMetadata(result ffmpegrunner.FFmpegResult) map[string]any {
	return map[string]any{
		"operation":            result.Operation,
		"process_spawn_ms":     result.ProcessSpawnMS,
		"first_output_ms":      result.FirstOutputMS,
		"processing_ms":        result.ProcessingMS,
		"exit_wait_ms":         result.ExitWaitMS,
		"process_wall_ms":      result.ProcessWallMS,
		"user_cpu_ms":          result.UserCPUMs,
		"system_cpu_ms":        result.SystemCPUMs,
		"peak_rss_bytes":       result.PeakRSSBytes,
		"read_bytes":           result.ReadBytes,
		"write_bytes":          result.WriteBytes,
		"exit_code":            result.ExitCode,
		"terminated_by_signal": result.TerminatedBySignal,
		"stream_timed_out":     result.StreamTimedOut,
		"command_fingerprint":  result.CommandFingerprint,
		"parameters":           result.Parameters,
	}
}

func runCommandExecutor(ctx context.Context, e *renderPlanExecutor, spec executor.TaskSpec, cp CommandPlan, outputType string, execCtx executor.ExecutionContext) (executor.ExecutionResult, error) {
	started := time.Now().UTC()
	if err := os.MkdirAll(filepath.Dir(cp.OutputPath), 0o750); err != nil {
		return failedResult(started, "output_directory", err), nil
	}
	rec := recorderFromExecutionContext(execCtx)
	specForCommand := telemetry.EventSpec{Origin: telemetry.OriginEngine, Scope: telemetry.ScopeSegment, Component: "engine", Action: "composite"}
	switch outputType {
	case "audio.mix":
		specForCommand = telemetry.EventSpec{Origin: telemetry.OriginEngine, Scope: telemetry.ScopeAudioTrack, Component: "engine.audio", Action: "mix"}
	case "video.output":
		specForCommand = telemetry.EventSpec{Origin: telemetry.OriginEngine, Scope: telemetry.ScopeSegment, Component: "engine.encode", Action: "setup"}
	}
	commandHandle := rec.Begin(specForCommand)

	// Every phase runs through the same canonical FFmpegRunner; the
	// profiling result is attached once, here, to the phase event and to
	// the execution metrics.
	result, runErr := e.runner.Run(ctx, ffmpegrunner.FFmpegRequest{
		Operation: operationForOutputType(outputType),
		Args:      cp.Args,
	})
	// Attempt-scoped aggregation: when the execution context exposes the
	// per-attempt sink, fold this process into it so the report can answer
	// "N processes → total spawn/setup vs total processing" per attempt.
	if sink, ok := execCtx.(interface {
		FFmpegProfiles() *ffmpegrunner.Aggregator
	}); ok {
		if sink.FFmpegProfiles() != nil {
			sink.FFmpegProfiles().Add(result)
		}
	}
	profile := ffmpegProfileMetadata(result)
	rawMetrics := rawMetricsFromFFmpegResult(result)
	commandHandle.SetMetadata("executor_id", cp.ExecutorID)
	commandHandle.SetMetadata("command_fingerprint", result.CommandFingerprint)
	commandHandle.SetMetadata("ffmpeg_profile", profile)
	if runErr != nil {
		commandHandle.Abort("command_failed", runErr.Error())
		detail := fmt.Errorf("%w (exit_code=%d signal=%v)", runErr, result.ExitCode, result.TerminatedBySignal)
		return executor.ExecutionResult{
			Status: "failed", ErrorCode: "command_failed", ErrorDetail: detail.Error(),
			RawMetrics: rawMetrics, Metrics: map[string]interface{}{"ffmpeg_profile": profile},
			StartedAt: started, CompletedAt: time.Now().UTC(),
		}, nil
	}
	commandHandle.CompleteWith(0, 0, 0, telemetry.StatusOK, "", "")
	artifact, err := artifactFromFile(outputType, cp.OutputPath)
	if err != nil {
		return executor.ExecutionResult{
			Status: "failed", ErrorCode: "artifact_invalid", ErrorDetail: err.Error(),
			RawMetrics: rawMetrics, Metrics: map[string]interface{}{"ffmpeg_profile": profile},
			StartedAt: started, CompletedAt: time.Now().UTC(),
		}, nil
	}
	metrics := map[string]interface{}{
		"command_plan":       cp.Canonical(),
		"render_plan_sha256": cp.PlanSHA256,
		"ffmpeg_profile":     profile,
	}
	// Determinism chain on the wire: when the master delivered the compiled
	// plan (Fase D), surface its identity so the attempt report shows which
	// compiled plan drove this command. Additive — absent payloads emit none.
	if compiled := compiledPlanEvidence(spec); compiled != nil {
		for key, value := range compiled {
			metrics[key] = value
		}
	}
	return executor.ExecutionResult{
		Status: "succeeded", Outputs: []executor.ArtifactRef{artifact},
		RawMetrics: rawMetrics, Metrics: metrics,
		StartedAt: started, CompletedAt: time.Now().UTC(),
	}, nil
}

func failedResult(started time.Time, code string, err error) executor.ExecutionResult {
	return executor.ExecutionResult{Status: "failed", ErrorCode: code, ErrorDetail: err.Error(), StartedAt: started, CompletedAt: time.Now().UTC()}
}

func artifactFromFile(kind, path string) (executor.ArtifactRef, error) {
	info, err := os.Stat(path)
	if err != nil {
		return executor.ArtifactRef{}, err
	}
	if info.Size() <= 0 {
		return executor.ArtifactRef{}, errors.New("artifact is empty")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return executor.ArtifactRef{}, err
	}
	sum := sha256.Sum256(data)
	return executor.ArtifactRef{Type: kind, Hash: hex.EncodeToString(sum[:]), URI: path, SizeBytes: info.Size()}, nil
}

func planDigest(p *plan.RenderPlan) string {
	data, _ := json.Marshal(p)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func inputArgs(inputs []string) []string {
	args := make([]string, 0, len(inputs)*2)
	for _, input := range inputs {
		args = append(args, "-i", input)
	}
	return args
}

func totalDuration(p *plan.RenderPlan) float64 {
	var total float64
	for _, item := range p.Timeline {
		total += item.DurationSeconds
	}
	return total
}

func sortedInputs(values []string) []string {
	out := append([]string(nil), values...)
	sort.Strings(out)
	return out
}

// NewAudioMix creates the deterministic audio mixer. It consumes the
// shared FFmpegRunner (AudioRecorder surface).
func NewAudioMix(runner ffmpegrunner.FFmpegRunner, outputRoot string) executor.Executor {
	return &audioMixExecutor{renderPlanExecutor: newRenderPlanExecutor(AudioMixID, []string{"audio.mix"}, runner, outputRoot)}
}

type audioMixExecutor struct{ *renderPlanExecutor }

func (e *audioMixExecutor) Execute(ctx context.Context, execCtx executor.ExecutionContext, spec executor.TaskSpec) (executor.ExecutionResult, error) {
	p, err := parseRenderPlanEnvelope(spec)
	started := time.Now().UTC()
	if err != nil {
		return failedResult(started, "validation_failed", err), nil
	}
	cp, err := buildAudioMixPlan(spec, p, e.outputPath(spec, ".audio.wav"))
	if err != nil {
		return failedResult(started, "validation_failed", err), nil
	}
	result, runErr := runCommandExecutor(ctx, e.renderPlanExecutor, spec, cp, "audio.mix", execCtx)
	if result.Metrics == nil {
		result.Metrics = map[string]interface{}{}
	}
	sfxOffsets := make([]float64, 0)
	for _, track := range p.AudioTracks {
		if strings.EqualFold(track.Role, "sfx") || strings.EqualFold(track.Role, "whoosh") {
			sfxOffsets = append(sfxOffsets, track.StartTimeOffset)
		}
	}
	result.Metrics["audio_mix_evidence"] = map[string]interface{}{
		"voiceover_events": countRole(p.AudioTracks, "voiceover"),
		"music_events":     countRole(p.AudioTracks, "music"),
		"sfx_events":       len(sfxOffsets),
		"sfx_timestamps":   sfxOffsets,
	}
	return result, runErr
}

func countRole(tracks []plan.AudioTrack, role string) int {
	count := 0
	for _, track := range tracks {
		if strings.EqualFold(track.Role, role) {
			count++
		}
	}
	return count
}

func buildAudioMixPlan(spec executor.TaskSpec, p *plan.RenderPlan, output string) (CommandPlan, error) {
	if len(p.AudioTracks) == 0 {
		return CommandPlan{}, errors.New("audio_mix: render_plan.audio_tracks is empty")
	}
	inputs := make([]string, 0, len(p.AudioTracks))
	filters := make([]string, 0, len(p.AudioTracks)+2)
	labels := make([]string, len(p.AudioTracks))
	for i, track := range p.AudioTracks {
		inputs = append(inputs, track.SourceURL)
		label := fmt.Sprintf("a%d", i)
		labels[i] = "[" + label + "]"
		volume := track.Volume
		if volume == 0 {
			volume = 1
		}
		chain := fmt.Sprintf("[%d:a]volume=%.6f", i, volume)
		if track.StartTimeOffset > 0 {
			ms := int(track.StartTimeOffset*1000 + 0.5)
			chain += fmt.Sprintf(",adelay=%d|%d", ms, ms)
		}
		if track.DurationSeconds > 0 {
			chain += fmt.Sprintf(",atrim=duration=%.6f", track.DurationSeconds)
		}
		if track.FadeInSeconds > 0 {
			chain += fmt.Sprintf(",afade=t=in:st=0:d=%.6f", track.FadeInSeconds)
		}
		if track.FadeOutSeconds > 0 && track.DurationSeconds > track.FadeOutSeconds {
			start := track.DurationSeconds - track.FadeOutSeconds
			chain += fmt.Sprintf(",afade=t=out:st=%.6f:d=%.6f", start, track.FadeOutSeconds)
		}
		filters = append(filters, chain+labels[i])
	}
	voice := -1
	for i, track := range p.AudioTracks {
		if strings.EqualFold(track.Role, "voiceover") {
			voice = i
			break
		}
	}
	for i, track := range p.AudioTracks {
		if !track.DuckingEnabled {
			continue
		}
		if voice < 0 {
			return CommandPlan{}, errors.New("audio_mix: ducking requires a voiceover track")
		}
		// Split the processed voiceover label so the sidechain and final mix
		// both consume the same declared gain/trim/delay pipeline.
		voiceMix := fmt.Sprintf("[a%d_mix]", voice)
		voiceSide := fmt.Sprintf("[a%d_side]", voice)
		filters = append(filters, labels[voice]+"asplit=2"+voiceMix+voiceSide)
		labels[voice] = voiceMix
		filters = append(filters, labels[i]+voiceSide+fmt.Sprintf("sidechaincompress=threshold=0.05:ratio=8:attack=20:release=300[duck%d]", i))
		labels[i] = fmt.Sprintf("[duck%d]", i)
	}
	mix := strings.Join(labels, "") + fmt.Sprintf("amix=inputs=%d:duration=longest:dropout_transition=0[aout]", len(labels))
	filters = append(filters, mix)
	filterGraph := strings.Join(filters, ";")
	args := append(inputArgs(inputs), "-filter_complex", filterGraph, "-map", "[aout]", "-ar", "48000", "-ac", "2", "-c:a", "pcm_s16le", "-y", output)
	return CommandPlan{ExecutorID: AudioMixID, Inputs: sortedInputs(inputs), FilterComplex: filterGraph, Args: args, OutputPath: output, PlanSHA256: planDigest(p)}, nil
}

// NewCompose creates the deterministic video compositor. It consumes
// the shared FFmpegRunner (SegmentRecorder surface).
func NewCompose(runner ffmpegrunner.FFmpegRunner, outputRoot string) executor.Executor {
	return &composeExecutor{renderPlanExecutor: newRenderPlanExecutor(ComposeID, []string{"video.compose"}, runner, outputRoot)}
}

type composeExecutor struct{ *renderPlanExecutor }

func (e *composeExecutor) Execute(ctx context.Context, execCtx executor.ExecutionContext, spec executor.TaskSpec) (executor.ExecutionResult, error) {
	p, err := parseRenderPlanEnvelope(spec)
	started := time.Now().UTC()
	if err != nil {
		return failedResult(started, "validation_failed", err), nil
	}
	cp, err := buildComposePlan(spec, p, e.outputPath(spec, ".compose.mp4"))
	if err != nil {
		return failedResult(started, "validation_failed", err), nil
	}
	return runCommandExecutor(ctx, e.renderPlanExecutor, spec, cp, "video.compose", execCtx)
}
func buildComposePlan(spec executor.TaskSpec, p *plan.RenderPlan, output string) (CommandPlan, error) {
	inputs := make([]string, 0, len(p.Timeline))
	for _, item := range p.Timeline {
		if item.Source.URL == "" && item.Source.CacheKey == "" {
			return CommandPlan{}, errors.New("compose: timeline source needs url or cache_key")
		}
		source := item.Source.URL
		if source == "" {
			source = item.Source.CacheKey
		}
		inputs = append(inputs, source)
	}
	if len(inputs) == 0 {
		return CommandPlan{}, errors.New("compose: timeline is empty")
	}
	labels := make([]string, len(inputs))
	filters := make([]string, len(inputs))
	for i := range p.Timeline {
		labels[i] = fmt.Sprintf("[v%d]", i)
		filters[i] = fmt.Sprintf("[%d:v]setpts=PTS-STARTPTS,fps=%d,scale=%d:%d:force_original_aspect_ratio=decrease,pad=%d:%d:(ow-iw)/2:(oh-ih)/2[v%d]", i, p.Canvas.Fps, p.Canvas.Width, p.Canvas.Height, p.Canvas.Width, p.Canvas.Height, i)
	}
	filters = append(filters, strings.Join(labels, "")+fmt.Sprintf("concat=n=%d:v=1:a=0[vout]", len(labels)))
	filterGraph := strings.Join(filters, ";")
	args := append(inputArgs(inputs), "-filter_complex", filterGraph, "-map", "[vout]", "-an", "-c:v", "libx264", "-pix_fmt", "yuv420p", "-r", fmt.Sprintf("%d", p.Canvas.Fps), "-y", output)
	return CommandPlan{ExecutorID: ComposeID, Inputs: sortedInputs(inputs), FilterComplex: filterGraph, Args: args, OutputPath: output, PlanSHA256: planDigest(p)}, nil
}

// NewEncode creates the deterministic final encoder. It consumes the
// shared FFmpegRunner (MuxRecorder surface).
func NewEncode(runner ffmpegrunner.FFmpegRunner, outputRoot string) executor.Executor {
	return &encodeExecutor{renderPlanExecutor: newRenderPlanExecutor(EncodeID, []string{"video.output"}, runner, outputRoot)}
}

type encodeExecutor struct{ *renderPlanExecutor }

func (e *encodeExecutor) Execute(ctx context.Context, execCtx executor.ExecutionContext, spec executor.TaskSpec) (executor.ExecutionResult, error) {
	p, err := parseRenderPlanEnvelope(spec)
	started := time.Now().UTC()
	if err != nil {
		return failedResult(started, "validation_failed", err), nil
	}
	input, _ := spec.Payload["input_path"].(string)
	if strings.TrimSpace(input) == "" {
		input, _ = spec.Payload["compose_path"].(string)
	}
	if strings.TrimSpace(input) == "" {
		return failedResult(started, "validation_failed", errors.New("encode: input_path or compose_path is required")), nil
	}
	output := e.outputPath(spec, ".mp4")
	args := []string{"-i", input}
	inputs := []string{input}
	if audioPath, _ := spec.Payload["audio_mix_path"].(string); strings.TrimSpace(audioPath) != "" {
		args = append(args, "-i", audioPath)
		inputs = append(inputs, audioPath)
	}
	args = append(args, "-map", "0:v:0")
	if len(inputs) == 2 {
		args = append(args, "-map", "1:a:0")
	}
	if subtitlePath, _ := spec.Payload["subtitle_path"].(string); strings.TrimSpace(subtitlePath) != "" {
		args = append(args, "-vf", "ass="+subtitlePath)
	}
	args = append(args, "-c:v", "libx264", "-preset", "medium", "-pix_fmt", "yuv420p", "-c:a", "aac", "-ar", "48000", "-ac", "2", "-movflags", "+faststart", "-y", output)
	cp := CommandPlan{ExecutorID: EncodeID, Inputs: sortedInputs(inputs), Args: args, OutputPath: output, PlanSHA256: planDigest(p)}
	return runCommandExecutor(ctx, e.renderPlanExecutor, spec, cp, "video.output", execCtx)
}
