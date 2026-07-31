package executors

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/pkg/video/plan"
)

const (
	SubtitleAlignID = "subtitle_align"
	AudioMixID      = "audio_mix"
	ComposeID       = "compose"
	EncodeID        = "encode"
)

const renderPlanVersion = 1

// CommandPlan is the immutable, deterministic description of one external
// render operation. Tests assert this value instead of depending on ffmpeg.
type CommandPlan struct {
	ExecutorID    string
	Inputs        []string
	FilterComplex string
	Args          []string
	OutputPath    string
	PlanSHA256    string
}

func (p CommandPlan) Canonical() string {
	b, _ := json.Marshal(p)
	return string(b)
}

// CommandRunner is injected in tests and keeps command construction separate
// from process execution.
type CommandRunner interface {
	Run(context.Context, []string) error
}

type ffmpegCommandRunner struct{}

func (ffmpegCommandRunner) Run(ctx context.Context, args []string) error {
	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

type renderPlanExecutor struct {
	descriptor executor.Descriptor
	runner     CommandRunner
	outputRoot string
}

func newRenderPlanExecutor(id string, outputTypes []string, runner CommandRunner, outputRoot string) *renderPlanExecutor {
	if runner == nil {
		runner = ffmpegCommandRunner{}
	}
	if strings.TrimSpace(outputRoot) == "" {
		outputRoot = filepath.Join(os.TempDir(), "velox", "render-plan")
	}
	return &renderPlanExecutor{
		descriptor: executor.Descriptor{
			ID:            id,
			Version:       1,
			InputTypes:    []string{"render.plan.v1"},
			OutputTypes:   outputTypes,
			ResourceClass: executor.ResourceCPU,
			Deterministic: true,
			Cacheable:     true,
			TemporalMode:  executor.TemporalGlobal,
		},
		runner:     runner,
		outputRoot: outputRoot,
	}
}

func (e *renderPlanExecutor) Descriptor() executor.Descriptor { return e.descriptor }

func (e *renderPlanExecutor) Validate(spec executor.TaskSpec) error {
	_, err := parseRenderPlanEnvelope(spec)
	return err
}

func parseRenderPlanEnvelope(spec executor.TaskSpec) (*plan.RenderPlan, error) {
	if spec.Payload == nil {
		return nil, errors.New("render-plan executor: payload is required")
	}
	allowed := map[string]bool{
		"render_plan": true, "render_plan_json": true,
		"input_path": true, "compose_path": true, "audio_mix_path": true,
		"subtitle_path": true, "output_path": true,
	}
	for key := range spec.Payload {
		if !allowed[key] {
			return nil, fmt.Errorf("render-plan executor: unsupported payload key %q; timeline must come from render_plan", key)
		}
	}
	var raw []byte
	if value, ok := spec.Payload["render_plan_json"].(string); ok && strings.TrimSpace(value) != "" {
		raw = []byte(value)
	} else if value, ok := spec.Payload["render_plan"]; ok {
		var err error
		raw, err = json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("render-plan executor: marshal render_plan: %w", err)
		}
	} else {
		return nil, errors.New("render-plan executor: render_plan or render_plan_json is required")
	}
	var p plan.RenderPlan
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&p); err != nil {
		return nil, fmt.Errorf("render-plan executor: strict decode: %w", err)
	}
	var trailing interface{}
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return nil, errors.New("render-plan executor: strict decode: trailing data")
		}
		return nil, fmt.Errorf("render-plan executor: strict decode: %w", err)
	}
	if err := validateRenderPlan(&p, spec.JobID); err != nil {
		return nil, err
	}
	// Marshal/unmarshal gives callers a detached value. Executors never mutate
	// the transport map or share plan pointers between tasks.
	canonical, err := json.Marshal(&p)
	if err != nil {
		return nil, fmt.Errorf("render-plan executor: canonicalize: %w", err)
	}
	var detached plan.RenderPlan
	if err := json.Unmarshal(canonical, &detached); err != nil {
		return nil, fmt.Errorf("render-plan executor: detach: %w", err)
	}
	return &detached, nil
}

func validateRenderPlan(p *plan.RenderPlan, taskJobID string) error {
	if p.Version != renderPlanVersion {
		return fmt.Errorf("render-plan executor: version must be %d (got %d)", renderPlanVersion, p.Version)
	}
	if strings.TrimSpace(p.JobID) == "" || (taskJobID != "" && p.JobID != taskJobID) {
		return fmt.Errorf("render-plan executor: job_id must match task (%q)", taskJobID)
	}
	if p.Canvas.Width <= 0 || p.Canvas.Height <= 0 || p.Canvas.Fps <= 0 {
		return errors.New("render-plan executor: canvas width, height and fps must be positive")
	}
	if len(p.Timeline) == 0 {
		return errors.New("render-plan executor: timeline must not be empty")
	}
	for i, item := range p.Timeline {
		if item.DurationSeconds <= 0 || strings.TrimSpace(item.Source.Type) == "" {
			return fmt.Errorf("render-plan executor: timeline[%d] has invalid source or duration", i)
		}
	}
	for i, track := range p.AudioTracks {
		if strings.TrimSpace(track.SourceURL) == "" {
			return fmt.Errorf("render-plan executor: audio_tracks[%d].source_url is required", i)
		}
		if track.Volume < 0 {
			return fmt.Errorf("render-plan executor: audio_tracks[%d].volume must not be negative", i)
		}
	}
	endOfTimeline := totalDuration(p)
	for i, subtitle := range p.Subtitles {
		if len(subtitle.Events) == 0 {
			return fmt.Errorf("render-plan executor: subtitle_tracks[%d] requires aligned events", i)
		}
		var previousEnd float64
		for j, event := range subtitle.Events {
			if event.EndSeconds <= event.StartSeconds || event.StartSeconds < 0 || strings.TrimSpace(event.Text) == "" {
				return fmt.Errorf("render-plan executor: subtitle_tracks[%d].events[%d] is invalid", i, j)
			}
			if event.EndSeconds-event.StartSeconds < 0.5 {
				return fmt.Errorf("render-plan executor: subtitle_tracks[%d].events[%d] is shorter than 500ms", i, j)
			}
			if event.StartSeconds < previousEnd {
				return fmt.Errorf("render-plan executor: subtitle_tracks[%d].events[%d] overlaps previous event", i, j)
			}
			if event.EndSeconds > endOfTimeline {
				return fmt.Errorf("render-plan executor: subtitle_tracks[%d].events[%d] exceeds timeline", i, j)
			}
			if strings.Count(event.Text, "\\n") > 1 {
				return fmt.Errorf("render-plan executor: subtitle_tracks[%d].events[%d] exceeds two lines", i, j)
			}
			previousEnd = event.EndSeconds
		}
	}
	return nil
}

func (e *renderPlanExecutor) outputPath(spec executor.TaskSpec, suffix string) string {
	return filepath.Join(e.outputRoot, spec.JobID+suffix)
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
	if err := e.runner.Run(ctx, cp.Args); err != nil {
		commandHandle.Abort("command_failed", err.Error())
		return failedResult(started, "command_failed", err), nil
	}
	commandHandle.SetMetadata("executor_id", cp.ExecutorID)
	commandHandle.CompleteWith(0, 0, 0, telemetry.StatusOK, "", "")
	artifact, err := artifactFromFile(outputType, cp.OutputPath)
	if err != nil {
		return failedResult(started, "artifact_invalid", err), nil
	}
	return executor.ExecutionResult{
		Status: "succeeded", Outputs: []executor.ArtifactRef{artifact},
		Metrics:   map[string]interface{}{"command_plan": cp.Canonical(), "render_plan_sha256": cp.PlanSHA256},
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

// RegisterRenderPlanExecutors installs the four canonical RenderPlan-only
// executors. Registration is deliberately centralized so worker capabilities
// and dispatch resolve the same implementations.
func RegisterRenderPlanExecutors(reg *executor.Registry, outputRoot string) error {
	if reg == nil {
		return errors.New("render-plan executors: registry is nil")
	}
	for _, item := range []executor.Executor{
		NewSubtitleAlign(nil, outputRoot), NewAudioMix(nil, outputRoot),
		NewCompose(nil, outputRoot), NewEncode(nil, outputRoot),
	} {
		if err := reg.Register(item); err != nil {
			return err
		}
	}
	return nil
}

// NewSubtitleAlign creates the deterministic subtitle executor.
func NewSubtitleAlign(runner CommandRunner, outputRoot string) executor.Executor {
	return &subtitleAlignExecutor{renderPlanExecutor: newRenderPlanExecutor(SubtitleAlignID, []string{"subtitle.ass"}, runner, outputRoot)}
}

type subtitleAlignExecutor struct{ *renderPlanExecutor }

func (e *subtitleAlignExecutor) Execute(_ context.Context, _ executor.ExecutionContext, spec executor.TaskSpec) (executor.ExecutionResult, error) {
	started := time.Now().UTC()
	p, err := parseRenderPlanEnvelope(spec)
	if err != nil {
		return failedResult(started, "validation_failed", err), nil
	}
	var b strings.Builder
	b.WriteString("[Script Info]\nScriptType: v4.00+\n\n[V4+ Styles]\nFormat: Name, Fontname, Fontsize, PrimaryColour, BackColour, Bold, Italic, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding\nStyle: Default,Arial,42,&H00FFFFFF,&H80000000,0,0,1,2,0,2,40,40,40,1\n\n[Events]\nFormat: Layer, Start, End, Style, Text\n")
	for _, track := range p.Subtitles {
		for _, event := range track.Events {
			b.WriteString(fmt.Sprintf("Dialogue: 0,%s,%s,Default,%s\n", assTime(event.StartSeconds), assTime(event.EndSeconds), strings.ReplaceAll(event.Text, "\n", "\\N")))
		}
	}
	path := e.outputPath(spec, ".subtitles.ass")
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return failedResult(started, "output_directory", err), nil
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o640); err != nil {
		return failedResult(started, "artifact_write", err), nil
	}
	artifact, err := artifactFromFile("subtitle.ass", path)
	if err != nil {
		return failedResult(started, "artifact_invalid", err), nil
	}
	return executor.ExecutionResult{Status: "succeeded", Outputs: []executor.ArtifactRef{artifact}, Metrics: map[string]interface{}{"caption_count": strings.Count(b.String(), "Dialogue: "), "render_plan_sha256": planDigest(p)}, StartedAt: started, CompletedAt: time.Now().UTC()}, nil
}

func assTime(seconds float64) string {
	if seconds < 0 {
		seconds = 0
	}
	h := int(seconds / 3600)
	m := int(seconds/60) % 60
	s := int(seconds) % 60
	cs := int(seconds*100+0.5) % 100
	return fmt.Sprintf("%d:%02d:%02d.%02d", h, m, s, cs)
}

// NewAudioMix creates the deterministic audio mixer.
func NewAudioMix(runner CommandRunner, outputRoot string) executor.Executor {
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

// NewCompose creates the deterministic video compositor.
func NewCompose(runner CommandRunner, outputRoot string) executor.Executor {
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

// NewEncode creates the deterministic final encoder.
func NewEncode(runner CommandRunner, outputRoot string) executor.Executor {
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

func recorderFromExecutionContext(execCtx executor.ExecutionContext) *telemetry.EventRecorder {
	if provider, ok := execCtx.(interface {
		Recorder() *telemetry.EventRecorder
	}); ok {
		return provider.Recorder()
	}
	return nil
}

// Keep totalDuration part of the common contract and make it observable in
// tests/callers without allowing executor-specific wall-clock decisions.
var _ = totalDuration
