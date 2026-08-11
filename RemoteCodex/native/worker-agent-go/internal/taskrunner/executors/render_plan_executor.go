package executors

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/pkg/video/ffmpegrunner"
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

// The three ffmpeg-driving executors — audioMix (AudioRecorder surface),
// compose (SegmentRecorder surface) and encode (MuxRecorder surface) —
// MUST consume the single canonical FFmpegRunner so process profiling is
// measured once, centrally, instead of each phase implementing its own
// exec/profile code. The runner is injected for tests and keeps command
// construction separate from process execution.
type renderPlanExecutor struct {
	descriptor executor.Descriptor
	runner     ffmpegrunner.FFmpegRunner
	outputRoot string
}

func newRenderPlanExecutor(id string, outputTypes []string, runner ffmpegrunner.FFmpegRunner, outputRoot string) *renderPlanExecutor {
	if runner == nil {
		runner = ffmpegrunner.NewProcessRunner()
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
		if track.Loop && track.DurationSeconds < 0 {
			return fmt.Errorf("render-plan executor: audio_tracks[%d].duration_seconds must be non-negative", i)
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

// NewSubtitleAlign creates the deterministic subtitle executor. It does
// not invoke ffmpeg (it writes the .ass file directly), but accepts the
// same runner type so the executor surface stays uniform.
func NewSubtitleAlign(runner ffmpegrunner.FFmpegRunner, outputRoot string) executor.Executor {
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
