package executors

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/pkg/api/renderplan"
	"velox-worker-agent/pkg/video/ffmpegrunner"
	"velox-worker-agent/pkg/video/plan"

	"velox-shared/contract"
)

const (
	SubtitleAlignID = "subtitle_align"
	AudioMixID      = "audio_mix"
	ComposeID       = "compose"
	EncodeID        = "encode"
)

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
	// The v1 render_plan/render_plan_json envelope drives the commands today;
	// a payload without it is not executable by this executor family.
	if _, err := parseRenderPlanEnvelope(spec); err != nil {
		return err
	}
	// Master-compiled plan (Fase D) is additive on top of the v1 envelope for
	// now. When the payload carries it, both the document and its stamped
	// identity hash must be valid; the v1 executor must never report evidence
	// for a different plan.
	if raw, ok := spec.Payload[contract.PayloadKeyCompiledRenderPlanJSON].(string); ok && strings.TrimSpace(raw) != "" {
		if _, err := parseCompiledRenderPlanEnvelope(spec); err != nil {
			return err
		}
	}
	return nil
}

// renderPlanPayloadKeys is the closed set of payload keys the render-plan
// executors accept. Timeline must come from render_plan/render_plan_json
// (v1) or the master-compiled plan (Fase D); everything else is rejected.
var renderPlanPayloadKeys = map[string]bool{
	"render_plan": true, "render_plan_json": true,
	// Master-compiled plan (Fase D) delivered at claim: the batch FFmpeg
	// path consumes compiled segments directly from this document.
	contract.PayloadKeyCompiledRenderPlanJSON: true,
	contract.PayloadKeyCompiledRenderPlanSHA:  true,
	"input_path":                              true, "compose_path": true, "audio_mix_path": true,
	"subtitle_path": true, "output_path": true,
}

func checkRenderPlanPayloadKeys(spec executor.TaskSpec) error {
	if spec.Payload == nil {
		return errors.New("render-plan executor: payload is required")
	}
	for key := range spec.Payload {
		if !renderPlanPayloadKeys[key] {
			return fmt.Errorf("render-plan executor: unsupported payload key %q; timeline must come from render_plan", key)
		}
	}
	return nil
}

func parseRenderPlanEnvelope(spec executor.TaskSpec) (*plan.RenderPlan, error) {
	if err := checkRenderPlanPayloadKeys(spec); err != nil {
		return nil, err
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
	p, err := plan.DecodeJSON(raw)
	if err != nil {
		return nil, fmt.Errorf("render-plan executor: %w", err)
	}
	if err := p.ValidateForJob(spec.JobID); err != nil {
		return nil, fmt.Errorf("render-plan executor: %w", err)
	}
	return p, nil
}

// parseCompiledRenderPlanEnvelope parses the master-compiled render plan
// (Fase D) delivered in the TaskOffer payload under
// contract.PayloadKeyCompiledRenderPlanJSON. It returns nil when the payload
// does not carry a compiled plan, so callers can treat the compiled plan as
// additive evidence on top of the v1 envelope.
func parseCompiledRenderPlanEnvelope(spec executor.TaskSpec) (*renderplan.CompiledRenderPlan, error) {
	if err := checkRenderPlanPayloadKeys(spec); err != nil {
		return nil, err
	}
	plan, err := renderplan.DecodeCompiledRenderPlanPayload(spec.Payload)
	if err != nil {
		return nil, fmt.Errorf("render-plan executor: compiled plan: %w", err)
	}
	if plan == nil {
		return nil, errors.New("render-plan executor: compiled_render_plan_json is required")
	}
	if spec.JobID != "" && plan.JobID != spec.JobID {
		return nil, fmt.Errorf("render-plan executor: compiled plan job_id %q must match task job %q", plan.JobID, spec.JobID)
	}
	return plan, nil
}

// compiledPlanEvidence returns the sanitized identity of the master-compiled
// plan when the payload carries one: the delivered plan_sha256 (the same
// value the master stamped on task_attempts.plan_sha256), its schema version
// and segment count. nil when absent — the compiled plan is additive today.
func compiledPlanEvidence(spec executor.TaskSpec) map[string]interface{} {
	plan, err := parseCompiledRenderPlanEnvelope(spec)
	if err != nil || plan == nil {
		return nil
	}
	evidence := map[string]interface{}{
		"compiled_render_plan_version":     plan.PlanVersion,
		"compiled_render_plan_segments":    len(plan.Segments),
		"compiled_render_plan_duration_ms": plan.DurationMS,
	}
	if sha, ok := spec.Payload[contract.PayloadKeyCompiledRenderPlanSHA].(string); ok && strings.TrimSpace(sha) != "" {
		evidence["compiled_render_plan_sha256"] = strings.TrimSpace(sha)
	}
	return evidence
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
			b.WriteString("Dialogue: 0,")
			writeASSTime(&b, event.StartSeconds)
			b.WriteByte(',')
			writeASSTime(&b, event.EndSeconds)
			b.WriteString(",Default,")
			writeASSText(&b, event.Text)
			b.WriteByte('\n')
		}
	}
	path := e.outputPath(spec, ".subtitles.ass")
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return failedResult(started, "output_directory", err), nil
	}
	// Materialize the .ass document once: b.String() copies the builder's
	// buffer, so calling it twice (once for the write, once for caption_count)
	// would pay a second full-document allocation per render.
	ass := b.String()
	if err := os.WriteFile(path, []byte(ass), 0o640); err != nil {
		return failedResult(started, "artifact_write", err), nil
	}
	artifact, err := artifactFromFile("subtitle.ass", path)
	if err != nil {
		return failedResult(started, "artifact_invalid", err), nil
	}
	return executor.ExecutionResult{Status: "succeeded", Outputs: []executor.ArtifactRef{artifact}, Metrics: map[string]interface{}{"caption_count": strings.Count(ass, "Dialogue: "), "render_plan_sha256": planDigest(p)}, StartedAt: started, CompletedAt: time.Now().UTC()}, nil
}

// writeASSTime writes an h:mm:ss.cc ASS timestamp directly into b, avoiding
// the format allocation incurred per subtitle event by a fmt.Sprintf call.
func writeASSTime(b *strings.Builder, seconds float64) {
	if seconds < 0 {
		seconds = 0
	}
	writeInt(b, int(seconds/3600))
	b.WriteByte(':')
	writeFixed2(b, int(seconds/60)%60)
	b.WriteByte(':')
	writeFixed2(b, int(seconds)%60)
	b.WriteByte('.')
	writeFixed2(b, int(seconds*100+0.5)%100)
}

// writeFixed2 writes a two-digit zero-padded number (values >=100 are written
// verbatim, since ASS tolerates hours > 99).
func writeFixed2(b *strings.Builder, v int) {
	if v < 10 {
		b.WriteByte('0')
	}
	writeInt(b, v)
}

func writeInt(b *strings.Builder, v int) {
	// AppendInt into a stack buffer avoids the per-call string allocation
	// that strconv.Itoa would incur on the hot per-segment/per-event/per-track
	// builder loops (same pattern as writeFloat6).
	var buf [24]byte
	b.Write(strconv.AppendInt(buf[:0], int64(v), 10))
}

// writeASSText writes the subtitle text with newlines escaped to \N. The
// replacement is guarded on Contains so text without newlines skips both the
// scan and the extra allocation of ReplaceAll.
func writeASSText(b *strings.Builder, text string) {
	if strings.Contains(text, "\n") {
		text = strings.ReplaceAll(text, "\n", "\\N")
	}
	b.WriteString(text)
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
