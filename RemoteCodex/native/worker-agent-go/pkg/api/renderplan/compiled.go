package renderplan

// compiled.go — the worker-facing view of the MASTER-COMPILED render plan
// (Fase D). The master RenderPlanCompiler interprets the normalized payload
// (scenes, assets, trims, voiceover, timeline) into a single canonical
// CompiledRenderPlan document and delivers it inside the TaskOffer payload
// under contract.PayloadKeyCompiledRenderPlanJSON (+ *_SHA = the same
// SHA256 stamped on task_attempts.plan_sha256).
//
// Contract guarantees mirrored from the master document:
//   - NO local paths. Assets are referenced ONLY by asset_id (+ sha256);
//     local path resolution stays the CacheResolver's responsibility.
//   - plan_version is the master schema version (CompiledPlanVersion);
//     bump ONLY with a master migration.
//
// The batch FFmpeg path consumes segments[] directly from this document
// instead of re-deriving a timeline from raw scenes.

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"velox-shared/contract"
)

// CompiledPlanVersion is the canonical master plan schema version delivered
// in the compiled envelope. Bump ONLY with a master migration; consumers
// compare against this constant.
const CompiledPlanVersion = 1

// CompiledMediaContract is the output encoding contract the compiled plan
// commits to (mirror of DataServer/internal/renderplan.MediaContract).
type CompiledMediaContract struct {
	CopyOnly   bool   `json:"copy_only,omitempty"`
	VideoCodec string `json:"video_codec,omitempty"`
	Width      int    `json:"width,omitempty"`
	Height     int    `json:"height,omitempty"`
	FpsNum     int    `json:"fps_num,omitempty"`
	FpsDen     int    `json:"fps_den,omitempty"`
}

// CompiledSegment is one compiled timeline segment (mirror of
// DataServer/internal/renderplan.Segment): which asset plays, from which
// source window, starting at which absolute timeline offset.
type CompiledSegment struct {
	SegmentID       string `json:"segment_id"`
	AssetID         string `json:"asset_id"`
	AssetSHA256     string `json:"asset_sha256,omitempty"`
	SourceInMS      int64  `json:"source_in_ms,omitempty"`
	SourceOutMS     int64  `json:"source_out_ms,omitempty"`
	TimelineStartMS int64  `json:"timeline_start_ms"`
}

// CompiledRenderPlan is the canonical, master-compiled execution plan for a
// single attempt (mirror of DataServer/internal/renderplan.CompiledRenderPlan).
// Field order mirrors the master struct so the canonical JSON (and therefore
// plan_sha256) is byte-stable across the wire.
type CompiledRenderPlan struct {
	PlanVersion   int                  `json:"plan_version"`
	JobID         string               `json:"job_id"`
	AttemptID     string               `json:"attempt_id"`
	DurationMS    int64                `json:"duration_ms"`
	MediaContract CompiledMediaContract `json:"media_contract"`
	Segments      []CompiledSegment    `json:"segments"`
	// Audio and Assets are accepted (they carry the asset identity +
	// integrity references the batch path resolves via the CacheResolver)
	// but are not yet validated in depth.
	Audio  []map[string]interface{} `json:"audio,omitempty"`
	Assets []map[string]interface{} `json:"assets,omitempty"`
}

// Validate checks the compiled document invariants. A delivered plan must
// pass this before it is treated as canonical input for the batch path.
func (p *CompiledRenderPlan) Validate() error {
	if p == nil {
		return planError(ERR_PLAN_REQUIRED_FIELD, "compiled_render_plan_json", "plan is required")
	}
	if p.PlanVersion != CompiledPlanVersion {
		return planError(ERR_PLAN_UNSUPPORTED_VERSION, "plan_version",
			fmt.Sprintf("unsupported compiled plan version %d (want %d)", p.PlanVersion, CompiledPlanVersion))
	}
	if strings.TrimSpace(p.JobID) == "" {
		return planError(ERR_PLAN_REQUIRED_FIELD, "job_id", "is required")
	}
	if strings.TrimSpace(p.AttemptID) == "" {
		return planError(ERR_PLAN_REQUIRED_FIELD, "attempt_id", "is required")
	}
	mc := p.MediaContract
	if mc.Width <= 0 || mc.Height <= 0 || mc.FpsNum <= 0 || mc.FpsDen <= 0 {
		return planError(ERR_PLAN_REQUIRED_FIELD, "media_contract",
			"width, height, fps_num and fps_den must be positive")
	}
	if len(p.Segments) == 0 {
		return planError(ERR_PLAN_REQUIRED_FIELD, "segments", "must be a non-empty array")
	}
	for i, seg := range p.Segments {
		if strings.TrimSpace(seg.SegmentID) == "" || strings.TrimSpace(seg.AssetID) == "" {
			return planError(ERR_PLAN_REQUIRED_FIELD, fmt.Sprintf("segments[%d]", i),
				"segment_id and asset_id are required")
		}
		if seg.TimelineStartMS < 0 || seg.SourceInMS < 0 || seg.SourceOutMS < 0 {
			return planError(ERR_PLAN_SCHEMA, fmt.Sprintf("segments[%d]", i),
				"timeline/source offsets must not be negative")
		}
	}
	return nil
}

// DecodeCompiledRenderPlan parses the canonical compiled-plan document
// delivered in the TaskOffer payload and validates it. It uses a regular
// (non-strict) decode so future additive master fields do not break older
// workers; the required-field validation above is the contract gate.
func DecodeCompiledRenderPlan(data string) (*CompiledRenderPlan, error) {
	if strings.TrimSpace(data) == "" {
		return nil, planError(ERR_PLAN_REQUIRED_FIELD, "compiled_render_plan_json", "document is required")
	}
	var plan CompiledRenderPlan
	if err := json.Unmarshal([]byte(data), &plan); err != nil {
		return nil, planError(ERR_PLAN_SCHEMA, "compiled_render_plan_json",
			fmt.Sprintf("invalid compiled plan JSON: %v", err))
	}
	if err := plan.Validate(); err != nil {
		return nil, err
	}
	return &plan, nil
}

// ValidateCompiledRenderPlan is the admission gate for a TaskOffer payload
// that carries the master-compiled plan. It validates the compiled document
// when present and returns nil when the payload does not carry one (legacy
// fleet). The delivered sha is format-checked when present — it is the same
// SHA256 the master stamped on task_attempts.plan_sha256.
func ValidateCompiledRenderPlan(raw map[string]interface{}) error {
	if raw == nil {
		return nil
	}
	rawJSON, ok := raw[contract.PayloadKeyCompiledRenderPlanJSON].(string)
	if !ok || strings.TrimSpace(rawJSON) == "" {
		return nil
	}
	if _, err := DecodeCompiledRenderPlan(rawJSON); err != nil {
		return err
	}
	if sha, ok := raw[contract.PayloadKeyCompiledRenderPlanSHA].(string); ok && strings.TrimSpace(sha) != "" {
		trimmed := strings.TrimSpace(sha)
		if len(trimmed) != 64 {
			return planError(ERR_PLAN_SCHEMA, "compiled_render_plan_sha256", "must be a 64-char hex SHA256")
		}
		if _, err := hex.DecodeString(trimmed); err != nil {
			return planError(ERR_PLAN_SCHEMA, "compiled_render_plan_sha256", "must be a 64-char hex SHA256")
		}
	}
	return nil
}
