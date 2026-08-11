// Package renderplan — canonical master-compiled render plan (Fase D).
//
// The master interprets the normalized worker payload (scenes, assets,
// trims, voiceover, timeline) into a single CompiledRenderPlan document:
// plan_version, job_id, attempt_id, duration_ms, media_contract,
// segments[], audio[] and a deduplicated assets[] list.
//
// Contract guarantees:
//
//   - NO local paths. Assets are referenced ONLY by asset_id (plus sha256
//     and registry metadata when available). Local path resolution stays
//     the CacheResolver's responsibility on the worker.
//   - NO downloads / no prefetch. Compile is pure interpretation over the
//     payload; the only optional I/O is best-effort registry-metadata
//     enrichment (MetadataResolver) — never blob transfer.
//   - Determinism. plan_sha256 = SHA256(canonical JSON) is stable for the
//     same payload + attempt_id, so the full chain
//     job→attempt→plan_version→plan_sha256→renderer_version→artifact_sha256
//     is reconstructable for any attempt.
package renderplan

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// PlanVersion is the canonical compiled-plan schema version stamped into
// every plan and persisted on the attempt row. Bump ONLY with a migration;
// consumers compare against this constant.
const PlanVersion = 1

// CompiledRenderPlan is the canonical, master-compiled execution plan for a
// single attempt. Field order is deliberate: it is the canonical JSON order
// and therefore the byte order of plan_sha256.
type CompiledRenderPlan struct {
	PlanVersion   int           `json:"plan_version"`
	JobID         string        `json:"job_id"`
	AttemptID     string        `json:"attempt_id"`
	DurationMS    int64         `json:"duration_ms"`
	MediaContract MediaContract `json:"media_contract"`
	Segments      []Segment     `json:"segments"`
	Audio         []AudioTrack  `json:"audio"`
	Assets        []AssetRef    `json:"assets"`
	// RendererVersion and ArtifactSHA256 are stamped by the worker report
	// (renderer_version / artifact_sha256 complete the determinism chain);
	// they are empty at compile time.
	RendererVersion string `json:"renderer_version,omitempty"`
	ArtifactSHA256  string `json:"artifact_sha256,omitempty"`
}

// MediaContract is the output encoding contract the compiled plan commits to.
type MediaContract struct {
	CopyOnly   bool   `json:"copy_only,omitempty"`
	VideoCodec string `json:"video_codec,omitempty"`
	Width      int    `json:"width,omitempty"`
	Height     int    `json:"height,omitempty"`
	FpsNum     int    `json:"fps_num,omitempty"`
	FpsDen     int    `json:"fps_den,omitempty"`
}

// Segment is one timeline segment: which asset plays, from which source
// window, starting at which timeline offset. TimelineStartMS is always
// absolute in the output timeline. source_in/source_out_ms are the trim
// window inside the source asset (0/absent means "use the full asset").
type Segment struct {
	SegmentID       string `json:"segment_id"`
	AssetID         string `json:"asset_id"`
	AssetSHA256     string `json:"asset_sha256,omitempty"`
	SourceInMS      int64  `json:"source_in_ms,omitempty"`
	SourceOutMS     int64  `json:"source_out_ms,omitempty"`
	TimelineStartMS int64  `json:"timeline_start_ms"`
}

// AudioTrack is one audio source mixed into the output. Only local asset
// references (velox-asset://) carry an asset_id; deferred Drive references
// remain in the worker payload and are resolved by the worker bridge.
type AudioTrack struct {
	AssetID     string  `json:"asset_id"`
	AssetSHA256 string  `json:"asset_sha256,omitempty"`
	Role        string  `json:"role,omitempty"`
	StartMS     int64   `json:"start_ms,omitempty"`
	DurationMS  int64   `json:"duration_ms,omitempty"`
	Volume      float64 `json:"volume,omitempty"`
	Loop        bool    `json:"loop,omitempty"`
	FadeInMS    int64   `json:"fade_in_ms,omitempty"`
	FadeOutMS   int64   `json:"fade_out_ms,omitempty"`
}

// AssetRef is the deduplicated registry-facing description of every asset
// referenced by the plan. It carries identity + integrity only — never a
// filesystem path.
type AssetRef struct {
	AssetID    string `json:"asset_id"`
	SHA256     string `json:"sha256,omitempty"`
	Kind       string `json:"kind,omitempty"`
	MimeType   string `json:"mime_type,omitempty"`
	SizeBytes  int64  `json:"size_bytes,omitempty"`
	DurationMS int64  `json:"duration_ms,omitempty"`
	Width      int    `json:"width,omitempty"`
	Height     int    `json:"height,omitempty"`
}

// Validate checks the plan document invariants. A persisted plan must pass
// this before it is treated as canonical.
func (p *CompiledRenderPlan) Validate() error {
	if p == nil {
		return fmt.Errorf("render plan: nil plan")
	}
	if p.PlanVersion != PlanVersion {
		return fmt.Errorf("render plan: unsupported plan_version %d", p.PlanVersion)
	}
	if strings.TrimSpace(p.JobID) == "" {
		return fmt.Errorf("render plan: job_id is required")
	}
	if strings.TrimSpace(p.AttemptID) == "" {
		return fmt.Errorf("render plan: attempt_id is required")
	}
	mc := p.MediaContract
	if mc.Width <= 0 || mc.Height <= 0 || mc.FpsNum <= 0 || mc.FpsDen <= 0 {
		return fmt.Errorf("render plan: media contract requires positive width, height and fps")
	}
	return nil
}

// CanonicalJSON returns the deterministic byte serialization of the plan:
// fixed struct field order and assets sorted by asset_id. Two plans with
// identical semantics produce identical bytes, which is the basis of
// plan_sha256.
func (p *CompiledRenderPlan) CanonicalJSON() ([]byte, error) {
	if p == nil {
		return nil, fmt.Errorf("render plan: nil plan")
	}
	canonical := *p
	canonical.Assets = append([]AssetRef(nil), p.Assets...)
	sort.SliceStable(canonical.Assets, func(i, j int) bool {
		return canonical.Assets[i].AssetID < canonical.Assets[j].AssetID
	})
	data, err := json.Marshal(&canonical)
	if err != nil {
		return nil, fmt.Errorf("render plan: canonical marshal: %w", err)
	}
	return data, nil
}

// PlanSHA256 returns the deterministic plan hash: SHA256 over the canonical
// JSON. This is the value persisted on the attempt row (plan_sha256).
func (p *CompiledRenderPlan) PlanSHA256() (string, error) {
	data, err := p.CanonicalJSON()
	if err != nil {
		return "", err
	}
	return HashCanonical(data), nil
}

// HashCanonical computes the plan hash over already-canonical bytes. Callers
// that hold the CanonicalJSON output (e.g. the placement stamp path) use this
// to avoid a second marshaling.
func HashCanonical(canonical []byte) string {
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:])
}

// Decode parses a persisted plan document and validates it.
func Decode(data []byte) (*CompiledRenderPlan, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("render plan: empty document")
	}
	var plan CompiledRenderPlan
	if err := json.Unmarshal(data, &plan); err != nil {
		return nil, fmt.Errorf("render plan: decode: %w", err)
	}
	if err := plan.Validate(); err != nil {
		return nil, err
	}
	return &plan, nil
}
