// Package rendercompiler compiles master-side process_video payloads into an
// immutable, canonical render plan. Registry values are persistent: Register
// returns a new registry and never mutates the receiver, which makes compiler
// selection safe to share between enqueue goroutines.
package rendercompiler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"velox-shared/contract"
	"velox-shared/contract/rendermanifest"
)

const (
	ProcessVideoJobType = "process_video"
	PayloadVersionV2    = "v2"
)

// Compiler transforms one typed payload into a render plan without mutating
// the payload or retaining references to any of its maps or slices.
type Compiler func(context.Context, *contract.JobPayloadV2) (*RenderPlan, error)

// Registry is an immutable compiler registry. Register returns a copied
// registry, so an existing registry remains safe for concurrent Compile calls.
type Registry struct {
	compilers map[string]Compiler
}

// NewRegistry creates an empty immutable registry.
func NewRegistry() Registry {
	return Registry{compilers: make(map[string]Compiler)}
}

// Register returns a new registry containing compiler for jobType/version.
func (r Registry) Register(jobType, version string, compiler Compiler) (Registry, error) {
	jobType = strings.TrimSpace(jobType)
	version = strings.TrimSpace(version)
	if jobType == "" || version == "" {
		return Registry{}, fmt.Errorf("rendercompiler: job type and version are required")
	}
	if compiler == nil {
		return Registry{}, fmt.Errorf("rendercompiler: compiler for %s/%s is nil", jobType, version)
	}
	copy := make(map[string]Compiler, len(r.compilers)+1)
	for key, value := range r.compilers {
		copy[key] = value
	}
	copy[registryKey(jobType, version)] = compiler
	return Registry{compilers: copy}, nil
}

// CompilePayload converts a raw process_video payload to the typed V2
// envelope and compiles it through the same registry path. The input map is
// never modified.
func (r Registry) CompilePayload(ctx context.Context, raw map[string]any) (*RenderPlan, error) {
	if raw == nil {
		return nil, fmt.Errorf("rendercompiler: raw payload is nil")
	}
	if rawManifest, present := raw["render_manifest"]; present {
		if rawManifest == nil {
			return nil, fmt.Errorf("rendercompiler: render_manifest must be an object")
		}
		manifest, ok := rawManifest.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("rendercompiler: render_manifest must be an object, got %T", rawManifest)
		}
		if len(manifest) == 0 {
			return nil, fmt.Errorf("rendercompiler: render_manifest must not be empty")
		}
	}
	payload, err := contract.NewJobPayloadV2Checked(raw)
	if err != nil {
		return nil, fmt.Errorf("rendercompiler: canonical payload validation: %w", err)
	}
	// NewJobPayloadV2 intentionally defaults missing routing fields for
	// legacy readers. The registry must nevertheless dispatch explicit raw
	// values faithfully so unsupported job types/versions fail closed.
	if value, ok := raw["job_type"].(string); ok && strings.TrimSpace(value) != "" {
		payload.JobType = strings.TrimSpace(value)
	}
	if value, ok := raw["version"].(string); ok && strings.TrimSpace(value) != "" {
		payload.Version = strings.TrimSpace(value)
	}
	return r.Compile(ctx, payload)
}

// Compile selects the compiler using JobType and Version. The returned plan
// owns all data it exposes through snapshots and byte copies.
func (r Registry) Compile(ctx context.Context, payload *contract.JobPayloadV2) (*RenderPlan, error) {
	if payload == nil {
		return nil, fmt.Errorf("rendercompiler: payload is nil")
	}
	key := registryKey(payload.JobType, payload.Version)
	compiler, ok := r.compilers[key]
	if !ok {
		return nil, fmt.Errorf("rendercompiler: no compiler registered for %s/%s", payload.JobType, payload.Version)
	}
	return compiler(ctx, payload)
}

// DefaultRegistry returns the canonical master-side registry.
func DefaultRegistry() Registry {
	registry, err := NewRegistry().Register(ProcessVideoJobType, PayloadVersionV2, compileProcessVideoV2)
	if err != nil {
		panic(err)
	}
	return registry
}

// RenderPlan is an immutable snapshot of a canonical render manifest. The
// manifest is private on purpose; callers receive a defensive snapshot or a
// copied canonical JSON document, never internal mutable storage.
type RenderPlan struct {
	manifest rendermanifest.Manifest
	json     []byte
	hash     string
}

// JSON returns a defensive copy of the canonical deterministic JSON.
func (p *RenderPlan) JSON() []byte {
	if p == nil {
		return nil
	}
	return append([]byte(nil), p.json...)
}

// SHA256 returns the full SHA-256 digest of JSON().
func (p *RenderPlan) SHA256() string {
	if p == nil {
		return ""
	}
	return p.hash
}

// Snapshot returns a deep copy of the typed manifest.
func (p *RenderPlan) Snapshot() (rendermanifest.Manifest, error) {
	if p == nil {
		return rendermanifest.Manifest{}, fmt.Errorf("rendercompiler: plan is nil")
	}
	parsed, err := rendermanifest.Parse(p.json)
	if err != nil {
		return rendermanifest.Manifest{}, fmt.Errorf("rendercompiler: plan snapshot: %w", err)
	}
	return *parsed, nil
}

// Validate checks that the stored plan remains a valid canonical manifest.
func (p *RenderPlan) Validate() error {
	if p == nil {
		return fmt.Errorf("rendercompiler: plan is nil")
	}
	return rendermanifest.ValidateJSON(p.json)
}

func compileProcessVideoV2(ctx context.Context, payload *contract.JobPayloadV2) (*RenderPlan, error) {
	if err := contextErr(ctx); err != nil {
		return nil, err
	}
	if payload == nil {
		return nil, fmt.Errorf("rendercompiler: process_video payload is nil")
	}
	if err := contextErr(ctx); err != nil {
		return nil, err
	}
	if len(payload.RenderManifest) > 0 {
		manifest, err := payload.TypedRenderManifest()
		if err != nil {
			return nil, fmt.Errorf("rendercompiler: typed render_manifest: %w", err)
		}
		return planFromManifest(manifest)
	}
	return planFromPayload(payload)
}

func planFromManifest(manifest *rendermanifest.Manifest) (*RenderPlan, error) {
	if manifest == nil {
		return nil, fmt.Errorf("rendercompiler: render_manifest is nil")
	}
	data, err := json.Marshal(manifest)
	if err != nil {
		return nil, fmt.Errorf("rendercompiler: marshal typed render_manifest: %w", err)
	}
	return newPlan(data)
}

func planFromPayload(payload *contract.JobPayloadV2) (*RenderPlan, error) {
	// Canvas is the typed single source of the output geometry, populated
	// from video_metadata by NewJobPayloadV2. Payloads built directly (tests)
	// may carry a zero Canvas, so fall back to the canonical default before
	// the strict manifest validation rejects the plan.
	canvas := payload.Canvas
	if canvas.Width <= 0 || canvas.Height <= 0 || canvas.FPSNum <= 0 || canvas.FPSDen <= 0 || canvas.PixelFormat == "" {
		canvas = rendermanifest.DefaultCanvas()
	}

	scenes, err := payloadScenes(payload)
	if err != nil {
		return nil, err
	}
	assets := make(map[string]rendermanifest.Asset)
	tracks := make([]rendermanifest.Track, 0, 2)
	videoEvents := make([]rendermanifest.Event, 0, len(scenes))
	voiceEvents := make([]rendermanifest.Event, 0, len(scenes))
	var timelineMS int64

	for index, scene := range scenes {
		duration := sceneDurationMS(scene)
		if duration <= 0 {
			return nil, fmt.Errorf("rendercompiler: scenes[%d].duration must be positive", index)
		}
		clip, clipOK := object(scene["clip"])
		if !clipOK {
			if legacyURL := stringValue(scene, "clip_link", "image_link"); legacyURL != "" {
				clip = map[string]any{
					"uri":         legacyURL,
					"sha256":      stringValue(scene, "clip_sha256"),
					"size_bytes":  scene["clip_size_bytes"],
					"duration_ms": scene["clip_duration_ms"],
				}
				clipOK = true
			}
		}
		if !clipOK {
			return nil, fmt.Errorf("rendercompiler: scenes[%d].clip is required when render_manifest is absent", index)
		}
		clipAsset, err := assetFromMap(clip, "video", fmt.Sprintf("clip-%03d", index))
		if err != nil {
			return nil, fmt.Errorf("rendercompiler: scenes[%d].clip: %w", index, err)
		}
		if err := addAsset(assets, clipAsset); err != nil {
			return nil, fmt.Errorf("rendercompiler: scenes[%d].clip: %w", index, err)
		}
		videoEvents = append(videoEvents, rendermanifest.Event{
			AssetID: clipAsset.ID, TimelineStartMS: timelineMS,
			SourceStartMS: integer(clip["start_ms"]), DurationMS: duration,
		})

		voice, voiceOK := object(scene["voiceover"])
		if !voiceOK && index < len(payload.VoiceoverPaths) {
			voice = map[string]any{"uri": payload.VoiceoverPaths[index]}
			voiceOK = true
		}
		if voiceOK {
			voiceAsset, voiceErr := assetFromMap(voice, "audio", fmt.Sprintf("voiceover-%03d", index))
			if voiceErr != nil {
				return nil, fmt.Errorf("rendercompiler: scenes[%d].voiceover: %w", index, voiceErr)
			}
			if err := addAsset(assets, voiceAsset); err != nil {
				return nil, fmt.Errorf("rendercompiler: scenes[%d].voiceover: %w", index, err)
			}
			voiceEvents = append(voiceEvents, rendermanifest.Event{
				AssetID: voiceAsset.ID, TimelineStartMS: timelineMS,
				DurationMS: duration,
			})
		}
		timelineMS += duration
	}

	tracks = append(tracks, rendermanifest.Track{ID: "main-video", Kind: "video", Events: videoEvents})
	if len(voiceEvents) > 0 {
		tracks = append(tracks, rendermanifest.Track{ID: "voiceover-track", Kind: "voiceover", Events: voiceEvents})
	}

	layers := append([]rendermanifest.Layer(nil), payload.Layers...)
	sort.SliceStable(layers, func(i, j int) bool {
		if layers[i].StartSeconds != layers[j].StartSeconds {
			return layers[i].StartSeconds < layers[j].StartSeconds
		}
		return layers[i].ID < layers[j].ID
	})

	assetList := make([]rendermanifest.Asset, 0, len(assets))
	for _, asset := range assets {
		assetList = append(assetList, asset)
	}
	sort.Slice(assetList, func(i, j int) bool { return assetList[i].ID < assetList[j].ID })

	manifest := rendermanifest.Manifest{
		Schema: rendermanifest.Schema,
		Canvas: canvas,
		Assets: assetList,
		Tracks: tracks,
		Layers: layers,
		Output: rendermanifest.Output{Container: "mp4", VideoCodec: "h264", AudioCodec: "aac", AudioSampleRate: 48000, AudioChannels: 2},
	}
	data, err := json.Marshal(manifest)
	if err != nil {
		return nil, fmt.Errorf("rendercompiler: marshal compiled plan: %w", err)
	}
	return newPlan(data)
}

func newPlan(data []byte) (*RenderPlan, error) {
	parsed, err := rendermanifest.Parse(data)
	if err != nil {
		return nil, fmt.Errorf("rendercompiler: validate compiled plan: %w", err)
	}
	canonical, err := json.Marshal(parsed)
	if err != nil {
		return nil, fmt.Errorf("rendercompiler: canonicalize compiled plan: %w", err)
	}
	sum := sha256.Sum256(canonical)
	return &RenderPlan{manifest: *parsed, json: append([]byte(nil), canonical...), hash: hex.EncodeToString(sum[:])}, nil
}

func payloadScenes(payload *contract.JobPayloadV2) ([]map[string]any, error) {
	if len(payload.Scenes) > 0 {
		return deepCopyMaps(payload.Scenes), nil
	}
	if strings.TrimSpace(payload.ScenesJSON) == "" {
		return nil, fmt.Errorf("rendercompiler: scenes or scenes_json is required")
	}
	var scenes []map[string]any
	if err := json.Unmarshal([]byte(payload.ScenesJSON), &scenes); err != nil {
		return nil, fmt.Errorf("rendercompiler: scenes_json: %w", err)
	}
	return scenes, nil
}

func assetFromMap(raw map[string]any, kind, fallbackID string) (rendermanifest.Asset, error) {
	id := stringValue(raw, "asset_id", "id")
	uri := stringValue(raw, "uri", "url", "source_url")
	if id == "" {
		id = fallbackID
	}
	asset := rendermanifest.Asset{
		ID: id, URI: uri, Kind: kind,
		Format:     stringValue(raw, "format"),
		SHA256:     stringValue(raw, "sha256"),
		SizeBytes:  integer(raw["size_bytes"]),
		DurationMS: integer(raw["duration_ms"]),
	}
	if asset.URI == "" {
		return rendermanifest.Asset{}, fmt.Errorf("uri is required")
	}
	if asset.SHA256 == "" || asset.SizeBytes <= 0 {
		return rendermanifest.Asset{}, fmt.Errorf("sha256 and positive size_bytes are required")
	}
	return asset, nil
}

func addAsset(assets map[string]rendermanifest.Asset, asset rendermanifest.Asset) error {
	if existing, ok := assets[asset.ID]; ok && existing != asset {
		return fmt.Errorf("asset id %q is reused with different content", asset.ID)
	}
	assets[asset.ID] = asset
	return nil
}

func object(value any) (map[string]any, bool) {
	m, ok := value.(map[string]any)
	return m, ok && m != nil
}

func deepCopyMaps(input []map[string]any) []map[string]any {
	output := make([]map[string]any, len(input))
	for i, value := range input {
		data, _ := json.Marshal(value)
		var copy map[string]any
		_ = json.Unmarshal(data, &copy)
		output[i] = copy
	}
	return output
}

func registryKey(jobType, version string) string {
	return strings.TrimSpace(jobType) + "@" + strings.TrimSpace(version)
}

func stringValue(raw map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := raw[key].(string); ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func integer(value any) int64 {
	switch value := value.(type) {
	case int:
		return int64(value)
	case int64:
		return value
	case float64:
		return int64(value)
	case json.Number:
		parsed, _ := value.Int64()
		return parsed
	default:
		return 0
	}
}

func number(value any, fallback float64) float64 {
	switch value := value.(type) {
	case int:
		return float64(value)
	case int64:
		return float64(value)
	case float64:
		return value
	case json.Number:
		parsed, err := value.Float64()
		if err == nil {
			return parsed
		}
	}
	return fallback
}

func contextErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return fmt.Errorf("rendercompiler: compilation canceled: %w", ctx.Err())
	default:
		return nil
	}
}

func sceneDurationMS(scene map[string]any) int64 {
	if duration := integer(scene["duration_ms"]); duration > 0 {
		return duration
	}
	if seconds := number(scene["duration_seconds"], 0); seconds > 0 {
		return int64(seconds * 1000)
	}
	return 0
}
