// Package rendermanifest defines the immutable render_manifest contract used
// by the Velox master and workers. The package deliberately has no dependency
// on an HTTP or renderer implementation: it can be used at every contract
// boundary (enqueue, task creation, and worker admission).
package rendermanifest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"regexp"
	"sort"
	"strings"
)

const Schema = "velox.render-manifest.v1"

var sha256Pattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

// Manifest is the complete, immutable render manifest. JSON decoding is
// strict through Parse, so adding a field requires an explicit contract
// change rather than silently ignoring a typo or an unsupported operation.
type Manifest struct {
	Schema string  `json:"schema"`
	Canvas Canvas  `json:"canvas"`
	Assets []Asset `json:"assets"`
	Tracks []Track `json:"tracks"`
	Layers []Layer `json:"layers,omitempty"`
	Output Output  `json:"output"`
}

// Asset identifies one verified input available through the Velox asset
// bridge. Local paths and provider URLs are intentionally not accepted.
type Asset struct {
	ID         string `json:"id"`
	URI        string `json:"uri"`
	Kind       string `json:"kind"`
	Format     string `json:"format,omitempty"`
	SHA256     string `json:"sha256"`
	SizeBytes  int64  `json:"size_bytes"`
	DurationMS int64  `json:"duration_ms,omitempty"`
}

// Canvas describes the normalized output timeline.
type Canvas struct {
	Width       int    `json:"width"`
	Height      int    `json:"height"`
	FPSNum      int    `json:"fps_num"`
	FPSDen      int    `json:"fps_den"`
	PixelFormat string `json:"pixel_format"`
}

// DefaultCanvas returns the fallback output geometry used by the scene-based
// authoring compiler when a payload does not carry an explicit canvas in
// video_metadata. It is a function (not a package var) so callers can never
// mutate a shared registry value.
func DefaultCanvas() Canvas {
	return Canvas{Width: 1920, Height: 1080, FPSNum: 30, FPSDen: 1, PixelFormat: "yuv420p"}
}

// CanvasFromMap converts the canvas keys of an authoring video_metadata map
// into the typed Canvas. It is the single typed boundary for the output
// timeline geometry: the render compiler reads the typed Canvas instead of
// poking width/height/fps_den fields out of an untyped map. Each field falls
// back to `defaults` when it is absent or non-numeric, matching the lenient
// authoring behavior of LayerFromMap.
func CanvasFromMap(raw map[string]any, defaults Canvas) Canvas {
	out := defaults
	if raw == nil {
		return out
	}
	if value := layerNumber(raw["width"]); value > 0 {
		out.Width = int(value)
	}
	if value := layerNumber(raw["height"]); value > 0 {
		out.Height = int(value)
	}
	if value := layerNumber(raw["fps_num"]); value > 0 {
		out.FPSNum = int(value)
	}
	if value := layerNumber(raw["fps_den"]); value > 0 {
		out.FPSDen = int(value)
	}
	if value, ok := raw["pixel_format"].(string); ok && strings.TrimSpace(value) != "" {
		out.PixelFormat = strings.TrimSpace(value)
	}
	return out
}

// Track is one logical timeline layer. Captions use AssetID at track level;
// media and audio tracks use Events to place their assets on the timeline.
type Track struct {
	ID      string  `json:"id"`
	Kind    string  `json:"kind"`
	AssetID string  `json:"asset_id,omitempty"`
	Events  []Event `json:"events,omitempty"`
	BurnIn  bool    `json:"burn_in,omitempty"`
}

// Layer is an ordered visual overlay. Layers are sorted by start time and ID
// by the master compiler before the manifest is serialized.
type Layer struct {
	ID              string    `json:"id"`
	Type            string    `json:"type"`
	Role            string    `json:"role,omitempty"`
	Text            string    `json:"text,omitempty"`
	Asset           string    `json:"asset,omitempty"`
	Source          string    `json:"source,omitempty"`
	Font            string    `json:"font,omitempty"`
	FontSize        float64   `json:"font_size,omitempty"`
	Position        []float64 `json:"position,omitempty"`
	StartSeconds    float64   `json:"start_seconds,omitempty"`
	DurationSeconds float64   `json:"duration_seconds,omitempty"`
	Preset          string    `json:"preset,omitempty"`
	Animation       string    `json:"animation,omitempty"`
}

// LayerFromMap converts one authoring-input layer map into the typed Layer.
// It is the lenient authoring boundary for JobPayloadV2.Layers: a missing ID
// is defaulted to layer-<index> and only the declared Layer fields are copied,
// so unknown authoring keys are dropped at the typed boundary. Semantic rules
// (non-empty type, finite font_size/start/duration, unique IDs) are validated
// later by Manifest.Validate, not here — this keeps a compatibility reader
// able to round-trip authoring input that a strict compiler would reject.
func LayerFromMap(raw map[string]any, index int) Layer {
	layer := Layer{
		ID:              layerString(raw, "id"),
		Type:            layerString(raw, "type"),
		Role:            layerString(raw, "role"),
		Text:            layerString(raw, "text"),
		Asset:           layerString(raw, "asset"),
		Source:          layerString(raw, "source"),
		Font:            layerString(raw, "font"),
		FontSize:        layerNumber(raw["font_size"]),
		StartSeconds:    layerNumber(raw["start_seconds"]),
		DurationSeconds: layerNumber(raw["duration_seconds"]),
		Preset:          layerString(raw, "preset"),
		Animation:       layerString(raw, "animation"),
	}
	if strings.TrimSpace(layer.ID) == "" {
		layer.ID = fmt.Sprintf("layer-%03d", index)
	}
	if position, ok := raw["position"].([]any); ok {
		for _, value := range position {
			layer.Position = append(layer.Position, layerNumber(value))
		}
	}
	return layer
}

// LayersToMaps converts typed layers to the generic authoring-input map
// representation used by JobPayloadV2.ToMap and raw-payload readers. The JSON
// round-trip reuses the Layer struct tags as the single source of truth, so
// the map shape can never drift from the wire contract.
func LayersToMaps(layers []Layer) ([]map[string]any, error) {
	if len(layers) == 0 {
		return nil, nil
	}
	data, err := json.Marshal(layers)
	if err != nil {
		return nil, fmt.Errorf("rendermanifest: marshal layers: %w", err)
	}
	var out []map[string]any
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, fmt.Errorf("rendermanifest: unmarshal layers: %w", err)
	}
	return out, nil
}

func layerString(raw map[string]any, key string) string {
	if value, ok := raw[key].(string); ok && strings.TrimSpace(value) != "" {
		return strings.TrimSpace(value)
	}
	return ""
}

func layerNumber(value any) float64 {
	switch v := value.(type) {
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case float64:
		return v
	case json.Number:
		if parsed, err := v.Float64(); err == nil {
			return parsed
		}
	}
	return 0
}

// Event places one asset on a track. SourceStartMS defaults to zero when it
// is omitted from JSON; all other timing values are validated in milliseconds.
type Event struct {
	AssetID            string  `json:"asset_id"`
	TimelineStartMS    int64   `json:"timeline_start_ms"`
	SourceStartMS      int64   `json:"source_start_ms,omitempty"`
	DurationMS         int64   `json:"duration_ms"`
	GainDB             float64 `json:"gain_db,omitempty"`
	FadeInMS           int64   `json:"fade_in_ms,omitempty"`
	FadeOutMS          int64   `json:"fade_out_ms,omitempty"`
	DuckUnderVoiceover bool    `json:"duck_under_voiceover,omitempty"`
}

// Output describes the final artifact encoding.
type Output struct {
	Container       string `json:"container"`
	VideoCodec      string `json:"video_codec"`
	AudioCodec      string `json:"audio_codec"`
	AudioSampleRate int    `json:"audio_sample_rate"`
	AudioChannels   int    `json:"audio_channels"`
}

// ValidationError is a machine-readable semantic contract violation.
type ValidationError struct {
	Path     string `json:"path"`
	Issue    string `json:"issue"`
	Expected string `json:"expected,omitempty"`
	Observed string `json:"observed,omitempty"`
}

func (e ValidationError) Error() string {
	message := e.Path + ": " + e.Issue
	if e.Expected != "" {
		message += " (expected " + e.Expected
		if e.Observed != "" {
			message += ", observed " + e.Observed
		}
		message += ")"
	}
	return message
}

// ValidationErrors is returned when one or more fields violate the contract.
type ValidationErrors []ValidationError

func (e ValidationErrors) Error() string {
	if len(e) == 0 {
		return ""
	}
	parts := make([]string, len(e))
	for i := range e {
		parts[i] = e[i].Error()
	}
	return strings.Join(parts, "; ")
}

// Parse strictly decodes one manifest and validates its semantic rules.
// Unknown fields, malformed JSON, trailing JSON values, and contract
// violations are all rejected.
func Parse(data []byte) (*Manifest, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return nil, fmt.Errorf("render manifest: empty document")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var manifest Manifest
	if err := decoder.Decode(&manifest); err != nil {
		return nil, fmt.Errorf("render manifest: decode: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("render manifest: trailing JSON value")
		}
		return nil, fmt.Errorf("render manifest: trailing data: %w", err)
	}
	if err := manifest.Validate(); err != nil {
		return nil, err
	}
	return &manifest, nil
}

// ValidateJSON is an explicit alias for Parse for callers that prefer a
// validation-oriented name.
func ValidateJSON(data []byte) error {
	_, err := Parse(data)
	return err
}

// ValidateMap strictly validates a map obtained from a generic payload.
// Marshaling first ensures the exact same unknown-field and type checks as
// the wire parser, rather than maintaining a second map-based validator.
func ValidateMap(raw map[string]interface{}) error {
	if raw == nil {
		return fmt.Errorf("render manifest: document must be an object")
	}
	data, err := json.Marshal(raw)
	if err != nil {
		return fmt.Errorf("render manifest: encode map: %w", err)
	}
	return ValidateJSON(data)
}

// Validate applies semantic checks after strict decoding.
func (m Manifest) Validate() error {
	var errs ValidationErrors
	if m.Schema != Schema {
		errs.add("schema", "unsupported_value", Schema, m.Schema)
	}
	errs = append(errs, m.Canvas.validate()...)
	if len(m.Assets) == 0 {
		errs.add("assets", "empty", "at least one asset", "")
	}
	assetByID := make(map[string]Asset, len(m.Assets))
	for i, asset := range m.Assets {
		path := fmt.Sprintf("assets[%d]", i)
		for _, violation := range asset.validate(path) {
			errs = append(errs, violation)
		}
		if asset.ID != "" {
			if _, exists := assetByID[asset.ID]; exists {
				errs.add(path+".id", "duplicate", "unique asset id", asset.ID)
			} else {
				assetByID[asset.ID] = asset
			}
		}
	}
	if len(m.Tracks) == 0 {
		errs.add("tracks", "empty", "at least one track", "")
	}
	hasVideo, hasVoiceover := false, false
	trackIDs := make(map[string]bool, len(m.Tracks))
	for i, track := range m.Tracks {
		path := fmt.Sprintf("tracks[%d]", i)
		if track.ID == "" {
			errs.add(path+".id", "required", "non-empty string", "")
		} else if trackIDs[track.ID] {
			errs.add(path+".id", "duplicate", "unique track id", track.ID)
		} else {
			trackIDs[track.ID] = true
		}
		if !validTrackKinds[track.Kind] {
			errs.add(path+".kind", "unsupported_value", "video, voiceover, music, sfx, or captions", track.Kind)
		}
		if track.Kind == "video" {
			hasVideo = true
		}
		if track.Kind == "voiceover" {
			hasVoiceover = true
		}
		for _, violation := range validateTrack(track, path, assetByID) {
			errs = append(errs, violation)
		}
	}
	if len(m.Tracks) > 0 && !hasVideo {
		errs.add("tracks", "missing_kind", "at least one video track", "")
	}
	if len(m.Tracks) > 0 && !hasVoiceover {
		errs.add("tracks", "missing_kind", "at least one voiceover track", "")
	}
	layerIDs := make(map[string]bool, len(m.Layers))
	for i, layer := range m.Layers {
		path := fmt.Sprintf("layers[%d]", i)
		for _, violation := range layer.validate(path) {
			errs = append(errs, violation)
		}
		if layer.ID != "" {
			if layerIDs[layer.ID] {
				errs.add(path+".id", "duplicate", "unique layer id", layer.ID)
			} else {
				layerIDs[layer.ID] = true
			}
		}
	}
	errs = append(errs, m.Output.validate()...)
	if len(errs) > 0 {
		return errs
	}
	return nil
}

var validAssetKinds = map[string]bool{"video": true, "audio": true, "final_audio": true, "subtitle": true, "image": true}
var validTrackKinds = map[string]bool{"video": true, "voiceover": true, "music": true, "sfx": true, "captions": true}
var validPixelFormats = map[string]bool{"yuv420p": true, "yuv422p": true, "yuv444p": true}

func (a Asset) validate(path string) (errs ValidationErrors) {
	if a.ID == "" {
		errs.add(path+".id", "required", "non-empty string", "")
	}
	if !strings.HasPrefix(a.URI, "velox-asset://") || len(strings.TrimPrefix(a.URI, "velox-asset://")) == 0 {
		errs.add(path+".uri", "invalid_scheme", "velox-asset://<asset-id>", a.URI)
	}
	if !validAssetKinds[a.Kind] {
		errs.add(path+".kind", "unsupported_value", "video, audio, subtitle, or image", a.Kind)
	}
	if !sha256Pattern.MatchString(a.SHA256) {
		errs.add(path+".sha256", "invalid_sha256", "64 lowercase hexadecimal characters", a.SHA256)
	}
	if a.SizeBytes <= 0 {
		errs.add(path+".size_bytes", "out_of_range", "positive integer", fmt.Sprint(a.SizeBytes))
	}
	if (a.Kind == "video" || a.Kind == "audio" || a.Kind == "final_audio") && a.DurationMS <= 0 {
		errs.add(path+".duration_ms", "required", "positive milliseconds", fmt.Sprint(a.DurationMS))
	}
	if a.Kind == "subtitle" && a.Format != "ass" && a.Format != "srt" && a.Format != "vtt" {
		errs.add(path+".format", "unsupported_value", "ass, srt, or vtt", a.Format)
	}
	return errs
}

func (l Layer) validate(path string) (errs ValidationErrors) {
	if strings.TrimSpace(l.ID) == "" {
		errs.add(path+".id", "required", "non-empty string", l.ID)
	}
	if strings.TrimSpace(l.Type) == "" {
		errs.add(path+".type", "required", "non-empty string", l.Type)
	}
	if l.FontSize < 0 || math.IsNaN(l.FontSize) || math.IsInf(l.FontSize, 0) {
		errs.add(path+".font_size", "out_of_range", "finite non-negative number", fmt.Sprint(l.FontSize))
	}
	if l.StartSeconds < 0 || math.IsNaN(l.StartSeconds) || math.IsInf(l.StartSeconds, 0) {
		errs.add(path+".start_seconds", "out_of_range", "finite non-negative seconds", fmt.Sprint(l.StartSeconds))
	}
	if l.DurationSeconds < 0 || math.IsNaN(l.DurationSeconds) || math.IsInf(l.DurationSeconds, 0) {
		errs.add(path+".duration_seconds", "out_of_range", "finite non-negative seconds", fmt.Sprint(l.DurationSeconds))
	}
	for i, value := range l.Position {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			errs.add(fmt.Sprintf("%s.position[%d]", path, i), "out_of_range", "finite number", fmt.Sprint(value))
		}
	}
	return errs
}

func (c Canvas) validate() (errs ValidationErrors) {
	if c.Width <= 0 {
		errs.add("canvas.width", "out_of_range", "positive integer", fmt.Sprint(c.Width))
	}
	if c.Height <= 0 {
		errs.add("canvas.height", "out_of_range", "positive integer", fmt.Sprint(c.Height))
	}
	if c.FPSNum <= 0 {
		errs.add("canvas.fps_num", "out_of_range", "positive integer", fmt.Sprint(c.FPSNum))
	}
	if c.FPSDen <= 0 {
		errs.add("canvas.fps_den", "out_of_range", "positive integer", fmt.Sprint(c.FPSDen))
	}
	if c.FPSNum > 0 && c.FPSDen > 0 && float64(c.FPSNum)/float64(c.FPSDen) > 120 {
		errs.add("canvas", "fps_too_high", "fps <= 120", fmt.Sprintf("%d/%d", c.FPSNum, c.FPSDen))
	}
	if !validPixelFormats[c.PixelFormat] {
		errs.add("canvas.pixel_format", "unsupported_value", "yuv420p, yuv422p, or yuv444p", c.PixelFormat)
	}
	return errs
}

func validateTrack(track Track, path string, assets map[string]Asset) (errs ValidationErrors) {
	if track.Kind == "captions" {
		if track.AssetID == "" {
			errs.add(path+".asset_id", "required", "subtitle asset id", "")
		} else if asset, ok := assets[track.AssetID]; !ok {
			errs.add(path+".asset_id", "unknown_reference", "known subtitle asset id", track.AssetID)
		} else if asset.Kind != "subtitle" {
			errs.add(path+".asset_id", "wrong_asset_kind", "subtitle", asset.Kind)
		}
		if len(track.Events) > 0 {
			errs.add(path+".events", "not_allowed", "caption track asset_id without events", "events present")
		}
		return errs
	}
	if len(track.Events) == 0 {
		errs.add(path+".events", "empty", "at least one event", "")
		return errs
	}
	for i, event := range track.Events {
		eventPath := fmt.Sprintf("%s.events[%d]", path, i)
		if event.AssetID == "" {
			errs.add(eventPath+".asset_id", "required", "known asset id", "")
			continue
		}
		asset, ok := assets[event.AssetID]
		if !ok {
			errs.add(eventPath+".asset_id", "unknown_reference", "known asset id", event.AssetID)
		} else {
			if track.Kind == "video" && asset.Kind != "video" {
				errs.add(eventPath+".asset_id", "wrong_asset_kind", "video", asset.Kind)
			}
			if (track.Kind == "voiceover" || track.Kind == "music" || track.Kind == "sfx") && asset.Kind != "audio" {
				errs.add(eventPath+".asset_id", "wrong_asset_kind", "audio", asset.Kind)
			}
			if asset.DurationMS > 0 && event.SourceStartMS+event.DurationMS > asset.DurationMS {
				errs.add(eventPath+".source_start_ms", "source_range_out_of_bounds", "source_start_ms + duration_ms <= asset duration", fmt.Sprintf("%d", event.SourceStartMS+event.DurationMS))
			}
		}
		if event.TimelineStartMS < 0 {
			errs.add(eventPath+".timeline_start_ms", "out_of_range", "non-negative milliseconds", fmt.Sprint(event.TimelineStartMS))
		}
		if event.SourceStartMS < 0 {
			errs.add(eventPath+".source_start_ms", "out_of_range", "non-negative milliseconds", fmt.Sprint(event.SourceStartMS))
		}
		if event.DurationMS <= 0 {
			errs.add(eventPath+".duration_ms", "out_of_range", "positive milliseconds", fmt.Sprint(event.DurationMS))
		}
		if event.FadeInMS < 0 {
			errs.add(eventPath+".fade_in_ms", "out_of_range", "non-negative milliseconds", fmt.Sprint(event.FadeInMS))
		}
		if event.FadeOutMS < 0 {
			errs.add(eventPath+".fade_out_ms", "out_of_range", "non-negative milliseconds", fmt.Sprint(event.FadeOutMS))
		}
	}
	return errs
}

func (o Output) validate() (errs ValidationErrors) {
	if o.Container != "mp4" {
		errs.add("output.container", "unsupported_value", "mp4", o.Container)
	}
	if o.VideoCodec != "h264" {
		errs.add("output.video_codec", "unsupported_value", "h264", o.VideoCodec)
	}
	if o.AudioCodec != "aac" {
		errs.add("output.audio_codec", "unsupported_value", "aac", o.AudioCodec)
	}
	if o.AudioSampleRate <= 0 {
		errs.add("output.audio_sample_rate", "out_of_range", "positive sample rate", fmt.Sprint(o.AudioSampleRate))
	}
	if o.AudioChannels != 1 && o.AudioChannels != 2 {
		errs.add("output.audio_channels", "unsupported_value", "1 or 2", fmt.Sprint(o.AudioChannels))
	}
	return errs
}

func (e *ValidationErrors) add(path, issue, expected, observed string) {
	*e = append(*e, ValidationError{Path: path, Issue: issue, Expected: expected, Observed: observed})
}

// Paths returns deterministic paths for callers that want to display a
// compact summary without depending on map iteration order.
func (e ValidationErrors) Paths() []string {
	paths := make([]string, len(e))
	for i := range e {
		paths[i] = e[i].Path
	}
	sort.Strings(paths)
	return paths
}
