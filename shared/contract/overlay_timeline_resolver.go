package contract

import (
	"fmt"
	"sort"
	"strings"

	"velox-shared/assetref"
)

// OverlayMode describes the only two visual semantics accepted at the
// editorial boundary. The native Velox renderer never receives this intent:
// replace is compiled into packet-copy segments and composite is prepared by
// Chronon before the compiled V2 plan is built.
type OverlayMode string

const (
	OverlayModeReplace        OverlayMode = "replace"
	OverlayModeComposite      OverlayMode = "composite"
	OverlayAudioPreserveFinal             = "preserve_final_audio"
)

// Overlay is producer/editorial intent. Timing is frame-native so the same
// value is used by the resolver, the acceptance tests and CompiledRenderPlanV2.
// URL is optional authoring metadata; asset_id/drive_file_id are the asset
// identity fields shared with clip and stock envelopes. Raw Drive URLs are
// canonicalized to velox-drive:// before the worker boundary.
type Overlay struct {
	ID          string `json:"id"`
	AssetID     string `json:"asset_id"`
	DriveFileID string `json:"drive_file_id,omitempty"`
	URL         string `json:"url,omitempty"`
	SHA256      string `json:"sha256,omitempty"`
	StartFrame  int64  `json:"start_frame"`
	FrameCount  int64  `json:"frame_count"`
	Mode        string `json:"mode"`
	ZIndex      int    `json:"z_index"`
	AudioMode   string `json:"audio_mode"`
}

// OverlayWindow is one deterministic interval that must be prepared by
// Chronon. A window contains every active composite overlay, sorted from
// bottom to top (z_index ascending, ID ascending for ties).
type OverlayWindow struct {
	WindowID   string    `json:"window_id"`
	StartFrame int64     `json:"start_frame"`
	FrameCount int64     `json:"frame_count"`
	Overlays   []Overlay `json:"overlays"`
}

// PreparedOverlayFragment is the result of one Chronon window render. The
// fragment must already be probed/certified against the canonical profile
// before it is passed to Velox.
type PreparedOverlayFragment struct {
	WindowID  string
	AssetID   string
	SHA256    string
	ProfileID string
}

// ApplyPreparedOverlayFragments replaces composite windows with the finished
// Chronon fragments. The result is still one contiguous video track; no
// overlay/layer field is emitted to the native renderer.
func ApplyPreparedOverlayFragments(base []VideoSegmentV2, windows []OverlayWindow, fragments []PreparedOverlayFragment, fpsNum, fpsDen int) ([]VideoSegmentV2, error) {
	if len(windows) != len(fragments) {
		return nil, fmt.Errorf("overlays: %d Chronon windows but %d prepared fragments", len(windows), len(fragments))
	}
	byWindow := make(map[string]PreparedOverlayFragment, len(fragments))
	for _, fragment := range fragments {
		if strings.TrimSpace(fragment.WindowID) == "" || strings.TrimSpace(fragment.AssetID) == "" || strings.TrimSpace(fragment.ProfileID) == "" {
			return nil, fmt.Errorf("overlays: prepared fragment requires window_id, asset_id and profile_id")
		}
		if _, exists := byWindow[fragment.WindowID]; exists {
			return nil, fmt.Errorf("overlays: duplicate prepared fragment window_id %q", fragment.WindowID)
		}
		byWindow[fragment.WindowID] = fragment
	}
	replacements := make([]VisualReplacement, 0, len(windows))
	for _, window := range windows {
		fragment, ok := byWindow[window.WindowID]
		if !ok {
			return nil, fmt.Errorf("overlays: missing prepared fragment for window %q", window.WindowID)
		}
		replacements = append(replacements, VisualReplacement{
			ReplacementID: window.WindowID, AssetID: fragment.AssetID, SHA256: fragment.SHA256,
			TimelineStartUS: frameToUS(window.StartFrame, fpsNum, fpsDen),
			TimelineEndUS:   frameToUS(window.StartFrame+window.FrameCount, fpsNum, fpsDen), ProfileID: fragment.ProfileID,
		})
	}
	return ResolveVisualReplacements(base, replacements, fpsNum, fpsDen)
}

// ValidateOverlays validates editorial intent against a base duration. It
// deliberately does not inspect media metadata; the worker probe remains the
// source of truth for canonical stream profile and audio presence.
func ValidateOverlays(overlays []Overlay, baseFrameCount int64) error {
	if baseFrameCount <= 0 {
		return fmt.Errorf("overlays: base frame count must be positive")
	}
	seen := make(map[string]struct{}, len(overlays))
	var replacements []Overlay
	for i := range overlays {
		o := overlays[i]
		id := strings.TrimSpace(o.ID)
		if id == "" {
			return fmt.Errorf("overlays[%d]: id is required", i)
		}
		if _, ok := seen[id]; ok {
			return fmt.Errorf("overlays[%d]: duplicate id %q", i, id)
		}
		seen[id] = struct{}{}
		if strings.TrimSpace(o.AssetID) == "" {
			return fmt.Errorf("overlays[%s]: asset_id is required", id)
		}
		if o.StartFrame < 0 || o.FrameCount <= 0 {
			return fmt.Errorf("overlays[%s]: start_frame must be >= 0 and frame_count must be positive", id)
		}
		if o.StartFrame+o.FrameCount > baseFrameCount {
			return fmt.Errorf("overlays[%s]: window [%d,%d) exceeds base frame count %d", id, o.StartFrame, o.StartFrame+o.FrameCount, baseFrameCount)
		}
		if o.Mode != string(OverlayModeReplace) && o.Mode != string(OverlayModeComposite) {
			return fmt.Errorf("overlays[%s]: mode must be replace or composite", id)
		}
		if o.AudioMode != "" && o.AudioMode != OverlayAudioPreserveFinal {
			return fmt.Errorf("overlays[%s]: audio_mode must be %q", id, OverlayAudioPreserveFinal)
		}
		if o.Mode == string(OverlayModeReplace) {
			replacements = append(replacements, o)
		}
	}
	sort.SliceStable(replacements, func(i, j int) bool {
		if replacements[i].StartFrame != replacements[j].StartFrame {
			return replacements[i].StartFrame < replacements[j].StartFrame
		}
		return replacements[i].ID < replacements[j].ID
	})
	for i := 1; i < len(replacements); i++ {
		prevEnd := replacements[i-1].StartFrame + replacements[i-1].FrameCount
		if replacements[i].StartFrame < prevEnd {
			return fmt.Errorf("overlays[%s]: replace window overlaps %s", replacements[i].ID, replacements[i-1].ID)
		}
	}
	return nil
}

// ResolveOverlayWindows partitions composite intent at every boundary. It
// never emits multiple video tracks and never performs pixel work.
func ResolveOverlayWindows(overlays []Overlay, baseFrameCount int64) ([]OverlayWindow, error) {
	if err := ValidateOverlays(overlays, baseFrameCount); err != nil {
		return nil, err
	}
	composites := make([]Overlay, 0, len(overlays))
	boundaries := map[int64]struct{}{0: {}}
	for _, o := range overlays {
		if o.Mode != string(OverlayModeComposite) {
			continue
		}
		composites = append(composites, o)
		boundaries[o.StartFrame] = struct{}{}
		boundaries[o.StartFrame+o.FrameCount] = struct{}{}
	}
	if len(composites) == 0 {
		return nil, nil
	}
	points := make([]int64, 0, len(boundaries))
	for point := range boundaries {
		points = append(points, point)
	}
	sort.Slice(points, func(i, j int) bool { return points[i] < points[j] })
	windows := make([]OverlayWindow, 0, len(points)-1)
	for i := 0; i < len(points)-1; i++ {
		start, end := points[i], points[i+1]
		if end <= start {
			continue
		}
		active := make([]Overlay, 0, len(composites))
		for _, o := range composites {
			if o.StartFrame <= start && start < o.StartFrame+o.FrameCount {
				active = append(active, o)
			}
		}
		if len(active) == 0 {
			continue
		}
		sort.SliceStable(active, func(i, j int) bool {
			if active[i].ZIndex != active[j].ZIndex {
				return active[i].ZIndex < active[j].ZIndex
			}
			return active[i].ID < active[j].ID
		})
		windows = append(windows, OverlayWindow{
			WindowID: fmt.Sprintf("overlay-window-%06d", len(windows)), StartFrame: start,
			FrameCount: end - start, Overlays: active,
		})
	}
	return windows, nil
}

// ResolveOverlayTimeline applies replace overlays and returns composite
// windows for the Chronon preparation step. Composite windows are intentionally
// not silently converted into renderer layers.
func ResolveOverlayTimeline(base []VideoSegmentV2, overlays []Overlay, fpsNum, fpsDen int) ([]VideoSegmentV2, []OverlayWindow, error) {
	if len(base) == 0 {
		return nil, nil, fmt.Errorf("overlays: base video track is empty")
	}
	if fpsNum <= 0 || fpsDen <= 0 {
		return nil, nil, fmt.Errorf("overlays: invalid frame rate %d/%d", fpsNum, fpsDen)
	}
	totalFrames := int64(0)
	for _, segment := range base {
		end := segment.TimelineStartFrame + segment.FrameCount
		if end > totalFrames {
			totalFrames = end
		}
	}
	if err := ValidateOverlays(overlays, totalFrames); err != nil {
		return nil, nil, err
	}
	replacements := make([]VisualReplacement, 0)
	for _, o := range overlays {
		if o.Mode != string(OverlayModeReplace) {
			continue
		}
		replacements = append(replacements, VisualReplacement{
			ReplacementID: o.ID, AssetID: o.AssetID, SHA256: o.SHA256,
			TimelineStartUS: frameToUS(o.StartFrame, fpsNum, fpsDen),
			TimelineEndUS:   frameToUS(o.StartFrame+o.FrameCount, fpsNum, fpsDen),
		})
	}
	resolved, err := ResolveVisualReplacements(base, replacements, fpsNum, fpsDen)
	if err != nil {
		return nil, nil, err
	}
	windows, err := ResolveOverlayWindows(overlays, totalFrames)
	if err != nil {
		return nil, nil, err
	}
	return resolved, windows, nil
}

// ParseOverlays converts a payload array into typed editorial intent. It
// accepts JSON-decoded values as well as an already typed []Overlay. The
// latter is important at the resolver boundary: the remoteengine adapter
// canonicalizes authoring values before enqueue builds the V2 payload, so a
// second parse must not silently discard those overlays.
func ParseOverlays(raw any) ([]Overlay, error) {
	if raw == nil {
		return nil, nil
	}
	var items []interface{}
	switch v := raw.(type) {
	case []interface{}:
		items = v
	case []map[string]interface{}:
		items = make([]interface{}, len(v))
		for i := range v {
			items[i] = v[i]
		}
	case []Overlay:
		items = make([]interface{}, len(v))
		for i := range v {
			items[i] = map[string]interface{}{
				"id":            v[i].ID,
				"asset_id":      v[i].AssetID,
				"drive_file_id": v[i].DriveFileID,
				"url":           v[i].URL,
				"sha256":        v[i].SHA256,
				"start_frame":   v[i].StartFrame,
				"frame_count":   v[i].FrameCount,
				"mode":          v[i].Mode,
				"z_index":       v[i].ZIndex,
				"audio_mode":    v[i].AudioMode,
			}
		}
	default:
		return nil, fmt.Errorf("overlays: must be an array, got %T", raw)
	}
	out := make([]Overlay, 0, len(items))
	for i, item := range items {
		m, ok := item.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("overlays[%d]: must be an object", i)
		}
		overlay := Overlay{
			ID:          overlayString(m["id"]),
			AssetID:     overlayString(m["asset_id"]),
			DriveFileID: overlayString(m["drive_file_id"]),
			URL:         overlayString(m["url"]),
			SHA256:      overlayString(m["sha256"]),
			StartFrame:  overlayInt64(m["start_frame"]),
			FrameCount:  overlayInt64(m["frame_count"]),
			Mode:        overlayString(m["mode"]),
			ZIndex:      int(overlayInt64(m["z_index"])),
			AudioMode:   overlayString(m["audio_mode"]),
		}
		// Authoring accepts the same Google Drive file URL used by clips and
		// stock, but the worker contract is deliberately credential-free and
		// only carries the opaque ID over the authenticated Master bridge.
		if driveID, err := assetref.ParseDriveFileID(overlay.URL); err == nil {
			overlay.DriveFileID = driveID.String()
			if ref, refErr := assetref.NewDeferredDrive(driveID.String()); refErr == nil {
				overlay.URL = ref.Wire()
			}
		}
		if overlay.DriveFileID == "" && strings.HasPrefix(strings.ToLower(strings.TrimSpace(overlay.URL)), assetref.SchemeVeloxDrive+"://") {
			if ref, err := assetref.Parse(overlay.URL); err == nil {
				overlay.DriveFileID = ref.ID()
			}
		}
		if overlay.AssetID == "" {
			overlay.AssetID = overlay.DriveFileID
		}
		if overlay.URL == "" && overlay.DriveFileID != "" {
			if ref, err := assetref.NewDeferredDrive(overlay.DriveFileID); err == nil {
				overlay.URL = ref.Wire()
			}
		}
		out = append(out, overlay)
	}
	return out, nil
}

func overlayString(value any) string { s, _ := value.(string); return s }
func overlayInt64(value any) int64 {
	switch n := value.(type) {
	case int:
		return int64(n)
	case int32:
		return int64(n)
	case int64:
		return n
	case float64:
		return int64(n)
	case float32:
		return int64(n)
	default:
		return 0
	}
}
