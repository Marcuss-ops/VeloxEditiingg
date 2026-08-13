package renderplan

// compiler_contract.go: media-contract derivation + asset collection /
// registry-metadata enrichment for the RenderPlanCompiler.

import (
	"context"
	"sort"
	"strings"
)

// mediaContract derives the output contract from the payload (nested output
// map + top-level copy_only/video_mode/orientation), falling back to the
// compiler default.
func (c *RenderPlanCompiler) mediaContract(payload map[string]interface{}) MediaContract {
	mc := c.defaultContract
	if output, ok := asMap(payload["output"]); ok {
		if boolParam(output, "copy_only") {
			mc.CopyOnly = true
		}
		if v := int64Param(output, "width"); v > 0 {
			mc.Width = int(v)
		}
		if v := int64Param(output, "height"); v > 0 {
			mc.Height = int(v)
		}
		if v := int64Param(output, "fps"); v > 0 {
			mc.FpsNum = int(v)
			mc.FpsDen = 1
		}
		if v := strings.TrimSpace(strParam(output, "codec", "video_codec")); v != "" {
			mc.VideoCodec = v
		}
	}
	if boolParam(payload, "copy_only") {
		mc.CopyOnly = true
	}
	if v := strings.TrimSpace(strParam(payload, "video_mode")); strings.EqualFold(v, "clip_stock") {
		mc.CopyOnly = true
	}
	if v := strings.TrimSpace(strParam(payload, "orientation")); strings.EqualFold(v, "vertical") || strings.EqualFold(v, "portrait") {
		if mc.Width <= 0 || mc.Height <= 0 {
			mc.Width, mc.Height = 1080, 1920
		}
	}
	return mc
}

// collectAssets dedupes every asset referenced by the plan and enriches it
// with registry metadata (best-effort). The result is sorted by asset_id so
// the canonical JSON (and therefore plan_sha256) is order-stable.
func (c *RenderPlanCompiler) collectAssets(ctx context.Context, segments []Segment, audio []AudioTrack) []AssetRef {
	seen := make(map[string]AssetRef)
	merge := func(id, sha string) {
		if strings.TrimSpace(id) == "" {
			return
		}
		ref := seen[id]
		if ref.AssetID == "" {
			ref.AssetID = id
		}
		if ref.SHA256 == "" {
			ref.SHA256 = sha
		}
		seen[id] = ref
	}
	for _, seg := range segments {
		merge(seg.AssetID, seg.AssetSHA256)
	}
	for _, track := range audio {
		merge(track.AssetID, track.AssetSHA256)
	}
	if c != nil && c.resolver != nil {
		for id := range seen {
			meta, err := c.resolver.ResolveAssetMetadata(ctx, id)
			if err != nil {
				// Best-effort enrichment: a missing registry row must never
				// fail the compile or invent metadata.
				continue
			}
			ref := seen[id]
			if ref.SHA256 == "" {
				ref.SHA256 = meta.SHA256
			}
			ref.Kind = meta.Kind
			ref.MimeType = meta.MimeType
			if ref.SizeBytes == 0 {
				ref.SizeBytes = meta.SizeBytes
			}
			ref.DurationMS = meta.DurationMs
			ref.Width = meta.Width
			ref.Height = meta.Height
			seen[id] = ref
		}
	}
	out := make([]AssetRef, 0, len(seen))
	for _, ref := range seen {
		out = append(out, ref)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].AssetID < out[j].AssetID })
	return out
}
