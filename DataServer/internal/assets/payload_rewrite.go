package assets

import (
	"context"
	"strings"
)

// payload_rewrite.go owns the shared applyRewrite orchestrator plus
// the two public role-specific entry points
// (RewriteVoiceoverPayload / RewriteSceneImagePayload). The role-
// specific collector+applicator pairs live in rewrite_voiceover.go
// and rewrite_scene_images.go; this file deliberately does NOT
// contain those implementations so each role stays a pure payload
// navigator with no DB / blob-store / filesystem knowledge.

// RewriteVoiceoverPayload resolves all voiceover references in the
// payload and rewrites them to canonical velox-asset:// references.
//
// PR15.6: delegates to applyRewrite with role=RoleVoiceover; the
// collector+applicator pair are the only role-specific knowledge.
func (s *AssetService) RewriteVoiceoverPayload(ctx context.Context, payloadMap map[string]interface{}) error {
	return s.applyRewrite(ctx, payloadMap, RoleVoiceover, collectVoiceoverReferences, applyVoiceoverReferences)
}

// RewriteSceneImagePayload resolves all scene image references in the
// payload and rewrites them to canonical velox-asset:// references.
func (s *AssetService) RewriteSceneImagePayload(ctx context.Context, payloadMap map[string]interface{}) error {
	return s.applyRewrite(ctx, payloadMap, RoleSceneImage, collectSceneImageReferences, applySceneImageReferences)
}

// applyRewrite is the shared collect→resolve→apply worker used by every
// payload rewrite flavor. It iterates the references collected from the
// payload, looks each one up via the asset service, then writes the
// canonical references back through the role-specific applicator.
//
// Parameters
//   - ctx:               request context for resolver + DB calls.
//   - payloadMap:        mutable payload to rewrite (top-level and parameters
//     sub-map, depending on the applicator).
//   - kind:              asset role label (RoleVoiceover / RoleSceneImage etc.)
//     used when registering a freshly-resolved asset.
//   - collector:         extracts references from the payload. Must dedup
//     internally; returns nil/empty when nothing to do.
//   - applicator:        writes the rewritten references back. Receives the
//     already-resolved canonical reference list (each item
//     is either a velox-asset:// scheme URL returned by
//     ResolveAndRegister, or an already-canonical URL
//     that was passed-through).
//
// Errors from the asset service surface to the caller verbatim. nil-nil
// short-circuits when the service is gone or the payload is nil; missing
// or empty collector result is a no-op.
func (s *AssetService) applyRewrite(
	ctx context.Context,
	payloadMap map[string]interface{},
	kind string,
	collector func(map[string]interface{}) []string,
	applicator func(map[string]interface{}, []string),
) error {
	if s == nil || payloadMap == nil || collector == nil || applicator == nil {
		return nil
	}
	references := collector(payloadMap)
	if len(references) == 0 {
		return nil
	}

	refs := make([]string, 0, len(references))
	for _, ref := range references {
		trimmed := strings.TrimSpace(ref)
		if trimmed == "" {
			continue
		}
		// Skip already-canonical velox-asset references — nothing to do.
		if strings.HasPrefix(trimmed, VeloxAssetScheme+"://") {
			refs = append(refs, trimmed)
			continue
		}
		asset, err := s.ResolveAndRegister(ctx, ResolveAssetCommand{
			Kind:      kind,
			Reference: trimmed,
		})
		if err != nil {
			return err
		}
		if asset == nil {
			continue
		}
		refs = append(refs, asset.Reference())
	}
	if len(refs) == 0 {
		return nil
	}

	applicator(payloadMap, refs)
	return nil
}
