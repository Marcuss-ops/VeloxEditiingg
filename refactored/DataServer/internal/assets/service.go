package assets

import (
	"context"
	"net/http"
	"strings"
	"time"

	"velox-shared/payload"
)

// Service exposes the voiceover asset bridge.
type Service struct {
	store    *Store
	registry *Registry
}

// NewService builds a voiceover bridge for the master.
func NewService(dataDir string, allowedRoots []string, maxBytes int64, drive DriveDownloader) *Service {
	store := NewStore(dataDir, maxBytes, allowedRoots)
	httpClient := &http.Client{
		Timeout: 90 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 5 {
				return http.ErrUseLastResponse
			}
			return nil
		},
	}

	registry := NewRegistry(
		&veloxAssetResolver{store: store},
		&driveResolver{store: store, drive: drive},
		&localFileResolver{store: store},
		&httpResolver{store: store, http: httpClient},
	)

	return &Service{
		store:    store,
		registry: registry,
	}
}

// Store returns the canonical audio store.
func (s *Service) Store() *Store {
	if s == nil {
		return nil
	}
	return s.store
}

// Resolve resolves a single voiceover reference.
func (s *Service) Resolve(ctx context.Context, reference string) (*ResolvedAsset, error) {
	if s == nil || s.registry == nil {
		return nil, newAcquisitionError("voiceover_path", "unknown", "voiceover asset service unavailable", nil)
	}
	return s.registry.Resolve(ctx, reference)
}

// ResolveAll resolves the provided references in order, deduplicating by value.
func (s *Service) ResolveAll(ctx context.Context, references []string) ([]*ResolvedAsset, error) {
	if s == nil {
		return nil, newAcquisitionError("voiceover_path", "unknown", "voiceover asset service unavailable", nil)
	}
	seen := make(map[string]struct{}, len(references))
	resolved := make([]*ResolvedAsset, 0, len(references))
	for _, ref := range references {
		trimmed := strings.TrimSpace(ref)
		if trimmed == "" {
			continue
		}
		if _, exists := seen[trimmed]; exists {
			continue
		}
		seen[trimmed] = struct{}{}
		asset, err := s.Resolve(ctx, trimmed)
		if err != nil {
			return nil, err
		}
		resolved = append(resolved, asset)
	}
	return resolved, nil
}

// RewriteVoiceoverPayload resolves all mirrored voiceover fields and rewrites
// them to canonical velox-asset references in place.
func (s *Service) RewriteVoiceoverPayload(ctx context.Context, payloadMap map[string]interface{}) error {
	if s == nil || payloadMap == nil {
		return nil
	}
	references := collectVoiceoverReferences(payloadMap)
	if len(references) == 0 {
		return nil
	}

	resolved, err := s.ResolveAll(ctx, references)
	if err != nil {
		return err
	}
	if len(resolved) == 0 {
		return nil
	}

	refs := make([]string, 0, len(resolved))
	for _, asset := range resolved {
		if asset == nil {
			continue
		}
		refs = append(refs, asset.VeloxReference())
	}
	if len(refs) == 0 {
		return nil
	}

	applyVoiceoverReferences(payloadMap, refs)
	return nil
}

func collectVoiceoverReferences(payloadMap map[string]interface{}) []string {
	if payloadMap == nil {
		return nil
	}
	candidates := []string{
		payload.FirstString(payloadMap, "voiceover_path", "audio_path", "voiceover", "unified_voiceover_link"),
	}
	if v, ok := payloadMap["voiceover_paths"]; ok {
		candidates = append(candidates, payload.NormalizeToStrings(v)...)
	}
	if params, ok := payloadMap["parameters"].(map[string]interface{}); ok {
		candidates = append(candidates, payload.FirstString(params, "voiceover_path", "audio_path", "voiceover"))
		if v, ok := params["voiceover_paths"]; ok {
			candidates = append(candidates, payload.NormalizeToStrings(v)...)
		}
	}
	return payload.DedupeStrings(candidates)
}

func applyVoiceoverReferences(payloadMap map[string]interface{}, refs []string) {
	if len(refs) == 0 || payloadMap == nil {
		return
	}
	first := refs[0]
	payloadMap["voiceover_paths"] = append([]string(nil), refs...)
	payloadMap["voiceover_path"] = first
	payloadMap["audio_path"] = first
	if params, ok := payloadMap["parameters"].(map[string]interface{}); ok {
		params["voiceover_paths"] = append([]string(nil), refs...)
		params["voiceover_path"] = first
		params["audio_path"] = first
		payloadMap["parameters"] = params
	}
}
