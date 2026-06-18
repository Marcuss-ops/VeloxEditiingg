package assets

import (
	"context"
	"net/http"
	"strings"
	"time"
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

	registry := NewRegistry(NewResolversFromStore(store, drive, httpClient)...)

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


