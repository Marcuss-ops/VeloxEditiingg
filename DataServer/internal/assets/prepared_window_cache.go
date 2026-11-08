package assets

import (
	"context"
	"fmt"
	"strings"
)

// LookupPreparedWindowAsset returns the certified fragment registered by any
// worker for a content-preparation identity. FINALIZE consults this before
// publishing a preparation task, so a window prepared for one job is reused
// by another job and by a different assembly worker without re-encoding,
// re-probing or transferring the source again.
//
// The identity is a hash over the canonical source window, output profile
// revision and preparation revision, so a hit already implies matching
// evidence; READY status plus a certified SHA-256/size still fail closed.
func (s *AssetService) LookupPreparedWindowAsset(ctx context.Context, identity string) (*PreparedWindowAssetBinding, error) {
	if s == nil || s.repo == nil {
		return nil, fmt.Errorf("prepared-window cache lookup is unavailable")
	}
	identity = strings.TrimSpace(identity)
	if len(identity) != 64 {
		return nil, fmt.Errorf("prepared-window cache identity must be a SHA-256")
	}
	asset, err := s.GetBySourceReference(ctx, "prepared-window:"+identity)
	if err != nil {
		return nil, fmt.Errorf("prepared-window cache lookup: %w", err)
	}
	if asset == nil {
		return nil, nil
	}
	if asset.Status != AssetStatusReady || !validStockSourceSHA256(asset.SHA256) || asset.SizeBytes <= 0 {
		return nil, fmt.Errorf("prepared-window cache asset %q is not a certified READY fragment", identity)
	}
	return &PreparedWindowAssetBinding{AssetID: asset.AssetID, SHA256: asset.SHA256, SizeBytes: asset.SizeBytes}, nil
}
