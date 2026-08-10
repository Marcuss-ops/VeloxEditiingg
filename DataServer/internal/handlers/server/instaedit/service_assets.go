package instaedit

import (
	"context"
	"fmt"
)

// GetAsset returns a single workspace-scoped asset.
func (s *Service) GetAsset(ctx context.Context, workspaceID int64, assetID string) (*assetResponse, error) {
	asset, err := s.assets.GetByIDAndWorkspace(ctx, assetID, workspaceID)
	if err != nil {
		return nil, err
	}
	if asset == nil {
		return nil, fmt.Errorf("%w: asset %s", ErrNotFound, assetID)
	}
	a := mapAsset(asset, workspaceID)
	return &a, nil
}
