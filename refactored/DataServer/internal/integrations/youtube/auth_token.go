package youtube

import (
	"context"
	"fmt"
)

// RevokeToken is the AuthManager-side façade for OAuth revocation. The
// canonical orchestration lives in Service.RevokeToken, which routes
// through the repository: HTTP Google revoke + MarkYouTubeOAuthTokenRevoked
// + RAM delete. The previous JSON file delete step is gone — SQLite is the
// single source of truth and a revoked credential stays revoked regardless
// of any on-disk JSON leftovers.
//
// This method exists for source-compatibility with handlers and tests that
// held a *AuthManager reference; new callers should invoke
// Service.RevokeToken directly.
func (am *AuthManager) RevokeToken(ctx context.Context, channelID string) error {
	if am == nil || am.service == nil {
		return fmt.Errorf("revoke: auth manager not wired")
	}
	return am.service.RevokeToken(ctx, channelID)
}
