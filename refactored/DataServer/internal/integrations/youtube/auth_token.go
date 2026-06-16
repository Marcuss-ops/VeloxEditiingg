package youtube

import (
	"context"
	"fmt"
)

// RevokeToken is the AuthManager-side façade for OAuth revocation. The
// canonical orchestration lives in Service.RevokeToken (which routes
// through the repository: HTTP Google revoke + MarkYouTubeOAuthTokenRevoked
// + JSON file delete + RAM delete). This method exists for
// source-compatibility with handlers and tests that held a *AuthManager
// reference; new callers should invoke Service.RevokeToken directly.
//
// Distinct from AuthManager.SaveChannelToken (which persists), distinct
// from AuthManager.DeleteChannel (which doesn't exist here \u2014 channel
// removal lives on Service.DeleteChannel and uses
// SQLiteStore.DeleteChannelAtomic for transactional cleanup).
func (am *AuthManager) RevokeToken(ctx context.Context, channelID string) error {
	if am == nil || am.service == nil {
		return fmt.Errorf("revoke: auth manager not wired")
	}
	return am.service.RevokeToken(ctx, channelID)
}

// saveChannelToken persists an AuthChannel's OAuth credentials. Delegates
// to Service.saveChannelToken so the two save paths stay byte-for-byte
// identical (canonical path, JSON-secrets-only payload). Logged via the
// service path so operators see one consistent message source.
func (am *AuthManager) saveChannelToken(channel *AuthChannel) error {
	if am == nil || am.service == nil {
		return fmt.Errorf("saveChannelToken: auth manager not wired to service")
	}
	return am.service.saveChannelToken(channel)
}
