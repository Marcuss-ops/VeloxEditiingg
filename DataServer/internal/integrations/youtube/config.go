// Package youtube: config.go owns the configuration bootstrapping
// logic for the Service — environment-variable defaults, manager
// wiring, and the OAuth config load.
//
// ServiceConfig itself stays in models.go (it is a domain type that
// is constructed elsewhere in the server and passed by value/
// pointer into NewService). What belongs here is anything that
// translates a partially-populated ServiceConfig into a wired,
// ready-to-use *Service:
//
//   - applyConfigDefaults fills the empty optional fields
//     (TokensDir, YoutubePostingPath) from environment variables with
//     a documented default fallback. It mutates the receiver (the
//     ServiceConfig fields are addressable only via pointer).
//
//   - wireServiceManagers installs the four sub-managers
//     (AuthManager, Uploader, VideoManager, QuotaManager) onto the
//     Service and triggers the OAuth config load.
//
// Both helpers are package-level (not methods on *Service) so they
// can't accidentally re-enter the lifecycle of an in-flight Service
// — they are construction-only utilities invoked once from
// NewService.
package youtube

import (
	"log"
	"os"
	"path/filepath"
)

// applyConfigDefaults scans cfg for empty optional fields and fills
// them from environment variables with a documented default fallback:
//
//   - cfg.TokensDir:
//     1. cfg.TokensDir if non-empty (caller already set).
//     2. VELOX_YOUTUBE_TOKENS_DIR environment variable.
//     3. <cfg.DataDir>/secrets/youtube/tokens (default layout).
//
//   - cfg.YoutubePostingPath:
//     1. cfg.YoutubePostingPath if non-empty.
//     2. VELOX_YOUTUBE_POSTING_PATH environment variable.
//     3. "YoutubePosting" (legacy default).
//
// Mutates cfg in place; panics-free.
//
// Order of preference is deterministic and matches the original
// NewService body so existing operator scripts that set the env
// vars continue to behave identically.
func applyConfigDefaults(cfg *ServiceConfig) {
	if cfg == nil {
		return
	}
	if cfg.TokensDir == "" {
		if env := os.Getenv("VELOX_YOUTUBE_TOKENS_DIR"); env != "" {
			cfg.TokensDir = env
		} else {
			cfg.TokensDir = filepath.Join(cfg.DataDir, "secrets", "youtube", "tokens")
		}
	}
	if cfg.YoutubePostingPath == "" {
		if env := os.Getenv("VELOX_YOUTUBE_POSTING_PATH"); env != "" {
			cfg.YoutubePostingPath = env
		} else {
			cfg.YoutubePostingPath = "YoutubePosting"
		}
	}
}

// wireServiceManagers installs the four sub-managers on s and
// triggers the OAuth config load. order matters:
//
//  1. AuthManager — needed first because the OAuth load in step 4
//     uses the AuthManager's client_id/client_secret lookup.
//  2. Uploader        — independent.
//  3. VideoManager    — independent.
//  4. QuotaManager    — wraps the other managers plus the DB; its
//     own SetStore/SetDB is invoked by app/youtube.go AFTER the
//     service is fully wired (the comment on NewQuotaManager keeps
//     this contract explicit).
//  5. s.loadOAuthConfig() — best-effort: a failure here only logs a
//     warning, it does NOT fail construction. Operators who want to
//     surface the failure can call s.AuthManager() / s.loadOAuthConfig()
//     again after init.
//
// The receiver `s` must already have its config + repo + cache wired;
// this function assumes the caller has populated those fields.
func wireServiceManagers(s *Service) {
	s.authManager = NewAuthManager(s)
	s.uploader = NewUploader(s)
	s.videoManager = NewVideoManager(s)
	// PR-YT-REPO: QuotaManager construction is last because its DB / repo
	// wiring is completed out-of-band by app/youtube.go via the
	// QuotaManager's own SetStore/SetDB methods. NewQuotaManager only
	// needs the Service receiver; the deep wiring is intentionally
	// deferred so the app layer can resolve the canonical repo URL
	// without forcing Service construction to wait on it.
	s.quotaManager = NewQuotaManager(s)
	if err := s.loadOAuthConfig(); err != nil {
		log.Printf("[WARN] YouTube OAuth config not loaded: %v", err)
	}
}
