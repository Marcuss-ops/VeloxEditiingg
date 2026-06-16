// Package youtube provides YouTube API integration for the Velox server.
package youtube

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"golang.org/x/oauth2"
	"google.golang.org/api/youtube/v3"
	ytanalytics "google.golang.org/api/youtubeanalytics/v2"

	"velox-server/internal/config"
	"velox-server/internal/secrets/aesgcm"
	"velox-server/internal/store/youtubetypes"
)

// os shim: previously used by the deleted saveChannelToken JSON writer and
// the deleted RevokeToken JSON file-delete step. Both paths are removed in
// step S6. The import is dropped from the top because module uses of os
// outside those paths do not exist (see channels.go, which was rewritten
// to drop the same os/path/filepath/strings imports).

// YouTubeStore defines the full SQLite-backed YouTube persistence
// interface, avoiding a direct import of the store package. Uses only
// canonical tables (youtube_channels, youtube_groups_v2,
// youtube_group_channels, youtube_cache).
//
// Relationship to YouTubeRepository (repository.go): YouTubeStore is the
// SQL-only contract Code on this branch keeps depending on. *SQLiteStore
// satisfies it today. YouTubeRepository is the wider single canonical
// interface that embeds YouTubeStore and adds the four in-memory
// methods (LoadData, SaveData, syncGroupLocked, saveAllReconcile).
// Code on this branch keeps depending on YouTubeStore; new code MUST
// depend on YouTubeRepository directly. A future compose(...) helper
// will yield a single concrete type satisfying both interfaces
// (YouTubeRepository ⊃ YouTubeStore).
//
// Module-level consumers (Cache.SetStore, the boot hydrator wiring,
// the quota manager) all depend on the full YouTubeStore. The Service
// runtime depends on a STRICT SUBSET — see ServiceStore below. Module
// wiring hands a wider YouTubeStore into NewService which satisfies
// ServiceStore implicitly via Go's structural interface typing.
type YouTubeStore interface {
	// Canonical: YouTube Channels (youtube_channels table)
	ListYouTubeChannels() ([]map[string]interface{}, error)
	GetYouTubeChannel(channelID string) (map[string]interface{}, error)
	UpsertYouTubeChannel(channelID, title, displayName, channelURL, thumbnailURL, language, notes string, viewCount, subCount int64, addedAt, lastSyncAt string) error
	// UpdateYouTubeChannelMetadata persists a YouTube-API metadata refresh into
	// youtube_channels targeting only title and thumbnail. Distinct from the
	// wide UpsertYouTubeChannel above: refresh never writes metadata_json nor
	// any user-edited column (display_name, language, view/sub counts, notes,
	// channel_url), so it cannot silently wipe them. Use this from
	// Service.RefreshChannelMetadata; use UpsertYouTubeChannel for initial
	// channel ingest.
	// Typed update methods. Each touches ONE column (or a small typed
	// cluster) without clobbering the others. Use these from operator-
	// edit paths and from targeted refresh / stats endpoints. Use
	// UpsertYouTubeChannel only on first-time channel ingest where every
	// column needs to be seeded. Errors are surfaced — callers must
	// adopt DB-first ordering (write to the store, mirror to RAM only
	// after the write returns nil).
	UpdateChannelTitle(channelID, title string) error
	UpdateChannelLanguage(channelID, language string) error
	UpdateChannelNotes(channelID, notes string) error
	UpdateChannelStats(channelID string, viewCount, subCount int64, lastSyncAt string) error
	UpdateYouTubeChannelMetadata(channelID, title, thumbnailURL string) error
	DeleteYouTubeChannel(channelID string) error
	// DeleteChannelAtomic removes the channel row, all group memberships,
	// and (via FK CASCADE) the matching youtube_oauth_tokens row in a
	// single SQLite transaction. Returns the count of memberships cleared
	// for the audit endpoint. Used by Service.DeleteChannel (full remove);
	// distinct from MarkYouTubeOAuthTokenRevoked (used by Service.RevokeToken
	// for the disable-but-keep semantics).
	DeleteChannelAtomic(channelID string) (int64, error)

	// Canonical: YouTube OAuth Tokens (youtube_oauth_tokens — encryption at-rest
	// is the caller's responsibility via internal/secrets/aesgcm).
	// UpsertYouTubeOAuthToken stores already-encrypted BLOBs; the store layer
	// does not hold a cipher. Use a Read-Modify-Write cycle in the service
	// layer when only one of (access, refresh) needs to change.
	UpsertYouTubeOAuthToken(channelID string, accessTokenEnc, refreshTokenEnc []byte, tokenType, expiry, scopes string, keyVersion int) error
	// ListActiveYouTubeOAuthTokens returns every non-revoked oauth row for
	// the boot hydrator (loadOAuthChannelsFromSQLite). Decryption is the
	// caller's responsibility. Returns an empty slice when no active rows
	// exist (never (nil, nil) wrapped in nil-row for missing entries).
	ListActiveYouTubeOAuthTokens() ([]map[string]interface{}, error)
	// AuditYouTubeOAuthTokenOrphans returns channel_ids in
	// youtube_oauth_tokens whose parent youtube_channels row is missing.
	// The boot hydrator logs these on startup so operators know whether
	// the canonical set is fully consistent.
	AuditYouTubeOAuthTokenOrphans() ([]youtubetypes.YouTubeTokenOrphan, error)
	// ConnectChannelAtomic creates a youtube_channels row and the matching
	// youtube_oauth_tokens row in ONE SQLite transaction. Fixes the FK
	// cascade failure mode where UpsertYouTubeOAuthToken was called before
	// the parent channel row existed.
	ConnectChannelAtomic(channel *youtubetypes.YouTubeChannelSeed, accessTokenEnc, refreshTokenEnc []byte, tokenType, expiry, scopes string, keyVersion int) error
	// GetYouTubeOAuthToken returns the encrypted row for channelID, or
	// (nil, nil) when the row does not exist. BLOB columns surface as
	// []byte; the caller decrypts via a matching aesgcm.Encryptor.
	GetYouTubeOAuthToken(channelID string) (map[string]interface{}, error)
	// MarkYouTubeOAuthTokenRevoked records a revocation timestamp on the
	// OAuth row. Idempotent (does not overwrite an existing revoked_at).
	MarkYouTubeOAuthTokenRevoked(channelID string) error

	// Canonical: YouTube Groups V2 (youtube_groups_v2 + youtube_group_channels)
	ListYouTubeGroupsV2() ([]map[string]interface{}, error)
	UpsertYouTubeGroupV2(name, groupType, description, privacy string) (int64, error)
	GetYouTubeGroupV2ID(name, groupType string) (int64, error)
	DeleteYouTubeGroupV2(id int64) error
	AddChannelToGroupV2(groupID int64, channelID string) error
	RemoveChannelFromGroupV2(groupID int64, channelID string) error
	DeleteYouTubeGroupChannelsByGroupID(groupID int64) error
	DeleteYouTubeGroupChannelsByChannelID(channelID string) error
	ListGroupChannelsV2(groupID int64) ([]string, error)
	ListAllGroupMembershipsV2() ([]map[string]interface{}, error)

	// Cache (shared)
	GetYouTubeCache(key string) (int64, string, error)
	SetYouTubeCache(key string, timestamp int64, dataJSON string) error
	CleanupYouTubeCache(maxAge int64) (int64, error)
	ClearYouTubeCache() error
	MigrateYouTubeCache(entries map[string]struct {
		Timestamp int64       `json:"timestamp"`
		Data      interface{} `json:"data"`
	}) (int, error)
}

// ServiceStore is the narrower contract *Service.store depends on —
// every method that Service body code calls via s.store.X() across
// channels.go / backfill.go / groups.go / oauth.go / service.go /
// storage_*.go. Strict SUBSET of YouTubeStore; explicit list (NOT an
// embedding) so Go's structural interface typing does NOT widen it
// back to the full YouTubeStore surface.
//
// Module wiring hands a wider YouTubeStore (a *SQLiteStore) into
// NewService which satisfies ServiceStore implicitly via structural
// typing — values implementing YouTubeStore also implement any subset
// declared explicitly here.
//
// Generated from `rg 's\.store\.' internal/integrations/youtube`. If
// a future S11 repo migration adds a new s.store.X call site, regenerate
// the membership test mock AND add the matching method on this interface.
// If the call site lives on *Storage (legacy) rather than *Service, leave
// both untouched.
type ServiceStore interface {
	// --- Channels (canonical: youtube_channels) ---
	ListYouTubeChannels() ([]map[string]interface{}, error)
	GetYouTubeChannel(channelID string) (map[string]interface{}, error)
	UpsertYouTubeChannel(channelID, title, displayName, channelURL, thumbnailURL, language, notes string, viewCount, subCount int64, addedAt, lastSyncAt string) error
	UpdateChannelTitle(channelID, title string) error
	UpdateChannelLanguage(channelID, language string) error
	UpdateYouTubeChannelMetadata(channelID, title, thumbnailURL string) error
	DeleteChannelAtomic(channelID string) (int64, error)

	// --- OAuth tokens (canonical: youtube_oauth_tokens) ---
	UpsertYouTubeOAuthToken(channelID string, accessTokenEnc, refreshTokenEnc []byte, tokenType, expiry, scopes string, keyVersion int) error
	ListActiveYouTubeOAuthTokens() ([]map[string]interface{}, error)
	AuditYouTubeOAuthTokenOrphans() ([]youtubetypes.YouTubeTokenOrphan, error)
	GetYouTubeOAuthToken(channelID string) (map[string]interface{}, error)
	MarkYouTubeOAuthTokenRevoked(channelID string) error

	// --- Groups V2 (canonical: youtube_groups_v2 + youtube_group_channels) ---
	ListYouTubeGroupsV2() ([]map[string]interface{}, error)
	UpsertYouTubeGroupV2(name, groupType, description, privacy string) (int64, error)
	GetYouTubeGroupV2ID(name, groupType string) (int64, error)
	DeleteYouTubeGroupV2(id int64) error
	AddChannelToGroupV2(groupID int64, channelID string) error
	RemoveChannelFromGroupV2(groupID int64, channelID string) error
	DeleteYouTubeGroupChannelsByGroupID(groupID int64) error
	ListGroupChannelsV2(groupID int64) ([]string, error)
	ListAllGroupMembershipsV2() ([]map[string]interface{}, error)
}

// Service provides YouTube API functionality
type Service struct {
	config      *ServiceConfig
	oauthConfig *oauth2.Config
	channels    map[string]*AuthChannel
	groups      map[string]*ChannelGroup
	mu          sync.RWMutex
	cache       *Cache
	store       ServiceStore

	// oauthBuf holds the AES-GCM cipher used by HandleOAuthCallback and
	// the OAuth auto-refresh path to encrypt credentials before writing
	// them to youtube_oauth_tokens. Always non-nil after construction:
	// NewService returns an error when cipher == nil, so the field is
	// set once at construction time inside NewService (fail-closed).
	// The previously-present SetOAuthSecretCipher side-channel is gone.
	oauthBuf *aesgcm.Encryptor

	authManager  *AuthManager
	uploader     *Uploader
	videoManager *VideoManager
	quotaManager *QuotaManager
}

// NewService creates a new YouTube service.
// store is optional — if nil, in-memory-only mode is used.
// cipher is required: the OAuth callback, the auto-refresh path, and the
// boot hydrator all need an AES-GCM cipher to read/write the encrypted
// blobs in youtube_oauth_tokens. Passing nil returns an error so the
// module wiring fails closed instead of silently degrading. The
// previously-present SetOAuthSecretCipher side-channel is gone — a
// service without a cipher is a programmer error at construction
// time, not an operator choice. Integration tests that exercise
// non-OAuth paths can still construct *Service via the struct literal
// directly (oauthBuf field), which keeps the unit tests independent
// of this fail-closed gate.
func NewService(cfg *ServiceConfig, store ServiceStore, cipher *aesgcm.Encryptor) (*Service, error) {
	if cfg.TokensDir == "" {
		if env := config.GetYouTubeTokensDir(); env != "" {
			cfg.TokensDir = env
		} else if cfg.DataDir != "" {
			cfg.TokensDir = filepath.Join(cfg.DataDir, "secrets", "youtube", "tokens")
		} else {
			cfg.TokensDir = filepath.Join(".velox", "secrets", "youtube", "tokens")
		}
	}
	if cfg.YoutubePostingPath == "" {
		if env := config.GetYouTubePostingPath(); env != "" {
			cfg.YoutubePostingPath = env
		} else {
			cfg.YoutubePostingPath = "YoutubePosting"
		}
	}

	if cipher == nil {
		return nil, fmt.Errorf("youtube.NewService: oauth cipher required (set VELOX_YT_OAUTH_TOKEN_KEY); refusing to construct service without AES key")
	}

	s := &Service{
		config:   cfg,
		store:    store,
		channels: make(map[string]*AuthChannel),
		groups:   make(map[string]*ChannelGroup),
		cache:    NewCache(cfg.DataDir, 12*time.Hour, store),
		// Cipher is mounted at construction time. The boot hydrator
		// (loadOAuthChannelsFromSQLite) can now read encrypted blobs
		// immediately, before any external SetOAuthSecretCipher call
		// — eliminating the "cipher nil on first boot" race called
		// out in the re-analysis.
		oauthBuf: cipher,
	}

	s.authManager = NewAuthManager(s)
	s.uploader = NewUploader(s)
	s.videoManager = NewVideoManager(s)
	s.quotaManager = NewQuotaManager(s)

	if err := s.loadOAuthConfig(); err != nil {
		log.Printf("[WARN] YouTube OAuth config not loaded: %v", err)
	}

	// Boot hydrator two-phase:
	//   1. SQLite-first: loadOAuthChannelsFromSQLite reads every non-revoked
	//      youtube_oauth_tokens row, decrypts, and rebuilds s.channels.
	//      This is the canonical path; it runs whenever a cipher is wired.
	//   2. JSON-fallback (deprecated, see step S6 of the migration plan):
	//      if the cipher is missing we still walk account_*.json so an
	//      operator installing without VELOX_YT_OAUTH_TOKEN_KEY can boot.
	//      Once the module becomes fail-closed on a missing cipher, the
	//      fallback is removed entirely.
	// SQLite-only boot: AES-GCM credentials are decrypted from
	// youtube_oauth_tokens and rebuilt into s.channels. JSON fallbacks are
	// gone (step S6 of the migration plan). The module wires the cipher
	// via SetOAuthSecretCipher before NewService is called and fails
	// closed if VELOX_YT_OAUTH_TOKEN_KEY is missing.
	s.loadOAuthChannelsFromSQLite()
	// Load from canonical tables — store is already set, so this works immediately
	s.loadCanonicalChannels()
	s.loadCanonicalGroups()

	return s, nil
}

// AuthManager returns the auth manager
func (s *Service) AuthManager() *AuthManager {
	return s.authManager
}

// Uploader returns the uploader
func (s *Service) Uploader() *Uploader {
	return s.uploader
}

// VideoManager returns the video manager
func (s *Service) VideoManager() *VideoManager {
	return s.videoManager
}

// QuotaManager returns the quota manager
func (s *Service) QuotaManager() *QuotaManager {
	return s.quotaManager
}

// SetStore sets the SQLite store for persistence, type-asserting from interface{}.
// If a store was already provided via NewService, this is a no-op.
// If called for the first time, it reloads data from the store.
func (s *Service) SetStore(st interface{}) {
	if s.store != nil {
		return // Already set via NewService
	}
	if store, ok := st.(YouTubeStore); ok {
		s.store = store
		s.cache.SetStore(store)
		s.loadCanonicalChannels()
		s.loadCanonicalGroups()
	}
}

// --- Public API: OAuth (Delegated to AuthManager) ---

func (s *Service) GetOAuthStartURL(channelName string, redirectURL string) string {
	return s.authManager.GetOAuthStartURL(channelName, redirectURL)
}

func (s *Service) HandleOAuthCallback(ctx context.Context, code string, channelName string, redirectURL string) (*Channel, error) {
	return s.authManager.HandleOAuthCallback(ctx, code, channelName, redirectURL)
}

func (s *Service) ValidateToken(ctx context.Context, channelID string) (map[string]interface{}, error) {
	return s.authManager.ValidateToken(ctx, channelID)
}

// RevokeToken is the canonical orchestration for taking a channel's OAuth
// credentials out of service WITHOUT removing the channel row from
// youtube_channels. Distinct from DeleteChannel (which nukes the channel
// + oauth row + groups + JSON, FK-cascaded) — see verdict/rationale in
// docs/youtube_sqlite_migration_plan.md step S5d.
//
// Sequence (deterministic order):
//  1. HTTP POST to Google oauth2 revoke endpoint (best-effort: the credential
//     may already be invalid, so a non-200 is logged but does not abort).
//  2. UPDATE youtube_oauth_tokens SET revoked_at = now WHERE channel_id = ? AND
//     revoked_at IS NULL (atomic SQL via the repository's
//     MarkYouTubeOAuthTokenRevoked; idempotent).
//  3. Remove the canonical account_<channel>.json on disk (best-effort:
//     orphan JSON is acceptable and garbage-collected later).
//  4. Delete the channel from the in-memory s.channels under the service's
//     RWMutex (so concurrent reads cannot see a half-revoked entry).
//
// Returns nil on success; returns an error if the repository step fails
// so the caller can retry without leaving SQL state / RAM state
// inconsistent. The RAM delete still happens even on SQL error so the
// user-facing surface stays consistent (the SQL row will be flagged as
// revoked by the next attach attempt or by an admin audit).
func (s *Service) RevokeToken(ctx context.Context, channelID string) error {
	s.mu.RLock()
	ch, exists := s.channels[channelID]
	s.mu.RUnlock()
	if !exists {
		return fmt.Errorf("revoke: channel not found: %s", channelID)
	}

	// Step 1: best-effort Google revocation POST. The provider may already
	// refuse an expired token; non-2xx is logged but does not abort so a
	// local flag is still set even when the provider is unreachable.
	if ch.AccessToken != "" {
		if err := s.revokeAtGoogle(ctx, ch.AccessToken); err != nil {
			log.Printf("[WARN] revoke: Google endpoint POST failed for %s: %v", channelID, err)
		}
	} // Step 2: atomic SQLite mark-revoked. This is THE single source-of-truth
	// step. A non-nil error here means the credential is still considered
	// active server-side, so we return the error WITHOUT the RAM delete
	// happening so the operator can retry on the SAME cached state.
	if s.store != nil {
		if err := s.store.MarkYouTubeOAuthTokenRevoked(channelID); err != nil {
			return fmt.Errorf("revoke: persist revoked_at to sqlite: %w", err)
		}
	}

	// JSON-token-file delete is gone (step S6 verbatim). SQLite
	// owns the credential; any orphan JSON on disk is harmless
	// because the boot hydrator (loadOAuthChannelsFromSQLite)
	// never reads from `account_*.json` and the boot never reaches
	// it now that requireIfMissing=true is in effect.

	// Step 4: in-memory delete only after the canonical store has accepted
	// the revocation, so concurrent readers don't see a half-revoked
	// channel. Done under the write lock so the map iteration order is
	// deterministic in tests.
	s.mu.Lock()
	delete(s.channels, channelID)
	s.mu.Unlock()

	log.Printf("[OK] Channel %s credentials revoked and removed from cache", channelID)
	return nil
}

// revokeAtGoogle POSTs to the Google OAuth2 token revocation endpoint.
// Returns nil on 200; non-200 responses are returned as error so the
// caller can decide whether to fail-closed (preferred for token-bearing
// endpoints) or just log (preferred for best-effort orchestration like
// RevokeToken, which already commits the local SQLite state).
//
// The HTTP body is discarded after the status check — Google's revoke
// endpoint returns no JSON body.
func (s *Service) revokeAtGoogle(ctx context.Context, accessToken string) error {
	const revokeURL = "https://oauth2.googleapis.com/revoke"
	req, err := http.NewRequestWithContext(ctx, "POST", revokeURL, strings.NewReader("token="+accessToken))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("network: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("google revoke status=%d", resp.StatusCode)
	}
	return nil
}

// --- Public API: Upload (Delegated to Uploader) ---

func (s *Service) UploadVideo(ctx context.Context, channelID string, videoPath string, config UploadConfig) (*UploadResult, error) {
	return s.uploader.UploadVideo(ctx, channelID, videoPath, config)
}

func (s *Service) SetThumbnail(ctx context.Context, channelID string, videoID string, thumbnailPath string) (string, error) {
	return s.uploader.SetThumbnail(ctx, channelID, videoID, thumbnailPath)
}

// --- Public API: Video Metadata (Delegated to VideoManager) ---

func (s *Service) UpdateVideoMetadata(ctx context.Context, channelID string, videoID string, config UploadConfig) error {
	return s.videoManager.UpdateVideoMetadata(ctx, channelID, videoID, config)
}

func (s *Service) DeleteVideo(ctx context.Context, channelID string, videoID string) error {
	return s.videoManager.DeleteVideo(ctx, channelID, videoID)
}

func (s *Service) ListVideos(ctx context.Context, channelID string, maxResults int64) ([]*youtube.Video, error) {
	return s.videoManager.ListVideos(ctx, channelID, maxResults)
}

// --- Public API: Quota/Analytics (Delegated to QuotaManager) ---

func (s *Service) GetQuotaUsage(ctx context.Context) map[string]interface{} {
	return s.quotaManager.GetQuotaUsage(ctx)
}

func (s *Service) GetAnalyticsService(ctx context.Context, channelID string) (*ytanalytics.Service, error) {
	return s.quotaManager.GetAnalyticsService(ctx, channelID)
}

func (s *Service) FetchAnalytics(ctx context.Context, channelID string, days int) (map[string]interface{}, error) {
	return s.quotaManager.FetchAnalytics(ctx, channelID, days)
}

func (s *Service) UpdateAnalyticsCache(ctx context.Context, channelID string, days int, data map[string]interface{}) error {
	return s.quotaManager.UpdateAnalyticsCache(ctx, channelID, days, data)
}
