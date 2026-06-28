// Package audit exposes operator-facing introspection endpoints for the
// Velox persistence layer. Designed as a smoke test for the YouTube
// catalog refactor: pulls live counts and the safety-guard historical
// record so an operator can verify there is exactly one source of truth
// and no destructive rewrites have happened.
package audit

import (
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"velox-server/internal/config"
	youtubeintegration "velox-server/internal/integrations/youtube"
	"velox-server/internal/store"
)

// PersistenceHandler reads the live YouTube integration Service, the canonical
// SQLite database on disk, and the audit snapshots derived from each.
//
// PR-YT-REPO: `ytStorage *youtubeintegration.Storage` is replaced by
// `ytsvc *youtubeintegration.Service` — the Storage facade was
// deleted when YouTubeStore + StorageStore were merged into a single
// Repository owned by *youtube.Service. The runtime still requires
// Group + Channel counts for the audit view; both are now produced
// via GetGroups() / GetAllChannels() on the Service.
type PersistenceHandler struct {
	cfg         *config.Config
	sqliteStore *store.SQLiteStore
	ytsvc       *youtubeintegration.Service
}

// NewPersistenceHandler builds a handler. All deps are optional; missing
// ones degrade the response gracefully instead of erroring.
//
// PR-YT-REPO: the third argument switched from
// *youtubeintegration.Storage to *youtubeintegration.Service.
func NewPersistenceHandler(cfg *config.Config, sqliteStore *store.SQLiteStore, ytsvc *youtubeintegration.Service) *PersistenceHandler {
	return &PersistenceHandler{
		cfg:         cfg,
		sqliteStore: sqliteStore,
		ytsvc:       ytsvc,
	}
}

// Handle implements GET /api/v1/audit/persistence.
func (h *PersistenceHandler) Handle(c *gin.Context) {
	resp := gin.H{
		"generated_at_utc": time.Now().UTC().Format(time.RFC3339Nano),
		"source_of_truth":  h.sourceOfTruth(),
		"live_counts":      h.liveCounts(),
		"dual_db_status":   h.dualDBStatus(),
		"safety_guard":     h.safetyGuardStatus(),
	}
	c.JSON(http.StatusOK, resp)
}

// sourceOfTruth returns the canonical persistence mapping for the YouTube
// catalog. JSON files are listed only where they are either OAuth-required
// or legacy-tolerant (catalog data is NEVER on JSON).
//
// PR15.4: every YouTube canonical owner now points to the SQL-only
// YouTubeStore contract (no `Storage.data.Groups` in-RAM mirror,
// no reconciler / safety-guard / per-group diff — those structs and
// methods were removed from integrations/youtube/storage.go + the
// storage_persistence.go / save_status.go files are deleted).
func (h *PersistenceHandler) sourceOfTruth() gin.H {
	return gin.H{
		"youtube_channels": gin.H{
			"backend": "sqlite",
			"table":   "youtube_channels",
			"owner":   "DataServer/internal/integrations/youtube/storage_channels.go (SQL pass-through via YouTubeStore)",
			"primary": true,
		},
		"youtube_groups": gin.H{
			"backend": "sqlite",
			"table":   "youtube_groups",
			"owner":   "DataServer/internal/integrations/youtube/storage_groups.go (SQL pass-through via YouTubeStore)",
			"primary": true,
		},
		"youtube_group_channels": gin.H{
			"backend": "sqlite",
			"table":   "youtube_group_channels",
			"owner":   "DataServer/internal/integrations/youtube/storage_channels.go / storage_groups.go (per-row SQL via YouTubeStore)",
			"primary": true,
		},
		"youtube_tracked_niches": gin.H{
			"backend": "sqlite",
			"table":   "youtube_tracked_niches",
			"owner":   "DataServer/internal/integrations/youtube/storage_groups.go (CleanupOldData tracked-niche sweep)",
			"primary": true,
		},
		"youtube_api_cache": gin.H{
			"backend": "sqlite",
			"table":   "youtube_api_cache",
			"owner":   "DataServer/internal/integrations/youtube/cache.go",
			"primary": true,
		},
		"oauth_tokens": gin.H{
			"backend": "sqlite",
			"table":   "youtube_oauth_tokens",
			"owner":   "DataServer/internal/integrations/youtube/auth_oauth.go (ConnectChannelAtomic / UpsertYouTubeOAuthToken)",
			"primary": true,
			"note":    "Encrypted at-rest via AES-256-GCM (access_token_encrypted / refresh_token_encrypted BLOBs). `channels.go` JSON writer was removed in S6.",
		},
		"google_credentials": gin.H{
			"backend": "json",
			"path":    "<dataDir>/secrets/youtube/credentials/client_secret.json (or credentials.json)",
			"owner":   "DataServer/internal/integrations/youtube/auth.go (findOAuthSecretFile)",
			"primary": false,
			"note":    "Google-required; cannot be moved into SQLite without re-implementing the OAuth client_secret format.",
		},
	}
}

// liveCounts returns the snapshot produced by *youtube.Service.LoadData()
// reflecting the canonical SQLite read at the moment of the request.
// Distinct fields:
//   - groups_total: count of manager/upload groups loaded
//   - channels_total: kept for SPA backward compat — equals channels_in_groups
//   - channels_in_groups: total Channel entries summed across every group
//     (counts duplicates if a channel is in multiple groups)
//   - channels_assigned: distinct channel IDs that appear in any group
//   - channels_undefined: catalog channels not in any manager group
//
// PR-YT-REPO: StorageData.Groups values are *Group (Channels []Channel)
// so iteration of group.Channels yields a Channel (with ID/Title/etc.)
// — the canonical post-PR15.4 shape, free of the legacy in-memory
// mirror that was destroyed by Storage facade removal.
func (h *PersistenceHandler) liveCounts() gin.H {
	result := gin.H{
		"available":          false,
		"groups_total":       0,
		"channels_total":     0,
		"channels_in_groups": 0,
		"channels_assigned":  0,
		"channels_undefined": 0,
	}

	if h.ytsvc == nil {
		result["reason"] = "youtube_storage_unavailable"
		return result
	}
	data := h.ytsvc.LoadData()
	groups := data.Groups
	if groups == nil {
		groups = map[string]*youtubeintegration.Group{}
	}
	result["available"] = true
	result["groups_total"] = len(groups)

	totalInGroups := 0
	assignedIDs := make(map[string]bool)
	for _, g := range groups {
		if g == nil {
			continue
		}
		totalInGroups += len(g.Channels)
		for _, ch := range g.Channels {
			if ch.ID != "" {
				assignedIDs[ch.ID] = true
			}
		}
	}
	result["channels_total"] = totalInGroups
	result["channels_in_groups"] = totalInGroups
	result["channels_assigned"] = len(assignedIDs)
	result["channels_undefined"] = h.deriveUndefined(assignedIDs)
	return result
}

// deriveUndefined returns the count of catalog channels not present in the
// assigned set. O(n) over GetAllChannels which is fine for the catalog sizes
// the audit endpoint targets (hundreds to low thousands).
//
// PR-YT-REPO: GetAllChannels is now served by *youtube.Service.
func (h *PersistenceHandler) deriveUndefined(assignedIDs map[string]bool) int {
	if h.ytsvc == nil {
		return 0
	}
	all := h.ytsvc.GetAllChannels()
	count := 0
	for _, ch := range all {
		if ch.ID == "" {
			continue
		}
		if !assignedIDs[ch.ID] {
			count++
		}
	}
	return count
}

// dualDBStatus interrogates the live SQLite DB file and surfaces duplicates
// at the well-known legacy paths so the operator can immediately see whether
// the runtime is reading from a stale source copy.
func (h *PersistenceHandler) dualDBStatus() gin.H {
	result := gin.H{
		"live_path_used":      "",
		"live_path_canonical": "",
		"live_db_exists":      false,
		"live_db_missing":     true,
		"live_db_size_bytes":  int64(0),
		"live_db_mtime_utc":   "",
		"duplicate_paths":     []string{},
	}

	if h.sqliteStore != nil {
		raw := h.sqliteStore.Path()
		result["live_path_used"] = raw
		result["live_path_canonical"] = canonicalAbs(raw)
		// Derive live_db_missing = !live_db_exists so permission errors and
		// other non-IsNotExist failures implicitly fall into the "missing"
		// bucket while stat_error explains why.
		info, err := os.Stat(raw)
		if err == nil && !info.IsDir() {
			result["live_db_exists"] = true
			result["live_db_missing"] = false
			result["live_db_size_bytes"] = info.Size()
			result["live_db_mtime_utc"] = info.ModTime().UTC().Format(time.RFC3339Nano)
		} else if err != nil && !os.IsNotExist(err) {
			result["stat_error"] = err.Error()
		}
	}

	// Well-known duplicate locations observed in production. Self-comparison
	// against the live canonical path is filtered so the same file never
	// appears as its own duplicate.
	live := ""
	if h.sqliteStore != nil {
		live = canonicalAbs(h.sqliteStore.Path())
	}
	candidates := []string{}
	if h.cfg != nil && h.cfg.Runtime.DataDir != "" {
		candidates = append(candidates,
			filepath.Join(h.cfg.Runtime.DataDir, "..", "data", "velox.db"),
			filepath.Join(h.cfg.Runtime.DataDir, "worker_runtime", "velox.db"),
			filepath.Join(h.cfg.Runtime.DataDir, ".velox", "data", "velox.db"),
		)
	}
	dups := []string{}
	for _, c := range candidates {
		cc := canonicalAbs(c)
		if cc == "" || cc == live {
			continue
		}
		info, err := os.Stat(c)
		if err != nil || info.IsDir() {
			continue
		}
		dups = append(dups, c+" (size="+strconv.FormatInt(info.Size(), 10)+
			" mtime="+info.ModTime().UTC().Format(time.RFC3339Nano)+")")
	}
	result["duplicate_paths"] = dups
	return result
}

// canonicalAbs returns an absolute canonical path, or "" when the input is
// empty. Used for stable duplicate-path comparison.
func canonicalAbs(p string) string {
	if p == "" {
		return ""
	}
	if abs, err := filepath.Abs(p); err == nil {
		return filepath.Clean(abs)
	}
	return filepath.Clean(p)
}

// safetyGuardStatus returns the PR15.4 status for the deleted safety-guard.
//
// PR15.4 dropped the in-RAM `Storage.data.Groups` mirror and the
// associated memory-vs-DB safety guard (safetyGuardMinRatio /
// safetyGuardMinDBGroups / ErrSaveRefusedBySafetyGuard). The Storage
// struct is now a thin SQL pass-through so there is no in-memory vs DB
// ratio to track and no full-state rewrite to refuse. The audit
// endpoint now surfaces this as `safety_guard_disabled` with the
// rationale so operators can confirm the cutover happened.
func (h *PersistenceHandler) safetyGuardStatus() gin.H {
	if h.ytsvc == nil {
		return gin.H{
			"available": false,
			"reason":    "youtube_storage_unavailable",
		}
	}
	return gin.H{
		"available":   true,
		"status":      "safety_guard_disabled",
		"description": "PR15.4 dropped Storage.data.Groups + memory-vs-DB reconciler guards. Storage is now a thin SQL pass-through; every write is a non-destructive per-row mutation via YouTubeStore. No destructive rewrite path to guard against.",
		"cutover":     "PR15.4 (youtube one-source-of-truth)",
		"prior_state": gin.H{
			"in_memory_groups": "removed (Storage.data.Groups dropped)",
			"safety_guard":     "removed (safetyGuardMinRatio / safetyGuardMinDBGroups / ErrSaveRefusedBySafetyGuard / saveAllReconcile / syncGroupLocked obsolete)",
			"reconciler":       "removed (no memory-vs-DB divergence possible)",
		},
		"notes": []string{
			"Live counts (groups_total / channels_total / channels_undefined) remain the operator-correct view; they are populated by SQL queries on every call.",
			"A destructive full-state rewrite is no longer expressible through Storage — if a rebuild is ever needed it must be done by an explicit per-row script (e.g. a sqlite_jobs_writer migration).",
		},
	}
}
