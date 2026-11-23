package pipeline

// media_m2m.go is the read-only M2M media catalogue for the Master.
//
// The Master stores every produced or ingested asset in the `assets` table
// (asset_id = sha256, kind, status, storage_provider/storage_key, mime_type),
// with side tables `asset_sources` (origin: drive, upload, worker, …) and
// `asset_media_metadata` (ffprobe facts). This surface exposes that catalogue
// under /api/v1/media with the shapes the remote smoke (chronon.py verify)
// expects, and serves the bytes for local assets. It never mutates anything.
//
// Routes (all behind M2M scope media.read):
//
//	GET  /api/v1/media/facets
//	GET  /api/v1/media/assets
//	GET  /api/v1/media/assets/:asset_id
//	GET  /api/v1/media/assets/:asset_id/download
//	POST /api/v1/media/search
//	GET  /api/v1/jobs/:id/artifact

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
)

const mediaDefaultLimit = 50

type mediaAsset struct {
	ID           string   `json:"id"`
	Name         string   `json:"name"`
	Filename     string   `json:"filename"`
	Source       string   `json:"source"`
	Provider     string   `json:"provider"`
	MediaType    string   `json:"media_type"`
	DurationMS   int64    `json:"duration_ms"`
	SizeBytes    int64    `json:"size_bytes"`
	Lifecycle    string   `json:"lifecycle_state"`
	AssetState   string   `json:"asset_state"`
	IndexState   string   `json:"index_state"`
	HasDriveFile bool     `json:"has_drive_file"`
	HasLocalFile bool     `json:"has_local_file"`
	HasEmbedding bool     `json:"has_embedding"`
	ContentHash  string   `json:"content_hash"`
	CreatedAt    string   `json:"created_at"`
	Locations    []string `json:"locations"`
}

func (h *Handlers) mediaDB() (*sql.DB, bool) {
	if h == nil || h.store == nil || h.store.DB() == nil {
		return nil, false
	}
	return h.store.DB(), true
}

// MediaFacets reports the catalogue dimensions with the group codes the
// operator CLI prints.
func (h *Handlers) MediaFacets() gin.HandlerFunc {
	return func(c *gin.Context) {
		db, ok := h.mediaDB()
		if !ok {
			c.JSON(http.StatusServiceUnavailable, gin.H{"ok": false, "error": "media_store_unavailable"})
			return
		}
		facet := func(query string) []gin.H {
			out := []gin.H{}
			rows, err := db.QueryContext(c.Request.Context(), query)
			if err != nil {
				return out
			}
			defer rows.Close()
			for rows.Next() {
				var code string
				var count int64
				if rows.Scan(&code, &count) != nil {
					continue
				}
				out = append(out, gin.H{"code": code, "label": code, "count": count})
			}
			return out
		}
		c.JSON(http.StatusOK, gin.H{
			"ok": true,
			"sources": facet(`SELECT COALESCE(NULLIF(s.source_type,''),'unknown'), COUNT(DISTINCT s.asset_id)
			                    FROM asset_sources s JOIN assets a ON a.asset_id=s.asset_id
			                   WHERE a.deleted_at IS NULL GROUP BY 1 ORDER BY 2 DESC`),
			"providers": facet(`SELECT COALESCE(NULLIF(storage_provider,''),'unknown'), COUNT(*)
			                      FROM assets WHERE deleted_at IS NULL GROUP BY 1 ORDER BY 2 DESC`),
			"media_types": facet(`SELECT COALESCE(NULLIF(kind,''),'unknown'), COUNT(*)
			                        FROM assets WHERE deleted_at IS NULL GROUP BY 1 ORDER BY 2 DESC`),
			"lifecycle_states": facet(`SELECT COALESCE(NULLIF(status,''),'unknown'), COUNT(*)
			                             FROM assets WHERE deleted_at IS NULL GROUP BY 1 ORDER BY 2 DESC`),
			"asset_states": facet(`SELECT COALESCE(NULLIF(status,''),'unknown'), COUNT(*)
			                         FROM assets WHERE deleted_at IS NULL GROUP BY 1 ORDER BY 2 DESC`),
			"index_states": []gin.H{},
		})
	}
}

type mediaQuery struct {
	where []string
	args  []any
}

func (q *mediaQuery) add(clause string, args ...any) {
	q.where = append(q.where, clause)
	q.args = append(q.args, args...)
}

func (q *mediaQuery) sql() string {
	if len(q.where) == 0 {
		return " WHERE 1=1"
	}
	return " WHERE " + strings.Join(q.where, " AND ")
}

func mediaListQuery(c *gin.Context) *mediaQuery {
	q := &mediaQuery{where: []string{"a.deleted_at IS NULL"}}
	if v := strings.TrimSpace(c.Query("source")); v != "" {
		q.add(`EXISTS (SELECT 1 FROM asset_sources s WHERE s.asset_id=a.asset_id AND s.source_type=?)`, v)
	}
	if v := strings.TrimSpace(c.Query("provider")); v != "" {
		q.add(`a.storage_provider=?`, v)
	}
	if v := strings.TrimSpace(c.Query("media_type")); v != "" {
		q.add(`a.kind=?`, v)
	}
	if v := strings.TrimSpace(c.Query("lifecycle_state")); v != "" {
		q.add(`a.status=?`, v)
	}
	if v := strings.TrimSpace(c.Query("asset_state")); v != "" {
		q.add(`a.status=?`, v)
	}
	if strings.TrimSpace(c.Query("index_state")) != "" ||
		strings.TrimSpace(c.Query("has_embedding")) == "true" ||
		strings.TrimSpace(c.Query("has_speech")) == "true" {
		// No embedding or speech index exists in this store: an explicit filter
		// must return nothing rather than pretend the dimension is supported.
		q.add(`1=0`)
	}
	if v := strings.TrimSpace(c.Query("has_drive_file")); v == "true" {
		q.add(`EXISTS (SELECT 1 FROM asset_sources s WHERE s.asset_id=a.asset_id AND s.source_type LIKE '%drive%')`)
	}
	if v := strings.TrimSpace(c.Query("search")); v != "" {
		like := "%" + v + "%"
		q.add(`(a.asset_id LIKE ? OR a.sha256 LIKE ? OR a.metadata_json LIKE ? OR
		        EXISTS (SELECT 1 FROM asset_sources s WHERE s.asset_id=a.asset_id
		                 AND (s.source_reference LIKE ? OR s.metadata_json LIKE ?)))`,
			like, like, like, like, like)
	}
	return q
}

func mediaLimit(c *gin.Context) int {
	limit, err := strconv.Atoi(c.Query("limit"))
	if err != nil || limit <= 0 {
		return mediaDefaultLimit
	}
	if limit > 500 {
		return 500
	}
	return limit
}

func mediaOffset(c *gin.Context) int {
	offset, err := strconv.Atoi(c.Query("cursor"))
	if err != nil || offset < 0 {
		return 0
	}
	return offset
}

// MediaAssets lists the catalogue with pagination and the same filters the
// operator CLI sends.
func (h *Handlers) MediaAssets() gin.HandlerFunc {
	return func(c *gin.Context) {
		db, ok := h.mediaDB()
		if !ok {
			c.JSON(http.StatusServiceUnavailable, gin.H{"ok": false, "error": "media_store_unavailable"})
			return
		}
		q := mediaListQuery(c)
		var total int64
		if err := db.QueryRowContext(c.Request.Context(), `SELECT COUNT(*) FROM assets a`+q.sql(), q.args...).Scan(&total); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "media_query_failed"})
			return
		}
		limit, offset := mediaLimit(c), mediaOffset(c)
		rows, err := db.QueryContext(c.Request.Context(), `
			SELECT a.asset_id, a.kind, a.status, COALESCE(a.sha256,''), COALESCE(a.mime_type,''),
			       COALESCE(a.size_bytes,0), COALESCE(a.storage_provider,''), COALESCE(a.storage_key,''),
			       COALESCE(a.created_at,''), COALESCE(m.duration_ms,0),
			       COALESCE((SELECT s.source_type FROM asset_sources s
			                  WHERE s.asset_id=a.asset_id ORDER BY s.created_at LIMIT 1),'')
			  FROM assets a LEFT JOIN asset_media_metadata m ON m.asset_id=a.asset_id`+q.sql()+`
			 ORDER BY a.created_at DESC LIMIT ? OFFSET ?`, append(q.args, limit, offset)...)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "media_query_failed"})
			return
		}
		defer rows.Close()
		items := []gin.H{}
		for rows.Next() {
			if item, ok := scanMediaAsset(rows); ok {
				items = append(items, item)
			}
		}
		next := ""
		if int64(offset+limit) < total {
			next = strconv.Itoa(offset + limit)
		}
		c.JSON(http.StatusOK, gin.H{
			"ok": true, "items": items, "total": total,
			"has_more": next != "", "next_cursor": next,
		})
	}
}

// MediaAsset returns one catalogue row with its provider locations.
func (h *Handlers) MediaAsset() gin.HandlerFunc {
	return func(c *gin.Context) {
		db, ok := h.mediaDB()
		if !ok {
			c.JSON(http.StatusServiceUnavailable, gin.H{"ok": false, "error": "media_store_unavailable"})
			return
		}
		assetID := strings.TrimSpace(c.Param("asset_id"))
		row := db.QueryRowContext(c.Request.Context(), `
			SELECT a.asset_id, a.kind, a.status, COALESCE(a.sha256,''), COALESCE(a.mime_type,''),
			       COALESCE(a.size_bytes,0), COALESCE(a.storage_provider,''), COALESCE(a.storage_key,''),
			       COALESCE(a.created_at,''), COALESCE(m.duration_ms,0),
			       COALESCE((SELECT s.source_type FROM asset_sources s
			                  WHERE s.asset_id=a.asset_id ORDER BY s.created_at LIMIT 1),'')
			  FROM assets a LEFT JOIN asset_media_metadata m ON m.asset_id=a.asset_id
			 WHERE a.asset_id=? AND a.deleted_at IS NULL`, assetID)
		item, ok := scanMediaAsset(row)
		if !ok {
			c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "asset_not_found", "asset_id": assetID})
			return
		}
		c.JSON(http.StatusOK, item)
	}
}

// MediaAssetDownload serves the bytes of a local asset. Other providers are
// reported explicitly instead of proxied: the Master never fetches a remote
// storage key on behalf of a client.
func (h *Handlers) MediaAssetDownload() gin.HandlerFunc {
	return func(c *gin.Context) {
		db, ok := h.mediaDB()
		if !ok {
			c.JSON(http.StatusServiceUnavailable, gin.H{"ok": false, "error": "media_store_unavailable"})
			return
		}
		assetID := strings.TrimSpace(c.Param("asset_id"))
		var provider, key, mime, sha string
		err := db.QueryRowContext(c.Request.Context(), `
			SELECT COALESCE(storage_provider,''), COALESCE(storage_key,''),
			       COALESCE(mime_type,''), COALESCE(sha256,'')
			  FROM assets WHERE asset_id=? AND deleted_at IS NULL`, assetID).
			Scan(&provider, &key, &mime, &sha)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "asset_not_found", "asset_id": assetID})
			return
		}
		if provider != "local" {
			c.JSON(http.StatusConflict, gin.H{
				"ok": false, "error": "asset_not_servable", "asset_id": assetID,
				"storage_provider": provider,
			})
			return
		}
		if _, statErr := os.Stat(key); statErr != nil {
			c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "asset_file_missing", "asset_id": assetID})
			return
		}
		if mime == "" {
			mime = "application/octet-stream"
		}
		c.Header("Content-Type", mime)
		if sha != "" {
			c.Header("X-Content-SHA256", sha)
		}
		c.File(key)
	}
}

// MediaSearch is the hybrid query surface. The Master has no embedding index,
// so the query runs a deterministic lexical match over the same catalogue and
// reports mode=lexical: it never claims semantic results it cannot compute.
func (h *Handlers) MediaSearch() gin.HandlerFunc {
	return func(c *gin.Context) {
		db, ok := h.mediaDB()
		if !ok {
			c.JSON(http.StatusServiceUnavailable, gin.H{"ok": false, "error": "media_store_unavailable"})
			return
		}
		var body struct {
			Query string `json:"query"`
			Mode  string `json:"mode"`
			Limit int    `json:"limit"`
		}
		if err := json.NewDecoder(c.Request.Body).Decode(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "invalid_body"})
			return
		}
		query := strings.TrimSpace(body.Query)
		if query == "" {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "query_required"})
			return
		}
		limit := body.Limit
		if limit <= 0 || limit > 100 {
			limit = 20
		}
		like := "%" + query + "%"
		rows, err := db.QueryContext(c.Request.Context(), `
			SELECT a.asset_id, a.kind, a.status, COALESCE(a.sha256,''), COALESCE(a.mime_type,''),
			       COALESCE(a.size_bytes,0), COALESCE(a.storage_provider,''), COALESCE(a.storage_key,''),
			       COALESCE(a.created_at,''), COALESCE(m.duration_ms,0),
			       COALESCE((SELECT s.source_type FROM asset_sources s
			                  WHERE s.asset_id=a.asset_id ORDER BY s.created_at LIMIT 1),'')
			  FROM assets a LEFT JOIN asset_media_metadata m ON m.asset_id=a.asset_id
			 WHERE a.deleted_at IS NULL AND (a.asset_id LIKE ? OR a.sha256 LIKE ? OR a.metadata_json LIKE ?
			       OR EXISTS (SELECT 1 FROM asset_sources s WHERE s.asset_id=a.asset_id
			                   AND (s.source_reference LIKE ? OR s.metadata_json LIKE ?)))
			 ORDER BY a.created_at DESC LIMIT ?`, like, like, like, like, like, limit)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "media_query_failed"})
			return
		}
		defer rows.Close()
		items := []gin.H{}
		for rows.Next() {
			if item, ok := scanMediaAsset(rows); ok {
				item["score"] = 1.0
				items = append(items, item)
			}
		}
		c.JSON(http.StatusOK, gin.H{
			"ok": true, "items": items, "total": len(items),
			"mode": "lexical", "requested_mode": body.Mode,
		})
	}
}

// JobArtifact streams the final artifact of a job with the digest header the
// remote smoke verifies.
func (h *Handlers) JobArtifact() gin.HandlerFunc {
	return func(c *gin.Context) {
		jobID := strings.TrimSpace(c.Param("id"))
		if jobID == "" {
			jobID = strings.TrimSpace(c.Param("job_id"))
		}
		if jobID == "" || h.store == nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "job_id_required"})
			return
		}
		artifacts, err := h.store.GetArtifactsByJob(jobID, 20)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "artifact_query_failed"})
			return
		}
		path, sha, mime := "", "", ""
		for _, artifact := range artifacts {
			candidate := strings.TrimSpace(artifact.LocalPath)
			if candidate == "" && artifact.StorageProvider == "local" {
				candidate = strings.TrimSpace(artifact.StorageKey)
			}
			if candidate == "" {
				continue
			}
			if _, statErr := os.Stat(candidate); statErr != nil {
				continue
			}
			path, sha, mime = candidate, artifact.SHA256, artifact.MimeType
			if strings.Contains(strings.ToLower(artifact.Type), "final") ||
				strings.HasPrefix(mime, "video/") {
				break
			}
		}
		if path == "" {
			c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "artifact_not_found", "job_id": jobID})
			return
		}
		if mime == "" {
			mime = "application/octet-stream"
		}
		c.Header("Content-Type", mime)
		if sha != "" {
			c.Header("X-Content-SHA256", sha)
		}
		c.Header("Content-Disposition", "attachment; filename=\""+filepath.Base(path)+"\"")
		c.File(path)
	}
}

type mediaScanner interface {
	Scan(dest ...any) error
}

func scanMediaAsset(row mediaScanner) (gin.H, bool) {
	var (
		id, kind, status, sha, mime, provider, key, created, source string
		size, duration                                              int64
	)
	if err := row.Scan(&id, &kind, &status, &sha, &mime, &size, &provider, &key, &created, &duration, &source); err != nil {
		return nil, false
	}
	name := filepath.Base(key)
	if key == "" {
		name = id
	}
	locations := []string{}
	if key != "" {
		if provider == "local" {
			locations = append(locations, "local:"+key)
		} else {
			locations = append(locations, provider+":"+key)
		}
	}
	return gin.H{
		"id":              id,
		"name":            name,
		"filename":        name,
		"source":          source,
		"provider":        provider,
		"media_type":      kind,
		"mime_type":       mime,
		"duration_ms":     duration,
		"size_bytes":      size,
		"lifecycle_state": status,
		"asset_state":     status,
		"index_state":     "not_indexed",
		"has_drive_file":  strings.Contains(source, "drive"),
		"has_local_file":  provider == "local",
		"has_embedding":   false,
		"content_hash":    sha,
		"created_at":      created,
		"locations":       locations,
	}, true
}
