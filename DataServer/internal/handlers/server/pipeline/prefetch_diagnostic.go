package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"velox-server/internal/store"
)

// PrefetchDiagnostic answers the MST-12 question the terminal API could not:
// which asset, and which prefetch phase, is keeping the job from dispatching.
// It reads the three surfaces the dispatcher itself uses — the job row, the
// prefetch.* journal, and worker_asset_downloads — and never mutates state.
//
// GET /api/v1/jobs/:id/prefetch
func (h *Handlers) PrefetchDiagnostic() gin.HandlerFunc {
	return func(c *gin.Context) {
		jobID := strings.TrimSpace(c.Param("job_id"))
		if jobID == "" {
			jobID = strings.TrimSpace(c.Param("id"))
		}
		if jobID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "job_id_required"})
			return
		}
		if h == nil || h.store == nil || h.store.DB() == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"ok": false, "error": "prefetch_diagnostic_unavailable"})
			return
		}
		job, err := h.loadPrefetchJob(c.Request.Context(), jobID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "prefetch_diagnostic_store_failure"})
			return
		}
		if job == nil {
			c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "job_not_found", "job_id": jobID})
			return
		}

		events, _ := h.store.ListJobEvents(jobID, 500)
		chronological := make([]store.JobEvent, len(events))
		for i := range events {
			chronological[len(events)-1-i] = events[i]
		}
		dispatchStatus, futurePlan := prefetchStateFromEvents(chronological)
		issue := prefetchIssueFromEvents(events)

		downloads := h.loadPrefetchDownloads(c.Request.Context(), jobID)
		items := buildPrefetchAssets(job.assets, downloads, events)
		ready, queued, downloading, failed, notScheduled := prefetchCounts(items)

		// The FutureAssetPlan can address artifacts that are produced by the job
		// itself (final_audio/final_video by content hash). Those are not part
		// of the declared input list, so a failed download for them would be
		// invisible in the per-input view above; surface them explicitly.
		declared := make(map[string]struct{}, len(job.assets)*2)
		for _, asset := range job.assets {
			for _, key := range []string{asset.AssetID, asset.AssetKey} {
				if key = strings.TrimSpace(key); key != "" {
					declared[key] = struct{}{}
				}
			}
		}
		planFailures := make([]prefetchPlanFailure, 0, 10)
		planAssets, planFailed := 0, 0
		for id, download := range downloads {
			if _, ok := declared[id]; ok {
				continue
			}
			planAssets++
			if download.state != "FAILED" {
				continue
			}
			planFailed++
			if len(planFailures) == cap(planFailures) {
				continue
			}
			planFailures = append(planFailures, prefetchPlanFailure{
				AssetID:     id,
				State:       download.state,
				ErrorCode:   download.errorCode,
				ErrorDetail: download.errorDetail,
				WorkerID:    download.workerID,
				UpdatedAt:   download.updatedAt,
			})
		}
		sort.SliceStable(planFailures, func(i, j int) bool {
			return planFailures[i].AssetID < planFailures[j].AssetID
		})

		blocking := make([]string, 0, 10)
		for _, item := range items {
			if item.State == "READY" || item.State == "CACHE_HIT" {
				continue
			}
			reason := item.ErrorDetail
			if reason == "" {
				reason = item.ErrorCode
			}
			if reason == "" {
				reason = "no prefetch event and no worker download row"
			}
			blocking = append(blocking, item.AssetID+": "+item.State+" ("+reason+")")
			if len(blocking) == cap(blocking) {
				break
			}
		}

		diagnosis := "prefetch non schedulato: nessun worker ha rivendicato gli asset"
		switch {
		case terminalJobStatus(job.status):
			diagnosis = "job terminale: " + job.status
		case planFailed > 0:
			diagnosis = fmt.Sprintf("prefetch fallito su %d asset del piano non materializzati", planFailed)
			if reason := firstNonEmpty(planFailures[0].ErrorDetail, planFailures[0].ErrorCode); reason != "" {
				diagnosis += ": " + reason
			}
		case failed > 0:
			diagnosis = fmt.Sprintf("prefetch fallito su %d/%d asset", failed, len(items))
			for _, item := range items {
				if item.State != "FAILED" {
					continue
				}
				if reason := firstNonEmpty(item.ErrorDetail, item.ErrorCode); reason != "" {
					diagnosis += ": " + reason
				}
				break
			}
		case job.workerName != "" && downloading > 0:
			diagnosis = "prefetch in corso sul worker assegnato"
		case job.workerName != "":
			diagnosis = "worker assegnato: prefetch in coda o in attesa di asset"
		case dispatchStatus != "":
			diagnosis = "nessun worker assegnato; stato journal: " + dispatchStatus
		}

		c.JSON(http.StatusOK, gin.H{
			"ok":                     true,
			"job_id":                 jobID,
			"status":                 job.status,
			"dispatch_status":        dispatchStatus,
			"future_asset_plan":      futurePlan,
			"runtime_assets_pending": job.runtimeAssetsPending,
			"created_at":             job.createdAt,
			"started_at":             job.startedAt,
			"assigned_at":            job.assignedAt,
			"worker_name":            job.workerName,
			"queue_seconds":          job.queueSeconds(),
			"diagnosis":              diagnosis,
			"issue":                  issue,
			"assets": gin.H{
				"total":         len(items),
				"ready":         ready,
				"queued":        queued,
				"downloading":   downloading,
				"failed":        failed,
				"not_scheduled": notScheduled,
			},
			"plan_assets":   planAssets,
			"plan_failed":   planFailed,
			"plan_failures": planFailures,
			"blocking":      blocking,
			"items":         items,
		})
	}
}

type prefetchJob struct {
	status               string
	createdAt            string
	startedAt            string
	assignedAt           string
	workerName           string
	runtimeAssetsPending bool
	assets               []prefetchPayloadAsset
}

type prefetchPayloadAsset struct {
	AssetID    string `json:"asset_id"`
	AssetKey   string `json:"asset_key"`
	Kind       string `json:"kind"`
	Role       string `json:"role"`
	SizeBytes  int64  `json:"size_bytes"`
	DurationMS int64  `json:"duration_ms"`
}

type prefetchDownload struct {
	state           string
	attempt         int
	bytesDownloaded int64
	bytesTotal      int64
	errorCode       string
	errorDetail     string
	workerID        string
	updatedAt       string
}

// prefetchPlanFailure is a resolved download for an artifact the plan asked for
// but the payload never declared (typically an output of a previous stage).
type prefetchPlanFailure struct {
	AssetID     string `json:"asset_id"`
	State       string `json:"prefetch_state"`
	ErrorCode   string `json:"error_code,omitempty"`
	ErrorDetail string `json:"error_detail,omitempty"`
	WorkerID    string `json:"worker_id,omitempty"`
	UpdatedAt   string `json:"updated_at,omitempty"`
}

type prefetchAssetView struct {
	AssetID         string `json:"asset_id"`
	Kind            string `json:"kind,omitempty"`
	Role            string `json:"role,omitempty"`
	SizeBytes       int64  `json:"size_bytes,omitempty"`
	DurationMS      int64  `json:"duration_ms,omitempty"`
	State           string `json:"prefetch_state"`
	Attempt         int    `json:"attempt,omitempty"`
	BytesDownloaded int64  `json:"bytes_downloaded,omitempty"`
	BytesTotal      int64  `json:"bytes_total,omitempty"`
	ErrorCode       string `json:"error_code,omitempty"`
	ErrorDetail     string `json:"error_detail,omitempty"`
	WorkerID        string `json:"worker_id,omitempty"`
	UpdatedAt       string `json:"updated_at,omitempty"`
}

func (h *Handlers) loadPrefetchJob(ctx context.Context, jobID string) (*prefetchJob, error) {
	var (
		job         prefetchJob
		startedAt   sql.NullString
		assignedAt  sql.NullString
		workerName  sql.NullString
		requestJSON sql.NullString
		pending     sql.NullBool
	)
	err := h.store.DB().QueryRowContext(ctx,
		`SELECT status, created_at, started_at, assigned_at, worker_name, request_json,
		        json_extract(request_json, '$.runtime_assets_pending')
		   FROM jobs WHERE job_id = ?`, jobID,
	).Scan(&job.status, &job.createdAt, &startedAt, &assignedAt, &workerName, &requestJSON, &pending)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	job.startedAt = startedAt.String
	job.assignedAt = assignedAt.String
	job.workerName = workerName.String
	job.runtimeAssetsPending = pending.Bool
	if requestJSON.Valid {
		var payload struct {
			RuntimeAssets []prefetchPayloadAsset `json:"runtime_assets"`
		}
		if json.Unmarshal([]byte(requestJSON.String), &payload) == nil {
			job.assets = payload.RuntimeAssets
		}
	}
	return &job, nil
}

func (h *Handlers) loadPrefetchDownloads(ctx context.Context, jobID string) map[string]prefetchDownload {
	out := map[string]prefetchDownload{}
	// The prefetch manifest addresses assets by content hash while the payload
	// declares provider ids; asset_identity_catalog is the only join between
	// the two. Indexing every alias keeps a failed download attributed to the
	// asset the operator actually submitted.
	rows, err := h.store.DB().QueryContext(ctx, `
		SELECT d.asset_id, c.asset_id AS source_asset_id,
		       d.state, d.attempt, d.bytes_downloaded, d.bytes_total,
		       d.error_code, d.error_detail, d.worker_id, d.updated_at
		  FROM job_asset_refs r
		  JOIN worker_asset_downloads d
		    ON d.worker_id = r.worker_id AND d.asset_key = r.asset_key
		  LEFT JOIN asset_identity_catalog c
		    ON c.sha256 = d.asset_id
		 WHERE r.job_id = ?
		 ORDER BY d.updated_at DESC`, jobID)
	if err != nil {
		return out
	}
	defer rows.Close()
	for rows.Next() {
		var (
			assetID         sql.NullString
			sourceID        sql.NullString
			state           sql.NullString
			errorCode       sql.NullString
			errorDetail     sql.NullString
			workerID        sql.NullString
			updatedAt       sql.NullString
			attempt         sql.NullInt64
			bytesDownloaded sql.NullInt64
			bytesTotal      sql.NullInt64
		)
		if rows.Scan(&assetID, &sourceID, &state, &attempt, &bytesDownloaded, &bytesTotal,
			&errorCode, &errorDetail, &workerID, &updatedAt) != nil {
			continue
		}
		download := prefetchDownload{
			state:           strings.ToUpper(strings.TrimSpace(state.String)),
			attempt:         int(attempt.Int64),
			bytesDownloaded: bytesDownloaded.Int64,
			bytesTotal:      bytesTotal.Int64,
			errorCode:       errorCode.String,
			errorDetail:     errorDetail.String,
			workerID:        workerID.String,
			updatedAt:       updatedAt.String,
		}
		for _, key := range []string{
			assetID.String,
			sourceID.String,
			strings.TrimPrefix(assetID.String, "sha256:"),
		} {
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			if _, seen := out[key]; seen {
				continue // newest first
			}
			out[key] = download
		}
	}
	return out
}

// buildPrefetchAssets merges the declared payload, the durable download rows and
// the prefetch journal. An asset with no evidence anywhere is NOT_SCHEDULED:
// that is the state the old terminal API could not distinguish from progress.
func buildPrefetchAssets(assets []prefetchPayloadAsset, downloads map[string]prefetchDownload, events []store.JobEvent) []prefetchAssetView {
	eventState := prefetchAssetStates(events)
	items := make([]prefetchAssetView, 0, len(assets))
	for _, asset := range assets {
		id := firstNonEmpty(asset.AssetID, asset.AssetKey)
		item := prefetchAssetView{
			AssetID:    id,
			Kind:       asset.Kind,
			Role:       asset.Role,
			SizeBytes:  asset.SizeBytes,
			DurationMS: asset.DurationMS,
			State:      "NOT_SCHEDULED",
		}
		if download, ok := downloads[id]; ok {
			item.State = download.state
			item.Attempt = download.attempt
			item.BytesDownloaded = download.bytesDownloaded
			item.BytesTotal = download.bytesTotal
			item.ErrorCode = download.errorCode
			item.ErrorDetail = download.errorDetail
			item.WorkerID = download.workerID
			item.UpdatedAt = download.updatedAt
		}
		if state, ok := eventState[id]; ok && item.State == "NOT_SCHEDULED" {
			item.State = state
		}
		if item.State == "" {
			item.State = "UNKNOWN"
		}
		items = append(items, item)
	}
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].State == items[j].State {
			return items[i].AssetID < items[j].AssetID
		}
		return items[i].State < items[j].State
	})
	return items
}

func prefetchAssetStates(events []store.JobEvent) map[string]string {
	out := map[string]string{}
	// events is newest first: walk backwards so the newest prefetch event wins.
	for i := len(events) - 1; i >= 0; i-- {
		event := events[i]
		if !strings.HasPrefix(event.Event, "prefetch.") {
			continue
		}
		var raw map[string]any
		if json.Unmarshal([]byte(event.RawJSON), &raw) != nil {
			continue
		}
		key := firstNonEmpty(prefetchStringField(raw, "asset_id"), prefetchStringField(raw, "asset_key"))
		if key == "" {
			continue
		}
		switch event.Event {
		case "prefetch.asset_ready":
			// Older workers mislabeled a failed resolution as asset_ready and
			// carried the failure only in error_reason. Keep those events
			// truthful while workers roll forward to the asset_failed event.
			if strings.TrimSpace(prefetchStringField(raw, "error_reason")) != "" {
				out[key] = "FAILED"
			} else {
				out[key] = "READY"
			}
		case "prefetch.asset_failed", "prefetch.prefetch_failed", "prefetch.prefetch_error":
			out[key] = "FAILED"
		case "prefetch.prefetch_preparation_waiting":
			out[key] = "WAITING"
		}
	}
	return out
}

func prefetchCounts(items []prefetchAssetView) (ready, queued, downloading, failed, notScheduled int) {
	for _, item := range items {
		switch item.State {
		case "READY", "CACHE_HIT":
			ready++
		case "QUEUED", "CACHE_CHECK", "RETRY_WAIT", "WAITING":
			queued++
		case "DOWNLOADING":
			downloading++
		case "FAILED", "CANCELLED":
			failed++
		default:
			notScheduled++
		}
	}
	return
}

func (j *prefetchJob) queueSeconds() int64 {
	created, err := time.Parse(time.RFC3339, j.createdAt)
	if err != nil {
		return 0
	}
	end := time.Now()
	if j.startedAt != "" {
		if started, err := time.Parse(time.RFC3339, j.startedAt); err == nil {
			end = started
		}
	}
	return int64(end.Sub(created).Seconds())
}

func terminalJobStatus(status string) bool {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "SUCCEEDED", "FAILED", "CANCELLED", "CANCELED", "REJECTED":
		return true
	default:
		return false
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func prefetchStringField(raw map[string]any, key string) string {
	value, _ := raw[key].(string)
	return value
}
