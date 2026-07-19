// Package api — RW-PROD-005 sessions endpoint.
//
// workers_sessions_handler.go owns the GET /api/v1/workers/:worker_id/sessions
// handler. It exposes the per-worker session history for dashboards /
// on-call tooling.
//
// SECURITY posture (canonical, see OWNERSHIP.md §3):
//   - token_hash is NEVER carried into the response. The store-layer
//     ListWorkerSessions helper omits the column from the SELECT
//     and the WorkerSessionRow struct has no TokenHash field, so a
//     future regression that re-adds it would fail to compile.
//   - ip_address IS surfaced but goes through sanitiseHostname() at
//     the handler boundary (IPv4/IPv6/long-hex redaction).
//
// File layout: see workers_metrics_handler.go header for the package
// file-split convention.
package api

import (
	"context"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"velox-server/internal/store"
)

// SessionsReader abstracts the underlying store.
type SessionsReader interface {
	ListWorkerSessions(ctx context.Context, workerID string, includeRevoked bool, limit int) ([]store.WorkerSessionRow, error)
}

// SessionsHandler holds the dependency on the sessions read store.
type SessionsHandler struct {
	reader SessionsReader
}

// NewSessionsHandler wires the handler. Returns nil when reader is
// nil so the caller can skip route registration on a no-store
// configuration.
func NewSessionsHandler(reader SessionsReader) *SessionsHandler {
	if reader == nil {
		return nil
	}
	return &SessionsHandler{reader: reader}
}

// ListWorkerSessions returns GET /api/v1/workers/:worker_id/sessions
//
// Query parameters:
//
//	limit           — optional page size, 1..MaxListLimit (default
//	                   DefaultListLimit).
//	include_revoked — optional boolean (default false). When true,
//	                   revoked sessions are included in the response.
//	                   Default is false because revoked sessions
//	                   can still expose token_hash metadata; operators
//	                   must opt in explicitly.
//
// Response: 200 WorkerSessionsListResponse, 400 on missing :worker_id,
// 503 on a nil reader.
func (h *SessionsHandler) ListWorkerSessions() gin.HandlerFunc {
	return func(c *gin.Context) {
		if h == nil || h.reader == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "sessions reader not available"})
			return
		}
		workerID := strings.TrimSpace(c.Param("worker_id"))
		if workerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "worker_id is required"})
			return
		}
		limit := clampLimit(c.Query("limit"))
		includeRevoked := parseBoolQuery(c.Query("include_revoked"))

		rows, err := h.reader.ListWorkerSessions(c.Request.Context(), workerID, includeRevoked, limit)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "list worker sessions: " + err.Error()})
			return
		}
		resp := WorkerSessionsListResponse{
			WorkerID: workerID,
			Count:    len(rows),
			Sessions: make([]SessionResponse, 0, len(rows)),
		}
		for _, r := range rows {
			resp.Sessions = append(resp.Sessions, sanitizeSession(r))
		}
		c.JSON(http.StatusOK, resp)
	}
}

// sanitizeSession converts a store.WorkerSessionRow into the
// operator-facing SessionResponse. IP goes through sanitiseHostname()
// so IPv4/IPv6/long-hex patterns cannot leak the worker's network
// topology.
func sanitizeSession(r store.WorkerSessionRow) SessionResponse {
	return SessionResponse{
		SessionID:        r.SessionID,
		WorkerID:         r.WorkerID,
		SessionType:      r.SessionType,
		Status:           r.Status,
		IPAddress:        sanitiseHostname(r.IPAddress),
		Revoked:          r.Revoked,
		ProtocolVersion:  r.ProtocolVersion,
		BundleVersion:    r.BundleVersion,
		CreatedAt:        r.CreatedAt,
		ExpiresAt:        r.ExpiresAt,
		ConnectedAt:      r.ConnectedAt,
		LastSeenAt:       r.LastSeenAt,
		DisconnectedAt:   r.DisconnectedAt,
		DisconnectReason: r.DisconnectReason,
	}
}

// parseBoolQuery accepts the canonical truthy spellings
// ("1", "true", "TRUE", "True", "yes") and treats everything else
// as false. Empty string returns false. Used by the sessions and
// events handlers for boolean query params.
func parseBoolQuery(raw string) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes":
		return true
	}
	return false
}
