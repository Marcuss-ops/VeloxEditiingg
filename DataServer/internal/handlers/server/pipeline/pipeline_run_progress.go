// Package pipeline / pipeline_run_progress.go — pipeline run advancement
// phase. Extracted from pipeline_run_actions.go: the run timeline (lifecycle
// events + job events + attempts).
package pipeline

import (
	"errors"
	"net/http"
	"sort"
	"strings"
	"time"

	"velox-server/internal/pipelineruns"
	"velox-server/internal/store"

	"github.com/gin-gonic/gin"
)

// PipelineRunTimeline handles GET /api/v1/pipeline-runs/:id/timeline.
//
// Returns a chronological list of events for the pipeline run. Events
// come from:
//  1. The pipeline_run's own state transitions (created_at, updated_at,
//     completed_at).
//  2. job_events for the Velox job (when velox_job_id is set).
//  3. job_attempts for the Velox job.
//
// Events are sorted by timestamp ascending.
func (h *Handlers) PipelineRunTimeline() gin.HandlerFunc {
	return func(c *gin.Context) {
		if h.store == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"ok": false, "error": "pipeline store not wired"})
			return
		}
		idParam := strings.TrimSpace(c.Param("id"))
		if idParam == "" {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "id is required"})
			return
		}

		ctx := c.Request.Context()
		clientID := strings.TrimSpace(ClientIDFromContext(c))
		pr, forwarding, err := h.lookupPipelineRun(ctx, idParam, clientID)
		if err != nil {
			if errors.Is(err, errPipelineRunNotFound) {
				if clientID != "" {
					writeM2MJobNotFound(c)
				} else {
					c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "pipeline run not found"})
				}
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
			return
		}

		events := []gin.H{}

		// 1. Pipeline run lifecycle events.
		events = append(events, gin.H{
			"timestamp": pr.CreatedAt,
			"stage":     string(pipelineruns.StageRemote),
			"event":     "pipeline_run_created",
			"status":    string(pipelineruns.StatusAccepted),
		})
		if pr.Status != pipelineruns.StatusAccepted {
			events = append(events, gin.H{
				"timestamp": pr.UpdatedAt,
				"stage":     string(pr.Status.StageOf()),
				"event":     "status_changed",
				"status":    string(pr.Status),
			})
		}
		if pr.ErrorCode != "" {
			events = append(events, gin.H{
				"timestamp":     pr.UpdatedAt,
				"stage":         pr.FailedStage,
				"event":         "error",
				"error_code":    pr.ErrorCode,
				"error_message": pr.ErrorMessage,
			})
		}
		if !pr.CompletedAt.IsZero() {
			events = append(events, gin.H{
				"timestamp": pr.CompletedAt,
				"stage":     string(pipelineruns.StageTerminal),
				"event":     "pipeline_run_completed",
				"status":    string(pr.Status),
			})
		}

		// 2. Job events for the Velox job.
		veloxJobID := pr.VeloxJobID
		if veloxJobID == "" && forwarding != nil {
			veloxJobID = forwarding.TargetJobID
		}
		if veloxJobID != "" {
			var jobEvents []store.JobEvent
			if clientID != "" {
				jobEvents, _ = h.store.ListJobEventsForClient(ctx, veloxJobID, clientID, 100)
			} else {
				jobEvents, _ = h.store.ListJobEvents(veloxJobID, 100)
			}
			for _, e := range jobEvents {
				events = append(events, gin.H{
					"timestamp": e.Timestamp,
					"stage":     string(pipelineruns.StageWorker),
					"event":     e.Event,
					"job_id":    veloxJobID,
					"raw":       e.RawJSON,
				})
			}

			// 3. Job attempts.
			var attempts []store.JobAttempt
			if clientID != "" {
				attempts, _ = h.store.GetJobAttemptsForClient(ctx, veloxJobID, clientID, 50)
			} else {
				attempts, _ = h.store.GetJobAttempts(veloxJobID, 50)
			}
			for _, a := range attempts {
				events = append(events, gin.H{
					"timestamp": a.StartedAt,
					"stage":     string(pipelineruns.StageWorker),
					"event":     "job_attempt",
					"job_id":    veloxJobID,
					"attempt":   a.AttemptNumber,
					"worker":    a.WorkerID,
					"status":    a.Status,
					"error":     a.ErrorCode,
				})
			}
		}

		// Sort events by timestamp ascending.
		sort.SliceStable(events, func(i, j int) bool {
			return eventTimestamp(events[i]) < eventTimestamp(events[j])
		})

		c.JSON(http.StatusOK, gin.H{
			"ok":              true,
			"pipeline_run_id": pr.ID,
			"events":          events,
			"count":           len(events),
		})
	}
}

// eventTimestamp extracts a comparable string from a timeline event.
// time.Time values format as RFC3339 (lexically sortable); string
// timestamps from the DB are also RFC3339. Empty strings sort first.
func eventTimestamp(e gin.H) string {
	switch v := e["timestamp"].(type) {
	case time.Time:
		return v.UTC().Format(time.RFC3339)
	case string:
		return v
	default:
		return ""
	}
}
