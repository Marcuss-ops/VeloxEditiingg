package jobs

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"velox-server/internal/config"
	"velox-server/internal/queue"
)

type JobSubmissionHandler struct {
	cfg   *config.Config
	fileQ *queue.FileQueue
}

func NewJobSubmissionHandler(cfg *config.Config, fileQ *queue.FileQueue) *JobSubmissionHandler {
	return &JobSubmissionHandler{
		cfg:   cfg,
		fileQ: fileQ,
	}
}

func (h *JobSubmissionHandler) CreateJobHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var jobData map[string]interface{}
		if err := c.ShouldBindJSON(&jobData); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid JSON payload", "detail": err.Error()})
			return
		}

		if len(jobData) == 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Empty payload"})
			return
		}

		isValid, validationErrors, validationDetails := validateJobPayload(jobData)
		if !isValid {
			c.JSON(http.StatusBadRequest, gin.H{
				"error":   "VALIDAZIONE FALLITA",
				"errors":  validationErrors,
				"details": validationDetails,
				"hint":    "Il job richiede almeno 1 clip E almeno 1 voiceover per essere processato",
			})
			return
		}

		splitByVoiceover, _ := jobData["split_by_voiceover"].(bool)

		var jobPayloads []map[string]interface{}
		if splitByVoiceover {
			jobPayloads = splitByVoiceoverJobs(jobData)
		} else {
			jobPayloads = []map[string]interface{}{jobData}
		}

		createdIDs := []string{}
		dedupedIDs := []string{}

		for _, payload := range jobPayloads {
			jobID, normalized, fingerprint := buildSingleJob(payload)

			dupID := h.findDuplicate(fingerprint, normalized)
			if dupID != "" {
				dedupedIDs = append(dedupedIDs, dupID)
				continue
			}

			if err := h.fileQ.SubmitJob(c.Request.Context(), jobID, normalized); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			createdIDs = append(createdIDs, jobID)
		}

		if splitByVoiceover {
			c.JSON(http.StatusOK, gin.H{
				"status":             "PENDING",
				"job_ids":            createdIDs,
				"deduplicated_ids":   dedupedIDs,
				"count_created":      len(createdIDs),
				"count_deduplicated": len(dedupedIDs),
				"split_by_voiceover": true,
			})
			return
		}

		if len(createdIDs) > 0 {
			c.JSON(http.StatusOK, gin.H{
				"status":       "PENDING",
				"job_id":       createdIDs[0],
				"deduplicated": false,
			})
			return
		}

		if len(dedupedIDs) > 0 {
			c.JSON(http.StatusOK, gin.H{
				"status":       "PENDING",
				"job_id":       dedupedIDs[0],
				"deduplicated": true,
			})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"status":       "PENDING",
			"job_id":       uuid.New().String(),
			"deduplicated": false,
		})
	}
}

func (h *JobSubmissionHandler) findDuplicate(fingerprint string, normalized map[string]interface{}) string {
	if h.fileQ == nil {
		return ""
	}

	jobs, err := h.fileQ.GetAllJobs(context.Background())
	if err != nil {
		return ""
	}

	outputVideoID := getStringOrEmpty(normalized, "output_video_id")
	if outputVideoID == "" {
		if slotData, ok := normalized["slot_data"].(map[string]interface{}); ok {
			outputVideoID = getStringOrEmpty(slotData, "output_video_id")
		}
	}

	for existingID, existing := range jobs {
		if existing.Status != queue.StatusPending && existing.Status != queue.StatusProcessing {
			continue
		}

		if outputVideoID != "" {
			existingOutID := getStringOrEmpty(existing.Payload, "output_video_id")
			if existingOutID == "" {
				if existing.SlotData != nil {
					existingOutID = getStringOrEmpty(existing.SlotData, "output_video_id")
				}
			}
			if existingOutID != "" && existingOutID == outputVideoID {
				return existingID
			}
		}

		if existing.JobFingerprint == fingerprint {
			return existingID
		}
	}

	return ""
}
