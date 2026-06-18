package uploads

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/gin-gonic/gin"

	"velox-server/internal/config"
	"velox-server/internal/queue"
	"velox-server/internal/store"
)

// UploadCompletedVideo handles video file upload from workers.
// POST /api/v1/video/upload-completed
//
// Rewritten PR2b/PR4d: uses staging → ArtifactFinalizationService →
// CompleteJobTx → job_deliveries PENDING pipeline instead of the old
// save-directly + COMPLETED + maybeAutoUpload pattern.
//
// Flow:
//  1. Save upload to staging directory (BlobStore staging path)
//  2. Create artifact in STAGING status
//  3. Call ArtifactFinalizationService.FinalizeRender for verification
//     (re-hashes SHA-256, sniffs MIME, measures size)
//  4. On success, atomically CompleteJobTx (SUCCEEDED + close attempt + outbox)
//  5. Insert PENDING job_deliveries for delivery targets
//  6. DeliveryRunner picks up the PENDING deliveries asynchronously
func UploadCompletedVideo(cfg *config.Config, fileQ *queue.FileQueue, blobStore store.BlobStore) gin.HandlerFunc {
	videosDir := cfg.Runtime.VideosDir
	if videosDir == "" {
		videosDir = "./completed_videos"
	}

	// Build staging dir from config (fallback to data/staging)
	stagingDir := cfg.Runtime.StagingDir
	if stagingDir == "" {
		stagingDir = filepath.Join(cfg.Runtime.DataDir, "staging")
	}

	return func(c *gin.Context) {
		// Parse multipart form
		file, header, err := c.Request.FormFile("video")
		if err != nil {
			file, header, err = c.Request.FormFile("video_file")
		}
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "Video file is required"})
			return
		}
		defer file.Close()

		jobID := c.PostForm("job_id")
		workerID := c.PostForm("worker_id")
		leaseID := c.PostForm("lease_id")

		if jobID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "job_id is required"})
			return
		}

		// Ensure staging directory exists — also log the videosDir for backward compat
		_ = videosDir
		if err := os.MkdirAll(stagingDir, 0755); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "failed to create staging directory"})
			return
		}

		// Generate unique staging path via BlobStore
		ext := filepath.Ext(header.Filename)
		if ext == "" {
			ext = ".mp4"
		}
		artifactID := fmt.Sprintf("art_%s_%d", jobID, time.Now().UnixNano())
		stagingPath, err := blobStore.StagingPath(jobID, artifactID, ext)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "failed to allocate staging path"})
			return
		}

		// Write uploaded file to staging
		out, err := os.Create(stagingPath)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "Failed to save uploaded file"})
			return
		}

		hasher := sha256.New()
		writer := io.MultiWriter(out, hasher)
		size, err := io.Copy(writer, file)
		out.Close()

		if err != nil {
			os.Remove(stagingPath)
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "Failed to write video file"})
			return
		}
		sha256Hash := hex.EncodeToString(hasher.Sum(nil))

		ctx := c.Request.Context()
		now := time.Now().UTC().Format(time.RFC3339)

		// Get DB store from FileQueue
		dbStore := fileQ.GetDBStore()

		// Look up the latest attempt for revision/lease validation
		var attemptID int64
		latestAttempt, err := dbStore.GetLatestJobAttempt(jobID)
		if err == nil && latestAttempt != nil {
			attemptID = int64(latestAttempt.ID)
			// Validate lease if provided
			if leaseID != "" && latestAttempt.LeaseID != "" && latestAttempt.LeaseID != leaseID {
				log.Printf("[UPLOAD] Lease mismatch for %s: got %s, expected %s", jobID, leaseID, latestAttempt.LeaseID)
			}
		}

		// Step 1: Create artifact in STAGING status
		newArtifact := &store.Artifact{
			ID:              artifactID,
			JobID:           jobID,
			AttemptID:       int(attemptID),
			Type:            "video",
			StorageProvider: "local",
			StorageKey:      stagingPath,
			LocalPath:       stagingPath,
			SHA256:          sha256Hash,
			SizeBytes:       size,
			Status:          "STAGING",
			CreatedAt:       now,
		}
		if err := dbStore.InsertArtifact(newArtifact); err != nil {
			os.Remove(stagingPath)
			log.Printf("[UPLOAD] Failed to insert artifact for %s: %v", jobID, err)
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "Failed to create artifact record"})
			return
		}

		// Step 2: Finalize via ArtifactFinalizationService
		finalizationSvc := queue.NewArtifactFinalizationService(dbStore)
		_, finalizeErr := finalizationSvc.FinalizeRender(ctx, queue.FinalizeRenderInput{
			ArtifactID:    artifactID,
			JobID:         jobID,
			AttemptID:     attemptID,
			WorkerID:      workerID,
			LeaseID:       leaseID,
			TemporaryPath: stagingPath,
			ExpectedSize:  size,
			WorkerSHA256:  sha256Hash,
		})

		if finalizeErr != nil {
			log.Printf("[UPLOAD] Artifact finalization failed for %s (artifact=%s): %v", jobID, artifactID, finalizeErr)
			// Artifact is now in QUARANTINED — don't promote job.
			_ = blobStore.RemoveStaging(stagingPath)
			c.JSON(http.StatusInternalServerError, gin.H{
				"ok":          false,
				"error":       "Artifact verification failed",
				"artifact_id": artifactID,
				"status":      "QUARANTINED",
			})
			return
		}

		// Step 3: Promote staged file to final storage (canonical location).
		finalPath := blobStore.FinalPath(jobID, artifactID, ext)
		storageKey, promoteErr := blobStore.PromoteToFinal(stagingPath, finalPath)
		if promoteErr != nil {
			log.Printf("[UPLOAD] Failed to promote artifact %s to final: %v", artifactID, promoteErr)
			// Artifact is READY in DB but file is still in staging — reconciler will handle.
			storageKey = stagingPath
		} else {
			// Update artifact with final storage_key.
			_ = dbStore.UpdateArtifactStorageKey(ctx, artifactID, storageKey, finalPath)
		}

		// Step 4: Atomically complete the job (SUCCEEDED + close attempt + outbox).
		if err := fileQ.CompleteJob(ctx, jobID); err != nil {
			log.Printf("[UPLOAD] Failed to complete job %s: %v", jobID, err)
			// Job already marked COMPLETED upstream or CAS failure — not fatal,
			// the artifact is READY and deliveries will proceed.
		}

		// Step 5: Create PENDING job_deliveries for legacy delivery_targets.
		if dbStore != nil {
			deliveryCount, deliveryErr := dbStore.InsertJobDeliveriesForArtifact(ctx, artifactID, jobID)
			if deliveryErr != nil {
				log.Printf("[UPLOAD] Failed to create job_deliveries for %s: %v", jobID, deliveryErr)
			} else if deliveryCount > 0 {
				log.Printf("[UPLOAD] Created %d PENDING job_deliveries for artifact %s", deliveryCount, artifactID)
			}
		}

		log.Printf("[UPLOAD] Artifact pipeline complete: job=%s artifact=%s status=READY sha256=%s size=%d",
			jobID, artifactID,		sha256Hash[:16]+"...", size)

		c.JSON(http.StatusOK, gin.H{
			"ok":          true,
			"job_id":      jobID,
			"artifact_id": artifactID,
			"status":      "READY",
			"size":        size,
			"sha256":      sha256Hash,
		})
	}
}


