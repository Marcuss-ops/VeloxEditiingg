package uploads

import (
	"log"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	"velox-server/internal/artifacts"
	"velox-server/internal/config"
)

// UploadCompletedVideo handles video file upload from workers.
// POST /api/v1/video/upload-completed
//
// PR 3.5-c rewrite: the HTTP upload handler now goes through the canonical
// artifacts.Service pipeline (BeginUpload → Receive → Finalize) — the same
// path used by the gRPC ArtifactUploaded handler. This is the SINGLE
// authoritative path for promoting a job to SUCCEEDED.
//
// Flow:
//  1. Parse multipart form fields (job_id, worker_id, lease_id, attempt)
//  2. artifacts.Service.BeginUpload — validates job auth, creates
//     artifacts + artifact_uploads atomically
//  3. artifacts.Service.Receive — streams bytes to staging, master-computes
//     SHA-256, post-write verification
//  4. artifacts.Service.Finalize — CAS RECEIVED→FINALIZING, then
//     FinalizeVerified (jobs SUCCEEDED + artifacts READY + outbox events +
//     delivery inserts) all in one atomic SQL transaction
func UploadCompletedVideo(cfg *config.Config, artifactSvc *artifacts.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
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

		ext := "video"
		if header != nil && header.Filename != "" {
			ext = header.Filename
		}

		revision, _ := strconv.Atoi(c.PostForm("revision"))
		attempt, _ := strconv.Atoi(c.PostForm("attempt"))
		if attempt < 1 {
			attempt = 1
		}

		ctx := c.Request.Context()

		// Step 1: BeginUpload — creates artifacts + artifact_uploads atomically.
		// Validates job RUNNING, worker/lease ownership, attempt RENDER_FINISHED.
		session, beginErr := artifactSvc.BeginUpload(ctx, artifacts.BeginUploadCommand{
			JobID:            jobID,
			WorkerID:         workerID,
			LeaseID:          leaseID,
			AttemptNumber:    attempt,
			ExpectedRevision: revision,
			Kind:             ext,
		})
		if beginErr != nil {
			log.Printf("[UPLOAD] BeginUpload failed for %s: %v", jobID, beginErr)
			c.JSON(http.StatusBadRequest, gin.H{
				"ok":     false,
				"error":  "BeginUpload rejected: " + beginErr.Error(),
				"job_id": jobID,
			})
			return
		}

		// Step 2: Receive — stream bytes to staging, master-computes SHA-256.
		recv, recvErr := artifactSvc.Receive(ctx, session.UploadID, file)
		if recvErr != nil {
			log.Printf("[UPLOAD] Receive failed for %s upload=%s: %v", jobID, session.UploadID, recvErr)
			c.JSON(http.StatusInternalServerError, gin.H{
				"ok":        false,
				"error":     "Receive rejected: " + recvErr.Error(),
				"upload_id": session.UploadID,
			})
			return
		}

		// Step 3: Finalize — CAS RECEIVED→FINALIZING → FinalizeVerified
		// (jobs SUCCEEDED + artifacts READY + outbox + delivery in one tx).
		art, finErr := artifactSvc.Finalize(ctx, artifacts.FinalizeArtifactCommand{
			UploadID:         session.UploadID,
			JobID:            jobID,
			WorkerID:         workerID,
			LeaseID:          leaseID,
			ExpectedRevision: revision,
		})
		if finErr != nil {
			log.Printf("[UPLOAD] Finalize failed for %s upload=%s: %v", jobID, session.UploadID, finErr)
			c.JSON(http.StatusInternalServerError, gin.H{
				"ok":        false,
				"error":     "Finalize rejected: " + finErr.Error(),
				"upload_id": session.UploadID,
			})
			return
		}

		log.Printf("[UPLOAD] Artifact pipeline complete: job=%s artifact=%s upload=%s sha256=%s size=%d",
			jobID, art.ID, session.UploadID, recv.ReceivedSHA256[:16]+"...", recv.ReceivedSizeBytes)

		c.JSON(http.StatusOK, gin.H{
			"ok":          true,
			"job_id":      jobID,
			"artifact_id": art.ID,
			"upload_id":   session.UploadID,
			"status":      "SUCCEEDED",
			"size":        recv.ReceivedSizeBytes,
			"sha256":      recv.ReceivedSHA256,
		})
	}
}
