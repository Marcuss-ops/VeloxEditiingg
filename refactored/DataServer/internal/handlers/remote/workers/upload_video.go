package workers

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"velox-server/internal/config"
	ytservice "velox-server/internal/integrations/youtube"
	"velox-server/internal/queue"
	"velox-server/internal/store"
)

// slugify converts a string to a safe filename
func slugify(s string) string {
	// Trim whitespace
	s = strings.TrimSpace(s)
	// Replace multiple spaces with single space
	re := regexp.MustCompile(`\s+`)
	s = re.ReplaceAllString(s, " ")
	// Remove non-alphanumeric chars except dash, underscore, dot, space
	re2 := regexp.MustCompile(`[^\w\-. ]`)
	s = re2.ReplaceAllString(s, "")
	// Replace spaces with underscores
	s = strings.ReplaceAll(s, " ", "_")
	// Limit length
	if len(s) > 120 {
		s = s[:120]
	}
	if s == "" {
		return "video"
	}
	return s
}

// UploadCompletedVideo handles video file upload from workers.
// - Accepts upload_info JSON form field
// - Saves the file locally and creates an artifact record
// - Marks job as COMPLETED in file queue
// - Triggers YouTube auto-upload directly if youtube_group is configured
// - Supports video naming with video_name from job spec
func UploadCompletedVideo(cfg *config.Config, fileQ *queue.FileQueue, youtubeService *ytservice.Service) gin.HandlerFunc {
	videosDir := cfg.VideosDir
	if videosDir == "" {
		videosDir = "./completed_videos"
	}

	return func(c *gin.Context) {
		log.Printf("[RECV] RICEVUTA richiesta /upload_completed_video")

		// Ensure directory exists
		if err := os.MkdirAll(videosDir, 0755); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "failed to create videos directory"})
			return
		}

		// Get form data
		jobID := c.PostForm("job_id")
		workerID := c.PostForm("worker_id")
		uploadInfoStr := c.PostForm("upload_info")
		jobRunID := c.PostForm("job_run_id")
		leaseID := c.PostForm("lease_id")
		attempt := c.PostForm("attempt")
		contractVersion := c.PostForm("contract_version")

		if jobID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "job_id required"})
			return
		}

		log.Printf("[RECV] Ricezione video da worker %s per job %s", workerID, jobID)

		// Parse upload_info if present
		var uploadInfo map[string]interface{}
		if uploadInfoStr != "" {
			if err := json.Unmarshal([]byte(uploadInfoStr), &uploadInfo); err != nil {
				log.Printf("[WARN] Impossibile parsare upload_info: %v", err)
			}
		}

		// Get job from queue to extract video_name and other metadata
		jobData, _ := fileQ.GetJobAsMap(c.Request.Context(), jobID)

		// Build canonical upload info from job spec (source of truth)
		canonicalUploadInfo := make(map[string]interface{})
		if uploadInfo != nil {
			// Copy existing upload_info
			for k, v := range uploadInfo {
				canonicalUploadInfo[k] = v
			}
		}
		if jobData != nil {
			// Override with job spec values
			if v, ok := jobData["youtube_group"]; ok && canonicalUploadInfo["youtube_group"] == nil {
				canonicalUploadInfo["youtube_group"] = v
			}
			if v, ok := jobData["video_name"]; ok && canonicalUploadInfo["video_name"] == nil {
				canonicalUploadInfo["video_name"] = v
			}
			if v, ok := jobData["output_video_id"]; ok && canonicalUploadInfo["output_video_id"] == nil {
				canonicalUploadInfo["output_video_id"] = v
			}
			if v, ok := jobData["output_video_mapping"]; ok && canonicalUploadInfo["output_video_mapping"] == nil {
				canonicalUploadInfo["output_video_mapping"] = v
			}
			if v, ok := jobData["voiceover_channel_mapping"]; ok && canonicalUploadInfo["voiceover_channel_mapping"] == nil {
				canonicalUploadInfo["voiceover_channel_mapping"] = v
			}
			if jobRunID == "" {
				if v, ok := jobData["job_run_id"].(string); ok && strings.TrimSpace(v) != "" {
					jobRunID = strings.TrimSpace(v)
				} else if v, ok := jobData["run_id"].(string); ok && strings.TrimSpace(v) != "" {
					jobRunID = strings.TrimSpace(v)
				}
			}
			if leaseID == "" {
				if v, ok := jobData["lease_id"].(string); ok && strings.TrimSpace(v) != "" {
					leaseID = strings.TrimSpace(v)
				}
			}
			if attempt == "" {
				if v, ok := jobData["attempt"]; ok {
					attempt = fmt.Sprintf("%v", v)
				}
			}
		}
		if jobData != nil && leaseID != "" {
			if currentLease, ok := jobData["lease_id"].(string); ok && strings.TrimSpace(currentLease) != "" && !strings.EqualFold(strings.TrimSpace(currentLease), leaseID) {
				c.JSON(http.StatusConflict, gin.H{"ok": false, "error": "lease mismatch"})
				return
			}
		}
		if contractVersion != "" && contractVersion != "2" {
			c.JSON(http.StatusConflict, gin.H{"ok": false, "error": "unsupported contract version"})
			return
		}

	// Set max multipart memory to 10MB (larger files stream to disk)
	if err := c.Request.ParseMultipartForm(10 << 20); err != nil { // 10 MB
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "invalid multipart form: " + err.Error()})
		return
	}

	// Get the file
	file, header, err := c.Request.FormFile("video_file")
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "video_file required"})
			return
		}
		defer file.Close()

		// Extract extension
		ext := filepath.Ext(header.Filename)
		if ext == "" {
			ext = ".mp4"
		}

		// Build sensible filename using video_name if available
		videoName := "video"
		if v, ok := canonicalUploadInfo["video_name"].(string); ok && v != "" {
			videoName = v
		} else if jobData != nil {
			if v, ok := jobData["video_name"].(string); ok && v != "" {
				videoName = v
			}
		}
		niceName := slugify(videoName)

		outputVideoID := jobID
		if v, ok := canonicalUploadInfo["output_video_id"].(string); ok && v != "" {
			outputVideoID = v
		} else if jobData != nil {
			if v, ok := jobData["output_video_id"].(string); ok && v != "" {
				outputVideoID = v
			}
		}

		// Build filename: nice_name_outputVideoID_jobRunID.ext
		var videoFilename string
		if jobRunID != "" {
			videoFilename = fmt.Sprintf("%s_%s_%s%s", niceName, outputVideoID, jobRunID, ext)
		} else {
			videoFilename = fmt.Sprintf("%s_%s%s", niceName, outputVideoID, ext)
		}

		// Create temp file first (atomic write)
		tempName := fmt.Sprintf(".tmp_%s_%d%s", jobID, time.Now().Unix(), ext)
		tempPath := filepath.Join(videosDir, tempName)

		out, err := os.Create(tempPath)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "failed to create temp file"})
			return
		}

		// Copy file content
		hash := sha256.New()
		written, err := io.Copy(out, io.TeeReader(file, hash))
		if err != nil {
			out.Close()
			os.Remove(tempPath)
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "failed to save video"})
			return
		}
		out.Close()

		// Rename to final name (atomic)
		finalPath := filepath.Join(videosDir, videoFilename)
		if err := os.Rename(tempPath, finalPath); err != nil {
			os.Remove(tempPath)
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "failed to save video"})
			return
		}
		absFinalPath, absErr := filepath.Abs(finalPath)
		if absErr != nil {
			absFinalPath = finalPath
		}
		outputSHA256 := hex.EncodeToString(hash.Sum(nil))
		if outputSHA256 == "" {
			outputSHA256 = computeFileSHA256(finalPath)
		}
		artifactID := ""
		if outputSHA256 != "" {
			seed := fmt.Sprintf("%s:%s:%s:%s", jobID, jobRunID, attempt, outputSHA256)
			sum := sha256.Sum256([]byte(seed))
			artifactID = "artifact_" + hex.EncodeToString(sum[:])[:24]
		}
		idempotencyKey := ""
		if outputSHA256 != "" {
			seed := fmt.Sprintf("%s:%s:%s", jobID, attempt, outputSHA256)
			sum := sha256.Sum256([]byte(seed))
			idempotencyKey = hex.EncodeToString(sum[:])
		}

		fileSize := written / (1024 * 1024) // MB
		log.Printf("[OK] Video salvato: %s (%d MB)", finalPath, fileSize)

		// Mark job as COMPLETED in file queue
		if fileQ != nil {
			// Create artifact record in the artifacts table
			if dbStore := fileQ.GetDBStore(); dbStore != nil && artifactID != "" {
				attemptNum := 1
				if a := strings.TrimSpace(attempt); a != "" {
					if n, err := fmt.Sscanf(a, "%d", &attemptNum); err != nil || n == 0 {
						attemptNum = 1
					}
				}
				artifact := &store.Artifact{
					ID:              artifactID,
					JobID:           jobID,
					AttemptID:       attemptNum,
					Type:            "video",
					StorageProvider: "local",
					StorageKey:      outputSHA256,
					LocalPath:       absFinalPath,
					SHA256:          outputSHA256,
					SizeBytes:       written,
					Status:          "completed",
				}
				if err := dbStore.InsertArtifact(artifact); err != nil {
					log.Printf("[WARN] Failed to insert artifact record for %s: %v", jobID, err)
				} else {
					log.Printf("[ARTIFACT] Created artifact %s for job %s (sha256=%s, size=%d bytes)",
						artifactID, jobID, outputSHA256, written)
				}
				_ = dbStore.LogJobEvent(jobID, "artifact_uploaded", map[string]interface{}{
					"artifact_id": artifactID, "sha256": outputSHA256, "size_bytes": written,
					"worker_id": workerID, "lease_id": leaseID,
				})
			}

			now := time.Now().UTC().Format(time.RFC3339)
			updates := map[string]interface{}{
				"status":                 "COMPLETED",
				"completed_at":           now,
				"completed_by":           workerID,
				"video_uploaded":         true,
				"master_video_path":      absFinalPath,
				"result_path_worker":     absFinalPath,
				"job_run_id":             jobRunID,
				"lease_id":               leaseID,
				"artifact_id":            artifactID,
				"output_sha256":          outputSHA256,
				"upload_idempotency_key": idempotencyKey,
			}
			if attempt != "" {
				updates["attempt"] = attempt
			}
			if contractVersion != "" {
				updates["contract_version"] = contractVersion
			}
			if err := fileQ.UpdateJobFields(c.Request.Context(), jobID, updates); err != nil {
				log.Printf("[WARN] Impossibile marcare COMPLETED il job %s: %v (will be set by SubmitResult)", jobID, err)
			} else {
				log.Printf("[OK] Job %s marcato COMPLETED via upload_completed_video", jobID)
			}
		}

		// Trigger YouTube auto-upload if group is configured
		if ytGroup, ok := canonicalUploadInfo["youtube_group"].(string); ok && ytGroup != "" {
			log.Printf("[UPLOAD] YouTube group: %s", ytGroup)
			maybeAutoUploadYouTube(fileQ, youtubeService, jobID, canonicalUploadInfo, finalPath)
		}
		if vidName, ok := canonicalUploadInfo["video_name"].(string); ok && vidName != "" {
			log.Printf("[UPLOAD] Video name: %s", vidName)
		}

		c.JSON(http.StatusOK, gin.H{
			"success":         true,
			"message":         fmt.Sprintf("Video ricevuto e salvato: %s", videoFilename),
			"job_id":          jobID,
			"video_path":      absFinalPath,
			"artifact_id":     artifactID,
			"output_sha256":   outputSHA256,
			"idempotency_key": idempotencyKey,
			"upload_info":     canonicalUploadInfo,
		})
	}
}

// WorkerAssetHandler serves master-staged media assets to remote workers.
type WorkerAssetHandler struct {
	dataDir string
}

func NewWorkerAssetHandler(cfg *config.Config) *WorkerAssetHandler {
	dataDir := ""
	if cfg != nil {
		dataDir = strings.TrimSpace(cfg.DataDir)
		if dataDir == "" {
			dataDir = strings.TrimSpace(cfg.Runtime.DataDir)
		}
	}
	return &WorkerAssetHandler{dataDir: dataDir}
}

func (h *WorkerAssetHandler) ServeVoiceoverAsset() gin.HandlerFunc {
	return h.serveScriptAsset("voiceover")
}

func (h *WorkerAssetHandler) ServeSceneImageAsset() gin.HandlerFunc {
	return h.serveScriptAsset("scene-image")
}

func (h *WorkerAssetHandler) serveScriptAsset(kind string) gin.HandlerFunc {
	return func(c *gin.Context) {
		if h == nil || strings.TrimSpace(h.dataDir) == "" {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "asset storage unavailable"})
			return
		}

		jobID := strings.TrimSpace(c.Param("job_id"))
		filename := strings.TrimSpace(c.Param("filename"))
		if jobID == "" || filename == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "job_id and filename required"})
			return
		}

		filename = filepath.Base(filename)
		if filename == "." || filename == string(filepath.Separator) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid filename"})
			return
		}

		baseDir := filepath.Clean(filepath.Join(h.dataDir, "worker_downloads", "script_assets"))
		filePath := filepath.Join(baseDir, jobID, filename)
		if !strings.HasPrefix(filepath.Clean(filePath), baseDir) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid asset path"})
			return
		}

		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			c.JSON(http.StatusNotFound, gin.H{"error": kind + " asset not found"})
			return
		}

		c.File(filePath)
	}
}
