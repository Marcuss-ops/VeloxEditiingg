package workers

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"velox-server/internal/config"
	"velox-server/internal/queue"
)

// ChunkedUploadState tracks the state of a chunked upload
type ChunkedUploadState struct {
	JobID       string    `json:"job_id"`
	WorkerID    string    `json:"worker_id"`
	TotalChunks int       `json:"total_chunks"`
	ChunkSize   int64     `json:"chunk_size"`
	TotalSize   int64     `json:"total_size"`
	Uploaded    []bool    `json:"uploaded"`
	TempDir     string    `json:"temp_dir"`
	Filename    string    `json:"filename"`
	CreatedAt   time.Time `json:"created_at"`
}

// ChunkedUploadManager manages chunked uploads
type ChunkedUploadManager struct {
	mu      sync.RWMutex
	uploads map[string]*ChunkedUploadState // job_id -> state
}

// NewChunkedUploadManager creates a new chunked upload manager
func NewChunkedUploadManager() *ChunkedUploadManager {
	return &ChunkedUploadManager{
		uploads: make(map[string]*ChunkedUploadState),
	}
}

// GetState returns the upload state for a job
func (m *ChunkedUploadManager) GetState(jobID string) *ChunkedUploadState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.uploads[jobID]
}

// SetState sets the upload state for a job
func (m *ChunkedUploadManager) SetState(jobID string, state *ChunkedUploadState) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.uploads[jobID] = state
}

// RemoveState removes the upload state for a job
func (m *ChunkedUploadManager) RemoveState(jobID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.uploads, jobID)
}

// MarkChunk marks a chunk as uploaded
func (m *ChunkedUploadManager) MarkChunk(jobID string, chunkIndex int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if state, ok := m.uploads[jobID]; ok {
		if chunkIndex >= 0 && chunkIndex < len(state.Uploaded) {
			state.Uploaded[chunkIndex] = true
		}
	}
}

// IsComplete checks if all chunks are uploaded
func (m *ChunkedUploadManager) IsComplete(jobID string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	state, ok := m.uploads[jobID]
	if !ok {
		return false
	}
	for _, uploaded := range state.Uploaded {
		if !uploaded {
			return false
		}
	}
	return true
}

// globalChunkedUploadManager is the global instance
var globalChunkedUploadManager = NewChunkedUploadManager()

// InitChunkedUpload initializes a chunked upload session
func InitChunkedUpload() gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			JobID       string `json:"job_id" binding:"required"`
			WorkerID    string `json:"worker_id"`
			Filename    string `json:"filename"`
			TotalChunks int    `json:"total_chunks" binding:"required"`
			ChunkSize   int64  `json:"chunk_size"`
			TotalSize   int64  `json:"total_size"`
		}
		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "invalid request"})
			return
		}

		// Check for existing incomplete upload
		existing := globalChunkedUploadManager.GetState(body.JobID)
		if existing != nil {
			// Resume existing upload
			c.JSON(http.StatusOK, gin.H{
				"ok":           true,
				"job_id":       body.JobID,
				"uploaded":     existing.Uploaded,
				"total_chunks": existing.TotalChunks,
				"resuming":     true,
			})
			return
		}

		// Create new upload state
		state := &ChunkedUploadState{
			JobID:       body.JobID,
			WorkerID:    body.WorkerID,
			TotalChunks: body.TotalChunks,
			ChunkSize:   body.ChunkSize,
			TotalSize:   body.TotalSize,
			Uploaded:    make([]bool, body.TotalChunks),
			Filename:    body.Filename,
			CreatedAt:   time.Now(),
		}

		// Create temp directory for chunks
		tempDir, err := os.MkdirTemp("", "velox_chunked_upload_*")
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "failed to create temp dir"})
			return
		}
		state.TempDir = tempDir

		globalChunkedUploadManager.SetState(body.JobID, state)

		c.JSON(http.StatusOK, gin.H{
			"ok":           true,
			"job_id":       body.JobID,
			"total_chunks": body.TotalChunks,
			"resuming":     false,
		})
	}
}

// UploadChunk handles a single chunk upload
func UploadChunk(cfg *config.Config) gin.HandlerFunc {
	return func(c *gin.Context) {
		jobID := c.Param("job_id")
		chunkIndexStr := c.Param("chunk_index")

		chunkIndex, err := strconv.Atoi(chunkIndexStr)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "invalid chunk_index"})
			return
		}

		state := globalChunkedUploadManager.GetState(jobID)
		if state == nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "upload session not initialized"})
			return
		}

		if chunkIndex < 0 || chunkIndex >= state.TotalChunks {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "chunk_index out of range"})
			return
		}

		// Check if chunk already uploaded (idempotent)
		if state.Uploaded[chunkIndex] {
			c.JSON(http.StatusOK, gin.H{"ok": true, "chunk": chunkIndex, "already_uploaded": true})
			return
		}

		// Get the chunk file
		file, _, err := c.Request.FormFile("chunk")
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "chunk file required"})
			return
		}
		defer file.Close()

		// Write chunk to temp file
		chunkPath := filepath.Join(state.TempDir, fmt.Sprintf("chunk_%04d", chunkIndex))
		out, err := os.Create(chunkPath)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "failed to write chunk"})
			return
		}

		written, err := io.Copy(out, file)
		out.Close()
		if err != nil {
			os.Remove(chunkPath)
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "failed to save chunk"})
			return
		}

		globalChunkedUploadManager.MarkChunk(jobID, chunkIndex)

		// Check if all chunks are complete
		complete := globalChunkedUploadManager.IsComplete(jobID)

		c.JSON(http.StatusOK, gin.H{
			"ok":           true,
			"chunk":        chunkIndex,
			"bytes":        written,
			"complete":     complete,
			"uploaded":     countUploaded(state),
			"total_chunks": state.TotalChunks,
		})
	}
}

// CompleteChunkedUpload assembles all chunks into the final file
func CompleteChunkedUpload(cfg *config.Config, fileQ *queue.FileQueue) gin.HandlerFunc {
	videosDir := cfg.VideosDir
	if videosDir == "" {
		videosDir = "./completed_videos"
	}

	return func(c *gin.Context) {
		jobID := c.Param("job_id")

		state := globalChunkedUploadManager.GetState(jobID)
		if state == nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "upload session not found"})
			return
		}

		if !globalChunkedUploadManager.IsComplete(jobID) {
			c.JSON(http.StatusBadRequest, gin.H{
				"ok":       false,
				"error":    "not all chunks uploaded",
				"uploaded": countUploaded(state),
				"total":    state.TotalChunks,
			})
			return
		}

		// Ensure videos directory exists
		if err := os.MkdirAll(videosDir, 0755); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "failed to create videos directory"})
			return
		}

		// Assemble chunks into final file
		ext := ".mp4"
		if state.Filename != "" {
			ext = filepath.Ext(state.Filename)
			if ext == "" {
				ext = ".mp4"
			}
		}

		finalPath := filepath.Join(videosDir, fmt.Sprintf("%s%s", jobID, ext))
		out, err := os.Create(finalPath)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "failed to create output file"})
			return
		}

		// Concatenate chunks in order
		for i := 0; i < state.TotalChunks; i++ {
			chunkPath := filepath.Join(state.TempDir, fmt.Sprintf("chunk_%04d", i))
			f, err := os.Open(chunkPath)
			if err != nil {
				out.Close()
				os.Remove(finalPath)
				c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": fmt.Sprintf("failed to open chunk %d", i)})
				return
			}
			_, err = io.Copy(out, f)
			f.Close()
			if err != nil {
				out.Close()
				os.Remove(finalPath)
				c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": fmt.Sprintf("failed to read chunk %d", i)})
				return
			}
		}
		out.Close()

		// Cleanup temp files
		os.RemoveAll(state.TempDir)
		globalChunkedUploadManager.RemoveState(jobID)

		c.JSON(http.StatusOK, gin.H{
			"ok":           true,
			"job_id":       jobID,
			"video_path":   finalPath,
			"total_chunks": state.TotalChunks,
		})
	}
}

// countUploaded counts the number of uploaded chunks
func countUploaded(state *ChunkedUploadState) int {
	count := 0
	for _, uploaded := range state.Uploaded {
		if uploaded {
			count++
		}
	}
	return count
}
