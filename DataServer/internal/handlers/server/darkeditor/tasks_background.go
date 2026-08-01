package darkeditor

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// BackgroundRemovalConfig holds configuration for background removal
type BackgroundRemovalConfig struct {
	PythonPath  string
	RembgScript string
	UseAPI      bool
	APIEndpoint string
	APIKey      string
	Timeout     time.Duration

	// TaskTTL bounds how long asynchronous task statuses remain available.
	// MaxTasks bounds in-memory status retention. CleanupInterval controls
	// periodic expired-task cleanup; zero uses the production default.
	TaskTTL         time.Duration
	MaxTasks        int
	CleanupInterval time.Duration
}

// BackgroundRemovalHandler handles background removal operations
type BackgroundRemovalHandler struct {
	cfg      *BackgroundRemovalConfig
	tempDir  string
	taskRepo *BackgroundTaskRepository
}

// NewBackgroundRemovalHandler creates a new background removal handler
func NewBackgroundRemovalHandler(cfg *BackgroundRemovalConfig, tempDir string) *BackgroundRemovalHandler {
	if cfg == nil {
		cfg = &BackgroundRemovalConfig{}
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 60 * time.Second
	}
	if cfg.PythonPath == "" {
		cfg.PythonPath = "python3"
	}
	cleanupInterval := cfg.CleanupInterval
	if cleanupInterval <= 0 {
		cleanupInterval = defaultBackgroundTaskCleanupInterval
	}
	return &BackgroundRemovalHandler{
		cfg:      cfg,
		tempDir:  tempDir,
		taskRepo: newBackgroundTaskRepository(cfg.TaskTTL, cfg.MaxTasks, cleanupInterval, time.Now),
	}
}

// Close stops the background-task cleanup loop. Handlers that are replaced
// during tests or shutdown should call Close to release the goroutine.
func (h *BackgroundRemovalHandler) Close() {
	if h != nil && h.taskRepo != nil {
		h.taskRepo.Close()
	}
}

// RemoveBackground handles background removal requests
func (h *BackgroundRemovalHandler) RemoveBackground(c *gin.Context) {
	var req RemoveBackgroundRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	inputData, pathErr := confinedReadFile(h.tempDir, req.Filename)
	if pathErr != nil {
		if os.IsNotExist(pathErr) {
			c.JSON(http.StatusNotFound, gin.H{"error": "Image not found"})
		} else {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid filename"})
		}
		return
	}

	if req.Model == "" {
		req.Model = "u2net"
	}
	if req.OutputFormat == "" {
		req.OutputFormat = "png"
	}

	outputFilename := fmt.Sprintf("nobg_%d_%s.%s",
		time.Now().UnixNano(), uuid.New().String(), req.OutputFormat)
	if _, err := validatePathComponent(outputFilename); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid output filename"})
		return
	}

	if req.Async {
		taskID := uuid.New().String()
		h.taskRepo.Set(BackgroundRemovalStatus{
			TaskID:    taskID,
			Status:    "pending",
			StartedAt: time.Now(),
		})

		go h.processBackgroundRemoval(taskID, inputData, outputFilename, req.Model)

		c.JSON(http.StatusAccepted, RemoveBackgroundResponse{
			Processing: true,
			TaskID:     taskID,
		})
		return
	}

	outputData, err := h.removeBackgroundSync(inputData, req.Model)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if err := confinedWriteFile(h.tempDir, outputFilename, outputData, 0644); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save output"})
		return
	}

	c.JSON(http.StatusOK, RemoveBackgroundResponse{
		Filename: outputFilename,
		URL:      fmt.Sprintf("temp/%s", outputFilename),
	})
}

// GetBackgroundRemovalStatus returns the status of an async background removal task
func (h *BackgroundRemovalHandler) GetBackgroundRemovalStatus(c *gin.Context) {
	taskID := c.Param("task_id")

	status, exists := h.taskRepo.Get(taskID)
	if !exists {
		c.JSON(http.StatusNotFound, gin.H{"error": "Task not found"})
		return
	}

	c.JSON(http.StatusOK, status)
}

func (h *BackgroundRemovalHandler) processBackgroundRemoval(taskID string, inputData []byte, outputFilename, model string) {
	h.taskRepo.Update(taskID, func(status *BackgroundRemovalStatus) {
		status.Status = "processing"
	})

	outputData, err := h.removeBackgroundSync(inputData, model)
	if err != nil {
		h.taskRepo.Update(taskID, func(status *BackgroundRemovalStatus) {
			status.Status = "failed"
			status.Error = err.Error()
			status.EndedAt = time.Now()
		})
		log.Printf("[ERROR] Background removal failed for task %s: %v", taskID, err)
		return
	}

	if err := confinedWriteFile(h.tempDir, outputFilename, outputData, 0644); err != nil {
		h.taskRepo.Update(taskID, func(status *BackgroundRemovalStatus) {
			status.Status = "failed"
			status.Error = err.Error()
			status.EndedAt = time.Now()
		})
		return
	}
	h.taskRepo.Update(taskID, func(status *BackgroundRemovalStatus) {
		status.Status = "completed"
		status.Filename = outputFilename
		status.URL = fmt.Sprintf("temp/%s", outputFilename)
		status.EndedAt = time.Now()
	})
	log.Printf("[OK] Background removal completed for task %s", taskID)
}

func (h *BackgroundRemovalHandler) removeBackgroundSync(inputData []byte, model string) ([]byte, error) {
	if h.cfg.UseAPI && h.cfg.APIEndpoint != "" {
		return h.removeBackgroundViaAPI(inputData, model)
	}
	return h.removeBackgroundLocal(inputData, model)
}

func (h *BackgroundRemovalHandler) removeBackgroundLocal(inputData []byte, model string) ([]byte, error) {
	checkCmd := exec.Command(h.cfg.PythonPath, "-c", "import rembg")
	if err := checkCmd.Run(); err != nil {
		return nil, fmt.Errorf("rembg not installed. Install with: pip install rembg")
	}

	privateDir, err := os.MkdirTemp("", "darkeditor-rembg-")
	if err != nil {
		return nil, fmt.Errorf("failed to create processing workspace: %w", err)
	}
	defer os.RemoveAll(privateDir)
	inputPath := filepath.Join(privateDir, "input.bin")
	outputPath := filepath.Join(privateDir, "output.bin")
	if err := os.WriteFile(inputPath, inputData, 0600); err != nil {
		return nil, fmt.Errorf("failed to prepare processing input: %w", err)
	}

	script := fmt.Sprintf(`
import rembg
from PIL import Image

input_path = %q
output_path = %q
model = %q

with open(input_path, 'rb') as f:
    input_data = f.read()

output_data = rembg.remove(input_data, model_name=model)

with open(output_path, 'wb') as f:
    f.write(output_data)

print("Background removed successfully")
`, inputPath, outputPath, model)

	ctx, cancel := context.WithTimeout(context.Background(), h.cfg.Timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, h.cfg.PythonPath, "-c", script)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("rembg execution failed: %v\n%s", err, string(output))
	}

	outputData, err := os.ReadFile(outputPath)
	if err != nil {
		return nil, fmt.Errorf("output file not created: %w", err)
	}
	return outputData, nil
}

func (h *BackgroundRemovalHandler) removeBackgroundViaAPI(inputData []byte, model string) ([]byte, error) {
	body := &bytes.Buffer{}
	writer, err := createMultipartWriter(body, inputData, "input.png")
	if err != nil {
		return nil, fmt.Errorf("failed to create multipart form: %w", err)
	}

	req, err := http.NewRequest("POST", h.cfg.APIEndpoint, body)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", writer.FormDataContentType())
	if h.cfg.APIKey != "" {
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", h.cfg.APIKey))
	}

	client := &http.Client{Timeout: h.cfg.Timeout}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("API request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API returned status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	outputData, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read API response: %w", err)
	}

	return outputData, nil
}

func createMultipartWriter(body *bytes.Buffer, data []byte, filename string) (*multipart.Writer, error) {
	writer := multipart.NewWriter(body)

	part, err := writer.CreateFormFile("file", filename)
	if err != nil {
		writer.Close()
		return nil, fmt.Errorf("failed to create form file: %w", err)
	}

	if _, err := part.Write(data); err != nil {
		writer.Close()
		return nil, fmt.Errorf("failed to write file data: %w", err)
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("failed to close multipart writer: %w", err)
	}

	return writer, nil
}

// RemoveBackgroundSimple accepts file upload directly for background removal
func (h *BackgroundRemovalHandler) RemoveBackgroundSimple(c *gin.Context) {
	file, _, err := c.Request.FormFile("file")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "No file provided"})
		return
	}
	defer file.Close()

	model := c.DefaultQuery("model", "u2net")

	// Never embed the multipart filename in a filesystem path. Multipart
	// filenames are attacker-controlled and may contain traversal syntax.
	outputFilename := fmt.Sprintf("nobg_%d_%s.png", time.Now().UnixNano(), uuid.New().String())
	if _, err := validatePathComponent(outputFilename); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Invalid output filename"})
		return
	}

	inputData, err := io.ReadAll(file)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to read file"})
		return
	}

	outputData, err := h.removeBackgroundSync(inputData, model)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if err := confinedWriteFile(h.tempDir, outputFilename, outputData, 0644); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save output"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"filename": outputFilename,
		"url":      fmt.Sprintf("temp/%s", outputFilename),
	})
}

// ListModels returns available background removal models
func (h *BackgroundRemovalHandler) ListModels(c *gin.Context) {
	models := []map[string]string{
		{"id": "u2net", "name": "U2Net", "description": "General purpose model, good for most images"},
		{"id": "u2netp", "name": "U2Net (Lite)", "description": "Lighter version, faster but less accurate"},
		{"id": "u2net_human_seg", "name": "U2Net Human Segmentation", "description": "Optimized for human subjects"},
		{"id": "u2net_cloth_seg", "name": "U2Net Cloth Segmentation", "description": "Optimized for clothing items"},
		{"id": "isnet-general-use", "name": "ISNet General", "description": "High quality general purpose model"},
		{"id": "silueta", "name": "Silueta", "description": "Fast and lightweight model"},
	}

	c.JSON(http.StatusOK, models)
}

// HealthCheck checks if background removal service is available
func (h *BackgroundRemovalHandler) HealthCheck(c *gin.Context) {
	if h.cfg.UseAPI {
		c.JSON(http.StatusOK, gin.H{"status": "ok", "mode": "api", "endpoint": h.cfg.APIEndpoint})
		return
	}

	cmd := exec.Command(h.cfg.PythonPath, "-c", "import rembg; print(rembg.__version__)")
	output, err := cmd.Output()

	if err != nil {
		c.JSON(http.StatusOK, gin.H{
			"status":  "unavailable",
			"mode":    "local",
			"error":   "rembg not installed",
			"install": "pip install rembg[gpu]",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":  "ok",
		"mode":    "local",
		"version": strings.TrimSpace(string(output)),
	})
}
