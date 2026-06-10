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
	// PythonPath is the path to Python executable (for rembg)
	PythonPath string
	// RembgScript is the path to a custom rembg script (optional)
	RembgScript string
	// UseAPI indicates whether to use an external API instead of local processing
	UseAPI bool
	// APIEndpoint is the endpoint for external background removal API
	APIEndpoint string
	// APIKey is the API key for external service
	APIKey string
	// Timeout for background removal operations
	Timeout time.Duration
}

// BackgroundRemovalHandler handles background removal operations
type BackgroundRemovalHandler struct {
	cfg     *BackgroundRemovalConfig
	tempDir string
}

// NewBackgroundRemovalHandler creates a new background removal handler
func NewBackgroundRemovalHandler(cfg *BackgroundRemovalConfig, tempDir string) *BackgroundRemovalHandler {
	if cfg.Timeout == 0 {
		cfg.Timeout = 60 * time.Second
	}
	if cfg.PythonPath == "" {
		cfg.PythonPath = "python3"
	}
	return &BackgroundRemovalHandler{
		cfg:     cfg,
		tempDir: tempDir,
	}
}

// Async task storage (in production, use Redis or database)
var backgroundTasks = make(map[string]*BackgroundRemovalStatus)

// RemoveBackground handles background removal requests
func (h *BackgroundRemovalHandler) RemoveBackground(c *gin.Context) {
	var req RemoveBackgroundRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	inputPath := filepath.Join(h.tempDir, req.Filename)
	if _, err := os.Stat(inputPath); os.IsNotExist(err) {
		c.JSON(http.StatusNotFound, gin.H{"error": "Image not found"})
		return
	}

	// Set defaults
	if req.Model == "" {
		req.Model = "u2net" // Default model
	}
	if req.OutputFormat == "" {
		req.OutputFormat = "png"
	}

	// Generate output filename
	outputFilename := fmt.Sprintf("nobg_%s.%s",
		strings.TrimSuffix(req.Filename, filepath.Ext(req.Filename)),
		req.OutputFormat)
	outputPath := filepath.Join(h.tempDir, outputFilename)

	if req.Async {
		// Process asynchronously
		taskID := uuid.New().String()
		backgroundTasks[taskID] = &BackgroundRemovalStatus{
			TaskID:    taskID,
			Status:    "pending",
			StartedAt: time.Now(),
		}

		go h.processBackgroundRemoval(taskID, inputPath, outputPath, req.Model)

		c.JSON(http.StatusAccepted, RemoveBackgroundResponse{
			Processing: true,
			TaskID:     taskID,
		})
		return
	}

	// Process synchronously
	result, err := h.removeBackgroundSync(inputPath, outputPath, req.Model)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, RemoveBackgroundResponse{
		Filename: result,
		URL:      fmt.Sprintf("temp/%s", result),
	})
}

// GetBackgroundRemovalStatus returns the status of an async background removal task
func (h *BackgroundRemovalHandler) GetBackgroundRemovalStatus(c *gin.Context) {
	taskID := c.Param("task_id")

	status, exists := backgroundTasks[taskID]
	if !exists {
		c.JSON(http.StatusNotFound, gin.H{"error": "Task not found"})
		return
	}

	c.JSON(http.StatusOK, status)
}

// processBackgroundRemoval processes background removal asynchronously
func (h *BackgroundRemovalHandler) processBackgroundRemoval(taskID, inputPath, outputPath, model string) {
	status := backgroundTasks[taskID]
	status.Status = "processing"

	result, err := h.removeBackgroundSync(inputPath, outputPath, model)
	if err != nil {
		status.Status = "failed"
		status.Error = err.Error()
		status.EndedAt = time.Now()
		log.Printf("❌ Background removal failed for task %s: %v", taskID, err)
		return
	}

	status.Status = "completed"
	status.Filename = result
	status.URL = fmt.Sprintf("temp/%s", result)
	status.EndedAt = time.Now()
	log.Printf("✅ Background removal completed for task %s", taskID)
}

// removeBackgroundSync removes background synchronously
func (h *BackgroundRemovalHandler) removeBackgroundSync(inputPath, outputPath, model string) (string, error) {
	// Check if using external API
	if h.cfg.UseAPI && h.cfg.APIEndpoint != "" {
		return h.removeBackgroundViaAPI(inputPath, outputPath, model)
	}

	// Use local rembg installation
	return h.removeBackgroundLocal(inputPath, outputPath, model)
}

// removeBackgroundLocal removes background using local rembg installation
func (h *BackgroundRemovalHandler) removeBackgroundLocal(inputPath, outputPath, model string) (string, error) {
	// Check if rembg is installed
	checkCmd := exec.Command(h.cfg.PythonPath, "-c", "import rembg")
	if err := checkCmd.Run(); err != nil {
		return "", fmt.Errorf("rembg not installed. Install with: pip install rembg")
	}

	// Create Python script for rembg
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

	// Execute Python script with timeout
	ctx, cancel := context.WithTimeout(context.Background(), h.cfg.Timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, h.cfg.PythonPath, "-c", script)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("rembg execution failed: %v\n%s", err, string(output))
	}

	// Verify output file was created
	if _, err := os.Stat(outputPath); os.IsNotExist(err) {
		return "", fmt.Errorf("output file not created")
	}

	return filepath.Base(outputPath), nil
}

// removeBackgroundViaAPI removes background using external API
func (h *BackgroundRemovalHandler) removeBackgroundViaAPI(inputPath, outputPath, model string) (string, error) {
	// Read input file
	inputData, err := os.ReadFile(inputPath)
	if err != nil {
		return "", fmt.Errorf("failed to read input file: %w", err)
	}

	// Create multipart form request
	body := &bytes.Buffer{}
	writer, err := createMultipartWriter(body, inputData, filepath.Base(inputPath))
	if err != nil {
		return "", fmt.Errorf("failed to create multipart form: %w", err)
	}

	// Create HTTP request
	req, err := http.NewRequest("POST", h.cfg.APIEndpoint, body)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", writer.FormDataContentType())
	if h.cfg.APIKey != "" {
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", h.cfg.APIKey))
	}

	// Execute request
	client := &http.Client{Timeout: h.cfg.Timeout}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("API request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("API returned status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	// Read response and save to output
	outputData, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read API response: %w", err)
	}

	if err := os.WriteFile(outputPath, outputData, 0644); err != nil {
		return "", fmt.Errorf("failed to save output: %w", err)
	}

	return filepath.Base(outputPath), nil
}

// createMultipartWriter creates a proper multipart form with the given file data
func createMultipartWriter(body *bytes.Buffer, data []byte, filename string) (*multipart.Writer, error) {
	writer := multipart.NewWriter(body)

	// Create a new form file field
	part, err := writer.CreateFormFile("file", filename)
	if err != nil {
		writer.Close()
		return nil, fmt.Errorf("failed to create form file: %w", err)
	}

	// Write the file data
	if _, err := part.Write(data); err != nil {
		writer.Close()
		return nil, fmt.Errorf("failed to write file data: %w", err)
	}

	// Close the writer to finalize the boundary (important!)
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("failed to close multipart writer: %w", err)
	}

	return writer, nil
}

// RemoveBackgroundSimple is a simpler endpoint that accepts file upload directly
func (h *BackgroundRemovalHandler) RemoveBackgroundSimple(c *gin.Context) {
	file, header, err := c.Request.FormFile("file")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "No file provided"})
		return
	}
	defer file.Close()

	// Get model parameter
	model := c.DefaultQuery("model", "u2net")

	// Save uploaded file
	inputFilename := fmt.Sprintf("input_%d_%s", time.Now().Unix(), header.Filename)
	inputPath := filepath.Join(h.tempDir, inputFilename)

	inputData, err := io.ReadAll(file)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to read file"})
		return
	}

	if err := os.WriteFile(inputPath, inputData, 0644); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save file"})
		return
	}

	// Generate output filename
	ext := filepath.Ext(header.Filename)
	outputFilename := fmt.Sprintf("nobg_%s.png", strings.TrimSuffix(header.Filename, ext))
	outputPath := filepath.Join(h.tempDir, outputFilename)

	// Process
	result, err := h.removeBackgroundSync(inputPath, outputPath, model)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Clean up input file
	os.Remove(inputPath)

	c.JSON(http.StatusOK, gin.H{
		"filename": result,
		"url":      fmt.Sprintf("temp/%s", result),
	})
}

// ListModels returns available background removal models
func (h *BackgroundRemovalHandler) ListModels(c *gin.Context) {
	models := []map[string]string{
		{
			"id":          "u2net",
			"name":        "U2Net",
			"description": "General purpose model, good for most images",
		},
		{
			"id":          "u2netp",
			"name":        "U2Net (Lite)",
			"description": "Lighter version, faster but less accurate",
		},
		{
			"id":          "u2net_human_seg",
			"name":        "U2Net Human Segmentation",
			"description": "Optimized for human subjects",
		},
		{
			"id":          "u2net_cloth_seg",
			"name":        "U2Net Cloth Segmentation",
			"description": "Optimized for clothing items",
		},
		{
			"id":          "isnet-general-use",
			"name":        "ISNet General",
			"description": "High quality general purpose model",
		},
		{
			"id":          "silueta",
			"name":        "Silueta",
			"description": "Fast and lightweight model",
		},
	}

	c.JSON(http.StatusOK, models)
}

// HealthCheck checks if background removal service is available
func (h *BackgroundRemovalHandler) HealthCheck(c *gin.Context) {
	// Check if using API
	if h.cfg.UseAPI {
		c.JSON(http.StatusOK, gin.H{
			"status":   "ok",
			"mode":     "api",
			"endpoint": h.cfg.APIEndpoint,
		})
		return
	}

	// Check if rembg is installed locally
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
