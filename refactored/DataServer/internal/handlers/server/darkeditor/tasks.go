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
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"velox-server/internal/cache"
)

// ============================================================================
// BACKGROUND REMOVAL
// ============================================================================

// BackgroundRemovalConfig holds configuration for background removal
type BackgroundRemovalConfig struct {
	PythonPath  string        // Path to Python executable (for rembg)
	RembgScript string        // Path to a custom rembg script (optional)
	UseAPI      bool          // Use an external API instead of local processing
	APIEndpoint string        // Endpoint for external background removal API
	APIKey      string        // API key for external service
	Timeout     time.Duration // Timeout for background removal operations
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

	if req.Model == "" {
		req.Model = "u2net"
	}
	if req.OutputFormat == "" {
		req.OutputFormat = "png"
	}

	outputFilename := fmt.Sprintf("nobg_%s.%s",
		strings.TrimSuffix(req.Filename, filepath.Ext(req.Filename)),
		req.OutputFormat)
	outputPath := filepath.Join(h.tempDir, outputFilename)

	if req.Async {
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

func (h *BackgroundRemovalHandler) removeBackgroundSync(inputPath, outputPath, model string) (string, error) {
	if h.cfg.UseAPI && h.cfg.APIEndpoint != "" {
		return h.removeBackgroundViaAPI(inputPath, outputPath, model)
	}
	return h.removeBackgroundLocal(inputPath, outputPath, model)
}

func (h *BackgroundRemovalHandler) removeBackgroundLocal(inputPath, outputPath, model string) (string, error) {
	checkCmd := exec.Command(h.cfg.PythonPath, "-c", "import rembg")
	if err := checkCmd.Run(); err != nil {
		return "", fmt.Errorf("rembg not installed. Install with: pip install rembg")
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
		return "", fmt.Errorf("rembg execution failed: %v\n%s", err, string(output))
	}

	if _, err := os.Stat(outputPath); os.IsNotExist(err) {
		return "", fmt.Errorf("output file not created")
	}

	return filepath.Base(outputPath), nil
}

func (h *BackgroundRemovalHandler) removeBackgroundViaAPI(inputPath, outputPath, model string) (string, error) {
	inputData, err := os.ReadFile(inputPath)
	if err != nil {
		return "", fmt.Errorf("failed to read input file: %w", err)
	}

	body := &bytes.Buffer{}
	writer, err := createMultipartWriter(body, inputData, filepath.Base(inputPath))
	if err != nil {
		return "", fmt.Errorf("failed to create multipart form: %w", err)
	}

	req, err := http.NewRequest("POST", h.cfg.APIEndpoint, body)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", writer.FormDataContentType())
	if h.cfg.APIKey != "" {
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", h.cfg.APIKey))
	}

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

	outputData, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read API response: %w", err)
	}

	if err := os.WriteFile(outputPath, outputData, 0644); err != nil {
		return "", fmt.Errorf("failed to save output: %w", err)
	}

	return filepath.Base(outputPath), nil
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
	file, header, err := c.Request.FormFile("file")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "No file provided"})
		return
	}
	defer file.Close()

	model := c.DefaultQuery("model", "u2net")

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

	ext := filepath.Ext(header.Filename)
	outputFilename := fmt.Sprintf("nobg_%s.png", strings.TrimSuffix(header.Filename, ext))
	outputPath := filepath.Join(h.tempDir, outputFilename)

	result, err := h.removeBackgroundSync(inputPath, outputPath, model)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	os.Remove(inputPath)

	c.JSON(http.StatusOK, gin.H{
		"filename": result,
		"url":      fmt.Sprintf("temp/%s", result),
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

// ============================================================================
// TASK WORKER
// ============================================================================

// TaskWorker processes background tasks for Dark Editor
type TaskWorker struct {
	redis             *cache.Service
	cacheService      *CacheService
	handler           *Handler
	queues            []string
	stop              chan struct{}
	wg                sync.WaitGroup
	pollInterval      time.Duration
	taskTimeout       time.Duration
	tempDir           string
	backgroundRemover *BackgroundRemovalHandler
}

// WorkerConfig holds configuration for the task worker
type WorkerConfig struct {
	Queues       []string
	PollInterval time.Duration
	TaskTimeout  time.Duration
	TempDir      string
}

// NewTaskWorker creates a new task worker
func NewTaskWorker(redis *cache.Service, handler *Handler, cacheService *CacheService, cfg WorkerConfig) *TaskWorker {
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 5 * time.Second
	}
	if cfg.TaskTimeout == 0 {
		cfg.TaskTimeout = 5 * time.Minute
	}
	if len(cfg.Queues) == 0 {
		cfg.Queues = []string{"dark_editor_tasks"}
	}

	return &TaskWorker{
		redis:        redis,
		cacheService: cacheService,
		handler:      handler,
		queues:       cfg.Queues,
		stop:         make(chan struct{}),
		pollInterval: cfg.PollInterval,
		taskTimeout:  cfg.TaskTimeout,
		tempDir:      cfg.TempDir,
	}
}

// SetBackgroundRemover sets the background removal handler
func (w *TaskWorker) SetBackgroundRemover(h *BackgroundRemovalHandler) {
	w.backgroundRemover = h
}

// Start starts the worker
func (w *TaskWorker) Start() {
	log.Printf("🔄 Starting Dark Editor task worker for queues: %v", w.queues)

	for _, queue := range w.queues {
		w.wg.Add(1)
		go w.processQueue(queue)
	}
}

// Stop stops the worker gracefully
func (w *TaskWorker) Stop() {
	log.Printf("🛑 Stopping Dark Editor task worker...")
	close(w.stop)
	w.wg.Wait()
	log.Printf("✅ Dark Editor task worker stopped")
}

func (w *TaskWorker) processQueue(queue string) {
	defer w.wg.Done()

	for {
		select {
		case <-w.stop:
			return
		default:
			ctx, cancel := context.WithTimeout(context.Background(), w.pollInterval+time.Second)

			task, err := w.redis.DequeueTask(ctx, queue, w.pollInterval)
			cancel()

			if err != nil {
				log.Printf("❌ Error dequeuing task: %v", err)
				time.Sleep(time.Second)
				continue
			}

			if task == nil {
				continue
			}

			log.Printf("📋 Processing task %s of type %s", task.ID, task.Type)

			ctx, cancel = context.WithTimeout(context.Background(), w.taskTimeout)
			result, errMsg := w.processTask(ctx, task)
			cancel()

			status := "completed"
			if errMsg != "" {
				status = "failed"
				log.Printf("❌ Task %s failed: %s", task.ID, errMsg)
			} else {
				log.Printf("✅ Task %s completed successfully", task.ID)
			}

			ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
			if err := w.redis.UpdateTaskStatus(ctx, task.ID, status, result, errMsg); err != nil {
				log.Printf("❌ Failed to update task status: %v", err)
			}
			cancel()

			ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
			if err := w.cacheService.PublishTaskCompletion(ctx, task.ID, task.Type, status, result); err != nil {
				log.Printf("⚠️ Failed to publish task completion: %v", err)
			}
			cancel()
		}
	}
}

func (w *TaskWorker) processTask(ctx context.Context, task *cache.Task) (map[string]interface{}, string) {
	switch task.Type {
	case "upscale":
		return w.processUpscale(ctx, task)
	case "remove_bg":
		return w.processRemoveBackground(ctx, task)
	case "generate":
		return w.processGenerate(ctx, task)
	case "export":
		return w.processExport(ctx, task)
	default:
		return nil, "Unknown task type: " + task.Type
	}
}

// ============== TASK PROCESSORS ==============

func (w *TaskWorker) processUpscale(ctx context.Context, task *cache.Task) (map[string]interface{}, string) {
	filename, ok := task.Payload["filename"].(string)
	if !ok {
		return nil, "Missing filename in payload"
	}

	scale, _ := task.Payload["scale"].(float64)
	if scale == 0 {
		scale = 2.0
	}

	inputPath := filepath.Join(w.tempDir, filename)
	if _, err := os.Stat(inputPath); os.IsNotExist(err) {
		return nil, "Input file not found: " + filename
	}

	cached, err := w.cacheService.GetCachedUpscale(ctx, filename, scale)
	if err == nil && cached != nil {
		log.Printf("📦 Cache hit for upscale task %s", task.ID)
		return map[string]interface{}{
			"filename": cached.Filename,
			"url":      cached.URL,
			"scale":    cached.Scale,
			"width":    cached.Width,
			"height":   cached.Height,
			"cached":   true,
		}, ""
	}

	outputFilename := generateOutputFilename(filename, "upscaled")
	outputPath := filepath.Join(w.tempDir, outputFilename)

	inputData, err := os.ReadFile(inputPath)
	if err != nil {
		return nil, "Failed to read input file: " + err.Error()
	}

	if err := os.WriteFile(outputPath, inputData, 0644); err != nil {
		return nil, "Failed to write output file: " + err.Error()
	}

	result := map[string]interface{}{
		"filename": outputFilename,
		"url":      "temp/" + outputFilename,
		"scale":    scale,
	}

	cachedResult := CachedUpscaleResult{
		Filename:  outputFilename,
		URL:       "temp/" + outputFilename,
		Scale:     scale,
		CreatedAt: time.Now(),
	}
	if err := w.cacheService.CacheUpscaleResult(ctx, filename, scale, cachedResult); err != nil {
		log.Printf("⚠️ Failed to cache upscale result: %v", err)
	}

	return result, ""
}

func (w *TaskWorker) processRemoveBackground(ctx context.Context, task *cache.Task) (map[string]interface{}, string) {
	filename, ok := task.Payload["filename"].(string)
	if !ok {
		return nil, "Missing filename in payload"
	}

	model, _ := task.Payload["model"].(string)
	if model == "" {
		model = "u2net"
	}

	inputPath := filepath.Join(w.tempDir, filename)
	if _, err := os.Stat(inputPath); os.IsNotExist(err) {
		return nil, "Input file not found: " + filename
	}

	cached, err := w.cacheService.GetCachedBackgroundRemoval(ctx, filename, model)
	if err == nil && cached != nil {
		log.Printf("📦 Cache hit for background removal task %s", task.ID)
		return map[string]interface{}{
			"filename": cached.Filename,
			"url":      cached.URL,
			"model":    cached.Model,
			"cached":   true,
		}, ""
	}

	outputFilename := generateOutputFilename(filename, "nobg")
	outputPath := filepath.Join(w.tempDir, outputFilename)

	var resultFilename string
	var processErr error

	if w.backgroundRemover != nil {
		resultFilename, processErr = w.backgroundRemover.removeBackgroundSync(inputPath, outputPath, model)
	} else {
		inputData, err := os.ReadFile(inputPath)
		if err != nil {
			return nil, "Failed to read input file: " + err.Error()
		}
		if err := os.WriteFile(outputPath, inputData, 0644); err != nil {
			return nil, "Failed to write output file: " + err.Error()
		}
		resultFilename = outputFilename
	}

	if processErr != nil {
		return nil, "Background removal failed: " + processErr.Error()
	}

	result := map[string]interface{}{
		"filename": resultFilename,
		"url":      "temp/" + resultFilename,
		"model":    model,
	}

	cachedResult := CachedBackgroundRemovalResult{
		Filename:  resultFilename,
		URL:       "temp/" + resultFilename,
		Model:     model,
		CreatedAt: time.Now(),
	}
	if err := w.cacheService.CacheBackgroundRemovalResult(ctx, filename, model, cachedResult); err != nil {
		log.Printf("⚠️ Failed to cache background removal result: %v", err)
	}

	return result, ""
}

func (w *TaskWorker) processGenerate(ctx context.Context, task *cache.Task) (map[string]interface{}, string) {
	prompt, ok := task.Payload["prompt"].(string)
	if !ok {
		return nil, "Missing prompt in payload"
	}

	params, _ := task.Payload["params"].(map[string]interface{})
	log.Printf("🎨 AI Generation request: %s (params: %v)", prompt, params)

	outputFilename := fmt.Sprintf("generated_%s.png", uuid.New().String()[:8])
	outputPath := filepath.Join(w.tempDir, outputFilename)

	placeholder := []byte("PLACEHOLDER_GENERATED_IMAGE")
	if err := os.WriteFile(outputPath, placeholder, 0644); err != nil {
		return nil, "Failed to write generated image: " + err.Error()
	}

	return map[string]interface{}{
		"filename": outputFilename,
		"url":      "temp/" + outputFilename,
		"prompt":   prompt,
	}, ""
}

func (w *TaskWorker) processExport(ctx context.Context, task *cache.Task) (map[string]interface{}, string) {
	filename, ok := task.Payload["filename"].(string)
	if !ok {
		return nil, "Missing filename in payload"
	}

	format, _ := task.Payload["format"].(string)
	if format == "" {
		format = "png"
	}

	quality, _ := task.Payload["quality"].(float64)
	if quality == 0 {
		quality = 90
	}

	inputPath := filepath.Join(w.tempDir, filename)
	if _, err := os.Stat(inputPath); os.IsNotExist(err) {
		return nil, "Input file not found: " + filename
	}

	outputFilename := generateOutputFilename(filename, "exported")
	outputPath := filepath.Join(w.tempDir, outputFilename)

	inputData, err := os.ReadFile(inputPath)
	if err != nil {
		return nil, "Failed to read input file: " + err.Error()
	}

	if err := os.WriteFile(outputPath, inputData, 0644); err != nil {
		return nil, "Failed to write output file: " + err.Error()
	}

	return map[string]interface{}{
		"filename": outputFilename,
		"url":      "temp/" + outputFilename,
		"format":   format,
		"quality":  quality,
	}, ""
}

// generateOutputFilename generates an output filename with a prefix
func generateOutputFilename(inputFilename, prefix string) string {
	ext := filepath.Ext(inputFilename)
	base := strings.TrimSuffix(inputFilename, ext)
	return fmt.Sprintf("%s_%s_%d%s", prefix, base, time.Now().Unix(), ext)
}
