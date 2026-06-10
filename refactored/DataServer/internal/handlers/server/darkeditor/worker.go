package darkeditor

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"velox-server/internal/cache"
)

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

			// Process with timeout
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

			// Update task status
			ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
			if err := w.redis.UpdateTaskStatus(ctx, task.ID, status, result, errMsg); err != nil {
				log.Printf("❌ Failed to update task status: %v", err)
			}
			cancel()

			// Publish completion notification
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

// ============== UPSCALE PROCESSING ==============

func (w *TaskWorker) processUpscale(ctx context.Context, task *cache.Task) (map[string]interface{}, string) {
	filename, ok := task.Payload["filename"].(string)
	if !ok {
		return nil, "Missing filename in payload"
	}

	scale, _ := task.Payload["scale"].(float64)
	if scale == 0 {
		scale = 2.0 // Default 2x upscale
	}

	inputPath := filepath.Join(w.tempDir, filename)
	if _, err := os.Stat(inputPath); os.IsNotExist(err) {
		return nil, "Input file not found: " + filename
	}

	// Check cache first
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

	// Process upscale
	outputFilename := generateOutputFilename(filename, "upscaled")
	outputPath := filepath.Join(w.tempDir, outputFilename)

	// For now, we'll just copy the file as a placeholder
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

	// Cache the result
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

// ============== BACKGROUND REMOVAL PROCESSING ==============

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

	// Check cache first
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

	// Process background removal
	outputFilename := generateOutputFilename(filename, "nobg")
	outputPath := filepath.Join(w.tempDir, outputFilename)

	var resultFilename string
	var processErr error

	if w.backgroundRemover != nil {
		// Use the background removal handler
		resultFilename, processErr = w.backgroundRemover.removeBackgroundSync(inputPath, outputPath, model)
	} else {
		// Fallback: just copy the file (placeholder)
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

	// Cache the result
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

// ============== AI GENERATION PROCESSING ==============

func (w *TaskWorker) processGenerate(ctx context.Context, task *cache.Task) (map[string]interface{}, string) {
	prompt, ok := task.Payload["prompt"].(string)
	if !ok {
		return nil, "Missing prompt in payload"
	}

	params, _ := task.Payload["params"].(map[string]interface{})

	// For now, return a placeholder

	log.Printf("🎨 AI Generation request: %s (params: %v)", prompt, params)

	// Generate a unique filename
	outputFilename := fmt.Sprintf("generated_%s.png", uuid.New().String()[:8])
	outputPath := filepath.Join(w.tempDir, outputFilename)

	// Create a placeholder file
	// In production, this would be the actual generated image
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

// ============== EXPORT PROCESSING ==============

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

	// For now, copy as placeholder

	outputFilename := generateOutputFilename(filename, "exported")
	outputPath := filepath.Join(w.tempDir, outputFilename)

	// Copy as placeholder
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

// ============== HELPER FUNCTIONS ==============

func generateOutputFilename(inputFilename, prefix string) string {
	ext := filepath.Ext(inputFilename)
	base := strings.TrimSuffix(inputFilename, ext)
	return fmt.Sprintf("%s_%s_%d%s", prefix, base, time.Now().Unix(), ext)
}
