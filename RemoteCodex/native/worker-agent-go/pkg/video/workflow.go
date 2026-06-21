package video

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"velox-shared/contract"
	"velox-worker-agent/pkg/config"
	"velox-worker-agent/pkg/logger"
	"velox-worker-agent/pkg/video/pipeline"
)

// VideoGenerationWorkflow orchestrates the complete video generation process.
type VideoGenerationWorkflow struct {
	config           *config.WorkerConfig
	logger           *logger.Logger
	tempFiles        []string
	progressCallback func(percent, scene, total int, stage string)
	runner           *pipeline.Runner
}

// VideoGenerationInput è un alias per contract.RenderJobParams.
type VideoGenerationInput = contract.RenderJobParams

// NewVideoGenerationWorkflow creates a new workflow instance.
//
// PR-3.9: the pipeline + render-client wiring lives in
// video.NewPipelineRunner so the SceneComposite adapter (composition
// root) and the legacy workflow share the same construction path.
func NewVideoGenerationWorkflow(cfg *config.WorkerConfig, log *logger.Logger) *VideoGenerationWorkflow {
	w := &VideoGenerationWorkflow{
		config:    cfg,
		logger:    log,
		tempFiles: make([]string, 0),
	}

	runner, err := NewPipelineRunner(log)
	if err != nil {
		log.Warn("Pipeline runner unavailable: %v (legacy fallback only)", err)
	} else {
		w.runner = runner
	}

	return w
}

// SetProgressCallback sets a callback for progress updates during video generation.
func (w *VideoGenerationWorkflow) SetProgressCallback(fn func(percent, scene, total int, stage string)) {
	w.progressCallback = fn
}

// Cleanup removes temporary files and resources.
func (w *VideoGenerationWorkflow) Cleanup() {
	w.logger.Info("Cleaning up workflow resources")
	for _, tempFile := range w.tempFiles {
		if err := os.RemoveAll(tempFile); err != nil {
			w.logger.Warn("Failed to remove temp file %s: %v", tempFile, err)
		}
	}
	w.tempFiles = nil
}

// ProcessSingleVideo processes a single video using the provided parameters.
func (w *VideoGenerationWorkflow) ProcessSingleVideo(ctx context.Context,
	input contract.RenderJobParams,
	statusCallback func(string, bool)) (string, error) {

	w.logger.Info("Starting video generation process")
	w.logger.Info("Audio path: %s", input.AudioPath)
	w.logger.Info("Output path: %s", input.OutputPath)
	if strings.TrimSpace(input.ScenesJSON) != "" {
		w.logger.Info("Scenes JSON provided (%d bytes)", len(strings.TrimSpace(input.ScenesJSON)))
	}
	if strings.TrimSpace(input.VideoMode) != "" {
		w.logger.Info("Video mode: %s", strings.TrimSpace(input.VideoMode))
	}
	if strings.TrimSpace(input.DriveOutputFolder) != "" {
		w.logger.Info("Drive output folder hint: %s", strings.TrimSpace(input.DriveOutputFolder))
	}
	defer w.Cleanup()

	// Create temp directory
	tempDir, err := os.MkdirTemp("", "velox_workflow_*")
	if err != nil {
		return "", fmt.Errorf("failed to create temp directory: %w", err)
	}
	w.tempFiles = append(w.tempFiles, tempDir)

	if trimmed := strings.TrimSpace(input.ScenesJSON); trimmed != "" {
		scenesFile := filepath.Join(tempDir, "scenes.json")
		if err := os.WriteFile(scenesFile, []byte(trimmed), 0644); err != nil {
			return "", fmt.Errorf("failed to persist scenes json: %w", err)
		}
		w.tempFiles = append(w.tempFiles, scenesFile)
		w.logger.Info("Scenes JSON staged at %s", scenesFile)
	}

	statusCallback("Starting video processing", false)

	// Try new --render path first
	p := CompileLegacyRenderJobParams("", input, input.OutputPath)
	if p != nil && len(p.Timeline) > 0 {
		w.logger.Info("Using new --render path with %d timeline items", len(p.Timeline))
		if renderErr := w.runRenderPlan(ctx, tempDir, p); renderErr != nil {
			w.logger.Warn("New --render path failed (%v), falling back to legacy --full-video", renderErr)
			if legacyErr := w.runNativeCxxEngine(ctx, tempDir, input); legacyErr != nil {
				return "", legacyErr
			}
		}
	} else {
		w.logger.Info("No renderable timeline, using legacy --full-video path")
		if legacyErr := w.runNativeCxxEngine(ctx, tempDir, input); legacyErr != nil {
			return "", legacyErr
		}
	}

	w.logger.Info("Native C++ video engine completed output at %s", input.OutputPath)

	return input.OutputPath, nil
}

// RunPipeline executes a specific pipeline by ID with the given parameters.
// This is the entry point for new endpoints that know their pipeline ID.
func (w *VideoGenerationWorkflow) RunPipeline(ctx context.Context, pipelineID string, jobID string, input map[string]interface{}, outputPath string) error {
	if w.runner == nil {
		return fmt.Errorf("pipeline runner not available")
	}
	return w.runner.Run(ctx, pipelineID, jobID, input, outputPath)
}

// TranscriptionSegment represents a single segment from audio transcription.
type TranscriptionSegment struct {
	Text  string  `json:"text"`
	Start float64 `json:"start"`
	End   float64 `json:"end"`
}

// MatchResult represents a single fuzzy match with timestamp.
type MatchResult struct {
	TimestampStart float64 `json:"timestamp_start"`
	TimestampEnd   float64 `json:"timestamp_end"`
	Score          float64 `json:"score"`
	Method         string  `json:"method"`
	Text           string  `json:"text"`
}

// EntitaResult represents an entity without text (image-only association result).
type EntitaResult struct {
	LinkImmagine []string      `json:"Link immagine"`
	Timestamps   []MatchResult `json:"Timestamps"`
}

// parseTranscriptionSegments parses the pre-transcribed segments from the input.
func parseTranscriptionSegments(raw []interface{}) []TranscriptionSegment {
	segments := make([]TranscriptionSegment, 0, len(raw))
	for _, item := range raw {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		text, _ := m["text"].(string)
		start, _ := toFloat64(m["start"])
		end, _ := toFloat64(m["end"])
		text = strings.TrimSpace(text)
		if text == "" {
			continue
		}
		segments = append(segments, TranscriptionSegment{
			Text:  text,
			Start: start,
			End:   end,
		})
	}
	return segments
}
