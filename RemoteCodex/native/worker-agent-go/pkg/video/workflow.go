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
)

// VideoGenerationWorkflow orchestrates the complete video generation process.
type VideoGenerationWorkflow struct {
	config           *config.WorkerConfig
	logger           *logger.Logger
	tempFiles        []string
	progressCallback func(percent, scene, total int, stage string)
}

// VideoGenerationInput è un alias per contract.RenderJobParams.
// Manteniamo l'alias per compatibilità verso l'esterno, ma i campi sono
// definiti in shared/contract/contract.go per evitare duplicazione.
type VideoGenerationInput = contract.RenderJobParams

// NewVideoGenerationWorkflow creates a new workflow instance.
func NewVideoGenerationWorkflow(cfg *config.WorkerConfig, log *logger.Logger) *VideoGenerationWorkflow {
	return &VideoGenerationWorkflow{
		config:    cfg,
		logger:    log,
		tempFiles: make([]string, 0),
	}
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

	// Try the new --render path first
	plan := CompileLegacyRenderJobParams("", input, input.OutputPath)
	if plan != nil && len(plan.Timeline) > 0 {
		w.logger.Info("Using new --render path with %d timeline items", len(plan.Timeline))
		if err := w.runRenderPlan(ctx, tempDir, plan); err != nil {
			w.logger.Warn("New --render path failed (%v), falling back to legacy --full-video", err)
			if err2 := w.runNativeCxxEngine(ctx, tempDir, input); err2 != nil {
				return "", err2
			}
		}
	} else {
		w.logger.Info("No renderable timeline, using legacy --full-video path")
		if err := w.runNativeCxxEngine(ctx, tempDir, input); err != nil {
			return "", err
		}
	}

	w.logger.Info("Native C++ video engine completed output at %s", input.OutputPath)

	return input.OutputPath, nil
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
// Usato nel processo di entity association per risultati puramente visivi.
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
