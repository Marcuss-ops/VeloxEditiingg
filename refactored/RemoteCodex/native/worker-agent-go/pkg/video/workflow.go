package video

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"velox-worker-agent/pkg/config"
	"velox-worker-agent/pkg/logger"
)

// VideoGenerationWorkflow orchestrates the complete video generation process.
type VideoGenerationWorkflow struct {
	config    *config.WorkerConfig
	logger    *logger.Logger
	tempFiles []string
}

// NewVideoGenerationWorkflow creates a new workflow instance.
func NewVideoGenerationWorkflow(cfg *config.WorkerConfig, log *logger.Logger) *VideoGenerationWorkflow {
	return &VideoGenerationWorkflow{
		config:    cfg,
		logger:    log,
		tempFiles: make([]string, 0),
	}
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
	audioPath string,
	outputPath string,
	scenesJSON string,
	scriptText string,
	startClipPaths []string,
	middleClipPaths []string,
	stockClipSources []string,
	endClipPaths []string,
	backgroundMusicPaths []string,
	backgroundVideoForImgOverlaysPath string,
	associazioniFinaliConTimestamp map[string]interface{},
	formattedImgEntities map[string]interface{},
	preAssociatedEntities map[string]interface{},
	rawEntities map[string]interface{},
	audioLanguageForSRT string,
	segmentsForSRTGeneration []interface{},
	statusCallback func(string, bool)) (string, error) {

	w.logger.Info("Starting video generation process")
	w.logger.Info("Audio path: %s", audioPath)
	w.logger.Info("Output path: %s", outputPath)
	if strings.TrimSpace(scenesJSON) != "" {
		w.logger.Info("Scenes JSON provided (%d bytes)", len(strings.TrimSpace(scenesJSON)))
	}
	defer w.Cleanup()

	// Create temp directory
	tempDir, err := os.MkdirTemp("", "velox_workflow_*")
	if err != nil {
		return "", fmt.Errorf("failed to create temp directory: %w", err)
	}
	w.tempFiles = append(w.tempFiles, tempDir)

	if trimmed := strings.TrimSpace(scenesJSON); trimmed != "" {
		scenesFile := filepath.Join(tempDir, "scenes.json")
		if err := os.WriteFile(scenesFile, []byte(trimmed), 0644); err != nil {
			return "", fmt.Errorf("failed to persist scenes json: %w", err)
		}
		w.tempFiles = append(w.tempFiles, scenesFile)
		w.logger.Info("Scenes JSON staged at %s", scenesFile)
	}

	statusCallback("Starting video processing", false)

	if err := w.runNativeCxxEngine(ctx, tempDir, outputPath, audioPath, scenesJSON, scriptText, audioLanguageForSRT); err != nil {
		return "", err
	}
	w.logger.Info("Native C++ video engine completed output at %s", outputPath)

	return outputPath, nil
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

// EntitaResult represents an entity without text (image-only).
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
