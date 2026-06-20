package video

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"velox-shared/contract"
)

// CppRenderPlan matches the C++ RenderPlan V1 model (render_plan.hpp).
type CppRenderPlan struct {
	Version     int            `json:"version"`
	JobID       string         `json:"job_id"`
	Canvas      CanvasSpec     `json:"canvas"`
	Timeline    []TimelineItem `json:"timeline"`
	AudioTracks []AudioTrack   `json:"audio_tracks"`
	OutputPath  string         `json:"output_path"`
}

// CanvasSpec matches C++ CanvasSpec.
type CanvasSpec struct {
	Width  int `json:"width"`
	Height int `json:"height"`
	Fps    int `json:"fps"`
}

// MediaSource is the C++ MediaSource variant.
type MediaSource struct {
	Type     string `json:"type"`
	URL      string `json:"url,omitempty"`
	CacheKey string `json:"cache_key,omitempty"`
	ColorHex string `json:"color_hex,omitempty"`
}

// TransformSpec matches C++ TransformSpec.
type TransformSpec struct {
	ScaleMode string `json:"scale_mode,omitempty"`
	SlowZoom  *bool  `json:"slow_zoom,omitempty"`
}

// TimelineItem matches C++ TimelineItem.
type TimelineItem struct {
	Source          MediaSource    `json:"source"`
	DurationSeconds float64        `json:"duration_seconds"`
	Transform       *TransformSpec `json:"transform,omitempty"`
}

// AudioTrack matches C++ AudioTrack.
type AudioTrack struct {
	SourceURL       string  `json:"source_url"`
	Volume          float64 `json:"volume,omitempty"`
	StartTimeOffset float64 `json:"start_time_offset,omitempty"`
}

// CompileLegacyRenderJobParams converts a legacy VideoEngineRequest into a CppRenderPlan.
// This adapter exists only to migrate existing endpoints to the RenderPlan path.
// New endpoints should implement PipelineCompiler instead.
// Returns nil if the input cannot be meaningfully compiled (e.g. no scenes/clips).
func CompileLegacyRenderJobParams(jobID string, input contract.RenderJobParams, outputPath string) *CppRenderPlan {
	if jobID == "" {
		b := make([]byte, 8)
		rand.Read(b)
		jobID = "plan_" + hex.EncodeToString(b)
	}

	videoMode := strings.TrimSpace(input.VideoMode)
	if videoMode == "" && (len(input.IntroClipPaths) > 0 || len(input.StockClipPaths) > 0 || len(input.ClipSegments) > 0) {
		videoMode = "clip_stock"
	}

	scenes := contract.ParseScenes(input.ScenesJSON)
	clips := contract.ParseClips(input.ClipSegments)

	plan := &CppRenderPlan{
		Version:    1,
		JobID:      jobID,
		OutputPath: outputPath,
		Canvas: CanvasSpec{
			Width:  1920,
			Height: 1080,
			Fps:    30,
		},
	}

	// Build timeline from scenes or clips
	if videoMode == "clip_stock" || len(clips) > 0 || len(input.IntroClipPaths) > 0 || len(input.StockClipPaths) > 0 {
		// Clip mode
		for _, path := range input.IntroClipPaths {
			plan.Timeline = append(plan.Timeline, TimelineItem{
				Source:          MediaSource{Type: "video", URL: path},
				DurationSeconds: 4.0,
				Transform:       &TransformSpec{ScaleMode: "contain"},
			})
		}
		for _, clip := range clips {
			url := clip.ClipLink
			if url == "" && len(clip.ClipLinks) > 0 {
				url = clip.ClipLinks[0]
			}
			dur := clip.DurationSeconds
			if dur <= 0 {
				dur = 4.0
			}
			if url != "" {
				plan.Timeline = append(plan.Timeline, TimelineItem{
					Source:          MediaSource{Type: "video", URL: url},
					DurationSeconds: dur,
					Transform:       &TransformSpec{ScaleMode: "contain"},
				})
			}
		}
		for _, path := range input.StockClipPaths {
			plan.Timeline = append(plan.Timeline, TimelineItem{
				Source:          MediaSource{Type: "video", URL: path},
				DurationSeconds: 5.0,
				Transform:       &TransformSpec{ScaleMode: "contain"},
			})
		}
	} else {
		// Scene image mode
		imagePaths := input.SceneImagePaths
		if len(imagePaths) == 0 {
			for _, s := range scenes {
				if s.ImageLink != "" {
					imagePaths = append(imagePaths, s.ImageLink)
				} else if len(s.ImageLinks) > 0 {
					imagePaths = append(imagePaths, s.ImageLinks[0])
				}
			}
		}

		// Calculate per-scene duration from voiceover
		voiceoverDuration := 0.0
		if strings.TrimSpace(input.AudioPath) != "" {
			voiceoverDuration = detectAudioDuration(strings.TrimSpace(input.AudioPath))
		}

		// Sum explicit scene durations for scenes that don't have one
		explicitScenes := 0
		explicitTotal := 0.0
		for _, s := range scenes {
			if s.DurationSeconds > 0 {
				explicitScenes++
				explicitTotal += s.DurationSeconds
			}
		}

		// Per-scene duration: distribute voiceoverDuration only among scenes without explicit duration
		unsetScenes := len(imagePaths) - explicitScenes
		if unsetScenes < 0 {
			unsetScenes = 0
		}
		unsetDuration := voiceoverDuration - explicitTotal
		if unsetDuration < 0 {
			unsetDuration = 0
		}
		perSceneDuration := 0.0
		if unsetScenes > 0 && unsetDuration > 0 {
			perSceneDuration = unsetDuration / float64(unsetScenes)
		}

		for i, imgPath := range imagePaths {
			dur := 0.0
			// Priority 1: explicit scene duration
			if i < len(scenes) && scenes[i].DurationSeconds > 0 {
				dur = scenes[i].DurationSeconds
			}
			// Priority 2: distributed voiceover duration
			if dur <= 0 && perSceneDuration > 0 {
				dur = perSceneDuration
			}
			// Priority 3: fallback
			if dur <= 0 {
				dur = 5.0
			}
			plan.Timeline = append(plan.Timeline, TimelineItem{
				Source:          MediaSource{Type: "image", URL: imgPath},
				DurationSeconds: dur,
				Transform:       &TransformSpec{ScaleMode: "cover", SlowZoom: boolPtr(true)},
			})
		}
	}

	if len(plan.Timeline) == 0 {
		return nil
	}

	// Add audio tracks
	if strings.TrimSpace(input.AudioPath) != "" {
		plan.AudioTracks = append(plan.AudioTracks, AudioTrack{
			SourceURL: strings.TrimSpace(input.AudioPath),
			Volume:    1.0,
		})
	}

	return plan
}

func boolPtr(b bool) *bool { return &b }

func detectAudioDuration(path string) float64 {
	out, err := exec.Command("ffprobe", "-v", "error", "-show_entries", "format=duration",
		"-of", "default=noprint_wrappers=1:nokey=1", path).Output()
	if err != nil {
		return 0
	}
	var dur float64
	fmt.Sscanf(strings.TrimSpace(string(out)), "%f", &dur)
	return dur
}

// runRenderPlan writes a CppRenderPlan to disk and launches the C++ engine
// with --render --plan. The progress callback receives percent updates.
func (w *VideoGenerationWorkflow) runRenderPlan(
	ctx context.Context,
	tempDir string,
	plan *CppRenderPlan,
) error {
	planPath := filepath.Join(tempDir, "render_plan.json")
	data, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal render plan: %w", err)
	}
	if err := os.WriteFile(planPath, data, 0o644); err != nil {
		return fmt.Errorf("write render plan: %w", err)
	}
	w.tempFiles = append(w.tempFiles, planPath)

	binaryPath, err := resolveNativeVideoEngineBinary()
	if err != nil {
		return fmt.Errorf("locate native engine: %w", err)
	}

	w.logger.Info("Launching native C++ engine (--render): %s", binaryPath)
	cmd := exec.Command(binaryPath, "--render", "--plan", planPath)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("stdout pipe: %w", err)
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start native engine: %w", err)
	}

	var stderrBuf strings.Builder
	stderrReader := bufio.NewReader(stderrPipe)
	progressDone := make(chan struct{})

	go func() {
		defer close(progressDone)
		for {
			line, err := stderrReader.ReadString('\n')
			if len(line) > 0 {
				line = strings.TrimRight(line, "\n\r")
				stderrBuf.WriteString(line)
				stderrBuf.WriteString("\n")
				var prog struct {
					Percent int    `json:"percent"`
					Scene   int    `json:"scene"`
					Total   int    `json:"total_scenes"`
					Stage   string `json:"stage"`
				}
				if json.Unmarshal([]byte(line), &prog) == nil && prog.Percent > 0 {
					if w.progressCallback != nil {
						w.progressCallback(prog.Percent, prog.Scene, prog.Total, prog.Stage)
					}
				}
			}
			if err != nil {
				break
			}
		}
	}()

	var stdoutBuf strings.Builder
	stdoutReader := bufio.NewReader(stdoutPipe)
	go func() {
		for {
			line, err := stdoutReader.ReadString('\n')
			if len(line) > 0 {
				stdoutBuf.WriteString(line)
			}
			if err != nil {
				break
			}
		}
	}()

	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	select {
	case <-ctx.Done():
		if cmd.Process != nil {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			}
		}
		<-progressDone
		return ctx.Err()
	case execErr := <-done:
		<-progressDone
		if execErr != nil {
			return fmt.Errorf("native C++ engine failed: %w (stderr=%s stdout=%s)",
				execErr, strings.TrimSpace(stderrBuf.String()), strings.TrimSpace(stdoutBuf.String()))
		}
	}

	if trimmed := strings.TrimSpace(stdoutBuf.String()); trimmed != "" {
		w.logger.Info("Native engine stdout: %s", trimmed)
	}
	if trimmed := strings.TrimSpace(stderrBuf.String()); trimmed != "" {
		w.logger.Info("Native engine stderr: %s", trimmed)
	}

	if _, err := os.Stat(plan.OutputPath); err != nil {
		return fmt.Errorf("native engine did not create output file %s: %w", plan.OutputPath, err)
	}

	return nil
}
