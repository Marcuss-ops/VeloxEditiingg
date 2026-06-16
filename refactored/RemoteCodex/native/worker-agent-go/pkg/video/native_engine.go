package video

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"velox-shared/contract"
	"velox-shared/media"
	"velox-shared/paths"
	"velox-worker-agent/pkg/binaryresolver"
)

// runNativeCxxEngine prepara una richiesta JSON per il C++ video engine, la serializza
// su disco e lancia il binary nativo. Gestisce:
//   - Costruzione di contract.VideoEngineRequest da contract.RenderJobParams
//   - Parse di ScenesJSON e ClipSegments via contract.ParseScenes/ParseClips
//   - Auto-detect della durata audio se nessuna scena ha duration_seconds
//   - Sanitizzazione di path/URL tramite paths.SanitizeStrings
func (w *VideoGenerationWorkflow) runNativeCxxEngine(
	ctx context.Context,
	tempDir string,
	input contract.RenderJobParams,
) error {
	videoMode := strings.TrimSpace(input.VideoMode)
	if videoMode == "" && (len(input.IntroClipPaths) > 0 || len(input.StockClipPaths) > 0 || len(input.ClipSegments) > 0) {
		videoMode = "clip_stock"
	}

	request := contract.VideoEngineRequest{
		VideoName:           filepath.Base(strings.TrimSuffix(input.OutputPath, filepath.Ext(input.OutputPath))),
		ScriptText:          input.ScriptText,
		OutputPath:          input.OutputPath,
		AudioLanguageForSRT: input.AudioLanguageForSRT,
		ScenesJSON:          strings.TrimSpace(input.ScenesJSON),
		VideoMode:           videoMode,
		IntroClipPaths:      paths.SanitizeStrings(input.IntroClipPaths),
		StockClipPaths:      paths.SanitizeStrings(input.StockClipPaths),
		DriveOutputFolder:   strings.TrimSpace(input.DriveOutputFolder),
		AssetCacheDir:       strings.TrimSpace(input.AssetCacheDir),
	}
	if strings.TrimSpace(input.AudioPath) != "" {
		request.VoiceoverPaths = []string{strings.TrimSpace(input.AudioPath)}
	}

	request.Scenes = contract.ParseScenes(input.ScenesJSON)
	if len(request.Scenes) == 0 {
		request.Scenes = []contract.SceneRequest{{
			Text: strings.TrimSpace(input.ScriptText),
		}}
	}

	hasDuration := false
	for _, s := range request.Scenes {
		if s.DurationSeconds > 0 {
			hasDuration = true
			break
		}
	}
	if !hasDuration && len(request.VoiceoverPaths) > 0 {
		detected := media.DetectAudioDurationSecs(request.VoiceoverPaths[0])
		if detected > 0 {
			perScene := detected / float64(len(request.Scenes))
			w.logger.Info("[AUDIO_DURATION] Worker auto-detected audio: %.1fs total, %.1fs per scene (%d scenes)",
				detected, perScene, len(request.Scenes))
			for i := range request.Scenes {
				request.Scenes[i].DurationSeconds = perScene
			}
		}
	}

	request.ClipSegments = contract.ParseClips(input.ClipSegments)
	request.SceneImagePaths = paths.SanitizeStrings(input.SceneImagePaths)

	requestPath := filepath.Join(tempDir, "native_video_request.json")
	data, err := json.MarshalIndent(request, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal native engine request: %w", err)
	}
	if err := os.WriteFile(requestPath, data, 0o644); err != nil {
		return fmt.Errorf("write native engine request: %w", err)
	}
	w.tempFiles = append(w.tempFiles, requestPath)

	binaryPath, err := resolveNativeVideoEngineBinary()
	if err != nil {
		return fmt.Errorf("locate native engine: %w", err)
	}
	if err != nil {
		return err
	}

	w.logger.Info("Launching native C++ engine: %s", binaryPath)
	cmd := exec.Command(binaryPath, "--full-video", "--request", requestPath)
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

	// Stream stderr for progress parsing + log capture
	var stderrBuf bytes.Buffer
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
				// Try to parse as progress JSON
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

	// Stream stdout for final result
	var stdoutBuf bytes.Buffer
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

	// Wait for process with context cancellation
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

	if _, err := os.Stat(input.OutputPath); err != nil {
		return fmt.Errorf("native engine did not create output file %s: %w", input.OutputPath, err)
	}

	return nil
}

// resolveNativeVideoEngineBinary locates the C++ video engine using the
// reusable pkg/binaryresolver. Resolution order:
//
//  1. VELOX_VIDEO_ENGINE_CPP_BIN env override
//  2. /usr/local/bin/velox_video_engine (production Docker install)
//  3. Source-tree build paths (dev workflow)
// The same Resolver is used elsewhere (e.g. ffmpeg, ansible-playbook) so the
// discovery behaviour stays consistent across the worker agent.
func resolveNativeVideoEngineBinary() (string, error) {
	r := binaryresolver.Resolver{
		Name:      "velox_video_engine",
		EnvVar:    "VELOX_VIDEO_ENGINE_CPP_BIN",
		AbsCandidates: []string{
			"/usr/local/bin/velox_video_engine",
		},
		RelOffsets: []string{
			filepath.Join("..", "..", "..", "video-engine-cpp", "build", "velox_video_engine"),
			filepath.Join("..", "..", "..", "video-engine-cpp", "velox_video_engine"),
			filepath.Join("..", "..", "..", "..", "video-engine-cpp", "build", "velox_video_engine"),
			filepath.Join("..", "..", "..", "..", "video-engine-cpp", "velox_video_engine"),
		},
	}
	return r.Resolve(0)
}
