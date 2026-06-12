package video

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"velox-shared/contract"
	"velox-shared/media"
	"velox-shared/paths"
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
		return err
	}

	w.logger.Info("Launching native C++ engine: %s", binaryPath)
	// The current C++ engine expects the explicit full pipeline subcommand.
	cmd := exec.CommandContext(ctx, binaryPath, "--full-video", "--request", requestPath)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("native C++ engine failed: %w (stderr=%s stdout=%s)", err, strings.TrimSpace(stderr.String()), strings.TrimSpace(stdout.String()))
	}

	if trimmed := strings.TrimSpace(stdout.String()); trimmed != "" {
		w.logger.Info("Native engine stdout: %s", trimmed)
	}
	if trimmed := strings.TrimSpace(stderr.String()); trimmed != "" {
		w.logger.Info("Native engine stderr: %s", trimmed)
	}

	if _, err := os.Stat(input.OutputPath); err != nil {
		return fmt.Errorf("native engine did not create output file %s: %w", input.OutputPath, err)
	}

	return nil
}

// resolveNativeVideoEngineBinary cerca il binary del C++ video engine.
// Cerca prima in VELOX_VIDEO_ENGINE_CPP_BIN env var, poi in path canonici
// relativi al source file del package.
func resolveNativeVideoEngineBinary() (string, error) {
	if override := strings.TrimSpace(os.Getenv("VELOX_VIDEO_ENGINE_CPP_BIN")); override != "" {
		if stat, err := os.Stat(override); err == nil && !stat.IsDir() {
			return override, nil
		}
	}

	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("unable to locate native engine source path")
	}
	pkgDir := filepath.Dir(sourceFile)
	candidates := []string{
		filepath.Join(pkgDir, "..", "..", "..", "video-engine-cpp", "build", "velox_video_engine"),
		filepath.Join(pkgDir, "..", "..", "..", "video-engine-cpp", "velox_video_engine"),
		filepath.Join(pkgDir, "..", "..", "..", "..", "video-engine-cpp", "build", "velox_video_engine"),
		filepath.Join(pkgDir, "..", "..", "..", "..", "video-engine-cpp", "velox_video_engine"),
	}

	for _, candidate := range candidates {
		cleaned := filepath.Clean(candidate)
		if stat, err := os.Stat(cleaned); err == nil && !stat.IsDir() {
			return cleaned, nil
		}
	}

	return "", fmt.Errorf("native C++ engine binary not found; set VELOX_VIDEO_ENGINE_CPP_BIN or build RemoteCodex/native/video-engine-cpp")
}
