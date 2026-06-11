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
)

type nativeVideoSceneRequest struct {
	Text            string   `json:"text"`
	ImageLink       string   `json:"image_link,omitempty"`
	ImageLinks      []string `json:"image_links,omitempty"`
	DurationSeconds float64  `json:"duration_seconds,omitempty"`
}

type nativeVideoClipRequest struct {
	Text            string   `json:"text,omitempty"`
	ClipLink        string   `json:"clip_link,omitempty"`
	ClipLinks       []string `json:"clip_links,omitempty"`
	DurationSeconds float64  `json:"duration_seconds,omitempty"`
	Kind            string   `json:"kind,omitempty"`
}

type nativeVideoEngineRequest struct {
	JobID               string                    `json:"job_id"`
	VideoName           string                    `json:"video_name"`
	ScriptText          string                    `json:"script_text"`
	VoiceoverPaths      []string                  `json:"voiceover_paths,omitempty"`
	Scenes              []nativeVideoSceneRequest `json:"scenes"`
	VideoMode           string                    `json:"video_mode,omitempty"`
	IntroClipPaths      []string                  `json:"intro_clip_paths,omitempty"`
	StockClipPaths      []string                  `json:"stock_clip_paths,omitempty"`
	ClipSegments        []nativeVideoClipRequest  `json:"clip_segments,omitempty"`
	ScenesJSON          string                    `json:"scenes_json,omitempty"`
	SceneImagePaths     []string                  `json:"scene_image_paths,omitempty"`
	OutputPath          string                    `json:"output_path"`
	DriveOutputFolder   string                    `json:"drive_output_folder,omitempty"`
	AudioLanguageForSRT string                    `json:"audio_language_for_srt,omitempty"`
}

func (w *VideoGenerationWorkflow) runNativeCxxEngine(
	ctx context.Context,
	tempDir string,
	input VideoGenerationInput,
) error {
	videoMode := strings.TrimSpace(input.VideoMode)
	if videoMode == "" && (len(input.IntroClipPaths) > 0 || len(input.StockClipPaths) > 0 || len(input.ClipSegments) > 0) {
		videoMode = "clip_stock"
	}

	request := nativeVideoEngineRequest{
		VideoName:           filepath.Base(strings.TrimSuffix(input.OutputPath, filepath.Ext(input.OutputPath))),
		ScriptText:          input.ScriptText,
		OutputPath:          input.OutputPath,
		AudioLanguageForSRT: input.AudioLanguageForSRT,
		ScenesJSON:          strings.TrimSpace(input.ScenesJSON),
		VideoMode:           videoMode,
		IntroClipPaths:      sanitizeStrings(input.IntroClipPaths),
		StockClipPaths:      sanitizeStrings(input.StockClipPaths),
		DriveOutputFolder:   strings.TrimSpace(input.DriveOutputFolder),
	}
	if strings.TrimSpace(input.AudioPath) != "" {
		request.VoiceoverPaths = []string{strings.TrimSpace(input.AudioPath)}
	}

	request.Scenes = parseNativeVideoScenes(input.ScenesJSON)
	if len(request.Scenes) == 0 {
		request.Scenes = []nativeVideoSceneRequest{{
			Text:            strings.TrimSpace(input.ScriptText),
			DurationSeconds: 5,
		}}
	}
	request.ClipSegments = parseNativeVideoClips(input.ClipSegments)
	request.SceneImagePaths = sanitizeStrings(input.SceneImagePaths)
	for i := range request.Scenes {
		if request.Scenes[i].DurationSeconds <= 0 {
			request.Scenes[i].DurationSeconds = 5
		}
	}

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
	cmd := exec.CommandContext(ctx, binaryPath, "--request", requestPath)
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

func parseNativeVideoClips(raw []interface{}) []nativeVideoClipRequest {
	if len(raw) == 0 {
		return nil
	}

	clips := make([]nativeVideoClipRequest, 0, len(raw))
	for _, item := range raw {
		obj, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		clip := nativeVideoClipRequest{
			Text:            toSceneString(obj["text"]),
			ClipLink:        firstClipSource(obj),
			ClipLinks:       clipSources(obj),
			DurationSeconds: clipDuration(obj),
			Kind:            toSceneString(obj["kind"]),
		}
		if len(clip.ClipLinks) == 0 && clip.ClipLink != "" {
			clip.ClipLinks = []string{clip.ClipLink}
		}
		clips = append(clips, clip)
	}
	return clips
}

func parseNativeVideoScenes(scenesJSON string) []nativeVideoSceneRequest {
	trimmed := strings.TrimSpace(scenesJSON)
	if trimmed == "" {
		return nil
	}

	var raw []map[string]interface{}
	if err := json.Unmarshal([]byte(trimmed), &raw); err != nil {
		return nil
	}

	scenes := make([]nativeVideoSceneRequest, 0, len(raw))
	for _, item := range raw {
		scene := nativeVideoSceneRequest{
			Text:      toSceneString(item["text"]),
			ImageLink: firstSceneImageLink(item),
		}
		scene.ImageLinks = sceneImageLinks(item)
		if len(scene.ImageLinks) == 0 && scene.ImageLink != "" {
			scene.ImageLinks = []string{scene.ImageLink}
		}
		scenes = append(scenes, scene)
	}
	return scenes
}

func firstSceneImageLink(scene map[string]interface{}) string {
	if scene == nil {
		return ""
	}
	if s := toSceneString(scene["image_link"]); s != "" {
		return s
	}
	for _, link := range sceneImageLinks(scene) {
		if strings.TrimSpace(link) != "" {
			return strings.TrimSpace(link)
		}
	}
	return ""
}

func sceneImageLinks(scene map[string]interface{}) []string {
	if scene == nil {
		return nil
	}
	var links []string
	if v, ok := scene["image_links"]; ok {
		switch vv := v.(type) {
		case []interface{}:
			for _, item := range vv {
				if s := toSceneString(item); s != "" {
					links = append(links, s)
				}
			}
		case []string:
			for _, s := range vv {
				if strings.TrimSpace(s) != "" {
					links = append(links, strings.TrimSpace(s))
				}
			}
		}
	}
	return links
}

func firstClipSource(item map[string]interface{}) string {
	if item == nil {
		return ""
	}
	if s := toSceneString(item["clip_link"]); s != "" {
		return s
	}
	for _, link := range clipSources(item) {
		if strings.TrimSpace(link) != "" {
			return strings.TrimSpace(link)
		}
	}
	return ""
}

func clipSources(item map[string]interface{}) []string {
	if item == nil {
		return nil
	}
	var links []string
	if v, ok := item["clip_links"]; ok {
		switch vv := v.(type) {
		case []interface{}:
			for _, it := range vv {
				if s := toSceneString(it); s != "" {
					links = append(links, s)
				}
			}
		case []string:
			for _, s := range vv {
				if strings.TrimSpace(s) != "" {
					links = append(links, strings.TrimSpace(s))
				}
			}
		}
	}
	return links
}

func clipDuration(item map[string]interface{}) float64 {
	if item == nil {
		return 0
	}
	switch v := item["duration_seconds"].(type) {
	case float64:
		return v
	case int:
		return float64(v)
	case int64:
		return float64(v)
	}
	return 0
}

func sanitizeStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func toSceneString(v interface{}) string {
	switch vv := v.(type) {
	case string:
		return strings.TrimSpace(vv)
	default:
		return ""
	}
}

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
