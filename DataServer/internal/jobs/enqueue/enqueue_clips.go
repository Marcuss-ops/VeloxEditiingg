// Package enqueue / enqueue_clips.go
//
// BuildClipPayloadForMaster is the canonical script-with-clip payload
// orchestrator. It is a linear flow that delegates:
//
//   - clip_input_normalizer.go    canonical scene asset validation
//     and renderer input normalization (clip, stock[], voiceover).
//   - narrated_clip_timeline.go   voiceover-bed + final-clip timeline
//     builder for canonical narrated scenes.
//
// Hard constraint: the renderer receives only canonical nested assets;
// legacy paths, bindings, links, and positional clip pools are rejected
// before timeline construction.
package enqueue

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"velox-shared/contract"
	"velox-shared/paths"
	"velox-shared/payload"

	"github.com/google/uuid"
)

// BuildClipPayloadForMaster builds the canonical script-with-clips payload.
// Accepts a canonical `scenes` array or `scenes_json` containing scenes
// with clip, stock[], and optional voiceover asset objects. The output map
// is ready for Enqueuer.Enqueue
// and ultimately for the scene.composite worker executor.
//
// Linear flow:
//  1. Resolve videoName from video_name / title / topic / source_text / fallback.
//  2. normalizeClipPayload extracts (sceneEntries, clipItems, clipURLs,
//     audioTracks, videoMode) from the input.
//  3. Build script text from explicit fields, scene-level text,
//     or buildScriptText fallback.
//  4. Derive the canonical voiceover path list from generated audio tracks
//     for the legacy envelope; scene assets remain the renderer source.
//  5. Resolve identity fields (job_id / job_run_id / correlation_id).
//  6. Strip legacy aliases (id / run_id / title / voiceover_path / audio_path).
//  7. Fill a contract.NewJobPayloadV2 envelope and project to the output map.
//  8. Attach clips, items, optional audio_tracks, fit, and (when no
//     audio tracks yet) audio_url from the first voiceover path.
func BuildClipPayloadForMaster(rawPayload map[string]interface{}, dataDir, videosDir, _ string, dbs ...*sql.DB) (map[string]interface{}, error) {
	var db *sql.DB
	if len(dbs) > 0 {
		db = dbs[0]
	}
	videoName := payload.FirstString(rawPayload, "video_name", "title", "topic")
	if videoName == "" {
		videoName = paths.SanitizeVideoName(payload.FirstString(rawPayload, "source_text"))
	}
	if videoName == "" {
		videoName = "script_generate_" + time.Now().UTC().Format("20060102_150405")
	}

	sceneEntries, clipItems, clipURLs, audioTracks, videoMode, err := normalizeClipPayload(rawPayload)
	if err != nil {
		return nil, err
	}
	if len(clipItems) == 0 {
		return nil, fmt.Errorf("at least one clip is required")
	}

	scriptText := payload.FirstString(rawPayload, "script_text", "script", "source_text")
	if scriptText == "" {
		var parts []string
		for _, scene := range sceneEntries {
			if text := payload.FirstString(scene, "text", "description"); text != "" {
				parts = append(parts, text)
			}
		}
		if len(parts) == 0 {
			scriptText = buildScriptText(rawPayload)
		} else {
			scriptText = strings.Join(parts, "\n")
		}
	}

	voiceoverPaths := payload.NormalizeStringList(rawPayload, "voiceover_paths", "voiceover_path", "audio_path", "source_media", "source_media_url", "audio_source")
	if len(voiceoverPaths) == 0 && len(audioTracks) > 0 {
		for _, track := range audioTracks {
			if url := payload.FirstString(track, "source_url", "url"); url != "" {
				voiceoverPaths = append(voiceoverPaths, url)
			}
		}
		voiceoverPaths = payload.DedupeStrings(voiceoverPaths)
	}

	jobID := payload.FirstString(rawPayload, "job_id", "script_id")
	if jobID == "" {
		jobID = "scriptclip_" + uuid.NewString()
	}
	jobRunID := payload.FirstString(rawPayload, "job_run_id", "run_id")
	if jobRunID == "" {
		jobRunID = "run_" + uuid.NewString()
	}
	correlationID := payload.FirstString(rawPayload, "correlation_id")
	if correlationID == "" {
		correlationID = "corr_" + uuid.NewString()
	}

	outputPath := payload.FirstString(rawPayload, "output_path")
	if outputPath == "" {
		outputPath = paths.DefaultOutputPath(videosDir, dataDir, videoName, "script_generate")
	}

	audioLanguage := payload.FirstString(rawPayload, "audio_language_for_srt", "language")
	if audioLanguage == "" {
		audioLanguage = "it"
	}

	normalized := make(map[string]interface{}, len(rawPayload)+24)
	for k, v := range rawPayload {
		normalized[k] = v
	}
	// WRITE-path complement of http_response_compat.go (READ-path dual-write
	// for pre-PR15.6 clients). Both paths are independently load-bearing:
	// removing the strip fails the canonical-form gate; the adapter is
	// allowlisted in scripts/ci/check-payload-canonical-form.sh.
	for _, alias := range []string{"id", "run_id", "title", "voiceover_path", "audio_path"} {
		delete(normalized, alias)
	}

	v2 := contract.NewJobPayloadV2(normalized)
	v2.SetIdentity(jobID, jobRunID, correlationID)
	v2.VideoName = videoName
	v2.ScriptText = scriptText
	v2.Scenes = sceneEntries
	v2.ScenesJSON = payload.MustJSON(sceneEntries)
	v2.SceneCount = len(sceneEntries)
	// Narrated clip rendering is sourced exclusively from scene.voiceover
	// assets. Do not re-emit the retired top-level voiceover_paths alias;
	// generated audio_tracks carry the renderer timeline instead.
	v2.VoiceoverPaths = nil
	v2.VoiceoverCount = 0
	v2.AudioLanguage = audioLanguage
	v2.VideoMode = videoMode
	v2.OutputPath = outputPath
	v2.DriveOutput = ResolveDriveOutputFolderReference(dataDir, payload.FirstString(rawPayload, "drive_output_folder", "output_directory"), db)
	v2.SubmittedVia = "api_script_generate"
	v2.Source = "script_generate"
	v2.Version = "v2"
	v2.Status = "PENDING"

	out, err := v2.ToMap()
	if err != nil {
		return nil, err
	}
	// Preserve the native translation envelope through the canonical V2
	// projection. The scene text/bindings are already carried by v2.Scenes;
	// these fields make the requested target and per-scene translation result
	// visible to the remote Worker and API pollers as well.
	for _, key := range []string{"translate_to", "translation_status", "translation_language", "translations", "google_doc_id", "google_doc_link", "google_doc_title"} {
		if value, ok := rawPayload[key]; ok {
			out[key] = value
		}
	}
	out["clips"] = clipURLs
	out["items"] = clipItems
	if len(audioTracks) > 0 {
		out["audio_tracks"] = audioTracks
	}
	out["fit"] = payload.FirstString(rawPayload, "fit")
	if out["fit"] == "" {
		out["fit"] = "contain"
	}
	if len(voiceoverPaths) > 0 && len(audioTracks) == 0 {
		out["audio_url"] = voiceoverPaths[0]
	}
	return out, nil
}
