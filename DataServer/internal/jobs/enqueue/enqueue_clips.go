// Package enqueue / enqueue_clips.go
//
// BuildClipPayloadForMaster is the canonical script-with-clip payload
// orchestrator. It is a linear flow that delegates:
//
//   - clip_input_normalizer.go    input-shape adapters + URL extractors
//                                 (three explicit input forms; no registry,
//                                 no per-format interface, no scene factory,
//                                 no generic pipeline).
//   - narrated_clip_timeline.go   voiceover-bed + final-clip timeline
//                                 builder used when any scene carries a
//                                 voiceover URL.
//
// Hard constraint: no format registry, no per-format interface, no scene
// factory, no generic pipeline builder. Three explicit functions cover
// the three input forms.
package enqueue

import (
	"fmt"
	"strings"
	"time"

	"velox-shared/contract"
	"velox-shared/paths"
	"velox-shared/payload"

	"github.com/google/uuid"
)

// BuildClipPayloadForMaster builds the canonical script-with-clips payload.
// Accepts either a SpecScene-style `scenes` payload (with optional
// drive_links / clip_links / voiceover_link), a `scenes_json` JSON string,
// or a flat `clips` payload. The output map is ready for Enqueuer.Enqueue
// and ultimately for the scene.composite worker executor.
//
// Linear flow:
//  1. Resolve videoName from video_name / title / topic / source_text / fallback.
//  2. normalizeClipPayload extracts (sceneEntries, clipItems, clipURLs,
//     audioTracks, videoMode) from the input.
//  3. Build script text from explicit fields, scene-level text,
//     or buildScriptText fallback.
//  4. Build voiceover paths from top-level field, or extract from
//     audioTracks if present.
//  5. Resolve identity fields (job_id / job_run_id / correlation_id).
//  6. Strip legacy aliases (id / run_id / title / voiceover_path / audio_path).
//  7. Fill a contract.NewJobPayloadV2 envelope and project to the output map.
//  8. Attach clips, items, optional audio_tracks, fit, and (when no
//     audio tracks yet) audio_url from the first voiceover path.
func BuildClipPayloadForMaster(rawPayload map[string]interface{}, dataDir, videosDir, _ string) (map[string]interface{}, error) {
	videoName := payload.FirstString(rawPayload, "video_name", "title", "topic")
	if videoName == "" {
		videoName = paths.SanitizeVideoName(payload.FirstString(rawPayload, "source_text"))
	}
	if videoName == "" {
		videoName = "generate_from_clips_" + time.Now().UTC().Format("20060102_150405")
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
		outputPath = paths.DefaultOutputPath(videosDir, dataDir, videoName, "generate_from_clips")
	}

	audioLanguage := payload.FirstString(rawPayload, "audio_language_for_srt", "language")
	if audioLanguage == "" {
		audioLanguage = "it"
	}

	normalized := make(map[string]interface{}, len(rawPayload)+24)
	for k, v := range rawPayload {
		normalized[k] = v
	}
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
	v2.VoiceoverPaths = append([]string{}, voiceoverPaths...)
	v2.VoiceoverCount = len(voiceoverPaths)
	v2.AudioLanguage = audioLanguage
	v2.VideoMode = videoMode
	v2.OutputPath = outputPath
	v2.DriveOutput = ResolveDriveOutputFolderReference(dataDir, payload.FirstString(rawPayload, "drive_output_folder", "output_directory"))
	v2.SubmittedVia = "api_script_generate_from_clips"
	v2.Source = "script_generate_from_clips"
	v2.Version = "v2"
	v2.Status = "PENDING"
	if youtubeGroup := payload.FirstString(rawPayload, "youtube_group", "channel_id"); youtubeGroup != "" {
		v2.YoutubeGroup = youtubeGroup
		v2.ChannelID = youtubeGroup
	}

	out, err := v2.ToMap()
	if err != nil {
		return nil, err
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
	// Carry through delivery_plan (canonical top-level key) from the raw
	// payload so the enqueue-time validateDeliveryPlanRequires preflight
	// passes. The V2 typed struct does not yet model this field, so ToMap
	// drops it; without this carry-through, every clip enqueue is rejected
	// with "delivery_plan is required".
	if dp, ok := rawPayload["delivery_plan"]; ok && dp != nil {
		out["delivery_plan"] = dp
	}
	return out, nil
}
