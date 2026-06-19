// Package contract definisce i tipi condivisi tra Go e C++ per la serializzazione JSON
// dei job di elaborazione video. Le strutture qui devono corrispondere esattamente
// alle controparti C++ in RemoteCodex/native/video-engine-cpp/include/video_contract.hpp.
//
// Mapping Go ↔ C++:
//
//	Go (shared/contract)        C++ (video_contract.hpp)
//	─────────────────────        ─────────────────────────
//	SceneRequest                 video::SceneAsset (alias velox::SceneRuntime)
//	ClipRequest                  video::ClipAsset  (alias velox::ClipRuntime)
//	VideoEngineRequest           video::SceneVideoRequest
//	(nessun equivalente)         video::SceneVideoResult
package contract

import (
	"encoding/json"
	"strings"

	"velox-shared/payload"
)

// SceneRequest corrisponde a video::SceneAsset in C++ (video_contract.hpp).
// JSON fields: text, image_link, image_links, duration_seconds
type SceneRequest struct {
	Text            string   `json:"text"`
	ImageLink       string   `json:"image_link,omitempty"`
	ImageLinks      []string `json:"image_links,omitempty"`
	DurationSeconds float64  `json:"duration_seconds,omitempty"`
}

// ClipRequest corrisponde a video::ClipAsset in C++ (video_contract.hpp).
// JSON fields: text, clip_link, clip_links, duration_seconds, kind
type ClipRequest struct {
	Text            string   `json:"text,omitempty"`
	ClipLink        string   `json:"clip_link,omitempty"`
	ClipLinks       []string `json:"clip_links,omitempty"`
	DurationSeconds float64  `json:"duration_seconds,omitempty"`
	Kind            string   `json:"kind,omitempty"`
}

// VideoEngineRequest corrisponde a video::SceneVideoRequest in C++ (video_contract.hpp).
// JSON fields: job_id, video_name, script_text, voiceover_paths, scenes, video_mode,
// intro_clip_paths, stock_clip_paths, clip_segments (C++: clip_segments_json string),
// scenes_json, scene_image_paths, output_path, drive_output_folder, audio_language_for_srt
type VideoEngineRequest struct {
	ContractVersion     int            `json:"contract_version,omitempty"`
	JobID               string         `json:"job_id"`
	VideoName           string         `json:"video_name"`
	ScriptText          string         `json:"script_text"`
	VoiceoverPaths      []string       `json:"voiceover_paths,omitempty"`
	Scenes              []SceneRequest `json:"scenes"`
	VideoMode           string         `json:"video_mode,omitempty"`
	IntroClipPaths      []string       `json:"intro_clip_paths,omitempty"`
	StockClipPaths      []string       `json:"stock_clip_paths,omitempty"`
	ClipSegments        []ClipRequest  `json:"clip_segments,omitempty"`
	ScenesJSON          string         `json:"scenes_json,omitempty"`
	SceneImagePaths     []string       `json:"scene_image_paths,omitempty"`
	OutputPath          string         `json:"output_path"`
	DriveOutputFolder   string         `json:"drive_output_folder,omitempty"`
	AudioLanguageForSRT string         `json:"audio_language_for_srt,omitempty"`
	AssetCacheDir       string         `json:"asset_cache_dir,omitempty"`
}

// RenderJobParams raggruppa tutti i parametri necessari per l'elaborazione di un job video.
// Viene popolato da ExtractRenderJobParams a partire da una mappa di parametri
// (tipicamente da JSON job).
//	// Nota: è l'unione di parametri per pipeline render + video + SRT.
	// I campi sovrapposti con VideoEngineRequest vengono riconciliati in
	// native_engine.go prima di inviare la richiesta al C++ engine.
type RenderJobParams struct {
	AudioPath                         string
	OutputPath                        string
	ScenesJSON                        string
	ScriptText                        string
	StartClipPaths                    []string
	MiddleClipPaths                   []string
	StockClipSources                  []string
	EndClipPaths                      []string
	BackgroundMusicPaths              []string
	BackgroundVideoForImgOverlaysPath string
	AssociazioniFinaliConTimestamp    map[string]interface{}
	FormattedImgEntities              map[string]interface{}
	PreAssociatedEntities             map[string]interface{}
	RawEntities                       map[string]interface{}
	AudioLanguageForSRT               string
	SegmentsForSRTGeneration          []interface{}
	VideoMode                         string
	IntroClipPaths                    []string
	StockClipPaths                    []string
	ClipSegments                      []interface{}
	SceneImagePaths                   []string
	DriveOutputFolder                 string
	AssetCacheDir                     string
}

// ExtractRenderJobParams estrae i parametri di un job da una mappa generica
// (tipicamente job.Parameters deserializzato da JSON) in un RenderJobParams tipizzato.
//
// Gestisce alias di campi (es. intro_clip_paths ← start_clip_paths) e fallback
// (es. drive_output_folder ← output_directory).
func ExtractRenderJobParams(params map[string]interface{}) RenderJobParams {
	introClipPaths := payload.ToSliceString(params["intro_clip_paths"])
	if len(introClipPaths) == 0 {
		introClipPaths = payload.ToSliceString(params["start_clip_paths"])
	}
	stockClipPaths := payload.ToSliceString(params["stock_clip_paths"])
	if len(stockClipPaths) == 0 {
		stockClipPaths = payload.ToSliceString(params["stock_clip_sources"])
	}

	return RenderJobParams{
		AudioPath:                         payload.StringParam(params, "audio_path", ""),
		OutputPath:                        payload.StringParam(params, "output_path", ""),
		ScenesJSON:                        payload.StringParam(params, "scenes_json", ""),
		ScriptText:                        payload.StringParam(params, "script_text", ""),
		StartClipPaths:                    payload.ToSliceString(params["start_clip_paths"]),
		MiddleClipPaths:                   payload.ToSliceString(params["middle_clip_paths"]),
		StockClipSources:                  payload.ToSliceString(params["stock_clip_sources"]),
		EndClipPaths:                      payload.ToSliceString(params["end_clip_paths"]),
		BackgroundMusicPaths:              payload.ToSliceString(params["background_music_paths"]),
		BackgroundVideoForImgOverlaysPath: payload.StringParam(params, "background_video_for_img_overlays_path", ""),
		AssociazioniFinaliConTimestamp:    payload.MapParam(params, "associazioni_finali_con_timestamp"),
		FormattedImgEntities:              payload.MapParam(params, "formatted_img_entities"),
		PreAssociatedEntities:             payload.MapParam(params, "pre_associated_entities"),
		RawEntities:                       payload.MapParam(params, "raw_entities"),
		AudioLanguageForSRT:               payload.StringParam(params, "audio_language_for_srt", ""),
		SegmentsForSRTGeneration:          payload.SliceParam(params, "segments_for_srt_generation"),
		VideoMode:                         payload.StringParam(params, "video_mode", ""),
		IntroClipPaths:                    introClipPaths,
		StockClipPaths:                    stockClipPaths,
		ClipSegments:                      payload.SliceParam(params, "clip_segments"),		SceneImagePaths:     payload.ToSliceString(params["scene_image_paths"]),
		DriveOutputFolder:   payload.StringParam(params, "drive_output_folder", payload.StringParam(params, "output_directory", "")),
		AssetCacheDir:       payload.StringParam(params, "asset_cache_dir", ""),
	}
}

func ParseScenes(scenesJSON string) []SceneRequest {
	trimmed := strings.TrimSpace(scenesJSON)
	if trimmed == "" {
		return nil
	}

	var raw []map[string]interface{}
	if err := json.Unmarshal([]byte(trimmed), &raw); err != nil {
		return nil
	}

	scenes := make([]SceneRequest, 0, len(raw))
	for _, item := range raw {
		scene := SceneRequest{
			Text:            toSceneString(item["text"]),
			ImageLink:       firstSceneImageLink(item),
			DurationSeconds: sceneDuration(item),
		}
		scene.ImageLinks = sceneImageLinks(item)
		if len(scene.ImageLinks) == 0 && scene.ImageLink != "" {
			scene.ImageLinks = []string{scene.ImageLink}
		}
		scenes = append(scenes, scene)
	}
	return scenes
}

// ParseClipsJSON parsa un JSON string contenente un array di clip in []ClipRequest.
// È simmetrica a ParseScenes: input stringa JSON, output slice tipizzata.
// Gli errori di parsing vengono silenziati (restituisce nil).
func ParseClipsJSON(clipsJSON string) []ClipRequest {
	trimmed := strings.TrimSpace(clipsJSON)
	if trimmed == "" {
		return nil
	}

	var raw []map[string]interface{}
	if err := json.Unmarshal([]byte(trimmed), &raw); err != nil {
		return nil
	}

	clips := make([]ClipRequest, 0, len(raw))
	for _, item := range raw {
		clip := ClipRequest{
			Text:            toSceneString(item["text"]),
			ClipLink:        firstClipSource(item),
			ClipLinks:       clipSources(item),
			DurationSeconds: clipDuration(item),
			Kind:            toSceneString(item["kind"]),
		}
		if len(clip.ClipLinks) == 0 && clip.ClipLink != "" {
			clip.ClipLinks = []string{clip.ClipLink}
		}
		clips = append(clips, clip)
	}
	return clips
}

// ParseClips parsa un []interface{} (tipicamente da json.Unmarshal in map[string]interface{})
// in []ClipRequest. Utile quando i dati arrivano già deserializzati.
func ParseClips(raw []interface{}) []ClipRequest {
	if len(raw) == 0 {
		return nil
	}

	clips := make([]ClipRequest, 0, len(raw))
	for _, item := range raw {
		obj, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		clip := ClipRequest{
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

// UnmarshalSceneRequest parsa un JSON string (singolo oggetto scena) in *SceneRequest.
// Restituisce errore se il JSON è malformato.
func UnmarshalSceneRequest(data []byte) (*SceneRequest, error) {
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" {
		return nil, nil
	}
	var raw map[string]interface{}
	if err := json.Unmarshal([]byte(trimmed), &raw); err != nil {
		return nil, err
	}
	scene := SceneRequest{
		Text:            toSceneString(raw["text"]),
		ImageLink:       firstSceneImageLink(raw),
		DurationSeconds: sceneDuration(raw),
	}
	scene.ImageLinks = sceneImageLinks(raw)
	if len(scene.ImageLinks) == 0 && scene.ImageLink != "" {
		scene.ImageLinks = []string{scene.ImageLink}
	}
	return &scene, nil
}

// UnmarshalClipRequest parsa un JSON string (singolo oggetto clip) in *ClipRequest.
// Restituisce errore se il JSON è malformato.
func UnmarshalClipRequest(data []byte) (*ClipRequest, error) {
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" {
		return nil, nil
	}
	var raw map[string]interface{}
	if err := json.Unmarshal([]byte(trimmed), &raw); err != nil {
		return nil, err
	}
	clip := ClipRequest{
		Text:            toSceneString(raw["text"]),
		ClipLink:        firstClipSource(raw),
		ClipLinks:       clipSources(raw),
		DurationSeconds: clipDuration(raw),
		Kind:            toSceneString(raw["kind"]),
	}
	if len(clip.ClipLinks) == 0 && clip.ClipLink != "" {
		clip.ClipLinks = []string{clip.ClipLink}
	}
	return &clip, nil
}

// UnmarshalScenes parsa un JSON string (array di scene) in []SceneRequest.
// Simile a ParseScenes ma restituisce l'errore in caso di JSON malformato.
func UnmarshalScenes(data []byte) ([]SceneRequest, error) {
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" {
		return nil, nil
	}
	var raw []map[string]interface{}
	if err := json.Unmarshal([]byte(trimmed), &raw); err != nil {
		return nil, err
	}
	scenes := make([]SceneRequest, 0, len(raw))
	for _, item := range raw {
		scenes = append(scenes, SceneRequest{
			Text:            toSceneString(item["text"]),
			ImageLink:       firstSceneImageLink(item),
			ImageLinks:      sceneImageLinks(item),
			DurationSeconds: sceneDuration(item),
		})
	}
	return scenes, nil
}

// UnmarshalClips parsa un JSON string (array di clip) in []ClipRequest.
// Simile a ParseClipsJSON ma restituisce l'errore in caso di JSON malformato.
func UnmarshalClips(data []byte) ([]ClipRequest, error) {
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" {
		return nil, nil
	}
	var raw []map[string]interface{}
	if err := json.Unmarshal([]byte(trimmed), &raw); err != nil {
		return nil, err
	}
	clips := make([]ClipRequest, 0, len(raw))
	for _, item := range raw {
		clip := ClipRequest{
			Text:            toSceneString(item["text"]),
			ClipLink:        firstClipSource(item),
			ClipLinks:       clipSources(item),
			DurationSeconds: clipDuration(item),
			Kind:            toSceneString(item["kind"]),
		}
		if len(clip.ClipLinks) == 0 && clip.ClipLink != "" {
			clip.ClipLinks = []string{clip.ClipLink}
		}
		clips = append(clips, clip)
	}
	return clips, nil
}

// NormalizeSceneEntry normalizza un entry scena da una mappa generica.
// Unifica campi: text, image_link (da image_url/image), image_links.
// Imposta duration_seconds a 5.0 se non specificato o <= 0.
func NormalizeSceneEntry(scene map[string]interface{}) map[string]interface{} {
	normalized := make(map[string]interface{}, len(scene)+4)
	for k, v := range scene {
		normalized[k] = v
	}
	if text := payload.FirstString(scene, "text"); text != "" {
		normalized["text"] = text
	}
	if image := payload.FirstString(scene, "image_link", "image_url", "image"); image != "" {
		normalized["image_link"] = image
	}
	if links := payload.NormalizeStringList(scene, "image_links"); len(links) > 0 {
		normalized["image_links"] = links
	} else if image := payload.FirstString(scene, "image_link"); image != "" {
		normalized["image_links"] = []string{image}
	}
	if duration := payload.NormalizedDuration(normalized["duration_seconds"]); duration <= 0 {
		normalized["duration_seconds"] = 5.0
	}
	return normalized
}

// FirstSceneImageLink restituisce il primo image_link disponibile da una scena,
// cercando in image_link, image_url, image e image_links[0] in ordine.
func FirstSceneImageLink(scene map[string]interface{}) string {
	return firstSceneImageLink(scene)
}

func firstSceneImageLink(scene map[string]interface{}) string {
	if scene == nil {
		return ""
	}
	if image := payload.FirstString(scene, "image_link", "image_url", "image"); image != "" {
		return image
	}
	if links := payload.NormalizeStringList(scene, "image_links"); len(links) > 0 {
		return links[0]
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

func sceneDuration(item map[string]interface{}) float64 {
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
	case json.Number:
		if f, err := v.Float64(); err == nil {
			return f
		}
	}
	return 0
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

func toSceneString(v interface{}) string {
	switch vv := v.(type) {
	case string:
		return strings.TrimSpace(vv)
	default:
		return ""
	}
}
