// Package worker provides job processing logic for the worker agent.
package worker

// renderJobParams holds the extracted parameters common to render/video/audio jobs.
type renderJobParams struct {
	audioPath                         string
	outputPath                        string
	scenesJSON                        string
	scriptText                        string
	startClipPaths                    []string
	middleClipPaths                   []string
	stockClipSources                  []string
	endClipPaths                      []string
	backgroundMusicPaths              []string
	backgroundVideoForImgOverlaysPath string
	associazioniFinaliConTimestamp    map[string]interface{}
	formattedImgEntities              map[string]interface{}
	preAssociatedEntities             map[string]interface{}
	rawEntities                       map[string]interface{}
	audioLanguageForSRT               string
	segmentsForSRTGeneration          []interface{}
	videoMode                         string
	introClipPaths                    []string
	stockClipPaths                    []string
	clipSegments                      []interface{}
	sceneImagePaths                   []string
	driveOutputFolder                 string
}

// convertToStringSlice safely converts an interface{} to []string.
func convertToStringSlice(input interface{}) []string {
	if input == nil {
		return []string{}
	}
	switch v := input.(type) {
	case []string:
		return v
	case []interface{}:
		result := make([]string, len(v))
		for i, item := range v {
			if str, ok := item.(string); ok {
				result[i] = str
			}
		}
		return result
	default:
		return []string{}
	}
}

// getStringParam safely extracts a string parameter.
func getStringParam(params map[string]interface{}, key, fallback string) string {
	if v, ok := params[key].(string); ok {
		return v
	}
	return fallback
}

// getMapParam safely extracts a map[string]interface{} parameter.
func getMapParam(params map[string]interface{}, key string) map[string]interface{} {
	if v, ok := params[key].(map[string]interface{}); ok {
		return v
	}
	return map[string]interface{}{}
}

// getSliceParam safely extracts a []interface{} parameter.
func getSliceParam(params map[string]interface{}, key string) []interface{} {
	if v, ok := params[key].([]interface{}); ok {
		return v
	}
	return []interface{}{}
}

// extractRenderJobParams safely extracts all render/video/audio job parameters.
func extractRenderJobParams(params map[string]interface{}) renderJobParams {
	introClipPaths := convertToStringSlice(params["intro_clip_paths"])
	if len(introClipPaths) == 0 {
		introClipPaths = convertToStringSlice(params["start_clip_paths"])
	}
	stockClipPaths := convertToStringSlice(params["stock_clip_paths"])
	if len(stockClipPaths) == 0 {
		stockClipPaths = convertToStringSlice(params["stock_clip_sources"])
	}

	return renderJobParams{
		audioPath:                         getStringParam(params, "audio_path", ""),
		outputPath:                        getStringParam(params, "output_path", ""),
		scenesJSON:                        getStringParam(params, "scenes_json", ""),
		scriptText:                        getStringParam(params, "script_text", ""),
		startClipPaths:                    convertToStringSlice(params["start_clip_paths"]),
		middleClipPaths:                   convertToStringSlice(params["middle_clip_paths"]),
		stockClipSources:                  convertToStringSlice(params["stock_clip_sources"]),
		endClipPaths:                      convertToStringSlice(params["end_clip_paths"]),
		backgroundMusicPaths:              convertToStringSlice(params["background_music_paths"]),
		backgroundVideoForImgOverlaysPath: getStringParam(params, "background_video_for_img_overlays_path", ""),
		associazioniFinaliConTimestamp:    getMapParam(params, "associazioni_finali_con_timestamp"),
		formattedImgEntities:              getMapParam(params, "formatted_img_entities"),
		preAssociatedEntities:             getMapParam(params, "pre_associated_entities"),
		rawEntities:                       getMapParam(params, "raw_entities"),
		audioLanguageForSRT:               getStringParam(params, "audio_language_for_srt", ""),
		segmentsForSRTGeneration:          getSliceParam(params, "segments_for_srt_generation"),
		videoMode:                         getStringParam(params, "video_mode", ""),
		introClipPaths:                    introClipPaths,
		stockClipPaths:                    stockClipPaths,
		clipSegments:                      getSliceParam(params, "clip_segments"),
		sceneImagePaths:                   convertToStringSlice(params["scene_image_paths"]),
		driveOutputFolder:                 getStringParam(params, "drive_output_folder", getStringParam(params, "output_directory", "")),
	}
}
