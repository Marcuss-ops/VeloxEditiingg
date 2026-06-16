package jobs

import (
	"fmt"
	"strings"
)

func validateJobPayload(payload map[string]interface{}) (bool, []string, map[string]interface{}) {
	errors := []string{}
	details := make(map[string]interface{})

	jobType := strings.ToLower(strings.TrimSpace(getStringOrEmpty(payload, "job_type")))
	if jobType == "health_check" || jobType == "smoke_check" {
		details["job_type"] = jobType
		videoName, _ := payload["video_name"].(string)
		if strings.TrimSpace(videoName) != "" {
			details["video_name"] = strings.TrimSpace(videoName)
		}
		return true, nil, details
	}

	videoName, _ := payload["video_name"].(string)
	if videoName == "" || strings.TrimSpace(videoName) == "" {
		errors = append(errors, "[ERROR] video_name mancante o vuoto")
	} else {
		if len(videoName) > 50 {
			details["video_name"] = videoName[:50]
		} else {
			details["video_name"] = videoName
		}
	}

	clipFields := []struct {
		listField string
		urlField  string
		display   string
	}{
		{"intro_clips", "intro_clips_urls", "intro_clips"},
		{"intro_clip_paths", "intro_clip_paths", "intro_clip_paths"},
		{"start_clips", "start_clips_urls", "start_clips"},
		{"middle_clips", "middle_clips_urls", "middle_clips"},
		{"end_clips", "end_clips_urls", "end_clips"},
		{"stock_clips", "stock_clips_urls", "stock_clips"},
		{"stock_clip_paths", "stock_clip_paths", "stock_clip_paths"},
	}

	hasAnyClip := false
	clipsDetail := []string{}
	for _, cf := range clipFields {
		if listVal, ok := payload[cf.listField].([]interface{}); ok && len(listVal) > 0 {
			clipsDetail = append(clipsDetail, fmt.Sprintf("[OK] %s: %d clip(s)", cf.display, len(listVal)))
			hasAnyClip = true
			details[cf.display] = fmt.Sprintf("%d clips", len(listVal))
		}
		if urlVal, ok := payload[cf.urlField].(string); ok && strings.TrimSpace(urlVal) != "" {
			urlCount := len(strings.Split(strings.TrimSpace(urlVal), "\n"))
			clipsDetail = append(clipsDetail, fmt.Sprintf("[OK] %s: %d URL(s)", cf.display, urlCount))
			hasAnyClip = true
			details[cf.display] = fmt.Sprintf("%d URLs", urlCount)
		}
	}

	if !hasAnyClip {
		if segments, ok := payload["clip_segments"].([]interface{}); ok && len(segments) > 0 {
			hasAnyClip = true
			details["clip_segments"] = fmt.Sprintf("%d segments", len(segments))
		}
	}

	if !hasAnyClip {
		errors = append(errors, "[ERROR] NESSUN CLIP PRESENTE - Servono clip in intro/start/middle/end/stock oppure clip_segments")
	} else {
		details["clips"] = clipsDetail
	}

	hasAnyVoiceover := false
	voiceoverDetail := []string{}

	if voList, ok := payload["voiceovers"].([]interface{}); ok && len(voList) > 0 {
		voiceoverDetail = append(voiceoverDetail, fmt.Sprintf("[OK] voiceovers: %d item(s)", len(voList)))
		hasAnyVoiceover = true
		details["voiceovers"] = fmt.Sprintf("%d voiceovers", len(voList))
	} else if voStr, ok := payload["voiceovers"].(string); ok && strings.TrimSpace(voStr) != "" {
		urlCount := len(strings.Split(strings.TrimSpace(voStr), "\n"))
		voiceoverDetail = append(voiceoverDetail, fmt.Sprintf("[OK] voiceovers: %d URL(s)", urlCount))
		hasAnyVoiceover = true
		details["voiceovers"] = fmt.Sprintf("%d voiceover URLs", urlCount)
	}

	if voURLs, ok := payload["voiceovers_urls"].(string); ok && strings.TrimSpace(voURLs) != "" {
		urlCount := len(strings.Split(strings.TrimSpace(voURLs), "\n"))
		voiceoverDetail = append(voiceoverDetail, fmt.Sprintf("[OK] voiceovers_urls: %d URL(s)", urlCount))
		hasAnyVoiceover = true
		details["voiceovers"] = fmt.Sprintf("%d voiceover URLs", urlCount)
	}

	if voItems, ok := payload["voiceover_items"].([]interface{}); ok && len(voItems) > 0 {
		voiceoverDetail = append(voiceoverDetail, fmt.Sprintf("[OK] voiceover_items: %d item(s)", len(voItems)))
		hasAnyVoiceover = true
		details["voiceover_items"] = fmt.Sprintf("%d items", len(voItems))
	}

	if !hasAnyVoiceover {
		errors = append(errors, "[ERROR] NESSUN VOICEOVER PRESENTE - Servono voiceover")
		errors = append(errors, "   [INFO] Usa: voiceovers (lista), voiceovers_urls (stringa), o voiceover_items (lista di dict)")
	} else {
		details["voiceover_detail"] = voiceoverDetail
	}

	return len(errors) == 0, errors, details
}
