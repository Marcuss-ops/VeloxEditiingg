package video

import (
	"strings"
)

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

// EntitaResult represents an entity without text (image-only association result).
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
