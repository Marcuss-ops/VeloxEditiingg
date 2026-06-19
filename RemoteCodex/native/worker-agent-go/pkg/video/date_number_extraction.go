package video

import (
	"encoding/json"
	"fmt"
	"strings"
)

// extractDates finds date-like patterns in transcription segments.
// FIX #3: Returns the set of date strings found, so extractNumbers can skip them.
// FIX #9: looksLikeDate now validates semantic ranges (day 1-31, month 1-12).
func extractDates(segments []TranscriptionSegment) map[string][]MatchResult {
	result := make(map[string][]MatchResult)

	months := map[string]bool{
		"gennaio": true, "febbraio": true, "marzo": true, "aprile": true,
		"maggio": true, "giugno": true, "luglio": true, "agosto": true,
		"settembre": true, "ottobre": true, "novembre": true, "dicembre": true,
	}

	for _, seg := range segments {
		lower := strings.ToLower(seg.Text)
		words := strings.Fields(lower)

		// Check for Italian month names with adjacent numeric day/year
		for i, w := range words {
			if months[w] && i > 0 && i < len(words)-1 {
				prevIsNum := isNumeric(words[i-1])
				nextIsNum := isNumeric(words[i+1])
				if prevIsNum && nextIsNum {
					day := parseIntSafe(words[i-1])
					year := parseIntSafe(words[i+1])
					// Semantic validation: day must be 1-31, year must be reasonable
					if day >= 1 && day <= 31 && year >= 1900 && year <= 2100 {
						dateStr := fmt.Sprintf("%d %s %d", day, w, year)
						result[dateStr] = append(result[dateStr], MatchResult{
							TimestampStart: seg.Start,
							TimestampEnd:   seg.End,
							Score:          90.0,
							Method:         "date_regex",
							Text:           seg.Text,
						})
					}
				}
			}
		}

		// Check for numeric date patterns (dd/mm/yyyy or dd-mm-yyyy)
		for _, w := range words {
			if day, month, year, ok := parseNumericDate(w); ok {
				// Semantic validation
				if day >= 1 && day <= 31 && month >= 1 && month <= 12 && year >= 1900 && year <= 2100 {
					result[w] = append(result[w], MatchResult{
						TimestampStart: seg.Start,
						TimestampEnd:   seg.End,
						Score:          85.0,
						Method:         "date_numeric",
						Text:           seg.Text,
					})
				}
			}
		}
	}
	return result
}

// extractNumbers finds numbers in transcription segments.
// FIX #3: Skips numbers that are part of date patterns (dd, mm, yyyy).
func extractNumbers(segments []TranscriptionSegment, dateTimestamps map[float64]bool) map[string][]MatchResult {
	result := make(map[string][]MatchResult)
	for _, seg := range segments {
		words := strings.Fields(seg.Text)
		for _, w := range words {
			clean := strings.Trim(w, ".,;:!?\"'()[]{}")
			if isNumeric(clean) && len(clean) >= 2 {
				// Skip numbers that fall within a date's timestamp range
				// to avoid duplicate overlays (date as text + date as separate numbers)
				if dateTimestamps[seg.Start] {
					continue
				}
				result[clean] = append(result[clean], MatchResult{
					TimestampStart: seg.Start,
					TimestampEnd:   seg.End,
					Score:          100.0,
					Method:         "number_extract",
					Text:           seg.Text,
				})
			}
		}
	}
	return result
}

// isNumeric checks if a string is a valid number.
func isNumeric(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// parseIntSafe safely parses a string as int, returns 0 on failure.
func parseIntSafe(s string) int {
	n := 0
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0
		}
		n = n*10 + int(r-'0')
	}
	return n
}

// parseNumericDate parses a string like "dd/mm/yyyy" or "dd-mm-yyyy" and validates ranges.
// Returns day, month, year, ok.
func parseNumericDate(s string) (day, month, year int, ok bool) {
	for _, sep := range []string{"/", "-"} {
		parts := strings.Split(s, sep)
		if len(parts) == 3 && isNumeric(parts[0]) && isNumeric(parts[1]) && isNumeric(parts[2]) {
			if len(parts[0]) <= 2 && len(parts[1]) <= 2 && len(parts[2]) == 4 {
				d, m, y := parseIntSafe(parts[0]), parseIntSafe(parts[1]), parseIntSafe(parts[2])
				return d, m, y, true
			}
		}
	}
	return 0, 0, 0, false
}

// collectDateTimestamps builds a set of timestamp start values from date matches.
// Used to skip number extraction for segments that already contain dates.
func collectDateTimestamps(dateResult map[string][]MatchResult) map[float64]bool {
	ts := make(map[float64]bool)
	for _, matches := range dateResult {
		for _, m := range matches {
			ts[m.TimestampStart] = true
		}
	}
	return ts
}

// parseJSONStringMap parses a JSON string into a map[string]interface{}.
func parseJSONStringMap(s string) map[string]interface{} {
	if strings.TrimSpace(s) == "" {
		return map[string]interface{}{}
	}
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		return map[string]interface{}{}
	}
	return m
}

// parseJSONStringSlice parses a JSON string into a []string.
func parseJSONStringSlice(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	var arr []string
	if err := json.Unmarshal([]byte(s), &arr); err != nil {
		// Try as []interface{}
		var rawArr []interface{}
		if err2 := json.Unmarshal([]byte(s), &rawArr); err2 == nil {
			for _, v := range rawArr {
				if str, ok := v.(string); ok {
					arr = append(arr, str)
				}
			}
		}
		return arr
	}
	return arr
}

// getRawEntityString safely extracts a JSON string from the rawEntities map.
// Returns empty string if not found or invalid.
func getRawEntityString(rawEntities map[string]interface{}, key string) string {
	if rawEntities == nil {
		return ""
	}
	if v, ok := rawEntities[key]; ok {
		switch val := v.(type) {
		case string:
			return val
		default:
			data, _ := json.Marshal(val)
			return string(data)
		}
	}
	return ""
}
