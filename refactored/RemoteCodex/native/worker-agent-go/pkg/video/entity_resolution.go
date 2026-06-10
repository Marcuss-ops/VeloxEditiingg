package video

import (
	"encoding/json"
	"fmt"
	"math"
)

// extractMaxDuration finds the maximum timestamp in transcription segments.
// Used to validate pre-associated entity timestamps are within audio range.
func extractMaxDuration(segments []interface{}) float64 {
	var maxDur float64
	for _, item := range segments {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		if end, ok := toFloat64(m["end"]); ok && end > maxDur {
			maxDur = end
		}
	}
	return maxDur
}

// toFloat64 converts interface{} to float64, handling both float64 and json.Number.
func toFloat64(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	default:
		return 0, false
	}
}

// deduplicateMatches removes redundant matches that overlap temporally.
// For matches within the same 5-second window, keeps only the highest-scoring one.
// FIX #6: Prevents duplicate overlays when the same entity matches consecutive segments.
func deduplicateMatches(matches []MatchResult) []MatchResult {
	if len(matches) <= 1 {
		return matches
	}

	// Sort by score descending (keep best match)
	sorted := make([]MatchResult, len(matches))
	copy(sorted, matches)
	for i := 0; i < len(sorted)-1; i++ {
		for j := i + 1; j < len(sorted); j++ {
			if sorted[j].Score > sorted[i].Score {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}

	// Keep only non-overlapping matches (5-second window)
	const dedupWindow = 5.0
	var result []MatchResult
	for _, m := range sorted {
		overlaps := false
		for _, kept := range result {
			// Check temporal overlap within window
			if m.TimestampStart < kept.TimestampEnd+dedupWindow && m.TimestampEnd > kept.TimestampStart-dedupWindow {
				overlaps = true
				break
			}
		}
		if !overlaps {
			result = append(result, m)
		}
	}

	// Sort result by timestamp for chronological order
	for i := 0; i < len(result)-1; i++ {
		for j := i + 1; j < len(result); j++ {
			if result[j].TimestampStart < result[i].TimestampStart {
				result[i], result[j] = result[j], result[i]
			}
		}
	}
	return result
}

// validatePreAssociatedEntities checks that pre-associated entity timestamps are reasonable.
// FIX #4: Validates timestamp ranges against audio duration to prevent out-of-range overlays.
func validatePreAssociatedEntities(entities map[string]interface{}, maxDuration float64) map[string]interface{} {
	if maxDuration <= 0 {
		return entities // No duration info, can't validate
	}

	validated := make(map[string]interface{})
	for category, value := range entities {
		entityMap, ok := value.(map[string]interface{})
		if !ok {
			validated[category] = value
			continue
		}

		validEntity := make(map[string]interface{})
		for entityName, matchList := range entityMap {
			matchArr, ok := matchList.([]interface{})
			if !ok {
				validEntity[entityName] = matchList
				continue
			}

			var validMatches []interface{}
			for _, m := range matchArr {
				matchMap, ok := m.(map[string]interface{})
				if !ok {
					validMatches = append(validMatches, m)
					continue
				}

				// Check timestamp_start is within audio duration
				if ts, ok := toFloat64(matchMap["timestamp_start"]); ok {
					if ts >= 0 && ts <= maxDuration {
						validMatches = append(validMatches, m)
					}
				}
			}

			if len(validMatches) > 0 {
				validEntity[entityName] = validMatches
			}
		}

		if len(validEntity) > 0 {
			validated[category] = validEntity
		}
	}
	return validated
}

// emptyAssociationResult returns an empty association result structure.
func emptyAssociationResult() map[string]interface{} {
	return map[string]interface{}{
		"Nomi_Con_Testo":     map[string][]MatchResult{},
		"Frasi_Importanti":   map[string][]MatchResult{},
		"Entita_Senza_Testo": map[string]EntitaResult{},
		"Nomi_Speciali":      map[string][]MatchResult{},
		"Date":               map[string][]MatchResult{},
		"Parole_Importanti":  map[string][]MatchResult{},
		"Numeri":             map[string][]MatchResult{},
	}
}

// matchEntityToSegments fuzzy-matches an entity string against transcription segments.
// Returns matches above the given threshold.
func matchEntityToSegments(entity string, segments []TranscriptionSegment, threshold float64, method string) []MatchResult {
	var results []MatchResult
	for _, seg := range segments {
		score := partialFuzzyRatio(entity, seg.Text)
		if score >= threshold {
			results = append(results, MatchResult{
				TimestampStart: seg.Start,
				TimestampEnd:   seg.End,
				Score:          math.Round(score*100) / 100,
				Method:         method,
				Text:           seg.Text,
			})
		}
	}
	return results
}

// matchEntityByKeywords matches an entity using keyword presence in segments.
// Score is differentiated: longer word matches and higher coverage get higher scores (50-80).
func matchEntityByKeywords(entity string, segments []TranscriptionSegment) []MatchResult {
	var results []MatchResult
	for _, seg := range segments {
		if matched, word, coverage := keywordMatch(entity, seg.Text); matched {
			// Score based on coverage: 50 (single short word) to 80 (full phrase match)
			score := 50.0 + math.Min(coverage, 30.0)
			results = append(results, MatchResult{
				TimestampStart: seg.Start,
				TimestampEnd:   seg.End,
				Score:          math.Round(score*100) / 100,
				Method:         fmt.Sprintf("keyword:%s", word),
				Text:           seg.Text,
			})
		}
	}
	return results
}
