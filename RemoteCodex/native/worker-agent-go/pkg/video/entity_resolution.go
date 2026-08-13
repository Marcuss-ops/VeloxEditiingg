package video

import (
	"encoding/json"
	"math"
	"sort"
	"strings"
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

	// Sort by score descending (keep best match). Stable so equal scores
	// retain their input order instead of the previous O(n^2) selection sort.
	sorted := make([]MatchResult, len(matches))
	copy(sorted, matches)
	sort.SliceStable(sorted, func(i, j int) bool {
		return sorted[i].Score > sorted[j].Score
	})

	// Keep only non-overlapping matches (5-second window). Preallocate to the
	// input size so the filter loop never re-grows the backing array.
	const dedupWindow = 5.0
	result := make([]MatchResult, 0, len(sorted))
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

	// Sort result by timestamp for chronological order.
	sort.SliceStable(result, func(i, j int) bool {
		return result[i].TimestampStart < result[j].TimestampStart
	})
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

// normalizedSegment is a transcription segment whose Text has been
// lowercased/filtered exactly once and whose words have been pre-split once.
// Raw keeps the original text for result payloads. It exists so a hot loop
// that matches N entities against S segments normalizes each segment text
// once (O(S)) and splits its words once (O(S)) instead of re-normalizing and
// re-splitting for every entity (O(N×S)).
type normalizedSegment struct {
	text  string
	words []string
	raw   string
	start float64
	end   float64
}

// normalizeSegments pre-normalizes every segment's text and pre-splits its
// words once. Callers that loop entities × segments pass the normalized slice
// to the match helpers so the per-entity loops never re-normalize, re-allocate
// nor re-split the segment text.
func normalizeSegments(segments []TranscriptionSegment) []normalizedSegment {
	norm := make([]normalizedSegment, len(segments))
	for i, seg := range segments {
		text := normalizeForMatch(seg.Text)
		norm[i] = normalizedSegment{
			text:  text,
			words: strings.Fields(text),
			raw:   seg.Text,
			start: seg.Start,
			end:   seg.End,
		}
	}
	return norm
}

// matchEntityToSegments fuzzy-matches an entity string against transcription segments.
// Returns matches above the given threshold. The entity is normalized once and
// reused across every segment (the needle is invariant), and the results slice
// is preallocated to the segment count.
func matchEntityToSegments(entity string, segments []normalizedSegment, threshold float64, method string) []MatchResult {
	needle := normalizeForMatch(entity)
	// The needle length is invariant across segments, so the two Levenshtein
	// scratch rows are allocated once per entity and reused for every segment
	// instead of being re-allocated per (entity × segment).
	prev := make([]int, len(needle)+1)
	curr := make([]int, len(needle)+1)
	results := make([]MatchResult, 0, len(segments))
	for _, seg := range segments {
		score := partialFuzzyRatioNormalizedBuf(needle, seg.text, prev, curr)
		if score >= threshold {
			results = append(results, MatchResult{
				TimestampStart: seg.start,
				TimestampEnd:   seg.end,
				Score:          math.Round(score*100) / 100,
				Method:         method,
				Text:           seg.raw,
			})
		}
	}
	return results
}

// matchEntityByKeywords matches an entity using keyword presence in segments.
// Score is differentiated: longer word matches and higher coverage get higher scores (50-80).
// The needle is normalized and split into words once (invariant across segments).
func matchEntityByKeywords(entity string, segments []normalizedSegment) []MatchResult {
	normEntity := normalizeForMatch(entity)
	entityWords := strings.Fields(normEntity)
	results := make([]MatchResult, 0, len(segments))
	for _, seg := range segments {
		if matched, word, coverage := keywordMatchFields(normEntity, entityWords, seg.words); matched {
			// Score based on coverage: 50 (single short word) to 80 (full phrase match)
			score := 50.0 + math.Min(coverage, 30.0)
			results = append(results, MatchResult{
				TimestampStart: seg.start,
				TimestampEnd:   seg.end,
				Score:          math.Round(score*100) / 100,
				Method:         "keyword:" + word,
				Text:           seg.raw,
			})
		}
	}
	return results
}
