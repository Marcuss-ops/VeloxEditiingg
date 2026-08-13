package video

import (
	"context"
	"fmt"
	"strings"
)

// PerformFullAssociation associates entities with audio transcription timestamps.
// It matches user-provided entities (names, phrases, words, dates, numbers)
// against transcription segments using fuzzy string matching.
//
// Updated thresholds (recalibrated for Italian transcription):
// - Nomi_Con_Testo: user names → partial fuzzy match (threshold 80, was 75)
// - Nomi_Speciali: AI-extracted special names → partial fuzzy match (threshold 50, was 35)
// - Frasi_Importanti: important phrases → keyword + partial fuzzy (threshold 45, was 30)
// - Parole_Importanti: important words → partial fuzzy (threshold 40, was 25)
// - Entita_Senza_Testo: image-only entities → fuzzy + keyword (threshold 70)
// - Date: dates → regex extraction with semantic validation (day 1-31, month 1-12, year 1900-2100)
// - Numeri: numbers → direct extraction (skips segments containing dates)
func PerformFullAssociation(ctx context.Context,
	_ string, // audioFilePath: kept for backward compatibility, not used internally
	entitaInputStr string, // JSON string: {"Nome Utente 1": {}, ...}
	nomiSpecialiInputStr string, // JSON string: ["Nome Speciale da Qwen 1", ...]
	entitaSenzaTestoInputStr string, // JSON string: {"Nome Trovato da Qwen 1": "url1", ...}
	frasiImportantiInputStr string, // JSON string: ["Frase da Qwen 1", ...]
	paroleImportantiInputStr string, // JSON string: ["Parola da Qwen 1", ...]
	statusCallback func(string, bool),
	configSettings map[string]interface{},
	preTranscribedSegments []interface{}) (map[string]interface{}, error) {

	statusCallback("Starting entity association", false)

	// Parse transcription segments
	segments := parseTranscriptionSegments(preTranscribedSegments)
	if len(segments) == 0 {
		statusCallback("Warning: no transcription segments provided, returning empty matches", false)
		return emptyAssociationResult(), nil
	}
	statusCallback(fmt.Sprintf("Parsed %d transcription segments", len(segments)), false)

	// Normalize every segment's text ONCE up front. Each entity below loops
	// over all segments, so without this the same segment text would be
	// re-lowercased/re-filtered for every entity (O(N×S) allocations).
	normSegments := normalizeSegments(segments)

	// Parse entity inputs
	entitaMap := parseJSONStringMap(entitaInputStr)
	nomiSpeciali := parseJSONStringSlice(nomiSpecialiInputStr)
	entitaSenzaTestoMap := parseJSONStringMap(entitaSenzaTestoInputStr)
	frasiImportanti := parseJSONStringSlice(frasiImportantiInputStr)
	paroleImportanti := parseJSONStringSlice(paroleImportantiInputStr)

	// FIX #8: Recalibrated thresholds to reduce false positives.
	// Previous thresholds were too permissive (25-35) causing massive false matches.
	// New thresholds balance recall vs precision for Italian transcription.

	// --- Nomi_Con_Testo (threshold 80, was 75) ---
	statusCallback("Matching Nomi_Con_Testo...", false)
	nomiConTesto, err := matchEntityCategory(ctx, stringMapKeys(entitaMap), normSegments, 80.0)
	if err != nil {
		return nil, err
	}

	// --- Nomi_Speciali (threshold 50, was 35) ---
	statusCallback("Matching Nomi_Speciali...", false)
	nomiSpecialiResult, err := matchEntityCategory(ctx, nomiSpeciali, normSegments, 50.0)
	if err != nil {
		return nil, err
	}

	// --- Frasi_Importanti (threshold 45, was 30) ---
	statusCallback("Matching Frasi_Importanti...", false)
	frasiResult, err := matchEntityCategory(ctx, frasiImportanti, normSegments, 45.0)
	if err != nil {
		return nil, err
	}

	// --- Parole_Importanti (threshold 40, was 25) ---
	statusCallback("Matching Parole_Importanti...", false)
	paroleResult, err := matchEntityCategory(ctx, paroleImportanti, normSegments, 40.0)
	if err != nil {
		return nil, err
	}

	// --- Entita_Senza_Testo (threshold 70, multi-strategy) ---
	statusCallback("Matching Entita_Senza_Testo...", false)
	entitaSenzaTestoResult := make(map[string]EntitaResult)
	for name, val := range entitaSenzaTestoMap {
		// Check context cancellation periodically.
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("entity association cancelled: %w", err)
		}

		urls := extractEntityURLs(val)
		// Direct fuzzy match, then keyword match, then partial-word fallback.
		// FIX #7: only include the entity when it has timestamps AND a URL; a
		// URL with no timestamps cannot be rendered.
		matches := matchEntityToSegments(name, normSegments, 70.0, "fuzzy")
		if len(matches) == 0 {
			matches = matchEntityByKeywords(name, normSegments)
		}
		if len(matches) == 0 {
			matches = matchPartialWords(name, normSegments)
		}
		if len(matches) > 0 && len(urls) > 0 {
			entitaSenzaTestoResult[name] = EntitaResult{
				LinkImmagine: urls,
				Timestamps:   matches,
			}
		}
	}

	// --- Date extraction from transcription ---
	statusCallback("Extracting dates...", false)
	dateResult := extractDates(segments)

	// --- Number extraction from transcription (skip date segments) ---
	statusCallback("Extracting numbers...", false)
	dateTimestamps := collectDateTimestamps(dateResult)
	numeriResult := extractNumbers(segments, dateTimestamps)

	statusCallback("Entity association completed", false)

	result := map[string]interface{}{
		"Nomi_Con_Testo":     nomiConTesto,
		"Frasi_Importanti":   frasiResult,
		"Entita_Senza_Testo": entitaSenzaTestoResult,
		"Nomi_Speciali":      nomiSpecialiResult,
		"Date":               dateResult,
		"Parole_Importanti":  paroleResult,
		"Numeri":             numeriResult,
	}
	return result, nil
}

// ResolveEntities determines the final entity associations to use for rendering.
// Priority order:
// 1. preAssociatedEntities (from API, Computer A provided)
// 2. associazioniFinaliConTimestamp (already computed)
// 3. Perform fuzzy matching if entity strings are provided
// 4. Return empty result if all entities are "None"
func ResolveEntities(
	ctx context.Context,
	_ string, // audioFilePath: kept for backward compatibility, not used internally
	entitaInputStr string,
	nomiSpecialiInputStr string,
	entitaSenzaTestoInputStr string,
	frasiImportantiInputStr string,
	paroleImportantiInputStr string,
	associazioniFinaliConTimestamp map[string]interface{},
	formattedImgEntities map[string]interface{},
	preAssociatedEntities map[string]interface{},
	segmentsForSRTGeneration []interface{},
	configSettings map[string]interface{},
	statusCallback func(string, bool),
) (map[string]interface{}, map[string]interface{}, error) {

	// Priority 1: Use pre-associated entities from API
	if hasNonEmptyContent(preAssociatedEntities) {
		statusCallback("Using pre-associated entities from API", false)
		// FIX #4: Validate timestamps against audio duration from segments
		maxDuration := extractMaxDuration(segmentsForSRTGeneration)
		validated := validatePreAssociatedEntities(preAssociatedEntities, maxDuration)
		return validated, formattedImgEntities, nil
	}

	// Priority 2: Use already-computed associations if available
	if hasNonEmptyContent(associazioniFinaliConTimestamp) {
		statusCallback("Using pre-computed entity association", false)
		// FIX #4: Validate timestamps against audio duration from segments
		maxDuration := extractMaxDuration(segmentsForSRTGeneration)
		validated := validatePreAssociatedEntities(associazioniFinaliConTimestamp, maxDuration)
		return validated, formattedImgEntities, nil
	}

	// Check if all entity inputs are "None" or empty
	allNone := isNoneOrEmpty(entitaInputStr) &&
		isNoneOrEmpty(nomiSpecialiInputStr) &&
		isNoneOrEmpty(entitaSenzaTestoInputStr) &&
		isNoneOrEmpty(frasiImportantiInputStr) &&
		isNoneOrEmpty(paroleImportantiInputStr)

	if allNone {
		statusCallback("All entities are None, skipping association", false)
		return emptyAssociationResult(), formattedImgEntities, nil
	}

	// Priority 3: Perform fuzzy matching association
	statusCallback("No pre-associated entities found, performing fuzzy matching...", false)
	associations, err := PerformFullAssociation(
		ctx,
		"", // audioFilePath not used by PerformFullAssociation
		entitaInputStr,
		nomiSpecialiInputStr,
		entitaSenzaTestoInputStr,
		frasiImportantiInputStr,
		paroleImportantiInputStr,
		statusCallback,
		configSettings,
		segmentsForSRTGeneration,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("entity association failed: %w", err)
	}
	return associations, formattedImgEntities, nil
}

// matchEntityCategory fuzzy-matches a list of entity names against the
// pre-normalized segments, falling back to keyword matching when the partial
// fuzzy pass finds nothing. It centralizes the per-category loop (context
// cancellation, fallback, dedup) that was previously repeated four times.
func matchEntityCategory(ctx context.Context, names []string, normSegments []normalizedSegment, threshold float64) (map[string][]MatchResult, error) {
	result := make(map[string][]MatchResult)
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("entity association cancelled: %w", err)
		}
		matches := matchEntityToSegments(name, normSegments, threshold, "partial_fuzzy")
		if len(matches) == 0 {
			matches = matchEntityByKeywords(name, normSegments)
		}
		if len(matches) > 0 {
			result[name] = deduplicateMatches(matches)
		}
	}
	return result, nil
}

// stringMapKeys returns the keys of a map as a slice, so callers can feed a
// map-keyed category (Nomi_Con_Testo) through the same slice-based matching
// helper as the slice-keyed categories.
func stringMapKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// extractEntityURLs normalizes the Entita_Senza_Testo value (either a single
// URL string or a []interface{} of strings) into a []string of image URLs.
func extractEntityURLs(val interface{}) []string {
	if urlStr, ok := val.(string); ok && urlStr != "" {
		return []string{urlStr}
	}
	urlSlice, ok := val.([]interface{})
	if !ok {
		return nil
	}
	urls := make([]string, 0, len(urlSlice))
	for _, u := range urlSlice {
		if s, ok := u.(string); ok {
			urls = append(urls, s)
		}
	}
	return urls
}

// matchPartialWords is the last-resort Entita_Senza_Testo strategy: it matches
// any entity word of >=4 chars against the segment's pre-split tokens as a
// full-token (word boundary) equality check.
func matchPartialWords(name string, normSegments []normalizedSegment) []MatchResult {
	nameWords := strings.Fields(normalizeForMatch(name))
	var matches []MatchResult
	for _, seg := range normSegments {
		for _, w := range nameWords {
			if len(w) < 4 {
				continue
			}
			for _, hw := range seg.words {
				if w == hw {
					matches = append(matches, MatchResult{
						TimestampStart: seg.start,
						TimestampEnd:   seg.end,
						Score:          50.0,
						Method:         "partial_word",
						Text:           seg.raw,
					})
					break
				}
			}
		}
	}
	return matches
}

// hasNonEmptyContent reports whether an association map carries at least one
// non-empty category. Used by ResolveEntities to decide whether a
// pre-associated / pre-computed result is usable.
func hasNonEmptyContent(m map[string]interface{}) bool {
	for _, v := range m {
		if vm, ok := v.(map[string]interface{}); ok && len(vm) > 0 {
			return true
		}
	}
	return false
}

// isNoneOrEmpty reports whether an entity input string is blank or an explicit
// "none"/"null" sentinel.
func isNoneOrEmpty(s string) bool {
	trimmed := strings.TrimSpace(s)
	return trimmed == "" || strings.EqualFold(trimmed, "none") || strings.EqualFold(trimmed, `"none"`) || strings.EqualFold(trimmed, "null")
}
