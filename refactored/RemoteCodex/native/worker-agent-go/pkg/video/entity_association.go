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
	nomiConTesto := make(map[string][]MatchResult)
	for name := range entitaMap {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("entity association cancelled: %w", ctx.Err())
		default:
		}
		matches := matchEntityToSegments(name, segments, 80.0, "partial_fuzzy")
		if len(matches) == 0 {
			matches = matchEntityByKeywords(name, segments)
		}
		if len(matches) > 0 {
			nomiConTesto[name] = deduplicateMatches(matches)
		}
	}

	// --- Nomi_Speciali (threshold 50, was 35) ---
	statusCallback("Matching Nomi_Speciali...", false)
	nomiSpecialiResult := make(map[string][]MatchResult)
	for _, name := range nomiSpeciali {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("entity association cancelled: %w", ctx.Err())
		default:
		}
		matches := matchEntityToSegments(name, segments, 50.0, "partial_fuzzy")
		if len(matches) == 0 {
			matches = matchEntityByKeywords(name, segments)
		}
		if len(matches) > 0 {
			nomiSpecialiResult[name] = deduplicateMatches(matches)
		}
	}

	// --- Frasi_Importanti (threshold 45, was 30) ---
	statusCallback("Matching Frasi_Importanti...", false)
	frasiResult := make(map[string][]MatchResult)
	for _, phrase := range frasiImportanti {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("entity association cancelled: %w", ctx.Err())
		default:
		}
		matches := matchEntityToSegments(phrase, segments, 45.0, "partial_fuzzy")
		if len(matches) == 0 {
			matches = matchEntityByKeywords(phrase, segments)
		}
		if len(matches) > 0 {
			frasiResult[phrase] = deduplicateMatches(matches)
		}
	}

	// --- Parole_Importanti (threshold 40, was 25) ---
	statusCallback("Matching Parole_Importanti...", false)
	paroleResult := make(map[string][]MatchResult)
	for _, word := range paroleImportanti {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("entity association cancelled: %w", ctx.Err())
		default:
		}
		matches := matchEntityToSegments(word, segments, 40.0, "partial_fuzzy")
		if len(matches) == 0 {
			matches = matchEntityByKeywords(word, segments)
		}
		if len(matches) > 0 {
			paroleResult[word] = deduplicateMatches(matches)
		}
	}

	// --- Entita_Senza_Testo (threshold 70, multi-strategy) ---
	statusCallback("Matching Entita_Senza_Testo...", false)
	entitaSenzaTestoResult := make(map[string]EntitaResult)
	for name, val := range entitaSenzaTestoMap {
		// Check context cancellation periodically
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("entity association cancelled: %w", ctx.Err())
		default:
		}

		// Extract URL from the value
		var urls []string
		if urlStr, ok := val.(string); ok && urlStr != "" {
			urls = []string{urlStr}
		} else if urlSlice, ok := val.([]interface{}); ok {
			for _, u := range urlSlice {
				if s, ok := u.(string); ok {
					urls = append(urls, s)
				}
			}
		}

		// Try direct fuzzy match
		matches := matchEntityToSegments(name, segments, 70.0, "fuzzy")
		// Try keyword match (with word boundary)
		if len(matches) == 0 {
			matches = matchEntityByKeywords(name, segments)
		}
		// Partial word match fallback (with word boundary check)
		if len(matches) == 0 {
			normName := normalizeForMatch(name)
			nameWords := strings.Fields(normName)
			for _, seg := range segments {
				hayWords := strings.Fields(normalizeForMatch(seg.Text))
				for _, w := range nameWords {
					if len(w) < 4 {
						continue
					}
					// Word boundary check: must match a full token
					for _, hw := range hayWords {
						if w == hw {
							matches = append(matches, MatchResult{
								TimestampStart: seg.Start,
								TimestampEnd:   seg.End,
								Score:          50.0,
								Method:         "partial_word",
								Text:           seg.Text,
							})
							break
						}
					}
				}
			}
		}
		// FIX #7: Only include entity if it has actual timestamp matches.
		// Having just an URL with no timestamps means the renderer won't know when to show it.
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
	if len(preAssociatedEntities) > 0 {
		hasContent := false
		for _, v := range preAssociatedEntities {
			if m, ok := v.(map[string]interface{}); ok && len(m) > 0 {
				hasContent = true
				break
			}
		}
		if hasContent {
			statusCallback("Using pre-associated entities from API", false)
			// FIX #4: Validate timestamps against audio duration from segments
			maxDuration := extractMaxDuration(segmentsForSRTGeneration)
			validated := validatePreAssociatedEntities(preAssociatedEntities, maxDuration)
			return validated, formattedImgEntities, nil
		}
	}

	// Priority 2: Use already-computed associations if available
	if len(associazioniFinaliConTimestamp) > 0 {
		hasContent := false
		for _, v := range associazioniFinaliConTimestamp {
			if m, ok := v.(map[string]interface{}); ok && len(m) > 0 {
				hasContent = true
				break
			}
		}
		if hasContent {
			statusCallback("Using pre-computed entity association", false)
			// FIX #4: Validate timestamps against audio duration from segments
			maxDuration := extractMaxDuration(segmentsForSRTGeneration)
			validated := validatePreAssociatedEntities(associazioniFinaliConTimestamp, maxDuration)
			return validated, formattedImgEntities, nil
		}
	}

	// Check if all entity inputs are "None" or empty
	isNoneOrEmpty := func(s string) bool {
		trimmed := strings.TrimSpace(s)
		return trimmed == "" || strings.ToLower(trimmed) == "none" || strings.ToLower(trimmed) == `"none"` || strings.ToLower(trimmed) == `null`
	}

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
