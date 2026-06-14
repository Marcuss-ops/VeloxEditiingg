package youtube

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// =============================================================================
// Translation types
// =============================================================================

type TranslateTextRequest struct {
	Text             string `json:"text" binding:"required"`
	TargetLanguage   string `json:"target_language" binding:"required"`
	Tone             string `json:"tone,omitempty"`
	PreserveHashtags bool   `json:"preserve_hashtags,omitempty"`
}

type TranslateTextResponse struct {
	OK             bool   `json:"ok"`
	SourceText     string `json:"source_text"`
	SanitizedText  string `json:"sanitized_text"`
	TranslatedText string `json:"translated_text"`
	TargetLanguage string `json:"target_language"`
	Provider       string `json:"provider"`
}

// =============================================================================
// Text sanitization (shared with cover generation)
// =============================================================================

var (
	specialCharStripper = regexp.MustCompile(`[^\p{L}\p{N}\s\-\_\:\,\!\.\?\&\'\"\/]`)
	spaceNormalizer     = regexp.MustCompile(`\s+`)
)

var profanityPatterns = []struct {
	re   *regexp.Regexp
	mask string
}{
	{regexp.MustCompile(`(?i)\b(cazzo|merda|stronzo|stronza|fanculo|culo|pazzo|idiota)\b`), "****"},
	{regexp.MustCompile(`(?i)\b(fuck|shit|bitch|asshole|bastard|dick)\b`), "****"},
}

func sanitizeCreativeText(text string, preserveHashtags bool) string {
	cleaned := strings.TrimSpace(text)
	if cleaned == "" {
		return ""
	}

	cleaned = strings.ReplaceAll(cleaned, "\u200b", "")
	cleaned = strings.ReplaceAll(cleaned, "\u200d", "")
	cleaned = strings.ReplaceAll(cleaned, "\ufeff", "")

	if !preserveHashtags {
		cleaned = strings.ReplaceAll(cleaned, "#", "")
	}

	cleaned = specialCharStripper.ReplaceAllString(cleaned, " ")
	for _, pat := range profanityPatterns {
		cleaned = pat.re.ReplaceAllString(cleaned, pat.mask)
	}
	cleaned = spaceNormalizer.ReplaceAllString(cleaned, " ")
	return strings.TrimSpace(cleaned)
}

// =============================================================================
// Translation handler
// =============================================================================

func (h *YouTubeHandlers) TranslateText(c *gin.Context) {
	var req TranslateTextRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "invalid request: " + err.Error()})
		return
	}

	sanitized := sanitizeCreativeText(req.Text, req.PreserveHashtags)
	translated, provider := h.translateTextBestEffort(c.Request.Context(), sanitized, req.TargetLanguage, req.Tone)

	c.JSON(http.StatusOK, TranslateTextResponse{
		OK:             true,
		SourceText:     req.Text,
		SanitizedText:  sanitized,
		TranslatedText: translated,
		TargetLanguage: req.TargetLanguage,
		Provider:       provider,
	})
}

func (h *YouTubeHandlers) sanitizeCreativeTextInput(text string) string {
	return sanitizeCreativeText(text, false)
}

// =============================================================================
// NVIDIA chat translation (best-effort with fallback)
// =============================================================================

func (h *YouTubeHandlers) translateTextBestEffort(ctx context.Context, text, targetLanguage, tone string) (string, string) {
	text = sanitizeCreativeText(text, true)
	if text == "" {
		return "", "fallback"
	}

	textURL := strings.TrimSpace(h.service.GetConfig().NVIDIATextURL)
	if textURL == "" {
		return fallbackTranslate(text, targetLanguage), "fallback"
	}

	payload := map[string]any{
		"model": "meta/llama-3.1-8b-instruct",
		"messages": []map[string]string{
			{
				"role":    "system",
				"content": "You are a translation engine. Return only the translated text, without quotes, notes, markdown or explanations.",
			},
			{
				"role": "user",
				"content": fmt.Sprintf(
					"Translate the following text into %s. Tone: %s. Preserve the meaning and keep it natural for a YouTube audience:\n\n%s",
					targetLanguage, tone, text,
				),
			},
		},
		"temperature": 0.2,
		"max_tokens":  256,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fallbackTranslate(text, targetLanguage), "fallback"
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, textURL, bytes.NewReader(body))
	if err != nil {
		return fallbackTranslate(text, targetLanguage), "fallback"
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", h.service.GetConfig().NVIDIAAPIKey))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fallbackTranslate(text, targetLanguage), "fallback"
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fallbackTranslate(text, targetLanguage), "fallback"
	}

	var result struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fallbackTranslate(text, targetLanguage), "fallback"
	}
	if len(result.Choices) == 0 {
		return fallbackTranslate(text, targetLanguage), "fallback"
	}

	translated := sanitizeCreativeText(result.Choices[0].Message.Content, true)
	if translated == "" {
		translated = fallbackTranslate(text, targetLanguage)
	}
	return translated, "nvidia"
}

func fallbackTranslate(text, targetLanguage string) string {
	switch strings.ToLower(strings.TrimSpace(targetLanguage)) {
	case "it", "it-it", "italian", "italiano":
		return text
	case "en", "en-us", "english":
		return text
	case "es", "es-es", "spanish", "español":
		return text
	case "fr", "fr-fr", "french", "français":
		return text
	default:
		return text
	}
}
