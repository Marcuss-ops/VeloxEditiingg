package youtube

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// =============================================================================
// Cover pack types
// =============================================================================

type CoverPackRequest struct {
	Title          string `json:"title" binding:"required"`
	Description    string `json:"description,omitempty"`
	TargetLanguage string `json:"target_language,omitempty"`
	Style          string `json:"style,omitempty"`
	ExtraPrompt    string `json:"extra_prompt,omitempty"`
	Width          int    `json:"width,omitempty"`
	Height         int    `json:"height,omitempty"`
	Steps          int    `json:"steps,omitempty"`
	VariantCount   int    `json:"variant_count,omitempty"`
}

type CoverVariant struct {
	ID             string `json:"id"`
	Label          string `json:"label"`
	Prompt         string `json:"prompt"`
	NegativePrompt string `json:"negative_prompt"`
	Headline       string `json:"headline"`
	Hook           string `json:"hook"`
	Filename       string `json:"filename,omitempty"`
	ImageBase64    string `json:"image_base64,omitempty"`
	Width          int    `json:"width"`
	Height         int    `json:"height"`
	Seed           int64  `json:"seed"`
	Provider       string `json:"provider,omitempty"`
	Translation    string `json:"translation,omitempty"`
}

type CoverPackResponse struct {
	OK              bool           `json:"ok"`
	Title           string         `json:"title"`
	SanitizedTitle  string         `json:"sanitized_title"`
	TranslatedTitle string         `json:"translated_title"`
	TranslatedBody  string         `json:"translated_body,omitempty"`
	TargetLanguage  string         `json:"target_language"`
	Style           string         `json:"style"`
	VariantCount    int            `json:"variant_count"`
	Variants        []CoverVariant `json:"variants"`
	Provider        string         `json:"provider"`
	Warnings        []string       `json:"warnings,omitempty"`
}

// =============================================================================
// Cover pack generation handler
// =============================================================================

func (h *YouTubeHandlers) GenerateCoverPack(c *gin.Context) {
	var req CoverPackRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "invalid request: " + err.Error()})
		return
	}

	if req.VariantCount <= 0 {
		req.VariantCount = 3
	}
	if req.VariantCount > 3 {
		req.VariantCount = 3
	}
	if req.Width <= 0 {
		req.Width = 1280
	}
	if req.Height <= 0 {
		req.Height = 720
	}
	if req.Steps <= 0 {
		req.Steps = 4
	}

	sanitizedTitle := sanitizeCreativeText(req.Title, false)
	sanitizedBody := sanitizeCreativeText(req.Description, false)
	targetLang := strings.TrimSpace(req.TargetLanguage)
	if targetLang == "" {
		targetLang = "it"
	}

	translatedTitle, provider := h.translateTextBestEffort(c.Request.Context(), sanitizedTitle, targetLang, "headline")
	translatedBody, _ := h.translateTextBestEffort(c.Request.Context(), sanitizedBody, targetLang, "description")
	if translatedBody == sanitizedBody {
		translatedBody = sanitizedBody
	}

	variants := buildCoverVariants(translatedTitle, translatedBody, req.Style, req.ExtraPrompt, req.Width, req.Height)
	var warnings []string
	if h.service.GetConfig().NVIDIAAPIKey == "" {
		warnings = append(warnings, "NVIDIA API key not configured: images will not be generated")
	}

	if h.service.GetConfig().NVIDIAAPIKey != "" {
		for i := range variants {
			variant := &variants[i]
			imageBytes, filename, genErr := h.generateNVIDIAThumbnail(c.Request.Context(), variant.Prompt, variant.NegativePrompt, req.Width, req.Height, req.Steps, variant.Seed)
			if genErr != nil {
				warnings = append(warnings, fmt.Sprintf("%s: %v", variant.ID, genErr))
				continue
			}

			variant.Filename = filename
			variant.ImageBase64 = base64.StdEncoding.EncodeToString(imageBytes)
			variant.Provider = "nvidia"
			variant.Translation = translatedTitle
		}
	}

	c.JSON(http.StatusOK, CoverPackResponse{
		OK:              true,
		Title:           req.Title,
		SanitizedTitle:  sanitizedTitle,
		TranslatedTitle: translatedTitle,
		TranslatedBody:  translatedBody,
		TargetLanguage:  targetLang,
		Style:           req.Style,
		VariantCount:    len(variants),
		Variants:        variants,
		Provider:        provider,
		Warnings:        warnings,
	})
}

// =============================================================================
// Cover variant builder
// =============================================================================

func buildCoverVariants(title, body, style, extraPrompt string, width, height int) []CoverVariant {
	baseHeadline := compactHeadline(title)
	hook := compactHook(body, title)
	style = strings.ToLower(strings.TrimSpace(style))

	preset := map[string][3]string{
		"cinematic": {
			"cinematic lighting, dramatic contrast, rich reds and golds, premium youtube thumbnail composition",
			"bold editorial portrait, sharp focus, energetic motion blur, high click-through feel",
			"wide action frame, explosive subject focus, dynamic perspective, intense thumbnail layout",
		},
		"news": {
			"clean news studio look, high clarity, strong visual hierarchy, bright contrast",
			"urgent breaking-news style, dynamic headline space, crisp edges, editorial look",
			"investigative documentary thumbnail, clean background, visual tension, professional composition",
		},
		"gaming": {
			"vibrant gaming thumbnail, neon glow, electric contrast, dramatic reaction",
			"high energy gaming action, exaggerated facial expression, bright colors, strong focal point",
			"arcade-inspired scene, intense motion, dramatic lighting, clickable youtube layout",
		},
		"tutorial": {
			"clean tutorial thumbnail, product demo layout, bright and clear, space for big text",
			"step-by-step educational visual, sleek UI elements, simple hierarchy, high readability",
			"professional how-to cover, minimal clutter, bold subject, instructional composition",
		},
	}

	defaultStyle := preset["cinematic"]
	if v, ok := preset[style]; ok {
		defaultStyle = v
	}

	variants := []CoverVariant{
		{
			ID:             "A",
			Label:          "A",
			Headline:       baseHeadline,
			Hook:           hook,
			NegativePrompt: "blurry, low quality, watermark, logo, unreadable text, extra fingers, distorted face, bad anatomy, noisy background",
			Prompt:         composeCoverPrompt(baseHeadline, hook, defaultStyle[0], extraPrompt, "A", width, height),
			Width:          width,
			Height:         height,
			Seed:           rand.New(rand.NewSource(time.Now().UnixNano())).Int63(),
		},
		{
			ID:             "B",
			Label:          "B",
			Headline:       shortenHeadline(baseHeadline, 5),
			Hook:           hook,
			NegativePrompt: "blurry, low quality, watermark, logo, unreadable text, extra fingers, distorted face, bad anatomy, noisy background",
			Prompt:         composeCoverPrompt(baseHeadline, hook, defaultStyle[1], extraPrompt, "B", width, height),
			Width:          width,
			Height:         height,
			Seed:           rand.New(rand.NewSource(time.Now().Add(7 * time.Second).UnixNano())).Int63(),
		},
		{
			ID:             "C",
			Label:          "C",
			Headline:       shortenHeadline(baseHeadline, 4),
			Hook:           hook,
			NegativePrompt: "blurry, low quality, watermark, logo, unreadable text, extra fingers, distorted face, bad anatomy, noisy background",
			Prompt:         composeCoverPrompt(baseHeadline, hook, defaultStyle[2], extraPrompt, "C", width, height),
			Width:          width,
			Height:         height,
			Seed:           rand.New(rand.NewSource(time.Now().Add(14 * time.Second).UnixNano())).Int63(),
		},
	}

	if len(variants) > 3 {
		return variants[:3]
	}
	return variants
}

func composeCoverPrompt(headline, hook, style, extraPrompt, variantLabel string, width, height int) string {
	parts := []string{
		fmt.Sprintf("YouTube thumbnail variant %s", variantLabel),
		fmt.Sprintf("canvas %dx%d, 16:9 composition", width, height),
		fmt.Sprintf("main headline concept: %s", headline),
		fmt.Sprintf("clickable hook: %s", hook),
		style,
		"leave clear space for text overlay, strong subject separation, premium thumbnail quality",
	}
	if extraPrompt = strings.TrimSpace(extraPrompt); extraPrompt != "" {
		parts = append(parts, "extra direction: "+extraPrompt)
	}
	return strings.Join(parts, ", ")
}

func compactHeadline(text string) string {
	text = sanitizeCreativeText(text, false)
	if text == "" {
		return "High Impact Thumbnail"
	}
	words := strings.Fields(text)
	if len(words) <= 7 {
		return text
	}
	return strings.Join(words[:7], " ")
}

func compactHook(body, title string) string {
	body = sanitizeCreativeText(body, false)
	if body != "" {
		words := strings.Fields(body)
		if len(words) > 10 {
			words = words[:10]
		}
		if len(words) > 0 {
			return strings.Join(words, " ")
		}
	}
	return strings.TrimSpace(title)
}

func shortenHeadline(text string, maxWords int) string {
	words := strings.Fields(text)
	if len(words) <= maxWords {
		return text
	}
	return strings.Join(words[:maxWords], " ")
}

// =============================================================================
// NVIDIA image generation
// =============================================================================

func (h *YouTubeHandlers) generateNVIDIAThumbnail(ctx context.Context, prompt, negativePrompt string, width, height, steps int, seed int64) ([]byte, string, error) {
	if h.service.GetConfig().NVIDIAAPIKey == "" {
		return nil, "", fmt.Errorf("NVIDIA API key not configured")
	}

	payload := map[string]any{
		"prompt":          prompt,
		"negative_prompt": negativePrompt,
		"width":           width,
		"height":          height,
		"seed":            seed,
		"steps":           steps,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, "", fmt.Errorf("failed to marshal NVIDIA payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://ai.api.nvidia.com/v1/genai/black-forest-labs/flux.1-schnell", bytes.NewReader(body))
	if err != nil {
		return nil, "", fmt.Errorf("failed to create NVIDIA request: %w", err)
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", h.service.GetConfig().NVIDIAAPIKey))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	client := &http.Client{Timeout: 120 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("NVIDIA request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return nil, "", fmt.Errorf("NVIDIA returned %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		Artifacts []struct {
			Base64 string `json:"base64"`
		} `json:"artifacts"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, "", fmt.Errorf("failed to decode NVIDIA response: %w", err)
	}
	if len(result.Artifacts) == 0 || result.Artifacts[0].Base64 == "" {
		return nil, "", fmt.Errorf("no artifact returned by NVIDIA")
	}

	imageBytes, err := base64.StdEncoding.DecodeString(result.Artifacts[0].Base64)
	if err != nil {
		return nil, "", fmt.Errorf("failed to decode image payload: %w", err)
	}

	filename := h.buildCoverFilename()
	if err := h.writeCoverFile(filename, imageBytes); err != nil {
		return nil, "", err
	}
	return imageBytes, filename, nil
}

func (h *YouTubeHandlers) buildCoverFilename() string {
	ts := time.Now().Unix()
	token := rand.New(rand.NewSource(ts)).Int63()
	return fmt.Sprintf("yt_cover_%d_%d.png", ts, token%100000)
}

func (h *YouTubeHandlers) writeCoverFile(filename string, data []byte) error {
	dir := h.getCoverTempDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("failed to create cover dir: %w", err)
	}
	path := filepath.Join(dir, filename)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("failed to write cover file: %w", err)
	}
	return nil
}

func (h *YouTubeHandlers) getCoverTempDir() string {
	cfg := h.service.GetConfig()
	if cfg.DataDir != "" {
		return filepath.Join(cfg.DataDir, "youtube", "generated_covers")
	}
	return filepath.Join(os.TempDir(), "velox-youtube-covers")
}
