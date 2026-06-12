package darkeditor

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"velox-server/internal/handlers/server/darkeditor/processors"
)

// UploadImage handles image upload
func (h *Handler) UploadImage(c *gin.Context) {
	file, header, err := c.Request.FormFile("file")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "No file provided"})
		return
	}
	defer file.Close()

	contentType := header.Header.Get("Content-Type")
	if !strings.HasPrefix(contentType, "image/") {
		c.JSON(http.StatusBadRequest, gin.H{"error": "File must be an image"})
		return
	}

	ext := strings.TrimPrefix(filepath.Ext(header.Filename), ".")
	if ext == "" {
		ext = "png"
	}

	filename := h.getUniqueFilename(ext)

	if err := h.ensureDir(h.cfg.TempDir); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create temp directory"})
		return
	}

	dstPath := h.getTempPath(filename)
	dst, err := os.Create(dstPath)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create file"})
		return
	}
	defer dst.Close()

	if _, err := io.Copy(dst, file); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save file"})
		return
	}

	c.JSON(http.StatusOK, UploadResponse{
		Filename: filename,
		URL:      fmt.Sprintf("temp/%s", filename),
	})
}

// ApplyFilter applies a filter to an image
func (h *Handler) ApplyFilter(c *gin.Context) {
	var req FilterRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	inputPath := h.getTempPath(req.Filename)
	if _, err := os.Stat(inputPath); os.IsNotExist(err) {
		c.JSON(http.StatusNotFound, gin.H{"error": "Image not found"})
		return
	}

	img, err := processors.LoadImage(inputPath)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to load image: %v", err)})
		return
	}

	filterOpts := processors.FilterOptions{
		Type:  processors.FilterType(strings.ToLower(req.FilterType)),
		Value: req.Value,
	}

	if strings.ToLower(req.FilterType) == "blur" {
		filterOpts.Radius = req.Value
	}

	processedImg := processors.ApplyFilter(img, filterOpts)

	ext := strings.TrimPrefix(filepath.Ext(req.Filename), ".")
	if ext == "" {
		ext = "png"
	}
	newFilename := h.getUniqueFilename(ext)
	outputPath := h.getTempPath(newFilename)

	if err := processors.SaveImage(processedImg, outputPath); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to save processed image: %v", err)})
		return
	}

	c.JSON(http.StatusOK, FilterResponse{
		Filename: newFilename,
		URL:      fmt.Sprintf("temp/%s", newFilename),
	})
}

// TransformImage handles crop and resize operations
func (h *Handler) TransformImage(c *gin.Context) {
	var req TransformRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	inputPath := h.getTempPath(req.Filename)
	if _, err := os.Stat(inputPath); os.IsNotExist(err) {
		c.JSON(http.StatusNotFound, gin.H{"error": "Image not found"})
		return
	}

	img, err := processors.LoadImage(inputPath)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to load image: %v", err)})
		return
	}

	transformOpts := processors.TransformOptions{
		CropBox:             req.CropBox,
		ResizeDims:          req.ResizeDims,
		MaintainAspectRatio: true,
	}

	processedImg := processors.Transform(img, transformOpts)

	ext := strings.TrimPrefix(filepath.Ext(req.Filename), ".")
	if ext == "" {
		ext = "png"
	}
	newFilename := h.getUniqueFilename(ext)
	outputPath := h.getTempPath(newFilename)

	if err := processors.SaveImage(processedImg, outputPath); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to save processed image: %v", err)})
		return
	}

	c.JSON(http.StatusOK, FilterResponse{
		Filename: newFilename,
		URL:      fmt.Sprintf("temp/%s", newFilename),
	})
}

// ExportImage exports an image in the specified format
func (h *Handler) ExportImage(c *gin.Context) {
	var req ExportRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	inputPath := h.getTempPath(req.Filename)
	if _, err := os.Stat(inputPath); os.IsNotExist(err) {
		c.JSON(http.StatusNotFound, gin.H{"error": "Image not found"})
		return
	}

	img, err := processors.LoadImage(inputPath)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to load image: %v", err)})
		return
	}

	format := processors.ParseFormat(req.Format)
	ext := processors.GetFileExtension(format)

	quality := req.Quality
	if quality <= 0 || quality > 100 {
		quality = 90
	}

	exportOpts := processors.ExportOptions{
		Format:  format,
		Quality: quality,
	}

	newFilename := h.getUniqueFilename(ext)
	outputPath := h.getTempPath(newFilename)

	if err := processors.ExportToFile(img, outputPath, exportOpts); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to export image: %v", err)})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"url":      fmt.Sprintf("temp/%s", newFilename),
		"filename": newFilename,
	})
}

// GenerateImage generates an image using NVIDIA FLUX API
func (h *Handler) GenerateImage(c *gin.Context) {
	var req GenerateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	if h.cfg.NVIDIAAPIKey == "" {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "NVIDIA API Key not configured"})
		return
	}

	if req.Width == 0 {
		req.Width = 1024
	}
	if req.Height == 0 {
		req.Height = 1024
	}
	if req.Steps == 0 {
		req.Steps = 4
	}

	payload := map[string]interface{}{
		"prompt": req.Prompt,
		"width":  req.Width,
		"height": req.Height,
		"seed":   req.Seed,
		"steps":  req.Steps,
	}

	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to prepare request"})
		return
	}

	reqHTTP, err := http.NewRequest("POST", "https://ai.api.nvidia.com/v1/genai/black-forest-labs/flux.1-schnell", bytes.NewBuffer(jsonPayload))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create request"})
		return
	}

	reqHTTP.Header.Set("Authorization", fmt.Sprintf("Bearer %s", h.cfg.NVIDIAAPIKey))
	reqHTTP.Header.Set("Content-Type", "application/json")
	reqHTTP.Header.Set("Accept", "application/json")

	client := &http.Client{Timeout: 120 * time.Second}
	resp, err := client.Do(reqHTTP)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("NVIDIA API error: %v", err)})
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("NVIDIA API returned %d: %s", resp.StatusCode, string(body))})
		return
	}

	var result struct {
		Artifacts []struct {
			Base64 string `json:"base64"`
		} `json:"artifacts"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to parse NVIDIA response"})
		return
	}

	if len(result.Artifacts) == 0 {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "No image generated"})
		return
	}

	imgData, err := base64.StdEncoding.DecodeString(result.Artifacts[0].Base64)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to decode image"})
		return
	}

	filename := h.getUniqueFilename("png")
	outputPath := h.getTempPath(filename)

	if err := os.WriteFile(outputPath, imgData, 0644); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save image"})
		return
	}

	c.JSON(http.StatusOK, GenerateResponse{
		Filename: filename,
		URL:      fmt.Sprintf("temp/%s", filename),
		Prompt:   req.Prompt,
	})
}

// UpscaleImage upscales an image using Real-ESRGAN or fallback method
func (h *Handler) UpscaleImage(c *gin.Context) {
	var req UpscaleRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	inputPath := h.getTempPath(req.Filename)
	if _, err := os.Stat(inputPath); os.IsNotExist(err) {
		if filepath.IsAbs(req.Filename) {
			inputPath = req.Filename
			if _, err := os.Stat(inputPath); os.IsNotExist(err) {
				c.JSON(http.StatusNotFound, gin.H{"error": "Input file not found"})
				return
			}
		} else {
			c.JSON(http.StatusNotFound, gin.H{"error": "Input file not found"})
			return
		}
	}

	if req.Scale == 0 {
		req.Scale = 2
	}

	img, err := processors.LoadImage(inputPath)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to load image: %v", err)})
		return
	}

	ext := strings.TrimPrefix(filepath.Ext(req.Filename), ".")
	if ext == "" {
		ext = "png"
	}
	newFilename := h.getUniqueFilename(ext)
	outputPath := h.getTempPath(newFilename)

	upscaleOpts := processors.UpscaleOptions{
		Scale:       req.Scale,
		SaveInPlace: req.SaveInPlace,
	}

	result, err := processors.Upscale(img, upscaleOpts, outputPath)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to upscale image: %v", err)})
		return
	}

	if !processors.IsRealESRGANAvailable() {
		h.logger.Log(LogLevelWarn, "Real-ESRGAN not available, used Lanczos upscaling fallback", nil, "server")
	}

	c.JSON(http.StatusOK, UpscaleResponse{
		Filename: newFilename,
		URL:      fmt.Sprintf("temp/%s", newFilename),
		SavedAt:  result.OutputPath,
	})
}
