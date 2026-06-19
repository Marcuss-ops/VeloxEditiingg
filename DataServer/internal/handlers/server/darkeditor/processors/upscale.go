// Package processors provides image processing functionality for Dark Editor
package processors

import (
	"bytes"
	"fmt"
	"image"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/disintegration/imaging"

	"velox-server/internal/logging"
)

// Package-level structured logger for upscaling operations.
var upgradeLog = logging.NewLogger("darkeditor.upscale")

// Reference upgradeLog so it stays in the file even if individual callsites are later removed.
var _ = upgradeLog

// UpscaleOptions contains options for image upscaling
type UpscaleOptions struct {
	Scale       int  // Scale factor (2 or 4)
	SaveInPlace bool // Whether to save in place
}

// UpscaleResult contains the result of upscaling
type UpscaleResult struct {
	Image      image.Image
	OutputPath string
	Width      int
	Height     int
}

// RealESRGANPath is the path to the Real-ESRGAN binary
var RealESRGANPath = "realesrgan-ncnn-vulkan"

// SetRealESRGANPath sets the path to the Real-ESRGAN binary
func SetRealESRGANPath(path string) {
	RealESRGANPath = path
}

// Upscale upscales an image using Real-ESRGAN or falls back to simple scaling
func Upscale(img image.Image, opts UpscaleOptions, outputPath string) (*UpscaleResult, error) {
	if opts.Scale == 0 {
		opts.Scale = 2
	}
	if opts.Scale != 2 && opts.Scale != 4 {
		opts.Scale = 2
	}

	// Try Real-ESRGAN first
	result, err := upscaleWithRealESRGAN(img, opts, outputPath)
	if err == nil {
		return result, nil
	}

	// Fallback to simple upscaling with imaging
	upgradeLog.WarnWithMsg("darkeditor_upscale_fallback",
		"Real-ESRGAN unavailable, falling back to imaging.Lanczos",
		map[string]interface{}{
			"fallback": "imaging.Lanczos",
			"err":      err.Error(),
		})
	return upscaleWithImaging(img, opts, outputPath)
}

// upscaleWithRealESRGAN attempts to use Real-ESRGAN binary
func upscaleWithRealESRGAN(img image.Image, opts UpscaleOptions, outputPath string) (*UpscaleResult, error) {
	// Check if Real-ESRGAN is available
	if _, err := exec.LookPath(RealESRGANPath); err != nil {
		return nil, fmt.Errorf("Real-ESRGAN binary not found: %w", err)
	}

	// Create temp directory for processing
	tempDir, err := os.MkdirTemp("", "esrgan-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp directory: %w", err)
	}
	defer os.RemoveAll(tempDir)

	// Save input image to temp file
	inputPath := filepath.Join(tempDir, "input.png")
	if saveErr := imaging.Save(img, inputPath); saveErr != nil {
		return nil, fmt.Errorf("failed to save input image: %w", saveErr)
	}

	// Determine model based on scale
	model := "realesrgan-x4plus"
	if opts.Scale == 2 {
		model = "realesrgan-x2plus"
	}

	// Build command
	// realesrgan-ncnn-vulkan -i input.png -o output.png -n model
	args := []string{
		"-i", inputPath,
		"-o", outputPath,
		"-n", model,
	}

	cmd := exec.Command(RealESRGANPath, args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("Real-ESRGAN failed: %w, stderr: %s", err, stderr.String())
	}

	// Load the result
	resultImg, err := LoadImage(outputPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load upscaled image: %w", err)
	}

	bounds := resultImg.Bounds()
	return &UpscaleResult{
		Image:      resultImg,
		OutputPath: outputPath,
		Width:      bounds.Dx(),
		Height:     bounds.Dy(),
	}, nil
}

// upscaleWithImaging uses simple bilinear upscaling as fallback
func upscaleWithImaging(img image.Image, opts UpscaleOptions, outputPath string) (*UpscaleResult, error) {
	bounds := img.Bounds()
	newWidth := bounds.Dx() * opts.Scale
	newHeight := bounds.Dy() * opts.Scale

	// Use Lanczos resampling for better quality
	upscaled := imaging.Resize(img, newWidth, newHeight, imaging.Lanczos)

	// Save to output path
	if outputPath != "" {
		if saveErr := imaging.Save(upscaled, outputPath); saveErr != nil {
			return nil, fmt.Errorf("failed to save upscaled image: %w", saveErr)
		}
	}

	return &UpscaleResult{
		Image:      upscaled,
		OutputPath: outputPath,
		Width:      newWidth,
		Height:     newHeight,
	}, nil
}

// IsRealESRGANAvailable checks if Real-ESRGAN is available on the system
func IsRealESRGANAvailable() bool {
	_, err := exec.LookPath(RealESRGANPath)
	return err == nil
}

// UpscaleFromFile upscales an image file
func UpscaleFromFile(inputPath string, opts UpscaleOptions, outputPath string) (*UpscaleResult, error) {
	img, err := LoadImage(inputPath)
	if err != nil {
		return nil, err
	}

	// If output path not specified, generate one
	if outputPath == "" {
		ext := filepath.Ext(inputPath)
		base := strings.TrimSuffix(filepath.Base(inputPath), ext)
		dir := filepath.Dir(inputPath)
		outputPath = filepath.Join(dir, fmt.Sprintf("%s_%dx%s", base, opts.Scale, ext))
	}

	return Upscale(img, opts, outputPath)
}

// SmartUpscale intelligently chooses the best upscaling method
// based on image size and available tools
func SmartUpscale(img image.Image, targetWidth, targetHeight int, outputPath string) (*UpscaleResult, error) {
	bounds := img.Bounds()
	currentWidth := bounds.Dx()
	currentHeight := bounds.Dy()

	// Calculate scale factor
	scaleX := float64(targetWidth) / float64(currentWidth)
	scaleY := float64(targetHeight) / float64(currentHeight)
	scale := scaleX
	if scaleY > scaleX {
		scale = scaleY
	}

	// Round to nearest integer scale
	intScale := int(scale + 0.5)
	if intScale < 2 {
		intScale = 2
	}
	if intScale > 4 {
		intScale = 4
	}

	opts := UpscaleOptions{
		Scale: intScale,
	}

	result, err := Upscale(img, opts, outputPath)
	if err != nil {
		return nil, err
	}

	// If the result is larger than target, resize down
	if result.Width > targetWidth || result.Height > targetHeight {
		finalImg := imaging.Resize(result.Image, targetWidth, targetHeight, imaging.Lanczos)
		bounds = finalImg.Bounds()

	if outputPath != "" {
		if saveErr := imaging.Save(finalImg, outputPath); saveErr != nil {
			return nil, fmt.Errorf("failed to save final image: %w", saveErr)
		}
	}

		return &UpscaleResult{
			Image:      finalImg,
			OutputPath: outputPath,
			Width:      bounds.Dx(),
			Height:     bounds.Dy(),
		}, nil
	}

	return result, nil
}
