// Package processors provides image processing functionality for Dark Editor
package processors

import (
	"bytes"
	"fmt"
	"image"
	"image/jpeg"
	"image/png"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/chai2010/webp"
	"github.com/disintegration/imaging"
)

// ExportFormat defines the output format for image export
type ExportFormat string

const (
	FormatPNG  ExportFormat = "PNG"
	FormatJPEG ExportFormat = "JPEG"
	FormatJPG  ExportFormat = "JPG"
	FormatWebP ExportFormat = "WEBP"
)

// ExportOptions contains options for image export
type ExportOptions struct {
	Format  ExportFormat
	Quality int // JPEG quality (1-100), WebP quality (1-100)
}

// DefaultExportOptions returns default export options
func DefaultExportOptions() ExportOptions {
	return ExportOptions{
		Format:  FormatPNG,
		Quality: 90,
	}
}

// ExportImage exports an image to the specified format
func ExportImage(img image.Image, opts ExportOptions) ([]byte, error) {
	var buf bytes.Buffer

	switch opts.Format {
	case FormatJPEG, FormatJPG:
		quality := opts.Quality
		if quality < 1 || quality > 100 {
			quality = 90
		}
		if err := jpeg.Encode(&buf, img, &jpeg.Options{Quality: quality}); err != nil {
			return nil, fmt.Errorf("failed to encode JPEG: %w", err)
		}

	case FormatWebP:
		quality := opts.Quality
		if quality < 1 || quality > 100 {
			quality = 90
		}
		if err := webp.Encode(&buf, img, &webp.Options{Lossless: false, Quality: float32(quality)}); err != nil {
			return nil, fmt.Errorf("failed to encode WebP: %w", err)
		}

	case FormatPNG:
		fallthrough
	default:
		if err := png.Encode(&buf, img); err != nil {
			return nil, fmt.Errorf("failed to encode PNG: %w", err)
		}
	}

	return buf.Bytes(), nil
}

// ExportToFile exports an image to a file
func ExportToFile(img image.Image, outputPath string, opts ExportOptions) error {
	// Ensure directory exists
	dir := filepath.Dir(outputPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	data, err := ExportImage(img, opts)
	if err != nil {
		return err
	}

	if err := os.WriteFile(outputPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	return nil
}

// ExportWithFormat detects format from filename and exports
func ExportWithFormat(img image.Image, outputPath string, quality int) error {
	ext := strings.ToUpper(filepath.Ext(outputPath))
	ext = strings.TrimPrefix(ext, ".")

	format := ExportFormat(ext)
	if format == FormatJPG {
		format = FormatJPEG
	}

	opts := ExportOptions{
		Format:  format,
		Quality: quality,
	}

	return ExportToFile(img, outputPath, opts)
}

// GetFileExtension returns the file extension for a format
func GetFileExtension(format ExportFormat) string {
	switch format {
	case FormatJPEG, FormatJPG:
		return "jpg"
	case FormatWebP:
		return "webp"
	case FormatPNG:
		fallthrough
	default:
		return "png"
	}
}

// ParseFormat parses a format string
func ParseFormat(s string) ExportFormat {
	s = strings.ToUpper(strings.TrimSpace(s))
	switch s {
	case "JPEG", "JPG":
		return FormatJPEG
	case "WEBP":
		return FormatWebP
	case "PNG":
		return FormatPNG
	default:
		return FormatPNG
	}
}

// LoadImage loads an image from a file
func LoadImage(path string) (image.Image, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open image: %w", err)
	}
	defer file.Close()

	img, err := imaging.Decode(file, imaging.AutoOrientation(true))
	if err != nil {
		return nil, fmt.Errorf("failed to decode image: %w", err)
	}

	return img, nil
}

// LoadImageFromReader loads an image from an io.Reader
func LoadImageFromReader(r io.Reader) (image.Image, error) {
	img, err := imaging.Decode(r, imaging.AutoOrientation(true))
	if err != nil {
		return nil, fmt.Errorf("failed to decode image: %w", err)
	}
	return img, nil
}

// LoadImageFromBytes loads an image from a byte slice
func LoadImageFromBytes(data []byte) (image.Image, error) {
	return LoadImageFromReader(bytes.NewReader(data))
}

// SaveImage saves an image to a file (auto-detects format from extension)
func SaveImage(img image.Image, path string) error {
	return imaging.Save(img, path)
}

// GetImageInfo returns basic information about an image
type ImageInfo struct {
	Width  int
	Height int
	Format string
	Size   int64
}

// GetImageInfoFromFile returns basic information about an image file
func GetImageInfoFromFile(path string) (*ImageInfo, error) {
	// Get file size
	stat, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	// Load image to get dimensions
	img, err := LoadImage(path)
	if err != nil {
		return nil, err
	}

	bounds := img.Bounds()

	return &ImageInfo{
		Width:  bounds.Dx(),
		Height: bounds.Dy(),
		Format: strings.TrimPrefix(filepath.Ext(path), "."),
		Size:   stat.Size(),
	}, nil
}

// OptimizeForWeb optimizes an image for web delivery
// - Resizes if larger than maxWidth/maxHeight
// - Converts to WebP for better compression
func OptimizeForWeb(img image.Image, maxWidth, maxHeight int, quality int) ([]byte, error) {
	result := img

	// Resize if needed
	bounds := img.Bounds()
	width := bounds.Dx()
	height := bounds.Dy()

	if maxWidth > 0 && width > maxWidth {
		ratio := float64(maxWidth) / float64(width)
		newHeight := int(float64(height) * ratio)
		result = imaging.Resize(result, maxWidth, newHeight, imaging.Lanczos)
	}

	bounds = result.Bounds()
	height = bounds.Dy()
	if maxHeight > 0 && height > maxHeight {
		ratio := float64(maxHeight) / float64(height)
		newWidth := int(float64(bounds.Dx()) * ratio)
		result = imaging.Resize(result, newWidth, maxHeight, imaging.Lanczos)
	}

	// Export as WebP
	return ExportImage(result, ExportOptions{
		Format:  FormatWebP,
		Quality: quality,
	})
}
