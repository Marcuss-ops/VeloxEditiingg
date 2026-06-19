//go:build windows

// Package processors provides image processing functionality for Dark Editor.
//
// Windows stub. The real implementation lives in export.go (gated
// behind //go:build !windows) because `github.com/chai2010/webp` is a
// CGo-backed encoder that the Windows gcc toolchain cannot link
// without extra C dependencies. Every function in this stub mirrors
// the signature of its non-Windows counterpart. Webp paths return
// ErrUnsupportedOnWindows; PNG/JPEG paths dispatch through the pure-Go
// stdlib encoders so callers that don't ask for webp still get a real
// (non-stubbed) export path on Windows.
package processors

import (
	"bytes"
	"errors"
	"image"
	"image/jpeg"
	"image/png"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// ErrUnsupportedOnWindows is returned by every webp path on the
// Windows stub. PNG/JPEG paths use the pure-Go stdlib encoders.
var ErrUnsupportedOnWindows = errors.New("processors: webp export not available on Windows; use a Linux worker for WebP encoding")

// ExportFormat defines the output format for image export (same
// shape as the non-Windows package; constants only).
type ExportFormat string

const (
	FormatPNG  ExportFormat = "PNG"
	FormatJPEG ExportFormat = "JPEG"
	FormatJPG  ExportFormat = "JPG"
	FormatWebP ExportFormat = "WEBP"
)

// ExportOptions mirrors the non-Windows struct verbatim.
type ExportOptions struct {
	Format  ExportFormat
	Quality int
}

// DefaultExportOptions returns default export options.
func DefaultExportOptions() ExportOptions {
	return ExportOptions{Format: FormatPNG, Quality: 90}
}

// ExportImage is the Windows stub: webp explicitly errors, but PNG and
// JPEG are dispatchable via the pure-Go stdlib encoders so callers
// that don't ask for webp get a real export path on Windows.
func ExportImage(img image.Image, opts ExportOptions) ([]byte, error) {
	var buf bytes.Buffer
	quality := opts.Quality
	if quality < 1 || quality > 100 {
		quality = 90
	}
	switch opts.Format {
	case FormatJPEG, FormatJPG:
		if err := jpeg.Encode(&buf, img, &jpeg.Options{Quality: quality}); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	case FormatPNG:
		if err := png.Encode(&buf, img); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	case FormatWebP:
		fallthrough
	default:
		return nil, ErrUnsupportedOnWindows
	}
}

// ExportToFile writes the encoded image to disk.
func ExportToFile(img image.Image, outputPath string, opts ExportOptions) error {
	if dir := filepath.Dir(outputPath); dir != "" {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}
	data, err := ExportImage(img, opts)
	if err != nil {
		return err
	}
	return os.WriteFile(outputPath, data, 0644)
}

// ExportWithFormat detects format from filename and exports.
func ExportWithFormat(img image.Image, outputPath string, quality int) error {
	ext := strings.ToUpper(strings.TrimPrefix(filepath.Ext(outputPath), "."))
	format := ParseFormat(ext)
	if format == FormatJPG {
		format = FormatJPEG
	}
	opts := ExportOptions{Format: format, Quality: quality}
	return ExportToFile(img, outputPath, opts)
}

// GetFileExtension returns the file extension for a format. Pure
// string logic, no webp dependency.
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

// ParseFormat parses a format string. Pure string logic.
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

// LoadImage reads PNG/JPEG from disk via the stdlib decoders.
func LoadImage(path string) (image.Image, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return LoadImageFromReader(file)
}

// LoadImageFromReader reads PNG/JPEG from a stream via stdlib decoders.
func LoadImageFromReader(r io.Reader) (image.Image, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	if img, err := png.Decode(bytes.NewReader(data)); err == nil {
		return img, nil
	}
	if img, err := jpeg.Decode(bytes.NewReader(data)); err == nil {
		return img, nil
	}
	return nil, ErrUnsupportedOnWindows
}

// LoadImageFromBytes reads PNG/JPEG from a byte slice via stdlib decoders.
func LoadImageFromBytes(data []byte) (image.Image, error) {
	return LoadImageFromReader(bytes.NewReader(data))
}

// SaveImage writes an image to a file. Disables webp save (returns
// ErrUnsupportedOnWindows). PNG / JPEG paths dispatch through
// ExportToFile so non-webp callers get a real write.
func SaveImage(img image.Image, path string) error {
	lower := strings.ToLower(path)
	switch {
	case strings.HasSuffix(lower, ".webp"):
		return ErrUnsupportedOnWindows
	case strings.HasSuffix(lower, ".jpg"), strings.HasSuffix(lower, ".jpeg"):
		return ExportToFile(img, path, ExportOptions{Format: FormatJPEG, Quality: 90})
	default:
		return ExportToFile(img, path, ExportOptions{Format: FormatPNG, Quality: 90})
	}
}

// ImageInfo mirrors the non-Windows struct.
type ImageInfo struct {
	Width  int
	Height int
	Format string
	Size   int64
}

// GetImageInfoFromFile returns dimensions + format + size from disk.
func GetImageInfoFromFile(path string) (*ImageInfo, error) {
	stat, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
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

// OptimizeForWeb: webp export is unavailable here so dispatch through
// the stdlib PNG path as a non-webp fallback.
func OptimizeForWeb(img image.Image, maxWidth, maxHeight int, quality int) ([]byte, error) {
	_ = maxWidth
	_ = maxHeight
	return ExportImage(img, ExportOptions{Format: FormatPNG, Quality: quality})
}
