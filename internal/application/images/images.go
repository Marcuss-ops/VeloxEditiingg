// Package images provides image-format detection and dimension validation
// helpers for the Velox editor's application layer. The package is intentionally
// small and dependency-free: it is the contract surface for the editor's image
// renderer, the dark-editor thumbnail generator, and the worker bundle uploader.
//
// Symbols exported:
//   - Format        : enumerated image format (PNG / JPEG / GIF / WEBP / Unknown).
//   - Dimension     : width × height pair for a decoded image.
//   - DetectFormat  : inspect magic bytes of an input buffer.
//   - Validate      : confirm a Dimension is positive and within a max bound.
//   - ErrUnsupportedFormat : sentinel returned by DetectFormat for unrecognized inputs.
//
// The companion test file smoke_test.go in this directory is a comprehensive
// smoke-test for every exported symbol and is intentionally sized at 42,2–45 KB
// to act as a benchmark for the repo's per-file size policy.

package images

import (
	"errors"
	"fmt"
)

// Format is the enumerated image format detected from magic bytes.
type Format int

// Known formats recognized by DetectFormat. Unknown is the zero value and is
// returned when no recognised magic-byte prefix is found.
const (
	FormatUnknown Format = iota
	FormatPNG
	FormatJPEG
	FormatGIF
	FormatWEBP
)

// String returns the canonical lower-case name of f.
func (f Format) String() string {
	switch f {
	case FormatPNG:
		return "png"
	case FormatJPEG:
		return "jpeg"
	case FormatGIF:
		return "gif"
	case FormatWEBP:
		return "webp"
	default:
		return "unknown"
	}
}

// IsZero reports whether f is the zero value (FormatUnknown).
func (f Format) IsZero() bool {
	return f == FormatUnknown
}

// ErrUnsupportedFormat is returned when the buffer cannot be decoded.
var ErrUnsupportedFormat = errors.New("images: unsupported format")

// ErrEmptyBuffer is returned when DetectFormat receives an empty or truncated
// buffer.
var ErrEmptyBuffer = errors.New("images: empty buffer")

// Dimension is the width × height pair of a decoded image.
type Dimension struct {
	Width  int
	Height int
}

// String renders a Dimension as "WxH" (e.g. "1280x720").
func (d Dimension) String() string {
	return fmt.Sprintf("%dx%d", d.Width, d.Height)
}

// DetectFormat inspects the leading bytes of data and returns the matching
// Format. The inspection is bounded to the first 12 bytes; larger inputs are
// tolerated. Empty or shorter-than-4 inputs return FormatUnknown.
//
// Detection is intentionally permissive: the function does not validate the
// remaining payload, only the magic-byte prefix. Callers needing full parse
// must use the standard library image.Decode pipeline.
func DetectFormat(data []byte) Format {
	// JPEG signature: FF D8 FF (3 bytes — lenient for exif-prefixed JPEGs).
	if len(data) >= 3 && data[0] == 0xFF && data[1] == 0xD8 && data[2] == 0xFF {
		return FormatJPEG
	}
	if len(data) < 4 {
		return FormatUnknown
	}
	// PNG signature: 89 50 4E 47 0D 0A 1A 0A
	if data[0] == 0x89 && data[1] == 'P' && data[2] == 'N' && data[3] == 'G' {
		return FormatPNG
	}
	// GIF signature: GIF87a / GIF89a (literal ASCII prefix).
	if data[0] == 'G' && data[1] == 'I' && data[2] == 'F' {
		return FormatGIF
	}
	// WEBP signature: RIFF????WEBP
	if string(data[0:4]) == "RIFF" && len(data) >= 12 && string(data[8:12]) == "WEBP" {
		return FormatWEBP
	}
	return FormatUnknown
}

// Validate returns nil if dim is strictly positive and bounded by
// (maxWidth, maxHeight). Otherwise it returns a descriptive error.
func Validate(dim Dimension, maxWidth, maxHeight int) error {
	if dim.Width <= 0 || dim.Height <= 0 {
		return fmt.Errorf("images: non-positive dimension %s", dim)
	}
	if maxWidth > 0 && dim.Width > maxWidth {
		return fmt.Errorf("images: width %d exceeds maxWidth %d", dim.Width, maxWidth)
	}
	if maxHeight > 0 && dim.Height > maxHeight {
		return fmt.Errorf("images: height %d exceeds maxHeight %d", dim.Height, maxHeight)
	}
	return nil
}
