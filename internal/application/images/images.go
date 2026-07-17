// Package images provides image-format detection and dimension-validation
// helpers for the Velox editor's application layer.
//
// Scope: small, dependency-free, contract surface for the editor's image
// renderer, the dark-editor thumbnail generator, and the worker bundle
// uploader. No I/O, no codec, no allocation beyond the Format / Dimension
// value types.
//
// Exported symbols (one canonical reference for every public name in this
// package; per-symbol godoc below carries the full contract):
//
//   - Format              enumerated image format (PNG / JPEG / GIF / WEBP /
//                           Unknown), iota-ordered. Stringer (`String()`
//                           returns the canonical lower-case name) and
//                           IsZero (`FormatUnknown` reports true). Zero
//                           value is FormatUnknown.
//
//   - Dimension           width × height pair for a decoded image (int
//                           fields, no validation — use Validate for
//                           positivity + bounds). String() renders the
//                           pair as "WxH" (e.g. "1280x720").
//
//   - DetectFormat        func DetectFormat(data []byte) Format.
//                           Inspects up to 12 leading bytes of data and
//                           returns the matching Format. Each branch
//                           matches the SHORTEST possible prefix (the
//                           residual payload is not validated; callers
//                           needing full parse use image.Decode):
//                             - 3-byte inputs matching 0xFF 0xD8 0xFF
//                               return FormatJPEG (lenient on exif-prefixed
//                               JPEGs; the JPEG magic-byte check runs FIRST).
//                             - <4-byte inputs that did NOT match the JPEG
//                               signature return FormatUnknown.
//                             - 4-byte inputs matching the PNG signature
//                               (0x89 'P' 'N' 'G') return FormatPNG.
//                             - 3-byte inputs matching the GIF literal
//                               ASCII prefix ('G' 'I' 'F') return
//                               FormatGIF (covers GIF87a and GIF89a).
//                             - 12-byte inputs matching "RIFF????WEBP"
//                               return FormatWEBP.
//                           Detection is intentionally permissive (magic-byte
//                           prefix only); for full parse use
//                           image.Decode from the standard library.
//
//   - Validate            func Validate(dim Dimension, maxWidth, maxHeight
//                           int) error. Returns nil iff dim is strictly
//                           positive AND (only when the corresponding bound
//                           is >0) within (maxWidth, maxHeight). A bound of
//                           0 means "no limit on that axis". On failure,
//                           returns a wrapped error naming the breached
//                           dimension and the bound that was violated.
//
//   - ErrUnsupportedFormat  sentinel error reserved for future codec paths
//                           that surface a typed error from the package
//                           (currently unused by DetectFormat directly —
//                           unknown formats return FormatUnknown).
//
//   - ErrEmptyBuffer      sentinel error reserved for callers that pass an
//                           empty buffer directly (DetectFormat currently
//                           returns FormatUnknown for empty input rather
//                           than this sentinel; the sentinel exists for
//                           API symmetry with future codecs).
//
// Build tag: the companion smoke_test.go in this directory exercises every
// exported symbol and is sized at 42,2–45 KB to act as the per-file
// size-policy benchmark artifact (see docs/CHANGELOG.md and the § 19
// tracker entry in docs/metrics/loc-refactor-history.md for the audit
// trail). The package itself is tag-free.

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

// ErrUnsupportedFormat is reserved for future codec paths that surface a
// typed error from the package. The current DetectFormat returns
// FormatUnknown for unsupported inputs rather than this sentinel; the
// sentinel exists for API symmetry with future codecs.
var ErrUnsupportedFormat = errors.New("images: unsupported format")

// ErrEmptyBuffer is reserved for callers that pass an empty or truncated
// buffer directly. The current DetectFormat returns FormatUnknown for such
// inputs rather than this sentinel; the sentinel exists for API symmetry
// with future codecs.
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
