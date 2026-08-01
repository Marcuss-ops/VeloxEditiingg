// Package inputsecurity contains the single security boundary for remote and
// local media inputs accepted by the master.
package inputsecurity

import (
	"errors"
	"fmt"
	"strings"
)

// Kind identifies the semantic role of an input. It is deliberately closed so
// size, MIME and validation policy cannot be selected by an arbitrary client
// string without first passing through NormalizeKind.
type Kind string

const (
	KindUnknown   Kind = "unknown"
	KindClip      Kind = "clip"
	KindImage     Kind = "image"
	KindAudio     Kind = "audio"
	KindVoiceover Kind = "voiceover"
	KindSubtitle  Kind = "subtitle"
	KindManifest  Kind = "manifest"
	KindFont      Kind = "font"
	KindThumbnail Kind = "thumbnail"
)

func NormalizeKind(value string) Kind {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "clip", "video", "stock_clip", "video_clip":
		return KindClip
	case "image", "scene_image", "still":
		return KindImage
	case "audio", "music":
		return KindAudio
	case "voiceover", "voice_over", "narration":
		return KindVoiceover
	case "subtitle", "subtitles", "caption", "captions":
		return KindSubtitle
	case "manifest", "project_file", "project":
		return KindManifest
	case "font", "typeface":
		return KindFont
	case "thumbnail", "thumb":
		return KindThumbnail
	default:
		return KindUnknown
	}
}

// ErrorCode is stable API/metric classification. Error messages are for
// operators; callers must branch on Code instead of parsing text.
type ErrorCode string

const (
	ErrInvalidURL        ErrorCode = "INPUT_URL_INVALID"
	ErrUnsupportedScheme ErrorCode = "INPUT_SCHEME_UNSUPPORTED"
	ErrPrivateNetwork    ErrorCode = "INPUT_SSRF_BLOCKED"
	ErrDNSRebinding      ErrorCode = "INPUT_DNS_REBINDING_BLOCKED"
	ErrRedirectLimit     ErrorCode = "INPUT_REDIRECT_LIMIT"
	ErrDownloadTooLarge  ErrorCode = "INPUT_DOWNLOAD_TOO_LARGE"
	ErrDownloadTimeout   ErrorCode = "INPUT_DOWNLOAD_TIMEOUT"
	ErrHTTPStatus        ErrorCode = "INPUT_HTTP_STATUS"
	ErrMIMEMismatch      ErrorCode = "INPUT_MIME_MISMATCH"
	ErrMIMEUnsupported   ErrorCode = "INPUT_MIME_UNSUPPORTED"
	ErrHTMLPayload       ErrorCode = "INPUT_HTML_PAYLOAD"
	ErrMediaCorrupt      ErrorCode = "INPUT_MEDIA_CORRUPT"
	ErrProbeFailed       ErrorCode = "INPUT_FFPROBE_FAILED"
	ErrPathViolation     ErrorCode = "INPUT_PATH_VIOLATION"
	ErrEmptyFile         ErrorCode = "INPUT_EMPTY_FILE"
	ErrQuarantineFailed  ErrorCode = "INPUT_QUARANTINE_FAILED"
	ErrReadFailed        ErrorCode = "INPUT_READ_FAILED"
)

// SecurityError is the canonical error returned by this package.
type SecurityError struct {
	Code      ErrorCode
	Kind      Kind
	Message   string
	Retryable bool
	Cause     error
}

func (e *SecurityError) Error() string {
	if e == nil {
		return ""
	}
	if e.Message == "" {
		return string(e.Code)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *SecurityError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func newError(kind Kind, code ErrorCode, message string, cause error) *SecurityError {
	return &SecurityError{Code: code, Kind: kind, Message: message, Cause: cause}
}

// NewError lets adjacent ingestion boundaries preserve the same canonical
// error taxonomy when they enforce the byte limit before a remote fetcher is
// involved (for example multipart and local-file staging).
func NewError(kind Kind, code ErrorCode, message string, cause error) *SecurityError {
	return newError(kind, code, message, cause)
}

// CodeOf returns the stable security code for err, or an empty code when err
// was not emitted by the input security boundary.
func CodeOf(err error) ErrorCode {
	var securityErr *SecurityError
	if errors.As(err, &securityErr) && securityErr != nil {
		return securityErr.Code
	}
	return ""
}
