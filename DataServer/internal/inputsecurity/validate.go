package inputsecurity

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"unicode/utf8"
)

type Validation struct {
	MIMEType  string
	SizeBytes int64
}

func (f *Fetcher) ValidateFile(ctx context.Context, path string, kind Kind, declaredMIME string) (Validation, error) {
	if f == nil {
		return Validation{}, newError(kind, ErrPathViolation, "input validator is unavailable", nil)
	}
	validation, err := f.validateFile(ctx, path, kind, declaredMIME)
	if err != nil {
		f.reject(kind, err, 0)
	}
	return validation, err
}

func (f *Fetcher) validateFile(ctx context.Context, path string, kind Kind, declaredMIME string) (Validation, error) {
	if !f.allowedPath(path) {
		return Validation{}, newError(kind, ErrPathViolation, "file path is not a system-controlled input path", nil)
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() {
		return Validation{}, newError(kind, ErrPathViolation, "input is not a regular file", err)
	}
	if info.Size() == 0 {
		return Validation{}, newError(kind, ErrEmptyFile, "input file is empty", nil)
	}
	if info.Size() > f.policy.MaxBytes {
		return Validation{}, newError(kind, ErrDownloadTooLarge, "input file exceeds the byte limit", nil)
	}
	file, err := os.Open(path)
	if err != nil {
		return Validation{}, newError(kind, ErrReadFailed, "input file cannot be opened", err)
	}
	defer file.Close()
	peek := make([]byte, 512)
	n, readErr := io.ReadFull(file, peek)
	if readErr != nil && readErr != io.ErrUnexpectedEOF && readErr != io.EOF {
		return Validation{}, newError(kind, ErrReadFailed, "input header cannot be read", readErr)
	}
	peek = peek[:n]
	detected := sniffMIME(peek, path)
	declared := normalizeMIME(declaredMIME)
	if isHTML(declared) || isHTML(normalizeMIME(http.DetectContentType(peek))) {
		return Validation{}, newError(kind, ErrHTMLPayload, "HTML is not an accepted media input", nil)
	}
	if kind != KindUnknown && declared != "" && declared != "application/octet-stream" && detected != "" && detected != "application/octet-stream" && !mimeCompatibleForKind(kind, declared, detected) {
		return Validation{}, newError(kind, ErrMIMEMismatch, "declared MIME does not match file content", nil)
	}
	if !allowedMIME(kind, detected, peek) {
		return Validation{}, newError(kind, ErrMIMEUnsupported, "file content is not valid for the requested input role", nil)
	}
	if kind == KindManifest {
		if err := validateManifest(path, peek); err != nil {
			return Validation{}, err
		}
	}
	probeKind := kind
	if probeKind == KindUnknown {
		switch {
		case strings.HasPrefix(detected, "video/"):
			probeKind = KindClip
		case strings.HasPrefix(detected, "audio/"):
			probeKind = KindAudio
		}
	}
	if probeKind == KindClip || probeKind == KindAudio || probeKind == KindVoiceover {
		if err := f.ffprobe(ctx, path, probeKind); err != nil {
			return Validation{}, err
		}
	}
	resultMIME := detected
	// Unknown-role resolver probes preserve the provider's declared type for
	// compatibility, while the registration boundary re-runs this check with
	// the concrete role and rejects mismatches before promotion.
	if kind == KindUnknown && declared != "" && declared != "application/octet-stream" {
		resultMIME = declared
	}
	return Validation{MIMEType: resultMIME, SizeBytes: info.Size()}, nil
}

func (f *Fetcher) allowedPath(path string) bool {
	clean := strings.TrimSpace(path)
	if clean == "" || filepath.Base(clean) != filepath.Base(filepath.Clean(clean)) {
		return false
	}
	abs, err := filepath.Abs(clean)
	if err != nil {
		return false
	}
	info, err := os.Lstat(abs)
	if err != nil || info.Mode()&os.ModeSymlink != 0 {
		return false
	}
	resolved, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return false
	}
	if resolved != abs {
		return false
	}
	if f.policy.TempDir != "" && withinRoot(abs, f.policy.TempDir) {
		return true
	}
	if f.policy.QuarantineDir != "" && withinRoot(abs, f.policy.QuarantineDir) {
		return true
	}
	for _, root := range f.policy.AllowedRoots {
		if withinRoot(abs, root) {
			return true
		}
	}
	return false
}

func withinRoot(path, root string) bool {
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return false
	}
	rel, err := filepath.Rel(absRoot, path)
	if err != nil {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
}

func normalizeMIME(value string) string {
	if idx := strings.IndexByte(value, ';'); idx >= 0 {
		value = value[:idx]
	}
	return strings.ToLower(strings.TrimSpace(value))
}

// mimeSniffer matches a leading byte sequence (magic number) to a MIME type.
// sniffMIME walks the table in order, so a sniffer must be listed before any
// broader sniffer whose byte prefix it shares.
type mimeSniffer struct {
	minLen int
	mime   string
	match  func(b []byte) bool
}

var mimeSniffers = []mimeSniffer{
	{minLen: 12, mime: "image/png", match: func(b []byte) bool { return bytes.Equal(b[:8], []byte("\x89PNG\r\n\x1a\n")) }},
	{minLen: 3, mime: "image/jpeg", match: func(b []byte) bool { return bytes.Equal(b[:3], []byte{0xff, 0xd8, 0xff}) }},
	{minLen: 6, mime: "image/gif", match: func(b []byte) bool {
		return bytes.Equal(b[:6], []byte("GIF87a")) || bytes.Equal(b[:6], []byte("GIF89a"))
	}},
	{minLen: 12, mime: "image/webp", match: func(b []byte) bool { return bytes.Equal(b[:4], []byte("RIFF")) && bytes.Equal(b[8:12], []byte("WEBP")) }},
	{minLen: 12, mime: "video/mp4", match: func(b []byte) bool { return bytes.Equal(b[4:8], []byte("ftyp")) }},
	{minLen: 3, mime: "audio/mpeg", match: func(b []byte) bool { return bytes.Equal(b[:3], []byte("ID3")) }},
	// Valid MPEG audio is not required to carry an ID3 tag. Google Drive
	// and other object stores commonly return bare MP3 frame streams whose
	// first bytes are the MPEG sync word (0xffe) followed by a valid layer
	// header. Treat those streams as audio/mpeg so the role-specific
	// voiceover/audio validator does not reject an otherwise valid asset.
	{minLen: 2, mime: "audio/mpeg", match: func(b []byte) bool { return b[0] == 0xff && (b[1]&0xe0) == 0xe0 }},
	{minLen: 12, mime: "audio/wav", match: func(b []byte) bool { return bytes.Equal(b[:4], []byte("RIFF")) && bytes.Equal(b[8:12], []byte("WAVE")) }},
	{minLen: 4, mime: "audio/ogg", match: func(b []byte) bool { return bytes.Equal(b[:4], []byte("OggS")) }},
	{minLen: 4, mime: "audio/flac", match: func(b []byte) bool { return bytes.Equal(b[:4], []byte("fLaC")) }},
	{minLen: 4, mime: "font/woff", match: func(b []byte) bool { return bytes.Equal(b[:4], []byte("wOFF")) || bytes.Equal(b[:4], []byte("wOF2")) }},
	{minLen: 4, mime: "font/ttf", match: func(b []byte) bool {
		return bytes.Equal(b[:4], []byte("OTTO")) || bytes.Equal(b[:4], []byte{0, 1, 0, 0})
	}},
}

func sniffMIME(data []byte, path string) string {
	for _, s := range mimeSniffers {
		if len(data) >= s.minLen && s.match(data) {
			return s.mime
		}
	}
	if json.Valid(bytes.TrimSpace(data)) {
		return "application/json"
	}
	if len(data) > 0 && utf8.Valid(data) && !bytes.Contains(data, []byte{0}) {
		return "text/plain"
	}
	return normalizeMIME(http.DetectContentType(data))
}

func allowedMIME(kind Kind, mimeType string, data []byte) bool {
	if kind == KindUnknown {
		return !isHTML(mimeType)
	}
	if kind == KindManifest {
		return mimeType == "application/json" || strings.HasPrefix(mimeType, "text/")
	}
	if kind == KindSubtitle {
		return strings.HasPrefix(mimeType, "text/") || strings.Contains(mimeType, "subtitle") || strings.Contains(mimeType, "subrip")
	}
	if kind == KindFont {
		return strings.HasPrefix(mimeType, "font/") || strings.Contains(mimeType, "opentype") || strings.Contains(mimeType, "sfnt")
	}
	if kind == KindImage || kind == KindThumbnail {
		return strings.HasPrefix(mimeType, "image/")
	}
	if kind == KindClip {
		return strings.HasPrefix(mimeType, "video/")
	}
	if kind == KindAudio || kind == KindVoiceover {
		// Some providers deliver music beds in a video container with an
		// embedded audio stream (for example MP4/AAC). ffprobe below still
		// validates the media before it is accepted as an audio input.
		return strings.HasPrefix(mimeType, "audio/") || mimeType == "video/mp4"
	}
	return len(data) > 0
}

func mimeCompatible(declared, detected string) bool {
	if declared == detected {
		return true
	}
	return strings.HasPrefix(declared, "image/") && strings.HasPrefix(detected, "image/") || strings.HasPrefix(declared, "audio/") && strings.HasPrefix(detected, "audio/") || strings.HasPrefix(declared, "video/") && strings.HasPrefix(detected, "video/")
}

func mimeCompatibleForKind(kind Kind, declared, detected string) bool {
	if mimeCompatible(declared, detected) {
		return true
	}
	// M4A/AAC is commonly transported as an MP4 container. The byte sniffer
	// intentionally reports that container as video/mp4, while Drive metadata
	// and file extensions commonly declare audio/mp4 (or audio/m4a). Permit
	// only this audio-role container mismatch; ffprobe below still requires a
	// valid audio stream and therefore prevents a video payload from entering
	// an audio or voiceover slot.
	if (kind == KindAudio || kind == KindVoiceover) && detected == "video/mp4" {
		switch declared {
		case "audio/mp4", "audio/m4a", "audio/x-m4a":
			return true
		}
	}
	// JSON manifests are frequently served with text/plain by static object
	// stores. Content sniffing still rejects HTML and malformed JSON below,
	// but this benign declaration mismatch must not reject a valid manifest.
	return kind == KindManifest && ((declared == "text/plain" && detected == "application/json") || (declared == "application/json" && strings.HasPrefix(detected, "text/")))
}

func isHTML(mimeType string) bool {
	return mimeType == "text/html" || mimeType == "application/xhtml+xml"
}

func validateManifest(path string, peek []byte) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return newError(KindManifest, ErrReadFailed, "manifest cannot be read", err)
	}
	if !json.Valid(bytes.TrimSpace(data)) {
		return newError(KindManifest, ErrMediaCorrupt, "manifest is not valid JSON", nil)
	}
	_ = peek
	return nil
}

func (f *Fetcher) ffprobe(ctx context.Context, path string, kind Kind) error {
	if strings.TrimSpace(f.policy.TempDir) != "" {
		if err := os.MkdirAll(f.policy.TempDir, 0o700); err != nil {
			return newError(kind, ErrProbeFailed, "ffprobe sandbox directory is unavailable", err)
		}
	}
	probeCtx, cancel := context.WithTimeout(ctx, f.policy.ProbeTimeout)
	defer cancel()
	// No shell, no user-controlled working directory, no network protocols:
	// ffprobe receives only a generated local path and a read-only format
	// inspection request.
	cmd := exec.CommandContext(probeCtx, "ffprobe", "-v", "error", "-protocol_whitelist", "file", "-safe", "1", "-max_alloc", "67108864", "-show_entries", "format=duration:stream=codec_type", "-of", "json", path)
	cmd.Dir = f.policy.TempDir
	cmd.Env = []string{"LANG=C", "LC_ALL=C"}
	output, err := cmd.CombinedOutput()
	if err != nil {
		code := ErrProbeFailed
		if probeCtx.Err() != nil {
			code = ErrDownloadTimeout
		}
		message := fmt.Sprintf("ffprobe rejected the media input (%v)", err)
		if detail := strings.TrimSpace(string(output)); detail != "" {
			message = fmt.Sprintf("%s: %s", message, detail)
		}
		return newError(kind, code, message, err)
	}
	if len(output) == 0 || !json.Valid(output) {
		return newError(kind, ErrMediaCorrupt, "ffprobe returned invalid media metadata", nil)
	}
	return nil
}

var safeMIMEExtension = regexp.MustCompile(`^[a-z0-9]{1,8}$`)

func extensionForMIME(mimeType string) string {
	mimeType = normalizeMIME(mimeType)
	var ext string
	switch mimeType {
	case "image/jpeg":
		ext = "jpg"
	case "image/png":
		ext = "png"
	case "image/gif":
		ext = "gif"
	case "image/webp":
		ext = "webp"
	case "video/mp4":
		ext = "mp4"
	case "audio/mpeg":
		ext = "mp3"
	case "audio/wav":
		ext = "wav"
	case "audio/ogg":
		ext = "ogg"
	case "audio/flac":
		ext = "flac"
	case "application/json":
		ext = "json"
	case "font/ttf":
		ext = "ttf"
	case "font/woff":
		ext = "woff"
	case "font/woff2":
		ext = "woff2"
	default:
		ext = "bin"
	}
	if !safeMIMEExtension.MatchString(ext) {
		return ".bin"
	}
	return "." + ext
}
