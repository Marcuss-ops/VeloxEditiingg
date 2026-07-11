// Package publisher is the worker-side output of the Artifact Commit
// Protocol (Phase 3.2 of docs/completion-protocol.md).
//
// The Manifest object emitted here is the "local" representation of
// what the encoder just produced: byte count + SHA-256 over the
// on-disk file, MIME guess from the file head, MP4 magic detection,
// and optional ffprobe enrichment (duration, dimensions, codec).
// The supervisor hands this object to the spool (RENDERING →
// OUTPUT_READY) and the publisher wraps a "manifest → DeclareOutputs
// payload" adapter so the master sees the canonical DataServer/
// internal/completion/types.go::OutputManifest wire shape.
//
// All operations are streaming-friendly so very large outputs do not
// allocate in memory. SHA-256 is computed via io.MultiWriter into a
// sha256.Digest + a counting writer; size is therefore the same int64
// that gets baked into the spool row, eliminating off-by-one drift
// between disk size and declared size.
package publisher

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

// OutputManifest is the worker-side representation of an encoder
// output. Field set matches the spec: SHA-256, byte size, MIME, raw
// format name, plus optional ffprobe enrichment.
//
// The master wire shape (DataServer/internal/completion/types.go::
// OutputManifest) overlaps but is not identical: the wire form
// demands OutputKind, LogicalName, WorkerSpoolKey, and drops
// dimensions. Mapping is the publisher's job — see AdaptToWireManifest
// in a follow-up Phase 3.3 PR.
type OutputManifest struct {
	// LocalPath is the source file path the manifest was computed for.
	LocalPath string
	// SizeBytes is the on-disk byte count, AND the count of bytes that
	// flowed through the SHA-256 streamer (drift-free guarantee).
	SizeBytes int64
	// SHA256Hex is the lowercase hex SHA-256 of the file content.
	SHA256Hex string
	// MIMEType is the best-effort sniff from http.DetectContentType on
	// the first 512 bytes. Empty if the head did not match a known
	// signature.
	MIMEType string
	// Format is the raw container string the sniff narrows in on:
	// "mp4" iff the ftyp box is present, else "" (we do not guess).
	Format string
	// Ffprobe populated iff ffprobe was on PATH and returned parseable
	// JSON. If ffprobe is missing the fields stay zero-valued and
	// FfprobeErr is set so the supervisor can decide to skip the
	// wire-shape adapter instead of crashing.
	Codec       string
	DurationSec float64
	Width       int
	Height      int
	FfprobeOK   bool
	FfprobeErr  string
}

// Sentinel errors.
var (
	ErrFileMissing = errors.New("publisher: file missing")
	ErrMIMEUnknown = errors.New("publisher: MIME unknown for head bytes")
)

// ────────────────────────────────────────────────────────────────────────
// Top-level computation.
// ────────────────────────────────────────────────────────────────────────

// ComputeLocalManifest reads path once, streams through the SHA
// hasher + byte counter, then enriches with sniff + optional
// ffprobe. Returns a fully-populated manifest.
//
// The function never buffers the whole file in memory; large outputs
// stay streaming even on 4 GB files.
func ComputeLocalManifest(ctx context.Context, path string) (*OutputManifest, error) {
	if path == "" {
		return nil, fmt.Errorf("%w: path is empty", ErrFileMissing)
	}
	st, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: %s", ErrFileMissing, path)
		}
		return nil, fmt.Errorf("publisher.ComputeLocalManifest: stat: %w", err)
	}
	m := &OutputManifest{LocalPath: path, SizeBytes: st.Size()}

	if err := streamSHAAndSize(path, m); err != nil {
		return nil, err
	}

	// mimeSniffLen is the head-buffer size http.DetectContentType needs
	// for a confident guess. Go's stdlib does not expose this constant;
	// 512 is the de-facto public value.
	const mimeSniffLen = 512

	// Sniff uses the first 512B we already hashed (truncated to that
	// window). We reread a small head segment to keep this function
	// the single entry point; the heavy byte-budget above stays
	// single-pass.
	head, err := readHead(path, mimeSniffLen)
	if err != nil {
		return nil, fmt.Errorf("publisher.ComputeLocalManifest: readHead: %w", err)
	}
	m.MIMEType = http.DetectContentType(head)
	if m.MIMEType == "" || m.MIMEType == "application/octet-stream" {
		// Treat "unknown" as a real signal so callers can decide:
		// ambiguous heads are not an error here (the file is still
		// hashable), but we don't want a default of "" silently
		// passed downstream. We mark Format empty and stop here for
		// MIME logic, then run ffprobe enrichment which may give us
		// a better name anyway.
		m.MIMEType = "application/octet-stream"
	}
	if looksLikeMP4(head) {
		m.Format = "mp4"
	}

	// ffprobe enrichment is best-effort; missing binary must not
	// fail the manifest computation.
	codec, dur, w, h, perr := ProbeMedia(ctx, path)
	if perr != nil {
		m.FfprobeErr = perr.Error()
	} else {
		m.Codec = codec
		m.DurationSec = dur
		m.Width = w
		m.Height = h
		m.FfprobeOK = true
	}
	return m, nil
}

// ────────────────────────────────────────────────────────────────────────
// Streaming SHA-256 + size.
// ────────────────────────────────────────────────────────────────────────

// streamSHAAndSize copies path through a hash.Hash + a counting writer
// so SizeBytes is guaranteed equal to the bytes that contributed to
// the hash (no drift between stat.Size() and the declared size).
func streamSHAAndSize(path string, m *OutputManifest) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("publisher.streamSHAAndSize: open: %w", err)
	}
	defer f.Close()

	sha := sha256.New()
	cw := &countingWriter{}

	// 1 MiB buffer; well below the kernel page cache, well above the
	// syscall overhead for huge files.
	buf := make([]byte, 1<<20)
	if _, err := io.CopyBuffer(io.MultiWriter(sha, cw), f, buf); err != nil {
		return fmt.Errorf("publisher.streamSHAAndSize: copy: %w", err)
	}

	digest := sha.Sum(nil)
	m.SHA256Hex = hex.EncodeToString(digest)
	// Reconcile size against the streaming counter (Stat can lie on
	// sparse files; the streamer is the ground truth).
	m.SizeBytes = cw.n
	return nil
}

type countingWriter struct{ n int64 }

func (c *countingWriter) Write(p []byte) (int, error) {
	c.n += int64(len(p))
	return len(p), nil
}

// ComputeAndStreamSHA256 is the exported helper other packages can
// reuse (e.g. the bootstrap selftest baseline fixture hashes) —
// signature stays io.Reader → (hex, n, error) so callers don't need
// to know about the spool or the on-disk path.
func ComputeAndStreamSHA256(r io.Reader) (string, int64, error) {
	if r == nil {
		return "", 0, fmt.Errorf("publisher.ComputeAndStreamSHA256: nil reader")
	}
	sha := sha256.New()
	cw := &countingWriter{}
	if _, err := io.Copy(io.MultiWriter(sha, cw), r); err != nil {
		return "", 0, fmt.Errorf("publisher.ComputeAndStreamSHA256: copy: %w", err)
	}
	return hex.EncodeToString(sha.Sum(nil)), cw.n, nil
}

// Sha256OfBytes is the small-input helper (manifests, key files) that
// doesn't need streaming.
func Sha256OfBytes(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// resetHash is an internal helper so tests can plug in a custom hash
// factory; kept unexported so the public surface stays minimal.
func resetHash() hash.Hash { return sha256.New() }

// ────────────────────────────────────────────────────────────────────────
// MIME + MP4 detection.
// ────────────────────────────────────────────────────────────────────────

// looksLikeMP4 returns true when head contains the ISO BMFF ftyp box
// signature at offset 4 (i.e. "....ftyp...."). The brand strings
// after "ftyp" vary ("isom", "mp42", "qt  ", "dash", "avc1", …);
// the spec mandates only the ftyp presence check.
func looksLikeMP4(head []byte) bool {
	if len(head) < 12 {
		return false
	}
	// bytes 4..7 spell "ftyp"
	return bytes.Equal(head[4:8], []byte{'f', 't', 'y', 'p'})
}

// DetectMIMEFromHead is the public re-entry point for callers that
// already have a buffered head.
func DetectMIMEFromHead(head []byte) string {
	return http.DetectContentType(head)
}

// ────────────────────────────────────────────────────────────────────────
// ffprobe enrichment (best-effort).
// ────────────────────────────────────────────────────────────────────────

// ProbeMedia calls ffprobe if it is on PATH and parses codec /
// duration / dimensions. Returns an error when ffprobe is missing
// or non-zero-exits; callers should treat that as a soft signal
// (the rest of the manifest is still valid).
func ProbeMedia(ctx context.Context, path string) (codec string, durationSec float64, width, height int, err error) {
	if _, lookErr := exec.LookPath("ffprobe"); lookErr != nil {
		return "", 0, 0, 0, fmt.Errorf("ffprobe missing on PATH")
	}

	// -show_streams narrow to the first video stream; -show_format
	// gives us the container duration in seconds.
	args := []string{
		"-v", "error",
		"-print_format", "json",
		"-show_format",
		"-show_streams",
		"-select_streams", "v:0",
		path,
	}
	cmd := exec.CommandContext(ctx, "ffprobe", args...)
	out, runErr := cmd.Output()
	if runErr != nil {
		return "", 0, 0, 0, fmt.Errorf("ffprobe exec: %w", runErr)
	}
	codec, durationSec, width, height = parseFfprobeJSON(out)
	if codec == "" && durationSec == 0 && width == 0 && height == 0 {
		return "", 0, 0, 0, fmt.Errorf("ffprobe returned no usable fields")
	}
	return codec, durationSec, width, height, nil
}

// parseFfprobeJSON tolerates minimal JSON shapes so the test fixtures
// can pass hand-rolled stubs (no real ffprobe involved). We deliberately
// avoid pulling in encoding/json into the hot path of the worker
// (yet) — instead we pull the few keys we care from the streams[0]
// and format blocks with a tiny scanner. This keeps the function
// pure-Go and easy to unit test.
//
// Real ffprobe output puts "duration" inside BOTH streams[0] (per-stream
// PTS) and format (container-level wall clock). streams[0] comes
// first in document order; we MUST read duration from the format
// block, not the first occurrence.
func parseFfprobeJSON(b []byte) (codec string, durationSec float64, width, height int) {
	s := string(b)
	// codec_name / width / height live under streams[0].*; the same
	// keys do not appear inside the format block, so a naive
	// first-match lookup is correct for these three.
	codec = firstJSONString(s, `"codec_name"`)
	width = firstJSONInt(s, `"width"`)
	height = firstJSONInt(s, `"height"`)
	// duration MUST come from the format block; firstJSONFloat on
	// the whole document would land on streams[0].duration which is
	// per-frame PTS for many codecs (often wrong).
	if formatBlock := extractJSONObject(s, `"format":`); formatBlock != "" {
		durationSec = firstJSONFloat(formatBlock, `"duration"`)
	}
	return
}

// extractJSONObject returns the substring of s starting at the JSON
// value following prefix (e.g. `"format":`) and ending at the matching
// closing brace. Tolerant of nested braces one level deep (the
// streams[i] objects) — sufficient for ffprobe's flat shape.
//
// Returns "" if prefix is missing or the structure is malformed.
func extractJSONObject(s, prefix string) string {
	idx := strings.Index(s, prefix)
	if idx < 0 {
		return ""
	}
	rest := s[idx+len(prefix):]
	rest = strings.TrimLeft(rest, " \t\n")
	if !strings.HasPrefix(rest, "{") {
		return ""
	}
	// Walk forward, tracking brace depth. Stream objects live INSIDE
	// the outer format block in some ffprobe outputs — though the
	// canonical shape is streams[] outside format{}. One level of
	// nesting tolerance is enough.
	depth := 0
	for i, c := range rest {
		switch c {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return rest[:i+1]
			}
		}
	}
	return ""
}

// ────────────────────────────────────────────────────────────────────────
// Tiny JSON helpers (extracted because we want zero allocation
// pressure on the worker hot path; encoding/json.Unmarshal is fine
// for tests but overkill for four fields).
// ────────────────────────────────────────────────────────────────────────

// firstJSONString returns the first primitive string value following
// key in the JSON document. Returns "" if no match.
func firstJSONString(s, key string) string {
	idx := strings.Index(s, key)
	if idx < 0 {
		return ""
	}
	rest := s[idx+len(key):]
	// skip whitespace and the colon / "if object"
	rest = strings.TrimLeft(rest, " \t:")
	// expect a quoted string
	if !strings.HasPrefix(rest, `"`) {
		return ""
	}
	rest = rest[1:]
	end := strings.IndexByte(rest, '"')
	if end < 0 {
		return ""
	}
	return rest[:end]
}

// firstJSONInt returns the first primitive numeric value following
// key. Returns 0 if not found / not parseable.
func firstJSONInt(s, key string) int {
	idx := strings.Index(s, key)
	if idx < 0 {
		return 0
	}
	rest := s[idx+len(key):]
	rest = strings.TrimLeft(rest, " \t,:")
	v, err := strconv.Atoi(firstNumber(rest))
	if err != nil {
		return 0
	}
	return v
}

// firstJSONFloat returns the first primitive numeric value following
// key. Returns 0 if not found / not parseable.
func firstJSONFloat(s, key string) float64 {
	idx := strings.Index(s, key)
	if idx < 0 {
		return 0
	}
	rest := s[idx+len(key):]
	rest = strings.TrimLeft(rest, " \t,:")
	v, err := strconv.ParseFloat(firstNumber(rest), 64)
	if err != nil {
		return 0
	}
	return v
}

// firstNumber returns the longest leading run of numeric chars. The
// first rune may be a leading sign (-/+) or a JSON string-opening
// quote (`"`) — the quote is silently skipped so the value following
// a JSON key can be read directly:
//
//	firstJSONFloat(s, `"duration"`) → rest is `:"42.5", ...`
//	firstNumber on that → "42.5"
//
// (the leading `"` is dropped; the digits and the dot survive).
func firstNumber(s string) string {
	var b strings.Builder
	for i, r := range s {
		if i == 0 {
			if r == '-' || r == '+' {
				b.WriteRune(r)
				continue
			}
			if r == '"' {
				// skip the JSON string-opening quote
				continue
			}
		}
		if (r >= '0' && r <= '9') || r == '.' || r == 'e' || r == 'E' {
			b.WriteRune(r)
			continue
		}
		break
	}
	return b.String()
}

// ────────────────────────────────────────────────────────────────────────
// Misc helpers.
// ────────────────────────────────────────────────────────────────────────

// readHead reads up to n bytes of the file. The caller is expected to
// use a sane cap (mimeSniffLen = 512).
func readHead(path string, n int) ([]byte, error) {
	if n <= 0 {
		return nil, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	buf := make([]byte, n)
	got, err := io.ReadFull(f, buf)
	if err != nil {
		if err == io.ErrUnexpectedEOF || err == io.EOF {
			return buf[:got], nil
		}
		return nil, err
	}
	return buf, nil
}

// CanonicalExtension returns the lower-cased extension without the
// leading dot, or "" if none present. Convenience helper for callers
// that want a wire-field-format hint without trusting MIME.
func CanonicalExtension(path string) string {
	ext := filepath.Ext(path)
	if ext == "" {
		return ""
	}
	return strings.ToLower(ext[1:])
}
