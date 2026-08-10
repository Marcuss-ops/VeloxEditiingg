package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

// manifest_ffprobe.go owns the best-effort ffprobe enrichment of
// OutputManifest (ProbeMediaDetails + parseFfprobeJSON) and the legacy
// pure parsing helpers retained for compatibility. The manifest computation
// itself lives in manifest.go.

// ────────────────────────────────────────────────────────────────────────
// ffprobe enrichment (best-effort).
// ────────────────────────────────────────────────────────────────────────

// MediaProbe is the final-artifact media metadata used by the publisher.
// Unlike the old video-only probe, it inspects every stream so the report can
// prove that the delivered artifact really contains audio as well as video.
type MediaProbe struct {
	VideoCodec      string
	AudioCodec      string
	DurationSec     float64
	Width           int
	Height          int
	HasVideo        bool
	HasAudio        bool
	AudioTrackCount int
}

type ffprobeDocument struct {
	Streams []ffprobeStream `json:"streams"`
	Format  ffprobeFormat   `json:"format"`
}

type ffprobeStream struct {
	CodecType string `json:"codec_type"`
	CodecName string `json:"codec_name"`
	Width     int    `json:"width"`
	Height    int    `json:"height"`
	Duration  string `json:"duration"`
}

type ffprobeFormat struct {
	Duration string `json:"duration"`
}

// ProbeMediaDetails calls ffprobe if it is on PATH and parses every stream.
// Returns an error when ffprobe is missing, non-zero-exits, or emits invalid
// JSON. Callers decide whether missing media streams are a quality failure.
func ProbeMediaDetails(ctx context.Context, path string) (MediaProbe, error) {
	if _, lookErr := exec.LookPath("ffprobe"); lookErr != nil {
		return MediaProbe{}, fmt.Errorf("ffprobe missing on PATH")
	}

	args := []string{
		"-v", "error",
		"-print_format", "json",
		"-show_format",
		"-show_streams",
		path,
	}
	cmd := exec.CommandContext(ctx, "ffprobe", args...)
	out, runErr := cmd.Output()
	if runErr != nil {
		return MediaProbe{}, fmt.Errorf("ffprobe exec: %w", runErr)
	}
	probe, err := parseFfprobeDetails(out)
	if err != nil {
		return MediaProbe{}, err
	}
	if !probe.HasVideo && !probe.HasAudio {
		return MediaProbe{}, fmt.Errorf("ffprobe returned no media streams")
	}
	return probe, nil
}

// ProbeMedia keeps the original API for callers that only need video fields.
func ProbeMedia(ctx context.Context, path string) (codec string, durationSec float64, width, height int, err error) {
	probe, err := ProbeMediaDetails(ctx, path)
	if err != nil {
		return "", 0, 0, 0, err
	}
	return probe.VideoCodec, probe.DurationSec, probe.Width, probe.Height, nil
}

func parseFfprobeDetails(b []byte) (MediaProbe, error) {
	var doc ffprobeDocument
	if err := json.Unmarshal(b, &doc); err != nil {
		return MediaProbe{}, fmt.Errorf("ffprobe JSON parse: %w", err)
	}
	probe := MediaProbe{}
	for _, stream := range doc.Streams {
		switch stream.CodecType {
		case "video":
			probe.HasVideo = true
			if probe.VideoCodec == "" {
				probe.VideoCodec = stream.CodecName
				probe.Width = stream.Width
				probe.Height = stream.Height
			}
		case "audio":
			probe.HasAudio = true
			probe.AudioTrackCount++
			if probe.AudioCodec == "" {
				probe.AudioCodec = stream.CodecName
			}
		}
		if probe.DurationSec == 0 && stream.Duration != "" {
			probe.DurationSec, _ = strconv.ParseFloat(stream.Duration, 64)
		}
	}
	if probe.DurationSec == 0 && doc.Format.Duration != "" {
		probe.DurationSec, _ = strconv.ParseFloat(doc.Format.Duration, 64)
	}
	return probe, nil
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
