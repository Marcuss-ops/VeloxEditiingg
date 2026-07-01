// Tests for OutputManifest computation (Phase 3.2 of the Artifact
// Commit Protocol). The fixture files are constructed from byte
// literals in t.TempDir() so no real ffmpeg / mp4 fixture is needed
// in the repo; ffprobe enrichment tests skip when the binary is
// missing on PATH.
package publisher

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// ────────────────────────────────────────────────────────────────────────
// SHA-256 + size streaming.
// ────────────────────────────────────────────────────────────────────────

// TestStreamSHAAndSize_Deterministic — known content (a sentence) must
// hash to the canonical SHA-256 of that sentence.
func TestStreamSHAAndSize_Deterministic(t *testing.T) {
	content := []byte("the quick brown fox jumps over the lazy dog\n")
	want := sha256.Sum256(content)
	wantHex := hex.EncodeToString(want[:])

	dir := t.TempDir()
	p := filepath.Join(dir, "out.txt")
	if err := os.WriteFile(p, content, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	got, n, err := ComputeAndStreamSHA256(bytes.NewReader(content))
	if err != nil {
		t.Fatalf("ComputeAndStreamSHA256 → %v", err)
	}
	if got != wantHex {
		t.Errorf("ComputeAndStreamSHA256 sha = %s; want %s", got, wantHex)
	}
	if n != int64(len(content)) {
		t.Errorf("ComputeAndStreamSHA256 bytes = %d; want %d", n, len(content))
	}

	// Sha256OfBytes sanity.
	if Sha256OfBytes(content) != wantHex {
		t.Errorf("Sha256OfBytes mismatch")
	}

	// End-to-end: ComputeLocalManifest must report the same hash AND
	// the same size for the same on-disk file. This is the drift-free
	// guarantee the wire-shape adapter relies on.
	st, _ := os.Stat(p)
	m, err := ComputeLocalManifest(context.Background(), p)
	if err != nil {
		t.Fatalf("ComputeLocalManifest → %v", err)
	}
	if m.SHA256Hex != wantHex {
		t.Errorf("ComputeLocalManifest sha = %s; want %s", m.SHA256Hex, wantHex)
	}
	if m.SizeBytes != st.Size() {
		t.Errorf("ComputeLocalManifest size = %d; want %d", m.SizeBytes, st.Size())
	}
}

// TestStreamSHAAndSize_LargeFile — 4 MiB random-ish content; we hash
// it as 4 MiB of "a" + newline and verify the streaming counter
// walks the whole file (not just the first chunk).
func TestStreamSHAAndSize_LargeFile(t *testing.T) {
	line := []byte("a\n")
	// ~4 MiB worth: use var because len(line) is not a constant
	// expression; the const evaluator rejects len() of a slice.
	N := (4 * 1024 * 1024) / len(line)
	var buf bytes.Buffer
	for i := 0; i < N; i++ {
		buf.Write(line)
	}
	content := buf.Bytes()
	want := sha256.Sum256(content)

	dir := t.TempDir()
	p := filepath.Join(dir, "big.txt")
	if err := os.WriteFile(p, content, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	m, err := ComputeLocalManifest(context.Background(), p)
	if err != nil {
		t.Fatalf("ComputeLocalManifest(big) → %v", err)
	}
	if m.SHA256Hex != hex.EncodeToString(want[:]) {
		t.Errorf("big sha mismatch")
	}
	if m.SizeBytes != int64(len(content)) {
		t.Errorf("big size = %d; want %d", m.SizeBytes, len(content))
	}
}

// ────────────────────────────────────────────────────────────────────────
// MIME detection + MP4 magic.
// ────────────────────────────────────────────────────────────────────────

func TestDetectMIMEAndMP4_DistinctFormats(t *testing.T) {
	dir := t.TempDir()

	// 1) Plain text → text/plain; charset=utf-8 (or text/plain).
	txtPath := filepath.Join(dir, "note.txt")
	if err := os.WriteFile(txtPath, []byte("hello world\n"), 0o644); err != nil {
		t.Fatalf("WriteFile txt: %v", err)
	}
	txtM, err := ComputeLocalManifest(context.Background(), txtPath)
	if err != nil {
		t.Fatalf("ComputeLocalManifest(txt) → %v", err)
	}
	if !strings.HasPrefix(txtM.MIMEType, "text/plain") {
		t.Errorf("txt MIME = %q; want text/plain prefix", txtM.MIMEType)
	}
	if txtM.Format != "" {
		t.Errorf("txt Format = %q; want empty (not mp4)", txtM.Format)
	}

	// 2) JSON — Go's http.DetectContentType is conservative and only
	// classifies the *strict* JSON shape (no values other than nested
	// objects/strings). A realistic payload like
	// `{"name":"example","version":"1.0"}` gets flagged
	// `text/plain; charset=utf-8`, NOT `application/json`. Either
	// outcome is acceptable: the manifest contract requires a
	// non-empty MIME guess, not a specific label.
	jsonPath := filepath.Join(dir, "thing.json")
	if err := os.WriteFile(jsonPath, []byte(`{"name":"example","version":"1.0"}`), 0o644); err != nil {
		t.Fatalf("WriteFile json: %v", err)
	}
	jsonM, err := ComputeLocalManifest(context.Background(), jsonPath)
	if err != nil {
		t.Fatalf("ComputeLocalManifest(json) → %v", err)
	}
	if !strings.HasPrefix(jsonM.MIMEType, "application/json") &&
		!strings.HasPrefix(jsonM.MIMEType, "text/plain") {
		t.Errorf("json MIME = %q; want application/json or text/plain prefix", jsonM.MIMEType)
	}

	// 3) PNG signature (8 bytes): 89 50 4E 47 0D 0A 1A 0A.
	pngPath := filepath.Join(dir, "img.png")
	pngSig := []byte{0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A}
	payload := append(pngSig, bytes.Repeat([]byte{0x00}, 32)...)
	if err := os.WriteFile(pngPath, payload, 0o644); err != nil {
		t.Fatalf("WriteFile png: %v", err)
	}
	pngM, err := ComputeLocalManifest(context.Background(), pngPath)
	if err != nil {
		t.Fatalf("ComputeLocalManifest(png) → %v", err)
	}
	if pngM.MIMEType != "image/png" {
		t.Errorf("png MIME = %q; want image/png", pngM.MIMEType)
	}
	if pngM.Format != "" {
		t.Errorf("png Format = %q; want empty", pngM.Format)
	}

	// 4) MP4 magic box: bytes 0..3 = box size (we use 32), bytes 4..7
	// = "ftyp", bytes 8..11 = major brand "isom".
	mp4Path := filepath.Join(dir, "movie.mp4")
	mp4Head := []byte{
		0x00, 0x00, 0x00, 0x20, // box size = 32
		'f', 't', 'y', 'p',
		'i', 's', 'o', 'm',
	}
	pad := bytes.Repeat([]byte{0x00}, 32)
	if err := os.WriteFile(mp4Path, append(mp4Head, pad...), 0o644); err != nil {
		t.Fatalf("WriteFile mp4: %v", err)
	}
	mp4M, err := ComputeLocalManifest(context.Background(), mp4Path)
	if err != nil {
		t.Fatalf("ComputeLocalManifest(mp4) → %v", err)
	}
	if mp4M.Format != "mp4" {
		t.Errorf("mp4 Format = %q; want mp4", mp4M.Format)
	}
	// MIME for MP4 is video/mp4 (Go's DetectContentType) — allow that
	// or octet-stream if the head is too short. Verifies that ffprobe
	// enrichment is the fallback when Go's sniffer is conservative
	// (Go only flags "video/mp4" when more of the ftyp box is
	// present). Either result is acceptable for the manifest contract
	// — the Format field carries the load-bearing info.
	if mp4M.MIMEType != "video/mp4" && mp4M.MIMEType != "application/octet-stream" {
		t.Errorf("mp4 MIME = %q; want video/mp4 or octet-stream", mp4M.MIMEType)
	}
}

func TestLooksLikeMP4_TruthTable(t *testing.T) {
	cases := []struct {
		name string
		head []byte
		want bool
	}{
		{"empty", []byte{}, false},
		{"too-short", []byte{1, 2, 3}, false},
		{"no-ftyp", append([]byte{0, 0, 0, 0x20}, []byte("XYZQ")...), false},
		{
			"ftyp-isom",
			[]byte{0, 0, 0, 0x20, 'f', 't', 'y', 'p', 'i', 's', 'o', 'm'},
			true,
		},
		{
			"ftyp-dash",
			[]byte{0, 0, 0, 0x20, 'f', 't', 'y', 'p', 'd', 'a', 's', 'h'},
			true,
		},
	}
	for _, c := range cases {
		if got := looksLikeMP4(c.head); got != c.want {
			t.Errorf("%s: looksLikeMP4 = %v; want %v", c.name, got, c.want)
		}
	}
}

// ────────────────────────────────────────────────────────────────────────
// ffprobe enrichment.
// ────────────────────────────────────────────────────────────────────────

// TestProbeMedia_MissingBinary_SoftFails — when ffprobe is missing,
// the function MUST return an error (so the manifest can populate
// FfprobeErr) but ComputeLocalManifest must NOT fail; the rest of
// the manifest stays valid.
func TestProbeMedia_MissingBinary_SoftFails(t *testing.T) {
	// Force PATH to a temp dir that has no binaries.
	origPath := os.Getenv("PATH")
	tmpBin := t.TempDir()
	t.Setenv("PATH", tmpBin)
	defer os.Setenv("PATH", origPath)

	if _, lookErr := exec.LookPath("ffprobe"); lookErr == nil {
		t.Skip("ffprobe found on PATH; this assertion only runs on hosts without it")
	}

	dir := t.TempDir()
	p := filepath.Join(dir, "any.txt")
	if err := os.WriteFile(p, []byte("hi"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	_, _, _, _, err := ProbeMedia(context.Background(), p)
	if err == nil {
		t.Fatalf("ProbeMedia: expected error on missing binary")
	}
	if !strings.Contains(err.Error(), "ffprobe") {
		t.Errorf("ProbeMedia error = %v; want to mention ffprobe", err)
	}

	// ComputeLocalManifest must still succeed and report no ffprobe.
	m, err := ComputeLocalManifest(context.Background(), p)
	if err != nil {
		t.Fatalf("ComputeLocalManifest must NOT fail on missing ffprobe: %v", err)
	}
	if m.FfprobeOK {
		t.Errorf("FfprobeOK = true; want false (binary missing)")
	}
	if m.FfprobeErr == "" {
		t.Errorf("FfprobeErr is empty; want a non-empty explanation")
	}
}

// TestProbeMedia_ParsesHandRolledJSON — exercises parseFfprobeJSON
// directly with a stub payload, no ffprobe runtime needed.
func TestProbeMedia_ParsesHandRolledJSON(t *testing.T) {
	payload := []byte(`{
		"streams": [
			{"codec_name":"h264","width":1920,"height":1080}
		],
		"format": {"duration":"42.5", "format_name":"mov,mp4,m4a,3gp,3g2,mj2"}
	}`)
	codec, dur, w, h := parseFfprobeJSON(payload)
	if codec != "h264" {
		t.Errorf("codec = %q; want h264", codec)
	}
	if w != 1920 || h != 1080 {
		t.Errorf("dims = %dx%d; want 1920x1080", w, h)
	}
	if dur != 42.5 {
		t.Errorf("duration = %v; want 42.5", dur)
	}
}

func TestProbeMedia_ParsesJSON_NoHit(t *testing.T) {
	codec, dur, w, h := parseFfprobeJSON([]byte(`{"streams":[{}]}`))
	if codec != "" || dur != 0 || w != 0 || h != 0 {
		t.Errorf("empty-streams parse: codec=%q dur=%v w=%d h=%d; all want zero", codec, dur, w, h)
	}
}

// ────────────────────────────────────────────────────────────────────────
// Misc helpers.
// ────────────────────────────────────────────────────────────────────────

func TestCountingWriter_Arithmetic(t *testing.T) {
	c := &countingWriter{}
	n, _ := c.Write([]byte("abc"))
	if n != 3 || c.n != 3 {
		t.Errorf("first write: n=%d counter=%d", n, c.n)
	}
	n, _ = c.Write([]byte(""))
	if n != 0 || c.n != 3 {
		t.Errorf("zero-byte write: n=%d counter=%d", n, c.n)
	}
	if cs := "abc"; cs != "abc" { // touch strings to satisfy unused import
		t.Errorf("unreachable")
	}
}

func TestCanonicalExtension_Normalization(t *testing.T) {
	cases := map[string]string{
		"/a/b/MOVIE.MP4": "mp4",
		"hello.json":     "json",
		"noext":          "",
		"/x/y/.hidden":   "hidden",
	}
	for in, want := range cases {
		if got := CanonicalExtension(in); got != want {
			t.Errorf("CanonicalExtension(%q) = %q; want %q", in, got, want)
		}
	}
}

func TestComputeLocalManifest_MissingFile_Errors(t *testing.T) {
	_, err := ComputeLocalManifest(context.Background(), filepath.Join(t.TempDir(), "nope.mp4"))
	if err == nil {
		t.Fatal("expected error on missing file")
	}
	if !strings.Contains(err.Error(), "missing") {
		t.Errorf("expected error to mention 'missing', got %v", err)
	}
}

func TestComputeLocalManifest_EmptyPath_Errors(t *testing.T) {
	_, err := ComputeLocalManifest(context.Background(), "")
	if err == nil {
		t.Fatal("expected error on empty path")
	}
}

func TestFirstJSONHelpers_RoundTrip(t *testing.T) {
	var b strings.Builder
	b.WriteString(`{"a":"hello","n":42,"f":3.14,"width":1280}`)
	s := b.String()
	if got := firstJSONString(s, `"a"`); got != "hello" {
		t.Errorf("firstJSONString = %q", got)
	}
	if got := firstJSONInt(s, `"n"`); got != 42 {
		t.Errorf("firstJSONInt = %d", got)
	}
	if got := firstJSONInt(s, `"width"`); got != 1280 {
		t.Errorf("firstJSONInt width = %d", got)
	}
	if got := firstJSONFloat(s, `"f"`); got != 3.14 {
		t.Errorf("firstJSONFloat = %v", got)
	}
	if got := firstJSONString(s, `"missing"`); got != "" {
		t.Errorf("firstJSONString miss = %q", got)
	}
}
