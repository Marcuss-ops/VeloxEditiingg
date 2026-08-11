package ffmpegrunner

import (
	"reflect"
	"strings"
	"testing"
)

func TestFingerprint_StableForIdenticalInvocation(t *testing.T) {
	req := FFmpegRequest{
		Operation: OperationCompose,
		Args:      []string{"-i", "/var/cache/worker/a.mp4", "-c:v", "libx264", "-y", "/tmp/out.mp4"},
	}
	first := Fingerprint(req)
	second := Fingerprint(req)
	if first == "" {
		t.Fatal("fingerprint must not be empty")
	}
	if first != second {
		t.Errorf("fingerprint not stable: %q != %q", first, second)
	}
	if len(first) != 64 {
		t.Errorf("fingerprint length = %d, want 64 (SHA-256 hex)", len(first))
	}
}

func TestFingerprint_ChangesWhenAnyArgumentChanges(t *testing.T) {
	base := FFmpegRequest{Operation: OperationEncode, Args: []string{"-i", "a.mp4", "-c:v", "libx264"}}
	other := FFmpegRequest{Operation: OperationEncode, Args: []string{"-i", "b.mp4", "-c:v", "libx264"}}
	if Fingerprint(base) == Fingerprint(other) {
		t.Error("fingerprint must change when an argument changes")
	}
	if Fingerprint(FFmpegRequest{Operation: OperationAudioMix, Args: []string{"-i", "a.mp4"}}) ==
		Fingerprint(FFmpegRequest{Operation: OperationCompose, Args: []string{"-i", "a.mp4"}}) {
		t.Error("fingerprint must distinguish operation types")
	}
}

func TestFingerprint_NeverLeaksRawArguments(t *testing.T) {
	secret := "/secrets/bearer-token-abc12345"
	req := FFmpegRequest{Operation: OperationEncode, Args: []string{"-i", secret, "-y", "/tmp/out.mp4"}}
	fp := Fingerprint(req)
	for _, chunk := range []string{secret, "bearer-token-abc12345", "/tmp/out.mp4"} {
		if strings.Contains(fp, chunk) {
			t.Errorf("fingerprint leaks raw argument %q", chunk)
		}
	}
}

func TestSanitize_ExtractsCodecsFiltersInputs(t *testing.T) {
	req := FFmpegRequest{
		Operation: OperationAudioMix,
		Args: []string{
			"-i", "/var/cache/worker/voice.wav",
			"-i", "/var/cache/worker/music.m4a",
			"-filter_complex", "[0:a]volume=1.0[a0];[1:a]volume=0.5[a1];[a0][a1]amix=inputs=2:duration=longest[aout]",
			"-map", "[aout]",
			"-c:a", "pcm_s16le",
			"-y", "/tmp/audio.wav",
		},
	}
	got := Sanitize(req)
	want := SanitizedParameters{
		Codecs:     []string{"pcm_s16le"},
		Filters:    []string{"amix", "volume"},
		InputCount: 2,
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Sanitize = %+v, want %+v", got, want)
	}
}

func TestSanitize_StripsLabelsAndPathValuedOptions(t *testing.T) {
	// ass= with an absolute path must yield only the filter name, never
	// the path. Labels [v0], [a0] must not appear.
	req := FFmpegRequest{
		Operation: OperationEncode,
		Args: []string{
			"-i", "/private/cache/worker/video.mp4",
			"-vf", "ass=/home/worker/.velox/cache/secret-sub.ass",
			"-c:v", "libx264",
			"-c:a", "aac",
			"-y", "/tmp/out.mp4",
		},
	}
	got := Sanitize(req)
	if len(got.Filters) != 1 || got.Filters[0] != "ass" {
		t.Errorf("Filters = %v, want [ass]", got.Filters)
	}
	if got.InputCount != 1 {
		t.Errorf("InputCount = %d, want 1", got.InputCount)
	}
	// No path or raw argument may survive in the sanitized projection.
	if !reflect.DeepEqual(got.Codecs, []string{"aac", "libx264"}) {
		t.Errorf("Codecs = %v, want [aac libx264]", got.Codecs)
	}
}

func TestSanitize_DeduplicatesAndSorts(t *testing.T) {
	req := FFmpegRequest{
		Operation: OperationCompose,
		Args: []string{
			"-i", "a.mp4", "-c:v", "libx264",
			"-i", "b.mp4", "-c:v", "libx264", "-c:a", "aac",
			"-filter_complex", "[0:v]scale=1920:1080[v0];[1:v]scale=1920:1080[v1];[v0][v1]concat=n=2:v=1:a=0[vout]",
		},
	}
	got := Sanitize(req)
	if !reflect.DeepEqual(got.Codecs, []string{"aac", "libx264"}) {
		t.Errorf("Codecs = %v, want deduplicated+sorted [aac libx264]", got.Codecs)
	}
	if !reflect.DeepEqual(got.Filters, []string{"concat", "scale"}) {
		t.Errorf("Filters = %v, want deduplicated+sorted [concat scale]", got.Filters)
	}
	if got.InputCount != 2 {
		t.Errorf("InputCount = %d, want 2", got.InputCount)
	}
}

func TestSanitize_EmptyArgs(t *testing.T) {
	got := Sanitize(FFmpegRequest{Operation: OperationCompose})
	if got.InputCount != 0 || len(got.Codecs) != 0 || len(got.Filters) != 0 {
		t.Errorf("Sanitize(empty) = %+v, want zero-value", got)
	}
}
