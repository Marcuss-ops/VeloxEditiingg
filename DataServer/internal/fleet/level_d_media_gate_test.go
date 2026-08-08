package fleet

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// fakeFFprobe writes a deterministic ffprobe stub that emits the given JSON
// for any input path, so the media-gate logic can be tested without real
// media tools. The stub is a real executable; FFprobeArtifactVerifier is
// constructed with FFprobeBin pointed at it.
func fakeFFprobe(t *testing.T, jsonOut string) *FFprobeArtifactVerifier {
	t.Helper()
	dir := t.TempDir()
	bin := filepath.Join(dir, "ffprobe")
	script := "#!/usr/bin/env bash\ncat <<'EOF'\n" + jsonOut + "\nEOF\n"
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake ffprobe: %v", err)
	}
	return &FFprobeArtifactVerifier{FFprobeBin: bin}
}

// nonEmptyMedia creates a non-empty file that satisfies the stat/size gate;
// media semantics come from the fake ffprobe JSON.
func nonEmptyMedia(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "artifact.mp4")
	if err := os.WriteFile(path, []byte("not-a-real-media-file-but-non-empty"), 0o644); err != nil {
		t.Fatalf("write artifact: %v", err)
	}
	return path
}

func hashOf(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read artifact: %v", err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func TestFFprobeArtifactVerifier_RejectsVideoOnlyArtifact(t *testing.T) {
	path := nonEmptyMedia(t)
	verifier := fakeFFprobe(t, `{"streams":[{"codec_type":"video","codec_name":"h264"}],"format":{"duration":"1.0"}}`)
	_, err := verifier.VerifyArtifact(context.Background(), path, 0)
	if err == nil {
		t.Fatal("video-only artifact was accepted; audio_track_count=0 must be FAIL")
	}
	if !strings.Contains(err.Error(), "audio_track_count=0") {
		t.Fatalf("error=%q, want audio_track_count=0 diagnostic", err)
	}
}

func TestFFprobeArtifactVerifier_RejectsMissingVideoTrack(t *testing.T) {
	path := nonEmptyMedia(t)
	verifier := fakeFFprobe(t, `{"streams":[{"codec_type":"audio","codec_name":"aac"}],"format":{"duration":"1.0"}}`)
	_, err := verifier.VerifyArtifact(context.Background(), path, 0)
	if err == nil {
		t.Fatal("audio-only artifact was accepted; video_track_count must be >= 1")
	}
	if !strings.Contains(err.Error(), "video_track_count=0") {
		t.Fatalf("error=%q, want video_track_count=0 diagnostic", err)
	}
}

func TestFFprobeArtifactVerifier_RejectsNonAACAudio(t *testing.T) {
	path := nonEmptyMedia(t)
	verifier := fakeFFprobe(t, `{"streams":[{"codec_type":"video","codec_name":"h264"},{"codec_type":"audio","codec_name":"mp3"}],"format":{"duration":"1.0"}}`)
	_, err := verifier.VerifyArtifact(context.Background(), path, 0)
	if err == nil {
		t.Fatal("artifact with non-AAC audio was accepted; audio codec must be aac")
	}
	if !strings.Contains(err.Error(), `audio_codec="mp3"`) {
		t.Fatalf("error=%q, want audio_codec diagnostic", err)
	}
}

func TestFFprobeArtifactVerifier_RejectsZeroDuration(t *testing.T) {
	path := nonEmptyMedia(t)
	verifier := fakeFFprobe(t, `{"streams":[{"codec_type":"video","codec_name":"h264"},{"codec_type":"audio","codec_name":"aac"}],"format":{"duration":"0"}}`)
	_, err := verifier.VerifyArtifact(context.Background(), path, 0)
	if err == nil {
		t.Fatal("zero-duration artifact was accepted")
	}
	if !strings.Contains(err.Error(), "duration=") {
		t.Fatalf("error=%q, want duration diagnostic", err)
	}
}

func TestFFprobeArtifactVerifier_AcceptsVideoPlusAAC(t *testing.T) {
	path := nonEmptyMedia(t)
	verifier := fakeFFprobe(t, `{"streams":[{"codec_type":"video","codec_name":"h264"},{"codec_type":"audio","codec_name":"aac"}],"format":{"duration":"1.5"}}`)
	got, err := verifier.VerifyArtifact(context.Background(), path, 0)
	if err != nil {
		t.Fatalf("video+aac artifact rejected: %v", err)
	}
	if want := hashOf(t, path); got != want {
		t.Fatalf("sha256=%s want=%s", got, want)
	}
}

func TestFFprobeArtifactVerifier_MediaGateExposesCounts(t *testing.T) {
	path := nonEmptyMedia(t)
	verifier := fakeFFprobe(t, `{"streams":[{"codec_type":"video","codec_name":"h264"},{"codec_type":"audio","codec_name":"aac"},{"codec_type":"audio","codec_name":"aac"}],"format":{"duration":"2.0"}}`)
	gate, err := verifier.VerifyMedia(context.Background(), path, 0)
	if err != nil {
		t.Fatalf("VerifyMedia: %v", err)
	}
	if gate.VideoTrackCount != 1 || gate.AudioTrackCount != 2 {
		t.Fatalf("gate counts video=%d audio=%d, want 1/2", gate.VideoTrackCount, gate.AudioTrackCount)
	}
	if gate.AudioCodec != "aac" || gate.DurationSec != 2.0 || gate.ArtifactBytes <= 0 || len(gate.SHA256Hex) != 64 {
		t.Fatalf("gate fields not fully populated: %+v", gate)
	}
}

func TestFFprobeArtifactVerifier_FFprobeMissingFailsClosed(t *testing.T) {
	path := nonEmptyMedia(t)
	verifier := &FFprobeArtifactVerifier{FFprobeBin: filepath.Join(t.TempDir(), "definitely-not-ffprobe")}
	_, err := verifier.VerifyArtifact(context.Background(), path, 0)
	if err == nil {
		t.Fatal("missing ffprobe binary was tolerated; verifier must fail closed")
	}
}

func TestFFprobeArtifactVerifier_RejectsAnyNonAACAudioStream(t *testing.T) {
	// [aac, opus] — the AAC gate applies to EVERY audio stream, so a
	// single non-AAC track fails even when an AAC track is present.
	path := nonEmptyMedia(t)
	verifier := fakeFFprobe(t, `{"streams":[{"codec_type":"video","codec_name":"h264"},{"codec_type":"audio","codec_name":"aac"},{"codec_type":"audio","codec_name":"opus"}],"format":{"duration":"1.0"}}`)
	_, err := verifier.VerifyArtifact(context.Background(), path, 0)
	if err == nil {
		t.Fatal("artifact with a non-AAC audio stream alongside AAC was accepted")
	}
	if !strings.Contains(err.Error(), "audio_codec") {
		t.Fatalf("error=%q, want audio_codec diagnostic", err)
	}
}

func TestFFprobeArtifactVerifier_RejectsGarbageFFprobeJSON(t *testing.T) {
	path := nonEmptyMedia(t)
	verifier := fakeFFprobe(t, "this is not json at all")
	_, err := verifier.VerifyArtifact(context.Background(), path, 0)
	if err == nil {
		t.Fatal("unparseable ffprobe output was tolerated; verifier must fail closed")
	}
	if !strings.Contains(err.Error(), "JSON parse") {
		t.Fatalf("error=%q, want JSON parse diagnostic", err)
	}
}
