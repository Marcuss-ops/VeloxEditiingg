package fleet

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

// BackendArtifactVerifier validates the rendered artifact before the smoke
// pipeline records success or uploads it. The verifier runs against the path
// returned by the worker adapter, so both local and SSH-backed smoke paths
// share the same media/hash gate.
type BackendArtifactVerifier interface {
	VerifyArtifact(ctx context.Context, path string, expectedBytes int64) (sha256Hex string, err error)
}

// MediaGate is the canonical Level-D smoke acceptance contract. The update
// pipeline (and ResumeExecutor) treat every gate as mandatory: an artifact
// that fails any gate is a FAIL — never a SUCCEEDED with a silent hole
// (e.g. audio_track_count=0 must be FAIL, per the rollout spec).
type MediaGate struct {
	VideoTrackCount int
	AudioTrackCount int
	AudioCodec      string // canonical AAC (lowercase codec_name from ffprobe)
	ArtifactBytes   int64
	SHA256Hex       string
	DurationSec     float64
}

// FFprobeArtifactVerifier is the real Level D verifier used by staging and
// production. It rejects missing/empty/truncated files, computes the master
// SHA-256, and runs ffprobe to enforce the canonical media gates:
//
//	video_track_count >= 1
//	audio_track_count >= 1
//	audio codec == aac
//	artifact size  > 0
//	sha256 valid (canonical lowercase hex, re-computed by upload boundary)
//	duration      > 0
//
// Each gate maps to a grep-friendly error key (video_track_count,
// audio_track_count, audio_codec, size, sha256, duration) so the audit
// dashboard can route on the exact missing property.
type FFprobeArtifactVerifier struct {
	FFprobeBin string
}

// NewFFprobeArtifactVerifier returns the default verifier using ffprobe from
// PATH. The binary is resolved at execution time so bootstrap remains cheap.
func NewFFprobeArtifactVerifier() *FFprobeArtifactVerifier {
	return &FFprobeArtifactVerifier{FFprobeBin: "ffprobe"}
}

// VerifyLocalArtifactDigest validates the exact bytes an uploader is about
// to send. It is shared by the production Drive adapter and local test
// uploader so the post-verifier upload boundary has one implementation.
func VerifyLocalArtifactDigest(path string, expectedBytes int64, expectedSHA256 string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("artifact upload verification: stat %s: %w", path, err)
	}
	if !info.Mode().IsRegular() || info.Size() <= 0 {
		return fmt.Errorf("artifact upload verification: %s is not a non-empty regular file", path)
	}
	if expectedBytes > 0 && info.Size() != expectedBytes {
		return fmt.Errorf("artifact upload verification: size=%d want=%d", info.Size(), expectedBytes)
	}
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("artifact upload verification: open %s: %w", path, err)
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return fmt.Errorf("artifact upload verification: hash %s: %w", path, err)
	}
	got := hex.EncodeToString(h.Sum(nil))
	if expectedSHA256 != "" && got != expectedSHA256 {
		return fmt.Errorf("artifact upload verification: sha256=%s want=%s", got, expectedSHA256)
	}
	return nil
}

func (v *FFprobeArtifactVerifier) VerifyArtifact(ctx context.Context, path string, expectedBytes int64) (string, error) {
	gate, err := v.VerifyMedia(ctx, path, expectedBytes)
	if err != nil {
		return "", err
	}
	return gate.SHA256Hex, nil
}

// VerifyMedia runs the full media gate set and returns the aggregated gate
// so callers (audit dashboards, smoke runner) can inspect the exact counts.
func (v *FFprobeArtifactVerifier) VerifyMedia(ctx context.Context, path string, expectedBytes int64) (MediaGate, error) {
	var gate MediaGate
	if v == nil {
		return gate, fmt.Errorf("artifact verification: verifier is nil")
	}
	if strings.TrimSpace(path) == "" {
		return gate, fmt.Errorf("artifact verification: path is empty")
	}
	info, err := os.Stat(path)
	if err != nil {
		return gate, fmt.Errorf("artifact verification: stat %s: %w", path, err)
	}
	if !info.Mode().IsRegular() || info.Size() <= 0 {
		return gate, fmt.Errorf("artifact verification: size=%d want=%d (artifact must be a non-empty regular file)", info.Size(), 1)
	}
	if expectedBytes > 0 && info.Size() != expectedBytes {
		return gate, fmt.Errorf("artifact verification: size=%d want=%d", info.Size(), expectedBytes)
	}
	gate.ArtifactBytes = info.Size()

	f, err := os.Open(path)
	if err != nil {
		return gate, fmt.Errorf("artifact verification: open %s: %w", path, err)
	}
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		_ = f.Close()
		return gate, fmt.Errorf("artifact verification: hash %s: %w", path, err)
	}
	if err := f.Close(); err != nil {
		return gate, fmt.Errorf("artifact verification: close %s: %w", path, err)
	}
	gate.SHA256Hex = hex.EncodeToString(h.Sum(nil))

	ffprobe := v.FFprobeBin
	if ffprobe == "" {
		ffprobe = "ffprobe"
	}
	// One ffprobe invocation gathers streams + duration. Stream-level
	// entries drive the track gates; format duration keeps the positive
	// playtime contract from the earlier verifier.
	cmd := exec.CommandContext(ctx, ffprobe,
		"-v", "error",
		"-show_entries", "stream=codec_type,codec_name:format=duration",
		"-of", "json",
		path,
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return gate, fmt.Errorf("artifact verification: ffprobe %s: %w (output: %s)", path, err, strings.TrimSpace(string(output)))
	}
	var doc ffprobeMediaDocument
	if err := json.Unmarshal(output, &doc); err != nil {
		return gate, fmt.Errorf("artifact verification: ffprobe JSON parse: %w (output: %s)", err, strings.TrimSpace(string(output)))
	}

	// Duration gate (format duration must parse to a positive number).
	duration, err := strconv.ParseFloat(strings.TrimSpace(doc.Format.Duration), 64)
	if err != nil || duration <= 0 {
		return gate, fmt.Errorf("artifact verification: duration=%q (must be > 0)", strings.TrimSpace(doc.Format.Duration))
	}
	gate.DurationSec = duration

	// Track gates. codec_name is the ffprobe canonical lowercase name
	// (e.g. "h264", "aac"). A non-media file yields zero streams → FAIL.
	// The AAC gate applies to EVERY audio stream: an artifact that mixes
	// an AAC track with a non-AAC track (e.g. [aac, opus]) is FAIL, and
	// one whose first audio stream is non-AAC ([mp3, aac]) is FAIL.
	aacAudioCount := 0
	for _, s := range doc.Streams {
		switch s.CodecType {
		case "video":
			gate.VideoTrackCount++
		case "audio":
			gate.AudioTrackCount++
			if gate.AudioCodec == "" {
				gate.AudioCodec = s.CodecName
			}
			if s.CodecName == "aac" {
				aacAudioCount++
			}
		}
	}
	if gate.VideoTrackCount < 1 {
		return gate, fmt.Errorf("artifact verification: video_track_count=%d want=%d", gate.VideoTrackCount, 1)
	}
	if gate.AudioTrackCount < 1 {
		return gate, fmt.Errorf("artifact verification: audio_track_count=%d want=%d (audio_track_count=0 must be FAIL)", gate.AudioTrackCount, 1)
	}
	if aacAudioCount != gate.AudioTrackCount {
		return gate, fmt.Errorf("artifact verification: audio_codec=%q aac_track_count=%d/%d want all-aac", gate.AudioCodec, aacAudioCount, gate.AudioTrackCount)
	}
	return gate, nil
}

// ffprobeMediaDocument is the JSON subset emitted by the verifier's ffprobe
// invocation. codec_type / codec_name are stream-level; duration is
// format-level.
type ffprobeMediaDocument struct {
	Streams []struct {
		CodecType string `json:"codec_type"`
		CodecName string `json:"codec_name"`
	} `json:"streams"`
	Format struct {
		Duration string `json:"duration"`
	} `json:"format"`
}

// Keep the hash implementation visible at this boundary: callers receive a
// canonical lowercase SHA-256 digest suitable for artifact metadata and
// deterministic assertions.
var _ = sha256.Size
