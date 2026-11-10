package contract

import (
	"encoding/json"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestCanonicalNormalizeSourceEmitsThePinnedStream is the Velox half of the
// real-byte cross-repository gate. It starts from a deliberately non-canonical
// MP4 (1280x720 @ 30 fps, 1/12288) and runs the exact whole-source
// normalization command used by the stock cache. The resulting bytes must
// carry the same 1/90000 timebase and canonical avcC that RenderingGen's V3
// overlay lane emits and PipelineGen transports; declaring them in Go is not
// enough.
func TestCanonicalNormalizeSourceEmitsThePinnedStream(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg is not installed")
	}
	if _, err := exec.LookPath("ffprobe"); err != nil {
		t.Skip("ffprobe is not installed")
	}
	dir := t.TempDir()
	source := filepath.Join(dir, "source.mp4")
	generate := exec.Command("ffmpeg",
		"-hide_banner", "-loglevel", "error", "-y",
		"-f", "lavfi", "-i", "testsrc=size=1280x720:rate=30",
		"-frames:v", "30", "-an", "-sn", "-dn",
		"-c:v", "libx264", "-preset", "ultrafast", "-bf", "2", "-g", "15",
		"-video_track_timescale", "12288", source,
	)
	if output, err := generate.CombinedOutput(); err != nil {
		t.Fatalf("generate non-canonical fixture: %v: %s", err, output)
	}

	output := filepath.Join(dir, "normalized.mp4")
	args := CanonicalVideoNormalizeSourceArgs(CanonicalVideoProfileV1Default, CanonicalPreparationQualityPolicyDefault, source, output)
	if out, err := exec.Command("ffmpeg", args...).CombinedOutput(); err != nil {
		t.Fatalf("canonical normalize: %v: %s", err, out)
	}

	data, err := exec.Command("ffprobe",
		"-v", "error", "-select_streams", "v:0", "-show_data_hash", "sha256",
		"-show_entries", "stream=codec_name,profile,level,width,height,pix_fmt,r_frame_rate,time_base,extradata_hash",
		"-of", "json", output,
	).Output()
	if err != nil {
		t.Fatalf("probe canonical normalize output: %v", err)
	}
	var doc struct {
		Streams []struct {
			CodecName     string `json:"codec_name"`
			Profile       string `json:"profile"`
			Level         int    `json:"level"`
			Width         int    `json:"width"`
			Height        int    `json:"height"`
			PixelFormat   string `json:"pix_fmt"`
			FrameRate     string `json:"r_frame_rate"`
			TimeBase      string `json:"time_base"`
			ExtradataHash string `json:"extradata_hash"`
		} `json:"streams"`
	}
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatalf("decode probe: %v", err)
	}
	if len(doc.Streams) != 1 {
		t.Fatalf("canonical output has %d video streams, want 1", len(doc.Streams))
	}
	stream := doc.Streams[0]
	if stream.CodecName != "h264" || stream.Profile != "High" || stream.Level != 40 {
		t.Fatalf("canonical codec = %s/%s/%d, want h264/High/40", stream.CodecName, stream.Profile, stream.Level)
	}
	if stream.Width != 1920 || stream.Height != 1080 || stream.PixelFormat != "yuv420p" {
		t.Fatalf("canonical frame = %dx%d %s", stream.Width, stream.Height, stream.PixelFormat)
	}
	if stream.FrameRate != "24/1" {
		t.Fatalf("canonical frame rate = %q, want 24/1", stream.FrameRate)
	}
	if stream.TimeBase != "1/90000" {
		t.Fatalf("canonical timebase = %q, want 1/90000", stream.TimeBase)
	}
	extradata := strings.TrimPrefix(strings.TrimSpace(stream.ExtradataHash), "SHA256:")
	if extradata != CanonicalVideoExtradataSHA256 {
		t.Fatalf("canonical avcC = %q, want %s", extradata, CanonicalVideoExtradataSHA256)
	}
}
