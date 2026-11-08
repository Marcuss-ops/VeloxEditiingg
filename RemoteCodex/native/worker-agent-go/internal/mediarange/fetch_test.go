package mediarange

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func be32(value uint32) []byte {
	out := make([]byte, 4)
	binary.BigEndian.PutUint32(out, value)
	return out
}

func mp4Box(typ string, parts ...[]byte) []byte {
	payload := bytes.Join(parts, nil)
	out := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint32(out[0:4], uint32(len(out)))
	copy(out[4:8], typ)
	copy(out[8:], payload)
	return out
}

const syntheticSamples = 4000
const syntheticSampleSize = 100

// syntheticMP4 builds a minimal progressive MP4 with a real stbl sample table:
// 100 samples of 100 bytes, 25 fps, sync samples every 25 frames.
func syntheticMP4(t *testing.T) (data []byte, sampleOffset int64) {
	t.Helper()
	buildMoov := func(chunkOffset uint32) []byte {
		mvhdPayload := make([]byte, 100)
		binary.BigEndian.PutUint32(mvhdPayload[12:16], 1000)
		binary.BigEndian.PutUint32(mvhdPayload[16:20], 4000)
		hdlrPayload := make([]byte, 24)
		copy(hdlrPayload[8:12], "vide")
		mdhdPayload := make([]byte, 24)
		binary.BigEndian.PutUint32(mdhdPayload[12:16], 1000)
		stts := mp4Box("stts", []byte{0, 0, 0, 0}, be32(1), be32(syntheticSamples), be32(40))
		syncParts := [][]byte{[]byte{0, 0, 0, 0}, be32(uint32((syntheticSamples + 24) / 25))}
		for frame := 1; frame <= syntheticSamples; frame += 25 {
			syncParts = append(syncParts, be32(uint32(frame)))
		}
		stss := mp4Box("stss", syncParts...)
		stsc := mp4Box("stsc", []byte{0, 0, 0, 0}, be32(1), be32(1), be32(syntheticSamples), be32(1))
		stsz := mp4Box("stsz", []byte{0, 0, 0, 0}, be32(syntheticSampleSize), be32(syntheticSamples))
		stco := mp4Box("stco", []byte{0, 0, 0, 0}, be32(1), be32(chunkOffset))
		stbl := mp4Box("stbl", stsc, stsz, stco, stts, stss)
		mdia := mp4Box("mdia", mp4Box("mdhd", mdhdPayload), mp4Box("hdlr", hdlrPayload), mp4Box("minf", stbl))
		return mp4Box("moov", mp4Box("mvhd", mvhdPayload), mp4Box("trak", mdia))
	}
	ftyp := mp4Box("ftyp", []byte("isom"), be32(0), []byte("isomiso2"))
	moov := buildMoov(0)
	sampleOffset = int64(len(ftyp) + len(moov) + 8)
	moov = buildMoov(uint32(sampleOffset))
	data = append(append(append([]byte{}, ftyp...), moov...), mp4Box("mdat", bytes.Repeat([]byte{0x5a}, syntheticSamples*syntheticSampleSize))...)
	return data, sampleOffset
}

type countingWriter struct {
	http.ResponseWriter
	written int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.ResponseWriter.Write(p)
	c.written += int64(n)
	return n, err
}

func TestFetchWindowDownloadsOnlyNeededSamples(t *testing.T) {
	data, sampleOffset := syntheticMP4(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(&countingWriter{ResponseWriter: w}, r, "video.mp4", time.Time{}, bytes.NewReader(data))
	}))
	defer server.Close()
	dest := filepath.Join(t.TempDir(), "window.mp4")
	stats, err := FetchWindow(context.Background(), server.Client(), server.URL, Options{
		Size: int64(len(data)), StartFrame: 2007, FrameCount: 5, Dest: dest,
	})
	if err != nil {
		t.Fatalf("FetchWindow: %v", err)
	}
	if stats.DownloadedBytes <= 0 || stats.DownloadedBytes >= int64(len(data))/2 {
		t.Fatalf("downloaded %d of %d bytes; want a small fraction", stats.DownloadedBytes, len(data))
	}
	if stats.PreRollSamples != 7 {
		t.Fatalf("pre-roll samples = %d; want 7 (frame 2007 -> sync 2000)", stats.PreRollSamples)
	}
	info, err := os.Stat(dest)
	if err != nil || info.Size() != int64(len(data)) {
		t.Fatalf("sparse file size = %v err=%v; want %d", info, err, len(data))
	}
	got, err := os.ReadFile(dest)
	if err != nil {
		t.Fatal(err)
	}
	// Sample 2007 is the first requested frame and must carry source bytes.
	from := int(sampleOffset) + 2007*syntheticSampleSize
	if !bytes.Equal(got[from:from+syntheticSampleSize], data[from:from+syntheticSampleSize]) {
		t.Fatal("requested sample bytes were not materialized")
	}
	// Sample 0 is outside the window and must remain a hole.
	if !bytes.Equal(got[sampleOffset:int(sampleOffset)+syntheticSampleSize], make([]byte, syntheticSampleSize)) {
		t.Fatal("sample outside the window was downloaded or is not a hole")
	}
}

func TestFetchWindowFallsBackWhenRangeIsIgnored(t *testing.T) {
	data, _ := syntheticMP4(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "10402")
		_, _ = w.Write(data)
	}))
	defer server.Close()
	dest := filepath.Join(t.TempDir(), "window.mp4")
	_, err := FetchWindow(context.Background(), server.Client(), server.URL, Options{
		Size: int64(len(data)), StartFrame: 1, FrameCount: 1, Dest: dest,
	})
	if !errors.Is(err, ErrRangeUnsupported) {
		t.Fatalf("error = %v; want ErrRangeUnsupported", err)
	}
	if _, statErr := os.Stat(dest); !os.IsNotExist(statErr) {
		t.Fatalf("failed sparse fetch must not leave %s behind", dest)
	}
}

func TestFetchWindowRealMP4DecodesIdentically(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg is required for the sparse decode equivalence test")
	}
	root := t.TempDir()
	full := filepath.Join(root, "full.mp4")
	generate := exec.Command("ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
		"-f", "lavfi", "-i", "testsrc=size=320x180:rate=24",
		"-frames:v", "120", "-an", "-c:v", "libx264", "-pix_fmt", "yuv420p",
		"-g", "24", "-keyint_min", "24", "-sc_threshold", "0", "-bf", "2",
		"-movflags", "+faststart", full)
	if output, err := generate.CombinedOutput(); err != nil {
		t.Fatalf("generate fixture: %v: %s", err, output)
	}
	info, err := os.Stat(full)
	if err != nil {
		t.Fatal(err)
	}
	source, err := os.ReadFile(full)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "full.mp4", time.Time{}, bytes.NewReader(source))
	}))
	defer server.Close()

	dest := filepath.Join(root, "sparse.mp4")
	stats, err := FetchWindow(context.Background(), server.Client(), server.URL, Options{
		Size: info.Size(), StartUS: 1_000_000, DurationUS: 500_000, Dest: dest,
	})
	if err != nil {
		t.Fatalf("FetchWindow: %v", err)
	}
	if stats.DownloadedBytes <= 0 || stats.DownloadedBytes >= info.Size() {
		t.Fatalf("downloaded %d of %d bytes; want a strict subset", stats.DownloadedBytes, info.Size())
	}
	if stats.PreRollSamples <= 0 {
		t.Fatalf("pre-roll samples = %d; want the preceding keyframe", stats.PreRollSamples)
	}
	want := frameMD5(t, full)
	got := frameMD5(t, dest)
	if want == "" || got == "" {
		t.Fatal("framemd5 output is empty")
	}
	if want != got {
		t.Fatalf("sparse decode differs from the full source\nwant:\n%s\ngot:\n%s", want, got)
	}
}

func frameMD5(t *testing.T, path string) string {
	t.Helper()
	command := exec.Command("ffmpeg", "-hide_banner", "-loglevel", "error",
		"-ss", "1.0", "-i", path, "-frames:v", "12", "-an", "-f", "framemd5", "-")
	output, err := command.Output()
	if err != nil {
		t.Fatalf("framemd5 %s: %v", path, err)
	}
	var builder strings.Builder
	for _, line := range strings.Split(string(output), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		builder.WriteString(line)
		builder.WriteString("\n")
	}
	return builder.String()
}

func TestFetchWindowRejectsNonMP4(t *testing.T) {
	data := bytes.Repeat([]byte{0}, 64)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "source.bin", time.Time{}, bytes.NewReader(data))
	}))
	defer server.Close()
	_, err := FetchWindow(context.Background(), server.Client(), server.URL, Options{
		Size: int64(len(data)), StartFrame: 0, FrameCount: 1, Dest: filepath.Join(t.TempDir(), "window.mp4"),
	})
	if !errors.Is(err, ErrUnsupportedContainer) {
		t.Fatalf("error = %v; want ErrUnsupportedContainer", err)
	}
}
