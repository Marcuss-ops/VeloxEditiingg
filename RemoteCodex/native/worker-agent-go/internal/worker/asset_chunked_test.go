package worker

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"velox-shared/assetref"
	"velox-worker-agent/internal/downloader"
)

func TestChunkPlan(t *testing.T) {
	const size = 100_000
	const concurrency = 4
	chunks := chunkPlan(size, concurrency)
	if len(chunks) != concurrency {
		t.Fatalf("chunks = %d, want %d", len(chunks), concurrency)
	}
	var total int64
	prevEnd := int64(-1)
	for i, c := range chunks {
		if c.start != prevEnd+1 {
			t.Fatalf("chunk %d start = %d, want %d", i, c.start, prevEnd+1)
		}
		if c.end < c.start {
			t.Fatalf("chunk %d end %d < start %d", i, c.end, c.start)
		}
		total += c.end - c.start + 1
		prevEnd = c.end
	}
	if total != size {
		t.Fatalf("total = %d, want %d", total, size)
	}
	if chunks[len(chunks)-1].end != size-1 {
		t.Fatalf("last end = %d, want %d", chunks[len(chunks)-1].end, size-1)
	}

	// A size smaller than the concurrency yields one range per byte.
	small := chunkPlan(3, 10)
	if len(small) != 3 {
		t.Fatalf("small chunks = %d, want 3", len(small))
	}
	if got := chunkPlan(0, 4); got != nil {
		t.Fatalf("zero-size plan = %v, want nil", got)
	}
}

func TestMasterAssetTransfererChunkedDownload(t *testing.T) {
	data := make([]byte, 100_000)
	for i := range data {
		data[i] = byte(i % 251)
	}
	digest := testAssetDigest(data)

	var mu sync.Mutex
	ranges := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		ranges[r.Header.Get("Range")]++
		mu.Unlock()
		http.ServeContent(w, r, "asset.mp3", time.Time{}, strings.NewReader(string(data)))
	}))
	defer srv.Close()

	stateDir := t.TempDir()
	worker := testTransferWorker(srv.URL, stateDir)
	worker.config.AssetChunkedDownloadEnabled = true
	worker.config.AssetChunkedDownloadThresholdBytes = 1024
	worker.config.AssetChunkedDownloadConcurrency = 4

	req := downloader.DownloadRequest{
		AssetID:   "chunked-asset",
		AssetKey:  assetref.AssetKey("chunked-asset"),
		SHA256:    assetref.ContentHash(digest),
		SizeBytes: int64(len(data)),
	}

	var progressMu sync.Mutex
	var reported []int64
	result, err := (&masterAssetTransferer{w: worker}).Transfer(context.Background(), context.Background(), req, func(downloaded int64) {
		progressMu.Lock()
		reported = append(reported, downloaded)
		progressMu.Unlock()
	})
	if err != nil {
		t.Fatalf("chunked transfer: %v", err)
	}
	got, err := os.ReadFile(result.LocalPath)
	if err != nil {
		t.Fatalf("read chunked final: %v", err)
	}
	if string(got) != string(data) {
		t.Fatalf("chunked bytes mismatch: got %d bytes, want %d", len(got), len(data))
	}
	if result.Bytes != int64(len(data)) || string(result.SHA256) != digest {
		t.Fatalf("result = bytes %d sha %q, want %d %q", result.Bytes, result.SHA256, len(data), digest)
	}

	progressMu.Lock()
	progress := append([]int64(nil), reported...)
	progressMu.Unlock()
	if len(progress) == 0 {
		t.Fatal("onProgress was never called")
	}
	for i := 1; i < len(progress); i++ {
		if progress[i] < progress[i-1] {
			t.Fatalf("aggregate progress not monotonic: %v", progress)
		}
	}
	if progress[len(progress)-1] != int64(len(data)) {
		t.Fatalf("final aggregate progress = %d, want %d", progress[len(progress)-1], len(data))
	}

	mu.Lock()
	defer mu.Unlock()
	if len(ranges) != 4 {
		t.Fatalf("distinct Range headers = %d (%v), want 4", len(ranges), ranges)
	}
	for rng := range ranges {
		if rng == "" || !strings.HasPrefix(rng, "bytes=") {
			t.Fatalf("unexpected Range header %q", rng)
		}
	}
}

func TestPerChunkBandwidthCap(t *testing.T) {
	cases := []struct {
		name string
		cap  int64
		n    int
		want int64
	}{
		{"uncapped", 0, 4, 0},
		{"negative", -1, 4, 0},
		{"even split", 100, 4, 25},
		{"cap smaller than chunks", 2, 4, 1},
		{"zero chunks defaults to one", 100, 0, 100},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := perChunkBandwidthCap(tc.cap, tc.n); got != tc.want {
				t.Fatalf("perChunkBandwidthCap(%d, %d) = %d, want %d", tc.cap, tc.n, got, tc.want)
			}
		})
	}
}

func TestFetchChunkRangeRejectsHTMLOnFirstChunk(t *testing.T) {
	html := []byte("<!DOCTYPE html><html><body>login</body></html>")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Non-HTML Content-Type so the PAYLOAD sniff (not the header check)
		// is what rejects the response.
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(html)-1, len(html)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(html)
	}))
	defer srv.Close()

	f, err := os.CreateTemp(t.TempDir(), "part-*")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	err = fetchChunkRange(context.Background(), srv.Client(), srv.URL, "", chunkRange{start: 0, end: int64(len(html) - 1)}, f, nil, nil, 0, true)
	if err == nil || !strings.Contains(err.Error(), "unexpected HTML response") {
		t.Fatalf("err = %v, want unexpected HTML response", err)
	}
}

func TestMasterAssetTransfererChunkedFallsBackWhenRangeUnsupported(t *testing.T) {
	data := []byte("upstream that ignores Range and serves the whole body")
	digest := testAssetDigest(data)

	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "audio/mpeg")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	}))
	defer srv.Close()

	stateDir := t.TempDir()
	worker := testTransferWorker(srv.URL, stateDir)
	worker.config.AssetChunkedDownloadEnabled = true
	worker.config.AssetChunkedDownloadThresholdBytes = 1
	worker.config.AssetChunkedDownloadConcurrency = 4

	req := downloader.DownloadRequest{
		AssetID:   "chunked-fallback",
		AssetKey:  assetref.AssetKey("chunked-fallback"),
		SHA256:    assetref.ContentHash(digest),
		SizeBytes: int64(len(data)),
	}

	result, err := (&masterAssetTransferer{w: worker}).Transfer(context.Background(), context.Background(), req, nil)
	if err != nil {
		t.Fatalf("fallback transfer: %v", err)
	}
	got, err := os.ReadFile(result.LocalPath)
	if err != nil || string(got) != string(data) {
		t.Fatalf("fallback final = %q/%v, want %q", got, err, data)
	}
	// The chunked probes (200, range ignored) plus the single-stream retry.
	if requests.Load() < 2 {
		t.Fatalf("requests = %d, want >= 2 (chunked probes + single-stream fallback)", requests.Load())
	}
}
