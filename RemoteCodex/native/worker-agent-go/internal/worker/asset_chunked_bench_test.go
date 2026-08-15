package worker

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"velox-shared/assetref"
	"velox-worker-agent/internal/downloader"
)

// assetChunkedBenchSize is the "large file" size used by the throughput
// benchmarks and the soak test. It is comfortably above the chunk threshold
// set in the tests, and large enough that the full transfer pipeline
// (network + pre-allocation + SHA-256 + fsync) dominates a single iteration
// rather than fixed request overhead.
const assetChunkedBenchSize = 64 << 20 // 64 MiB

// benchAssetBytes returns a deterministic payload reused read-only across
// benchmark/soak iterations (no per-iteration allocation). i%251 keeps the
// bytes non-trivial for content sniffing and the SHA-256 path.
func benchAssetBytes(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 251)
	}
	return data
}

// benchTransferWorker builds a worker wired to srv for one iteration, with
// chunked download enabled or disabled, and returns the transfer request for
// the shared payload.
func benchTransferWorker(srv *httptest.Server, stateDir string, chunked bool, digest string) (*Worker, downloader.DownloadRequest) {
	worker := testTransferWorker(srv.URL, stateDir)
	if chunked {
		worker.config.AssetChunkedDownloadEnabled = true
		worker.config.AssetChunkedDownloadThresholdBytes = 1
		worker.config.AssetChunkedDownloadConcurrency = 4
	}
	req := downloader.DownloadRequest{
		AssetID:   "bench-asset",
		AssetKey:  assetref.AssetKey("bench-asset"),
		SHA256:    assetref.ContentHash(digest),
		SizeBytes: assetChunkedBenchSize,
	}
	return worker, req
}

// benchTransfer runs one benchmark iteration over the shared large payload.
// b.SetBytes makes the default benchmark output report throughput (MB/s) for
// a direct single-stream vs chunked comparison.
func benchTransfer(b *testing.B, chunked bool) {
	data := benchAssetBytes(assetChunkedBenchSize)
	digest := testAssetDigest(data)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "asset.bin", time.Time{}, bytes.NewReader(data))
	}))
	defer srv.Close()

	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		stateDir := b.TempDir()
		worker, req := benchTransferWorker(srv, stateDir, chunked, digest)
		if _, err := (&masterAssetTransferer{w: worker}).Transfer(context.Background(), context.Background(), req, nil); err != nil {
			b.Fatalf("transfer: %v", err)
		}
	}
}

// BenchmarkAssetTransferSingleStream is the "before" baseline: one HTTP
// connection streams the whole file into the cache.
func BenchmarkAssetTransferSingleStream(b *testing.B) { benchTransfer(b, false) }

// BenchmarkAssetTransferChunked is the "after" case: N parallel Range
// requests write the same file at fixed offsets.
func BenchmarkAssetTransferChunked(b *testing.B) { benchTransfer(b, true) }

// TestSoakChunkedDownloadThroughput runs the large-file transfer repeatedly
// (single-stream then chunked), reports measured throughput and asserts the
// integrity/no-leftover invariants. It is gated behind VELOX_SOAK=1 because a
// 64 MiB payload per iteration is too heavy for the default CI test run; run
// it (ideally under -race) as:
//
//	VELOX_SOAK=1 go test -race -run TestSoakChunkedDownloadThroughput ./internal/worker/
func TestSoakChunkedDownloadThroughput(t *testing.T) {
	if os.Getenv("VELOX_SOAK") == "" {
		t.Skip("set VELOX_SOAK=1 to run the chunked download soak")
	}

	data := benchAssetBytes(assetChunkedBenchSize)
	digest := testAssetDigest(data)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "asset.bin", time.Time{}, bytes.NewReader(data))
	}))
	defer srv.Close()

	const iterations = 5
	for _, chunked := range []bool{false, true} {
		label := "single-stream"
		if chunked {
			label = "chunked"
		}
		for i := 0; i < iterations; i++ {
			stateDir := t.TempDir()
			worker, req := benchTransferWorker(srv, stateDir, chunked, digest)
			start := time.Now()
			result, err := (&masterAssetTransferer{w: worker}).Transfer(context.Background(), context.Background(), req, nil)
			elapsed := time.Since(start)
			if err != nil {
				t.Fatalf("%s iteration %d: %v", label, i, err)
			}
			mibps := float64(result.Bytes) / elapsed.Seconds() / (1 << 20)
			t.Logf("%s iteration %d: %d bytes in %v (%.1f MiB/s)", label, i, result.Bytes, elapsed, mibps)

			// A completed transfer must not leave a partial behind.
			part := assetPartialPath(worker.assetCacheDir(), req.AssetID, string(req.SHA256))
			if _, statErr := os.Stat(part); !os.IsNotExist(statErr) {
				t.Fatalf("%s iteration %d: partial remains after success: %v", label, i, statErr)
			}
		}
	}
}
