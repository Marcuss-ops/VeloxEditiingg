package worker

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"velox-shared/assetref"
	"velox-worker-agent/internal/downloader"
	"velox-worker-agent/internal/telemetry"
)

// asset_transferer.go owns the integrity-aware byte transport: the
type chunkRange struct {
	start int64
	end   int64
}

// chunkPlan splits size bytes into concurrency contiguous, non-overlapping
// ranges. A size smaller than the concurrency yields one range per byte.
func chunkPlan(size int64, concurrency int) []chunkRange {
	if size <= 0 {
		return nil
	}
	if concurrency < 1 {
		concurrency = 1
	}
	if int64(concurrency) > size {
		concurrency = int(size)
	}
	n := int64(concurrency)
	base := size / n
	rem := size % n
	chunks := make([]chunkRange, 0, concurrency)
	var start int64
	for i := int64(0); i < n; i++ {
		length := base
		if i < rem {
			length++
		}
		chunks = append(chunks, chunkRange{start: start, end: start + length - 1})
		start += length
	}
	return chunks
}

// sharedBandwidthLimiter paces aggregate byte throughput across concurrent
// chunk readers against a single virtual clock, so the total transfer never
// exceeds cap bytes/second. It replaces the previous per-chunk division of
// the cap: every chunk accounts its bytes to the SAME clock, enforcing the
// aggregate ceiling exactly instead of approximately per connection.
type sharedBandwidthLimiter struct {
	mu       sync.Mutex
	cap      int64
	start    time.Time
	consumed int64
}

// newSharedBandwidthLimiter returns a limiter enforcing cap bytes/second, or
// nil for an uncapped transfer (cap <= 0).
func newSharedBandwidthLimiter(cap int64) *sharedBandwidthLimiter {
	if cap <= 0 {
		return nil
	}
	return &sharedBandwidthLimiter{cap: cap}
}

// pace accounts n bytes against the shared clock and sleeps until those bytes
// are due at the capped rate. A nil limiter is a no-op. Safe for concurrent
// use; returns the ctx error when the wait is cancelled.
func (l *sharedBandwidthLimiter) pace(ctx context.Context, n int64) error {
	if l == nil {
		return nil
	}
	now := time.Now()
	l.mu.Lock()
	if l.start.IsZero() {
		l.start = now
		l.consumed = 0
	}
	l.consumed += n
	target := l.start.Add(time.Duration(float64(l.consumed) / float64(l.cap) * float64(time.Second)))
	l.mu.Unlock()

	if wait := time.Until(target); wait > 0 {
		return waitForAssetDuration(ctx, wait)
	}
	return nil
}

// transferChunked downloads one large asset with N parallel Range requests
// writing directly into a pre-allocated partial at fixed offsets. It keeps the
// same integrity gate as the single-stream path (size + SHA-256 before atomic
// promotion) and reports aggregate progress through the shared counter. A
// return of errChunkRangeUnsupported signals the upstream cannot be chunked
// and the dispatcher should fall back.
func (t *masterAssetTransferer) transferChunked(ctx context.Context, reportCtx context.Context, req downloader.DownloadRequest, onProgress func(downloadedBytes int64)) (downloader.TransferResult, error) {
	w := t.w
	size := req.SizeBytes
	_, _, concurrency := t.chunkedConfig()
	cacheDir := w.assetCacheDir()
	if err := os.MkdirAll(filepath.Join(cacheDir, "partial"), 0o755); err != nil {
		return downloader.TransferResult{}, err
	}

	partialPath := assetPartialPath(cacheDir, req.AssetID, string(req.SHA256))
	deactivatePartial := markAssetPartialActive(partialPath)
	defer deactivatePartial()
	// Chunked always starts from a clean, pre-allocated partial: a leftover
	// from a prior single-stream resume would corrupt the offset-write layout.
	_ = os.Remove(partialPath)

	f, err := os.OpenFile(partialPath, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return downloader.TransferResult{}, err
	}
	defer f.Close()
	// Pre-allocate the full size so each chunk writes to a reserved offset
	// without incremental growth (avoids fragmentation and per-append churn).
	if err := preallocateFile(f, size); err != nil {
		return downloader.TransferResult{}, err
	}

	chunks := chunkPlan(size, concurrency)
	if len(chunks) == 0 {
		_ = os.Remove(partialPath)
		return downloader.TransferResult{}, fmt.Errorf("chunked: cannot plan chunks for size %d", size)
	}

	downloadURL, authToken, client := t.assetTransferRequest(req.AssetID)

	var downloaded atomic.Int64
	var progressMu sync.Mutex
	var lastReported int64

	// Dedicated chunk telemetry: the number of in-flight chunk connections
	// (additive, so concurrent chunked transfers SUM on the shared gauge) and
	// the current transfer throughput (bytes/s, last-writer-wins). Throughput
	// is sampled during progress under a time throttle; both gauges settle
	// back to zero when this transfer ends so no stale rate lingers.
	chunkMetrics := telemetry.GetPrometheusMetrics()
	chunkMetrics.AddAssetDownloadChunksActive(len(chunks))
	defer chunkMetrics.AddAssetDownloadChunksActive(-len(chunks))
	chunkStarted := time.Now()
	defer chunkMetrics.SetAssetDownloadChunkThroughput(0)
	var lastThroughputPublish atomic.Int64 // UnixNano of last throttled publish
	publishThroughput := func() {
		now := time.Now().UnixNano()
		last := lastThroughputPublish.Load()
		if now-last < int64(250*time.Millisecond) {
			return
		}
		if !lastThroughputPublish.CompareAndSwap(last, now) {
			return
		}
		if elapsed := time.Since(chunkStarted).Seconds(); elapsed > 0 {
			chunkMetrics.SetAssetDownloadChunkThroughput(float64(downloaded.Load()) / elapsed)
		}
	}

	report := func() {
		publishThroughput()
		if onProgress == nil {
			return
		}
		total := downloaded.Load()
		progressMu.Lock()
		if total > lastReported {
			lastReported = total
			onProgress(total)
			progressMu.Unlock()
			return
		}
		progressMu.Unlock()
	}

	// One shared token-bucket paces every chunk against a single virtual
	// clock, so the aggregate transfer stays exactly at/under the prefetch
	// QoS cap instead of dividing it (approximately) per connection.
	limiter := newSharedBandwidthLimiter(req.MaxBandwidthBytesPerSecond)

	chunkCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	var primaryOnce sync.Once
	primaryErr := make(chan error, 1)
	for _, c := range chunks {
		wg.Add(1)
		go func(c chunkRange) {
			defer wg.Done()
			if err := fetchChunkRange(chunkCtx, client, downloadURL, authToken, c, f, &downloaded, report, limiter, c.start == 0); err != nil {
				primaryOnce.Do(func() {
					primaryErr <- err
					cancel()
				})
			}
		}(c)
	}
	wg.Wait()
	cancel()

	select {
	case err := <-primaryErr:
		_ = os.Remove(partialPath)
		return downloader.TransferResult{}, err
	default:
	}

	if err := f.Sync(); err != nil {
		_ = os.Remove(partialPath)
		return downloader.TransferResult{}, err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(partialPath)
		return downloader.TransferResult{}, err
	}

	ext := extensionForMediaType(req.MIMEType)
	if ext == "" {
		ext = sniffAssetExtension(partialPath)
	}
	finalPath, written, actualSHA, verifyDuration, err := verifyAndPromoteVeloxAsset(cacheDir, string(req.SHA256), size, partialPath, ext, syncAssetDirectory)
	if err != nil {
		recordCacheProjectionEvent(reportCtx, "hash_verify", verifyDuration, telemetry.StatusFailed, "", 0)
		return downloader.TransferResult{}, err
	}
	recordCacheProjectionEvent(reportCtx, "hash_verify", verifyDuration, telemetry.StatusOK, "", 0)
	return downloader.TransferResult{LocalPath: finalPath, Bytes: written, SHA256: assetref.ContentHash(actualSHA)}, nil
}

// fetchChunkRange downloads one byte range into the pre-allocated partial at
// its offset, retrying transient failures with backoff. It returns
// errChunkRangeUnsupported when the upstream returns a full 200 or a
// malformed/absent Content-Range for the requested window (so the dispatcher
// can fall back to single-stream), and otherwise a permanent/retryable-style
// error. shared/report aggregate progress across all chunk goroutines. The
// authToken getter is re-read per attempt so a retry after a master restart
// uses the freshly re-issued session token.
func fetchChunkRange(ctx context.Context, client *http.Client, downloadURL string, authToken func() string, c chunkRange, w io.WriterAt, shared *atomic.Int64, report func(), limiter *sharedBandwidthLimiter, sniffHTML bool) error {
	backoffs := downloader.BackoffSchedule(downloader.DefaultMaxAttempts, downloader.DefaultBaseBackoff, downloader.DefaultJitter)
	var lastErr error
	for attempt := 0; attempt < downloader.DefaultMaxAttempts; attempt++ {
		if attempt > 0 {
			if err := waitForAssetDuration(ctx, backoffs[attempt-1]); err != nil {
				return err
			}
		}
		reqHTTP, err := http.NewRequestWithContext(ctx, http.MethodGet, downloadURL, nil)
		if err != nil {
			return err
		}
		if token := authToken(); token != "" {
			reqHTTP.Header.Set("Authorization", "Bearer "+token)
		}
		reqHTTP.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", c.start, c.end))

		resp, err := client.Do(reqHTTP)
		if err != nil {
			lastErr = err
			continue
		}

		switch {
		case resp.StatusCode == http.StatusPartialContent:
			contentRange := strings.TrimSpace(resp.Header.Get("Content-Range"))
			start, end, _, parseErr := parseAssetContentRange(contentRange)
			if parseErr != nil || start != c.start || end != c.end {
				resp.Body.Close()
				return errChunkRangeUnsupported
			}
			if sniffHTML && isHTMLMediaType(resp.Header.Get("Content-Type")) {
				resp.Body.Close()
				return fmt.Errorf("unexpected HTML response while downloading asset")
			}
			// A fresh section writer per attempt: a failed mid-stream copy
			// advances the section offset, so a retry must restart at c.start.
			section := &sectionWriter{w: w, off: c.start, n: c.end - c.start + 1}
			body := io.Reader(resp.Body)
			if sniffHTML {
				// Only the first chunk (byte zero) can carry a login/error
				// page; sniff its leading bytes before any byte is written so
				// a misbehaving upstream cannot persist HTML as an asset.
				br := bufio.NewReader(body)
				peek, _ := br.Peek(512)
				if isHTMLPayload(peek) {
					resp.Body.Close()
					return fmt.Errorf("unexpected HTML response while downloading asset")
				}
				body = br
			}
			if shared != nil && report != nil {
				body = &chunkProgressReader{ctx: ctx, src: body, shared: shared, report: report, limiter: limiter}
			}
			_, copyErr := io.Copy(section, body)
			resp.Body.Close()
			if copyErr != nil {
				lastErr = copyErr
				continue
			}
			return nil
		case resp.StatusCode == http.StatusNotFound:
			resp.Body.Close()
			return fmt.Errorf("asset not found")
		case downloader.IsPermanentStatus(resp.StatusCode):
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			resp.Body.Close()
			return fmt.Errorf("asset download failed: %s", strings.TrimSpace(string(body)))
		case downloader.IsRetryableStatus(resp.StatusCode):
			retryAfter := downloader.RetryAfter(resp)
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			resp.Body.Close()
			lastErr = fmt.Errorf("master returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
			if retryAfter > 0 && attempt+1 < len(backoffs)+1 {
				backoffs[attempt] = retryAfter
			}
			continue
		default:
			// 200 (server ignored Range), a 3xx, or an unexpected 2xx: the
			// upstream cannot safely satisfy the requested window.
			resp.Body.Close()
			return errChunkRangeUnsupported
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("download failed")
	}
	return lastErr
}

// sectionWriter adapts an io.WriterAt into an io.Writer bounded to a fixed
// [off, off+n) window. It is the write-side counterpart of io.SectionReader
// and lets each chunk land at its offset via os.File.WriteAt without a global
// lock.
type sectionWriter struct {
	w   io.WriterAt
	off int64
	n   int64
}

func (s *sectionWriter) Write(p []byte) (int, error) {
	if s.n <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > s.n {
		p = p[:s.n]
	}
	n, err := s.w.WriteAt(p, s.off)
	s.off += int64(n)
	s.n -= int64(n)
	return n, err
}

// chunkProgressReader reports bytes landed by one chunk into a shared atomic
// counter so the manager sees aggregate progress across all chunk goroutines.
// maxBPS is the per-chunk bandwidth cap (the aggregate QoS cap already divided
// by the chunk count).
type chunkProgressReader struct {
	ctx     context.Context
	src     io.Reader
	shared  *atomic.Int64
	report  func()
	limiter *sharedBandwidthLimiter
}

func (r *chunkProgressReader) Read(b []byte) (int, error) {
	n, err := r.src.Read(b)
	if n > 0 {
		r.shared.Add(int64(n))
		r.report()
		if waitErr := r.limiter.pace(r.ctx, int64(n)); waitErr != nil {
			return n, waitErr
		}
	}
	return n, err
}
