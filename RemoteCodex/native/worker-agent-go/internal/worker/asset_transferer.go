package worker

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	neturl "net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"velox-shared/assetref"
	"velox-worker-agent/internal/downloader"
	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/internal/workercache"
	"velox-worker-agent/pkg/config"
)

// asset_transferer.go owns the integrity-aware byte transport: the
// masterAssetTransferer cache probe (Check) and its resumable, retrying HTTP
// GET (Transfer) against the master asset bridge. This file never emits cache
// accounting — the CacheResolver boundary records classified outcomes once
// per resolution.

type masterAssetTransferer struct{ w *Worker }

// Check probes the local cache and classifies the outcome AT the lookup
// point. A hit is valid only when the supplied size and SHA-256 pass against
// the on-disk file. The classification is the canonical CacheOutcome
// (HIT_VALID / MISS_*); telemetry is NOT emitted here — the resolver
// boundary (CacheResolver.Resolve) records it exactly once per resolution.
//
// Requests that arrive with partial (or no) integrity metadata are upgraded
// using the remembered self-verified digest of the last successful download
// of the same asset, so a fresh cache access for a folder-backed asset can
// still be served as a verified hit without a master round-trip.
func (t *masterAssetTransferer) Check(ctx context.Context, reportCtx context.Context, req downloader.DownloadRequest) (downloader.CacheCheckResult, error) {
	w := t.w
	key := assetref.AssetKey(req.AssetKey)
	var foundIncomplete, foundHashMismatch, foundSizeMismatch, foundExpired bool
	if w.canonicalAssetCache != nil {
		// Route by verified content identity first: a known SHA probes the
		// blob store directly, so an asset whose bytes are already cached
		// under a different asset ID is still a verified hit without a
		// master round-trip or a full re-hash. The stat+size check (no hash
		// recompute) is the verified-blob contract: the file was SHA-256
		// verified at promotion time.
		if req.SHA256 != "" {
			if blob, found, err := w.canonicalAssetCache.FindBlob(ctx, req.SHA256); err != nil {
				return downloader.CacheCheckResult{}, err
			} else if found && blob.DownloadComplete && (req.SizeBytes <= 0 || blob.SizeBytes == req.SizeBytes) {
				_ = w.canonicalAssetCache.MarkBlobUsed(ctx, req.SHA256)
				return downloader.CacheCheckResult{CacheHit: true, LocalPath: blob.LocalPath, SHA256: req.SHA256, Outcome: downloader.CacheOutcomeHitValid}, nil
			}
		}
		if entry, found, err := w.canonicalAssetCache.Find(ctx, key); err != nil {
			return downloader.CacheCheckResult{}, err
		} else if found {
			requestedHash := req.SHA256
			if requestedHash == "" {
				requestedHash = entry.ContentHash
			}
			hashOK := requestedHash == "" || entry.ContentHash == requestedHash
			sizeOK := req.SizeBytes <= 0 || entry.SizeBytes == req.SizeBytes
			switch {
			case !entry.DownloadComplete:
				// An index row without a committed download: incomplete.
				// Fall through to the probe path — a fully written physical
				// file may still satisfy the request.
				foundIncomplete = true
			case !hashOK:
				// A durable entry whose content hash contradicts the request:
				// corrupt/foreign bytes, distinct from a plain not-found.
				foundHashMismatch = true
			case !sizeOK:
				// A durable entry whose recorded size contradicts the request
				// is a size-invalid entry (MISS_INVALID), not a hash corrupt.
				foundSizeMismatch = true
			default:
				return downloader.CacheCheckResult{CacheHit: true, LocalPath: entry.LocalPath, SHA256: entry.ContentHash, Outcome: downloader.CacheOutcomeHitValid}, nil
				// The durable index claims a complete entry but the physical
				// file is gone (evicted/expired underneath the index). Keep
				// the probe as a final chance before classifying MISS_EXPIRED.
				foundExpired = true
			}
		}
	}
	probeSHA, probeSize := string(req.SHA256), req.SizeBytes
	if probeSHA == "" || probeSize <= 0 {
		if remembered, ok := w.rememberedAssetIntegrity(req.AssetID); ok {
			probeSHA, probeSize = remembered.SHA256, remembered.SizeBytes
		}
	}
	if probeSHA != "" && probeSize > 0 {
		if existing, _, err := cachedAssetPathTimedWithContext(reportCtx, w.assetCacheDir(), probeSHA, probeSize); err == nil && existing != "" {
			return downloader.CacheCheckResult{CacheHit: true, LocalPath: existing, SHA256: assetref.ContentHash(probeSHA), Outcome: downloader.CacheOutcomeHitValid}, nil
		}
	}
	switch {
	case foundExpired:
		return downloader.CacheCheckResult{Outcome: downloader.CacheOutcomeMissExpired}, nil
	case foundHashMismatch:
		return downloader.CacheCheckResult{Outcome: downloader.CacheOutcomeMissHashMismatch}, nil
	case foundSizeMismatch, foundIncomplete:
		return downloader.CacheCheckResult{Outcome: downloader.CacheOutcomeMissInvalid}, nil
	default:
		return downloader.CacheCheckResult{Outcome: downloader.CacheOutcomeMissNotFound}, nil
	}
}

// Transfer streams the asset from the master asset bridge into the local
// cache, verifying size and SHA-256 before the file becomes visible. When
// chunked download is enabled and the asset is at/above the threshold, N
// parallel Range requests saturate the pipe; otherwise — and whenever the
// upstream does not honor Range — the single-stream resumable path is used.
func (t *masterAssetTransferer) Transfer(ctx context.Context, reportCtx context.Context, req downloader.DownloadRequest, onProgress func(downloadedBytes int64)) (downloader.TransferResult, error) {
	if t.shouldChunk(req) {
		result, err := t.transferChunked(ctx, reportCtx, req, onProgress)
		if err == nil || !errors.Is(err, errChunkRangeUnsupported) {
			return result, err
		}
		// The upstream ignored or malformed the Range header for a ranged
		// request: fall back to the single-stream resume path, which has its
		// own "server ignores Range" handling. transferChunked already removed
		// the partial, so the fallback starts from a clean slate.
	}
	return t.transferSingleStream(ctx, reportCtx, req, onProgress)
}

// transferSingleStream is the original resumable single-connection pipeline.
// The retry loop is classification-aware: 401/403/404 and other permanent 4xx
// are terminal; 408/429/5xx and transport errors are retried (1s/2s/4s +
// jitter).
func (t *masterAssetTransferer) transferSingleStream(ctx context.Context, reportCtx context.Context, req downloader.DownloadRequest, onProgress func(downloadedBytes int64)) (downloader.TransferResult, error) {
	w := t.w
	assetID := req.AssetID
	cacheDir := w.assetCacheDir()
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return downloader.TransferResult{}, err
	}

	// NOTE: cache miss accounting is intentionally NOT emitted here. The
	// canonical CacheResolver boundary records the classified miss exactly
	// once per resolution (attempt + worker views). This transfer only owns
	// the byte pipeline.
	source := t.assetSource(assetID)
	// Reserve this asset's partial before cleanup so an active transfer in
	// this process cannot be mistaken for an orphan. The cleanup is scoped
	// to this worker's asset cache and never touches final cache entries.
	partialPath := assetPartialPath(cacheDir, assetID, string(req.SHA256))
	deactivatePartial := markAssetPartialActive(partialPath)
	defer deactivatePartial()
	if _, cleanupErr := cleanupOrphanedAssetPartials(cacheDir, 24*time.Hour); cleanupErr != nil {
		w.logger.Warn("[ASSET] partial cleanup failed: %v", cleanupErr)
	}
	backoffs := downloader.BackoffSchedule(downloader.DefaultMaxAttempts, downloader.DefaultBaseBackoff, downloader.DefaultJitter)

	var lastErr error
	for attempt := 0; attempt < downloader.DefaultMaxAttempts; attempt++ {
		if attempt > 0 {
			wait := backoffs[attempt-1]
			if err := waitForAssetDuration(ctx, wait); err != nil {
				return downloader.TransferResult{}, err
			}
		}

		resumeOffset := assetPartialSize(cacheDir, assetID, string(req.SHA256))
		if !source.SupportsRange() {
			// The byte source cannot satisfy a suffix request: restart the
			// whole asset from byte zero (the writer truncates the partial).
			resumeOffset = 0
		}

		body, meta, openErr := source.Open(ctx, resumeOffset)
		if openErr != nil {
			lastErr = openErr
			var re *retryableStatusError
			var pe *permanentStatusError
			switch {
			case errors.Is(openErr, errAssetNotFound):
				return downloader.TransferResult{}, fmt.Errorf("asset not found")
			case errors.Is(openErr, errRangeNotSatisfiable):
				if resumeOffset <= 0 {
					return downloader.TransferResult{}, openErr
				}
				removeAssetPartial(cacheDir, assetID, string(req.SHA256))
				continue
			case errors.Is(openErr, errRangeIgnored) && resumeOffset > 0:
				// The upstream ignored the Range header (or returned a
				// mismatched Content-Range): discard the stale partial and
				// restart from byte zero within this attempt.
				removeAssetPartial(cacheDir, assetID, string(req.SHA256))
				body, meta, openErr = source.Open(ctx, 0)
				if openErr != nil {
					lastErr = openErr
					if errors.Is(openErr, errAssetNotFound) {
						return downloader.TransferResult{}, fmt.Errorf("asset not found")
					}
					if errors.As(openErr, &re) {
						if re.retryAfter > 0 && attempt+1 < len(backoffs)+1 {
							backoffs[attempt] = re.retryAfter
						}
						continue
					}
					return downloader.TransferResult{}, openErr
				}
				resumeOffset = 0
			case errors.As(openErr, &re):
				if re.retryAfter > 0 && attempt+1 < len(backoffs)+1 {
					backoffs[attempt] = re.retryAfter
				}
				continue
			case errors.As(openErr, &pe):
				// Permanent status (auth/forbidden/other 4xx): terminal.
				return downloader.TransferResult{}, openErr
			default:
				// Transport error or other transient failure: retry.
				continue
			}
		}

		transfer := telemetry.RecorderFromContext(reportCtx)
		transferHandle := (*telemetry.EventHandle)(nil)
		if transfer != nil {
			transferHandle = transfer.Begin(telemetry.EventSpec{Origin: telemetry.OriginWorker, Scope: telemetry.ScopeTask, Component: "worker.asset", Action: "transfer"})
			transferHandle.SetMetadata("asset_id", assetID)
		}
		// Report streamed bytes to the download manager so throttled
		// progress snapshots (percent, throughput, ETA) stay live during the
		// transfer.
		if onProgress != nil {
			// Each retry is a fresh logical attempt. Preserve bytes already
			// present in a resumable partial before the suffix starts.
			if attempt > 0 {
				onProgress(resumeOffset)
			}
			body = &assetProgressBody{ctx: ctx, src: body, onProgress: onProgress, done: resumeOffset, maxBPS: req.MaxBandwidthBytesPerSecond}
		}
		localPath, downloadedBytes, actualSHA, verifyDuration, err := writeVeloxAssetStreamToCacheAtOffset(cacheDir, assetID, string(req.SHA256), req.SizeBytes, body, resumeOffset, meta.MIMEType, meta.SizeBytes, syncAssetDirectory)
		body.Close()
		if err != nil {
			recordCacheProjectionEvent(reportCtx, "hash_verify", verifyDuration, telemetry.StatusFailed, "", 0)
			if transferHandle != nil {
				transferHandle.Abort("asset_transfer", err.Error())
			}
			lastErr = err
			if errors.Is(err, ErrAssetVerification) {
				return downloader.TransferResult{}, fmt.Errorf("%w: %v", downloader.ErrVerify, err)
			}
			continue
		}
		recordCacheProjectionEvent(reportCtx, "hash_verify", verifyDuration, telemetry.StatusOK, "", 0)
		if transferHandle != nil {
			transferHandle.CompleteWith(downloadedBytes, downloadedBytes, 0, telemetry.StatusOK, "", "")
		}
		// Remember the self-verified digest so later partial-metadata accesses
		// for the same asset become verified cache hits (no re-download).
		w.rememberAssetIntegrity(assetID, actualSHA, downloadedBytes)
		verifiedHash := assetref.ContentHash(actualSHA)
		if w.canonicalAssetCache != nil {
			key := assetref.AssetKey(req.AssetKey)
			entry := workercache.Entry{AssetKey: key, LocalPath: localPath}
			if err := w.canonicalAssetCache.Store(ctx, entry); err != nil && !errors.Is(err, workercache.ErrDuplicate) {
				return downloader.TransferResult{}, fmt.Errorf("register verified asset %s: %w", key, err)
			}
			if err := w.canonicalAssetCache.MarkDownloadCompleteWithHash(ctx, key, localPath, downloadedBytes, verifiedHash); err != nil {
				return downloader.TransferResult{}, fmt.Errorf("commit verified asset %s: %w", key, err)
			}
		}
		return downloader.TransferResult{LocalPath: localPath, Bytes: downloadedBytes, SHA256: verifiedHash}, nil
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("download failed")
	}
	return downloader.TransferResult{}, fmt.Errorf("failed to download velox asset %s: %w", assetID, lastErr)
}

// assetTransferRequest builds the master-bridge download URL, the bearer token
// and the redirect-hardened HTTP client shared by the single-stream and
// chunked pipelines (single source of truth for the integration boundary).
func (t *masterAssetTransferer) assetTransferRequest(assetID string) (downloadURL, authToken string, client *http.Client) {
	w := t.w
	downloadURL = strings.TrimRight(strings.TrimSpace(w.config.MasterURL), "/") + "/api/v1/agent/assets/" + neturl.PathEscape(assetID)
	if w.apiClient != nil {
		authToken = strings.TrimSpace(w.apiClient.AuthToken())
	}
	baseHost := ""
	if parsed, err := neturl.Parse(strings.TrimSpace(w.config.MasterURL)); err == nil {
		baseHost = strings.ToLower(strings.TrimSpace(parsed.Host))
	}
	client = &http.Client{
		Timeout: 2 * time.Minute,
		CheckRedirect: func(r *http.Request, via []*http.Request) error {
			if len(via) >= 5 {
				return fmt.Errorf("too many redirects")
			}
			if baseHost != "" && strings.ToLower(strings.TrimSpace(r.URL.Host)) != baseHost {
				return fmt.Errorf("redirected to unexpected host")
			}
			return nil
		},
	}
	return downloadURL, authToken, client
}

// assetSource builds the downloader.AssetSource seam for one asset. The URL,
// token and client are still produced by assetTransferRequest (single source
// of truth for the integration boundary); the source is the pluggable
// byte-open layer on top, shared by the resume pipeline.
func (t *masterAssetTransferer) assetSource(assetID string) downloader.AssetSource {
	downloadURL, authToken, client := t.assetTransferRequest(assetID)
	return newHTTPAssetSource(downloadURL, authToken, client)
}

// shouldChunk reports whether req should use the parallel chunked path: the
// feature is enabled, the asset size is known, and it meets the threshold.
func (t *masterAssetTransferer) shouldChunk(req downloader.DownloadRequest) bool {
	enabled, threshold, _ := t.chunkedConfig()
	return enabled && req.SizeBytes > 0 && req.SizeBytes >= threshold
}

// chunkedConfig resolves the effective chunked-download settings from the
// worker config, applying the canonical defaults for zero values so callers
// never see a misconfigured (threshold=0, concurrency=0) combination.
func (t *masterAssetTransferer) chunkedConfig() (enabled bool, threshold int64, concurrency int) {
	if t == nil || t.w == nil || t.w.config == nil {
		return false, 0, 0
	}
	cfg := t.w.config
	if !cfg.AssetChunkedDownloadEnabled {
		return false, 0, 0
	}
	threshold = cfg.AssetChunkedDownloadThresholdBytes
	if threshold <= 0 {
		threshold = config.DefaultAssetChunkedDownloadThresholdBytes
	}
	concurrency = cfg.AssetChunkedDownloadConcurrency
	if concurrency < 1 {
		concurrency = config.DefaultAssetChunkedDownloadConcurrency
	}
	return true, threshold, concurrency
}

// errChunkRangeUnsupported marks an upstream that returned a full 200 (or a
// malformed/absent Content-Range) for a ranged request. The Transfer
// dispatcher falls back to the single-stream path on this error.
var errChunkRangeUnsupported = errors.New("chunked: upstream does not honor Range requests")

// chunkRange is one [start, end] inclusive byte window of a chunked download.
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
// error. shared/report aggregate progress across all chunk goroutines.
func fetchChunkRange(ctx context.Context, client *http.Client, downloadURL, authToken string, c chunkRange, w io.WriterAt, shared *atomic.Int64, report func(), limiter *sharedBandwidthLimiter, sniffHTML bool) error {
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
		if authToken != "" {
			reqHTTP.Header.Set("Authorization", "Bearer "+authToken)
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
