package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	neturl "net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"velox-shared/assetref"
	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"
	"velox-worker-agent/internal/downloader"
	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/internal/workercache"
)

// downloadVeloxAssetWithMetadata is the integrity-aware asset path. It is now
// a THIN ADAPTER over the canonical CacheResolver: build the explicit
// DownloadRequest, Resolve it through the structured resolution surface (the
// single point where cache telemetry is emitted), then record the
// caller-scoped operation detail. All byte transport, dedup, state tracking,
// pooling, outcome classification and verification live in internal/downloader;
// this function never touches the network itself.
func (w *Worker) downloadVeloxAssetWithMetadata(ctx context.Context, assetID, expectedSHA256 string, expectedSizeBytes int64) (string, error) {
	operationStarted := time.Now().UTC()
	accessStarted := time.Now()
	cacheDir := w.assetCacheDir()
	loggedAccess := false
	defer func() {
		if !loggedAccess {
			logAssetCacheAccess(ctx, w.config.WorkerID, cacheAssetKey(assetID, expectedSHA256), "error", 0, time.Since(accessStarted).Milliseconds(), 0)
		}
	}()
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return "", err
	}

	jobID, role := telemetry.CacheAccessContextFromContext(ctx)
	resolution, err := w.assetCacheResolver().Resolve(ctx, downloader.DownloadRequest{
		JobID:     jobID,
		AssetKey:  assetref.AssetKey(assetID),
		AssetID:   assetID,
		Role:      downloader.RoleFromString(role),
		SHA256:    assetref.ContentHash(expectedSHA256),
		SizeBytes: expectedSizeBytes,
		Source:    "master_asset_bridge",
		Priority:  downloader.DefaultPriority,
	})
	if err != nil {
		return "", fmt.Errorf("failed to download velox asset %s: %w", assetID, err)
	}

	completed := time.Now().UTC()
	status := "miss"
	downloadedBytes := resolution.DownloadBytes
	downloadMS := completed.Sub(operationStarted).Milliseconds()
	if resolution.CacheHit {
		status = "hit"
		downloadedBytes = 0
		downloadMS = 0
	}
	recordAssetOperation(ctx, AssetOperationRecord{
		AssetID:             assetID,
		CacheStatus:         status,
		DownloadStartedAt:   operationStarted,
		DownloadCompletedAt: completed,
		DownloadMS:          downloadMS,
		DownloadedBytes:     downloadedBytes,
		SHA256Verified:      expectedSHA256 != "",
		IntegrityCheck:      integrityCheck(expectedSHA256, expectedSizeBytes),
		IntegrityValid:      expectedSHA256 != "" && expectedSizeBytes > 0,
		LocalPath:           resolution.LocalPath,
		Source:              "master_asset_bridge",
	})
	syncSize := resolution.DownloadBytes
	if resolution.CacheHit {
		// A cache hit reports zero downloaded bytes; the durable index should
		// still record the expected file size.
		syncSize = expectedSizeBytes
		if syncSize <= 0 {
			// Legacy hit: the payload carried no size, use the remembered
			// self-verified size so the durable index stays honest.
			if remembered, ok := w.rememberedAssetIntegrity(assetID); ok {
				syncSize = remembered.SizeBytes
			}
		}
	}
	if err := w.syncClipCache(ctx, assetID, resolution.LocalPath, syncSize, assetref.ContentHash(resolution.SHA256)); err != nil {
		return "", fmt.Errorf("record downloaded asset %s: %w", assetID, err)
	}
	logAssetCacheAccess(ctx, w.config.WorkerID, cacheAssetKey(assetID, expectedSHA256), status, downloadedBytes, time.Since(accessStarted).Milliseconds(), 0)
	loggedAccess = true
	return resolution.LocalPath, nil
}

// assetCacheResolver returns the canonical structured-resolution adapter over
// the shared download manager, building it lazily on first use (mirrors the
// manager's lazy construction). The wired sink is the single emission point
// for cache telemetry: every resolution feeds the attempt-scoped tracker
// (per-attempt counters starting at zero) AND the worker-lifetime Prometheus
// view. Rebuilt after Stop() nils the manager.
func (w *Worker) assetCacheResolver() *downloader.CacheResolver {
	w.cacheResolverMu.Lock()
	defer w.cacheResolverMu.Unlock()
	if w.cacheResolver == nil {
		w.cacheResolver = downloader.NewCacheResolver(w.assetDownloadManager(), cacheResolutionSink{})
	}
	return w.cacheResolver
}

// assetDownloadManager returns the worker's canonical download manager,
// constructing it lazily on first use so bare test Workers keep working
// without explicit wiring. The manager's pool size comes from
// VELOX_ASSET_DOWNLOAD_CONCURRENCY (default 4).
func (w *Worker) assetDownloadManager() downloader.AssetDownloadManager {
	w.assetManagerMu.Lock()
	defer w.assetManagerMu.Unlock()
	if w.assetManager == nil {
		w.assetManager = downloader.NewManager(downloader.Config{
			Concurrency: w.assetDownloadConcurrency(),
			// Durable progress checkpoints (throttled ~2s / 16MB by the
			// manager): emitted as worker-side telemetry events that feed the
			// master's asset-download read model. Non-blocking by contract.
			OnCheckpoint: func(snap downloader.DownloadSnapshot, reportCtx context.Context) {
				progressCtx := context.WithValue(reportCtx, assetProgressWorkerContextKey{}, w)
				emitAssetProgressCheckpoint(progressCtx, snap)
			},
			OnOperationalSnapshot: func(snapshot downloader.OperationalSnapshot) {
				telemetry.GetPrometheusMetrics().SetAssetDownloadOperational(
					snapshot.ActiveTransfers, snapshot.QueuedTransfers, snapshot.ReadyTransfers,
					snapshot.FailedTransfers, snapshot.CacheHitTransfers, snapshot.BytesDownloaded,
					snapshot.BytesTotal, snapshot.ThroughputBPS, float64(snapshot.ETASeconds),
					snapshot.CoalescedRequestsTotal,
				)
			},
			OnCoalescedRequest: func(sizeBytes int64) {
				telemetry.GetPrometheusMetrics().RecordCacheDuplicateDownload(sizeBytes)
			},
		}, &masterAssetTransferer{w: w})
	}
	return w.assetManager
}

// emitAssetProgressCheckpoint records one durable asset-download checkpoint
// event (origin: worker, scope: task). The manager throttles these to ~2s or
// 16MB per transfer; terminal transitions always checkpoint. The first
// waiter's telemetry context supplies the recorder — value-reads only, per
// the Transferer context contract.
func emitAssetProgressCheckpoint(ctx context.Context, snap downloader.DownloadSnapshot) {
	// The checkpoint hook is deliberately non-blocking with respect to the
	// downloader. A single FIFO sender owns all progress sends, preserving
	// checkpoint order while transport reconnects are handled by taking a
	// synchronized snapshot of the current session at send time.
	if w, ok := workerFromProgressContext(ctx); ok && w != nil {
		jobs := append([]string(nil), snap.JobIDs...)
		sort.Strings(jobs)
		msg := &pb.AssetDownloadProgress{
			WorkerId: w.config.WorkerID, TransferId: snap.TransferID,
			AssetKey: string(snap.AssetKey), AssetId: snap.AssetID, Role: string(snap.Role),
			State: string(snap.State), BytesDownloaded: snap.BytesDownloaded,
			BytesTotal: snap.BytesTotal, BytesPerSecond: snap.ThroughputBytesPerSecond,
			EtaSeconds: snap.ETASeconds, Attempt: int32(snap.Attempt),
			SharedWaiters: int32(snap.SharedWaiters), CacheHit: snap.CacheHit,
			QueuedAtUnixMs: unixMillis(snap.QueuedAt), StartedAtUnixMs: unixMillis(snap.StartedAt),
			UpdatedAtUnixMs: unixMillis(snap.UpdatedAt), CompletedAtUnixMs: unixMillis(snap.CompletedAt),
			JobIds: jobs, TaskId: snap.TaskID, SceneIds: append([]string(nil), snap.SceneIDs...),
			MimeType: snap.MIMEType, Sha256: string(snap.SHA256), ErrorCode: snap.ErrorCode, ErrorDetail: snap.ErrorDetail,
			CheckpointSequence: snap.CheckpointSequence,
			TransferGeneration: snap.TransferGeneration,
		}
		for _, ref := range snap.JobRefs {
			msg.JobRefs = append(msg.JobRefs, &pb.AssetJobReference{JobId: ref.JobID, TaskId: ref.TaskID, SceneIds: append([]string(nil), ref.SceneIDs...)})
		}
		w.enqueueAssetProgress(msg)
	}

	rec := telemetry.RecorderFromContext(ctx)
	if rec == nil {
		return
	}
	h := rec.Begin(telemetry.EventSpec{
		Origin: telemetry.OriginWorker, Scope: telemetry.ScopeTask,
		Component: "worker.asset", Action: "progress_checkpoint",
	})
	h.SetMetadata("asset_id", snap.AssetID)
	h.SetMetadata("transfer_id", snap.TransferID)
	h.SetMetadata("state", string(snap.State))
	h.SetMetadata("progress_percent", fmt.Sprintf("%.1f", snap.ProgressPercent))
	h.SetMetadata("bytes_downloaded", snap.BytesDownloaded)
	h.SetMetadata("bytes_total", snap.BytesTotal)
	h.SetMetadata("throughput_bps", int64(snap.ThroughputBytesPerSecond))
	h.SetMetadata("eta_seconds", snap.ETASeconds)
	h.Complete()
}

func unixMillis(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}

// workerFromProgressContext is intentionally a placeholder until the worker
// context carrier is wired by the caller; the asset manager callback currently
// receives only the telemetry report context. It returns false for contexts
// without the worker carrier, preserving headless tests.
func workerFromProgressContext(ctx context.Context) (*Worker, bool) {
	if ctx == nil {
		return nil, false
	}
	w, ok := ctx.Value(assetProgressWorkerContextKey{}).(*Worker)
	return w, ok
}

type assetProgressWorkerContextKey struct{}

type assetProgressEnvelope struct {
	message *pb.AssetDownloadProgress
}

func (w *Worker) enqueueAssetProgress(message *pb.AssetDownloadProgress) {
	if w == nil || message == nil {
		return
	}
	w.assetProgressOnce.Do(func() {
		w.assetProgressQueue = make(chan assetProgressEnvelope, 256)
		go w.assetProgressSender()
	})
	select {
	case w.assetProgressQueue <- assetProgressEnvelope{message: message}:
	case <-w.stopChan:
	default:
		// Progress is diagnostic and throttled. Dropping an intermediate
		// checkpoint under backpressure is preferable to stalling a byte
		// transfer; the next checkpoint or terminal event supersedes it.
		w.logger.Warn("[ASSET_PROGRESS] checkpoint queue full; dropping update")
	}
}

func (w *Worker) assetProgressSender() {
	for {
		select {
		case <-w.stopChan:
			return
		case envelope := <-w.assetProgressQueue:
			if envelope.message == nil {
				continue
			}
			w.transportMu.RLock()
			transport := w.transport
			w.transportMu.RUnlock()
			if transport == nil {
				continue
			}
			w.assetProgressSendMu.Lock()
			err := transport.Send(context.Background(), controltransport.NewTypedMessage(controltransport.MsgAssetDownloadProgress, w.config.WorkerID, w.config.ProtocolVersion, envelope.message))
			w.assetProgressSendMu.Unlock()
			if err != nil {
				w.logger.Warn("[ASSET_PROGRESS] send failed: %v", err)
			}
		}
	}
}

func (w *Worker) assetDownloadConcurrency() int {
	if w.config != nil && w.config.AssetDownloadConcurrency > 0 {
		return w.config.AssetDownloadConcurrency
	}
	return downloader.DefaultAssetConcurrency
}

// masterAssetTransferer implements downloader.Transferer with the worker's
// existing integrity-aware byte pipeline: cache probe on the canonical
// assets directory, then an authenticated HTTP GET on the master asset bridge
// with atomic .part-style promotion (writeVeloxAssetToCache) and size+SHA-256
// verification.
//
// Context contract (see downloader/source.go): ctx is the TRANSFER context
// (cancellation only); reportCtx is the first waiter's caller context and is
// used purely for non-blocking telemetry value reads.
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
				if info, statErr := os.Stat(entry.LocalPath); statErr == nil && info.Mode().IsRegular() {
					return downloader.CacheCheckResult{CacheHit: true, LocalPath: entry.LocalPath, SHA256: entry.ContentHash, Outcome: downloader.CacheOutcomeHitValid}, nil
				}
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
		if existing, _, err := cachedAssetPathTimed(w.assetCacheDir(), req.AssetID, probeSHA, probeSize); err == nil && existing != "" {
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
// cache, verifying size and SHA-256 before the file becomes visible. The
// retry loop is classification-aware: 401/403/404 and other permanent 4xx are
// terminal; 408/429/5xx and transport errors are retried (1s/2s/4s + jitter).
func (t *masterAssetTransferer) Transfer(ctx context.Context, reportCtx context.Context, req downloader.DownloadRequest, onProgress func(downloadedBytes int64)) (downloader.TransferResult, error) {
	w := t.w
	assetID := req.AssetID
	cacheDir := w.assetCacheDir()
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return downloader.TransferResult{}, err
	}

	transferStarted := time.Now()

	// NOTE: cache miss accounting is intentionally NOT emitted here. The
	// canonical CacheResolver boundary records the classified miss exactly
	// once per resolution (attempt + worker views). This transfer only owns
	// the byte pipeline.
	downloadURL := strings.TrimRight(strings.TrimSpace(w.config.MasterURL), "/") + "/api/v1/agent/assets/" + neturl.PathEscape(assetID)
	authToken := ""
	if w.apiClient != nil {
		authToken = strings.TrimSpace(w.apiClient.AuthToken())
	}
	baseHost := ""
	if parsed, err := neturl.Parse(strings.TrimSpace(w.config.MasterURL)); err == nil {
		baseHost = strings.ToLower(strings.TrimSpace(parsed.Host))
	}
	client := &http.Client{
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
			select {
			case <-ctx.Done():
				return downloader.TransferResult{}, ctx.Err()
			case <-time.After(wait):
			}
		}

		resumeOffset := assetPartialSize(cacheDir, assetID, string(req.SHA256))
		reqHTTP, err := http.NewRequestWithContext(ctx, http.MethodGet, downloadURL, nil)
		if err != nil {
			return downloader.TransferResult{}, err
		}
		if authToken != "" {
			reqHTTP.Header.Set("Authorization", "Bearer "+authToken)
		}
		if resumeOffset > 0 {
			reqHTTP.Header.Set("Range", fmt.Sprintf("bytes=%d-", resumeOffset))
		}

		resp, err := client.Do(reqHTTP)
		if err != nil {
			lastErr = err
			continue
		}

		if resp.StatusCode == http.StatusRequestedRangeNotSatisfiable && resumeOffset > 0 {
			resp.Body.Close()
			removeAssetPartial(cacheDir, assetID, string(req.SHA256))
			lastErr = fmt.Errorf("range offset %d is no longer valid", resumeOffset)
			continue
		}
		if resp.StatusCode == http.StatusNotFound {
			resp.Body.Close()
			return downloader.TransferResult{}, fmt.Errorf("asset not found")
		}
		if downloader.IsPermanentStatus(resp.StatusCode) {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			resp.Body.Close()
			return downloader.TransferResult{}, fmt.Errorf("asset download failed: %s", strings.TrimSpace(string(body)))
		}
		if downloader.IsRetryableStatus(resp.StatusCode) {
			retryAfter := downloader.RetryAfter(resp)
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			resp.Body.Close()
			lastErr = fmt.Errorf("master returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
			if retryAfter > 0 && attempt+1 < len(backoffs)+1 {
				backoffs[attempt] = retryAfter
			}
			continue
		}
		if resumeOffset > 0 && resp.StatusCode != http.StatusPartialContent {
			// A server that ignores Range is safe: the writer truncates the
			// old partial and starts from byte zero, preventing concatenation
			// of a complete response onto stale bytes.
			resumeOffset = 0
		}
		if resumeOffset > 0 {
			contentRange := strings.TrimSpace(resp.Header.Get("Content-Range"))
			start, _, _, parseErr := parseAssetContentRange(contentRange)
			if parseErr != nil || start != resumeOffset {
				resp.Body.Close()
				// The upstream did not honor the requested offset safely.
				// Discard the partial before retrying so the next attempt
				// cannot concatenate a full/ambiguous response onto it.
				removeAssetPartial(cacheDir, assetID, string(req.SHA256))
				lastErr = fmt.Errorf("invalid Content-Range for resume offset %d: %q", resumeOffset, contentRange)
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
			// from failed attempts must not make a later attempt look READY.
			if attempt > 0 {
				// Preserve the bytes already on disk when a retry resumes a
				// partial; progress must not regress while the suffix is fetched.
				onProgress(resumeOffset)
			}
			resp.Body = &assetProgressBody{src: resp.Body, onProgress: onProgress, done: resumeOffset}
		}
		localPath, downloadedBytes, actualSHA, verifyDuration, err := writeVeloxAssetToCacheAtOffset(cacheDir, assetID, string(req.SHA256), req.SizeBytes, resp, resumeOffset)
		resp.Body.Close()
		if err != nil {
			telemetry.GetPrometheusMetrics().RecordCacheVerify(verifyDuration)
			if transferHandle != nil {
				transferHandle.Abort("asset_transfer", err.Error())
			}
			lastErr = err
			if errors.Is(err, ErrAssetVerification) {
				return downloader.TransferResult{}, fmt.Errorf("%w: %v", downloader.ErrVerify, err)
			}
			continue
		}
		telemetry.GetPrometheusMetrics().RecordCacheVerify(verifyDuration)
		telemetry.GetPrometheusMetrics().RecordCacheDownload(downloadedBytes, time.Since(transferStarted))
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

// assetProgressBody wraps an http response body to report streamed bytes to
// the download manager's progress hook (one call per read chunk; the manager
// throttles its own publishes). Counts bytes actually read, so a partial or
// aborted stream reports exactly the bytes received.
type assetProgressBody struct {
	src        io.ReadCloser
	onProgress func(downloadedBytes int64)
	done       int64
}

func (p *assetProgressBody) Read(b []byte) (int, error) {
	n, err := p.src.Read(b)
	if n > 0 {
		p.done += int64(n)
		p.onProgress(p.done)
	}
	return n, err
}

func (p *assetProgressBody) Close() error { return p.src.Close() }

func parseAssetContentRange(value string) (start, end, total int64, err error) {
	value = strings.TrimSpace(value)
	if !strings.HasPrefix(value, "bytes ") {
		return 0, 0, 0, fmt.Errorf("invalid range unit")
	}
	parts := strings.SplitN(strings.TrimPrefix(value, "bytes "), "/", 2)
	if len(parts) != 2 {
		return 0, 0, 0, fmt.Errorf("invalid range shape")
	}
	bounds := strings.SplitN(parts[0], "-", 2)
	if len(bounds) != 2 {
		return 0, 0, 0, fmt.Errorf("invalid range bounds")
	}
	start, err = strconv.ParseInt(bounds[0], 10, 64)
	if err != nil {
		return 0, 0, 0, err
	}
	end, err = strconv.ParseInt(bounds[1], 10, 64)
	if err != nil || end < start {
		return 0, 0, 0, fmt.Errorf("invalid range end")
	}
	if parts[1] == "*" {
		return start, end, -1, nil
	}
	total, err = strconv.ParseInt(parts[1], 10, 64)
	if err != nil || total <= end {
		return 0, 0, 0, fmt.Errorf("invalid range total")
	}
	return start, end, total, nil
}

// syncClipCache records a verified asset in the durable remote-worker index.
// The content-addressed byte cache remains the data source; this index owns
// leases, protected snapshots and eviction decisions.
func (w *Worker) syncClipCache(ctx context.Context, assetID, localPath string, sizeBytes int64, hash assetref.ContentHash) error {
	if w.canonicalAssetCache == nil {
		return nil
	}
	key := assetref.AssetKey(assetID)
	entry := workercache.Entry{AssetKey: key, ContentHash: hash, LocalPath: localPath, SizeBytes: sizeBytes, DownloadComplete: true}
	if err := w.canonicalAssetCache.Store(ctx, entry); err != nil {
		if !errors.Is(err, workercache.ErrDuplicate) {
			return err
		}
		if err := w.canonicalAssetCache.PreserveContentHash(ctx, key, localPath, sizeBytes, hash); err != nil {
			return err
		}
	}
	return w.canonicalAssetCache.MarkUsed(ctx, key)
}
