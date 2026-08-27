package worker

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	neturl "net/url"
	"os"
	"strings"
	"time"

	"velox-shared/assetref"
	"velox-worker-agent/internal/downloader"
	"velox-worker-agent/internal/prefetch"
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
				// The durable index claims a complete entry, but the physical
				// blob may still be gone (evicted/expired underneath the index,
				// or a root-less fixture whose cache layer skipped the stat).
				// Only a present, regular, non-empty file is a valid hit;
				// otherwise fall through to the probe for a final chance before
				// classifying MISS_EXPIRED.
				if info, statErr := os.Stat(entry.LocalPath); statErr == nil && info.Mode().IsRegular() && info.Size() > 0 {
					return downloader.CacheCheckResult{CacheHit: true, LocalPath: entry.LocalPath, SHA256: entry.ContentHash, Outcome: downloader.CacheOutcomeHitValid}, nil
				}
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
// The retry loop is classification-aware: 403/404 and other permanent 4xx
// are terminal; 401/408/429/5xx and transport errors are retried (1s/2s/4s +
// jitter). The 401 heals because the token getter re-reads the re-issued
// session token on each retry.
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
				// Permanent status (forbidden/not-found/other non-retryable
				// 4xx): terminal.
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
			var np prefetch.NetworkPacer
			if t.w != nil && t.w.networkAdmissionController != nil {
				np = t.w.networkAdmissionController
			}
			body = &assetProgressBody{ctx: ctx, src: body, onProgress: onProgress, done: resumeOffset, maxBPS: req.MaxBandwidthBytesPerSecond, networkPacer: np}
		}
		localPath, downloadedBytes, actualSHA, hashVerifyMS, materializeLocalMS, err := writeVeloxAssetStreamToCacheAtOffset(cacheDir, assetID, string(req.SHA256), req.SizeBytes, body, resumeOffset, meta.MIMEType, meta.SizeBytes, syncAssetDirectory)
		body.Close()
		if err != nil {
			recordCacheProjectionEvent(reportCtx, "hash_verify", hashVerifyMS+materializeLocalMS, telemetry.StatusFailed, "", 0)
			if transferHandle != nil {
				transferHandle.Abort("asset_transfer", err.Error())
			}
			lastErr = err
			if errors.Is(err, ErrAssetVerification) {
				return downloader.TransferResult{}, fmt.Errorf("%w: %v", downloader.ErrVerify, err)
			}
			continue
		}
		recordCacheProjectionEvent(reportCtx, "hash_verify", hashVerifyMS+materializeLocalMS, telemetry.StatusOK, "", 0)
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
		return downloader.TransferResult{
			LocalPath: localPath,
			Bytes:     downloadedBytes,
			SHA256:    verifiedHash,
			Timing: downloader.TransferSubPhases{
				HashVerifyMS:       hashVerifyMS.Milliseconds(),
				MaterializeLocalMS: materializeLocalMS.Milliseconds(),
				DownloadWorkMS:     0, // single-stream work ~ wall; set by caller span
			},
		}, nil
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("download failed")
	}
	return downloader.TransferResult{}, fmt.Errorf("failed to download velox asset %s: %w", assetID, lastErr)
}

// assetTransferRequest builds the master-bridge download URL, the bearer-token
// GETTER and the redirect-hardened HTTP client shared by the single-stream and
// chunked pipelines (single source of truth for the integration boundary).
// The token is a getter (not a snapshot) so a retry after a master restart
// re-reads the freshly re-issued session token instead of reusing the stale/
// cleared one captured at transfer start.
func (t *masterAssetTransferer) assetTransferRequest(assetID string) (downloadURL string, authToken func() string, client *http.Client) {
	w := t.w
	downloadURL = strings.TrimRight(strings.TrimSpace(w.config.MasterURL), "/") + "/api/v1/agent/assets/" + neturl.PathEscape(assetID)
	authToken = func() string {
		if w.apiClient != nil {
			return strings.TrimSpace(w.apiClient.AuthToken())
		}
		return ""
	}
	baseHost := ""
	if parsed, err := neturl.Parse(strings.TrimSpace(w.config.MasterURL)); err == nil {
		baseHost = strings.ToLower(strings.TrimSpace(parsed.Host))
	}
	client = &http.Client{
		Transport: assetTransport,
		Timeout:   2 * time.Minute,
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
