package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	neturl "net/url"
	"os"
	"strings"
	"time"

	"velox-shared/assetref"
	"velox-worker-agent/internal/downloader"
	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/internal/workercache"
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
		if existing, _, err := cachedAssetPathTimedWithContext(reportCtx, w.assetCacheDir(), req.AssetID, probeSHA, probeSize); err == nil && existing != "" {
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
			if err := waitForAssetDuration(ctx, wait); err != nil {
				return downloader.TransferResult{}, err
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
			if attempt > 0 {
				onProgress(resumeOffset)
			}
			resp.Body = &assetProgressBody{ctx: ctx, src: resp.Body, onProgress: onProgress, done: resumeOffset, maxBPS: req.MaxBandwidthBytesPerSecond}
		}
		localPath, downloadedBytes, actualSHA, verifyDuration, err := writeVeloxAssetToCacheAtOffset(cacheDir, assetID, string(req.SHA256), req.SizeBytes, resp, resumeOffset, syncAssetDirectory)
		resp.Body.Close()
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