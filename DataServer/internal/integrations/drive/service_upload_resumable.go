package drive

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"velox-server/internal/credentials"
)

// resumableUploadThreshold is the Drive-recommended cutoff: files larger
// than 5 MB use the resumable protocol so the body is streamed in chunks
// (never buffered whole in memory) and an interrupted upload resumes from
// the committed offset instead of restarting from zero.
const resumableUploadThreshold int64 = 5 * 1024 * 1024

// resumableChunkSize is the per-chunk payload. Drive requires every chunk
// (except the final one) to be a multiple of 256 KiB; 8 MiB is 32×256 KiB.
// It is a var (not a const) so tests can shrink it to exercise multi-chunk
// sequencing without allocating multi-MB fixtures.
var resumableChunkSize int64 = 8 * 1024 * 1024

// maxResumableChunkAttempts bounds the per-chunk retry loop. A transient
// transport failure or a 5xx triggers a status query, and the chunk is
// retried from the committed offset; this cap prevents a pathological
// session from spinning forever.
const maxResumableChunkAttempts = 3

// chunkUploadError carries the HTTP status of a failed chunk PUT so the
// resume loop can fail fast on a permanent 4xx instead of retrying it as if
// it were a transient interruption.
type chunkUploadError struct {
	status int
	body   string
}

func (e *chunkUploadError) Error() string {
	return fmt.Sprintf("chunk upload returned %d: %s", e.status, e.body)
}

// resumableStatusTransient reports whether an HTTP status is worth resuming
// through a status query: 5xx, 408 and 429 are transient; other 4xx are
// permanent and resume cannot fix them.
func resumableStatusTransient(status int) bool {
	if status == http.StatusRequestTimeout || status == http.StatusTooManyRequests {
		return true
	}
	return status >= 500
}

// buildUploadMetadata renders the Drive file metadata shared by the
// multipart and resumable paths. deliveryID is stamped as a public
// properties key so retries of the same delivery are traceable without the
// drive.appdata OAuth scope.
func buildUploadMetadata(fileName, folderID, deliveryID string) ([]byte, error) {
	meta := map[string]interface{}{
		"name":    fileName,
		"parents": []string{folderID},
	}
	if deliveryID != "" {
		meta["properties"] = map[string]string{
			"velox_delivery_id": deliveryID,
		}
	}
	return json.Marshal(meta)
}

// uploadResumable drives the resumable protocol: initiate a session, then
// stream the file in fixed chunks. Each chunk is PUT with a Content-Range;
// a 308 means "accepted, send the next chunk" and a 200/201 means done. On a
// transient failure the committed offset is queried via `bytes */<total>`
// and the loop resumes from there; a permanent 4xx aborts immediately.
func (s *Service) uploadResumable(
	ctx context.Context,
	file *os.File,
	size int64,
	fileName, folderID, deliveryID string,
	token *Token,
) (*UploadResult, error) {
	metaJSON, err := buildUploadMetadata(fileName, folderID, deliveryID)
	if err != nil {
		return nil, fmt.Errorf("marshal upload metadata: %w", err)
	}

	// networkMS accumulates the Drive HTTP round-trips (init + chunk PUTs +
	// status queries); localMS accumulates the time spent reading the
	// artifact off local disk into each chunk buffer.
	var networkMS, localMS time.Duration
	sessionURI, initNetwork, err := s.initiateResumableUpload(ctx, metaJSON, size, token)
	networkMS += initNetwork
	if err != nil {
		return nil, err
	}

	var offset int64
	for offset < size {
		length := resumableChunkSize
		if remaining := size - offset; remaining < length {
			length = remaining
		}

		var (
			result       *UploadResult
			complete     bool
			chunkNetwork time.Duration
			chunkLocal   time.Duration
			lastErr      error
		)
		for attempt := 0; attempt < maxResumableChunkAttempts; attempt++ {
			result, complete, chunkLocal, chunkNetwork, lastErr = s.uploadResumableChunk(ctx, file, sessionURI, offset, length, size, token)
			networkMS += chunkNetwork
			localMS += chunkLocal
			if lastErr == nil {
				break
			}
			var chunkErr *chunkUploadError
			if errors.As(lastErr, &chunkErr) && !resumableStatusTransient(chunkErr.status) {
				return nil, lastErr // permanent 4xx: resume cannot fix it
			}
			log.Printf("[CLOUD] resumable chunk at %d failed (attempt %d/%d): %v",
				offset, attempt+1, maxResumableChunkAttempts, lastErr)
			// Recover the committed offset and resume. A chunk that was
			// fully committed but whose response was lost is skipped by
			// advancing offset past it.
			if committed, qNetwork, qErr := s.resumableCommittedOffset(ctx, sessionURI, size, token); qErr == nil {
				networkMS += qNetwork
				offset = committed + 1
				length = resumableChunkSize
				if remaining := size - offset; remaining < length {
					length = remaining
				}
			}
		}
		if lastErr != nil {
			return nil, fmt.Errorf("resumable upload failed at offset %d: %w", offset, lastErr)
		}
		if complete {
			result.NetworkMS = networkMS.Milliseconds()
			result.LocalBufferMS = localMS.Milliseconds()
			return result, nil
		}
		offset += length
	}

	return nil, fmt.Errorf("resumable upload session ended without a completed response")
}

// initiateResumableUpload creates the session and returns the Location
// (session URI) that the chunk PUTs target.
func (s *Service) initiateResumableUpload(ctx context.Context, metaJSON []byte, size int64, token *Token) (string, time.Duration, error) {
	const initURL = "https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, initURL, bytes.NewReader(metaJSON))
	if err != nil {
		return "", 0, fmt.Errorf("failed to create resumable init request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token.AccessToken)
	req.Header.Set("Content-Type", "application/json; charset=UTF-8")
	req.Header.Set("X-Upload-Content-Type", "application/octet-stream")
	req.Header.Set("X-Upload-Content-Length", strconv.FormatInt(size, 10))

	networkStart := time.Now()
	resp, err := s.httpClient.Do(req)
	networkMS := time.Since(networkStart)
	if err != nil {
		return "", networkMS, fmt.Errorf("resumable init failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		raw, _ := io.ReadAll(resp.Body)
		return "", networkMS, fmt.Errorf("resumable init failed (%d): %s", resp.StatusCode, credentials.JSON(string(raw)))
	}

	sessionURI := resp.Header.Get("Location")
	if sessionURI == "" {
		return "", networkMS, fmt.Errorf("resumable init did not return a session Location")
	}
	return sessionURI, networkMS, nil
}

// uploadResumableChunk PUTs one chunk. It returns (result, true, nil) on the
// final 200/201 completion, (nil, false, nil) on 308 Resume Incomplete, and
// a non-nil error on a transport failure or a non-308/200 HTTP status. The
// HTTP-status errors are *chunkUploadError so the caller can distinguish a
// permanent 4xx from a transient 5xx.
func (s *Service) uploadResumableChunk(
	ctx context.Context,
	file *os.File,
	sessionURI string,
	offset, length, total int64,
	token *Token,
) (*UploadResult, bool, time.Duration, time.Duration, error) {
	buf := make([]byte, length)
	localStart := time.Now()
	n, readErr := file.ReadAt(buf, offset)
	localMS := time.Since(localStart)
	if readErr != nil && readErr != io.EOF {
		return nil, false, localMS, 0, fmt.Errorf("read upload chunk at %d: %w", offset, readErr)
	}
	if int64(n) != length {
		return nil, false, localMS, 0, fmt.Errorf("short read at %d: got %d bytes, want %d", offset, n, length)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, sessionURI, bytes.NewReader(buf))
	if err != nil {
		return nil, false, localMS, 0, fmt.Errorf("failed to create chunk request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token.AccessToken)
	req.Header.Set("Content-Length", strconv.FormatInt(length, 10))
	req.Header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", offset, offset+length-1, total))

	networkStart := time.Now()
	resp, err := s.httpClient.Do(req)
	networkMS := time.Since(networkStart)
	if err != nil {
		return nil, false, localMS, networkMS, fmt.Errorf("chunk upload failed: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK, http.StatusCreated:
		var fileMeta File
		if err := json.NewDecoder(resp.Body).Decode(&fileMeta); err != nil {
			return nil, false, localMS, networkMS, fmt.Errorf("failed to decode resumable completion: %w", err)
		}
		return &UploadResult{
			Success:     true,
			FileID:      fileMeta.ID,
			WebViewLink: fileMeta.WebViewLink,
		}, true, localMS, networkMS, nil
	case http.StatusPermanentRedirect: // 308 Resume Incomplete
		return nil, false, localMS, networkMS, nil
	default:
		raw, _ := io.ReadAll(resp.Body)
		return nil, false, localMS, networkMS, &chunkUploadError{status: resp.StatusCode, body: credentials.JSON(string(raw))}
	}
}

// resumableCommittedOffset queries the session status (bytes */<total>) and
// returns the last committed byte index from the 308 Range header. The
// caller resumes from committed+1.
func (s *Service) resumableCommittedOffset(ctx context.Context, sessionURI string, total int64, token *Token) (int64, time.Duration, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, sessionURI, nil)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to create status query: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token.AccessToken)
	req.Header.Set("Content-Length", "0")
	req.Header.Set("Content-Range", fmt.Sprintf("bytes */%d", total))

	networkStart := time.Now()
	resp, err := s.httpClient.Do(req)
	networkMS := time.Since(networkStart)
	if err != nil {
		return 0, networkMS, fmt.Errorf("status query failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPermanentRedirect {
		return 0, networkMS, fmt.Errorf("status query returned %d, want 308", resp.StatusCode)
	}

	rangeHeader := resp.Header.Get("Range")
	// Format: "bytes=0-<last-committed-byte>".
	parts := strings.SplitN(rangeHeader, "-", 2)
	if len(parts) != 2 {
		return 0, networkMS, fmt.Errorf("status query returned unexpected Range header %q", rangeHeader)
	}
	committed, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
	if err != nil {
		return 0, networkMS, fmt.Errorf("parse Range header %q: %w", rangeHeader, err)
	}
	return committed, networkMS, nil
}
