package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"velox-worker-agent/internal/downloader"
)

// asset_source_http.go is the production implementation of the
// downloader.AssetSource seam. It owns the redirect-hardened client, bearer
// auth, and the byte-range open against the master asset bridge, so the
// transfer pipelines fetch bytes through the pluggable interface instead of
// building raw HTTP requests inline. assetTransferRequest remains the single
// source of truth for the URL/token/client handed to newHTTPAssetSource.

// httpAssetSource adapts one master-bridge asset GET to downloader.AssetSource.
type httpAssetSource struct {
	baseURL   string
	authToken string
	client    *http.Client
}

func newHTTPAssetSource(baseURL, authToken string, client *http.Client) *httpAssetSource {
	return &httpAssetSource{baseURL: baseURL, authToken: authToken, client: client}
}

// SupportsRange reports whether the source can satisfy byte-range opens. The
// master asset bridge serves Range for both local and Drive-backed assets, so
// the HTTP source always advertises true. A non-compliant upstream that
// ignores Range is still handled safely: a ranged Open on such a server
// returns errRangeIgnored and the pipeline restarts from byte zero.
func (s *httpAssetSource) SupportsRange() bool { return true }

// Open issues one GET, requesting a byte suffix when offset > 0, and returns
// the response body plus size/MIME metadata. The returned body is owned by
// the caller. Non-2xx/206 outcomes are classified into sentinel/typed errors
// so the transfer retry loop can decide whether to retry, terminate, or
// restart from zero without reaching into net/http.
func (s *httpAssetSource) Open(ctx context.Context, offset int64) (io.ReadCloser, downloader.SourceMetadata, error) {
	if err := downloader.ValidateSourceOffset(offset); err != nil {
		return nil, downloader.SourceMetadata{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.baseURL, nil)
	if err != nil {
		return nil, downloader.SourceMetadata{}, err
	}
	if s.authToken != "" {
		req.Header.Set("Authorization", "Bearer "+s.authToken)
	}
	if offset > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
	}
	resp, err := s.client.Do(req)
	if err != nil {
		// Transport error: bare, so the retry loop treats it as transient.
		return nil, downloader.SourceMetadata{}, err
	}

	switch {
	case resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusPartialContent:
		if offset > 0 && resp.StatusCode != http.StatusPartialContent {
			// A server that ignored the Range header for a resumed request:
			// returning the full body would corrupt the partial, so signal
			// range-unsupported instead of silently returning bytes from zero.
			resp.Body.Close()
			return nil, downloader.SourceMetadata{}, errRangeIgnored
		}
		meta := downloader.SourceMetadata{
			SizeBytes: resp.ContentLength,
			MIMEType:  resp.Header.Get("Content-Type"),
		}
		if offset > 0 {
			contentRange := strings.TrimSpace(resp.Header.Get("Content-Range"))
			start, _, total, parseErr := parseAssetContentRange(contentRange)
			if parseErr != nil || start != offset {
				resp.Body.Close()
				return nil, downloader.SourceMetadata{}, errRangeIgnored
			}
			if total > 0 {
				meta.SizeBytes = total
			}
		}
		return resp.Body, meta, nil
	case resp.StatusCode == http.StatusNotFound:
		resp.Body.Close()
		return nil, downloader.SourceMetadata{}, errAssetNotFound
	case resp.StatusCode == http.StatusRequestedRangeNotSatisfiable:
		resp.Body.Close()
		return nil, downloader.SourceMetadata{}, errRangeNotSatisfiable
	case downloader.IsPermanentStatus(resp.StatusCode):
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		return nil, downloader.SourceMetadata{}, &permanentStatusError{
			statusCode: resp.StatusCode,
			body:       strings.TrimSpace(string(body)),
		}
	case downloader.IsRetryableStatus(resp.StatusCode):
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		return nil, downloader.SourceMetadata{}, &retryableStatusError{
			statusCode: resp.StatusCode,
			retryAfter: downloader.RetryAfter(resp),
			body:       strings.TrimSpace(string(body)),
		}
	default:
		// Any other status (3xx, unexpected 2xx) cannot safely satisfy the
		// requested window.
		resp.Body.Close()
		return nil, downloader.SourceMetadata{}, errRangeIgnored
	}
}

// Sentinel/typed errors returned by httpAssetSource.Open so the transfer
// retry loop can classify an open failure without inspecting the response.
var (
	errRangeIgnored        = errors.New("asset source: upstream ignored Range header")
	errRangeNotSatisfiable = errors.New("asset source: range offset no longer valid")
	errAssetNotFound       = errors.New("asset not found")
)

// retryableStatusError carries a retryable upstream status (408/429/5xx) so
// the retry loop can honour Retry-After and keep its attempt accounting.
type retryableStatusError struct {
	statusCode int
	retryAfter time.Duration
	body       string
}

func (e *retryableStatusError) Error() string {
	return fmt.Sprintf("master returned %d: %s", e.statusCode, e.body)
}

// permanentStatusError carries a permanent upstream status (auth/forbidden and
// other 4xx) that must never be retried.
type permanentStatusError struct {
	statusCode int
	body       string
}

func (e *permanentStatusError) Error() string {
	return fmt.Sprintf("asset download failed: %s", e.body)
}
