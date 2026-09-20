package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	neturl "net/url"
	"strings"

	"golang.org/x/net/html"
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
	baseURL string
	// authToken is a getter so every Open (each retry) re-reads the worker's
	// CURRENT session token. The token is cleared on disconnect and re-issued
	// on reconnect, so a 401 during a master restart heals only if the retry
	// uses the fresh token instead of the one captured at transfer start.
	authToken func() string
	client    *http.Client
}

func newHTTPAssetSource(baseURL string, authToken func() string, client *http.Client) *httpAssetSource {
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
	if token := s.authToken(); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
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
		if isHTMLMediaType(resp.Header.Get("Content-Type")) && isDirectDriveSource(s.baseURL) {
			resp, err = s.followDriveConfirmation(ctx, resp, offset)
			if err != nil {
				return nil, downloader.SourceMetadata{}, err
			}
		}
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
		return nil, downloader.SourceMetadata{}, downloader.NewHTTPStatusError(
			resp.StatusCode, strings.TrimSpace(string(body)), 0)
	case downloader.IsRetryableStatus(resp.StatusCode):
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		return nil, downloader.SourceMetadata{}, downloader.NewHTTPStatusError(
			resp.StatusCode, strings.TrimSpace(string(body)), downloader.RetryAfter(resp))
	default:
		// Any other status (3xx, unexpected 2xx) cannot safely satisfy the
		// requested window.
		resp.Body.Close()
		return nil, downloader.SourceMetadata{}, errRangeIgnored
	}
}

// followDriveConfirmation handles Google's large-file download interstitial.
// Drive returns a small HTML form for files that require virus-scan
// confirmation. The form contains a short-lived UUID; a fixed confirm=t query
// is not sufficient by itself. Follow exactly that form, then let the normal
// range/size/integrity pipeline consume the resulting binary response.
func (s *httpAssetSource) followDriveConfirmation(ctx context.Context, resp *http.Response, offset int64) (*http.Response, error) {
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	resp.Body.Close()
	if readErr != nil {
		return nil, readErr
	}
	action, method, values, ok := parseDriveConfirmationForm(body)
	if !ok {
		return nil, errUnexpectedHTMLResponse
	}

	base, err := neturl.Parse(s.baseURL)
	if err != nil {
		return nil, errUnexpectedHTMLResponse
	}
	formURL, err := base.Parse(action)
	if err != nil || formURL.Scheme != "https" || !isAllowedDirectDriveHost(formURL.Hostname()) {
		return nil, fmt.Errorf("drive confirmation redirected to unexpected host")
	}

	var bodyReader io.Reader
	if strings.EqualFold(method, http.MethodPost) {
		bodyReader = strings.NewReader(values.Encode())
	} else {
		query := formURL.Query()
		for key, items := range values {
			for _, value := range items {
				query.Add(key, value)
			}
		}
		formURL.RawQuery = query.Encode()
		method = http.MethodGet
	}
	request, err := http.NewRequestWithContext(ctx, method, formURL.String(), bodyReader)
	if err != nil {
		return nil, err
	}
	if method == http.MethodPost {
		request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	if offset > 0 {
		request.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
	}
	confirmed, err := s.client.Do(request)
	if err != nil {
		return nil, err
	}
	if confirmed.StatusCode != http.StatusOK && confirmed.StatusCode != http.StatusPartialContent {
		confirmed.Body.Close()
		return nil, fmt.Errorf("drive confirmation returned HTTP %d", confirmed.StatusCode)
	}
	if isHTMLMediaType(confirmed.Header.Get("Content-Type")) {
		confirmed.Body.Close()
		return nil, errUnexpectedHTMLResponse
	}
	return confirmed, nil
}

// parseDriveConfirmationForm extracts Google's hidden download form without
// depending on a particular attribute order or HTML quoting style.
func parseDriveConfirmationForm(body []byte) (action, method string, values neturl.Values, ok bool) {
	tokenizer := html.NewTokenizer(bytes.NewReader(body))
	values = make(neturl.Values)
	inForm := false
	for {
		tokenType := tokenizer.Next()
		switch tokenType {
		case html.ErrorToken:
			return action, method, values, inForm && action != "" && values.Get("id") != ""
		case html.StartTagToken:
			token := tokenizer.Token()
			switch token.Data {
			case "form":
				if inForm {
					continue
				}
				inForm = true
				action = ""
				method = http.MethodGet
				for _, attr := range token.Attr {
					switch attr.Key {
					case "action":
						action = strings.TrimSpace(attr.Val)
					case "method":
						if strings.TrimSpace(attr.Val) != "" {
							method = strings.ToUpper(strings.TrimSpace(attr.Val))
						}
					}
				}
			case "input":
				if !inForm {
					continue
				}
				name, value := "", ""
				for _, attr := range token.Attr {
					switch attr.Key {
					case "name":
						name = strings.TrimSpace(attr.Val)
					case "value":
						value = attr.Val
					}
				}
				if name != "" {
					values.Add(name, value)
				}
			}
		case html.EndTagToken:
			token := tokenizer.Token()
			if token.Data == "form" && inForm {
				return action, method, values, action != "" && values.Get("id") != ""
			}
		}
	}
}

func isDirectDriveSource(sourceURI string) bool {
	parsed, err := neturl.Parse(strings.TrimSpace(sourceURI))
	return err == nil && parsed.Scheme == "https" && isAllowedDirectDriveHost(parsed.Hostname())
}

// Sentinel/typed errors returned by httpAssetSource.Open so the transfer
// retry loop can classify an open failure without inspecting the response.
var (
	errRangeIgnored           = errors.New("asset source: upstream ignored Range header")
	errRangeNotSatisfiable    = errors.New("asset source: range offset no longer valid")
	errAssetNotFound          = errors.New("asset not found")
	errUnexpectedHTMLResponse = errors.New("unexpected HTML response while downloading asset")
)
