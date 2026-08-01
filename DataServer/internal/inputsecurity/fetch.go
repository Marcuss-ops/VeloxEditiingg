package inputsecurity

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

// Fetched is a fully bounded, system-generated temporary download. Callers
// own the file and must remove it after opening or promoting it.
type Fetched struct {
	Path          string
	SuggestedName string
	MIMEType      string
	ExpectedSize  int64
	SourceType    string
}

type Fetcher struct{ policy Policy }

func NewFetcher(policy Policy) *Fetcher {
	policy = policy.withDefaults()
	return &Fetcher{policy: policy}
}

func (f *Fetcher) Metrics() *Metrics {
	if f == nil {
		return nil
	}
	return f.policy.Metrics
}

func (f *Fetcher) Policy() Policy {
	if f == nil {
		return Policy{}
	}
	return f.policy
}

// Quarantine moves a suspicious system-generated file out of the active
// staging path and records only bounded metadata, never the original URL.
func (f *Fetcher) Quarantine(path string, kind Kind, code ErrorCode, reason string) error {
	if f == nil {
		return newError(kind, ErrQuarantineFailed, "input security fetcher is unavailable", nil)
	}
	return f.policy.quarantine(path, kind, code, reason)
}

func (f *Fetcher) Fetch(ctx context.Context, rawURL string, kind Kind) (*Fetched, error) {
	if f == nil {
		return nil, newError(kind, ErrInvalidURL, "input security fetcher is unavailable", nil)
	}
	policy := f.policy
	if _, err := policy.validateURL(ctx, rawURL, false); err != nil {
		return nil, f.reject(kind, err, 0)
	}
	if strings.TrimSpace(policy.TempDir) == "" {
		policy.TempDir = os.TempDir()
	}
	if err := os.MkdirAll(policy.TempDir, 0o700); err != nil {
		return nil, f.reject(kind, newError(kind, ErrReadFailed, "cannot create secure temporary directory", err), 0)
	}
	tmp, err := os.CreateTemp(policy.TempDir, ".velox-input-*")
	if err != nil {
		return nil, f.reject(kind, newError(kind, ErrReadFailed, "cannot create secure temporary file", err), 0)
	}
	tmpPath := tmp.Name()
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return nil, f.reject(kind, newError(kind, ErrReadFailed, "cannot protect temporary file", err), 0)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return nil, f.reject(kind, newError(kind, ErrReadFailed, "cannot close temporary file", err), 0)
	}

	transferCtx, cancel := context.WithTimeout(ctx, policy.TransferTimeout)
	defer cancel()
	client := f.httpClient(policy)
	req, err := http.NewRequestWithContext(transferCtx, http.MethodGet, strings.TrimSpace(rawURL), nil)
	if err != nil {
		_ = os.Remove(tmpPath)
		return nil, f.reject(kind, newError(kind, ErrInvalidURL, "cannot create HTTP request", err), 0)
	}
	resp, err := client.Do(req)
	if err != nil {
		_ = os.Remove(tmpPath)
		code := CodeOf(err)
		if code == "" {
			if errors.Is(transferCtx.Err(), context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
				code = ErrDownloadTimeout
			} else {
				code = ErrReadFailed
			}
		}
		return nil, f.reject(kind, newError(kind, code, "secure HTTP request failed", err), 0)
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		_ = os.Remove(tmpPath)
		return nil, f.reject(kind, newError(kind, ErrHTTPStatus, fmt.Sprintf("upstream returned HTTP %d", resp.StatusCode), nil), 0)
	}
	if resp.ContentLength > policy.MaxBytes {
		_ = os.Remove(tmpPath)
		return nil, f.reject(kind, newError(kind, ErrDownloadTooLarge, "declared content length exceeds the input limit", nil), uint64(maxInt64(resp.ContentLength)))
	}
	out, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		_ = os.Remove(tmpPath)
		return nil, f.reject(kind, newError(kind, ErrReadFailed, "cannot open secure temporary file", err), 0)
	}
	limited := io.LimitReader(resp.Body, policy.MaxBytes+1)
	written, copyErr := io.Copy(out, limited)
	closeErr := out.Close()
	if copyErr != nil || closeErr != nil {
		code := ErrReadFailed
		if written > policy.MaxBytes {
			code = ErrDownloadTooLarge
		} else if errors.Is(copyErr, context.DeadlineExceeded) || errors.Is(copyErr, context.Canceled) || transferCtx.Err() != nil {
			code = ErrDownloadTimeout
		}
		_ = f.quarantineOrRemove(tmpPath, kind, code, "response body could not be fully read")
		if copyErr != nil {
			return nil, f.reject(kind, newError(kind, code, "response body could not be fully read", copyErr), uint64(maxInt64(written)))
		}
		return nil, f.reject(kind, newError(kind, ErrReadFailed, "secure temporary file could not be closed", closeErr), uint64(maxInt64(written)))
	}
	if written > policy.MaxBytes {
		_ = f.quarantineOrRemove(tmpPath, kind, ErrDownloadTooLarge, "stream exceeded the input byte limit")
		return nil, f.reject(kind, newError(kind, ErrDownloadTooLarge, "stream exceeded the input byte limit", nil), uint64(maxInt64(written)))
	}
	// Fetch may fill in a system temp directory when the caller supplied a
	// partial policy. Validate against that effective policy, otherwise a
	// valid download created in os.TempDir would fail its own path allowlist.
	validator := &Fetcher{policy: policy}
	validation, validationErr := validator.ValidateFile(ctx, tmpPath, kind, resp.Header.Get("Content-Type"))
	if validationErr != nil {
		_ = f.quarantineOrRemove(tmpPath, kind, CodeOf(validationErr), validationErr.Error())
		return nil, f.reject(kind, validationErr, uint64(maxInt64(written)))
	}
	return &Fetched{Path: tmpPath, SuggestedName: filenameFromURL(rawURL), MIMEType: validation.MIMEType, ExpectedSize: written, SourceType: "https"}, nil
}

func (f *Fetcher) httpClient(policy Policy) *http.Client {
	base := policy.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	transport := &secureRoundTripper{base: base, policy: policy}
	return &http.Client{
		Transport: transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= policy.MaxRedirects {
				return newError(KindUnknown, ErrRedirectLimit, "redirect limit exceeded", nil)
			}
			if _, err := policy.validateURL(req.Context(), req.URL.String(), true); err != nil {
				return err
			}
			return nil
		},
	}
}

type secureRoundTripper struct {
	base   http.RoundTripper
	policy Policy
}

func (t *secureRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if _, err := t.policy.validateURL(req.Context(), req.URL.String(), false); err != nil {
		return nil, err
	}
	if transport, ok := t.base.(*http.Transport); ok && !t.policy.AllowPrivateNetworks {
		clone := transport.Clone()
		clone.DialContext = t.policy.dialContext
		clone.ResponseHeaderTimeout = t.policy.ResponseHeaderTimeout
		clone.TLSHandshakeTimeout = t.policy.ConnectTimeout
		clone.MaxResponseHeaderBytes = 64 * 1024
		return clone.RoundTrip(req)
	}
	return t.base.RoundTrip(req)
}

func (f *Fetcher) reject(kind Kind, err error, bytes uint64) error {
	code := CodeOf(err)
	if code == "" {
		code = ErrReadFailed
		err = newError(kind, code, "input acquisition failed", err)
	}
	f.policy.Metrics.ObserveRejected(kind, code, bytes)
	return err
}

func (f *Fetcher) quarantineOrRemove(path string, kind Kind, code ErrorCode, reason string) error {
	if err := f.policy.quarantine(path, kind, code, reason); err != nil {
		_ = os.Remove(path)
		return err
	}
	return nil
}

func filenameFromURL(raw string) string {
	parsed, err := urlParse(raw)
	if err != nil {
		return ""
	}
	base := filepath.Base(parsed.Path)
	if base == "." || base == "/" || base == "" {
		return "download"
	}
	return filepath.Base(base)
}

// urlParse is kept tiny so filename extraction cannot accidentally consult
// the filesystem or execute any client-provided path.
func urlParse(raw string) (*url.URL, error) {
	return url.Parse(strings.TrimSpace(raw))
}

func maxInt64(value int64) int64 {
	if value < 0 {
		return 0
	}
	return value
}
