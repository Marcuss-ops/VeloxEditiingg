package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"

	"velox-shared/contract"
)

type preparedWindowRegistration struct {
	AssetID   string `json:"asset_id"`
	SHA256    string `json:"sha256"`
	SizeBytes int64  `json:"size_bytes"`
}

// PublishPreparedWindow uploads a worker-certified fragment to the Master
// fragment registry. It checks the shared content identity first so another
// worker's existing registration avoids transferring the bytes.
func (c *Client) PublishPreparedWindow(ctx context.Context, request contract.VideoPrepareWindowV1, certificate contract.VideoPrepareWindowCertificateV1, path string) (string, bool, error) {
	if c == nil || c.httpClient == nil {
		return "", false, fmt.Errorf("prepared window client is unavailable")
	}
	if err := certificate.ValidateFor(request); err != nil {
		return "", false, err
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() != certificate.OutputSizeBytes {
		if err == nil {
			err = fmt.Errorf("local output size differs from certificate")
		}
		return "", false, fmt.Errorf("prepared window output: %w", err)
	}
	identity, err := contract.VideoPrepareWindowCacheIdentity(request)
	if err != nil {
		return "", false, err
	}
	endpoint := "/api/v1/agent/prepared-windows/" + url.PathEscape(identity)
	requestJSON, err := json.Marshal(request)
	if err != nil {
		return "", false, err
	}
	certificateJSON, err := json.Marshal(certificate)
	if err != nil {
		return "", false, err
	}
	requestB64 := base64.RawURLEncoding.EncodeToString(requestJSON)
	certificateB64 := base64.RawURLEncoding.EncodeToString(certificateJSON)
	registered, status, lookupErr := c.preparedWindowRequest(ctx, http.MethodGet, endpoint, nil, 0, requestB64, certificateB64)
	if lookupErr == nil && status == http.StatusOK {
		if registered.SHA256 != certificate.OutputSHA256 || registered.SizeBytes != certificate.OutputSizeBytes || registered.AssetID == "" {
			return "", false, fmt.Errorf("master prepared-window cache identity conflicts with certified output")
		}
		return registered.AssetID, true, nil
	}
	if lookupErr != nil && status != http.StatusNotFound {
		return "", false, lookupErr
	}
	file, err := os.Open(path)
	if err != nil {
		return "", false, fmt.Errorf("open prepared window for upload: %w", err)
	}
	defer file.Close()
	registered, status, err = c.preparedWindowRequest(ctx, http.MethodPut, endpoint, file, info.Size(), requestB64, certificateB64)
	if err != nil {
		return "", false, err
	}
	if status != http.StatusCreated && status != http.StatusOK {
		return "", false, fmt.Errorf("master rejected prepared-window registration: HTTP %d", status)
	}
	if registered.AssetID == "" || registered.SHA256 != certificate.OutputSHA256 || registered.SizeBytes != certificate.OutputSizeBytes {
		return "", false, fmt.Errorf("master prepared-window registration returned mismatched identity")
	}
	return registered.AssetID, false, nil
}

func (c *Client) preparedWindowRequest(ctx context.Context, method, path string, body io.Reader, size int64, requestB64, certificateB64 string) (preparedWindowRegistration, int, error) {
	var out preparedWindowRegistration
	base, err := url.Parse(c.baseURL)
	if err != nil {
		return out, 0, err
	}
	rel, err := url.Parse(path)
	if err != nil {
		return out, 0, err
	}
	req, err := http.NewRequestWithContext(ctx, method, base.ResolveReference(rel).String(), body)
	if err != nil {
		return out, 0, err
	}
	if method == http.MethodPut {
		req.Header.Set("Content-Type", "video/mp4")
		req.ContentLength = size
	}
	req.Header.Set("X-Velox-Window-Request", requestB64)
	req.Header.Set("X-Velox-Window-Certificate", certificateB64)
	if token := c.AuthToken(); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	httpClient := c.httpClient
	if method == http.MethodPut && httpClient.Timeout > 0 {
		clientCopy := *httpClient
		clientCopy.Timeout = 0 // task context bounds large fragment transfers
		httpClient = &clientCopy
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return out, 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return out, resp.StatusCode, nil
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 16*1024))
	if err != nil {
		return out, resp.StatusCode, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return out, resp.StatusCode, fmt.Errorf("master prepared-window request failed: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return out, resp.StatusCode, fmt.Errorf("decode prepared-window response: %w", err)
	}
	return out, resp.StatusCode, nil
}
