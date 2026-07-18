package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
)

type UploadCompletedVideoRequest struct {
	JobID         string
	AttemptID     string
	WorkerID      string
	LeaseID       string
	AttemptNumber int
	Revision      int
	FilePath      string
}

type UploadCompletedVideoResponse struct {
	OK         bool   `json:"ok"`
	JobID      string `json:"job_id"`
	ArtifactID string `json:"artifact_id"`
	UploadID   string `json:"upload_id"`
	Status     string `json:"status"`
	Size       int64  `json:"size"`
	SHA256     string `json:"sha256"`
	Error      string `json:"error"`
}

func (c *Client) UploadCompletedVideo(ctx context.Context, req UploadCompletedVideoRequest) (*UploadCompletedVideoResponse, error) {
	if req.JobID == "" || req.WorkerID == "" || req.LeaseID == "" {
		return nil, fmt.Errorf("upload completed video: job_id, worker_id and lease_id are required")
	}
	if req.FilePath == "" {
		return nil, fmt.Errorf("upload completed video: file_path is required")
	}

	f, err := os.Open(req.FilePath)
	if err != nil {
		return nil, fmt.Errorf("upload completed video: open file: %w", err)
	}
	defer f.Close()

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)

	part, err := writer.CreateFormFile("video", filepath.Base(req.FilePath))
	if err != nil {
		return nil, fmt.Errorf("upload completed video: create form file: %w", err)
	}
	if _, err := io.Copy(part, f); err != nil {
		return nil, fmt.Errorf("upload completed video: copy file: %w", err)
	}

	_ = writer.WriteField("job_id", req.JobID)
	_ = writer.WriteField("attempt_id", req.AttemptID)
	_ = writer.WriteField("worker_id", req.WorkerID)
	_ = writer.WriteField("lease_id", req.LeaseID)
	_ = writer.WriteField("attempt", strconv.Itoa(req.AttemptNumber))
	_ = writer.WriteField("revision", strconv.Itoa(req.Revision))

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("upload completed video: close multipart writer: %w", err)
	}

	base, err := url.Parse(c.baseURL)
	if err != nil {
		return nil, fmt.Errorf("upload completed video: parse base URL: %w", err)
	}
	rel, err := url.Parse("/api/v1/video/upload-completed")
	if err != nil {
		return nil, fmt.Errorf("upload completed video: parse path: %w", err)
	}
	fullURL := base.ResolveReference(rel).String()

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, fullURL, &body)
	if err != nil {
		return nil, fmt.Errorf("upload completed video: create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", writer.FormDataContentType())
	if c.authToken != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.authToken)
	}
	for key, value := range c.headers {
		httpReq.Header.Set(key, value)
	}

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("upload completed video: request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("upload completed video: read response: %w", err)
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("upload completed video: API error (status %d): %s", resp.StatusCode, string(respBody))
	}

	var out UploadCompletedVideoResponse
	if err := json.Unmarshal(respBody, &out); err != nil {
		return nil, fmt.Errorf("upload completed video: decode response: %w", err)
	}
	return &out, nil
}
