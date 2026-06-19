// Package worker provides job processing logic for the worker agent.
package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"velox-worker-agent/pkg/api"
)

func resolveJobRunID(job *api.Job) string {
	if job == nil {
		return ""
	}
	if trimmed := strings.TrimSpace(job.JobRunID); trimmed != "" {
		return trimmed
	}
	if trimmed, ok := job.Parameters["job_run_id"].(string); ok && strings.TrimSpace(trimmed) != "" {
		return strings.TrimSpace(trimmed)
	}
	if trimmed, ok := job.Parameters["run_id"].(string); ok && strings.TrimSpace(trimmed) != "" {
		return strings.TrimSpace(trimmed)
	}
	return ""
}

// chunkSizeThreshold is the file size above which we use resumable chunked upload.
const chunkSizeThreshold int64 = 50 * 1024 * 1024 // 50 MB

// maxChunkSize is the size of each chunk for chunked uploads.
const maxChunkSize int64 = 5 * 1024 * 1024 // 5 MB

func extractOutputVideoPath(output map[string]interface{}) string {
	if len(output) == 0 {
		return ""
	}
	for _, k := range []string{"master_video_path", "output_path", "result_path", "video_path", "output"} {
		if s, ok := output[k].(string); ok && strings.TrimSpace(s) != "" {
			return strings.TrimSpace(s)
		}
	}
	if nested, ok := output["result"].(map[string]interface{}); ok {
		return extractOutputVideoPathWithDepth(nested, 5)
	}
	return ""
}

func extractOutputVideoPathWithDepth(output map[string]interface{}, depth int) string {
	if depth <= 0 || len(output) == 0 {
		return ""
	}
	for _, k := range []string{"master_video_path", "output_path", "result_path", "video_path", "output"} {
		if s, ok := output[k].(string); ok && strings.TrimSpace(s) != "" {
			return strings.TrimSpace(s)
		}
	}
	if nested, ok := output["result"].(map[string]interface{}); ok {
		return extractOutputVideoPathWithDepth(nested, depth-1)
	}
	return ""
}

// uploadCompletedVideo chooses between chunked (resumable) and single-shot upload
// based on file size. Files > chunkSizeThreshold (50MB) use chunked upload.
func (w *Worker) uploadCompletedVideo(ctx context.Context, job *api.Job, output map[string]interface{}) (map[string]interface{}, error) {
	if len(output) == 0 {
		return output, fmt.Errorf("empty job output")
	}
	localVideoPath := extractOutputVideoPath(output)
	if strings.TrimSpace(localVideoPath) == "" {
		return output, fmt.Errorf("missing output video path in result payload")
	}
	st, err := os.Stat(localVideoPath)
	if err != nil || st.IsDir() {
		return output, fmt.Errorf("output video not found on worker filesystem: %s", localVideoPath)
	}

	fileSize := st.Size()
	if fileSize > chunkSizeThreshold {
		w.logger.Info("[UPLOAD] File is %.1f MB, using resumable chunked upload for %s",
			float64(fileSize)/(1024*1024), job.JobID)
		return w.chunkedUploadVideo(ctx, job, output, localVideoPath, fileSize)
	}

	return w.singleUploadVideo(ctx, job, output, localVideoPath)
}

// singleUploadVideo uploads the video file in a single multipart POST (original behavior).
func (w *Worker) singleUploadVideo(ctx context.Context, job *api.Job, output map[string]interface{}, localVideoPath string) (map[string]interface{}, error) {
	uploadURL := strings.TrimRight(w.config.MasterURL, "/") + "/api/v1/video/upload-completed"
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	if err := writer.WriteField("job_id", job.JobID); err != nil {
		return output, fmt.Errorf("write job_id field: %w", err)
	}
	if err := writer.WriteField("worker_id", w.config.WorkerID); err != nil {
		return output, fmt.Errorf("write worker_id field: %w", err)
	}
	if leaseID := resolveLeaseID(job); strings.TrimSpace(leaseID) != "" {
		if err := writer.WriteField("lease_id", strings.TrimSpace(leaseID)); err != nil {
			return output, fmt.Errorf("write lease_id field: %w", err)
		}
	}
	if attempt := resolveJobAttempt(job); attempt > 0 {
		if err := writer.WriteField("attempt", fmt.Sprintf("%d", attempt)); err != nil {
			return output, fmt.Errorf("write attempt field: %w", err)
		}
	}
	if err := writer.WriteField("contract_version", fmt.Sprintf("%d", api.ContractVersionV2)); err != nil {
		return output, fmt.Errorf("write contract_version field: %w", err)
	}
	if runID := resolveJobRunID(job); strings.TrimSpace(runID) != "" {
		if err := writer.WriteField("job_run_id", strings.TrimSpace(runID)); err != nil {
			return output, fmt.Errorf("write job_run_id field: %w", err)
		}
	}
	if len(job.Parameters) > 0 {
		if raw, err := json.Marshal(job.Parameters); err == nil {
			if err := writer.WriteField("upload_info", string(raw)); err != nil {
				return output, fmt.Errorf("write upload_info field: %w", err)
			}
		}
	}

	part, err := writer.CreateFormFile("video_file", filepath.Base(localVideoPath))
	if err != nil {
		return output, err
	}
	f, err := os.Open(localVideoPath)
	if err != nil {
		return output, err
	}
	defer f.Close()
	if _, err := io.Copy(part, f); err != nil {
		return output, err
	}
	if err := writer.Close(); err != nil {
		return output, err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", uploadURL, body)
	if err != nil {
		return output, err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	client := &http.Client{Timeout: 10 * time.Minute}
	resp, err := client.Do(req)
	if err != nil {
		return output, err
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(io.LimitReader(resp.Body, 10*1024*1024))
	if err != nil {
		return output, fmt.Errorf("read upload response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return output, fmt.Errorf("upload endpoint http %d: %s", resp.StatusCode, string(respBytes))
	}

	return w.parseUploadResponse(respBytes, output)
}

// chunkedUploadVideo uploads the video file in chunks with resume support.
func (w *Worker) chunkedUploadVideo(ctx context.Context, job *api.Job, output map[string]interface{}, localVideoPath string, fileSize int64) (map[string]interface{}, error) {
	masterURL := strings.TrimRight(w.config.MasterURL, "/")

	// Calculate number of chunks
	totalChunks := int((fileSize + maxChunkSize - 1) / maxChunkSize)
	filename := filepath.Base(localVideoPath)

	// Step 1: Initiate chunked upload session
	initURL := masterURL + "/api/v1/video/chunked/init"
	initBody := map[string]interface{}{
		"job_id":       job.JobID,
		"worker_id":    w.config.WorkerID,
		"filename":     filename,
		"total_chunks": totalChunks,
		"chunk_size":   maxChunkSize,
		"total_size":   fileSize,
	}
	initJSON, _ := json.Marshal(initBody)
	initReq, err := http.NewRequestWithContext(ctx, "POST", initURL, bytes.NewReader(initJSON))
	if err != nil {
		return output, fmt.Errorf("chunked init request: %w", err)
	}
	initReq.Header.Set("Content-Type", "application/json")
	if w.apiClient.AuthToken() != "" {
		initReq.Header.Set("Authorization", "Bearer "+w.apiClient.AuthToken())
	}

	client := &http.Client{Timeout: 30 * time.Second}
	initResp, err := client.Do(initReq)
	if err != nil {
		return output, fmt.Errorf("chunked init failed: %w", err)
	}
	defer initResp.Body.Close()

	initRespBytes, _ := io.ReadAll(initResp.Body)
	if initResp.StatusCode < 200 || initResp.StatusCode >= 300 {
		return output, fmt.Errorf("chunked init http %d: %s", initResp.StatusCode, string(initRespBytes))
	}

	var initResult struct {
		Ok          bool   `json:"ok"`
		Resuming    bool   `json:"resuming"`
		TotalChunks int    `json:"total_chunks"`
		Uploaded    []bool `json:"uploaded"`
	}
	json.Unmarshal(initRespBytes, &initResult)

	w.logger.Info("[UPLOAD] Chunked upload initialized: %d chunks, resuming=%v, file=%.1f MB",
		totalChunks, initResult.Resuming, float64(fileSize)/(1024*1024))

	// Step 2: Upload each chunk
	f, err := os.Open(localVideoPath)
	if err != nil {
		return output, fmt.Errorf("open video for chunked upload: %w", err)
	}
	defer f.Close()

	for chunkIdx := 0; chunkIdx < totalChunks; chunkIdx++ {
		// Skip already uploaded chunks (resume)
		if initResult.Resuming && chunkIdx < len(initResult.Uploaded) && initResult.Uploaded[chunkIdx] {
			w.logger.Debug("[UPLOAD] Chunk %d already uploaded, skipping", chunkIdx)
			continue
		}

		// Read chunk data
		offset := int64(chunkIdx) * maxChunkSize
		chunkSize := maxChunkSize
		if offset+chunkSize > fileSize {
			chunkSize = fileSize - offset
		}

		chunkData := make([]byte, chunkSize)
		if _, err := f.ReadAt(chunkData, offset); err != nil && err != io.EOF {
			return output, fmt.Errorf("read chunk %d: %w", chunkIdx, err)
		}

		// Upload chunk with retries
		if err := w.uploadSingleChunk(ctx, masterURL, job.JobID, chunkIdx, chunkData, client); err != nil {
			return output, fmt.Errorf("upload chunk %d failed after retries: %w", chunkIdx, err)
		}

		w.logger.Info("[UPLOAD] Chunk %d/%d uploaded (%.1f MB)",
			chunkIdx+1, totalChunks, float64(chunkSize)/(1024*1024))
	}

	// Step 3: Complete the upload
	completeURL := masterURL + "/api/v1/video/chunked/" + job.JobID + "/complete"
	completeReq, err := http.NewRequestWithContext(ctx, "POST", completeURL, nil)
	if err != nil {
		return output, fmt.Errorf("chunked complete request: %w", err)
	}
	if w.apiClient.AuthToken() != "" {
		completeReq.Header.Set("Authorization", "Bearer "+w.apiClient.AuthToken())
	}

	completeResp, err := client.Do(completeReq)
	if err != nil {
		return output, fmt.Errorf("chunked complete failed: %w", err)
	}
	defer completeResp.Body.Close()

	completeRespBytes, _ := io.ReadAll(completeResp.Body)
	if completeResp.StatusCode < 200 || completeResp.StatusCode >= 300 {
		return output, fmt.Errorf("chunked complete http %d: %s", completeResp.StatusCode, string(completeRespBytes))
	}

	w.logger.Info("[UPLOAD] Chunked upload completed for %s", job.JobID)
	return w.parseUploadResponse(completeRespBytes, output)
}

// uploadSingleChunk uploads a single chunk with retry on transient failures.
func (w *Worker) uploadSingleChunk(ctx context.Context, masterURL, jobID string, chunkIdx int, data []byte, client *http.Client) error {
	maxRetries := 3
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			w.logger.Warn("[UPLOAD] Retrying chunk %d (attempt %d/%d)", chunkIdx, attempt, maxRetries)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt) * 2 * time.Second):
			}
		}

		chunkURL := fmt.Sprintf("%s/api/v1/video/chunked/%s/%d", masterURL, jobID, chunkIdx)
		body := &bytes.Buffer{}
		writer := multipart.NewWriter(body)
		part, _ := writer.CreateFormFile("chunk", fmt.Sprintf("chunk_%04d", chunkIdx))
		part.Write(data)
		writer.Close()

		req, err := http.NewRequestWithContext(ctx, "POST", chunkURL, body)
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", writer.FormDataContentType())
		if w.apiClient.AuthToken() != "" {
			req.Header.Set("Authorization", "Bearer "+w.apiClient.AuthToken())
		}

		resp, err := client.Do(req)
		if err != nil {
			if attempt < maxRetries {
				continue
			}
			return fmt.Errorf("chunk %d request failed: %w", chunkIdx, err)
		}
		defer resp.Body.Close()

		respBytes, _ := io.ReadAll(resp.Body)
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil // success
		}

		if attempt >= maxRetries {
			return fmt.Errorf("chunk %d http %d: %s", chunkIdx, resp.StatusCode, string(respBytes))
		}
		// Non-retryable error codes
		if resp.StatusCode == 400 || resp.StatusCode == 404 || resp.StatusCode == 409 {
			return fmt.Errorf("chunk %d http %d: %s", chunkIdx, resp.StatusCode, string(respBytes))
		}
	}
	return nil
}

// parseUploadResponse extracts fields from upload response JSON.
func (w *Worker) parseUploadResponse(respBytes []byte, output map[string]interface{}) (map[string]interface{}, error) {
	var parsed map[string]interface{}
	if err := json.Unmarshal(respBytes, &parsed); err == nil {
		if vp, ok := parsed["video_path"].(string); ok && strings.TrimSpace(vp) != "" {
			output["master_video_path"] = strings.TrimSpace(vp)
			output["video_uploaded"] = true
		}
		if v, ok := parsed["artifact_id"].(string); ok && strings.TrimSpace(v) != "" {
			output["artifact_id"] = strings.TrimSpace(v)
		}
		if v, ok := parsed["output_sha256"].(string); ok && strings.TrimSpace(v) != "" {
			output["output_sha256"] = strings.TrimSpace(v)
		}
		if v, ok := parsed["idempotency_key"].(string); ok && strings.TrimSpace(v) != "" {
			output["idempotency_key"] = strings.TrimSpace(v)
		}
	}
	return output, nil
}
