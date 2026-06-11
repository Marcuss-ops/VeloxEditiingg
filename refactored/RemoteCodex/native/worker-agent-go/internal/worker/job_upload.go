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

	uploadURL := strings.TrimRight(w.config.MasterURL, "/") + "/api/v1/video/upload-completed"
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	if err := writer.WriteField("job_id", job.JobID); err != nil {
		return output, fmt.Errorf("write job_id field: %w", err)
	}
	if err := writer.WriteField("worker_id", w.config.WorkerID); err != nil {
		return output, fmt.Errorf("write worker_id field: %w", err)
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

	var parsed map[string]interface{}
	if err := json.Unmarshal(respBytes, &parsed); err == nil {
		if vp, ok := parsed["video_path"].(string); ok && strings.TrimSpace(vp) != "" {
			output["master_video_path"] = strings.TrimSpace(vp)
			output["video_uploaded"] = true
		}
	}
	return output, nil
}
