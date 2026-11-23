package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"velox-server/internal/observability"
)

func parseJobDownloadArgs(args []string) (jobID, output string, err error) {
	for i := 0; i < len(args); i++ {
		switch {
		case args[i] == "-o" || args[i] == "--output":
			if i+1 >= len(args) {
				return "", "", fmt.Errorf("%s requires a file path", args[i])
			}
			i++
			output = args[i]
		case strings.HasPrefix(args[i], "--output="):
			output = strings.TrimPrefix(args[i], "--output=")
		case strings.HasPrefix(args[i], "--master=") || strings.HasPrefix(args[i], "--token-file=") || args[i] == "--verbose":
		case args[i] == "--master" || args[i] == "--token-file":
			if i+1 >= len(args) {
				return "", "", fmt.Errorf("%s requires a value", args[i])
			}
			i++
		case strings.HasPrefix(args[i], "-"):
			return "", "", fmt.Errorf("unknown job download option %q", args[i])
		default:
			if jobID == "" {
				jobID = args[i]
			} else if output == "" {
				output = args[i]
			} else {
				return "", "", errors.New("job download accepts one job_id and one output path")
			}
		}
	}
	if strings.TrimSpace(jobID) == "" {
		return "", "", errors.New("job download requires a job_id")
	}
	if output == "" {
		output = jobID + ".mp4"
	}
	return jobID, output, nil
}

func runJobDownload(client *fleetClient, jobID, outputPath string) int {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	var inspection observability.JobInspection
	status, err := client.doJSON(ctx, http.MethodGet, "/api/v1/admin/jobs/"+url.PathEscape(jobID), nil, &inspection)
	cancel()
	if err != nil {
		fmt.Fprintln(os.Stderr, fmtExit(ExitUnexpected, "inspect job before download: %v", err))
		return ExitUnexpected
	}
	if status != http.StatusOK {
		ec := MapHTTPStatusToOpExit(status)
		fmt.Fprintln(os.Stderr, fmtExit(ec, "GET job %s status=%d", jobID, status))
		return ec
	}
	if inspection.Job == nil || inspection.Job.Status != "SUCCEEDED" {
		state := "unknown"
		if inspection.Job != nil {
			state = string(inspection.Job.Status)
		}
		fmt.Fprintln(os.Stderr, fmtExit(ExitUnexpected, "job %s is %s; final artifact is downloadable only after SUCCEEDED", jobID, state))
		return ExitUnexpected
	}
	var artifact *observability.ArtifactSnapshot
	for i := range inspection.Artifacts {
		candidate := &inspection.Artifacts[i]
		if candidate.Status == "READY" && (candidate.Type == "final_video" || strings.HasPrefix(candidate.Type, "video/")) {
			if artifact == nil || candidate.CreatedAt > artifact.CreatedAt {
				artifact = candidate
			}
		}
	}
	if artifact == nil {
		fmt.Fprintln(os.Stderr, fmtExit(ExitUnexpected, "job %s has no READY final video artifact", jobID))
		return ExitUnexpected
	}

	if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		fmt.Fprintln(os.Stderr, fmtExit(ExitUnexpected, "create output directory: %v", err))
		return ExitUnexpected
	}
	tmp, err := os.CreateTemp(filepath.Dir(outputPath), ".fleetctl-download-*.part")
	if err != nil {
		fmt.Fprintln(os.Stderr, fmtExit(ExitUnexpected, "create temporary output: %v", err))
		return ExitUnexpected
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)

	downloadCtx, downloadCancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer downloadCancel()
	req, err := http.NewRequestWithContext(downloadCtx, http.MethodGet,
		client.baseURL+"/api/internal/artifacts/"+url.PathEscape(artifact.ID)+"/download", nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, fmtExit(ExitUnexpected, "build artifact download request: %v", err))
		return ExitUnexpected
	}
	req.Header.Set("Authorization", "Bearer "+client.token)
	longClient := *client.http
	longClient.Timeout = 30 * time.Minute
	resp, err := longClient.Do(req)
	if err != nil {
		fmt.Fprintln(os.Stderr, fmtExit(ExitUnexpected, "download artifact: %v", err))
		return ExitUnexpected
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		ec := MapHTTPStatusToOpExit(resp.StatusCode)
		fmt.Fprintln(os.Stderr, fmtExit(ec, "GET artifact %s status=%d", artifact.ID, resp.StatusCode))
		return ec
	}
	hash := sha256.New()
	written, err := io.Copy(io.MultiWriter(tmp, hash), resp.Body)
	if err != nil {
		fmt.Fprintln(os.Stderr, fmtExit(ExitUnexpected, "stream artifact to disk: %v", err))
		return ExitUnexpected
	}
	if written == 0 || (artifact.SizeBytes > 0 && written != artifact.SizeBytes) {
		fmt.Fprintln(os.Stderr, fmtExit(ExitUnexpected, "artifact size mismatch: downloaded=%d expected=%d", written, artifact.SizeBytes))
		return ExitUnexpected
	}
	gotSHA := hex.EncodeToString(hash.Sum(nil))
	if artifact.SHA256 != "" && !strings.EqualFold(gotSHA, artifact.SHA256) {
		fmt.Fprintln(os.Stderr, fmtExit(ExitUnexpected, "artifact SHA-256 mismatch: downloaded=%s expected=%s", gotSHA, artifact.SHA256))
		return ExitUnexpected
	}
	if err := tmp.Sync(); err != nil {
		fmt.Fprintln(os.Stderr, fmtExit(ExitUnexpected, "sync downloaded artifact: %v", err))
		return ExitUnexpected
	}
	if err := tmp.Close(); err != nil {
		fmt.Fprintln(os.Stderr, fmtExit(ExitUnexpected, "close downloaded artifact: %v", err))
		return ExitUnexpected
	}
	if err := os.Rename(tmpPath, outputPath); err != nil {
		fmt.Fprintln(os.Stderr, fmtExit(ExitUnexpected, "commit downloaded artifact: %v", err))
		return ExitUnexpected
	}
	fmt.Printf("downloaded job=%s artifact=%s bytes=%d sha256=%s file=%s\n", jobID, artifact.ID, written, gotSHA, outputPath)
	return ExitOK
}
