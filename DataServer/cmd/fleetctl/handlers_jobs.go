// handlers_jobs.go — job diagnostics and production doctor handlers.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"velox-server/internal/jobs"
)

func runJob(client *fleetClient, args []string) int {
	if len(args) < 2 {
		fmt.Fprintln(os.Stderr, fmtExit(ExitMisuse, "job requires inspect, metrics, or watch plus a job_id"))
		return ExitMisuse
	}
	switch args[0] {
	case "submit":
		return runJobSubmit(client, args[1:], false)
	case "certify":
		return runJobSubmit(client, args[1:], true)
	case "cancel":
		return runJobCancel(client, args[1:])
	case "inspect":
		jobID, jsonOutput, err := parseJobReadArgs(args[1:], "inspect")
		if err != nil {
			fmt.Fprintln(os.Stderr, fmtExit(ExitMisuse, "%v", err))
			return ExitMisuse
		}
		return runJobInspect(client, jobID, jsonOutput)
	case "metrics":
		jobID, _, err := parseJobReadArgs(args[1:], "metrics")
		if err != nil {
			fmt.Fprintln(os.Stderr, fmtExit(ExitMisuse, "%v", err))
			return ExitMisuse
		}
		return runJobMetrics(client, jobID)
	case "watch":
		jobID, timeout, interval, jsonOutput, err := parseJobWatchArgs(args[1:])
		if err != nil {
			fmt.Fprintln(os.Stderr, fmtExit(ExitMisuse, "%v", err))
			return ExitMisuse
		}
		return runJobWatchWithInterval(client, jobID, timeout, interval, jsonOutput)
	default:
		fmt.Fprintln(os.Stderr, fmtExit(ExitMisuse, "unknown job command %q", args[0]))
		return ExitMisuse
	}
}

func runJobCancel(client *fleetClient, args []string) int {
	var jobID, reason string
	for i := 0; i < len(args); i++ {
		switch {
		case args[i] == "--reason":
			if i+1 >= len(args) {
				fmt.Fprintln(os.Stderr, fmtExit(ExitMisuse, "--reason requires a value"))
				return ExitMisuse
			}
			i++
			reason = args[i]
		case strings.HasPrefix(args[i], "--reason="):
			reason = strings.TrimPrefix(args[i], "--reason=")
		case strings.HasPrefix(args[i], "--master=") || strings.HasPrefix(args[i], "--token-file=") || args[i] == "--verbose":
		case args[i] == "--master" || args[i] == "--token-file":
			if i+1 >= len(args) {
				fmt.Fprintln(os.Stderr, fmtExit(ExitMisuse, "%s requires a value", args[i]))
				return ExitMisuse
			}
			i++
		case strings.HasPrefix(args[i], "-"):
			fmt.Fprintln(os.Stderr, fmtExit(ExitMisuse, "unknown job cancel option %q", args[i]))
			return ExitMisuse
		default:
			if jobID != "" {
				fmt.Fprintln(os.Stderr, fmtExit(ExitMisuse, "job cancel accepts exactly one job_id"))
				return ExitMisuse
			}
			jobID = args[i]
		}
	}
	if jobID == "" {
		fmt.Fprintln(os.Stderr, fmtExit(ExitMisuse, "job cancel requires a job_id"))
		return ExitMisuse
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var response map[string]any
	status, err := client.doJSON(ctx, "POST", "/api/v1/admin/jobs/"+url.PathEscape(jobID)+"/cancel", map[string]any{"reason": reason}, &response)
	if err != nil {
		fmt.Fprintln(os.Stderr, fmtExit(ExitUnexpected, "%v", err))
		return ExitUnexpected
	}
	if status != httpOK {
		ec := MapHTTPStatusToOpExit(status)
		fmt.Fprintln(os.Stderr, fmtExit(ec, "POST /api/v1/admin/jobs/%s/cancel status=%d", jobID, status))
		return ec
	}
	encoded, _ := json.Marshal(response)
	fmt.Println(string(encoded))
	return ExitOK
}

func parseJobReadArgs(args []string, command string) (string, bool, error) {
	var jobID string
	jsonOutput := false
	for i := 0; i < len(args); i++ {
		switch {
		case args[i] == "--json":
			jsonOutput = true
		case strings.HasPrefix(args[i], "--master=") || strings.HasPrefix(args[i], "--token-file=") || args[i] == "--verbose":
		case args[i] == "--master" || args[i] == "--token-file":
			if i+1 >= len(args) {
				return "", false, fmt.Errorf("%s requires a value", args[i])
			}
			i++
		case strings.HasPrefix(args[i], "-"):
			return "", false, fmt.Errorf("unknown job %s option %q", command, args[i])
		default:
			if jobID != "" {
				return "", false, fmt.Errorf("job %s accepts exactly one job_id", command)
			}
			jobID = args[i]
		}
	}
	if jobID == "" {
		return "", false, fmt.Errorf("job %s requires a job_id", command)
	}
	if command == "metrics" && jsonOutput {
		return "", false, errors.New("job metrics does not support --json")
	}
	return jobID, jsonOutput, nil
}

func parseJobWatchArgs(args []string) (jobID string, timeout, interval time.Duration, jsonOutput bool, err error) {
	timeout = envSeconds("FLEETCTL_JOB_TIMEOUT_SECONDS", 3600)
	interval = envSeconds("FLEETCTL_JOB_POLL_SECONDS", 2)
	for i := 0; i < len(args); i++ {
		switch {
		case args[i] == "--timeout" || args[i] == "--poll":
			if i+1 >= len(args) {
				return "", 0, 0, false, fmt.Errorf("%s requires a value", args[i])
			}
			value, flagName := args[i+1], args[i]
			parsed, parseErr := parseSeconds(value, flagName)
			if parseErr != nil {
				return "", 0, 0, false, parseErr
			}
			if args[i] == "--timeout" {
				timeout = parsed
			} else {
				interval = parsed
			}
			i++
		case strings.HasPrefix(args[i], "--timeout="):
			parsed, parseErr := parseSeconds(strings.TrimPrefix(args[i], "--timeout="), "--timeout")
			if parseErr != nil {
				return "", 0, 0, false, parseErr
			}
			timeout = parsed
		case strings.HasPrefix(args[i], "--poll="):
			parsed, parseErr := parseSeconds(strings.TrimPrefix(args[i], "--poll="), "--poll")
			if parseErr != nil {
				return "", 0, 0, false, parseErr
			}
			interval = parsed
		case args[i] == "--json":
			jsonOutput = true
		case strings.HasPrefix(args[i], "--master=") || strings.HasPrefix(args[i], "--token-file=") || args[i] == "--verbose":
		case args[i] == "--master" || args[i] == "--token-file":
			if i+1 >= len(args) {
				return "", 0, 0, false, fmt.Errorf("%s requires a value", args[i])
			}
			i++
		case strings.HasPrefix(args[i], "-"):
			return "", 0, 0, false, fmt.Errorf("unknown job watch option %q", args[i])
		default:
			if jobID != "" {
				return "", 0, 0, false, errors.New("job watch accepts exactly one job_id")
			}
			jobID = args[i]
		}
	}
	if jobID == "" {
		return "", 0, 0, false, errors.New("job watch requires a job_id")
	}
	if timeout <= 0 || interval < 0 {
		return "", 0, 0, false, errors.New("job watch timeout must be positive and poll interval cannot be negative")
	}
	return jobID, timeout, interval, jsonOutput, nil
}

func runJobInspect(client *fleetClient, jobID string, jsonOutput bool) int {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var response map[string]any
	status, err := client.doJSON(ctx, "GET", "/api/v1/admin/jobs/"+url.PathEscape(jobID), nil, &response)
	if err != nil {
		fmt.Fprintln(os.Stderr, fmtExit(ExitUnexpected, "%v", err))
		return ExitUnexpected
	}
	if status != httpOK {
		ec := MapHTTPStatusToOpExit(status)
		fmt.Fprintln(os.Stderr, fmtExit(ec, "GET /api/v1/admin/jobs/%s status=%d", jobID, status))
		return ec
	}
	if jsonOutput {
		bs, _ := json.Marshal(response)
		fmt.Println(string(bs))
	} else {
		bs, _ := json.MarshalIndent(response, "", "  ")
		fmt.Println(string(bs))
	}
	return ExitOK
}
func runJobMetrics(client *fleetClient, jobID string) int {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var response map[string]any
	status, err := client.doJSON(ctx, "GET", "/api/v1/admin/jobs/"+url.PathEscape(jobID)+"/metrics", nil, &response)
	if err != nil {
		fmt.Fprintln(os.Stderr, fmtExit(ExitUnexpected, "%v", err))
		return ExitUnexpected
	}
	if status != httpOK {
		ec := MapHTTPStatusToOpExit(status)
		fmt.Fprintln(os.Stderr, fmtExit(ec, "GET job metrics status=%d", status))
		return ec
	}
	bs, _ := json.MarshalIndent(response, "", "  ")
	fmt.Println(string(bs))
	return ExitOK
}
func runJobWatchWithInterval(client *fleetClient, jobID string, timeout, interval time.Duration, jsonOutput bool) int {
	seen := map[string]bool{}
	var lastPhase string
	var lastPercent int
	var sawLive bool
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for {
		// ── Fetch /events (deduped timeline) ─────────────────────────
		eventsCtx, eventsCancel := context.WithTimeout(ctx, 30*time.Second)
		var eventsResp struct {
			Status string `json:"status"`
			Job    struct {
				Status string `json:"status"`
			} `json:"job"`
			Events []struct {
				Timestamp string         `json:"timestamp"`
				Event     string         `json:"event"`
				Payload   map[string]any `json:"payload"`
			} `json:"events"`
		}
		eventsStatus, eventsErr := client.doJSON(eventsCtx, "GET", "/api/v1/admin/jobs/"+url.PathEscape(jobID)+"/events", nil, &eventsResp)
		eventsCancel()
		if eventsErr != nil {
			fmt.Fprintln(os.Stderr, fmtExit(ExitUnexpected, "%v", eventsErr))
			return ExitUnexpected
		}
		if eventsStatus != httpOK {
			ec := MapHTTPStatusToOpExit(eventsStatus)
			fmt.Fprintln(os.Stderr, fmtExit(ec, "GET job events status=%d", eventsStatus))
			return ec
		}

		// ── Fetch /live (compact live snapshot) ───────────────────────
		liveCtx, liveCancel := context.WithTimeout(ctx, 30*time.Second)
		var liveResp struct {
			JobID  string `json:"job_id"`
			Status string `json:"status"`
			Worker struct {
				WorkerID       string `json:"worker_id"`
				WorkerName     string `json:"worker_name"`
				Connection     string `json:"connection"`
				HeartbeatAgeMS int64  `json:"heartbeat_age_ms"`
			} `json:"worker"`
			Execution struct {
				Phase            string  `json:"phase"`
				OperationalPhase string  `json:"operational_phase"`
				Percent          int     `json:"percent"`
				Scene            int     `json:"scene"`
				ScenesTotal      int     `json:"scenes_total"`
				Segment          int     `json:"segment"`
				SegmentsTotal    int     `json:"segments_total"`
				ElapsedMS        int64   `json:"elapsed_ms"`
				FramesDecoded    int64   `json:"frames_decoded"`
				FramesComposited int64   `json:"frames_composited"`
				FramesEncoded    int64   `json:"frames_encoded"`
				SpeedX           float64 `json:"speed_x"`
			} `json:"execution"`
			Publication struct {
				State            string  `json:"state"`
				UploadBytes      int64   `json:"upload_bytes"`
				UploadTotalBytes int64   `json:"upload_total_bytes"`
				UploadPercent    float64 `json:"upload_percent"`
				UploadBPS        float64 `json:"upload_bytes_per_second"`
				UploadETASeconds float64 `json:"upload_eta_seconds"`
			} `json:"publication"`
			Stalled       bool   `json:"stalled"`
			StallReason   string `json:"stall_reason"`
			ProgressAgeMS int64  `json:"progress_age_ms"`
		}
		_, liveErr := client.doJSON(liveCtx, "GET", "/api/v1/admin/jobs/"+url.PathEscape(jobID)+"/live", nil, &liveResp)
		liveCancel()
		// /live may not exist yet on older servers; treat 404 as non-fatal.
		if liveErr != nil {
			// fall through — events-only mode
		} else {
			// Print live progress line when phase or percent changes.
			if liveResp.Execution.Phase != "" && (!sawLive || liveResp.Execution.Phase != lastPhase || liveResp.Execution.Percent != lastPercent) {
				workerLabel := liveResp.Worker.WorkerID
				if liveResp.Worker.WorkerName != "" {
					workerLabel = liveResp.Worker.WorkerName
				}
				ts := time.Now().Format("15:04:05")
				// Use operational phase when available, fall back to renderer phase.
				displayPhase := liveResp.Execution.Phase
				if liveResp.Execution.OperationalPhase != "" {
					displayPhase = liveResp.Execution.OperationalPhase
				}
				fmt.Printf("%s  worker=%-20s  %-24s  %3d%%", ts, workerLabel, displayPhase, liveResp.Execution.Percent)
				if liveResp.Execution.SpeedX > 0 {
					fmt.Printf("  %.2fx", liveResp.Execution.SpeedX)
				}
				if liveResp.Execution.ScenesTotal > 0 {
					fmt.Printf("  scene %d/%d", liveResp.Execution.Scene, liveResp.Execution.ScenesTotal)
				}
				if liveResp.Execution.SegmentsTotal > 0 {
					fmt.Printf("  seg %d/%d", liveResp.Execution.Segment, liveResp.Execution.SegmentsTotal)
				}
				if liveResp.Stalled {
					fmt.Printf("  STALLED (%s)", liveResp.StallReason)
				}
				fmt.Println()
				lastPhase = liveResp.Execution.Phase
				lastPercent = liveResp.Execution.Percent
				sawLive = true
			}
			// Print upload progress line when in publishing phase.
			if liveResp.Publication.State == "UPLOADING" && liveResp.Publication.UploadTotalBytes > 0 {
				ts := time.Now().Format("15:04:05")
				fmt.Printf("%s  PUBLISHING  %s / %s  %.1f%%", ts,
					formatBytes(liveResp.Publication.UploadBytes),
					formatBytes(liveResp.Publication.UploadTotalBytes),
					liveResp.Publication.UploadPercent)
				if liveResp.Publication.UploadBPS > 0 {
					fmt.Printf("  %s/s", formatBytes(int64(liveResp.Publication.UploadBPS)))
				}
				if liveResp.Publication.UploadETASeconds > 0 {
					fmt.Printf("  ETA %.1fs", liveResp.Publication.UploadETASeconds)
				}
				fmt.Println()
			}
		}

		// ── Print new events (deduped) ───────────────────────────────
		if !jsonOutput {
			for _, event := range eventsResp.Events {
				key := event.Timestamp + "\x00" + event.Event
				if seen[key] {
					continue
				}
				seen[key] = true
				fmt.Printf("%s %-24s", event.Timestamp, event.Event)
				if len(event.Payload) > 0 {
					payload, _ := json.Marshal(event.Payload)
					fmt.Printf(" %s", payload)
				}
				fmt.Println()
			}
		} else {
			// JSON mode: merge live + events into one object per cycle.
			merged := map[string]any{
				"status": eventsResp.Status,
				"events": eventsResp.Events,
			}
			if liveErr == nil && liveResp.JobID != "" {
				merged["live"] = liveResp
			}
			encoded, _ := json.Marshal(merged)
			fmt.Println(string(encoded))
		}

		// ── Terminal status check ─────────────────────────────────────
		terminalStatus := eventsResp.Status
		if terminalStatus == "" {
			terminalStatus = eventsResp.Job.Status
			if terminalStatus == "" && liveErr == nil {
				terminalStatus = liveResp.Status
			}
		}
		// This endpoint reports the Velox JobStatus domain. Parse it into
		// the domain type before applying terminal semantics. COMPLETED is
		// reserved for producer-side InputAssemblyStatus and is therefore
		// rejected rather than treated as job success.
		jobStatus := jobs.JobStatus(strings.TrimSpace(terminalStatus))
		switch jobStatus {
		case jobs.StatusSucceeded:
			if !jsonOutput {
				fmt.Printf("%s  %s\n", time.Now().Format("15:04:05"), jobStatus)
			}
			return ExitOK
		case jobs.StatusFailed, jobs.StatusCancelled:
			fmt.Fprintln(os.Stderr, fmtExit(ExitUnexpected, "job %s ended %s", jobID, jobStatus))
			return ExitUnexpected
		case jobs.JobStatus("COMPLETED"):
			fmt.Fprintln(os.Stderr, fmtExit(ExitUnexpected, "job %s returned input-assembly status COMPLETED, not a terminal JobStatus", jobID))
			return ExitUnexpected
		}
		if !waitPollingInterval(ctx, interval) {
			fmt.Fprintln(os.Stderr, fmtExit(ExitUnexpected, "timed out waiting for job %s", jobID))
			return ExitUnexpected
		}
	}
}

func runDoctor(client *fleetClient, args []string) int {
	production := false
	for _, arg := range args {
		if arg == "--production" {
			production = true
		}
	}
	if !production {
		fmt.Fprintln(os.Stderr, fmtExit(ExitMisuse, "doctor requires --production"))
		return ExitMisuse
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var response struct {
		Healthy bool `json:"healthy"`
		Checks  []struct {
			WorkerID string `json:"worker_id"`
			Name     string `json:"name"`
			Check    string `json:"check"`
			Status   string `json:"status"`
			Detail   string `json:"detail"`
		} `json:"checks"`
	}
	status, err := client.doJSON(ctx, "GET", "/api/v1/admin/doctor/production", nil, &response)
	if err != nil {
		fmt.Fprintln(os.Stderr, fmtExit(ExitUnexpected, "%v", err))
		return ExitUnexpected
	}
	if status != httpOK {
		fmt.Fprintln(os.Stderr, fmtExit(MapHTTPStatusToOpExit(status), "GET production doctor status=%d", status))
		return MapHTTPStatusToOpExit(status)
	}
	for _, check := range response.Checks {
		who := check.WorkerID
		if check.Name != "" {
			who = check.Name + " (" + who + ")"
		}
		fmt.Printf("%-42s %-12s %-8s %s\n", who, check.Check, check.Status, check.Detail)
	}
	if !response.Healthy {
		return ExitUnexpected
	}
	return ExitOK
}

// formatBytes formats bytes as a human-readable string (B, KB, MB, GB).
func formatBytes(b int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)
	switch {
	case b >= GB:
		return fmt.Sprintf("%.1f GB", float64(b)/float64(GB))
	case b >= MB:
		return fmt.Sprintf("%.1f MB", float64(b)/float64(MB))
	case b >= KB:
		return fmt.Sprintf("%.1f KB", float64(b)/float64(KB))
	default:
		return fmt.Sprintf("%d B", b)
	}
}

const httpOK = 200

// runDrain — POST /api/v1/admin/workers/{id}/drain;
// polls /admin/operations/{op_id} until terminal.
