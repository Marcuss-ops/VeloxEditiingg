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
	interval = envSeconds("FLEETCTL_JOB_POLL_SECONDS", 5)
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
func runJobWatch(client *fleetClient, jobID string) int {
	return runJobWatchWithInterval(client, jobID, envSeconds("FLEETCTL_JOB_TIMEOUT_SECONDS", 3600), envSeconds("FLEETCTL_JOB_POLL_SECONDS", 5), false)
}

func runJobWatchWithInterval(client *fleetClient, jobID string, timeout, interval time.Duration, jsonOutput bool) int {
	seen := map[string]bool{}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for {
		requestCtx, requestCancel := context.WithTimeout(ctx, 30*time.Second)
		var response struct {
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
		status, err := client.doJSON(requestCtx, "GET", "/api/v1/admin/jobs/"+url.PathEscape(jobID)+"/events", nil, &response)
		requestCancel()
		if err != nil {
			fmt.Fprintln(os.Stderr, fmtExit(ExitUnexpected, "%v", err))
			return ExitUnexpected
		}
		if status != httpOK {
			ec := MapHTTPStatusToOpExit(status)
			fmt.Fprintln(os.Stderr, fmtExit(ec, "GET job events status=%d", status))
			return ec
		}
		if jsonOutput {
			encoded, _ := json.Marshal(response)
			fmt.Println(string(encoded))
		} else {
			for _, event := range response.Events {
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
		}
		terminalStatus := response.Status
		if terminalStatus == "" {
			terminalStatus = response.Job.Status
		}
		// This endpoint reports the Velox JobStatus domain. Parse it into
		// the domain type before applying terminal semantics. COMPLETED is
		// reserved for producer-side InputAssemblyStatus and is therefore
		// rejected rather than treated as job success.
		jobStatus := jobs.JobStatus(strings.TrimSpace(terminalStatus))
		switch jobStatus {
		case jobs.StatusSucceeded:
			return ExitOK
		case jobs.StatusFailed, jobs.StatusCancelled:
			fmt.Fprintln(os.Stderr, fmtExit(ExitUnexpected, "job %s ended %s", jobID, jobStatus))
			return ExitUnexpected
		case jobs.JobStatus("COMPLETED"):
			fmt.Fprintln(os.Stderr, fmtExit(ExitUnexpected, "job %s returned input-assembly status COMPLETED, not a terminal JobStatus", jobID))
			return ExitUnexpected
		}
		select {
		case <-ctx.Done():
			fmt.Fprintln(os.Stderr, fmtExit(ExitUnexpected, "timed out waiting for job %s", jobID))
			return ExitUnexpected
		case <-time.After(interval):
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

const httpOK = 200

// runDrain — POST /api/v1/admin/workers/{id}/drain;
// polls /admin/operations/{op_id} until terminal.
