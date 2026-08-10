// handlers_jobs_submit.go — job submit/certify in Go (migrated from
// scripts/fleetctl-legacy job_submit()).
//
// The flow mirrors the legacy Bash exactly:
//
//  1. Parse --payload/--workers/--idempotency-prefix/--wait.
//  2. Resolve worker selection ("all" → GET /api/v1/admin/workers,
//     otherwise the comma-separated list).
//  3. Issue an ephemeral M2M key (POST /api/v1/admin/m2m/keys,
//     scopes=["jobs.submit"]) authenticated with the ADMIN token.
//  4. For each worker, POST /api/v1/jobs with the payload augmented by
//     idempotency_key=<prefix>-<worker>-<ns> and
//     placement_pin_worker_id=<worker>, authenticated with the M2M
//     token (least-privilege: the admin token never submits jobs).
//  5. DELETE the ephemeral M2M key on exit (defer — the Bash trap).
//  6. With --wait (always on for certify), poll /api/v1/jobs/{id} with
//     the M2M token until terminal, then print the final admin snapshot.
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
)

// runJobSubmit implements `job submit` (wait=false) and `job certify`
// (wait=true). Returns the canonical fleetctl exit code.
func runJobSubmit(client *fleetClient, args []string, certify bool) int {
	opts, err := parseJobSubmitArgs(args)
	if err != nil {
		fmt.Fprintln(os.Stderr, fmtExit(ExitMisuse, "%v", err))
		return ExitMisuse
	}
	payloadFile := opts.payloadFile
	raw, err := os.ReadFile(payloadFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, fmtExit(ExitMisuse, "read job payload file: %v", err))
		return ExitMisuse
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil || payload == nil {
		fmt.Fprintln(os.Stderr, fmtExit(ExitMisuse, "job payload must be a JSON object: %v", err))
		return ExitMisuse
	}

	workers, err := resolveWorkerIDs(client, opts.selection)
	if err != nil {
		fmt.Fprintln(os.Stderr, fmtExit(ExitUnexpected, "%v", err))
		return ExitUnexpected
	}
	if len(workers) == 0 {
		fmt.Fprintln(os.Stderr, fmtExit(ExitMisuse, "no workers selected"))
		return ExitMisuse
	}

	// Ephemeral M2M key lifecycle. The Bash bridge used a trap; Go uses
	// defer so the key is disabled even on partial-failure exits.
	m2mToken, cleanup, err := issueM2MKey(client, opts.idemPrefix)
	if err != nil {
		fmt.Fprintln(os.Stderr, fmtExit(ExitUnexpected, "%v", err))
		return ExitUnexpected
	}
	defer cleanup()

	m2mClient := client.withToken(m2mToken)

	jobIDs := make([]string, 0, len(workers))
	for _, workerID := range workers {
		requestCtx, requestCancel := context.WithTimeout(context.Background(), 60*time.Second)
		key := fmt.Sprintf("%s-%s-%d", opts.idemPrefix, workerID, time.Now().UnixNano())
		jobID, err := submitOneJob(requestCtx, m2mClient, payload, workerID, key)
		requestCancel()
		if err != nil {
			fmt.Fprintln(os.Stderr, fmtExit(ExitUnexpected, "%v", err))
			return ExitUnexpected
		}
		jobIDs = append(jobIDs, jobID)
		fmt.Fprintf(os.Stderr, "fleetctl: submitted job=%s worker=%s\n", jobID, workerID)
	}

	if !opts.wait && !certify {
		for _, jobID := range jobIDs {
			fmt.Println(jobID)
		}
		return ExitOK
	}

	// --wait / certify: poll each job to terminal state, then print the
	// final admin snapshot (same surface the legacy print_job_card used).
	overall := ExitOK
	for _, jobID := range jobIDs {
		if err := waitForJob(context.Background(), m2mClient, jobID); err != nil {
			fmt.Fprintln(os.Stderr, fmtExit(ExitUnexpected, "%v", err))
			overall = ExitUnexpected
			continue
		}
		if err := printFinalJobSnapshot(client, jobID); err != nil {
			fmt.Fprintln(os.Stderr, fmtExit(ExitUnexpected, "%v", err))
			overall = ExitUnexpected
		}
	}
	return overall
}

// jobSubmitOptions holds the parsed `job submit` flags.
type jobSubmitOptions struct {
	payloadFile string
	selection   string // "all" or a comma-separated worker_id list
	idemPrefix  string
	wait        bool
}

func parseJobSubmitArgs(args []string) (jobSubmitOptions, error) {
	opts := jobSubmitOptions{
		selection:  "all",
		idemPrefix: fmt.Sprintf("fleetctl-%d", time.Now().Unix()),
	}
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case strings.HasPrefix(arg, "--payload="):
			opts.payloadFile = strings.TrimPrefix(arg, "--payload=")
		case arg == "--payload":
			if i+1 >= len(args) {
				return opts, errors.New("--payload requires a file")
			}
			i++
			opts.payloadFile = args[i]
		case strings.HasPrefix(arg, "--workers="):
			opts.selection = strings.TrimPrefix(arg, "--workers=")
		case arg == "--workers":
			if i+1 >= len(args) {
				return opts, errors.New("--workers requires all or a comma-separated worker_id list")
			}
			i++
			opts.selection = args[i]
		case strings.HasPrefix(arg, "--idempotency-prefix="):
			opts.idemPrefix = strings.TrimPrefix(arg, "--idempotency-prefix=")
		case arg == "--idempotency-prefix":
			if i+1 >= len(args) {
				return opts, errors.New("--idempotency-prefix requires a value")
			}
			i++
			opts.idemPrefix = args[i]
		case arg == "--wait":
			opts.wait = true
		case strings.HasPrefix(arg, "--master=") || strings.HasPrefix(arg, "--token-file=") || arg == "--verbose":
			// global flags already consumed by loadClientConfig
		case arg == "--master" || arg == "--token-file":
			if i+1 >= len(args) {
				return opts, errors.New(arg + " requires a value")
			}
			i++
		case strings.HasPrefix(arg, "-"):
			return opts, fmt.Errorf("unknown job submit option %q", arg)
		default:
			return opts, fmt.Errorf("unexpected job submit argument %q", arg)
		}
	}
	if opts.payloadFile == "" {
		return opts, errors.New("job payload file is required and must be readable")
	}
	return opts, nil
}

// resolveWorkerIDs expands a selection into a worker_id slice. "all"
// reads the admin worker list; a comma-separated value is used verbatim.
func resolveWorkerIDs(client *fleetClient, selection string) ([]string, error) {
	if selection != "all" {
		var ids []string
		for _, w := range strings.Split(selection, ",") {
			w = strings.TrimSpace(w)
			if w == "" {
				return nil, errors.New("worker selection contains an empty worker_id")
			}
			ids = append(ids, w)
		}
		return ids, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var response struct {
		Workers []struct {
			WorkerID string `json:"worker_id"`
		} `json:"workers"`
	}
	status, err := client.doJSON(ctx, "GET", "/api/v1/admin/workers", nil, &response)
	if err != nil {
		return nil, fmt.Errorf("list workers: %w", err)
	}
	if status != httpOK {
		return nil, fmt.Errorf("list workers status=%d", status)
	}
	ids := make([]string, 0, len(response.Workers))
	for _, w := range response.Workers {
		if w.WorkerID != "" {
			ids = append(ids, w.WorkerID)
		}
	}
	return ids, nil
}

// issueM2MKey creates an ephemeral jobs.submit key with the admin token
// and returns the plaintext secret plus a cleanup func that disables the
// key (best-effort, mirroring the legacy trap).
func issueM2MKey(client *fleetClient, prefix string) (string, func(), error) {
	clientID := fmt.Sprintf("%s-%d-%d", strings.TrimPrefix(prefix, "fleetctl-"), os.Getpid(), time.Now().UnixNano())
	if strings.HasPrefix(prefix, "fleetctl-") {
		clientID = fmt.Sprintf("fleetctl-%s-%d-%d", strings.TrimPrefix(prefix, "fleetctl-"), os.Getpid(), time.Now().UnixNano())
	}
	body := map[string]any{
		"client_id":   clientID,
		"description": "fleetctl ephemeral job operator",
		"scopes":      []string{"jobs.submit"},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var response struct {
		ClientID        string   `json:"client_id"`
		PlaintextSecret string   `json:"plaintext_secret"`
		Scopes          []string `json:"scopes"`
	}
	status, err := client.doJSON(ctx, "POST", "/api/v1/admin/m2m/keys", body, &response)
	if err != nil {
		return "", nil, fmt.Errorf("issue m2m key: %w", err)
	}
	if status != httpOK && status != 201 {
		return "", nil, fmt.Errorf("issue m2m key status=%d", status)
	}
	if response.PlaintextSecret == "" {
		return "", nil, errors.New("m2m issue response did not contain plaintext_secret")
	}
	cleanup := func() {
		delCtx, delCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer delCancel()
		// Best-effort: ignore errors so the operator's primary exit
		// code is not masked by a cleanup failure.
		_, _ = client.doJSON(delCtx, "DELETE", "/api/v1/admin/m2m/keys/"+url.PathEscape(response.ClientID), nil, nil)
	}
	return response.PlaintextSecret, cleanup, nil
}

// submitOneJob POSTs the payload augmented with idempotency_key and
// placement_pin_worker_id to /api/v1/jobs using the M2M token.
func submitOneJob(ctx context.Context, m2m *fleetClient, payload map[string]any, workerID, key string) (string, error) {
	augmented := make(map[string]any, len(payload)+2)
	for k, v := range payload {
		augmented[k] = v
	}
	augmented["idempotency_key"] = key
	augmented["placement_pin_worker_id"] = workerID

	var response map[string]any
	status, err := m2m.doJSON(ctx, "POST", "/api/v1/jobs", augmented, &response)
	if err != nil {
		return "", fmt.Errorf("submit job worker=%s: %w", workerID, err)
	}
	if status != httpOK && status != 202 {
		return "", fmt.Errorf("submit job worker=%s status=%d", workerID, status)
	}
	jobID, _ := response["job_id"].(string)
	if jobID == "" {
		if id, ok := response["id"].(string); ok {
			jobID = id
		}
	}
	if jobID == "" {
		return "", fmt.Errorf("submit job worker=%s returned no job_id", workerID)
	}
	return jobID, nil
}

// waitForJob polls /api/v1/jobs/{id} with the M2M token until the job
// reaches a terminal lifecycle state or the 1h default / explicit env
// timeout elapses.
func waitForJob(ctx context.Context, m2m *fleetClient, jobID string) error {
	timeout := envSeconds("FLEETCTL_JOB_TIMEOUT_SECONDS", 3600)
	poll := envSeconds("FLEETCTL_JOB_POLL_SECONDS", 5)
	deadline := time.Now().Add(timeout)
	for {
		requestCtx, requestCancel := context.WithTimeout(ctx, 30*time.Second)
		var response struct {
			Status string `json:"status"`
			State  string `json:"state"`
		}
		status, err := m2m.doJSON(requestCtx, "GET", "/api/v1/jobs/"+url.PathEscape(jobID), nil, &response)
		requestCancel()
		if err != nil {
			return fmt.Errorf("poll job %s: %w", jobID, err)
		}
		if status != httpOK {
			return fmt.Errorf("poll job %s status=%d", jobID, status)
		}
		current := strings.ToUpper(strings.TrimSpace(response.Status))
		if current == "" {
			current = strings.ToUpper(strings.TrimSpace(response.State))
		}
		fmt.Fprintf(os.Stderr, "fleetctl: job=%s status=%s\n", jobID, current)
		switch current {
		case "SUCCEEDED":
			return nil
		case "COMPLETED":
			return fmt.Errorf("job %s returned ambiguous COMPLETED status; expected SUCCEEDED", jobID)
		case "FAILED", "CANCELLED":
			return fmt.Errorf("job %s ended %s", jobID, current)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for job=%s (last status=%s)", jobID, current)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for job=%s", jobID)
		case <-time.After(poll):
		}
	}
}

// printFinalJobSnapshot fetches the admin job snapshot and prints the
// complete JSON document (the surface the legacy print_job_card read).
func printFinalJobSnapshot(client *fleetClient, jobID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var response map[string]any
	status, err := client.doJSON(ctx, "GET", "/api/v1/admin/jobs/"+url.PathEscape(jobID), nil, &response)
	if err != nil {
		return fmt.Errorf("job snapshot %s: %w", jobID, err)
	}
	if status != httpOK {
		return fmt.Errorf("job snapshot %s status=%d", jobID, status)
	}
	bs, err := json.MarshalIndent(response, "", "  ")
	if err != nil {
		return fmt.Errorf("job snapshot %s: %w", jobID, err)
	}
	fmt.Println(string(bs))
	return nil
}
