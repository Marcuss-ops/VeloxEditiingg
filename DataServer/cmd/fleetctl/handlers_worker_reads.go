// handlers_worker_reads.go — worker status, inspect, and SSH diagnostics.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type workerListResponse struct {
	Workers []map[string]any `json:"workers"`
	Count   int              `json:"count"`
}
type workerCardResponse map[string]any
type sshCheckResponse struct {
	CheckedAt  string `json:"checked_at"`
	KeyFile    string `json:"key_file"`
	KnownHosts string `json:"known_hosts_file"`
	Workers    []struct {
		WorkerID string `json:"worker_id"`
		Host     string `json:"host"`
		User     string `json:"user"`
		Port     int    `json:"port"`
		SSH      string `json:"ssh"`
		HostKey  string `json:"hostkey"`
		Sudo     string `json:"sudo"`
		Detail   string `json:"detail"`
	} `json:"workers"`
	Summary struct {
		Total    int `json:"total"`
		SSHPass  int `json:"ssh_pass"`
		KeyPass  int `json:"key_pass"`
		SudoPass int `json:"sudo_pass"`
		Ready    int `json:"ready"`
	} `json:"summary"`
}

// runStatus — GET /api/v1/admin/workers; pretty-prints a
// WorkerCard-shaped table per worker. Synchronous; no polling.
func runStatus(client *fleetClient) int {
	return runStatusModeWithOutput(client, false, false)
}

func runStatusMode(client *fleetClient, production bool) int {
	return runStatusModeWithOutput(client, production, false)
}

func runStatusModeWithOutput(client *fleetClient, production, jsonOutput bool) int {
	ctx, cancel := context.WithTimeout(context.Background(), 30*1e9)
	defer cancel()
	resp := workerListResponse{}
	status, err := client.doJSON(ctx, "GET", "/api/v1/admin/workers", nil, &resp)
	if err != nil {
		fmt.Fprintln(os.Stderr, fmtExit(ExitUnexpected, "%v", err))
		return ExitUnexpected
	}
	if status != 200 {
		ec := MapHTTPStatusToOpExit(status)
		fmt.Fprintln(os.Stderr, fmtExit(ec, "GET /admin/workers status=%d", status))
		return ec
	}
	if jsonOutput {
		bs, _ := json.Marshal(resp)
		fmt.Println(string(bs))
		return ExitOK
	}
	// Pretty-print.
	if production {
		fmt.Printf("%-24s  %-32s  %-71s  %-71s  %-8s\n", "NAME", "WORKER_ID", "DESIRED DIGEST", "RUNNING DIGEST", "STATE")
		fmt.Printf("%-24s  %-32s  %-71s  %-71s  %-8s\n", "----", "---------", "--------------", "--------------", "-----")
		verified := true
		for _, w := range resp.Workers {
			wid, _ := w["worker_id"].(string)
			name, _ := w["worker_name"].(string)
			if name == "" {
				name, _ = w["hostname"].(string)
			}
			desired, _ := cardImageField(w, "target_digest").(string)
			running, _ := w["image_digest"].(string)
			desiredDigest, runningDigest := digestFromRef(desired), digestFromRef(running)
			state := "CLEAN"
			if desiredDigest == "" || runningDigest == "" || desiredDigest != runningDigest {
				state, verified = "DRIFT", false
			}
			fmt.Printf("%-24s  %-32s  %-71s  %-71s  %-8s\n", name, wid, desired, running, state)
		}
		if !verified || resp.Count == 0 {
			return ExitUnexpected
		}
		fmt.Printf("\n%d/%d workers verified\n", resp.Count, resp.Count)
		return ExitOK
	}
	fmt.Printf("%-24s  %-32s  %-9s  %-9s  %-9s  %-26s  %-13s\n", "NAME", "WORKER_ID", "STATUS", "HEALTH", "JOBS", "EXECUTOR@VERSION", "LAST_SMOKE")
	fmt.Printf("%-24s  %-32s  %-9s  %-9s  %-9s  %-26s  %-13s\n", "----", "---------", "------", "------", "----", "--------------", "----------")
	for _, w := range resp.Workers {
		wid, _ := w["worker_id"].(string)
		name, _ := w["worker_name"].(string)
		if name == "" {
			name, _ = w["hostname"].(string)
		}
		if name == "" {
			name = wid
		}
		statusS, _ := w["status"].(string)
		health, _ := w["health"].(string)
		executor, _ := w["executor"].(string)
		execVer, _ := w["executor_version"].(string)
		active, _ := w["active_jobs"].(float64)
		maxJobs, _ := w["max_active_jobs"].(float64)
		lastSmoke, _ := w["last_smoke_status"].(string)
		execLine := fmt.Sprintf("%s@%v", executor, execVer)
		fmt.Printf("%-24s  %-32s  %-9s  %-9s  %-9s  %-26s  %-13s\n",
			name, wid, statusS, health,
			fmt.Sprintf("%v/%v", active, maxJobs),
			execLine, lastSmoke)
	}
	fmt.Printf("\n%d workers\n", resp.Count)
	return ExitOK
}

// runInspect — GET /api/v1/admin/workers/{id}; pretty-prints the
// WorkerCard in two canonical sections by default:
//
//	IMAGE                    — real-time image state (running vs target
//	                           vs digest_match), NO operation history
//	LAST UPDATE OPERATION    — the last rollout operation (status/reason),
//	                           deliberately separate from the IMAGE view
//
// With --json, prints the full WorkerCard as indented JSON (the
// machine-readable contract consumed by scripts like
// align-worker-digest.sh / canary-worker-rollout.sh).
// Synchronous; no polling.
func runInspect(client *fleetClient, args []string) int {
	workerID, jsonOutput, err := parseInspectArgs(args)
	if err != nil {
		fmt.Fprintln(os.Stderr, fmtExit(ExitMisuse, "%v", err))
		return ExitMisuse
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*1e9)
	defer cancel()
	resp := workerCardResponse{}
	status, err := client.doJSON(ctx, "GET", workerReadPath(workerID), nil, &resp)
	if err != nil {
		fmt.Fprintln(os.Stderr, fmtExit(ExitUnexpected, "%v", err))
		return ExitUnexpected
	}
	if status != 200 {
		fmt.Fprintln(os.Stderr, fmtExit(MapHTTPStatusToOpExit(status), "GET /admin/workers/%s status=%d", workerID, status))
		return MapHTTPStatusToOpExit(status)
	}
	if jsonOutput {
		bs, _ := json.MarshalIndent(resp, "", "  ")
		fmt.Println(string(bs))
		return ExitOK
	}
	printInspectSections(resp)
	return ExitOK
}

// parseInspectArgs accepts the optional --json flag (raw card JSON) plus
// the mandatory worker_id. Global --master=/--token-file=/--verbose flags
// (both = and space forms) are consumed by loadClientConfig and ignored
// here, mirroring parseOperationsArgs / parseWaitReadyArgs.
func parseInspectArgs(args []string) (string, bool, error) {
	var workerID string
	jsonOutput := false
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "--json":
			jsonOutput = true
		case arg == "--master" || arg == "--token-file":
			if i+1 >= len(args) {
				return "", false, fmt.Errorf("%s requires a value", arg)
			}
			i++
		case strings.HasPrefix(arg, "--master=") || strings.HasPrefix(arg, "--token-file=") || arg == "--verbose":
		case strings.HasPrefix(arg, "-"):
			return "", false, fmt.Errorf("unknown inspect option %q", arg)
		default:
			if workerID != "" {
				return "", false, errors.New("inspect accepts exactly one worker_id")
			}
			workerID = arg
		}
	}
	if workerID == "" {
		return "", false, errors.New("inspect requires a worker_id")
	}
	return workerID, jsonOutput, nil
}

// printInspectSections renders the WorkerCard as the two canonical
// operator sections: IMAGE (real-time image state, no history) and
// LAST UPDATE OPERATION (the last rollout, status + reason).
func printInspectSections(resp workerCardResponse) {
	wid, _ := resp["worker_id"].(string)
	name, _ := resp["worker_name"].(string)
	if name == "" {
		name, _ = resp["hostname"].(string)
	}
	status, _ := resp["status"].(string)
	health, _ := resp["health"].(string)
	if health == "" {
		health, _ = resp["health_state"].(string)
	}
	executor, _ := resp["executor"].(string)
	execVer, _ := resp["executor_version"].(string)
	active, _ := resp["active_jobs"].(float64)
	maxJobs, _ := resp["max_active_jobs"].(float64)

	fmt.Printf("worker_id:  %s\n", displayValue(wid))
	if name != "" && name != wid {
		fmt.Printf("worker_name: %s\n", name)
	}
	fmt.Printf("status:     %s\n", displayValue(status))
	fmt.Printf("health:     %s\n", displayValue(health))
	if executor != "" {
		fmt.Printf("executor:   %s@%v\n", executor, execVer)
	}
	fmt.Printf("jobs:       %v/%v\n", displayValue(fmt.Sprintf("%.0f", active)), displayValue(fmt.Sprintf("%.0f", maxJobs)))

	fmt.Println()
	fmt.Println("IMAGE")
	running, _ := cardImageField(resp, "running_digest").(string)
	target, _ := cardImageField(resp, "target_digest").(string)
	match := "unknown"
	if m, ok := cardImageField(resp, "digest_match").(bool); ok {
		match = strconv.FormatBool(m)
	}
	fmt.Printf("  running_digest = %s\n", displayValue(running))
	fmt.Printf("  target_digest  = %s\n", displayValue(target))
	fmt.Printf("  digest_match   = %s\n", match)

	fmt.Println()
	fmt.Println("LAST UPDATE OPERATION")
	if op, ok := resp["operation_state"].(map[string]any); ok {
		opStatus, _ := op["status"].(string)
		reason, _ := op["error"].(string)
		opID, _ := op["operation_id"].(string)
		opType, _ := op["type"].(string)
		started, _ := op["started_at"].(string)
		finished, _ := op["finished_at"].(string)
		fmt.Printf("  status       = %s\n", displayValue(opStatus))
		fmt.Printf("  reason       = %s\n", displayValue(reason))
		fmt.Printf("  operation_id = %s\n", displayValue(opID))
		fmt.Printf("  type         = %s\n", displayValue(opType))
		fmt.Printf("  started_at   = %s\n", displayValue(started))
		fmt.Printf("  finished_at  = %s\n", displayValue(finished))
	} else {
		fmt.Println("  (no update operation on record)")
	}
}

// cardImageField reads a field from the canonical image_state section,
// falling back to the legacy flat card key when the nested section is
// absent (older master / diagnostic surface).
func cardImageField(card workerCardResponse, key string) any {
	if img, ok := card["image_state"].(map[string]any); ok {
		if value, present := img[key]; present {
			return value
		}
	}
	return card[key]
}
func runSSHCheck(client *fleetClient) int {
	ctx, cancel := context.WithTimeout(context.Background(), 2*60*1e9)
	defer cancel()
	resp := sshCheckResponse{}
	status, err := client.doJSON(ctx, "GET", "/api/v1/admin/workers/ssh-check", nil, &resp)
	if err != nil {
		fmt.Fprintln(os.Stderr, fmtExit(ExitUnexpected, "%v", err))
		return ExitUnexpected
	}
	if status != 200 {
		ec := MapHTTPStatusToOpExit(status)
		fmt.Fprintln(os.Stderr, fmtExit(ec, "GET /admin/workers/ssh-check status=%d", status))
		return ec
	}
	fmt.Printf("%-24s  %-5s  %-6s  %-5s  %-10s\n", "WORKER", "SSH", "HOSTKEY", "SUDO", "RESULT")
	fmt.Printf("%-24s  %-5s  %-6s  %-5s  %-10s\n", strings.Repeat("-", 24), "-----", "------", "-----", "----------")
	for _, w := range resp.Workers {
		result := "READY"
		if w.SSH != "PASS" || w.HostKey != "PASS" || w.Sudo != "PASS" {
			result = "NOT-READY"
		}
		fmt.Printf("%-29s  %-5s  %-6s  %-5s  %-10s\n", w.WorkerID, w.SSH, w.HostKey, w.Sudo, result)
		if w.Detail != "" && w.SSH != "PASS" {
			fmt.Fprintf(os.Stderr, "  └─ %s: %s\n", w.WorkerID, w.Detail)
		}
	}
	fmt.Printf("\n%d/%d READY (ssh=%d key=%d sudo=%d)  key=%s known_hosts=%s\n",
		resp.Summary.Ready, resp.Summary.Total,
		resp.Summary.SSHPass, resp.Summary.KeyPass, resp.Summary.SudoPass,
		resp.KeyFile, resp.KnownHosts)
	if resp.Summary.Total == 0 {
		return ExitUnexpected
	}
	if resp.Summary.Ready != resp.Summary.Total {
		return ExitUnexpected
	}
	return ExitOK
}
