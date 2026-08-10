// handlers_worker_reads.go — worker status, inspect, and SSH diagnostics.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
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
	return runStatusMode(client, false)
}
func runStatusMode(client *fleetClient, production bool) int {
	ctx, cancel := context.WithTimeout(context.Background(), 30*1e9)
	defer cancel()
	resp := workerListResponse{}
	status, err := client.doJSON(ctx, "GET", "/api/v1/admin/workers", nil, &resp)
	if err != nil {
		fmt.Fprintln(os.Stderr, fmtExit(ExitUnexpected, "%v", err))
		return ExitUnexpected
	}
	if status != 200 {
		fmt.Fprintln(os.Stderr, fmtExit(MapHTTPStatusToOpExit(status), "GET /admin/workers status=%d", status))
		return MapHTTPStatusToOpExit(status)
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
			desired, _ := w["target_digest"].(string)
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

// runInspect — GET /api/v1/admin/workers/{id}; pretty-prints
// the full WorkerCard. Synchronous; no polling.
func runInspect(client *fleetClient, args []string) int {
	workerID, ok := oneArg(args)
	if !ok {
		fmt.Fprintln(os.Stderr, fmtExit(ExitMisuse, "inspect requires a worker_id"))
		return ExitMisuse
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*1e9)
	defer cancel()
	resp := workerCardResponse{}
	status, err := client.doJSON(ctx, "GET", "/api/v1/admin/workers/"+workerID, nil, &resp)
	if err != nil {
		fmt.Fprintln(os.Stderr, fmtExit(ExitUnexpected, "%v", err))
		return ExitUnexpected
	}
	if status != 200 {
		fmt.Fprintln(os.Stderr, fmtExit(MapHTTPStatusToOpExit(status), "GET /admin/workers/%s status=%d", workerID, status))
		return MapHTTPStatusToOpExit(status)
	}
	bs, _ := json.MarshalIndent(resp, "", "  ")
	fmt.Println(string(bs))
	return ExitOK
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
