// handlers.go — per-sub-command handlers for fleetctl's 7 listed
// sub-commands. Each handler:
//
//  1. Validates the input (via the dedicated helpers in
//     digest.go / auth.go).
//  2. Calls the Master REST endpoint via the fleetClient.
//  3. Maps non-2xx responses to canonical exit codes per
//     exit_codes.go (MapHTTPStatusToOpExit for op-issuing
//     requests; for read-only requests like status/inspect,
//     404 → ExitWorkerNotFound; 401/403 → ExitMisuse).
//  4. For OPERATION-issuing requests (drain/update/smoke/
//     resume/rollback), polls the ledger via pollOperationLedger
//     with the kind-specific default budget. On terminal
//     SUCCEEDED, handler prints the operation row + returns
//     ExitOK. On FAILED / ROLLBACK, handler maps via
//     MapOperationKindToExit and surfaces the audit row's
//     error_message verbatim.
//
// Output conventions:
//   - Success (terminal SUCCEEDED): one row per worker / per
//     op; JSON table-like format that scripts can grep.
//   - Success (synchronous read): the WorkerCard JSON line.
//   - Failure: "fleetctl: <error-tag>: <message>" to stderr.
//     The error message is the canonical audit row's
//     error_message OR a transport/system error verbatim.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
)

// workerListResponse matches GET /api/v1/admin/workers envelope
// (top-level keys: count, workers).
type workerListResponse struct {
	Workers []map[string]any `json:"workers"`
	Count   int              `json:"count"`
}

type workerCardResponse map[string]any

type mutationResponse struct {
	WorkerID    string `json:"worker_id"`
	OperationID string `json:"operation_id"`
	Op          string `json:"op"`
	Status      string `json:"status"`
	QueuedAt    string `json:"queued_at"`
	Reason      string `json:"reason"`
}

// runStatus — GET /api/v1/admin/workers; pretty-prints a
// WorkerCard-shaped table per worker. Synchronous; no polling.
func runStatus(client *fleetClient) int {
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
	fmt.Printf("%-32s  %-9s  %-9s  %-9s  %-26s  %-13s\n", "WORKER_ID", "STATUS", "HEALTH", "JOBS", "EXECUTOR@VERSION", "LAST_SMOKE")
	fmt.Printf("%-32s  %-9s  %-9s  %-9s  %-26s  %-13s\n", "---------", "------", "------", "----", "--------------", "----------")
	for _, w := range resp.Workers {
		wid, _ := w["worker_id"].(string)
		statusS, _ := w["status"].(string)
		health, _ := w["health"].(string)
		executor, _ := w["executor"].(string)
		execVer, _ := w["executor_version"].(string)
		active, _ := w["active_jobs"].(float64)
		maxJobs, _ := w["max_active_jobs"].(float64)
		lastSmoke, _ := w["last_smoke_status"].(string)
		execLine := fmt.Sprintf("%s@%v", executor, execVer)
		fmt.Printf("%-32s  %-9s  %-9s  %-9s  %-26s  %-13s\n",
			wid, statusS, health,
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

// runDrain — POST /api/v1/admin/workers/{id}/drain;
// polls /admin/operations/{op_id} until terminal.
func runDrain(client *fleetClient, args []string) int {
	workerID, ok := oneArg(args)
	if !ok {
		fmt.Fprintln(os.Stderr, fmtExit(ExitMisuse, "drain requires a worker_id"))
		return ExitMisuse
	}
	reason := parseReasonFlag(args)
	return runMutation(client, "drain", workerID, "/api/v1/admin/workers/"+workerID+"/drain",
		map[string]any{"reason": reason})
}

// runUpdate — POST /api/v1/admin/workers/{id}/update
// after validating --digest sha256: regex.
func runUpdate(client *fleetClient, args []string) int {
	workerID, ok := oneArg(args)
	if !ok {
		fmt.Fprintln(os.Stderr, fmtExit(ExitMisuse, "update requires a worker_id"))
		return ExitMisuse
	}
	fs := flag.NewFlagSet("update", flag.ContinueOnError)
	digest := fs.String("digest", "", "target image digest, must match ^sha256:[0-9a-f]{64}$")
	reason := fs.String("reason", "fleetctl update", "operator-readable reason (audit row)")
	if err := fs.Parse(args); err != nil {
		fmt.Fprintln(os.Stderr, fmtExit(ExitMisuse, "%v", err))
		return ExitMisuse
	}
	if err := validateDigest(*digest); err != nil {
		fmt.Fprintln(os.Stderr, fmtExit(ExitImageInvalid, "%v", err))
		return ExitImageInvalid
	}
	return runMutation(client, "update", workerID,
		"/api/v1/admin/workers/"+workerID+"/update",
		map[string]any{"target_digest": *digest, "reason": *reason})
}

// runSmoke — POST /api/v1/admin/workers/{id}/smoke;
// polls /admin/operations/{op_id}; on terminal FAILED, exit 6.
func runSmoke(client *fleetClient, args []string) int {
	workerID, ok := oneArg(args)
	if !ok {
		fmt.Fprintln(os.Stderr, fmtExit(ExitMisuse, "smoke requires a worker_id"))
		return ExitMisuse
	}
	assetID := "asset-canary-001"
	renderPlan := "ffmpeg -i {{ pickup_url }} -c:v libx264 -t 5 /tmp/smoke-{{ worker_id }}.mp4"
	timeoutSec := 600
	reason := "fleetctl smoke"
	// Allow --asset-id etc. override in future; today's atomic
	// surface uses the defaults.
	return runMutation(client, "smoke", workerID,
		"/api/v1/admin/workers/"+workerID+"/smoke",
		map[string]any{"asset_id": assetID, "render_plan": renderPlan, "timeout_sec": timeoutSec, "reason": reason})
}

// runResume — POST /api/v1/admin/workers/{id}/resume;
// polls /admin/operations/{op_id}.
func runResume(client *fleetClient, args []string) int {
	workerID, ok := oneArg(args)
	if !ok {
		fmt.Fprintln(os.Stderr, fmtExit(ExitMisuse, "resume requires a worker_id"))
		return ExitMisuse
	}
	reason := parseReasonFlag(args)
	return runMutation(client, "resume", workerID,
		"/api/v1/admin/workers/"+workerID+"/resume",
		map[string]any{"reason": reason})
}

// runRollback — POST /api/v1/admin/workers/{id}/rollback;
// polls /admin/operations/{op_id}; on terminal FAILED/ROLLBACK, exit 8.
func runRollback(client *fleetClient, args []string) int {
	workerID, ok := oneArg(args)
	if !ok {
		fmt.Fprintln(os.Stderr, fmtExit(ExitMisuse, "rollback requires a worker_id"))
		return ExitMisuse
	}
	reason := parseReasonFlag(args)
	return runMutation(client, "rollback", workerID,
		"/api/v1/admin/workers/"+workerID+"/rollback",
		map[string]any{"reason": reason})
}

// runMutation is the shared post+polling helper. Issues the
// POST then polls until terminal SUCCEEDED (return ExitOK) or
// terminal FAILED/ROLLBACK (return MapOperationKindToExit).
func runMutation(client *fleetClient, opKind, workerID, path string, body map[string]any) int {
	ctx, cancel := context.WithTimeout(context.Background(), 5*60*1e9)
	defer cancel()
	post := mutationResponse{}
	status, err := client.doJSON(ctx, "POST", path, body, &post)
	if err != nil {
		fmt.Fprintln(os.Stderr, fmtExit(ExitUnexpected, "%v", err))
		return ExitUnexpected
	}
	if status != 202 && status != 200 {
		ec := MapHTTPStatusToOpExit(status)
		fmt.Fprintln(os.Stderr, fmtExit(ec, "POST %s status=%d", path, status))
		return ec
	}
	if post.OperationID == "" {
		fmt.Fprintln(os.Stderr, fmtExit(ExitUnexpected, "POST %s returned no operation_id", path))
		return ExitUnexpected
	}
	fmt.Printf("[fleetctl] %s queued: operation_id=%s worker_id=%s queued_at=%s\n",
		opKind, post.OperationID, post.WorkerID, post.QueuedAt)

	budget := defaultWaitBudget[opKind]
	row, err := pollOperationLedger(ctx, client, post.OperationID, budget, client.verbose)
	if err != nil {
		fmt.Fprintln(os.Stderr, fmtExit(ExitUnexpected, "%v", err))
		return ExitUnexpected
	}
	if row.Status == "SUCCEEDED" {
		fmt.Printf("[fleetctl] %s SUCCEEDED: operation_id=%s\n", opKind, row.OperationID)
		return ExitOK
	}
	// FAILED / ROLLBACK
	ec := MapOperationKindToExit(opKind)
	if row.Status == "ROLLBACK" && opKind != "rollback" {
		// Update-cascade rollback without operator intent.
		fmt.Fprintln(os.Stderr, fmtExit(ec, "operation %s ended in ROLLBACK (auto-revert by Step 9/15): error_message=%q", row.OperationID, row.ErrorMessage))
	} else {
		fmt.Fprintln(os.Stderr, fmtExit(ec, "operation %s ended %s: error_message=%q", row.OperationID, row.Status, row.ErrorMessage))
	}
	return ec
}

// oneArg returns the first positional arg of `args` AFTER flag
// stripping. Used by sub-command handlers that need a single
// worker_id. Empty string + false if missing.
func oneArg(args []string) (string, bool) {
	for _, a := range args {
		if strings.HasPrefix(a, "-") {
			continue
		}
		return a, true
	}
	return "", false
}

// parseReasonFlag pulls --reason=<value> out of args defensively
// (returns the default if missing). Kept simple; richer flag
// parsing lives in runUpdate.
func parseReasonFlag(args []string) string {
	for _, a := range args {
		if strings.HasPrefix(a, "--reason=") {
			return strings.TrimPrefix(a, "--reason=")
		}
	}
	return "fleetctl " + safeFirstArg(args)
}

func safeFirstArg(args []string) string {
	for _, a := range args {
		if !strings.HasPrefix(a, "-") {
			return a
		}
	}
	return ""
}
