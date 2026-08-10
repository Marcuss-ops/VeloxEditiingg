// handlers_mutations.go — worker mutation commands and ledger polling.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
)

type mutationResponse struct {
	WorkerID    string `json:"worker_id"`
	OperationID string `json:"operation_id"`
	Op          string `json:"op"`
	Status      string `json:"status"`
	QueuedAt    string `json:"queued_at"`
	Reason      string `json:"reason"`
}

// sshCheckResponse matches GET /api/v1/admin/workers/ssh-check.
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
		map[string]any{"target_digest": workerImageRef(*digest), "reason": *reason})
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
