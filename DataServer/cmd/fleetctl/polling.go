// polling.go — fleetctl operation polling loop per design Q6.
//
// The Master REST API is asynchronous for mutations:
//   - POST /api/v1/admin/workers/{id}/{drain,smoke,update,
//     rollback,resume} returns 202 Accepted with
//     {operation_id, queued_at}.
//   - The FleetController tick goroutine (Step 4/15) drives
//     the registered executor (Step 9/15 for update /
//     rollback; Step 6/15 for drain / resume / quarantine;
//     Step 12/15 for smoke).
//   - The terminal status is visible GET /api/v1/admin/
//     operations/{operation_id}, polled every 5s until the
//     status is in {SUCCEEDED, FAILED, ROLLBACK} OR
//     deadlineMS elapses.
//
// Default budgets match the per-step opTimeout values:
//
//	drain    10 min  (Step 6/15 budget)
//	resume     5 min  (no specific budget; shorter surface)
//	update    30 min  (Step 9/15 cascade)
//	smoke     12 min  (Step 12/15 sub-budget)
//	rollback  30 min  (Step 9/15 rollback)
//
// --wait=<duration> flag overrides per-run.
package main

import (
	"context"
	"fmt"
	"os"
	"time"
)

// defaultWaitBudget is the time fleetctl spends polling before
// declaring timeout. Indexed by OperationKind — ANY sub-command
// using pollOperationLedger passes the matching kind.
var defaultWaitBudget = map[string]time.Duration{
	"smoke":      12 * time.Minute,
	"drain":      10 * time.Minute,
	"resume":     5 * time.Minute,
	"quarantine": 5 * time.Minute,
	"update":     30 * time.Minute,
	"rollback":   30 * time.Minute,
}

// waitPollingInterval sleeps until the next poll or until the caller's
// context is cancelled. A timer is used instead of time.After so an
// interrupted poll does not retain an uncancellable timer until the interval
// expires.
func waitPollingInterval(ctx context.Context, interval time.Duration) bool {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// pollInterval is the steady-state poll period for the Master operation
// ledger. FleetController is the sole production mutation owner.
const pollInterval = 5 * time.Second

// polledOperationRow is the slice of GET /api/v1/admin/operations/{id}
// we read. Mirrors fleet_operations ledger columns.
type polledOperationRow struct {
	OperationID  string `json:"operation_id"`
	WorkerID     string `json:"worker_id"`
	Op           string `json:"op"`
	Status       string `json:"status"`
	ErrorMessage string `json:"error_message"`
	QueuedAt     string `json:"queued_at"`
	StartedAt    string `json:"started_at,omitempty"`
	FinishedAt   string `json:"finished_at,omitempty"`
}

// terminalStatuses returns true when the row has reached a
// terminal state (SUCCEEDED / FAILED / ROLLBACK).
func terminalStatuses(s string) bool {
	switch s {
	case "SUCCEEDED", "FAILED", "ROLLBACK":
		return true
	default:
		return false
	}
}

// pollOperationLedger polls GET /api/v1/admin/operations/{id}
// every pollInterval until the row is terminal OR deadline
// elapses. Returns the final row on success; on deadline, returns
// a non-nil row in the last-known non-terminal status + a
// formatted timeout error message.
//
// When verbose is true (mirrors --verbose global flag), each
// poll cycle writes one line to stderr with the current status so operators
// can see the "DRAINING → QUEUED → RUNNING → SUCCEEDED" trace.
func pollOperationLedger(ctx context.Context, client *fleetClient, operationID string, deadline time.Duration, verbose bool) (*polledOperationRow, error) {
	return pollOperationLedgerWithInterval(ctx, client, operationID, deadline, verbose, pollInterval)
}

func pollOperationLedgerWithInterval(ctx context.Context, client *fleetClient, operationID string, deadline time.Duration, verbose bool, interval time.Duration) (*polledOperationRow, error) {
	endAt := time.Now().Add(deadline)
	attempts := 0
	last := &polledOperationRow{OperationID: operationID, Status: "QUEUED"}
	for {
		attempts++
		row := &polledOperationRow{}
		status, err := client.doJSON(ctx, "GET", operationPath(operationID), nil, row)
		if err != nil {
			if verbose {
				fmt.Fprintf(os.Stderr, "[fleetctl] poll #%d status=%d err=%v\n", attempts, status, err)
			}
			if status == 404 {
				return nil, fmt.Errorf("operation %q not found on Master (already cleaned up?)", operationID)
			}
			// Transient transport error: retry within budget.
		} else {
			last = row
			if terminalStatuses(row.Status) {
				if verbose {
					fmt.Fprintf(os.Stderr, "[fleetctl] poll #%d -> %s\n", attempts, row.Status)
				}
				return row, nil
			}
			if verbose {
				fmt.Fprintf(os.Stderr, "[fleetctl] poll #%d status=%s\n", attempts, row.Status)
			}
		}
		if time.Now().After(endAt) {
			return last, fmt.Errorf("deadline (%s) reached; last known status=%s", deadline, last.Status)
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return last, ctx.Err()
		case <-timer.C:
		}
	}
}
