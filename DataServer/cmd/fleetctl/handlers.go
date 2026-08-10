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
