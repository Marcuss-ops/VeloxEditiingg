// exit_codes.go — canonical exit-code matrix per design Q3 of
// the Step 15/15 thinker call.
//
// Mapping table:
//
//   0  ExitOK              operation completed (sync read or polled to SUCCEEDED)
//   1  ExitUnexpected      transport / decode / unexpected HTTP 5xx
//   2  ExitMisuse          bad flag, missing arg, missing token, invalid --digest
//   4  ExitWorkerNotFound  Master returned 404 for /admin/workers/{id}
//   5  ExitLeaseUnavailable Master returned 409 (operation in-flight for worker)
//   6  ExitSmokeFailed     smoke polled to FAILED in fleet_operations ledger
//   7  ExitImageInvalid    --digest rejected by client-side validator OR Master (Step 5/15 invariant)
//   8  ExitRollbackFailed  rollback polled to FAILED / ROLLBACK in fleet_operations ledger
//
// The matrix is exported so operator scripts can pattern-match
// on $? without parsing stderr. Examples do not exist in the
// repo but the README's Exit-Code Matrix section documents
// each.
package main

import "fmt"

// Exit codes — intentionally small + scriptable. Don't reintroduce
// categories without first updating deploy/fleetctl/README.md
// so dashboards stay in sync with shell scripts.
const (
	ExitOK              = 0
	ExitUnexpected      = 1
	ExitMisuse          = 2
	ExitWorkerNotFound  = 4
	ExitLeaseUnavailable = 5
	ExitSmokeFailed     = 6
	ExitImageInvalid    = 7
	ExitRollbackFailed  = 8
)

// exitCodeName returns a stable lowercase string for the exit
// code. Used in error messages so an operator running
// `fleetctl smoke <id>; echo $?` can correlate $?=6 with
// "smoke failed" without referring to the docs.
func exitCodeName(c int) string {
	switch c {
	case ExitOK:
		return "ok"
	case ExitUnexpected:
		return "unexpected"
	case ExitMisuse:
		return "misuse"
	case ExitWorkerNotFound:
		return "worker-not-found"
	case ExitLeaseUnavailable:
		return "lease-unavailable"
	case ExitSmokeFailed:
		return "smoke-failed"
	case ExitImageInvalid:
		return "image-invalid"
	case ExitRollbackFailed:
		return "rollback-failed"
	default:
		return fmt.Sprintf("code=%d", c)
	}
}

// fmtExit renders "fleetctl: <error-tag>: <message>" to stderr
// without exiting. Caller adds the `os.Exit(N)` line so test
// paths can assert the integer without invoking os.Exit.
func fmtExit(code int, format string, args ...any) string {
	tag := exitCodeName(code)
	msg := fmt.Sprintf(format, args...)
	return fmt.Sprintf("fleetctl: %s: %s", tag, msg)
}

// MapHTTPStatusToOpExit maps a Master-API HTTP status code (when
// the response is non-2xx for an OPERATION-issuing request) to
// the canonical fleetctl exit. The handler chain keeps the
// mapping centralised so we don't double-encode 409 / 404 / 422
// across drain / smoke / rollback / update / resume.
func MapHTTPStatusToOpExit(status int) int {
	switch status {
	case 404:
		return ExitWorkerNotFound
	case 409:
		return ExitLeaseUnavailable
	case 400, 422:
		return ExitImageInvalid
	case 500, 502, 503, 504:
		return ExitUnexpected
	default:
		// 403 / 401: surface as misuse (operator likely has the
		// wrong token; the operator's fix is to re-source the
		// token, not to retry the request).
		if status == 401 || status == 403 {
			return ExitMisuse
		}
		return ExitUnexpected
	}
}

// MapOperationKindToExit returns the exit code for a polled
// FAILED fleet_operations row by OperationKind. Pre-conditions:
// the row is already known to be in a terminal FAILED / ROLLBACK
// state.
func MapOperationKindToExit(opKind string) int {
	switch opKind {
	case "smoke":
		return ExitSmokeFailed
	case "rollback":
		return ExitRollbackFailed
	case "update", "drain", "resume", "quarantine":
		// Generic per-operation failure maps to unexpected —
		// the dashboard renders the canonical sentence error
		// in stderr, and the operator triages the failure
		// from the audit row's error_message column.
		return ExitUnexpected
	default:
		return ExitUnexpected
	}
}
