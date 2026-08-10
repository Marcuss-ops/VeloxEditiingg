// handlers_helpers.go — shared handler parsing and digest helpers.
package main

import (
	"net/url"
	"regexp"
	"strings"
)

var fullImageDigest = regexp.MustCompile(`@sha256:[0-9a-f]{64}$`)

func digestFromRef(ref string) string {
	ref = strings.TrimSpace(ref)
	if digestRegex.MatchString(ref) {
		return ref
	}
	if fullImageDigest.MatchString(ref) {
		return "sha256:" + strings.TrimPrefix(ref[strings.LastIndex(ref, "@")+1:], "sha256:")
	}
	return ""
}
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

func workerPath(workerID, action string) string {
	return "/api/v1/admin/workers/" + url.PathEscape(workerID) + "/" + action
}

func workerReadPath(workerID string) string {
	return "/api/v1/admin/workers/" + url.PathEscape(workerID)
}

func operationPath(operationID string) string {
	return "/api/v1/admin/operations/" + url.PathEscape(operationID)
}

// runSSHCheck — GET /api/v1/admin/workers/ssh-check. Prints one row
// per worker in the canonical WorkerNodeRegistry with the ssh / hostkey
// / sudo -n verdicts plus a fleet summary. Synchronous; no polling.
