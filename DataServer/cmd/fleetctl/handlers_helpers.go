// handlers_helpers.go — shared handler parsing and digest helpers.
package main

import (
	"net/url"
	"regexp"
	"strings"
)

var (
	fullImageDigest = regexp.MustCompile(`@sha256:[0-9a-f]{64}$`)
	rawDigest       = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

func digestFromRef(ref string) string {
	ref = strings.TrimSpace(ref)
	if digestRegex.MatchString(ref) {
		return ref
	}
	if rawDigest.MatchString(ref) {
		return "sha256:" + ref
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

// parseReasonFlag preserves the historical positional form
// (`command WORKER REASON`) while accepting the typed form
// (`command WORKER --reason REASON` / `--reason=REASON`).
func parseReasonFlag(args []string) string {
	for i, arg := range args {
		if arg == "--reason" && i+1 < len(args) {
			return args[i+1]
		}
		if strings.HasPrefix(arg, "--reason=") {
			return strings.TrimPrefix(arg, "--reason=")
		}
	}
	positional := make([]string, 0, 2)
	for _, arg := range args {
		if strings.HasPrefix(arg, "-") {
			continue
		}
		positional = append(positional, arg)
	}
	if len(positional) > 1 {
		return strings.Join(positional[1:], " ")
	}
	return "fleetctl " + safeFirstArg(args)
}

func safeFirstArg(args []string) string {
	for _, arg := range args {
		if !strings.HasPrefix(arg, "-") {
			return arg
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
