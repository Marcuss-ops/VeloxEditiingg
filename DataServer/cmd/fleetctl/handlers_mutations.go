// handlers_mutations.go — worker mutation commands and ledger polling.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
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
	return runMutation(client, "drain", workerID, workerPath(workerID, "drain"),
		map[string]any{"reason": reason})
}

// runUpdate — POST /api/v1/admin/workers/{id}/update.
// The parser deliberately accepts both the historical positional form
// (`update WORKER IMAGE REASON`) and the typed-client form
// (`update WORKER --digest IMAGE --reason REASON`).
func runUpdate(client *fleetClient, args []string) int {
	workerID, image, reason, err := parseImageMutationArgs("update", args)
	if err != nil {
		fmt.Fprintln(os.Stderr, fmtExit(ExitMisuse, "%v", err))
		return ExitMisuse
	}
	imageRef, err := normalizeImageArg(image)
	if err != nil {
		fmt.Fprintln(os.Stderr, fmtExit(ExitImageInvalid, "%v", err))
		return ExitImageInvalid
	}
	return runMutation(client, "update", workerID,
		workerPath(workerID, "update"),
		map[string]any{"target_digest": imageRef, "reason": reason})
}

// runSmoke — POST /api/v1/admin/workers/{id}/smoke;
// polls /admin/operations/{op_id}; on terminal FAILED, exit 6.
func runSmoke(client *fleetClient, args []string) int {
	workerID := ""
	assetID := strings.TrimSpace(os.Getenv("VELOX_SMOKE_ASSET_ID"))
	reason := "fleetctl smoke"
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--asset-id":
			if i+1 >= len(args) {
				fmt.Fprintln(os.Stderr, fmtExit(ExitMisuse, "--asset-id requires a value"))
				return ExitMisuse
			}
			assetID = strings.TrimSpace(args[i+1])
			i++
		case "--reason":
			if i+1 >= len(args) {
				fmt.Fprintln(os.Stderr, fmtExit(ExitMisuse, "--reason requires a value"))
				return ExitMisuse
			}
			reason = args[i+1]
			i++
		default:
			if strings.HasPrefix(args[i], "--reason=") {
				reason = strings.TrimPrefix(args[i], "--reason=")
				continue
			}
			if strings.HasPrefix(args[i], "-") {
				continue
			}
			if workerID == "" {
				workerID = args[i]
			}
		}
	}
	if workerID == "" {
		fmt.Fprintln(os.Stderr, fmtExit(ExitMisuse, "smoke requires a worker_id"))
		return ExitMisuse
	}
	if assetID == "" {
		fmt.Fprintln(os.Stderr, fmtExit(ExitMisuse, "smoke requires VELOX_SMOKE_ASSET_ID or --asset-id"))
		return ExitMisuse
	}
	renderPlan := "ffmpeg -i {{ pickup_url }} -c:v libx264 -t 5 /tmp/smoke-{{ worker_id }}.mp4"
	timeoutSec := 600
	return runMutation(client, "smoke", workerID,
		workerPath(workerID, "smoke"),
		map[string]any{"asset_id": assetID, "render_plan": renderPlan, "timeout_sec": timeoutSec, "reason": reason})
}

// runResume — POST /api/v1/admin/workers/{id}/resume;
// polls /admin/operations/{op_id}.
func runQuarantine(client *fleetClient, args []string) int {
	workerID, ok := oneArg(args)
	if !ok {
		fmt.Fprintln(os.Stderr, fmtExit(ExitMisuse, "quarantine requires a worker_id"))
		return ExitMisuse
	}
	return runMutation(client, "quarantine", workerID, workerPath(workerID, "quarantine"), map[string]any{"reason": parseReasonFlag(args)})
}

func runRestart(client *fleetClient, args []string) int {
	workerID, ok := oneArg(args)
	if !ok {
		fmt.Fprintln(os.Stderr, fmtExit(ExitMisuse, "restart requires a worker_id"))
		return ExitMisuse
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var response map[string]any
	status, err := client.doJSON(ctx, "POST", workerPath(workerID, "restart"), map[string]any{"reason": parseReasonFlag(args)}, &response)
	if err != nil {
		fmt.Fprintln(os.Stderr, fmtExit(ExitUnexpected, "%v", err))
		return ExitUnexpected
	}
	if status != 200 {
		ec := MapHTTPStatusToOpExit(status)
		fmt.Fprintln(os.Stderr, fmtExit(ec, "POST restart status=%d", status))
		return ec
	}
	fmt.Printf("fleetctl: restart scheduled worker=%s\n", workerID)
	return ExitOK
}

func runWorkerConfig(client *fleetClient, args []string) int {
	if len(args) < 2 || args[0] != "set" {
		fmt.Fprintln(os.Stderr, fmtExit(ExitMisuse, "worker-config requires: set <worker_id> [--audio-mix-strategy ...] [--audio-mix-profile 0|1]"))
		return ExitMisuse
	}
	workerID := args[1]
	strategy := ""
	profile := (*int)(nil)
	reason := "fleetctl worker-config set"
	for i := 2; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "--audio-mix-strategy" || arg == "--audio-mix-profile" || arg == "--reason":
			if i+1 >= len(args) {
				fmt.Fprintln(os.Stderr, fmtExit(ExitMisuse, "%s requires a value", arg))
				return ExitMisuse
			}
			value := args[i+1]
			switch arg {
			case "--audio-mix-strategy":
				strategy = value
			case "--audio-mix-profile":
				parsed := 0
				if value != "0" && value != "1" {
					fmt.Fprintln(os.Stderr, fmtExit(ExitMisuse, "audio-mix-profile must be 0 or 1"))
					return ExitMisuse
				}
				if value == "1" {
					parsed = 1
				}
				profile = &parsed
			case "--reason":
				reason = value
			}
			i++
		case strings.HasPrefix(arg, "--audio-mix-strategy="):
			strategy = strings.TrimPrefix(arg, "--audio-mix-strategy=")
		case strings.HasPrefix(arg, "--audio-mix-profile="):
			value := strings.TrimPrefix(arg, "--audio-mix-profile=")
			parsed := 0
			if value != "0" && value != "1" {
				fmt.Fprintln(os.Stderr, fmtExit(ExitMisuse, "audio-mix-profile must be 0 or 1"))
				return ExitMisuse
			}
			if value == "1" {
				parsed = 1
			}
			profile = &parsed
		case strings.HasPrefix(arg, "--reason="):
			reason = strings.TrimPrefix(arg, "--reason=")
		default:
			fmt.Fprintln(os.Stderr, fmtExit(ExitMisuse, "unknown worker-config option %q", arg))
			return ExitMisuse
		}
	}
	if strategy != "" && strategy != "legacy" && strategy != "optimized" && strategy != "auto" {
		fmt.Fprintln(os.Stderr, fmtExit(ExitMisuse, "audio-mix-strategy must be legacy, optimized, or auto"))
		return ExitMisuse
	}
	if strategy == "" && profile == nil {
		fmt.Fprintln(os.Stderr, fmtExit(ExitMisuse, "worker-config requires at least one supported setting"))
		return ExitMisuse
	}
	body := map[string]any{"reason": reason}
	if strategy != "" {
		body["audio_mix_strategy"] = strategy
	}
	if profile != nil {
		body["audio_mix_profile"] = *profile
	}
	return runMutation(client, "restart", workerID, workerPath(workerID, "config"), body)
}

func runResume(client *fleetClient, args []string) int {
	workerID, ok := oneArg(args)
	if !ok {
		fmt.Fprintln(os.Stderr, fmtExit(ExitMisuse, "resume requires a worker_id"))
		return ExitMisuse
	}
	reason := parseReasonFlag(args)
	return runMutation(client, "resume", workerID,
		workerPath(workerID, "resume"),
		map[string]any{"reason": reason})
}

// runRollback — POST /api/v1/admin/workers/{id}/update with the
// previous-known-good pinned image. The Master owns the rollback cascade;
// there is intentionally no separate rollback HTTP route.
func runRollback(client *fleetClient, args []string) int {
	workerID, image, reason, err := parseImageMutationArgs("rollback", args)
	if err != nil {
		fmt.Fprintln(os.Stderr, fmtExit(ExitMisuse, "%v", err))
		return ExitMisuse
	}
	imageRef, err := normalizeImageArg(image)
	if err != nil {
		fmt.Fprintln(os.Stderr, fmtExit(ExitImageInvalid, "%v", err))
		return ExitImageInvalid
	}
	return runMutation(client, "rollback", workerID,
		workerPath(workerID, "update"),
		map[string]any{"target_digest": imageRef, "reason": reason})
}

// parseImageMutationArgs is the compatibility parser for update and rollback.
// It consumes only flags owned by the mutation and ignores global client flags
// that loadClientConfig already handled. Keeping this parser independent from
// flag.FlagSet is important: Go's standard parser stops at the first positional
// worker ID and would silently ignore a following positional image/reason.
func parseImageMutationArgs(action string, args []string) (workerID, image, reason string, err error) {
	var positional []string
	var imageSet, reasonSet bool
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "--digest" || arg == "--reason" || arg == "--master" || arg == "--token-file":
			if i+1 >= len(args) {
				return "", "", "", fmt.Errorf("%s requires a value", arg)
			}
			value := args[i+1]
			switch arg {
			case "--digest":
				if imageSet {
					return "", "", "", errors.New("digest specified more than once")
				}
				image, imageSet = value, true
			case "--reason":
				if reasonSet {
					return "", "", "", errors.New("reason specified more than once")
				}
				reason, reasonSet = value, true
			}
			i++
		case strings.HasPrefix(arg, "--digest="):
			if imageSet {
				return "", "", "", errors.New("digest specified more than once")
			}
			image, imageSet = strings.TrimPrefix(arg, "--digest="), true
		case strings.HasPrefix(arg, "--reason="):
			if reasonSet {
				return "", "", "", errors.New("reason specified more than once")
			}
			reason, reasonSet = strings.TrimPrefix(arg, "--reason="), true
		case strings.HasPrefix(arg, "--master=") || strings.HasPrefix(arg, "--token-file=") || arg == "--verbose":
			// Global flags are resolved before dispatch by loadClientConfig.
		case strings.HasPrefix(arg, "-"):
			return "", "", "", fmt.Errorf("unknown %s option %q", action, arg)
		default:
			positional = append(positional, arg)
		}
	}
	if len(positional) == 0 {
		return "", "", "", fmt.Errorf("%s requires a worker_id", action)
	}
	if len(positional) > 3 {
		return "", "", "", fmt.Errorf("%s accepts worker_id, image and optional reason", action)
	}
	workerID = positional[0]
	if !imageSet && len(positional) > 1 {
		image, imageSet = positional[1], true
	}
	if !reasonSet && len(positional) > 2 {
		reason, reasonSet = positional[2], true
	}
	if !imageSet || strings.TrimSpace(image) == "" {
		return "", "", "", fmt.Errorf("%s requires a pinned image via --digest or positional argument", action)
	}
	if !reasonSet {
		reason = "fleetctl " + action
	}
	return workerID, image, reason, nil
}

func normalizeDigestArg(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if strings.Contains(raw, "@sha256:") {
		digest := raw[strings.LastIndex(raw, "@")+1:]
		if err := validateDigest(digest); err != nil {
			return "", err
		}
		return digest, nil
	}
	if err := validateDigest(raw); err != nil {
		return "", err
	}
	return raw, nil
}

// normalizeImageArg validates an operator image argument and preserves a
// complete pinned reference when one was supplied. Only a bare digest is
// expanded to the configured worker repository; silently replacing the
// repository of an explicit image would make rollback target the wrong image.
func normalizeImageArg(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if digestRegex.MatchString(raw) {
		return workerImageRef(raw), nil
	}
	if !fullImageDigest.MatchString(raw) {
		return "", fmt.Errorf("image %q must be sha256:<64-hex> or a pinned image@sha256:<64-hex> reference", raw)
	}
	if _, err := normalizeDigestArg(raw); err != nil {
		return "", err
	}
	return raw, nil
}

// runMutation is the shared post+polling helper. Issues the
// POST then polls until terminal SUCCEEDED (return ExitOK) or
// terminal FAILED/ROLLBACK (return MapOperationKindToExit).
func runMutation(client *fleetClient, opKind, workerID, path string, body map[string]any) int {
	budget := defaultWaitBudget[opKind]
	if budget <= 0 {
		budget = 5 * time.Minute
	}
	ctx, cancel := context.WithTimeout(context.Background(), budget)
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
