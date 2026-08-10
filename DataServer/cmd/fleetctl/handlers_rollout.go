// handlers_rollout.go — fleetctl rollout: serial multi-worker update.
//
// rollout is the last fleet mutation to move from the Bash compatibility
// bridge into the typed Go client. It performs the same serial cascade the
// legacy `rollout()` Bash function implemented, but through the single
// canonical HTTP + ledger-polling path owned by this client:
//
//	for each selected worker (serially):
//	    POST /api/v1/admin/workers/{id}/update {target_digest, reason}
//	    poll /api/v1/admin/operations/{op_id} to terminal state
//	    if --wait-ready: poll the worker WorkerCard until READY on the digest
//
// Worker selection mirrors the legacy contract: `--workers all` (default)
// resolves the canonical worker inventory via GET /api/v1/admin/workers;
// a comma-separated list is applied verbatim. Rollout is serial-only by
// design (the FleetController tick serialises operations per worker anyway);
// `--parallel` is rejected with the historical error message.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

// rolloutOptions is the parsed `rollout` surface. Serial is implicit —
// rollout is serial-only, so no option field exists for it.
type rolloutOptions struct {
	image     string // pinned GHCR ref (or bare sha256 digest, expanded later)
	selection string // "all" or comma-separated worker_id list
	reason    string
	waitReady bool
}

// runRollout — `fleetctl rollout [--digest IMAGE] [--workers all|id1,id2] [--reason R] [--serial] [--wait-ready]`.
// Serial update of the selected workers; stops at the first worker whose
// update does not reach terminal SUCCEEDED (mirrors the legacy Bash loop).
func runRollout(client *fleetClient, args []string) int {
	opts, err := parseRolloutArgs(args)
	if err != nil {
		fmt.Fprintln(os.Stderr, fmtExit(ExitMisuse, "%v", err))
		return ExitMisuse
	}
	imageRef, err := normalizeImageArg(opts.image)
	if err != nil {
		fmt.Fprintln(os.Stderr, fmtExit(ExitImageInvalid, "%v", err))
		return ExitImageInvalid
	}
	workers, err := resolveRolloutWorkers(client, opts.selection)
	if err != nil {
		fmt.Fprintln(os.Stderr, fmtExit(ExitUnexpected, "%v", err))
		return ExitUnexpected
	}
	if len(workers) == 0 {
		fmt.Fprintln(os.Stderr, fmtExit(ExitMisuse, "rollout: no workers selected"))
		return ExitMisuse
	}
	fmt.Fprintf(os.Stderr, "fleetctl: rolling out %s to %d worker(s) serially\n", imageRef, len(workers))
	for _, workerID := range workers {
		fmt.Fprintf(os.Stderr, "fleetctl: rollout worker=%s image=%s reason=%s\n", workerID, imageRef, opts.reason)
		if ec := runMutation(client, "update", workerID,
			workerPath(workerID, "update"),
			map[string]any{"target_digest": imageRef, "reason": opts.reason}); ec != ExitOK {
			fmt.Fprintf(os.Stderr, "fleetctl: rollout stopped at worker=%s (exit=%d)\n", workerID, ec)
			return ec
		}
		if opts.waitReady {
			timeout := envSeconds("FLEETCTL_READY_TIMEOUT_SECONDS", 180)
			interval := envSeconds("FLEETCTL_READY_POLL_SECONDS", 5)
			if ec := runWaitReadyWithInterval(client, workerID, imageRef, timeout, interval); ec != ExitOK {
				fmt.Fprintf(os.Stderr, "fleetctl: rollout worker=%s not READY (exit=%d)\n", workerID, ec)
				return ec
			}
		}
	}
	fmt.Fprintf(os.Stderr, "fleetctl: rollout complete — %d worker(s) updated to %s\n", len(workers), imageRef)
	return ExitOK
}

// parseRolloutArgs mirrors the legacy Bash argument surface:
//
//	--digest IMAGE | --digest=IMAGE | positional IMAGE
//	--workers all|id1,id2,...   (default all)
//	--reason R | --reason=R     (default fleetctl-rollout)
//	--serial                    accepted (rollout is always serial)
//	--wait-ready                wait for each worker to become READY
//	--parallel                  rejected (rollout is serial-only)
func parseRolloutArgs(args []string) (rolloutOptions, error) {
	opts := rolloutOptions{selection: "all", reason: "fleetctl-rollout"}
	var positional []string
	var imageSet, reasonSet, selectionSet bool
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "--digest" || arg == "--workers" || arg == "--reason":
			if i+1 >= len(args) {
				return opts, fmt.Errorf("%s requires a value", arg)
			}
			value := args[i+1]
			switch arg {
			case "--digest":
				if imageSet {
					return opts, errors.New("digest specified more than once")
				}
				opts.image, imageSet = value, true
			case "--workers":
				if selectionSet {
					return opts, errors.New("workers specified more than once")
				}
				opts.selection, selectionSet = value, true
			case "--reason":
				if reasonSet {
					return opts, errors.New("reason specified more than once")
				}
				opts.reason, reasonSet = value, true
			}
			i++
		case strings.HasPrefix(arg, "--digest="):
			if imageSet {
				return opts, errors.New("digest specified more than once")
			}
			opts.image, imageSet = strings.TrimPrefix(arg, "--digest="), true
		case strings.HasPrefix(arg, "--workers="):
			if selectionSet {
				return opts, errors.New("workers specified more than once")
			}
			opts.selection, selectionSet = strings.TrimPrefix(arg, "--workers="), true
		case strings.HasPrefix(arg, "--reason="):
			if reasonSet {
				return opts, errors.New("reason specified more than once")
			}
			opts.reason, reasonSet = strings.TrimPrefix(arg, "--reason="), true
		case arg == "--serial":
			// Serial is the only supported mode; accepted for historical
			// script compatibility.
		case arg == "--wait-ready":
			opts.waitReady = true
		case arg == "--parallel":
			return opts, errors.New("rollout is serial-only; omit --parallel and use --serial")
		case strings.HasPrefix(arg, "--master=") || strings.HasPrefix(arg, "--token-file=") || arg == "--verbose":
			// Global flags resolved by loadClientConfig before dispatch.
		case arg == "--master" || arg == "--token-file":
			if i+1 >= len(args) {
				return opts, fmt.Errorf("%s requires a value", arg)
			}
			i++
		case strings.HasPrefix(arg, "-"):
			return opts, fmt.Errorf("unknown rollout option %q", arg)
		default:
			positional = append(positional, arg)
		}
	}
	if len(positional) > 1 {
		return opts, errors.New("rollout accepts at most one positional image reference")
	}
	if len(positional) == 1 {
		if imageSet {
			return opts, errors.New("digest specified both positionally and by flag")
		}
		opts.image, imageSet = positional[0], true
	}
	if !imageSet || strings.TrimSpace(opts.image) == "" {
		return opts, errors.New("rollout requires a pinned image via --digest or positional argument")
	}
	if opts.selection == "" {
		return opts, errors.New("workers selection cannot be empty")
	}
	return opts, nil
}

// resolveRolloutWorkers resolves the rollout target set. "all" reads the
// canonical worker inventory (GET /api/v1/admin/workers → workers[].worker_id);
// any other value is a comma-separated worker_id list applied verbatim.
func resolveRolloutWorkers(client *fleetClient, selection string) ([]string, error) {
	if selection == "all" {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		resp := workerListResponse{}
		status, err := client.doJSON(ctx, "GET", "/api/v1/admin/workers", nil, &resp)
		if err != nil {
			return nil, fmt.Errorf("rollout: resolve workers: %w", err)
		}
		if status != 200 {
			return nil, fmt.Errorf("rollout: GET /admin/workers status=%d", status)
		}
		workers := make([]string, 0, len(resp.Workers))
		for _, w := range resp.Workers {
			wid, _ := w["worker_id"].(string)
			if strings.TrimSpace(wid) != "" {
				workers = append(workers, wid)
			}
		}
		return workers, nil
	}
	var workers []string
	for _, wid := range strings.Split(selection, ",") {
		wid = strings.TrimSpace(wid)
		if wid == "" {
			return nil, errors.New("workers selection contains an empty worker_id")
		}
		workers = append(workers, wid)
	}
	return workers, nil
}
