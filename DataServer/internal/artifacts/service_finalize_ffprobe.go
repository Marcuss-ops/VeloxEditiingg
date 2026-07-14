// Package artifacts / service_finalize_ffprobe.go
//
// Pre-commit ffprobe invariant (RW-PROD-008 A4). Gated on the env
// var VELOX_FFPROBE_VERIFY_ON_FINALIZE which accepts a tri-state:
//
//   - "shadow"             : run ffprobe + log every outcome; NEVER
//     abort the finalize tx (production-safe
//     observability window for Stage 1 rollout)
//   - "enforce" | "true"   : run ffprobe + log; ABORT on count
//     mismatch (ErrFFProbeAudioCountMismatch)
//     or binary missing
//     (ErrFFProbeInvariantMissingBinary).
//     "true" is the legacy alias kept so
//     envfiles shipped before the tri-state
//     keep the same hard-trip behavior.
//   - "off" | unset | typos : noop. Strict case-sensitive match —
//     "Shadow", "SHADOW", "True", "1",
//     "yes" all NO-OP so a typo disables the
//     gate visibly rather than silently.
//
// Strict semantics match the preflight_ffprobe_invariant bash helper
// in deploy/install-server.sh EXACTLY — see the runbook at
// docs/rw-prod/RW-PROD-008-A4-shadow-rollout.md for the operator
// rollout procedure.
//
// Tripwire semantics: this invariant catches pipeline regressions —
// most notably the amix-collapse bug where a 6-voice payload
// collapses to 1 audio stream in the C++ engine's master-mix step
// while the per-job plan still carries 6 destinations. Without
// this invariant a 1-track artifacts.status=READY row lands
// silently; with it the orchestrator surfaces
// ErrFFProbeAudioCountMismatch to the RPC layer so the regression
// is loud instead of quiet.
//
// Placement: PRE-commit, after PromoteToCanonical (blob on disk,
// no DB write yet) and before the CAS RECEIVED → FINALIZING
// transition. Enforce-mode mismatch aborts cleanly: the upload
// session row stays at RECEIVED, jobs.status stays RUNNING,
// artifacts.status stays STAGING, no job_deliveries are stamped.
// Shadow mode logs the same trip but lets the writer continue so
// the master keeps producing SUCCEEDED outcomes during Stage 1
// observation. The blob DOES exist on disk at this point — this
// is the existing orphan-blob pattern the 24h Reconciler sweep
// already handles ("un blob orfano eliminabile è preferibile
// rispetto a (artifact READY con file inesistente)").
//
// Failure-mode policy:
//
//   - ffprobe missing on PATH        → log event=ffprobe_invariant_missing_binary
//     enforce: return ErrFFProbeInvariantMissingBinary
//     shadow:  return nil (log only)
//   - ffprobe timeout / parse error  → log event=ffprobe_invariant_mismatch
//     (reason=ffprobe_exec, err=...)
//     enforce: return ErrFFProbeAudioCountMismatch
//     shadow:  return nil (log only)
//   - audio count != expected count  → log event=ffprobe_invariant_mismatch
//     (expected_streams=N actual_streams=M)
//     enforce: return ErrFFProbeAudioCountMismatch
//     shadow:  return nil (log only)
//   - audio count == expected count  → log event=ffprobe_invariant_match (shadow only)
//     enforce: silent (preserves pre-shadow log noise floor)
//     both: return nil
//
// Counter wiring: the gate reads through Service.deliveryCounter,
// a purpose-built JobDeliveryCounter typed reader (see
// job_delivery_counter.go). Required at NewService construction
// — NewService panics on nil — so a misconfigured production
// deploy fails fast at boot instead of silently bypassing the
// intent. The runtime guard below is defensive parity for the
// same panic stance so the gate cannot soft-fail if a future
// refactor accidentally bypasses the construction-time panic.
package artifacts

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// ffprobeInvariantTimeout bounds a single ffprobe shell-out. The
// moov-atom read on -show_entries stream completes in milliseconds
// on real files; 5s is the safety ceiling to keep finalize RPC
// latency bounded under unexpected filesystem contention.
const ffprobeInvariantTimeout = 5 * time.Second

// ffprobeInvariantMode is a closed enum mirroring the env var's
// tri-state value space. Internal-only (no exported surface — the
// orchestrator cares about behaviour, not the enum).
type ffprobeInvariantMode int

const (
	ffprobeModeOff ffprobeInvariantMode = iota
	ffprobeModeShadow
	ffprobeModeEnforce
)

// parseFFProbeMode maps the env literal to the closed enum.
// Strict case-sensitive literal match: "shadow", "enforce", "true"
// alone. Everything else (unset, "", "off", typos, "1", "TRUE",
// "yes", "Shadow", "ENFORCE", ...) → ffprobeModeOff. This matches
// the preflight bash helper and the runbook precisely so an
// operator typo cannot silently enable a trip mode.
func parseFFProbeMode(envValue string) ffprobeInvariantMode {
	switch envValue {
	case "shadow":
		return ffprobeModeShadow
	case "enforce", "true": // "true" is the pre-shadow-mode legacy alias for enforce
		return ffprobeModeEnforce
	default:
		return ffprobeModeOff
	}
}

// modeString mirrors the env-friendly literal so the structured
// log events emit a value the runbook's grep queries match without
// case-folding ("event=ffprobe_invariant_mismatch mode=shadow ..."
// is the canonical grep key).
func (m ffprobeInvariantMode) modeString() string {
	switch m {
	case ffprobeModeShadow:
		return "shadow"
	case ffprobeModeEnforce:
		return "enforce"
	default:
		return "off"
	}
}

// runPreCommitFFProbeInvariant runs the post-promote, pre-tx
// invariant when VELOX_FFPROBE_VERIFY_ON_FINALIZE is set to a
// recognized tri-state value (shadow / enforce / true); no-op
// otherwise. Reads the just-promoted blob path, shells out to
// ffprobe, and (a) always emits a structured log line with
// event=ffprobe_invariant_* and (b) returns an error to the
// orchestrator ONLY in enforce mode on a trip.
//
// Wire callers must invoke this AFTER PromoteToCanonical (blob
// on disk) and BEFORE the CAS RECEIVED → FINALIZING transition
// (so a gate failure cleanly aborts without DB writes). The
// shadow mode is production-safe in the rollback sense: even if a
// false-positive fires (count mismatch on legitimate traffic) the
// orchestrator continues and the master still produces SUCCEEDED,
// so the operator can flip to OFF (unset the env var) at any time
// without losing jobs.
func (s *Service) runPreCommitFFProbeInvariant(ctx context.Context, jobID, overrideDestID, absBlob string) error {
	mode := parseFFProbeMode(os.Getenv("VELOX_FFPROBE_VERIFY_ON_FINALIZE"))
	if mode == ffprobeModeOff {
		return nil
	}

	// Orchestrator-bug class: missing fields on the Finalize command
	// itself (these are defenses against wrong callers, NOT data
	// regressions). Both modes log + log a distinct
	// orchestrator_bug sub-event so the runbook's grep can triage
	// them separately from real count-mismatch trips.
	if jobID == "" {
		log.Printf("[TRIPWIRE] event=ffprobe_invariant_orchestrator_bug mode=%s reason=empty_job_id", mode.modeString())
		if mode == ffprobeModeEnforce {
			return fmt.Errorf("artifacts: runPreCommitFFProbeInvariant: empty jobID")
		}
		return nil
	}
	if absBlob == "" {
		log.Printf("[TRIPWIRE] event=ffprobe_invariant_orchestrator_bug mode=%s reason=empty_abs_blob job_id=%s", mode.modeString(), jobID)
		if mode == ffprobeModeEnforce {
			return fmt.Errorf("artifacts: runPreCommitFFProbeInvariant: empty absBlob for job_id=%s", jobID)
		}
		return nil
	}
	if _, statErr := os.Stat(absBlob); statErr != nil {
		log.Printf("[TRIPWIRE] event=ffprobe_invariant_orchestrator_bug mode=%s reason=stat_blob_failed job_id=%s blob=%s err=%q", mode.modeString(), jobID, absBlob, statErr.Error())
		if mode == ffprobeModeEnforce {
			return fmt.Errorf("artifacts: runPreCommitFFProbeInvariant: stat blob %s: %w", absBlob, statErr)
		}
		return nil
	}
	if s.deliveryCounter == nil {
		// Defensive paranoia. NewService panics on a nil
		// deliveryCounter at construction, so this branch is
		// unreachable in practice — kept here as a bare panic
		// rather than a soft-fail no-op, since the whole point of
		// this gate is to fail loudly on misconfigured deployments
		// (matches the panic-at-construction stance).
		panic("artifacts: runPreCommitFFProbeInvariant: nil deliveryCounter")
	}
	expectedCount, err := s.deliveryCounter.CountExpectedDeliveries(ctx, jobID, overrideDestID)
	if err != nil {
		log.Printf("[TRIPWIRE] event=ffprobe_invariant_counter_error mode=%s job_id=%s err=%q", mode.modeString(), jobID, err.Error())
		if mode == ffprobeModeEnforce {
			return fmt.Errorf("artifacts: runPreCommitFFProbeInvariant: count expected deliveries: %w", err)
		}
		return nil
	}
	actualCount, err := probeAudioStreamCount(ctx, absBlob)
	if err != nil {
		// Distinguish the binary-missing class (PATH lookup miss)
		// from the count-or-exec class so the runbook's greps can
		// route them to different on-call paths.
		isMissingBinary := errors.Is(err, ErrFFProbeInvariantMissingBinary)
		if isMissingBinary {
			log.Printf("[TRIPWIRE] event=ffprobe_invariant_missing_binary mode=%s job_id=%s err=%q", mode.modeString(), jobID, err.Error())
		} else {
			log.Printf("[TRIPWIRE] event=ffprobe_invariant_mismatch mode=%s job_id=%s reason=ffprobe_exec err=%q", mode.modeString(), jobID, err.Error())
		}
		if mode == ffprobeModeEnforce {
			return err
		}
		return nil
	}
	if actualCount != expectedCount {
		log.Printf("[TRIPWIRE] event=ffprobe_invariant_mismatch mode=%s job_id=%s expected_streams=%d actual_streams=%d override_dest_id=%q blob=%s",
			mode.modeString(), jobID, expectedCount, actualCount, overrideDestID, absBlob)
		if mode == ffprobeModeEnforce {
			return fmt.Errorf("%w: job_id=%s expected_audio_streams=%d (pre-commit dest count) actual_audio_streams=%d override_dest_id=%q",
				ErrFFProbeAudioCountMismatch, jobID, expectedCount, actualCount, overrideDestID)
		}
		return nil
	}
	// Match path.
	if mode == ffprobeModeShadow {
		// Enforce mode is silent on match — preserves the
		// pre-shadow log noise floor so existing happy-path tests
		// don't need to capture bag-of-bytes log buffers.
		// Shadow mode emits a match event so operators can confirm
		// the gate ran on HEALTHY traffic during Stage 1 (without
		// it, the absence of `_mismatch` events could equally
		// mean "everything fine" or "the gate never ran" — the
		// match counter answers that ambiguity).
		log.Printf("[TRIPWIRE] event=ffprobe_invariant_match mode=shadow job_id=%s expected_streams=%d actual_streams=%d blob=%s",
			jobID, expectedCount, actualCount, absBlob)
	}
	return nil
}

// probeAudioStreamCount runs ffprobe on the resolved absolute path
// and returns the count of audio streams in the container.
//
// Failure-mode mapping (per the package docstring policy):
//   - ffprobe binary missing on PATH → ErrFFProbeInvariantMissingBinary
//   - ffprobe exit != 0 / timeout     → ErrFFProbeAudioCountMismatch
//   - 0 audio streams                 → 0 (valid, not an error)
func probeAudioStreamCount(ctx context.Context, absBlob string) (int, error) {
	if _, err := exec.LookPath("ffprobe"); err != nil {
		return 0, fmt.Errorf("%w: %v", ErrFFProbeInvariantMissingBinary, err)
	}
	timeoutCtx, cancel := context.WithTimeout(ctx, ffprobeInvariantTimeout)
	defer cancel()
	// -select_streams a counts just the audio streams. -show_entries
	// stream=index prints one csv line per stream. csv=p=0 strips the
	// header row so `wc -l` of the output equals the stream count.
	cmd := exec.CommandContext(timeoutCtx, "ffprobe",
		"-v", "error",
		"-select_streams", "a",
		"-show_entries", "stream=index",
		"-of", "csv=p=0",
		filepath.Clean(absBlob),
	)
	var out, errBuf bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errBuf
	if err := cmd.Run(); err != nil {
		return 0, fmt.Errorf("%w: ffprobe rc=%v: %s", ErrFFProbeAudioCountMismatch, err, strings.TrimSpace(errBuf.String()))
	}
	trimmed := strings.TrimSpace(out.String())
	if trimmed == "" {
		// 0 audio streams is a valid ffprobe output (video-only file).
		// Do NOT raise — the gate compares exactly.
		return 0, nil
	}
	return strings.Count(trimmed, "\n") + 1, nil
}
