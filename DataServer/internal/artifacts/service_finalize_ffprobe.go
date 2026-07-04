// Package artifacts / service_finalize_ffprobe.go
//
// Pre-commit ffprobe invariant (RW-PROD-008 A4). Gated on the env
// var VELOX_FFPROBE_VERIFY_ON_FINALIZE=true.
//
// The literal "true" is required — "1", "TRUE", "yes" are NOT
// honored, so a future operator typo disables the gate visibly
// rather than silently. Mirrors the go.work selector strictness
// pattern used elsewhere in the master.
//
// Asserts the artifact's audio stream count matches the count of
// delivery destinations the finalize tx WILL stamp at Step 5
// (per SQLiteFinalizeWriter::resolveDeliveryDestinationsTx: explicit
// cmd.DestinationID override → 1, else job_delivery_plans WHERE
// enabled=1, else fallback to all-enabled delivery_destinations).
// The tripwire catches pipeline regressions — most notably the
// amix-collapse bug where a 6-voice payload collapses to 1 audio
// stream in the C++ engine's master-mix step but the per-job plan
// carries 6 destinations. Without this invariant a 1-track
// artifacts.status=READY row lands silently; with it the
// orchestrator surfaces ErrFFProbeAudioCountMismatch to the worker
// RPC layer so the regression is loud instead of quiet.
//
// Placement: PRE-commit, after PromoteToCanonical (blob on disk,
// no DB write yet) and before the CAS RECEIVED → FINALIZING
// transition. Mismatch aborts cleanly: the upload session row
// stays at RECEIVED, jobs.status stays RUNNING, artifacts.status
// stays STAGING, no job_deliveries are stamped, no orphan
// artifact_uploads row is committed. The blob DOES exist on disk
// at this point — this is the existing orphan-blob pattern the 24h
// Reconciler sweep already handles ("un blob orfano eliminabile è
// preferibile rispetto a (artifact READY con file inesistente)").
//
// Failure-mode policy:
//   - ffprobe missing on PATH       → ErrFFProbeInvariantMissingBinary
//   - ffprobe timeout / parse error → ErrFFProbeAudioCountMismatch
//     (binary missing is distinct from count mismatch so a
//      deploy-gate miss is triaged separately from a count
//      regression by sentinel)
//   - audio count != expected count → ErrFFProbeAudioCountMismatch
//
// Counter wiring: the gate reads through Service.deliveryCounter,
// a purpose-built JobDeliveryCounter typed reader (see
// job_delivery_counter.go). Required at NewService construction
// — NewService panics on nil — so a misconfigured production
// deploy fails fast at boot instead of silently bypassing the
// intent. The runtime guard below is defensive parity for the
// same panic stance so the gate cannot soft-fail if a future
// refactor accidentally bypasses the construction-time panic
// (e.g. reflect-based bypass, swapped constructor, etc.).
package artifacts

import (
	"bytes"
	"context"
	"fmt"
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

// runPreCommitFFProbeInvariant runs the post-promote, pre-tx
// invariant when VELOX_FFPROBE_VERIFY_ON_FINALIZE=true is set;
// no-op otherwise. Reads the just-promoted blob path, shells out
// to ffprobe, and compares the audio stream count against the
// count the finalize tx WOULD stamp at Step 5.
//
// Wire callers must invoke this AFTER PromoteToCanonical (blob
// on disk) and BEFORE the CAS RECEIVED → FINALIZING transition
// (so a gate failure cleanly aborts without DB writes). Returning
// an error here surfaces the regression to the RPC caller; no
// rollback is needed because no tx started yet.
func (s *Service) runPreCommitFFProbeInvariant(ctx context.Context, jobID, overrideDestID, absBlob string) error {
	if os.Getenv("VELOX_FFPROBE_VERIFY_ON_FINALIZE") != "true" {
		return nil
	}
	if jobID == "" {
		// A empty jobID already indicates a deeper orchestrator bug
		// (FinalizeArtifactCommand JobID is required). Surface
		// distinctly so operators don't chase a misleading count.
		return fmt.Errorf("artifacts: runPreCommitFFProbeInvariant: empty jobID")
	}
	if absBlob == "" {
		// Defensive: PromoteToCanonical should always return a
		// non-empty storage key. Empty here means the orchestrator
		// lost the storage key between promote and gate — surface
		// before stat() reads "." or "".
		return fmt.Errorf("artifacts: runPreCommitFFProbeInvariant: empty absBlob for job_id=%s", jobID)
	}
	if _, statErr := os.Stat(absBlob); statErr != nil {
		// Promote just wrote the file; stat failing means a race
		// with the reconciler sweep — extremely rare; surface
		// distinctly from the count-mismatch case.
		return fmt.Errorf("artifacts: runPreCommitFFProbeInvariant: stat blob %s: %w", absBlob, statErr)
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
		return fmt.Errorf("artifacts: runPreCommitFFProbeInvariant: count expected deliveries: %w", err)
	}

	actualCount, err := probeAudioStreamCount(ctx, absBlob)
	if err != nil {
		return err
	}
	if actualCount != expectedCount {
		return fmt.Errorf("%w: job_id=%s expected_audio_streams=%d (pre-commit dest count) actual_audio_streams=%d override_dest_id=%q",
			ErrFFProbeAudioCountMismatch, jobID, expectedCount, actualCount, overrideDestID)
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
