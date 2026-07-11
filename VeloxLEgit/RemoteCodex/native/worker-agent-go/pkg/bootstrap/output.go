// pkg/bootstrap/output.go — OutputDir smoke test (RW-PROD-003 §3 A4).
//
// The first concrete job-render failure mode we see in the field is
// "the disk is full / read-only / unwritable", detected at the first
// write of the first .mp4. This step pushes that discovery back to
// boot so the worker exits 1 BEFORE sending Hello — the master thus
// never sees an unusable worker as `registered=true`.
//
// The step performs three smoke tests:
//
//	MkdirAll    (catch read-only parent + permission denied)
//	WriteFile   (catch ENOSPC, EACCES on a present-but-protected dir)
//	Remove      (catch immovable files / dir mounted r/o at upper layer)
//
// Each failure mode is mapped to a stable code:
//
//   - output_dir.readonly  : the dir is on a read-only mount
//   - output_dir.unwritable: write succeeded against a stale handle but a fresh
//     fd from a different syscall path cannot — rarer than the above, but observed
//     when upper-layer bind-mounts intercept the writes
//   - output_dir.remove_failed: file is sticky / chattr +i has been applied
package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// runOutputDirSmokeTest (RW-PROD-003 A4) creates OutputDir, writes a
// tiny sentinel file into it, removes the file, and reports the
// canonical StepResult. ctx is honoured so a hung FS call does not
// block boot indefinitely.
func runOutputDirSmokeTest(ctx context.Context, dir string) StepResult {
	start := time.Now().UTC()
	res := StepResult{
		Name:      "output_dir",
		StartedAt: start,
	}

	if dir == "" {
		res.Status = "FAIL"
		res.Code = "output_dir.unwritable"
		res.Detail = "OutputDir is empty — opts must populate DefaultOutputDir"
		res.CompletedAt = time.Now().UTC()
		res.DurMs = res.CompletedAt.Sub(start).Milliseconds()
		return res
	}

	// MkdirAll covers "dir does not exist yet" AND the parent permissions.
	if err := os.MkdirAll(dir, 0o755); err != nil {
		res.Status = "FAIL"
		res.Code = classifyMkdirErr(err)
		res.Detail = fmt.Sprintf("MkdirAll(%q): %v", dir, err)
		res.CompletedAt = time.Now().UTC()
		res.DurMs = res.CompletedAt.Sub(start).Milliseconds()
		return res
	}

	// Probe writability BEFORE writing the sentinel: a chmod-protected
	// dir reaches MkdirAll (root-owned path) but rejects open(WRONLY).
	if err := ctx.Err(); err != nil {
		res.Status = "FAIL"
		res.Code = "output_dir.unwritable"
		res.Detail = fmt.Sprintf("ctx cancelled before write probe: %v", err)
		res.CompletedAt = time.Now().UTC()
		res.DurMs = res.CompletedAt.Sub(start).Milliseconds()
		return res
	}

	sentinel := filepath.Join(dir, ".bootstrap_write_test")
	if err := os.WriteFile(sentinel, []byte("velox-bootstrap-smoke\n"), 0o644); err != nil {
		res.Status = "FAIL"
		res.Code = classifyWriteErr(err)
		res.Detail = fmt.Sprintf("WriteFile(%q): %v", sentinel, err)
		res.CompletedAt = time.Now().UTC()
		res.DurMs = res.CompletedAt.Sub(start).Milliseconds()
		return res
	}

	// Defense-in-depth: re-open with a brand-new syscall path. Some
	// bind-mount layers (e.g. overlayfs with quirky upper) report
	// success on the first open but fail on subsequent unrelated
	// fds if the file was created via O_CREAT against a stale
	// snapshot handle.
	reopen, err := os.OpenFile(sentinel, os.O_RDWR, 0)
	if err != nil {
		// Surface the file we created before retreat.
		_ = os.Remove(sentinel)
		res.Status = "FAIL"
		res.Code = "output_dir.unwritable"
		res.Detail = fmt.Sprintf("re-OpenFile(%q) failed after initial write: %v", sentinel, err)
		res.CompletedAt = time.Now().UTC()
		res.DurMs = res.CompletedAt.Sub(start).Milliseconds()
		return res
	}
	_ = reopen.Close()

	if err := os.Remove(sentinel); err != nil {
		res.Status = "FAIL"
		res.Code = "output_dir.remove_failed"
		res.Detail = fmt.Sprintf("Remove(%q): %v — dir may be mounted with chattr +i", sentinel, err)
		res.CompletedAt = time.Now().UTC()
		res.DurMs = res.CompletedAt.Sub(start).Milliseconds()
		return res
	}

	res.Status = "OK"
	res.Code = "output_dir_ok"
	res.Detail = fmt.Sprintf("mkdir+write+remove=ok at %s", dir)
	res.CompletedAt = time.Now().UTC()
	res.DurMs = res.CompletedAt.Sub(start).Milliseconds()
	return res
}

// classifyMkdirErr maps MkdirAll failures to stable codes. The intent:
// ErrPermission → "readonly" (a parent we lack traverse on) and
// any other failure → "unwritable".
func classifyMkdirErr(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, os.ErrPermission) {
		return "output_dir.readonly"
	}
	return "output_dir.unwritable"
}

// classifyWriteErr mirrors classifyMkdirErr for the WriteFile probe.
// ENOSPC falls under "unwritable" with the underlying cause in Detail.
func classifyWriteErr(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, os.ErrPermission) || errors.Is(err, os.ErrExist) {
		return "output_dir.readonly"
	}
	return "output_dir.unwritable"
}
