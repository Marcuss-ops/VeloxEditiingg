// pkg/bootstrap/renderer.go — engine self-render (RW-PROD-003 §3 A1 + A2).
//
// The bootstrap builds a synthetic one-frame, minimal even-dimension black
// RenderPlan, asks the canonical Runner to render it through the same
// RenderClient that production tasks use, computes the SHA-256 of the
// output file, and compares it against a baseline committed at
// tests/fixtures/engine_selftest_baseline.sha256.
//
// Two failure modes are explicitly surfaced:
//
//   - The C++ engine refused the request or did not produce the file
//     → code "engine_missing".
//   - The engine produced SOMETHING, but the bytes do not match the
//     baseline → code "engine_selftest_baseline_mismatch" (which is
//     a stronger signal: the engine boots but is no longer rendering
//     the canonical contract; either the C++ binary or the render
//     pipeline drifted).
//
// The render is bounded by a hard deadline (`opts.SelfRenderTimeout`,
// default 5s) so a hung subprocess cannot block boot indefinitely.
package bootstrap

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"velox-worker-agent/pkg/video/plan"
)

// DefaultSelfRenderTimeout is the hard deadline for the engine self-
// render (RW-PROD-003 §3 A1, "≤5s" budget). Exceeding it is itself
// considered a fail-closed condition (the engine hung, the binary is
// suspect).
const DefaultSelfRenderTimeout = 5 * time.Second

// blackFrameRenderJobID is the synthetic job-id used by the self-render
// plan. Stable but obviously synthetic so it is grep-able in the C++
// engine's stderr.
const blackFrameRenderJobID = "bootstrap.engine_selftest"

// buildSelfRenderPlan constructs the minimal RenderPlan that asks the
// engine for a single still frame. We intentionally use a 2x2 canvas
// instead of 1x1 because the current x264-backed engine requires even
// dimensions and rejects odd widths/heights during bootstrap.
//
// The plan is committed at build time so the C++ engine's output is
// byte-stable across host installations — required for SHA-256 matching.
//
// TODO(rw-prod-003-cpp-engine-contract): the choice of MediaSource.Type
// = "color" with ColorHex = "#000000" is a CONVENTION that has not yet
// been verified against the actual velox-render-cpp --render --plan
// parser. If the C++ engine rejects the type, the production smoke
// will fail with code="engine_missing". Resolution paths in priority
// order:
//
//	(a) confirm via /usr/local/bin/velox_video_engine --help render that
//	    "color" is a recognized source.kind;
//	(b) if not, fall back to a Go-side PNG producer (1×1 black PNG
//	    written into t.TempDir/cache_key.png + MediaSource.Type="image"
//	    + MediaSource.CacheKey pointing at the file);
//	(c) only after (a)/(b) can `make bootstrap-selftest-regenerate`
//	    produce a real baseline sha256 to commit in
//	    tests/fixtures/engine_selftest_baseline.sha256.
func buildSelfRenderPlan(outputPath string) *plan.RenderPlan {
	return &plan.RenderPlan{
		Version: 1,
		JobID:   blackFrameRenderJobID,
		Canvas: plan.CanvasSpec{
			Width:  2,
			Height: 2,
			Fps:    1,
		},
		Timeline: []plan.TimelineItem{
			{
				Source: plan.MediaSource{
					Type:     "color",
					ColorHex: "#000000",
				},
				DurationSeconds: 0.1,
			},
		},
		OutputPath: outputPath,
	}
}

// runEngineSelfRender (RW-PROD-003 A1+A2) executes the synthetic 1×1
// black-frame render through `runner.RenderClient()`, computes the
// SHA-256 of the produced file, and compares it to the expected
// baseline. Each observable failure mode returns a code stable enough
// for dashboarding.
func runEngineSelfRender(
	ctx context.Context,
	opts Options,
	rc interface {
		Render(context.Context, *plan.RenderPlan) error
	},
) StepResult {
	start := time.Now().UTC()
	res := StepResult{
		Name:      "engine_self_render",
		StartedAt: start,
	}

	if rc == nil {
		res.Status = "FAIL"
		res.Code = "engine_missing"
		res.Detail = "RenderClient is nil — pkg/video pipeline was not constructed upstream"
		res.CompletedAt = time.Now().UTC()
		res.DurMs = res.CompletedAt.Sub(start).Milliseconds()
		return res
	}

	if opts.BaselineSHA256Path == "" {
		res.Status = "FAIL"
		res.Code = "engine_selftest_baseline_missing"
		res.Detail = "BaselineSHA256Path is empty — opts must point at tests/fixtures/engine_selftest_baseline.sha256"
		res.CompletedAt = time.Now().UTC()
		res.DurMs = res.CompletedAt.Sub(start).Milliseconds()
		return res
	}

	// Hard deadline — a hung C++ engine must NOT block boot.
	deadline, hasDeadline := ctx.Deadline()
	timeout := DefaultSelfRenderTimeout
	if hasDeadline {
		// If the parent ctx has a shorter deadline (compose-time 10s
		// gate, see main.go) honour it; otherwise stick to the 5s
		// self-render budget.
		remaining := time.Until(deadline)
		if remaining > 0 && remaining < timeout {
			timeout = remaining
		}
	}
	rCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Render into a fresh temp dir so the output does not collide with
	// concurrent workers. Cleaned up after the SHA compare.
	tmpDir, err := os.MkdirTemp("", "velox_bootstrap_selftest_")
	if err != nil {
		res.Status = "FAIL"
		res.Code = "engine_missing"
		res.Detail = fmt.Sprintf("could not create temp dir for self-render: %v", err)
		res.CompletedAt = time.Now().UTC()
		res.DurMs = res.CompletedAt.Sub(start).Milliseconds()
		return res
	}
	defer os.RemoveAll(tmpDir)

	outputPath := tmpDir + "/frame.mp4"
	if err := rc.Render(rCtx, buildSelfRenderPlan(outputPath)); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			res.Status = "FAIL"
			res.Code = "engine_selftest_timeout"
			res.Detail = fmt.Sprintf("self-render exceeded %v deadline (RW-PROD-003 A1 ≤5s)", timeout)
		} else {
			res.Status = "FAIL"
			res.Code = "engine_missing"
			res.Detail = fmt.Sprintf("RenderClient.Render refused the self-render plan: %v", err)
		}
		res.CompletedAt = time.Now().UTC()
		res.DurMs = res.CompletedAt.Sub(start).Milliseconds()
		return res
	}

	if info, statErr := os.Stat(outputPath); statErr != nil {
		res.Status = "FAIL"
		res.Code = "engine_missing"
		res.Detail = fmt.Sprintf("C++ engine did not create %s: %v", outputPath, statErr)
		res.CompletedAt = time.Now().UTC()
		res.DurMs = res.CompletedAt.Sub(start).Milliseconds()
		return res
	} else if info.Size() == 0 {
		res.Status = "FAIL"
		res.Code = "engine_missing"
		res.Detail = fmt.Sprintf("C++ engine produced empty output at %s", outputPath)
		res.CompletedAt = time.Now().UTC()
		res.DurMs = res.CompletedAt.Sub(start).Milliseconds()
		return res
	}

	actualSHA, hashErr := sha256File(outputPath)
	if hashErr != nil {
		res.Status = "FAIL"
		res.Code = "engine_selftest_hash_unreadable"
		res.Detail = fmt.Sprintf("could not SHA-256 %s: %v", outputPath, hashErr)
		res.CompletedAt = time.Now().UTC()
		res.DurMs = res.CompletedAt.Sub(start).Milliseconds()
		return res
	}

	expectedSHA, baselineErr := readBaselineSHA256(opts.BaselineSHA256Path)
	if baselineErr != nil {
		res.Status = "FAIL"
		res.Code = "engine_selftest_baseline_missing"
		res.Detail = fmt.Sprintf("could not read baseline fixture %s: %v", opts.BaselineSHA256Path, baselineErr)
		res.CompletedAt = time.Now().UTC()
		res.DurMs = res.CompletedAt.Sub(start).Milliseconds()
		return res
	}

	if actualSHA != expectedSHA {
		res.Status = "FAIL"
		res.Code = "engine_selftest_baseline_mismatch"
		res.Detail = fmt.Sprintf(
			"engine self-render SHA mismatch: actual=%s expected=%s baseline=%s (RW-PROD-003 A2)",
			actualSHA, expectedSHA, opts.BaselineSHA256Path,
		)
		res.CompletedAt = time.Now().UTC()
		res.DurMs = res.CompletedAt.Sub(start).Milliseconds()
		return res
	}

	res.Status = "OK"
	res.Code = "engine_selftest_ok"
	res.Detail = fmt.Sprintf("SHA=%s baseline=%s dur=%v", actualSHA, expectedSHA, time.Until(start).Round(time.Millisecond))
	res.CompletedAt = time.Now().UTC()
	res.DurMs = res.CompletedAt.Sub(start).Milliseconds()
	return res
}

// sha256File streams a file through SHA-256 and returns its hex
// encoding. Read in 64 KiB chunks to keep memory bounded for the (rare)
// case where the C++ engine produces a multi-megabyte file.
func sha256File(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// readBaselineSHA256 reads the baseline file, accepts both raw hex and
// "<sha>  <optional filename>" forms (sha256sum output format), and
// returns the canonical lowercase hex.
func readBaselineSHA256(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	raw := string(data)
	// sha256sum prints "<hex>  <file>" — split on whitespace and take
	// the first token. Tolerate LF / CRLF.
	for _, candidate := range splitOnWhitespace(raw) {
		if isHex64(candidate) {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("baseline file %s did not contain a 64-char hex SHA", path)
}
