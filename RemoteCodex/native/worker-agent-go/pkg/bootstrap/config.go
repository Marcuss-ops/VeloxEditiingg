// Package bootstrap — Options struct for Run().
//
// The composition root (cmd/velox-worker-agent/main.go) constructs the
// Options from cfg + env; pkg/bootstrap never reads environment
// variables directly. This keeps the package deterministic for unit
// tests and clean for future opts-driven invocations (e.g. --doctor).
package bootstrap

import (
	"path/filepath"
	"strings"

	"velox-worker-agent/pkg/config"
	"velox-worker-agent/pkg/logger"
)

// DefaultFFmpegMinMajor is the lowest major version of ffmpeg/ffprobe
// that the bootstrap accepts (RW-PROD-003 §3 A3). ffmpeg 4 was released
// April 2018; anything older is unsupported on the production target.
const DefaultFFmpegMinMajor = 4

// DefaultOutputDir is the canonical scratch directory the worker uses
// for scene-composite outputs. Mirrors main.go's hard-coded path so
// the smoke-test checks the same location as the real work.
const DefaultOutputDir = "/tmp/velox/scene-composite"

// DefaultBaselineFixtureRel is the canonical path — relative to
// opts.WorkDir — of the engine self-render SHA-256 baseline file. The
// workstation install bakes this at /opt/velox/tests/fixtures/...,
// and the source-tree install puts it next to the worker-agent-go module.
//
// The Options struct lets the caller override either location.
const DefaultBaselineFixtureRel = "tests/fixtures/engine_selftest_baseline.sha256"

// FFmpegBinaries is the list of binaries the bootstrap probes.
// ffmpeg and ffprobe MUST come from the same vendor build (mixed
// vendors in the same PATH have historically produced subtle filter-arg
// incompatibilities on libx264 — RW-PROD-003 §1 pain-point #2).
var FFmpegBinaries = []string{"ffprobe", "ffmpeg"}

// Options bundles every knob Run() needs. Zero-value relies on the
// defaults applied by applyOptions.
type Options struct {
	// WorkDir is the worker's installation root. Defaults cfg.WorkDir
	// when zero.
	WorkDir string
	// OutputDir is the directory to smoke-test writability (A4).
	// Defaults to DefaultOutputDir when zero. Operators override via
	// VELOX_OUTPUT_DIR env var in the composition root.
	OutputDir string
	// TempDir is the scratch directory for intermediate artifacts (A4).
	// Defaults to cfg.TempDir when zero.
	TempDir string
	// StateDir is the canonical root for mutable worker state (cache,
	// blobs, spool). Defaults to cfg.StateDir when zero.
	StateDir string
	// FFmpegMinMajor is the lowest acceptable major version for ffmpeg
	// and ffprobe (A3). Defaults to DefaultFFmpegMinMajor when zero.
	FFmpegMinMajor int
	// BaselineSHA256Path is the file containing the expected SHA-256
	// of a 1-second black-frame self-render (A1 + A2). Defaults to
	// <WorkDir>/<DefaultBaselineFixtureRel>.
	BaselineSHA256Path string
	// Logger is used for structured LOG output (each Step gets a row
	// with name/dur_ms/verdict). nil falls back to the package-global
	// default logger, which keeps tests dependency-free.
	Logger *logger.Logger
}

func applyOptions(opts Options, cfg *config.WorkerConfig) Options {
	if opts.WorkDir == "" {
		if cfg != nil {
			opts.WorkDir = strings.TrimSpace(cfg.WorkDir)
		}
		if opts.WorkDir == "" {
			opts.WorkDir = "/opt/velox"
		}
	}
	// opts.OutputDir is the SOLE source of truth for the write-test
	// smoke target. WorkerConfig does not yet ship an OutputDir field
	// (RW-PROD-002 A4 will add it; until then the default
	// /tmp/velox/scene-composite is the canonical scratch path the
	// engine writes to). Operators override via VELOX_OUTPUT_DIR
	// env var in the composition root.
	if opts.OutputDir == "" {
		opts.OutputDir = DefaultOutputDir
	}
	// TempDir/StateDir fall back to the validated WorkerConfig values.
	if opts.TempDir == "" && cfg != nil {
		opts.TempDir = strings.TrimSpace(cfg.TempDir)
	}
	if opts.StateDir == "" && cfg != nil {
		opts.StateDir = strings.TrimSpace(cfg.StateDir)
	}
	if opts.FFmpegMinMajor == 0 {
		opts.FFmpegMinMajor = DefaultFFmpegMinMajor
	}
	if opts.BaselineSHA256Path == "" {
		opts.BaselineSHA256Path = filepath.Join(opts.WorkDir, DefaultBaselineFixtureRel)
	}
	return opts
}
