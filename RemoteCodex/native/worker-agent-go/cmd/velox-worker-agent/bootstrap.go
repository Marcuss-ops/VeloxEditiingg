// Command velox-worker-agent is the main entry point for the Velox Worker Agent.
//
// AGENT 2 - Standardized Logging:
// - Uses structured events for all startup/register/heartbeat/shutdown operations
// - All log output follows [EVENT_CODE] format for automatic parsing
//
// Composition-root layout:
//   - internal/bootstrap owns the initial-configuration resolution
//     (ResolveConfig), the default-config generator (GenerateDefaultConfig),
//     the env helpers (EnvOr/EnvBool), and the RW-PROD-003 bootstrap-gate
//     dispatch (Dispatch).
//   - This file keeps the process-level wiring: flag parsing, executor
//     registry construction, cache/blob wiring, and worker start/shutdown.
package main

import (
	"context"
	"sync"
	"time"

	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/taskrunner/executors"
	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/pkg/config"
	"velox-worker-agent/pkg/logger"
	"velox-worker-agent/pkg/video/pipeline"
)

// diskWatcher owns the process-lifetime disk watcher. Keeping the
// single-flight guard on the runtime object avoids mutable package-global
// state while preserving the invariant that the watcher starts at most once.
type diskWatcher struct {
	once    sync.Once
	onStart func()
	onStop  func()
}

// start (RW-PROD-004 §3 A4) launches a 15s-tier goroutine
// that polls the engine-output directory's free bytes and forwards the
// change to telemetry.SetDiskState. Readiness is held until the first
// sample lands (DiskFreeBytes=0 = unknown) so dashboards do not see a
// misleading `disk_free_bytes=0` reading on a fresh boot.
//
// Returns immediately if the watcher has already been started
// (sync.Once-style). Safe to call before cache/blob/registry wiring;
// the watcher operates on cfg.MinDiskFreeMB and /tmp/velox/scene-composite.
//
// Cancel via ctx. The goroutine exits cleanly on ctx.Done().
func (w *diskWatcher) start(ctx context.Context, cfg *config.WorkerConfig, watchDir string, log *logger.Logger) {
	if w == nil {
		return
	}
	w.once.Do(func() {
		if w.onStart != nil {
			w.onStart()
		}
		thresholdBytes := int64(cfg.MinDiskFreeMB) * 1024 * 1024
		go func() {
			defer func() {
				if w.onStop != nil {
					w.onStop()
				}
			}()
			// Initial sample on startup so /health/ready has a real
			// disk_free_bytes value before any traffic. Without this
			// first read, the first 15 seconds of a fresh boot would
			// have DiskFreeBytes=0 → potentially `disk.critical` if the
			// threshold is positive, masking actual readiness.
			if free, err := telemetry.DiskFreeAt(watchDir); err == nil {
				telemetry.SetDiskState(free, thresholdBytes)
			}
			ticker := time.NewTicker(15 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					free, err := telemetry.DiskFreeAt(watchDir)
					if err != nil {
						log.Warn("[DISK_WATCH] statfs %s failed: %v", watchDir, err)
						continue
					}
					telemetry.SetDiskState(free, thresholdBytes)
					if free < thresholdBytes {
						log.Warn("[DISK_WATCH] free=%d threshold=%d (below floor)", free, thresholdBytes)
					}
				}
			}
		}()
	})
}

// Version is set at build time via -ldflags.
var Version = "dev"

// registerCanonicalRenderExecutors is the composition-root registration
// boundary for the render executor family. V1 remains registered first and
// unchanged; V2 is additive and uses the same registry advertised by worker
// capabilities and used by dispatch.
func registerCanonicalRenderExecutors(reg *executor.Registry, outputRoot string, runners ...*pipeline.Runner) error {
	if err := executors.RegisterRenderPlanExecutors(reg, outputRoot); err != nil {
		return err
	}
	if err := executors.RegisterRenderBatchExecutor(reg, nil, outputRoot); err != nil {
		return err
	}
	if len(runners) > 0 && runners[0] != nil {
		if err := reg.Register(executors.NewVideoAssembleCopy(runners[0], outputRoot)); err != nil {
			return err
		}
	}
	return nil
}

