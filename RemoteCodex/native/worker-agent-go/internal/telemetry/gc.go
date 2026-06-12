// Package telemetry provides disk garbage collection and metrics for the worker agent.
package telemetry

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"velox-worker-agent/pkg/logger"
)

// GC event names for structured logging.
const (
	EventDiskCheck       = "DISK_CHECK"
	EventDiskWarning     = "DISK_WARNING"
	EventDiskCritical    = "DISK_CRITICAL"
	EventGCleanupLRU     = "GC_LRU_CLEANUP"
	EventGCleanupAggressive = "GC_AGGRESSIVE_CLEANUP"
	EventGCleanupPurge   = "GC_PURGE_VERSION"
	EventGCleanupCache   = "GC_CLEAR_CACHE"
)

type DiskGC struct {
	workDir       string
	isDiskFull    bool
	diskThreshold float64
	warnThreshold float64
}

func NewDiskGC(workDir string) *DiskGC {
	return &DiskGC{
		workDir:       workDir,
		diskThreshold: 0.98, // 98% (Raised from 90% for constrained environments)
		warnThreshold: 0.95, // 95% (Raised from 80%)
	}
}

func (g *DiskGC) IsDiskFull() bool {
	return g.isDiskFull
}

func (g *DiskGC) getDiskUsage(path string) (float64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0, err
	}

	total := float64(stat.Blocks) * float64(stat.Bsize)
	free := float64(stat.Bavail) * float64(stat.Bsize)
	used := total - free

	if total == 0 {
		return 0, nil
	}
	return used / total, nil
}

// RunCheck performs a disk usage check and triggers cleanup if needed.
func (g *DiskGC) RunCheck(ctx context.Context) {
	usage, err := g.getDiskUsage(g.workDir)
	if err != nil {
		logger.Error("[%s] Failed to get disk usage for %s: %v", EventDiskCheck, g.workDir, err)
		return
	}

	logger.Debug("[%s] Disk usage: %.2f%% (path: %s)", EventDiskCheck, usage*100, g.workDir)

	// Phase 4: GC Rules
	if usage >= g.diskThreshold {
		if !g.isDiskFull {
			logger.Warn("[%s] Disk usage exceeded %.0f%%! Halting downloads. (usage: %.2f%%, path: %s)",
				EventDiskCritical, g.diskThreshold*100, usage*100, g.workDir)
			g.isDiskFull = true
		}
		g.performAggressiveCleanup()
	} else if usage >= g.warnThreshold {
		logger.Warn("[%s] Disk usage exceeded %.0f%%. Running garbage collection. (usage: %.2f%%, path: %s)",
			EventDiskWarning, g.warnThreshold*100, usage*100, g.workDir)
		g.performLRUCleanup()
		g.isDiskFull = false
	} else {
		g.isDiskFull = false // Normal operation
	}
}

// performLRUCleanup removes old versions keeping the active one.
func (g *DiskGC) performLRUCleanup() {
	versionsDir := filepath.Join(g.workDir, "versions")
	entries, err := os.ReadDir(versionsDir)
	if err != nil {
		logger.Debug("[%s] Could not read versions dir: %v", EventGCleanupLRU, err)
		return
	}

	// Figure out currently active version via symlink
	currentLink := filepath.Join(g.workDir, "current")
	target, _ := os.Readlink(currentLink)
	activeVersion := filepath.Base(target)

	logger.Debug("[%s] Active version: %s", EventGCleanupLRU, activeVersion)

	deleted := 0
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		if entry.Name() == activeVersion {
			continue // Never delete active
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}

		// If older than 24 hours, prune it
		if time.Since(info.ModTime()) > 24*time.Hour {
			purgePath := filepath.Join(versionsDir, entry.Name())
			logger.Info("[%s] Removing old version dir: %s (age: %v)",
				EventGCleanupPurge, purgePath, time.Since(info.ModTime()).Round(time.Hour))
			os.RemoveAll(purgePath)
			deleted++
		}
	}

	if deleted > 0 {
		logger.Info("[%s] LRU cleanup complete. Removed %d old versions.", EventGCleanupLRU, deleted)
	}
}

// performAggressiveCleanup performs LRU cleanup plus clears workspace caches.
func (g *DiskGC) performAggressiveCleanup() {
	// First do normal LRU
	g.performLRUCleanup()

	// Then clear workspace temp caches (ffmpeg / downloads)
	workspaceCache := filepath.Join(g.workDir, "workspace", "cache")
	if _, err := os.Stat(workspaceCache); err == nil {
		logger.Info("[%s] Clearing workspace cache: %s", EventGCleanupCache, workspaceCache)
		os.RemoveAll(workspaceCache)
		os.MkdirAll(workspaceCache, 0755)
	}
}