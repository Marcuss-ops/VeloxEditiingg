package worker

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"velox-worker-agent/internal/spool"
	"velox-worker-agent/internal/telemetry"
)

func (w *Worker) releaseCommittedArtifact(entry spool.SpoolEntry) {
	if w == nil || entry.LocalPath == "" {
		return
	}
	path, err := filepath.Abs(entry.LocalPath)
	if err != nil {
		if w.logger != nil {
			w.logger.Warn("[ARTIFACT_CLEANUP] resolve committed path spool=%s: %v", entry.SpoolID, err)
		}
		return
	}
	var roots []string
	if w.config != nil && w.config.OutputDir != "" {
		roots = append(roots, w.config.OutputDir)
	}
	if w.storageResolver != nil {
		cfg := w.storageResolver.Config()
		if cfg.ArtifactDir != "" {
			roots = append(roots, cfg.ArtifactDir)
		}
		if cfg.ArtifactStaging.Dir != "" {
			roots = append(roots, cfg.ArtifactStaging.Dir)
		}
	}
	if !pathWithinAnyRoot(roots, path) {
		if w.logger != nil {
			w.logger.Warn("[ARTIFACT_CLEANUP] refusing committed output outside configured roots spool=%s path=%q", entry.SpoolID, path)
		}
		return
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		if w.logger != nil {
			w.logger.Warn("[ARTIFACT_CLEANUP] remove committed output failed spool=%s path=%q: %v", entry.SpoolID, path, err)
		}
		return
	}
	if entry.StorageTier == spool.StorageTierTmpfsVolatile && w.storageResolver != nil {
		w.storageResolver.ReleaseStagingPath(path)
	}
}

func (w *Worker) rejectOutputSpool(entries []spool.SpoolEntry, skip map[string]bool, code, message string) {
	if w == nil || w.outputSpool == nil {
		return
	}
	for _, entry := range entries {
		if skip != nil && skip[entry.SpoolID] {
			continue
		}
		if err := w.outputSpool.MarkRejected(context.Background(), entry.SpoolID, code, message); err != nil && !errors.Is(err, spool.ErrCASConflict) && w.logger != nil {
			w.logger.Warn("[ARTIFACT_CLEANUP] reject spool=%s failed: %v", entry.SpoolID, err)
		}
	}
}

func (w *Worker) spillVolatileToNVMe(ctx context.Context, entry spool.SpoolEntry) bool {
	if w == nil || w.outputSpool == nil || entry.StorageTier != spool.StorageTierTmpfsVolatile || entry.LocalPath == "" {
		return false
	}
	durableDir := ""
	if w.config != nil {
		durableDir = w.config.OutputDir
	}
	if w.storageResolver != nil && w.storageResolver.Config().ArtifactDir != "" {
		durableDir = w.storageResolver.Config().ArtifactDir
	}
	if durableDir == "" {
		return false
	}
	newPath := filepath.Join(durableDir, entry.SpoolID+"_"+filepath.Base(entry.LocalPath))
	if err := copyFileDurable(entry.LocalPath, newPath); err != nil {
		if w.logger != nil {
			w.logger.Warn("[SPILL] copy tmpfs to NVMe failed spool=%s: %v", entry.SpoolID, err)
		}
		return false
	}
	if err := w.outputSpool.MarkSpilled(ctx, entry.SpoolID, newPath); err != nil {
		_ = os.Remove(newPath)
		if w.logger != nil {
			w.logger.Warn("[SPILL] mark spilled failed spool=%s: %v", entry.SpoolID, err)
		}
		return false
	}
	if err := os.Remove(entry.LocalPath); err != nil && !errors.Is(err, os.ErrNotExist) && w.logger != nil {
		w.logger.Warn("[SPILL] remove tmpfs copy failed spool=%s: %v", entry.SpoolID, err)
	}
	if w.storageResolver != nil {
		w.storageResolver.ReleaseStagingPath(entry.LocalPath)
	}
	var bytes int64
	if stat, err := os.Stat(newPath); err == nil {
		bytes = stat.Size()
	}
	telemetry.GetPrometheusMetrics().RecordArtifactTmpfsSpill(bytes)
	return true
}

func (w *Worker) spillVolatileUncommitted() {
	if w == nil || w.outputSpool == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	entries, err := w.outputSpool.ListVolatileUncommitted(ctx)
	if err != nil {
		if w.logger != nil {
			w.logger.Warn("[SPILL] list volatile uncommitted failed: %v", err)
		}
		return
	}
	for _, entry := range entries {
		w.spillVolatileToNVMe(ctx, entry)
	}
}

func copyFileDurable(src, dst string) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o750); err != nil {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o640)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		_ = os.Remove(dst)
		return err
	}
	if err := out.Sync(); err != nil {
		_ = out.Close()
		_ = os.Remove(dst)
		return err
	}
	if err := out.Close(); err != nil {
		_ = os.Remove(dst)
		return err
	}
	return nil
}

func pathWithinRoot(root, path string) bool {
	if root == "" || path == "" {
		return false
	}
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return false
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		return false
	}
	rel, err := filepath.Rel(absRoot, absPath)
	if err != nil || rel == "." || rel == ".." {
		return false
	}
	return !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func pathWithinAnyRoot(roots []string, path string) bool {
	for _, root := range roots {
		if pathWithinRoot(root, path) {
			return true
		}
	}
	return false
}
