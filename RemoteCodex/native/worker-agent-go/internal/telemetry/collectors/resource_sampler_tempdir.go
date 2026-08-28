package collectors

import (
	"os"
	"path/filepath"
)

// resource_sampler_tempdir.go owns the temp-dir activity accounting of
// the Sampler (sampleTempActivity): cumulative observed growth of the
// worker scratch tree + the current count of worker-process
// descriptors pointing into it. The rest of the sampler lives in
// resource_sampler.go.

// sampleTempActivity returns cumulative observed growth, the number of
// worker-process descriptors currently pointing into TempDir, the current
// scratch dir occupancy (sum of file sizes), and the peak occupancy since
// worker start. Deletions never reduce the cumulative counter. This is
// deliberately scoped to the worker's own temp tree; counting the entire
// OS temp directory would mix unrelated processes into the render telemetry.
func (s *Sampler) sampleTempActivity() (int64, int, int64, int64) {
	if s == nil {
		return -1, 0, 0, 0
	}
	s.mu.Lock()
	tempDir := s.tempDir
	if tempDir == "" {
		s.mu.Unlock()
		return 0, 0, 0, 0
	}
	var current int64
	_ = filepath.WalkDir(tempDir, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry == nil {
			return nil
		}
		if entry.Type().IsRegular() {
			if info, statErr := entry.Info(); statErr == nil && info.Size() > 0 {
				current += info.Size()
			}
		}
		return nil
	})
	if !s.tempInitialized {
		s.lastTempBytes = current
		s.tempInitialized = true
	} else if current > s.lastTempBytes {
		s.tempBytesWritten += current - s.lastTempBytes
		s.lastTempBytes = current
	} else {
		s.lastTempBytes = current
	}
	if current > s.scratchPeakBytes {
		s.scratchPeakBytes = current
	}
	cumulative := s.tempBytesWritten
	peak := s.scratchPeakBytes
	s.mu.Unlock()

	open := 0
	entries, err := os.ReadDir(filepath.Join(s.procRoot, "self", "fd"))
	if err != nil {
		return cumulative, 0, current, peak
	}
	root, err := filepath.Abs(tempDir)
	if err != nil {
		return cumulative, 0, current, peak
	}
	for _, entry := range entries {
		target, linkErr := os.Readlink(filepath.Join(s.procRoot, "self", "fd", entry.Name()))
		if linkErr != nil {
			continue
		}
		if abs, absErr := filepath.Abs(target); absErr == nil && (abs == root || filepath.HasPrefix(abs, root+string(os.PathSeparator))) {
			open++
		}
	}
	return cumulative, open, current, peak
}
