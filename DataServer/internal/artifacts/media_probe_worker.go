package artifacts

import (
	"context"
	"fmt"
	"log"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"velox-server/internal/store"
)

const (
	defaultMediaProbeConcurrency = 2
	defaultMediaProbePoll        = 250 * time.Millisecond
	defaultMediaProbeLease       = 2 * time.Minute
)

// MediaProbeFunc is injectable so tests can model a slow ffprobe without
// starting a subprocess. The returned values are audio stream count and
// duration in milliseconds.
type MediaProbeFunc func(context.Context, string) (int, int64, error)

// MediaProbeWorker owns a bounded pool. A worker claims only one durable row
// before probing, so a slow ffprobe consumes a pool slot but never blocks the
// ingest request or causes local over-claiming.
type MediaProbeWorker struct {
	repo        *store.MediaProbeRepository
	finalDir    string
	concurrency int
	poll        time.Duration
	lease       time.Duration
	probe       MediaProbeFunc
	owner       string
}

func NewMediaProbeWorker(repo *store.MediaProbeRepository, finalDir string, concurrency int, probe MediaProbeFunc) *MediaProbeWorker {
	if concurrency <= 0 {
		concurrency = defaultMediaProbeConcurrency
	}
	if probe == nil {
		probe = probeMediaFile
	}
	return &MediaProbeWorker{repo: repo, finalDir: finalDir, concurrency: concurrency, poll: defaultMediaProbePoll, lease: defaultMediaProbeLease, probe: probe, owner: "media-probe"}
}

func (w *MediaProbeWorker) WithPollInterval(d time.Duration) *MediaProbeWorker {
	if d > 0 {
		w.poll = d
	}
	return w
}
func (w *MediaProbeWorker) Run(ctx context.Context) error {
	if w == nil || w.repo == nil {
		return fmt.Errorf("artifacts: media probe worker unavailable")
	}
	var wg sync.WaitGroup
	wg.Add(w.concurrency)
	for i := 0; i < w.concurrency; i++ {
		go func(index int) {
			defer wg.Done()
			w.runSlot(ctx, fmt.Sprintf("%s-%d", w.owner, index))
		}(i)
	}
	wg.Wait()
	return ctx.Err()
}

func (w *MediaProbeWorker) runSlot(ctx context.Context, owner string) {
	for {
		if ctx.Err() != nil {
			return
		}
		job, err := w.repo.ClaimMediaProbe(ctx, owner, w.lease, time.Now().UTC())
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[MEDIA-PROBE] claim failed: %v", err)
			if !sleepProbe(ctx, w.poll) {
				return
			}
			continue
		}
		if job == nil {
			if !sleepProbe(ctx, w.poll) {
				return
			}
			continue
		}
		path := filepath.Join(w.finalDir, filepath.FromSlash(job.StorageKey))
		probeCtx, cancel := context.WithTimeout(ctx, w.lease/2)
		actual, duration, probeErr := w.probe(probeCtx, path)
		cancel()
		if probeErr != nil {
			if err := w.repo.FailMediaProbe(ctx, *job, probeErr, time.Now().UTC()); err != nil && ctx.Err() == nil {
				log.Printf("[MEDIA-PROBE] fail update artifact=%s: %v", job.ArtifactID, err)
			}
			continue
		}
		if err := w.repo.CompleteMediaProbe(ctx, *job, actual, duration, time.Now().UTC()); err != nil && ctx.Err() == nil {
			log.Printf("[MEDIA-PROBE] complete artifact=%s: %v", job.ArtifactID, err)
		}
	}
}

func sleepProbe(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func probeMediaFile(ctx context.Context, path string) (int, int64, error) {
	cmd := exec.CommandContext(ctx, "ffprobe", "-v", "error", "-show_entries", "stream=codec_type:format=duration", "-of", "default=noprint_wrappers=1:nokey=1", filepath.Clean(path))
	out, err := cmd.Output()
	if err != nil {
		return 0, 0, fmt.Errorf("ffprobe %s: %w", path, err)
	}
	var audio, durationMs int
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if line == "audio" {
			audio++
		}
		if v, parseErr := strconv.ParseFloat(line, 64); parseErr == nil && v > 0 {
			durationMs = int(v * 1000)
		}
	}
	return audio, int64(durationMs), nil
}
