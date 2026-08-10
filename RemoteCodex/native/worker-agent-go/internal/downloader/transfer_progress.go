package downloader

import "time"

// reportProgress is the transferer's incremental byte hook: it refreshes the
// byte counter, recomputes throughput, and publishes / checkpoints under the
// configured throttles (500 ms / 4 MiB publishes; 2 s / 16 MiB checkpoints).
// State changes and terminal transitions bypass the throttles elsewhere.
// Safe to call once per read chunk from the transferer goroutine.
func (t *Transfer) reportProgress(downloaded int64) {
	now := t.now()
	var ckptSnap DownloadSnapshot
	publish := false
	checkpoint := false

	t.mu.Lock()
	if downloaded < 0 {
		downloaded = 0
	}
	if t.bytesTotal > 0 && downloaded > t.bytesTotal {
		downloaded = t.bytesTotal
	}
	t.bytesDownloaded = downloaded
	t.updatedAt = now
	if downloaded == 0 {
		// A retry reset starts a new logical attempt. Do not carry the
		// previous attempt's rate into the new ETA calculation.
		t.throughputBPS = 0
	} else if !t.lastSampleAt.IsZero() {
		if elapsed := now.Sub(t.lastSampleAt).Seconds(); elapsed > 0 && downloaded >= t.lastSampleBytes {
			t.throughputBPS = float64(downloaded-t.lastSampleBytes) / elapsed
		}
	}
	t.lastSampleAt = now
	t.lastSampleBytes = downloaded

	if t.lastPublishAt.IsZero() {
		publish = true
	} else {
		publish = now.Sub(t.lastPublishAt) >= t.publishInterval ||
			downloaded-t.lastPublishBytes >= t.publishBytes
	}
	if t.onCheckpoint != nil {
		if t.lastCheckpointAt.IsZero() {
			checkpoint = true
		} else {
			checkpoint = now.Sub(t.lastCheckpointAt) >= t.checkpointInterval ||
				downloaded-t.lastCheckpointBytes >= t.checkpointBytes
		}
		if checkpoint {
			ckptSnap = t.snapshotLocked(now)
			t.lastCheckpointAt = now
			t.lastCheckpointBytes = downloaded
		}
	}
	t.mu.Unlock()

	if publish {
		t.publish(now)
		t.mu.Lock()
		t.lastPublishAt = now
		t.lastPublishBytes = downloaded
		t.mu.Unlock()
	}
	if publish || checkpoint {
		t.notifyOperational()
	}
	if checkpoint {
		t.mu.Lock()
		t.checkpointSequence++
		ckptSnap.CheckpointSequence = t.checkpointSequence
		t.mu.Unlock()
		t.onCheckpoint(ckptSnap, t.reportCtx)
	}
}

// emitCheckpoint invokes the durable checkpoint hook unconditionally with the
// current snapshot (used for terminal transitions, which always checkpoint).
// No-op when no hook is configured. Called outside the transfer mutex.
func (t *Transfer) emitCheckpoint(now time.Time) {
	if t.onCheckpoint == nil {
		return
	}
	t.mu.Lock()
	t.checkpointSequence++
	snap := t.snapshotLocked(now)
	t.mu.Unlock()
	t.onCheckpoint(snap, t.reportCtx)
}
