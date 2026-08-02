package taskattempts

// report_ratios.go owns the computed ratio methods over
// AttemptMetrics / AttemptCacheStats (the scorecard's normalized
// surfaces). The struct declarations live in report.go.

// CacheHitRatio is cache_hits / (cache_hits + cache_misses). Returns 0 when
// no cache activity was recorded.
func (s AttemptCacheStats) CacheHitRatio() float64 {
	total := s.CacheHits + s.CacheMisses
	if total <= 0 {
		return 0
	}
	return float64(s.CacheHits) / float64(total)
}

// CacheByteHitRatio is the canonical scorecard ratio: bytes_from_local_cache
// divided by total bytes (cache + blobstore + drive). Reported as a fraction
// in [0,1] for both the API surface and the Prometheus gauge.
func (m AttemptMetrics) CacheByteHitRatio() float64 {
	total := m.BytesFromDrive + m.BytesFromBlobstore + m.BytesFromLocalCache
	if total == 0 {
		return 0
	}
	return float64(m.BytesFromLocalCache) / float64(total)
}

// DuplicateDownloadRatio is the canonical scorecard ratio:
// duplicate_download_bytes / (duplicate + unique). Returns 0 when no bytes
// were downloaded at all (first-attempt cache hit).
func (m AttemptMetrics) DuplicateDownloadRatio() float64 {
	unique := m.InputBytes - m.DuplicateDownloadBytes
	if unique <= 0 && m.DuplicateDownloadBytes == 0 {
		return 0
	}
	total := m.DuplicateDownloadBytes + unique
	if total == 0 {
		return 0
	}
	return float64(m.DuplicateDownloadBytes) / float64(total)
}

// TempStorageAmplification is temp_bytes_written / output_bytes. Returns 0
// when the attempt produced no output (e.g. failed).
func (m AttemptMetrics) TempStorageAmplification() float64 {
	if m.OutputBytes == 0 {
		return 0
	}
	return float64(m.TempBytesWritten) / float64(m.OutputBytes)
}

// EncodeAmplification is frames_encoded / output_frames. Returns 0 when
// frames_encoded is zero (older workers / pre-PR-2 bridge) OR output is 0.
// Output frames are derived from media_duration_seconds * fps; we keep a
// raw int on the table when the worker surfaces it (PR-2 followup):
// frames_encoded is a strict superset proxy for output_frames, so a value
// >1 here is a real signal of re-encoding.
func (m AttemptMetrics) EncodeAmplification() float64 {
	if m.FramesEncoded == 0 {
		return 0
	}
	// PR-2 followup will introduce OutputFrames as a separate column;
	// until then frames_encoded IS the only signal we have, so the
	// amplification ratio is upper-bounded by 1 — before this worker
	// refactor we conservatively report 1.
	return 1.0
}

// RenderSpeedRatio is media_duration_seconds / wall_clock_seconds. The
// single most important number on the scorecard: >1 means we beat
// realtime. Returns 0 when either side is unknown.
func (m AttemptMetrics) RenderSpeedRatio() float64 {
	if m.WallClockSeconds <= 0 {
		return 0
	}
	if m.MediaDurationSeconds <= 0 {
		return 0
	}
	return m.MediaDurationSeconds / m.WallClockSeconds
}

// RenderFactor is wall_clock_seconds / media_duration_seconds. It is the
// inverse of RenderSpeedRatio: a value of 0.5 means the attempt finished in
// half the media duration; a value of 2 means it took twice as long. Returns
// 0 when either side is unknown.
func (m AttemptMetrics) RenderFactor() float64 {
	if m.MediaDurationSeconds <= 0 {
		return 0
	}
	if m.WallClockSeconds <= 0 {
		return 0
	}
	return m.WallClockSeconds / m.MediaDurationSeconds
}

// EncodeMsPerOutputMinute divides engine_segment_build_ms by output minutes.
// Returns 0 when no output duration is available.
func (m AttemptMetrics) EncodeMsPerOutputMinute() float64 {
	outputMinutes := m.MediaDurationSeconds / 60.0
	if outputMinutes <= 0 {
		return 0
	}
	if m.EngineSegmentBuildMs <= 0 {
		return 0
	}
	return float64(m.EngineSegmentBuildMs) / outputMinutes
}

// CpuMsPerOutputMinute divides cpu_time_ms by output minutes. Returns 0 when
// no output duration is available.
func (m AttemptMetrics) CpuMsPerOutputMinute() float64 {
	outputMinutes := m.MediaDurationSeconds / 60.0
	if outputMinutes <= 0 {
		return 0
	}
	if m.CPUTimeMS <= 0 {
		return 0
	}
	return float64(m.CPUTimeMS) / outputMinutes
}

// DownloadThroughputBytesPerSec is downloaded_bytes / download_seconds.
// Downloaded bytes are approximated by bytes from blobstore plus drive;
// download time is engine_asset_download_ms. Returns 0 when no download
// time was recorded.
func (m AttemptMetrics) DownloadThroughputBytesPerSec() float64 {
	downloadSeconds := float64(m.EngineAssetDownloadMs) / 1000.0
	if downloadSeconds <= 0 {
		return 0
	}
	downloadedBytes := m.BytesFromBlobstore + m.BytesFromDrive
	if downloadedBytes <= 0 {
		return 0
	}
	return float64(downloadedBytes) / downloadSeconds
}
