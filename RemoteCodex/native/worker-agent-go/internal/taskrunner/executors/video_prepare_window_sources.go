package executors

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"velox-shared/assetref"
	"velox-shared/contract"
	"velox-worker-agent/internal/workercache"
	"velox-worker-agent/pkg/storage"
	"velox-worker-agent/pkg/video/ffmpegrunner"
)

// normalizedSourcePath returns a cached whole-source normalization for this
// immutable source, creating it once on a miss. Normalization is an
// optimization: any failure falls back to the original source so a transient
// cache problem cannot fail an otherwise valid window. The output carries the
// same canonical encoder lane as the final fragment; the per-window encode
// still selects the exact interval because the source GOP is arbitrary.
func (e *videoPrepareWindowExecutor) normalizedSourcePath(ctx context.Context, sourcePath, sourceSHA string) (string, bool) {
	if e == nil || e.derivedCache == nil || !isLowerSHA256(sourceSHA) {
		return "", false
	}
	profile := e.normalizationProfile()
	if err := profile.Validate(); err != nil {
		return "", false
	}
	sourceHash := assetref.ContentHash(sourceSHA)
	if entry, found, err := e.derivedCache.FindDerived(ctx, sourceHash, profile); err == nil && found && strings.TrimSpace(entry.LocalPath) != "" {
		if e.normalizedSourceIsValid(ctx, entry.LocalPath) {
			e.cacheMetrics().RecordNormalizedSourceCacheHit()
			return entry.LocalPath, true
		}
	}
	key, err := workercache.DerivedAssetKey(sourceHash, profile)
	if err != nil {
		return "", false
	}
	// Several preparation tasks can normalize windows from the same source at
	// once. Serialize cache publication for this derived key and recheck after
	// taking the lock; otherwise concurrent FFmpeg processes can race on one
	// shared partial path and publish a truncated MP4.
	unlock := e.lockWindow("normalized-source:" + string(key))
	defer unlock()
	sum := sha256.Sum256([]byte(key))
	dir := filepath.Join(e.outputRoot, "derived-sources")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return "", false
	}
	outputPath := filepath.Join(dir, hex.EncodeToString(sum[:])+".mp4")
	if e.normalizedSourceIsValid(ctx, outputPath) {
		e.cacheMetrics().RecordNormalizedSourceCacheHit()
		return outputPath, true
	} else {
		// A previous interrupted/competing encoder may have left a non-empty
		// but unusable MP4. Never treat file size alone as cache validation.
		_ = os.Remove(outputPath)
	}
	e.cacheMetrics().RecordNormalizedSourceCacheMiss()
	tmp, err := os.CreateTemp(dir, ".normalized-source-*.partial.mp4")
	if err != nil {
		return "", false
	}
	tmpPath := tmp.Name()
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return "", false
	}
	defer os.Remove(tmpPath)
	args := contract.CanonicalVideoNormalizeSourceArgs(e.profile, e.quality, sourcePath, tmpPath)
	if _, err := e.runner.Run(ctx, ffmpegrunner.FFmpegRequest{Operation: ffmpegrunner.OperationEncode, Args: args}); err != nil {
		return "", false
	}
	e.cacheMetrics().RecordNormalizedSourceEncode()
	if !e.normalizedSourceIsValid(ctx, tmpPath) {
		return "", false
	}
	if err := os.Rename(tmpPath, outputPath); err != nil {
		return "", false
	}
	artifact, err := artifactFromFile("video/mp4", outputPath)
	if err != nil {
		_ = os.Remove(outputPath)
		return "", false
	}
	_, _ = e.derivedCache.StoreDerived(ctx, sourceHash, profile, workercache.Entry{
		ContentHash: assetref.ContentHash(artifact.Hash), LocalPath: outputPath,
		SizeBytes: artifact.SizeBytes, DownloadComplete: true,
	})
	return outputPath, true
}

// validNormalizedSource verifies that the container can be parsed and that
// the expected video stream is present before a derived file is reused or
// published. Full per-window certification still runs after encoding.
func validNormalizedSource(ctx context.Context, path string, profile contract.CanonicalVideoProfileV1) bool {
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 {
		return false
	}
	args := []string{"-v", "error", "-select_streams", "v:0", "-show_entries", "stream=codec_name,width,height", "-of", "json", path}
	data, err := exec.CommandContext(ctx, "ffprobe", args...).Output()
	if err != nil {
		return false
	}
	var doc windowFFprobeDocument
	if json.Unmarshal(data, &doc) != nil || len(doc.Streams) != 1 {
		return false
	}
	stream := doc.Streams[0]
	return stream.CodecName == profile.Codec && stream.Width == profile.Width && stream.Height == profile.Height
}

func (e *videoPrepareWindowExecutor) normalizedSourceIsValid(ctx context.Context, path string) bool {
	if e != nil && e.normalizedSourceValidator != nil {
		return e.normalizedSourceValidator(ctx, path, e.profile)
	}
	return validNormalizedSource(ctx, path, e.profile)
}

func (e *videoPrepareWindowExecutor) normalizationProfile() workercache.NormalizationProfile {
	encoderSettings, _ := contract.CanonicalVideoEncoderArgsDigest(e.profile, e.quality)
	return workercache.NormalizationProfile{
		NormalizerVersion: 1,
		Codec:             e.profile.Codec,
		Width:             e.profile.Width,
		Height:            e.profile.Height,
		FPSNum:            e.profile.FPSNum,
		FPSDen:            e.profile.FPSDen,
		PixelFormat:       e.profile.PixelFormat,
		ColorProperties:   contract.CanonicalVideoColorspace + "/" + contract.CanonicalVideoColorRange,
		Profile:           e.profile.CodecProfile,
		Level:             e.profile.CodecLevel,
		GOPPolicy:         fmt.Sprintf("closed-gop:%d", e.profile.GOPSize),
		EncoderSettings:   encoderSettings,
	}
}

func (e *videoPrepareWindowExecutor) preparedWindowPath(identity string, request contract.VideoPrepareWindowV1, resolver *storage.Resolver) string {
	outputName := identity + ".mp4"
	outputPath := filepath.Join(e.outputRoot, outputName)
	if resolver != nil {
		if placement, err := resolver.Place(storage.CachePersistent, filepath.Join("prepared-windows", outputName), estimateOutputBytesFromDuration(int64(float64(request.OutputFrameCount)*1_000_000*float64(e.profile.FPSDen)/float64(e.profile.FPSNum)))); err == nil {
			outputPath = placement.Path
		}
	}
	return outputPath
}

func (e *videoPrepareWindowExecutor) probeSource(ctx context.Context, path, sha256 string) (windowSourceProbe, error) {
	// Coalesce probes for the same immutable source while allowing different
	// sources to proceed concurrently. Recheck the cache after acquiring the
	// keyed lock because another caller may have populated it while we waited.
	unlock := e.lockWindow("source-probe:" + sha256)
	defer unlock()
	if e != nil {
		e.sourceProbeMu.Lock()
		cached, ok := e.sourceProbes[sha256]
		e.sourceProbeMu.Unlock()
		if ok {
			return cached, nil
		}
	}
	if cached, ok := e.readPersistedSourceProbe(sha256); ok {
		e.sourceProbeMu.Lock()
		if e.sourceProbes == nil {
			e.sourceProbes = make(map[string]windowSourceProbe)
		}
		e.sourceProbes[sha256] = cached
		e.sourceProbeMu.Unlock()
		return cached, nil
	}
	args := []string{
		"-v", "error", "-skip_frame", "nokey", "-select_streams", "v:0", "-show_streams", "-show_frames", "-show_data_hash", "sha256",
		"-show_entries", "stream=codec_type,codec_name,profile,level,width,height,pix_fmt,r_frame_rate,time_base,has_b_frames,extradata_hash,bit_rate,start_time:frame=key_frame,best_effort_timestamp_time",
		"-of", "json", path,
	}
	data, err := exec.CommandContext(ctx, "ffprobe", args...).Output()
	if err != nil {
		return windowSourceProbe{}, fmt.Errorf("ffprobe source window: %w", err)
	}
	var doc windowFFprobeDocument
	if err := json.Unmarshal(data, &doc); err != nil {
		return windowSourceProbe{}, fmt.Errorf("decode source window probe: %w", err)
	}
	if len(doc.Streams) != 1 || doc.Streams[0].CodecType != "video" {
		return windowSourceProbe{}, fmt.Errorf("source window has %d selected video streams, expected one", len(doc.Streams))
	}
	stream := doc.Streams[0]
	fpsNum, fpsDen := parseWindowRatio(stream.FrameRate)
	tbNum, tbDen := parseWindowRatio(stream.TimeBase)
	bitRate, err := parseWindowBitRateBPS(stream.BitRate)
	if err != nil {
		return windowSourceProbe{}, err
	}
	startTime := 0.0
	if strings.TrimSpace(stream.StartTime) != "" {
		startTime, err = strconv.ParseFloat(stream.StartTime, 64)
		if err != nil {
			return windowSourceProbe{}, fmt.Errorf("source window has invalid start_time %q", stream.StartTime)
		}
	}
	probe := windowSourceProbe{
		CodecName: stream.CodecName, Profile: strings.ToLower(stream.Profile), Level: stream.Level,
		Width: stream.Width, Height: stream.Height, PixelFormat: stream.PixelFormat,
		FPSNum: fpsNum, FPSDen: fpsDen, TimeBaseNum: tbNum, TimeBaseDen: tbDen,
		BFrames: stream.BFrames, BitRateBPS: bitRate, StartTimeSec: startTime,
		ExtradataSHA: strings.TrimPrefix(strings.TrimSpace(stream.ExtradataHash), "SHA256:"),
	}
	for _, frame := range doc.Frames {
		if frame.KeyFrame != 1 || strings.TrimSpace(frame.BestEffortTimestamp) == "" {
			continue
		}
		timestamp, err := strconv.ParseFloat(frame.BestEffortTimestamp, 64)
		if err != nil {
			continue
		}
		probe.KeyframeTimes = append(probe.KeyframeTimes, timestamp)
	}
	if e != nil {
		e.sourceProbeMu.Lock()
		if e.sourceProbes == nil {
			e.sourceProbes = make(map[string]windowSourceProbe)
		}
		e.sourceProbes[sha256] = probe
		e.sourceProbeMu.Unlock()
		e.writePersistedSourceProbe(sha256, probe)
	}
	return probe, nil
}

func (e *videoPrepareWindowExecutor) sourceProbeIndexPath(sourceSHA string) string {
	if e == nil || !isLowerSHA256(sourceSHA) {
		return ""
	}
	return filepath.Join(e.outputRoot, "source-probes", sourceSHA+".json")
}

func (e *videoPrepareWindowExecutor) readPersistedSourceProbe(sourceSHA string) (windowSourceProbe, bool) {
	path := e.sourceProbeIndexPath(sourceSHA)
	if path == "" {
		return windowSourceProbe{}, false
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return windowSourceProbe{}, false
	}
	var index windowSourceProbeIndex
	if json.Unmarshal(data, &index) != nil || index.Version != windowSourceProbeIndexVersion || index.SourceSHA != sourceSHA || !validWindowSourceProbe(index.Probe) {
		return windowSourceProbe{}, false
	}
	return index.Probe, true
}

func (e *videoPrepareWindowExecutor) writePersistedSourceProbe(sourceSHA string, probe windowSourceProbe) {
	path := e.sourceProbeIndexPath(sourceSHA)
	if path == "" || !validWindowSourceProbe(probe) {
		return
	}
	data, err := json.Marshal(windowSourceProbeIndex{Version: windowSourceProbeIndexVersion, SourceSHA: sourceSHA, Probe: probe})
	if err != nil || os.MkdirAll(filepath.Dir(path), 0o750) != nil {
		return
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".source-probe-*.partial")
	if err != nil {
		return
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if _, err = tmp.Write(data); err != nil {
		_ = tmp.Close()
		return
	}
	if err = tmp.Sync(); err != nil {
		_ = tmp.Close()
		return
	}
	if err = tmp.Close(); err != nil {
		return
	}
	_ = os.Rename(tmpPath, path)
}

func validWindowSourceProbe(probe windowSourceProbe) bool {
	return probe.CodecName != "" && probe.Width > 0 && probe.Height > 0 && probe.FPSNum > 0 && probe.FPSDen > 0 && probe.TimeBaseNum > 0 && probe.TimeBaseDen > 0 && len(probe.KeyframeTimes) > 0
}

type windowCopyEligibility struct {
	Eligible bool
	Reasons  []string
}

func sourceWindowCopyEligible(source windowSourceProbe, request contract.VideoPrepareWindowV1, profile contract.CanonicalVideoProfileV1, quality contract.PreparationQualityPolicy, expectedExtradata string) bool {
	return diagnoseSourceWindowCopy(source, request, profile, quality, expectedExtradata).Eligible
}

func diagnoseSourceWindowCopy(source windowSourceProbe, request contract.VideoPrepareWindowV1, profile contract.CanonicalVideoProfileV1, quality contract.PreparationQualityPolicy, expectedExtradata string) windowCopyEligibility {
	var reasons []string
	if source.CodecName != profile.Codec {
		reasons = append(reasons, "codec_mismatch")
	}
	if source.Profile != strings.ToLower(profile.CodecProfile) {
		reasons = append(reasons, "profile_mismatch")
	}
	if source.Level != canonicalLevelNumber(profile.CodecLevel) {
		reasons = append(reasons, "level_mismatch")
	}
	if source.Width != profile.Width || source.Height != profile.Height {
		reasons = append(reasons, "geometry_mismatch")
	}
	if source.PixelFormat != profile.PixelFormat {
		reasons = append(reasons, "pixel_format_mismatch")
	}
	if source.FPSNum != profile.FPSNum || source.FPSDen != profile.FPSDen {
		reasons = append(reasons, "frame_rate_mismatch")
	}
	if source.TimeBaseNum != profile.TimeBaseNum || source.TimeBaseDen != profile.TimeBaseDen {
		reasons = append(reasons, "time_base_mismatch")
	}
	if profile.BFrames != 0 || source.BFrames != 0 {
		reasons = append(reasons, "b_frames_present")
	}
	if !profile.ClosedGOP {
		reasons = append(reasons, "profile_not_closed_gop")
	}
	if source.ExtradataSHA != expectedExtradata || !isLowerSHA256(source.ExtradataSHA) {
		reasons = append(reasons, "extradata_mismatch")
	}
	maxRate, err := parseWindowBitRateBPS(quality.VideoMaxRate)
	if err != nil || maxRate <= 0 || source.BitRateBPS <= 0 || source.BitRateBPS > maxRate {
		reasons = append(reasons, "bitrate_out_of_range")
	}
	startSeconds, durationSeconds, ok := requestedWindowSeconds(request)
	if !ok || absFloat(source.StartTimeSec) > 1e-6 || request.OutputFrameCount <= 0 {
		reasons = append(reasons, "invalid_window_or_source_start_time")
	}
	if ok {
		wantFrameCount := durationSeconds * float64(profile.FPSNum) / float64(profile.FPSDen)
		if absFloat(wantFrameCount-float64(request.OutputFrameCount)) > 1e-6 {
			reasons = append(reasons, "output_frame_count_mismatch")
		}
	}
	if ok {
		startFrame := startSeconds * float64(profile.FPSNum) / float64(profile.FPSDen)
		if absFloat(startFrame-math.Round(startFrame)) > 1e-6 {
			reasons = append(reasons, "window_start_not_frame_aligned")
		}
	}
	keyframeFound := false
	for _, keyframeTime := range source.KeyframeTimes {
		if absFloat(keyframeTime-startSeconds) <= 1e-6 {
			// The end may be any frame: with B-frames disabled no packet in
			// the copied window references a frame after its final packet.
			keyframeFound = true
			break
		}
	}
	if !keyframeFound {
		reasons = append(reasons, "window_start_not_keyframe")
	}
	return windowCopyEligibility{Eligible: len(reasons) == 0, Reasons: reasons}
}

func requestedWindowSeconds(request contract.VideoPrepareWindowV1) (float64, float64, bool) {
	if request.SourceDurationUS > 0 {
		return float64(request.SourceStartUS) / 1_000_000, float64(request.SourceDurationUS) / 1_000_000, true
	}
	if request.SourceFPSNum <= 0 || request.SourceFPSDen <= 0 || request.SourceFrameCount <= 0 {
		return 0, 0, false
	}
	fps := float64(request.SourceFPSNum) / float64(request.SourceFPSDen)
	return float64(request.SourceStartFrame) / fps, float64(request.SourceFrameCount) / fps, true
}

func canonicalWindowStreamCopyArgs(profile contract.CanonicalVideoProfileV1, inputPath, outputPath string, request contract.VideoPrepareWindowV1) []string {
	startSeconds, _, _ := requestedWindowSeconds(request)
	return []string{
		"-hide_banner", "-loglevel", "error", "-y", "-ss", strconv.FormatFloat(startSeconds, 'f', 6, 64), "-i", inputPath,
		"-map", "0:v:0", "-an", "-sn", "-dn", "-c:v", "copy", "-frames:v", strconv.FormatInt(request.OutputFrameCount, 10),
		"-video_track_timescale", strconv.Itoa(profile.TimeBaseDen), "-avoid_negative_ts", "make_zero", "-reset_timestamps", "1",
		"-movflags", "+faststart", outputPath,
	}
}

func isFirstFrameIDR(ctx context.Context, path string) bool {
	args := []string{
		"-hide_banner", "-loglevel", "verbose", "-i", path, "-map", "0:v:0", "-frames:v", "1", "-c:v", "copy",
		"-bsf:v", "trace_headers", "-f", "null", "-",
	}
	output, err := exec.CommandContext(ctx, "ffmpeg", args...).CombinedOutput()
	if err != nil {
		return false
	}
	for _, line := range strings.Split(string(output), "\n") {
		if !strings.Contains(line, "nal_unit_type") {
			continue
		}
		if strings.TrimSpace(line[strings.LastIndex(line, "=")+1:]) == "5" {
			return true
		}
	}
	return false
}

func parseWindowBitRateBPS(value string) (int64, error) {
	value = strings.TrimSpace(value)
	if value == "" || value == "N/A" {
		return 0, nil
	}
	multiplier := float64(1)
	last := value[len(value)-1]
	switch last {
	case 'k', 'K':
		multiplier, value = 1_000, value[:len(value)-1]
	case 'm', 'M':
		multiplier, value = 1_000_000, value[:len(value)-1]
	case 'g', 'G':
		multiplier, value = 1_000_000_000, value[:len(value)-1]
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil || parsed <= 0 || math.IsNaN(parsed) || math.IsInf(parsed, 0) {
		return 0, fmt.Errorf("invalid video bitrate %q", value)
	}
	return int64(parsed * multiplier), nil
}

func absFloat(value float64) float64 {
	if value < 0 {
		return -value
	}
	return value
}
