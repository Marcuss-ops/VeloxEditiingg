package executors

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"velox-shared/contract"
	"velox-worker-agent/pkg/storage"
)

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
		"-show_entries", "stream=codec_type,codec_name,profile,level,width,height,pix_fmt,r_frame_rate,time_base,has_b_frames,extradata_hash,bit_rate,start_time,nb_frames:frame=key_frame,best_effort_timestamp_time",
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
	frameCount, _ := strconv.ParseInt(stream.SourceFrameCount, 10, 64)
	probe := windowSourceProbe{
		FrameCount: frameCount,
		CodecName:  stream.CodecName, Profile: strings.ToLower(stream.Profile), Level: stream.Level,
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
	// The source stream time base is an input packet timestamp scale, not a
	// bitstream compatibility property. FFmpeg's MP4 muxer rescales copied
	// packet timestamps to the canonical output track scale configured by
	// canonicalWindowStreamCopyArgs. The output certifier still requires the
	// canonical time base, exact frame count and canonical stream signature.
	if profile.BFrames != 0 || source.BFrames != 0 {
		reasons = append(reasons, "b_frames_present")
	}
	if !profile.ClosedGOP {
		reasons = append(reasons, "profile_not_closed_gop")
	}
	if source.ExtradataSHA != expectedExtradata || !isLowerSHA256(source.ExtradataSHA) {
		reasons = append(reasons, "extradata_mismatch")
	}
	// VideoMaxRate is an encoder VBV constraint, not a packet-copy
	// compatibility property. In particular, ffprobe's average stream
	// bitrate for very short windows (including a single IDR frame) can
	// exceed that value even though the bytes are unchanged and the stream
	// signature is canonical. Applying the encode-time ceiling here rejects
	// valid immutable media and cannot improve packet-copy safety.
	startSeconds, _, ok := sourceWindowStartSeconds(source.StartTimeSec, request)
	if !ok || request.OutputFrameCount <= 0 {
		reasons = append(reasons, "invalid_window_or_source_start_time")
	}
	if ok {
		frameCountMatches := request.SourceDurationUS > 0 && timestampMatchesFrames(request.SourceDurationUS, request.OutputFrameCount, profile.FPSNum, profile.FPSDen)
		if request.SourceDurationUS == 0 {
			frameCountMatches = request.SourceFPSNum == profile.FPSNum && request.SourceFPSDen == profile.FPSDen && request.SourceFrameCount == request.OutputFrameCount
		}
		if !frameCountMatches {
			reasons = append(reasons, "output_frame_count_mismatch")
		}
	}
	if ok && request.SourceDurationUS > 0 {
		if !timestampMatchesFrames(request.SourceStartUS, -1, profile.FPSNum, profile.FPSDen) {
			reasons = append(reasons, "window_start_not_frame_aligned")
		}
	}
	keyframeFound := false
	for _, keyframeTime := range source.KeyframeTimes {
		if ok && absFloat(keyframeTime-startSeconds) <= 1e-6 {
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

// timestampMatchesFrames checks microsecond timestamps against an exact
// rational frame rate. Window plans convert frame boundaries to integer
// microseconds, so the round trip can differ by at most one microsecond;
// floating-point equality incorrectly rejects valid 24000/1001 boundaries.
func timestampMatchesFrames(timestampUS, expectedFrames int64, fpsNum, fpsDen int) bool {
	if timestampUS < 0 || fpsNum <= 0 || fpsDen <= 0 {
		return false
	}
	den := int64(1_000_000) * int64(fpsDen)
	if den <= 0 || timestampUS > math.MaxInt64/int64(fpsNum) {
		return false
	}
	numerator := timestampUS * int64(fpsNum)
	frames, remainder := numerator/den, numerator%den
	if remainder >= (den+1)/2 {
		frames++
	}
	if expectedFrames >= 0 && frames != expectedFrames {
		return false
	}
	if frames > numerator/den {
		remainder = den - remainder
	}
	// Frame boundaries are stored as integer microseconds and producers may
	// truncate or round the rational frame duration. Allow at most one
	// microsecond of quantization, represented in numerator units.
	return remainder <= int64(fpsNum)
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

// sourceWindowStartSeconds converts the window's source-relative offset to the
// absolute presentation timestamp ffmpeg must seek. Some valid H.264 MP4s
// begin at a small positive PTS (for example 21 ms); that timestamp is the
// source origin, not a leading gap that should make a first-frame window
// incompatible with packet copy.
func sourceWindowStartSeconds(sourceStartTimeSec float64, request contract.VideoPrepareWindowV1) (float64, float64, bool) {
	startSeconds, durationSeconds, ok := requestedWindowSeconds(request)
	if !ok || math.IsNaN(sourceStartTimeSec) || math.IsInf(sourceStartTimeSec, 0) || sourceStartTimeSec < 0 {
		return 0, 0, false
	}
	absoluteStart := sourceStartTimeSec + startSeconds
	if math.IsNaN(absoluteStart) || math.IsInf(absoluteStart, 0) {
		return 0, 0, false
	}
	return absoluteStart, durationSeconds, true
}

func canonicalWindowStreamCopyArgs(profile contract.CanonicalVideoProfileV1, inputPath, outputPath string, request contract.VideoPrepareWindowV1, sourceStartTimeSec float64) []string {
	startSeconds, _, _ := sourceWindowStartSeconds(sourceStartTimeSec, request)
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

// Missing container frame counts or a non-zero presentation origin keep the
// existing window-only path. Never fabricate full-source evidence from a slice.
func sourceCertificateFromProbe(source windowSourceProbe, request contract.VideoPrepareWindowV1, profile contract.CanonicalVideoProfileV1) *contract.VideoSourceCertificateV1 {
	if source.FrameCount <= 0 || source.StartTimeSec != 0 {
		return nil
	}
	certificate := &contract.VideoSourceCertificateV1{Version: 1, SHA256: request.Source.SHA256, SizeBytes: request.Source.SizeBytes, ProfileID: request.OutputProfileID, PreparationRevision: request.OutputProfileRevision,
		Evidence: contract.VideoWindowOutputEvidenceV1{Codec: source.CodecName, Profile: source.Profile, Level: source.Level, Width: source.Width, Height: source.Height, PixelFormat: source.PixelFormat, FPSNum: source.FPSNum, FPSDen: source.FPSDen, TimeBaseNum: source.TimeBaseNum, TimeBaseDen: source.TimeBaseDen, BFrames: source.BFrames, FrameCount: source.FrameCount, FirstFrameKeyframe: true, ExtradataSHA256: source.ExtradataSHA}}
	if source.FrameCount > math.MaxInt64/int64(profile.FPSDen)/1_000_000 {
		return nil
	}
	certificate.DurationUS = source.FrameCount * int64(profile.FPSDen) * 1_000_000 / int64(profile.FPSNum)
	for _, seconds := range source.KeyframeTimes {
		if math.IsNaN(seconds) || math.IsInf(seconds, 0) || seconds < 0 || seconds > float64(certificate.DurationUS)/1e6 {
			return nil
		}
		certificate.KeyframesUS = append(certificate.KeyframesUS, int64(math.Round(seconds*1e6)))
	}
	if certificate.Validate() != nil {
		return nil
	}
	return certificate
}
