package executors

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"velox-shared/contract"
	"velox-worker-agent/pkg/binaryresolver"
)

func (e *videoSourceInspector) probeSource(ctx context.Context, path, sha256 string) (windowSourceProbe, error) {
	unlock := e.lockWindow("source-probe:" + sha256)
	defer unlock()
	e.sourceProbeMu.Lock()
	cached, ok := e.sourceProbes[sha256]
	e.sourceProbeMu.Unlock()
	if ok {
		return cached, nil
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
	probe, err := e.probeCompleteSourceWithLibAV(ctx, path)
	if err != nil {
		return windowSourceProbe{}, err
	}
	e.sourceProbeMu.Lock()
	if e.sourceProbes == nil {
		e.sourceProbes = make(map[string]windowSourceProbe)
	}
	e.sourceProbes[sha256] = probe
	e.sourceProbeMu.Unlock()
	e.writePersistedSourceProbe(sha256, probe)
	return probe, nil
}

type nativeSourceProbeDocument struct {
	CodecName         string    `json:"codec_name"`
	Profile           string    `json:"profile"`
	Level             int       `json:"level"`
	Width             int       `json:"width"`
	Height            int       `json:"height"`
	PixelFormat       string    `json:"pixel_format"`
	FrameRate         string    `json:"frame_rate"`
	AverageFrameRate  string    `json:"average_frame_rate"`
	TimeBase          string    `json:"time_base"`
	StartTimeTicks    int64     `json:"start_time_ticks"`
	StartTimeVerified bool      `json:"start_time_verified"`
	StartTimeSeconds  float64   `json:"start_time_seconds"`
	BFrames           int       `json:"b_frames"`
	BitRate           int64     `json:"bit_rate"`
	ExactFrameCount   int64     `json:"exact_frame_count"`
	ExtradataSHA      string    `json:"extradata_sha256"`
	KeyframeTimes     []float64 `json:"packet_keyframe_times"`
}

func (e *videoSourceInspector) probeCompleteSourceWithLibAV(ctx context.Context, path string) (windowSourceProbe, error) {
	probeFn := e.sourceProbeFn
	if probeFn == nil {
		probeFn = runNativeSourceProbe
	}
	return probeFn(ctx, path)
}

func runNativeSourceProbe(ctx context.Context, path string) (windowSourceProbe, error) {
	resolver := binaryresolver.Resolver{
		Name: "velox_video_engine", EnvVar: "VELOX_VIDEO_ENGINE_CPP_BIN",
		AbsCandidates: []string{"/usr/local/bin/velox_video_engine"},
		RelOffsets: []string{
			filepath.Join("../../../../video-engine-cpp/build", "velox_video_engine"),
			filepath.Join("../../../../video-engine-cpp", "velox_video_engine"),
		},
	}
	binary, err := resolver.Resolve(0)
	if err != nil {
		return windowSourceProbe{}, fmt.Errorf("resolve LibAV source inspector: %w", err)
	}
	command := exec.CommandContext(ctx, binary, "--probe-source", "--input", path)
	data, err := command.Output()
	if err != nil {
		if exit, ok := err.(*exec.ExitError); ok {
			return windowSourceProbe{}, fmt.Errorf("LibAV source certification probe: %w: %s", err, strings.TrimSpace(string(exit.Stderr)))
		}
		return windowSourceProbe{}, fmt.Errorf("LibAV source certification probe: %w", err)
	}
	var doc nativeSourceProbeDocument
	if err := json.Unmarshal(data, &doc); err != nil {
		return windowSourceProbe{}, fmt.Errorf("decode LibAV source certification probe: %w", err)
	}
	fpsNum, fpsDen := parseWindowRatio(doc.FrameRate)
	avgNum, avgDen := parseWindowRatio(doc.AverageFrameRate)
	tbNum, tbDen := parseWindowRatio(doc.TimeBase)
	if !samePositiveRatio(fpsNum, fpsDen, avgNum, avgDen) {
		return windowSourceProbe{}, fmt.Errorf("LibAV source has inconsistent or unavailable frame-rate evidence: r=%q avg=%q", doc.FrameRate, doc.AverageFrameRate)
	}
	if doc.CodecName == "" || doc.Profile == "" || doc.Width <= 0 || doc.Height <= 0 ||
		doc.PixelFormat == "" || tbNum <= 0 || tbDen <= 0 || !doc.StartTimeVerified ||
		doc.ExactFrameCount <= 0 || !isLowerSHA256(doc.ExtradataSHA) || len(doc.KeyframeTimes) == 0 {
		return windowSourceProbe{}, errors.New("LibAV source probe returned incomplete identity, timestamp, frame-count, or packet evidence")
	}
	derivedOrigin := float64(doc.StartTimeTicks) * float64(tbNum) / float64(tbDen)
	if math.IsNaN(doc.StartTimeSeconds) || math.IsInf(doc.StartTimeSeconds, 0) || math.Abs(derivedOrigin-doc.StartTimeSeconds) > 1e-9 {
		return windowSourceProbe{}, errors.New("LibAV source probe returned inconsistent timestamp-origin evidence")
	}
	return windowSourceProbe{
		FrameCount: doc.ExactFrameCount, ExactFrameCount: doc.ExactFrameCount,
		CodecName: doc.CodecName, Profile: strings.ToLower(doc.Profile), Level: doc.Level,
		Width: doc.Width, Height: doc.Height, PixelFormat: doc.PixelFormat,
		FPSNum: fpsNum, FPSDen: fpsDen, AverageFPSNum: avgNum, AverageFPSDen: avgDen,
		TimeBaseNum: tbNum, TimeBaseDen: tbDen, StartTimeTicks: doc.StartTimeTicks,
		StartTimeVerified: doc.StartTimeVerified,
		BFrames:           doc.BFrames, StartTimeSec: doc.StartTimeSeconds,
		ExtradataSHA: doc.ExtradataSHA, KeyframeTimes: doc.KeyframeTimes,
	}, nil
}

// ensureExactSourceFrameCount requires the exact decoded-frame count produced
// by LibAV's complete source walk. Source certificates must not turn an
// optional container hint into exact evidence.
func (e *videoSourceInspector) ensureExactSourceFrameCount(ctx context.Context, path, sourceSHA string, probe *windowSourceProbe) error {
	if probe == nil || !isLowerSHA256(sourceSHA) {
		return fmt.Errorf("exact source frame count requires a verified source identity")
	}
	if probe.ExactFrameCount > 0 {
		probe.FrameCount = probe.ExactFrameCount
		return nil
	}
	unlock := e.lockWindow("source-frame-count:" + sourceSHA)
	defer unlock()
	e.sourceProbeMu.Lock()
	if cached, ok := e.sourceProbes[sourceSHA]; ok && cached.ExactFrameCount > 0 {
		*probe = cached
		e.sourceProbeMu.Unlock()
		return nil
	}
	e.sourceProbeMu.Unlock()
	if persisted, ok := e.readPersistedSourceProbe(sourceSHA); ok && persisted.ExactFrameCount > 0 {
		*probe = persisted
		e.sourceProbeMu.Lock()
		if e.sourceProbes == nil {
			e.sourceProbes = make(map[string]windowSourceProbe)
		}
		e.sourceProbes[sourceSHA] = persisted
		e.sourceProbeMu.Unlock()
		return nil
	}
	return fmt.Errorf("exact frame count is missing from the complete LibAV source probe")
}

func (e *videoSourceInspector) sourceProbeIndexPath(sourceSHA string) string {
	if e == nil || !isLowerSHA256(sourceSHA) {
		return ""
	}
	return filepath.Join(e.outputRoot, "source-probes", sourceSHA+".json")
}

func (e *videoSourceInspector) readPersistedSourceProbe(sourceSHA string) (windowSourceProbe, bool) {
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

func (e *videoSourceInspector) writePersistedSourceProbe(sourceSHA string, probe windowSourceProbe) {
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
	return probe.CodecName != "" && probe.Width > 0 && probe.Height > 0 &&
		probe.ExactFrameCount > 0 && probe.FrameCount == probe.ExactFrameCount &&
		probe.FPSNum > 0 && probe.FPSDen > 0 && probe.AverageFPSNum > 0 && probe.AverageFPSDen > 0 &&
		samePositiveRatio(probe.FPSNum, probe.FPSDen, probe.AverageFPSNum, probe.AverageFPSDen) &&
		probe.TimeBaseNum > 0 && probe.TimeBaseDen > 0 && probe.StartTimeVerified && len(probe.KeyframeTimes) > 0
}

func samePositiveRatio(aNum, aDen, bNum, bDen int) bool {
	if aNum <= 0 || aDen <= 0 || bNum <= 0 || bDen <= 0 {
		return false
	}
	gcd := func(a, b int) int {
		for b != 0 {
			a, b = b, a%b
		}
		return a
	}
	aGCD, bGCD := gcd(aNum, aDen), gcd(bNum, bDen)
	return aNum/aGCD == bNum/bGCD && aDen/aGCD == bDen/bGCD
}

// Source certificates require complete frame-count and timestamp-origin evidence.
// Never fabricate full-source evidence from a slice.
func sourceCertificateFromProbe(source windowSourceProbe, asset contract.AssetRefV2, profileID, profileRevision string, profile contract.CanonicalVideoProfileV1) *contract.VideoSourceCertificateV1 {
	if source.ExactFrameCount <= 0 || source.StartTimeSec != 0 || !source.StartTimeVerified || source.StartTimeTicks != 0 ||
		!samePositiveRatio(source.AverageFPSNum, source.AverageFPSDen, source.FPSNum, source.FPSDen) {
		return nil
	}
	certificate := &contract.VideoSourceCertificateV1{Version: 1, SHA256: asset.SHA256, SizeBytes: asset.SizeBytes, ProfileID: profileID, PreparationRevision: profileRevision,
		SourceTimestampOriginTicks: source.StartTimeTicks, SourceTimestampOriginVerified: source.StartTimeVerified,
		AverageFPSNum: source.AverageFPSNum, AverageFPSDen: source.AverageFPSDen,
		Evidence: contract.VideoWindowOutputEvidenceV1{Codec: source.CodecName, Profile: source.Profile, Level: source.Level, Width: source.Width, Height: source.Height, PixelFormat: source.PixelFormat, FPSNum: source.FPSNum, FPSDen: source.FPSDen, TimeBaseNum: source.TimeBaseNum, TimeBaseDen: source.TimeBaseDen, BFrames: source.BFrames, FrameCount: source.ExactFrameCount, FirstFrameKeyframe: true, ExtradataSHA256: source.ExtradataSHA}}
	if source.ExactFrameCount > math.MaxInt64/int64(profile.FPSDen)/1_000_000 {
		return nil
	}
	certificate.DurationUS = source.ExactFrameCount * int64(profile.FPSDen) * 1_000_000 / int64(profile.FPSNum)
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
