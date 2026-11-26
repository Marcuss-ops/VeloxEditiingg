package executors

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"velox-shared/contract"
)

// videoSourceInspector contains the probe cache and source-fact helpers used
// by video.source.certify.v1. It does not implement or publish preparation
// fragments.
type videoSourceInspector struct {
	outputRoot    string
	profile       contract.CanonicalVideoProfileV1
	quality       contract.PreparationQualityPolicy
	windowLockMu  sync.Mutex
	windowLocks   map[string]*keyedWindowLock
	sourceProbeMu sync.Mutex
	sourceProbes  map[string]windowSourceProbe
	sourceProbeFn func(context.Context, string) (windowSourceProbe, error)
}

type windowSourceProbe struct {
	Chunks            *contract.VideoSourceChunksV1 `json:"chunks,omitempty"`
	FrameCount        int64
	ExactFrameCount   int64 `json:"exact_frame_count,omitempty"`
	CodecName         string
	Profile           string
	Level             int
	Width             int
	Height            int
	PixelFormat       string
	FPSNum            int
	FPSDen            int
	AverageFPSNum     int `json:"average_fps_num,omitempty"`
	AverageFPSDen     int `json:"average_fps_den,omitempty"`
	TimeBaseNum       int
	TimeBaseDen       int
	StartTimeTicks    int64 `json:"start_time_ticks,omitempty"`
	StartTimeVerified bool  `json:"start_time_verified,omitempty"`
	BFrames           int
	StartTimeSec      float64
	ExtradataSHA      string
	KeyframeTimes     []float64
}

// windowSourceProbeIndex binds persisted facts to the immutable source bytes
// and probe schema that produced them.
type windowSourceProbeIndex struct {
	Version   int               `json:"version"`
	SourceSHA string            `json:"source_sha256"`
	Probe     windowSourceProbe `json:"probe"`
}

const windowSourceProbeIndexVersion = 5

type keyedWindowLock struct {
	mu   sync.Mutex
	refs int
}

func newVideoSourceInspector(outputRoot string) *videoSourceInspector {
	if strings.TrimSpace(outputRoot) == "" {
		outputRoot = filepath.Join(os.TempDir(), "velox", "source-certification")
	}
	return &videoSourceInspector{
		outputRoot: outputRoot,
		profile:    contract.CanonicalVideoProfileV1Default,
		quality:    contract.CanonicalPreparationQualityPolicyDefault,
	}
}

func (e *videoSourceInspector) lockWindow(key string) func() {
	e.windowLockMu.Lock()
	if e.windowLocks == nil {
		e.windowLocks = make(map[string]*keyedWindowLock)
	}
	lock := e.windowLocks[key]
	if lock == nil {
		lock = &keyedWindowLock{}
		e.windowLocks[key] = lock
	}
	lock.refs++
	e.windowLockMu.Unlock()
	lock.mu.Lock()
	return func() {
		lock.mu.Unlock()
		e.windowLockMu.Lock()
		lock.refs--
		if lock.refs == 0 {
			delete(e.windowLocks, key)
		}
		e.windowLockMu.Unlock()
	}
}

func parseWindowRatio(raw string) (int, int) {
	parts := strings.SplitN(strings.TrimSpace(raw), "/", 2)
	if len(parts) != 2 {
		return 0, 0
	}
	num, errNum := strconv.Atoi(parts[0])
	den, errDen := strconv.Atoi(parts[1])
	if errNum != nil || errDen != nil || num <= 0 || den <= 0 {
		return 0, 0
	}
	return num, den
}

func canonicalLevelNumber(level string) int {
	parts := strings.Split(strings.TrimSpace(level), ".")
	if len(parts) != 2 {
		return 0
	}
	major, errMajor := strconv.Atoi(parts[0])
	minor, errMinor := strconv.Atoi(parts[1])
	if errMajor != nil || errMinor != nil {
		return 0
	}
	return major*10 + minor
}

func isLowerSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, c := range value {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
