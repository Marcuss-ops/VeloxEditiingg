package metrics

import (
	"fmt"
	"strings"
)

// RenderCohortInput contains normalized workload dimensions. ExecutorVersion
// is deliberately excluded from the base key so versions can be compared.
type RenderCohortInput struct {
	ExecutorID       string
	ExecutorVersion  int
	WorkerClass      string
	ResolutionWidth  int
	ResolutionHeight int
	FPS              float64
	OutputDuration   float64
	SceneCount       int
	SegmentCount     int
	AudioTracks      int
	SubtitleCount    int
	Codec            string
	Preset           string
	CacheMode        string
	TemplateID       string
	ConfigHash       string
}

// BuildRenderCohortKeys returns a versioned key and a version-neutral key.
func BuildRenderCohortKeys(in RenderCohortInput) (string, string) {
	base := strings.Join([]string{
		"executor=" + safeCohortPart(in.ExecutorID),
		"worker_class=" + safeCohortPart(in.WorkerClass),
		"resolution=" + resolutionBucket(in.ResolutionWidth, in.ResolutionHeight),
		"fps=" + fpsBucket(in.FPS),
		"duration=" + durationBucket(in.OutputDuration),
		"scenes=" + countBucket(in.SceneCount),
		"segments=" + countBucket(in.SegmentCount),
		"audio=" + countBucket(in.AudioTracks),
		"subtitles=" + countBucket(in.SubtitleCount),
		"codec=" + safeCohortPart(in.Codec),
		"preset=" + safeCohortPart(in.Preset),
		"cache=" + safeCohortPart(in.CacheMode),
		"template=" + safeCohortPart(in.TemplateID),
		"config=" + safeCohortPart(in.ConfigHash),
	}, "|")
	return fmt.Sprintf("%s|executor_version=%d", base, in.ExecutorVersion), base
}

func safeCohortPart(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.NewReplacer("|", "%7c", "=", "%3d", " ", "_").Replace(value)
	if value == "" {
		return "unknown"
	}
	return value
}

func resolutionBucket(width, height int) string {
	if width <= 0 || height <= 0 {
		return "unknown"
	}
	return fmt.Sprintf("%dx%d", width, height)
}

func fpsBucket(fps float64) string {
	switch {
	case fps <= 0:
		return "unknown"
	case fps < 24:
		return "lt24"
	case fps <= 30:
		return "24_30"
	case fps <= 60:
		return "30_60"
	default:
		return "gt60"
	}
}

func durationBucket(seconds float64) string {
	switch {
	case seconds <= 0:
		return "unknown"
	case seconds < 15:
		return "lt15s"
	case seconds < 60:
		return "15_60s"
	case seconds < 300:
		return "1_5m"
	case seconds < 900:
		return "5_15m"
	default:
		return "gt15m"
	}
}

func countBucket(value int) string {
	switch {
	case value <= 0:
		return "0"
	case value <= 5:
		return "1_5"
	case value <= 20:
		return "6_20"
	case value <= 50:
		return "21_50"
	default:
		return "gt50"
	}
}

// RenderPerformanceDaily is one compact day/cohort/phase observation.
type RenderPerformanceDaily struct {
	Day                      string
	CohortKey                string
	CohortBaseKey            string
	Phase                    string
	ExecutorID               string
	ExecutorVersion          int
	WorkerID                 string
	WorkerClass              string
	GitSHA                   string
	EngineVersion            string
	FFmpegVersion            string
	DockerImageDigest        string
	ConfigHash               string
	Attempts                 int
	Succeeded                int
	Failed                   int
	PhaseMSTotal             float64
	PhaseMSAvg               float64
	PhaseMSP25               float64
	PhaseMSP50               float64
	PhaseMSP95               float64
	PhaseMSP99               float64
	BaselineP25MS            float64
	RecoverableMSTotal       float64
	OutputSeconds            float64
	WallMSTotal              float64
	CPUMSTotal               float64
	DownloadMSTotal          float64
	DecodeMSTotal            float64
	CompositeMSTotal         float64
	EncodeMSTotal            float64
	UploadMSTotal            float64
	OutputBytesTotal         int64
	TempBytesTotal           int64
	WastedCPUMSTotal         int64
	WastedDownloadBytesTotal int64
	RenderFactorAvg          float64
	CalculatedAt             string
}

type RenderPerformanceVersionRegression struct {
	CohortBaseKey   string
	Phase           string
	WorkerID        string
	WorkerClass     string
	ExecutorVersion int
	Day             string
	PhaseMSP25      float64
	BaselineP25MS   float64
	RecoverableMS   float64
	Attempts        int
	Succeeded       int
}

type performanceObservation struct {
	day, attemptID, status, workerID string
	input                            RenderCohortInput
	gitSHA, engineVersion            string
	ffmpegVersion, dockerDigest      string
	phase                            string
	durationMS                       float64
	wallMS, cpuMS, outputSeconds     float64
	outputBytes, tempBytes           int64
	wastedCPU, wastedDownload        int64
}

type performanceGroup struct {
	day, cohortKey, cohortBaseKey, phase, workerID string
	input                                          RenderCohortInput
	gitSHA, engineVersion, ffmpegVersion           string
	dockerDigest                                   string
	values, baselineValues                         []float64
	statuses                                       map[string]string
	attemptTotals                                  map[string]performanceAttemptTotals
}

type performanceAttemptTotals struct {
	wallMS, cpuMS, outputSeconds float64
	outputBytes, tempBytes       int64
	wastedCPU, wastedDownload    int64
}
