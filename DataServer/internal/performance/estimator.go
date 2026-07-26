package performance

import (
	"math"
	"strconv"
)

// Workload describes the measurable inputs needed for a placement estimate.
// Durations are milliseconds and sizes are bytes.
type Workload struct {
	WorkloadKey       string
	ExecutorID        string
	ExecutorVersion   int
	WorkerClass       string
	InputDurationMs   float64
	InputBytes        int64
	OutputBytes       int64
	MissingAssetBytes int64
	UploadBytes       int64
	CacheHit          bool
	QueueMs           float64
	BandwidthMbps     float64
	CPUThreads        int
	MemoryBytes       int64
}

// WorkerEconomics describes the optional operator price sheet. Zero prices
// are valid for owned hardware and result in a zero monetary estimate.
type WorkerEconomics struct {
	CPUCoreHourEUR   float64
	MemoryGiBHourEUR float64
	NetworkGBEUR     float64
	StorageGBHourEUR float64
	WorkerHourEUR    float64
}

// Estimate is the explainable output consumed by placement and admission.
type Estimate struct {
	ComputeMs       float64 `json:"compute_ms"`
	TransferMs      float64 `json:"transfer_ms"`
	QueueMs         float64 `json:"queue_ms"`
	UploadMs        float64 `json:"upload_ms"`
	FinishMs        float64 `json:"finish_ms"`
	MemoryPeakBytes int64   `json:"memory_peak_bytes"`
	TempBytes       int64   `json:"temp_bytes"`
	Confidence      float64 `json:"confidence"`
	SampleCount     int     `json:"sample_count"`
	CostEUR         float64 `json:"cost_eur"`
}

// Estimator is the single executor-version keyed estimation contract.
type Estimator interface {
	Estimate(workload Workload, baseline *Baseline, economics WorkerEconomics) Estimate
}

// Registry holds estimators by executor@version. Unknown executors use the
// conservative baseline fallback instead of inventing executor-specific logic.
type Registry struct{ estimators map[string]Estimator }

func NewRegistry() *Registry { return &Registry{estimators: make(map[string]Estimator)} }

func (r *Registry) Register(executorID string, version int, e Estimator) {
	if r == nil || e == nil || executorID == "" {
		return
	}
	if r.estimators == nil {
		r.estimators = make(map[string]Estimator)
	}
	r.estimators[key(executorID, version)] = e
}

func (r *Registry) Estimate(workload Workload, baseline *Baseline, economics WorkerEconomics) Estimate {
	if r != nil {
		if e := r.estimators[key(workload.ExecutorID, workload.ExecutorVersion)]; e != nil {
			return e.Estimate(workload, baseline, economics)
		}
	}
	return ConservativeEstimator{}.Estimate(workload, baseline, economics)
}

// ConservativeEstimator uses p95 when available and adds explicit transfer,
// queue and upload costs. This keeps unknown workloads safe and explainable.
type ConservativeEstimator struct{}

func (ConservativeEstimator) Estimate(w Workload, b *Baseline, economics WorkerEconomics) Estimate {
	compute := w.InputDurationMs
	confidence := 0.15
	samples := 0
	if b != nil {
		samples = b.SampleCount
		if b.P95WallMs > 0 {
			compute = b.P95WallMs
		} else if b.P50WallMs > 0 {
			compute = b.P50WallMs
		}
		confidence = confidenceFromSamples(b.SampleCount, b.ErrorRate)
	}
	transfer := transferMs(w.MissingAssetBytes, w.BandwidthMbps)
	upload := transferMs(w.UploadBytes, w.BandwidthMbps)
	if w.CacheHit {
		transfer = 0
	}
	total := maxPositive(w.QueueMs) + compute + transfer + upload
	workerHours := total / 3600000
	cpuCores := float64(maxInt(w.CPUThreads, 1))
	memoryGiB := float64(w.MemoryBytes) / (1 << 30)
	networkGB := float64(w.MissingAssetBytes+w.UploadBytes) / (1 << 30)
	cost := workerHours*economics.WorkerHourEUR + workerHours*cpuCores*economics.CPUCoreHourEUR +
		workerHours*memoryGiB*economics.MemoryGiBHourEUR + networkGB*economics.NetworkGBEUR
	return Estimate{ComputeMs: compute, TransferMs: transfer, QueueMs: maxPositive(w.QueueMs), UploadMs: upload,
		FinishMs: total, MemoryPeakBytes: w.MemoryBytes, TempBytes: maxInt64(w.InputBytes, w.OutputBytes),
		Confidence: confidence, SampleCount: samples, CostEUR: cost}
}

func confidenceFromSamples(samples int, errorRate float64) float64 {
	if samples <= 0 {
		return 0.15
	}
	confidence := 1 - math.Exp(-float64(samples)/20)
	confidence *= 1 - minPositive(errorRate, 1)
	if confidence < 0.05 {
		return 0.05
	}
	if confidence > 0.99 {
		return 0.99
	}
	return confidence
}

func transferMs(bytes int64, bandwidthMbps float64) float64 {
	if bytes <= 0 || bandwidthMbps <= 0 {
		return 0
	}
	return float64(bytes) * 8 / (bandwidthMbps * 1_000_000) * 1000
}
func key(id string, version int) string { return id + "@" + itoa(version) }
func itoa(v int) string                 { return strconv.Itoa(v) }
func maxPositive(v float64) float64 {
	if v > 0 {
		return v
	}
	return 0
}
func minPositive(v, max float64) float64 {
	if v < 0 {
		return 0
	}
	if v > max {
		return max
	}
	return v
}
func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
