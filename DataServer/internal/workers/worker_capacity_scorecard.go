package workers

import (
	"fmt"
	"math"
	"strings"
)

// CapacityScorecard is the per-worker resource capacity recommendation.
// It answers: "How many concurrent render/prefetch/publisher jobs can this
// worker handle without degrading throughput?"
//
// For each resource dimension, safe_slots = usable_resource / p95_per_job.
// The overall capacity is min(ram_slots, cpu_slots, disk_slots, network_slots).
// LimitingResource identifies the bottleneck.
type CapacityScorecard struct {
	WorkerID string `json:"worker_id"`

	// Per-resource slot recommendations.
	RenderSlots    int `json:"render_slots"`
	PrefetchSlots  int `json:"prefetch_slots"`
	PublisherSlots int `json:"publisher_slots"`

	// Raw slot estimates per resource dimension (for diagnostics).
	RAMSlots     int `json:"ram_slots"`
	CPUSlots     int `json:"cpu_slots"`
	DiskSlots    int `json:"disk_slots"`
	NetworkSlots int `json:"network_slots"`

	// The bottleneck resource.
	LimitingResource string `json:"limiting_resource"`

	// Resource inputs (for the scorecard display).
	TotalRAMBytes     int64   `json:"total_ram_bytes"`
	AvailableRAMBytes int64   `json:"available_ram_bytes"`
	EffectiveCPUCores int32   `json:"effective_cpu_cores"`
	DiskReadMbps      float64 `json:"disk_read_mbps"`
	DiskWriteMbps     float64 `json:"disk_write_mbps"`
	DownloadMbps      float64 `json:"download_mbps"`
	UploadMbps        float64 `json:"upload_mbps"`

	// Per-job resource estimates (p95 or average).
	RAMPerJobBytes       int64   `json:"ram_per_job_bytes"`
	CPUCoresPerJob       float64 `json:"cpu_cores_per_job"`
	DiskMBpsPerJob       float64 `json:"disk_mbps_per_job"`
	NetworkMbpsPerJob    float64 `json:"network_mbps_per_job"`
	RenderWallMsPerJob   int64   `json:"render_wall_ms_per_job"`
	PrefetchWallMsPerJob int64   `json:"prefetch_wall_ms_per_job"`
	PublishWallMsPerJob  int64   `json:"publish_wall_ms_per_job"`

	// Saturation thresholds (configurable, sensible defaults).
	RAMSafetyFraction     float64 `json:"-"` // default 0.75 (75% of usable RAM)
	DiskSafetyFraction    float64 `json:"-"` // default 0.75 (75% of NVMe throughput)
	NetworkSafetyFraction float64 `json:"-"` // default 0.80 (80% of bandwidth)
}

// ScorecardInput holds the worker's current resource snapshot and per-job
// cost estimates needed to compute the scorecard.
type ScorecardInput struct {
	WorkerID string

	// Worker-level resources.
	TotalRAMBytes     int64
	AvailableRAMBytes int64
	EffectiveCPUCores int32
	DiskReadMbps      float64
	DiskWriteMbps     float64
	DownloadMbps      float64
	UploadMbps        float64

	// Per-job resource costs (from historical job metrics or benchmarks).
	RAMPerJobBytes       int64
	CPUCoresPerJob       float64
	DiskMBpsPerJob       float64
	NetworkMbpsPerJob    float64
	RenderWallMsPerJob   int64
	PrefetchWallMsPerJob int64
	PublishWallMsPerJob  int64
}

// DefaultScorecardThresholds are the default saturation thresholds.
var DefaultScorecardThresholds = struct {
	RAMSafety     float64
	DiskSafety    float64
	NetworkSafety float64
}{
	RAMSafety:     0.75,
	DiskSafety:    0.75,
	NetworkSafety: 0.80,
}

// ComputeCapacityScorecard calculates the recommended slot allocation for a
// worker given its resource snapshot and per-job cost estimates.
//
// The formula per resource dimension:
//
//	ram_slots     = (available_ram * ram_safety) / ram_per_job
//	cpu_slots     = effective_cores / cpu_per_job
//	disk_slots    = (max(disk_read, disk_write) * disk_safety) / disk_per_job
//	network_slots = (max(download, upload) * network_safety) / network_per_job
//
// The overall safe_slots = min(all non-zero dimensions).
func ComputeCapacityScorecard(input ScorecardInput) CapacityScorecard {
	sc := CapacityScorecard{
		WorkerID:             input.WorkerID,
		TotalRAMBytes:        input.TotalRAMBytes,
		AvailableRAMBytes:    input.AvailableRAMBytes,
		EffectiveCPUCores:    input.EffectiveCPUCores,
		DiskReadMbps:         input.DiskReadMbps,
		DiskWriteMbps:        input.DiskWriteMbps,
		DownloadMbps:         input.DownloadMbps,
		UploadMbps:           input.UploadMbps,
		RAMPerJobBytes:       input.RAMPerJobBytes,
		CPUCoresPerJob:       input.CPUCoresPerJob,
		DiskMBpsPerJob:       input.DiskMBpsPerJob,
		NetworkMbpsPerJob:    input.NetworkMbpsPerJob,
		RenderWallMsPerJob:   input.RenderWallMsPerJob,
		PrefetchWallMsPerJob: input.PrefetchWallMsPerJob,
		PublishWallMsPerJob:  input.PublishWallMsPerJob,

		RAMSafetyFraction:     DefaultScorecardThresholds.RAMSafety,
		DiskSafetyFraction:    DefaultScorecardThresholds.DiskSafety,
		NetworkSafetyFraction: DefaultScorecardThresholds.NetworkSafety,
	}

	// ── RAM slots ────────────────────────────────────────────────
	if input.RAMPerJobBytes > 0 {
		usableRAM := float64(input.AvailableRAMBytes) * sc.RAMSafetyFraction
		sc.RAMSlots = int(math.Floor(usableRAM / float64(input.RAMPerJobBytes)))
	}

	// ── CPU slots ────────────────────────────────────────────────
	if input.CPUCoresPerJob > 0 && input.EffectiveCPUCores > 0 {
		sc.CPUSlots = int(math.Floor(float64(input.EffectiveCPUCores) / input.CPUCoresPerJob))
	}

	// ── Disk slots ───────────────────────────────────────────────
	diskThroughput := math.Max(input.DiskReadMbps, input.DiskWriteMbps)
	if input.DiskMBpsPerJob > 0 && diskThroughput > 0 {
		sc.DiskSlots = int(math.Floor((diskThroughput * sc.DiskSafetyFraction) / input.DiskMBpsPerJob))
	}

	// ── Network slots ────────────────────────────────────────────
	networkBandwidth := math.Max(input.DownloadMbps, input.UploadMbps)
	if input.NetworkMbpsPerJob > 0 && networkBandwidth > 0 {
		sc.NetworkSlots = int(math.Floor((networkBandwidth * sc.NetworkSafetyFraction) / input.NetworkMbpsPerJob))
	}

	// ── Determine limiting resource and safe_slots ───────────────
	type dim struct {
		slots int
		name  string
	}
	dims := []dim{
		{sc.RAMSlots, "RAM"},
		{sc.CPUSlots, "CPU"},
		{sc.DiskSlots, "NVME"},
		{sc.NetworkSlots, "NETWORK"},
	}

	safeSlots := math.MaxInt32
	limiting := "UNKNOWN"
	for _, d := range dims {
		if d.slots > 0 && d.slots < safeSlots {
			safeSlots = d.slots
			limiting = d.name
		}
	}
	if safeSlots == math.MaxInt32 || safeSlots < 1 {
		safeSlots = 1
		if limiting == "UNKNOWN" {
			limiting = "INSUFFICIENT_DATA"
		}
	}

	sc.LimitingResource = limiting

	// ── Split into per-phase slot recommendations ────────────────
	// Render is the primary workload: gets safe_slots.
	sc.RenderSlots = safeSlots

	// Prefetch is I/O-bound and typically lighter on CPU/RAM.
	// It can often run one more concurrent job than render.
	sc.PrefetchSlots = safeSlots + 1

	// Publisher is network-heavy (upload). It should be limited
	// by the network bottleneck, which is often tighter.
	sc.PublisherSlots = safeSlots
	if limiting == "NETWORK" && sc.NetworkSlots > 0 && sc.NetworkSlots < safeSlots {
		sc.PublisherSlots = sc.NetworkSlots
	}

	return sc
}

// NormalizeResourceSample converts a heartbeat resource map into a
// ScorecardInput by extracting the canonical keys and combining them
// with per-job cost estimates.
func NormalizeResourceSample(metrics map[string]interface{}, workerID string, jobCost ScorecardInput) ScorecardInput {
	input := jobCost
	input.WorkerID = workerID

	if v, ok := scorecardInt64FromMap(metrics, "total_ram_bytes"); ok {
		input.TotalRAMBytes = v
	}
	if v, ok := scorecardInt64FromMap(metrics, "memory_available_bytes"); ok {
		input.AvailableRAMBytes = v
	}
	if v, ok := scorecardFloat64FromMap(metrics, "effective_cpu_cores"); ok {
		input.EffectiveCPUCores = int32(v)
	}
	if v, ok := scorecardFloat64FromMap(metrics, "disk_read_mbps"); ok {
		input.DiskReadMbps = v
	}
	if v, ok := scorecardFloat64FromMap(metrics, "disk_write_mbps"); ok {
		input.DiskWriteMbps = v
	}
	if v, ok := scorecardFloat64FromMap(metrics, "download_mbps"); ok {
		input.DownloadMbps = v
	}
	if v, ok := scorecardFloat64FromMap(metrics, "upload_mbps"); ok {
		input.UploadMbps = v
	}

	return input
}

func scorecardInt64FromMap(m map[string]interface{}, key string) (int64, bool) {
	if m == nil {
		return 0, false
	}
	v, ok := m[key]
	if !ok {
		return 0, false
	}
	switch n := v.(type) {
	case int64:
		return n, true
	case int32:
		return int64(n), true
	case float64:
		return int64(n), true
	case float32:
		return int64(n), true
	case int:
		return int64(n), true
	}
	return 0, false
}

func scorecardFloat64FromMap(m map[string]interface{}, key string) (float64, bool) {
	if m == nil {
		return 0, false
	}
	v, ok := m[key]
	if !ok {
		return 0, false
	}
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int64:
		return float64(n), true
	case int32:
		return float64(n), true
	case int:
		return float64(n), true
	}
	return 0, false
}

// String returns a human-readable summary of the scorecard.
func (sc CapacityScorecard) String() string {
	var b strings.Builder
	b.WriteString("Worker Capacity Scorecard\n")
	b.WriteString("========================\n")
	fmt.Fprintf(&b, "Worker: %s\n\n", sc.WorkerID)

	b.WriteString("RAM:\n")
	fmt.Fprintf(&b, "  total     %s\n", formatBytes(sc.TotalRAMBytes))
	fmt.Fprintf(&b, "  available %s\n", formatBytes(sc.AvailableRAMBytes))
	fmt.Fprintf(&b, "  per-job   %s\n", formatBytes(sc.RAMPerJobBytes))

	b.WriteString("\nCPU:\n")
	fmt.Fprintf(&b, "  effective cores %d\n", sc.EffectiveCPUCores)
	fmt.Fprintf(&b, "  per-job cores   %.1f\n", sc.CPUCoresPerJob)

	b.WriteString("\nDisk:\n")
	fmt.Fprintf(&b, "  read  %.1f Mbit/s\n", sc.DiskReadMbps)
	fmt.Fprintf(&b, "  write %.1f Mbit/s\n", sc.DiskWriteMbps)
	fmt.Fprintf(&b, "  per-job %.1f Mbit/s\n", sc.DiskMBpsPerJob)

	b.WriteString("\nNetwork:\n")
	fmt.Fprintf(&b, "  download %.1f Mbit/s\n", sc.DownloadMbps)
	fmt.Fprintf(&b, "  upload   %.1f Mbit/s\n", sc.UploadMbps)
	fmt.Fprintf(&b, "  per-job  %.1f Mbit/s\n", sc.NetworkMbpsPerJob)

	b.WriteString("\nRecommended:\n")
	fmt.Fprintf(&b, "  render_slots     %d\n", sc.RenderSlots)
	fmt.Fprintf(&b, "  prefetch_slots   %d\n", sc.PrefetchSlots)
	fmt.Fprintf(&b, "  publisher_slots  %d\n", sc.PublisherSlots)

	b.WriteString("\nSlot estimates:\n")
	fmt.Fprintf(&b, "  RAM     %d\n", sc.RAMSlots)
	fmt.Fprintf(&b, "  CPU     %d\n", sc.CPUSlots)
	fmt.Fprintf(&b, "  NVME    %d\n", sc.DiskSlots)
	fmt.Fprintf(&b, "  NETWORK %d\n", sc.NetworkSlots)

	fmt.Fprintf(&b, "\nLimiting resource: %s\n", sc.LimitingResource)

	return b.String()
}

func formatBytes(b int64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.1f GB", float64(b)/(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1f MB", float64(b)/(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1f KB", float64(b)/(1<<10))
	default:
		return fmt.Sprintf("%d B", b)
	}
}
