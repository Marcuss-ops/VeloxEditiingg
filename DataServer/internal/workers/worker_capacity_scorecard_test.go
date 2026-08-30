package workers

import (
	"testing"
)

func TestComputeCapacityScorecard_NetworkLimited(t *testing.T) {
	input := ScorecardInput{
		WorkerID:          "worker-1",
		TotalRAMBytes:     16 * 1024 * 1024 * 1024, // 16 GB
		AvailableRAMBytes: 12 * 1024 * 1024 * 1024, // 12 GB available
		EffectiveCPUCores: 8,
		DiskReadMbps:      1500, // 1.5 Gbit/s NVMe
		DiskWriteMbps:     1200,
		DownloadMbps:      100, // 100 Mbit/s network
		UploadMbps:        90,
		RAMPerJobBytes:    500 * 1024 * 1024, // 500 MB per job
		CPUCoresPerJob:    0.8,
		DiskMBpsPerJob:    100,
		NetworkMbpsPerJob: 50, // 50 Mbit/s per job (upload-heavy)
	}

	sc := ComputeCapacityScorecard(input)

	// RAM: 12GB * 0.75 / 500MB = 18
	if sc.RAMSlots != 18 {
		t.Errorf("RAMSlots = %d, want 18", sc.RAMSlots)
	}
	// CPU: 8 / 0.8 = 10
	if sc.CPUSlots != 10 {
		t.Errorf("CPUSlots = %d, want 10", sc.CPUSlots)
	}
	// Disk: 1500 * 0.75 / 100 = 11.25 → 11
	if sc.DiskSlots != 11 {
		t.Errorf("DiskSlots = %d, want 11", sc.DiskSlots)
	}
	// Network: 100 * 0.80 / 50 = 1.6 → 1
	if sc.NetworkSlots != 1 {
		t.Errorf("NetworkSlots = %d, want 1", sc.NetworkSlots)
	}

	// Limiting resource is NETWORK
	if sc.LimitingResource != "NETWORK" {
		t.Errorf("LimitingResource = %q, want NETWORK", sc.LimitingResource)
	}
	// Safe slots = min(18, 10, 11, 1) = 1
	if sc.RenderSlots != 1 {
		t.Errorf("RenderSlots = %d, want 1", sc.RenderSlots)
	}
	// Publisher limited by network
	if sc.PublisherSlots != 1 {
		t.Errorf("PublisherSlots = %d, want 1", sc.PublisherSlots)
	}
	// Prefetch = safe + 1
	if sc.PrefetchSlots != 2 {
		t.Errorf("PrefetchSlots = %d, want 2", sc.PrefetchSlots)
	}
}

func TestComputeCapacityScorecard_RAMLimited(t *testing.T) {
	input := ScorecardInput{
		WorkerID:          "worker-2",
		TotalRAMBytes:     8 * 1024 * 1024 * 1024, // 8 GB
		AvailableRAMBytes: 6 * 1024 * 1024 * 1024, // 6 GB
		EffectiveCPUCores: 4,
		DiskReadMbps:      1500,
		DiskWriteMbps:     1200,
		DownloadMbps:      1000, // fast network
		UploadMbps:        900,
		RAMPerJobBytes:    2 * 1024 * 1024 * 1024, // 2 GB per job (heavy)
		CPUCoresPerJob:    1.0,
		DiskMBpsPerJob:    50,
		NetworkMbpsPerJob: 100,
	}

	sc := ComputeCapacityScorecard(input)

	// RAM: 6GB * 0.75 / 2GB = 2.25 → 2
	if sc.RAMSlots != 2 {
		t.Errorf("RAMSlots = %d, want 2", sc.RAMSlots)
	}
	// CPU: 4 / 1 = 4
	if sc.CPUSlots != 4 {
		t.Errorf("CPUSlots = %d, want 4", sc.CPUSlots)
	}

	if sc.LimitingResource != "RAM" {
		t.Errorf("LimitingResource = %q, want RAM", sc.LimitingResource)
	}
	if sc.RenderSlots != 2 {
		t.Errorf("RenderSlots = %d, want 2", sc.RenderSlots)
	}
}

func TestComputeCapacityScorecard_CPULimited(t *testing.T) {
	input := ScorecardInput{
		WorkerID:          "worker-3",
		TotalRAMBytes:     32 * 1024 * 1024 * 1024,
		AvailableRAMBytes: 28 * 1024 * 1024 * 1024,
		EffectiveCPUCores: 2,
		DiskReadMbps:      2000,
		DiskWriteMbps:     1800,
		DownloadMbps:      1000,
		UploadMbps:        900,
		RAMPerJobBytes:    1 * 1024 * 1024 * 1024,
		CPUCoresPerJob:    1.5, // CPU-heavy job
		DiskMBpsPerJob:    50,
		NetworkMbpsPerJob: 50,
	}

	sc := ComputeCapacityScorecard(input)

	// CPU: 2 / 1.5 = 1.33 → 1
	if sc.CPUSlots != 1 {
		t.Errorf("CPUSlots = %d, want 1", sc.CPUSlots)
	}

	if sc.LimitingResource != "CPU" {
		t.Errorf("LimitingResource = %q, want CPU", sc.LimitingResource)
	}
	if sc.RenderSlots != 1 {
		t.Errorf("RenderSlots = %d, want 1", sc.RenderSlots)
	}
}

func TestComputeCapacityScorecard_NoData(t *testing.T) {
	input := ScorecardInput{
		WorkerID: "worker-empty",
	}

	sc := ComputeCapacityScorecard(input)

	if sc.LimitingResource != "INSUFFICIENT_DATA" {
		t.Errorf("LimitingResource = %q, want INSUFFICIENT_DATA", sc.LimitingResource)
	}
	if sc.RenderSlots != 1 {
		t.Errorf("RenderSlots = %d, want 1 (minimum)", sc.RenderSlots)
	}
}

func TestComputeCapacityScorecard_DiskLimited(t *testing.T) {
	input := ScorecardInput{
		WorkerID:          "worker-4",
		TotalRAMBytes:     16 * 1024 * 1024 * 1024,
		AvailableRAMBytes: 12 * 1024 * 1024 * 1024,
		EffectiveCPUCores: 8,
		DiskReadMbps:      500, // slow disk
		DiskWriteMbps:     400,
		DownloadMbps:      1000,
		UploadMbps:        900,
		RAMPerJobBytes:    500 * 1024 * 1024,
		CPUCoresPerJob:    0.5,
		DiskMBpsPerJob:    200, // disk-heavy job
		NetworkMbpsPerJob: 10,
	}

	sc := ComputeCapacityScorecard(input)

	// Disk: 500 * 0.75 / 200 = 1.875 → 1
	if sc.DiskSlots != 1 {
		t.Errorf("DiskSlots = %d, want 1", sc.DiskSlots)
	}

	if sc.LimitingResource != "NVME" {
		t.Errorf("LimitingResource = %q, want NVME", sc.LimitingResource)
	}
	if sc.RenderSlots != 1 {
		t.Errorf("RenderSlots = %d, want 1", sc.RenderSlots)
	}
}

func TestNormalizeResourceSample(t *testing.T) {
	metrics := map[string]interface{}{
		"total_ram_bytes":        int64(16 * 1024 * 1024 * 1024),
		"memory_available_bytes": int64(12 * 1024 * 1024 * 1024),
		"effective_cpu_cores":    float64(8),
		"disk_read_mbps":         float64(1500),
		"disk_write_mbps":        float64(1200),
		"download_mbps":          float64(100),
		"upload_mbps":            float64(90),
	}

	jobCost := ScorecardInput{
		RAMPerJobBytes:    500 * 1024 * 1024,
		CPUCoresPerJob:    0.8,
		DiskMBpsPerJob:    100,
		NetworkMbpsPerJob: 50,
	}

	input := NormalizeResourceSample(metrics, "worker-test", jobCost)

	if input.WorkerID != "worker-test" {
		t.Errorf("WorkerID = %q, want worker-test", input.WorkerID)
	}
	if input.TotalRAMBytes != 16*1024*1024*1024 {
		t.Errorf("TotalRAMBytes = %d, want %d", input.TotalRAMBytes, 16*1024*1024*1024)
	}
	if input.AvailableRAMBytes != 12*1024*1024*1024 {
		t.Errorf("AvailableRAMBytes = %d, want %d", input.AvailableRAMBytes, 12*1024*1024*1024)
	}
	if input.EffectiveCPUCores != 8 {
		t.Errorf("EffectiveCPUCores = %d, want 8", input.EffectiveCPUCores)
	}
	if input.DiskReadMbps != 1500 {
		t.Errorf("DiskReadMbps = %f, want 1500", input.DiskReadMbps)
	}

	// Verify job cost is preserved
	if input.RAMPerJobBytes != 500*1024*1024 {
		t.Errorf("RAMPerJobBytes = %d, want %d", input.RAMPerJobBytes, 500*1024*1024)
	}
}

func TestCapacityScorecard_String(t *testing.T) {
	sc := CapacityScorecard{
		WorkerID:          "worker-1",
		TotalRAMBytes:     16 * 1024 * 1024 * 1024,
		AvailableRAMBytes: 12 * 1024 * 1024 * 1024,
		EffectiveCPUCores: 8,
		DiskReadMbps:      1500,
		DiskWriteMbps:     1200,
		DownloadMbps:      100,
		UploadMbps:        90,
		RenderSlots:       3,
		PrefetchSlots:     4,
		PublisherSlots:    1,
		RAMSlots:          18,
		CPUSlots:          10,
		DiskSlots:         11,
		NetworkSlots:      1,
		LimitingResource:  "NETWORK",
	}

	s := sc.String()
	if s == "" {
		t.Error("String() returned empty")
	}
	if !contains(s, "render_slots") {
		t.Error("String() missing render_slots")
	}
	if !contains(s, "NETWORK") {
		t.Error("String() missing NETWORK")
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(s) > 0 && containsSubstr(s, sub))
}

func containsSubstr(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
