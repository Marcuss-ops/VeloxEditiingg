package telemetry

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestReadCgroupUsageV2(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "cpu.stat"), []byte("usage_usec 123456\nuser_usec 100000\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "memory.current"), []byte("4096\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "memory.peak"), []byte("8192\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "io.stat"), []byte("8:0 rbytes=100 wbytes=250 rios=1 wios=2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	got := readCgroupUsage(root)
	if got.CPUUsec != 123456 || got.MemoryCurrent != 4096 || got.MemoryPeak != 8192 || got.DiskReadBytes != 100 || got.DiskWriteBytes != 250 {
		t.Fatalf("unexpected cgroup usage: %+v", got)
	}
}

func TestAttemptTelemetryStopKeepsRawFactsTyped(t *testing.T) {
	result := AttemptTelemetry{
		Metrics:  RawExecutionMetrics{CpuTimeMs: 1200, PeakRssBytes: 4096, TelemetryCPUSource: "cgroup_v2"},
		Coverage: map[string]bool{"cpu": true, "memory": true, "disk": true, "network": true, "gpu": false},
		Complete: true,
	}
	if result.Metrics.CpuTimeMs != 1200 || result.Metrics.PeakRssBytes != 4096 || !result.Complete {
		t.Fatalf("raw attempt metrics not retained: %+v", result)
	}
	if !result.Coverage["cpu"] || result.Coverage["gpu"] {
		t.Fatalf("unexpected coverage: %#v", result.Coverage)
	}
}

func TestAttemptSessionWithoutSamplerIsExplicitlyIncomplete(t *testing.T) {
	s := NewAttemptTelemetrySession(nil)
	s.Start(context.Background())
	result := s.Stop(context.Background())
	if result.Complete {
		t.Fatalf("nil sampler session must not certify telemetry: %+v", result)
	}
	if result.Metrics.TelemetryCPUSource != "cgroup_v2" && result.Metrics.TelemetryCPUSource != "proc" {
		t.Fatalf("cpu source = %q, want cgroup_v2 or proc", result.Metrics.TelemetryCPUSource)
	}
}
