package taskrunner

import (
	"encoding/json"
	"testing"

	"velox-worker-agent/internal/telemetry"
)

func TestLegacyMetricsAdapterProjectsRawAttemptFacts(t *testing.T) {
	result := telemetry.AttemptTelemetry{
		Metrics: telemetry.RawExecutionMetrics{
			CpuTimeMs: 12, PeakRssBytes: 4096, DiskReadBytes: 99,
			TelemetryCPUSource: "cgroup_v2",
		},
		Coverage: map[string]bool{"cpu": true},
		Complete: true,
	}
	legacy := (LegacyMetricsAdapter{}).ProjectAttempt(result)
	if legacy["cpu.ms"] != int64(12) || legacy["rss.peak.bytes"] != int64(4096) || legacy["disk.read.bytes"] != int64(99) {
		t.Fatalf("legacy projection = %#v", legacy)
	}
	if legacy["telemetry.complete"] != true || legacy["telemetry.cpu.source"] != "cgroup_v2" {
		t.Fatalf("coverage projection = %#v", legacy)
	}
	result.Metrics.CpuTimeMs = 100
	if legacy["cpu.ms"] != int64(12) {
		t.Fatal("legacy projection aliases mutable raw storage")
	}
}

func TestLegacyMetricsAdapterFromMapProducesRawFacts(t *testing.T) {
	coverage, _ := json.Marshal(map[string]bool{"cpu": true, "memory": true})
	raw := (LegacyMetricsAdapter{}).FromMap(map[string]interface{}{
		"cpu.ms":                  55,
		"rss.peak.bytes":          int64(8192),
		"telemetry.coverage.json": string(coverage),
		"telemetry.complete":      true,
	})
	if raw == nil || raw.CpuTimeMs != 55 || raw.PeakRssBytes != 8192 || !raw.TelemetryComplete {
		t.Fatalf("raw adapter result = %+v", raw)
	}
}

func TestMergeStatsIntoPreservesExistingRawFacts(t *testing.T) {
	report := &TaskExecutionReport{
		RawMetrics: &telemetry.RawExecutionMetrics{OutputSha256: "publisher-sha", FramesEncoded: 42},
	}
	(&TaskRunner{}).mergeStatsInto(report, map[string]interface{}{"cpu.ms": int64(7)})
	if report.RawMetrics == nil || report.RawMetrics.OutputSha256 != "publisher-sha" || report.RawMetrics.FramesEncoded != 42 {
		t.Fatalf("existing raw facts were replaced: %+v", report.RawMetrics)
	}
	if report.RawMetrics.CpuTimeMs != 7 {
		t.Fatalf("legacy cpu fact was not overlaid: %+v", report.RawMetrics)
	}
}

func TestMergeAttemptResourceFactsPreservesUnavailableOwners(t *testing.T) {
	raw := telemetry.RawExecutionMetrics{
		FramesEncoded: 30, OutputSha256: "artifact-sha", CacheDownloadBytes: 2048,
		DiskReadBytes: 700,
	}
	attemptCoverage, _ := json.Marshal(map[string]bool{"cpu": true, "memory": true, "disk": false})
	attempt := telemetry.RawExecutionMetrics{
		CpuTimeMs:             55,
		PeakRssBytes:          8192,
		DiskReadBytes:         100,
		TelemetryCoverageJSON: string(attemptCoverage),
		TelemetryComplete:     false,
		TelemetryCPUSource:    "proc",
	}
	telemetry.MergeAttemptResourceFactsInto(&raw, attempt)
	if raw.CpuTimeMs != 55 || raw.PeakRssBytes != 8192 || raw.DiskReadBytes != 700 {
		t.Fatalf("attempt-owned facts/coverage = %+v", raw)
	}
	if raw.FramesEncoded != 30 || raw.OutputSha256 != "artifact-sha" || raw.CacheDownloadBytes != 2048 {
		t.Fatalf("non-attempt facts overwritten: %+v", raw)
	}
}
