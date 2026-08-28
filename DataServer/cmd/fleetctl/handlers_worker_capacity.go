// handlers_worker_capacity.go — fleetctl worker capacity sub-command handler.
//
// Usage:
//
//	fleetctl worker capacity <worker_id> [--json]
//
// Calls GET /api/v1/admin/workers/{id}/capacity which reads from the Master
// SQL database (worker_resource_samples, task_attempt_metrics,
// capacity_benchmark_runs) and returns a single canonical capacity report.
// No Prometheus dependency.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"
)

// capacityReport matches the response shape from GET /api/v1/admin/workers/:worker_id/capacity.
type capacityReport struct {
	WorkerID string `json:"worker_id"`

	// Host resource peaks/floors from worker_resource_samples.
	CPUPeakRatio          float64 `json:"cpu_peak_ratio"`
	CPUIOWaitPeakRatio    float64 `json:"cpu_iowait_peak_ratio"`
	MemoryPeakRatio       float64 `json:"memory_peak_ratio"`
	MemoryAvailableMinMB  float64 `json:"memory_available_min_mb"`
	DiskFreeMinBytes      int64   `json:"disk_free_min_bytes"`
	ScratchPeakBytes      int64   `json:"scratch_peak_bytes"`
	FDPeak                int64   `json:"fd_peak"`
	FDLimit               int64   `json:"fd_limit"`
	FDUtillizationPeak    float64 `json:"fd_utilization_peak"`
	NetworkRxMBPS         float64 `json:"network_rx_mbps"`
	NetworkTxMBPS         float64 `json:"network_tx_mbps"`
	Load1Peak             float64 `json:"load_1_peak"`
	RunQueuePeak          int64   `json:"run_queue_peak"`
	DiskIOWaitMSMax       int64   `json:"disk_io_wait_ms_max"`
	NetworkRetransmitsMax int64   `json:"network_retransmits_max"`
	RenderJobsActiveAvg   float64 `json:"render_jobs_active_avg"`
	PrefetchJobsActiveAvg float64 `json:"prefetch_jobs_active_avg"`
	PublisherJobsActiveAvg float64 `json:"publisher_jobs_active_avg"`
	TaskSlotsAvg          float64 `json:"task_slots_avg"`
	SampleCount           int     `json:"sample_count"`
	WindowStart           string  `json:"window_start"`
	WindowEnd             string  `json:"window_end"`

	// Per-job capacity facts from task_attempt_metrics.
	AvgJobScratchPeakBytes int64 `json:"avg_job_scratch_peak_bytes"`
	MaxJobScratchPeakBytes int64 `json:"max_job_scratch_peak_bytes"`
	AvgJobPublishBytes     int64 `json:"avg_job_publish_bytes"`
	AvgJobPageFaults       int64 `json:"avg_job_page_faults"`
	AvgJobPeakRSSDelta     int64 `json:"avg_job_peak_rss_delta_bytes"`
	AttemptCount           int   `json:"attempt_count"`

	// Latest benchmark result.
	BenchmarkRunID  string  `json:"benchmark_run_id,omitempty"`
	SweetSpot       int     `json:"sweet_spot,omitempty"`
	LimitingFactor  string  `json:"limiting_factor,omitempty"`
	PredictedSlots  *int    `json:"predicted_render_slots,omitempty"`
	PredictionAcc   *string `json:"prediction_accuracy,omitempty"`
	BenchmarkRunAt  string  `json:"benchmark_run_at,omitempty"`

	// Derived recommendation.
	MaxRecommendedJobs int    `json:"max_recommended_jobs"`
	DataSource         string `json:"data_source"`
}

// runWorkerCapacity calls GET /api/v1/admin/workers/:worker_id/capacity
// and pretty-prints the capacity report.
func runWorkerCapacity(client *fleetClient, args []string) int {
	fs := flag.NewFlagSet("worker capacity", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	jsonOutput := fs.Bool("json", false, "print raw JSON instead of formatted output")
	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "fleetctl worker capacity: %v\n", err)
		return ExitMisuse
	}

	workerID := strings.TrimSpace(fs.Arg(0))
	if workerID == "" {
		fmt.Fprintln(os.Stderr, "fleetctl worker capacity: worker_id is required (first positional argument)")
		fmt.Fprintln(os.Stderr, "Usage: fleetctl worker capacity <worker_id> [--json]")
		return ExitMisuse
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var report capacityReport
	status, err := client.doJSON(ctx, "GET", "/api/v1/admin/workers/"+workerID+"/capacity", nil, &report)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fleetctl worker capacity: request failed: %v\n", err)
		return ExitUnexpected
	}
	if status == 404 {
		fmt.Fprintf(os.Stderr, "fleetctl worker capacity: no capacity data for worker %s\n", workerID)
		return ExitWorkerNotFound
	}
	if status != 200 {
		ec := MapHTTPStatusToOpExit(status)
		fmt.Fprintf(os.Stderr, "fleetctl worker capacity: GET /admin/workers/%s/capacity status=%d\n", workerID, status)
		return ec
	}

	if *jsonOutput {
		encoded, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			fmt.Fprintf(os.Stderr, "fleetctl worker capacity: encode JSON: %v\n", err)
			return ExitUnexpected
		}
		fmt.Println(string(encoded))
		return ExitOK
	}

	// Pretty-print the capacity report.
	printCapacityReport(&report)
	return ExitOK
}

func printCapacityReport(r *capacityReport) {
	fmt.Printf("Worker Capacity Report: %s\n", r.WorkerID)
	fmt.Printf("  Data source: %s\n", r.DataSource)

	// Host resources.
	fmt.Println("\n--- Host Resources (24h window) ---")
	if r.SampleCount > 0 {
		fmt.Printf("  Samples:         %d (%s → %s)\n", r.SampleCount, truncateTime(r.WindowStart), truncateTime(r.WindowEnd))
	} else {
		fmt.Println("  Samples:         none")
	}
	fmt.Printf("  CPU peak:        %.1f%%\n", r.CPUPeakRatio*100)
	fmt.Printf("  IOWait peak:     %.1f%%\n", r.CPUIOWaitPeakRatio*100)
	fmt.Printf("  Memory peak:     %.1f%%\n", r.MemoryPeakRatio*100)
	fmt.Printf("  Memory avail:    %.0f MB (min)\n", r.MemoryAvailableMinMB)
	fmt.Printf("  Disk free:       %s (min)\n", formatBytes(r.DiskFreeMinBytes))
	fmt.Printf("  Scratch peak:    %s\n", formatBytes(r.ScratchPeakBytes))
	fmt.Printf("  FD peak:         %d / %d (%.1f%%)\n", r.FDPeak, r.FDLimit, r.FDUtillizationPeak*100)
	fmt.Printf("  Network rx:      %.1f Mbps (avg)\n", r.NetworkRxMBPS)
	fmt.Printf("  Network tx:      %.1f Mbps (avg)\n", r.NetworkTxMBPS)
	fmt.Printf("  Load1 peak:      %.1f\n", r.Load1Peak)
	fmt.Printf("  Run queue peak:  %d\n", r.RunQueuePeak)
	fmt.Printf("  IO wait max:     %d ms\n", r.DiskIOWaitMSMax)
	fmt.Printf("  Retransmits max: %d\n", r.NetworkRetransmitsMax)

	// Per-job facts.
	fmt.Println("\n--- Per-Job Capacity Facts ---")
	if r.AttemptCount > 0 {
		fmt.Printf("  Attempts:            %d (recent succeeded)\n", r.AttemptCount)
		fmt.Printf("  Scratch peak/avg:    %s / %s\n", formatBytes(r.MaxJobScratchPeakBytes), formatBytes(r.AvgJobScratchPeakBytes))
		fmt.Printf("  Publish bytes/avg:   %s\n", formatBytes(r.AvgJobPublishBytes))
		fmt.Printf("  Page faults/avg:     %d\n", r.AvgJobPageFaults)
		fmt.Printf("  RSS delta/avg:       %s\n", formatBytes(r.AvgJobPeakRSSDelta))
	} else {
		fmt.Println("  No succeeded attempts yet")
	}

	// Benchmark.
	fmt.Println("\n--- Latest Benchmark ---")
	if r.BenchmarkRunID != "" {
		fmt.Printf("  Run ID:      %s\n", r.BenchmarkRunID)
		fmt.Printf("  Sweet spot:  %d jobs\n", r.SweetSpot)
		fmt.Printf("  Limiting:    %s\n", r.LimitingFactor)
		if r.PredictedSlots != nil {
			fmt.Printf("  Predicted:   %d\n", *r.PredictedSlots)
		}
		if r.PredictionAcc != nil {
			fmt.Printf("  Accuracy:    %s\n", *r.PredictionAcc)
		}
		fmt.Printf("  Run at:      %s\n", truncateTime(r.BenchmarkRunAt))
	} else {
		fmt.Println("  No benchmark data")
	}

	// Recommendation.
	fmt.Println("\n--- Recommendation ---")
	fmt.Printf("  Max recommended jobs: %d\n", r.MaxRecommendedJobs)
}

// truncateTime truncates an RFC3339Nano timestamp to a shorter form for display.
func truncateTime(s string) string {
	if s == "" {
		return "-"
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return s
	}
	return t.Format("2006-01-02 15:04:05")
}
