"""Bottleneck classification for capacity certification results."""

from __future__ import annotations

from bench.models import CapResult


def classify_bottleneck(result: CapResult) -> str:
    """Classify the dominant observed resource using one canonical policy.

    Precedence is chosen so that the tightest / hardest-to-resolve bound
    wins: FD exhaustion is a hard crash, memory pressure triggers OOM
    kills, GPU saturation stalls NVENC pipelines, I/O wait starves CPU,
    and only last does raw CPU saturation dominate.

    Thresholds (derived from the capacity certification plan):
      FD_BOUND:       fd_util_peak >= 0.80
      MEMORY_BOUND:   host_memory_peak_ratio >= 0.85
      GPU_BOUND:      gpu_util_peak >= 0.90  (N/A when metrics absent)
      IO_BOUND:       iowait_avg >= 0.25
      CPU_BOUND:      cpu_peak >= 0.90
    """
    if result.fd_util_peak_ratio is not None and result.fd_util_peak_ratio >= 0.80:
        return "FD_BOUND"
    if result.host_memory_peak_ratio is not None and result.host_memory_peak_ratio >= 0.85:
        return "MEMORY_BOUND"
    if result.gpu_util_peak_ratio is not None and result.gpu_util_peak_ratio >= 0.90:
        return "GPU_BOUND"
    if result.disk_wait_avg_ratio is not None and result.disk_wait_avg_ratio >= 0.25:
        return "IO_BOUND"
    if result.cpu_peak_ratio is not None and result.cpu_peak_ratio >= 0.90:
        return "CPU_BOUND"
    return "UNKNOWN"
