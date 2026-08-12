#pragma once
// io_counters.hpp — process-scoped I/O counters for one render.
//
// The engine CLI runs exactly one render per process, so process-global
// counters are reset at the start of RenderEngine::render() and read by
// the sidecar writer at finalize (sidecarJson). They are populated at
// three chokepoints:
//
//   file::copyFile                → recordFileCopy (every filesystem copy)
//   file::downloadAsset           → recordAssetCopy (asset materialization)
//   media openInput / probeOpen   → recordInputOpen (every avformat open)
//
// file_copy_count/bytes and asset_bytes_copied are deliberately separate
// counters over the SAME bytes: the first counts generic copy activity,
// the second counts the copies that materialize assets. They never
// double-count each other because they are incremented at distinct call
// sites.
//
// All accessors are thread-safe (relaxed ordering is sufficient: the only
// observer is the sidecar writer at finalize, plus tests).

#include <atomic>
#include <cstdint>
#include <string>

namespace velox::services {

struct IOCounters {
    std::atomic<int64_t> file_copy_count{0};
    std::atomic<int64_t> file_copy_bytes{0};
    std::atomic<int64_t> asset_bytes_copied{0};
    std::atomic<int64_t> input_open_count{0};
    std::atomic<int64_t> input_reopen_count{0};

    // External tool spawn counters (engine-declared): every external
    // process the engine launches through the file_utils chokepoints
    // (runCommandTimed / runCommand / captureCommandOutput). The total
    // and the per-kind breakdown are DISJOINT facts from the Go-side
    // /proc sampler (worker-observed): the engine counts what IT
    // spawned, the sampler counts what it observed. The Phase-1
    // zero-spawn invariant is verifiable from this block alone:
    // copy-only jobs must report external_spawn_count == 0.
    std::atomic<int64_t> external_spawn_count{0};
    std::atomic<int64_t> ffmpeg_spawn_count{0};
    std::atomic<int64_t> ffprobe_spawn_count{0};
    std::atomic<int64_t> shell_spawn_count{0};
    std::atomic<int64_t> curl_spawn_count{0};
};

// ProcessUsage is the engine process's OWN resource accounting
// (getrusage(RUSAGE_SELF) at sidecar emission time): CPU user/system
// milliseconds, voluntary/involuntary context switches and minor/major
// page faults. It complements the Go-side /proc tree sampler (which
// aggregates the whole engine process tree) with the engine-declared
// view of its own execution.
//
// NOTE: getrusage is process-lifetime and CANNOT be reset — for
// sequential in-process renders (tests) the values are cumulative, not
// per-render deltas. The production CLI runs one render per process, so
// usage facts always describe exactly that render; do not add them to
// resetIOCounters (there is no rusage reset syscall).
struct ProcessUsage {
    int64_t cpu_user_ms{0};
    int64_t cpu_system_ms{0};
    int64_t voluntary_context_switches{0};
    int64_t involuntary_context_switches{0};
    int64_t minor_page_faults{0};
    int64_t major_page_faults{0};
};

// Returns the process-scoped counter block.
IOCounters& ioCounters();

// Resets every counter and the reopen-tracking set. Called by
// RenderEngine::render() on each run so sequential in-process renders
// stay independent.
void resetIOCounters();

// Records one external tool spawn. The command is classified by its
// leading token into the ffmpeg/ffprobe/curl buckets; anything else is
// counted as a shell spawn (external commands execute through
// /bin/sh -c, so "shell" is the residual bucket, not a suggestion that
// no exec happens). Absolute-path invocations (/usr/bin/ffmpeg …) and
// env-prefixed commands fall into the shell bucket — the per-kind
// breakdown undercounts those, while external_spawn_count (the total,
// and the copy-only zero-spawn invariant) is always exact.
void recordExternalSpawn(const std::string& command);

// Returns the engine process's own usage snapshot (getrusage
// RUSAGE_SELF). Failures return a zero struct.
ProcessUsage processUsage();

// Records one filesystem copy of `bytes` (file::copyFile chokepoint).
void recordFileCopy(int64_t bytes);

// Records one asset materialization copy of `bytes` (the downloadAsset
// staging copies into the workdir and the cache). The copied bytes are
// ALSO recorded by recordFileCopy at the copyFile chokepoint; this is
// the asset-specific projection of the same I/O.
void recordAssetCopy(int64_t bytes);

// Records one avformat input open. Opening a path that this process has
// already opened counts as a reopen (e.g. the copy-only muxer opening a
// segment once for stream discovery and again inside readPackets).
void recordInputOpen(const std::string& path);

} // namespace velox::services
