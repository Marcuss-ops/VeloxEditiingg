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
};

// Returns the process-scoped counter block.
IOCounters& ioCounters();

// Resets every counter and the reopen-tracking set. Called by
// RenderEngine::render() on each run so sequential in-process renders
// stay independent.
void resetIOCounters();

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
