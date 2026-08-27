#include "velox/services/io_counters.hpp"

#include <cctype>
#include <mutex>
#include <sys/resource.h>
#include <unordered_set>
#include <chrono>

namespace velox::services {

namespace {
IOCounters g_ioCounters;
std::mutex g_openMutex;
std::unordered_set<std::string> g_openedPaths;
std::chrono::steady_clock::time_point g_render_started;

// leadingToken returns the first whitespace-delimited token of a
// command string (the executable name), lowercased.
std::string leadingToken(const std::string& command) {
    std::string token;
    token.reserve(command.size());
    bool started = false;
    for (const char c : command) {
        if (std::isspace(static_cast<unsigned char>(c))) {
            if (started) break;
            continue;
        }
        started = true;
        token.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
    }
    return token;
}
} // namespace

IOCounters& ioCounters() {
    return g_ioCounters;
}

void resetIOCounters() {
	g_render_started = std::chrono::steady_clock::now();
    g_ioCounters.file_copy_count.store(0);
    g_ioCounters.file_copy_bytes.store(0);
    g_ioCounters.asset_bytes_copied.store(0);
    g_ioCounters.input_open_count.store(0);
    g_ioCounters.input_reopen_count.store(0);
    g_ioCounters.input_seek_count.store(0);
    g_ioCounters.output_backward_seek_count.store(0);
    g_ioCounters.output_backward_seek_bytes.store(0);
    g_ioCounters.first_packet_read_ms.store(0);
    g_ioCounters.first_output_write_ms.store(0);
    g_ioCounters.file_fsync_ms.store(0);
    g_ioCounters.directory_fsync_ms.store(0);
    g_ioCounters.output_rename_ms.store(0);
    g_ioCounters.external_spawn_count.store(0);
    g_ioCounters.ffmpeg_spawn_count.store(0);
    g_ioCounters.ffprobe_spawn_count.store(0);
    g_ioCounters.shell_spawn_count.store(0);
    g_ioCounters.curl_spawn_count.store(0);
    {
        std::lock_guard<std::mutex> lock(g_openMutex);
        g_openedPaths.clear();
    }
}

void recordFileCopy(int64_t bytes) {
    g_ioCounters.file_copy_count.fetch_add(1);
    if (bytes > 0) {
        g_ioCounters.file_copy_bytes.fetch_add(bytes);
    }
}

void recordAssetCopy(int64_t bytes) {
    if (bytes > 0) {
        g_ioCounters.asset_bytes_copied.fetch_add(bytes);
    }
}

void recordInputOpen(const std::string& path) {
    g_ioCounters.input_open_count.fetch_add(1);
    std::lock_guard<std::mutex> lock(g_openMutex);
    if (g_openedPaths.insert(path).second == false) {
        g_ioCounters.input_reopen_count.fetch_add(1);
    }
}

void recordInputSeek() { g_ioCounters.input_seek_count.fetch_add(1); }
void recordOutputBackwardSeek(int64_t rewound_bytes) {
    g_ioCounters.output_backward_seek_count.fetch_add(1);
    if (rewound_bytes > 0) {
        g_ioCounters.output_backward_seek_bytes.fetch_add(rewound_bytes);
    }
}
void recordFirstPacketRead() {
    int64_t expected = 0;
    const int64_t now = static_cast<int64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - g_render_started).count());
    g_ioCounters.first_packet_read_ms.compare_exchange_strong(expected, now);
}
void recordFirstOutputWrite() {
    int64_t expected = 0;
    const int64_t now = static_cast<int64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - g_render_started).count());
    g_ioCounters.first_output_write_ms.compare_exchange_strong(expected, now);
}
void recordFileFsync(int64_t elapsed_ms) { g_ioCounters.file_fsync_ms.fetch_add(elapsed_ms); }
void recordDirectoryFsync(int64_t elapsed_ms) { g_ioCounters.directory_fsync_ms.fetch_add(elapsed_ms); }
void recordOutputRename(int64_t elapsed_ms) { g_ioCounters.output_rename_ms.fetch_add(elapsed_ms); }

void recordExternalSpawn(const std::string& command) {
    g_ioCounters.external_spawn_count.fetch_add(1);
    const std::string token = leadingToken(command);
    if (token == "ffmpeg") {
        g_ioCounters.ffmpeg_spawn_count.fetch_add(1);
    } else if (token == "ffprobe") {
        g_ioCounters.ffprobe_spawn_count.fetch_add(1);
    } else if (token == "curl") {
        g_ioCounters.curl_spawn_count.fetch_add(1);
    } else {
        // External commands execute through /bin/sh -c, so anything that
        // is not a recognized tool is the residual shell bucket.
        g_ioCounters.shell_spawn_count.fetch_add(1);
    }
}

ProcessUsage processUsage() {
    ProcessUsage usage;
    struct rusage self{};
    if (getrusage(RUSAGE_SELF, &self) != 0) {
        return usage;
    }
    usage.cpu_user_ms =
        static_cast<int64_t>(self.ru_utime.tv_sec) * 1000 +
        static_cast<int64_t>(self.ru_utime.tv_usec) / 1000;
    usage.cpu_system_ms =
        static_cast<int64_t>(self.ru_stime.tv_sec) * 1000 +
        static_cast<int64_t>(self.ru_stime.tv_usec) / 1000;
    usage.voluntary_context_switches = self.ru_nvcsw;
    usage.involuntary_context_switches = self.ru_nivcsw;
    usage.minor_page_faults = self.ru_minflt;
    usage.major_page_faults = self.ru_majflt;
    return usage;
}

} // namespace velox::services
