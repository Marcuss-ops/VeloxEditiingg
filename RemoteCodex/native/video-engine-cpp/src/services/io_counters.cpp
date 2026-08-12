#include "velox/services/io_counters.hpp"

#include <cctype>
#include <mutex>
#include <sys/resource.h>
#include <unordered_set>

namespace velox::services {

namespace {
IOCounters g_ioCounters;
std::mutex g_openMutex;
std::unordered_set<std::string> g_openedPaths;

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
    g_ioCounters.file_copy_count.store(0);
    g_ioCounters.file_copy_bytes.store(0);
    g_ioCounters.asset_bytes_copied.store(0);
    g_ioCounters.input_open_count.store(0);
    g_ioCounters.input_reopen_count.store(0);
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
