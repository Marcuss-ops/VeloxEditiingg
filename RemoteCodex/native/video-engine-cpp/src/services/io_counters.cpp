#include "velox/services/io_counters.hpp"

#include <mutex>
#include <unordered_set>

namespace velox::services {

namespace {
IOCounters g_ioCounters;
std::mutex g_openMutex;
std::unordered_set<std::string> g_openedPaths;
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

} // namespace velox::services
