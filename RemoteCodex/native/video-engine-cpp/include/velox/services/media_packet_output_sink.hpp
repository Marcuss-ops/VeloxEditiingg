#pragma once

#ifdef VELOX_ENABLE_LIBAV
extern "C" {
#include <libavformat/avio.h>
}

#include <cstdint>
#include <filesystem>
#include <functional>
#include <string>

#include "velox/services/file_utils.hpp"

namespace velox::media::packet {

struct PacketOutputSinkResult {
    std::string sha256;
    bool sha256_valid{false};
    int64_t output_size_bytes{0};
    bool backward_seek_seen{false};
    // Telemetry for the opportunistic-SHA fallback: number of backward
    // seeks below the hashed prefix and total rewound bytes across them.
    // Both are zero on a clean append-only write.
    int64_t backward_seek_count{0};
    int64_t backward_seek_bytes{0};
    // Durability evidence: set to true when finalize() successfully
    // fsynced the file data. The caller (publishAtomic) can use this to
    // skip the redundant file-level fsync.
    bool file_data_synced{false};
};

// Progress callback invoked after each write to the output sink.  The
// callback receives the file path being written and the cumulative bytes
// written so far (the safe offset the Go progressive upload can start
// reading).
using WriteProgressCallback = std::function<void(const std::filesystem::path& path, int64_t bytes_written)>;

class PacketOutputSink {
public:
    PacketOutputSink() = default;
    ~PacketOutputSink();

    PacketOutputSink(const PacketOutputSink&) = delete;
    PacketOutputSink& operator=(const PacketOutputSink&) = delete;

    bool open(const std::filesystem::path& path, std::string& error);
    AVIOContext* avio() const { return avio_; }
    bool finalize(PacketOutputSinkResult& result, std::string& error);
    void close();

    // Set a callback that fires after each write.  The callback receives
    // the cumulative bytes written so far (the safe offset for the
    // progressive upload).
    void setWriteProgressCallback(WriteProgressCallback cb) { writeProgressCb_ = std::move(cb); }
    const std::filesystem::path& path() const { return path_; }

private:
#if LIBAVFORMAT_VERSION_MAJOR >= 62
    static int writeCallback(void* opaque, const uint8_t* data, int size);
#else
    static int writeCallback(void* opaque, uint8_t* data, int size);
#endif
    static int64_t seekCallback(void* opaque, int64_t offset, int whence);

    int fd_{-1};
    AVIOContext* avio_{nullptr};
    std::filesystem::path path_;
    int64_t position_{0};
    int64_t high_watermark_{0};
    int64_t hashed_until_{0};
    bool append_only_{true};
    int64_t backward_seek_count_{0};
    int64_t backward_seek_bytes_{0};
    bool finalized_{false};
    void* sha_{nullptr};
    WriteProgressCallback writeProgressCb_;
};

} // namespace velox::media::packet
#endif
