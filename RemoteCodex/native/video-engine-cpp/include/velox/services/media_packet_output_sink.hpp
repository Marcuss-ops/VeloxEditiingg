#pragma once

#ifdef VELOX_ENABLE_LIBAV
extern "C" {
#include <libavformat/avio.h>
}

#include <cstdint>
#include <filesystem>
#include <string>

namespace velox::media::packet {

struct PacketOutputSinkResult {
    std::string sha256;
    bool sha256_valid{false};
    int64_t output_size_bytes{0};
    bool backward_seek_seen{false};
};

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

private:
    static int writeCallback(void* opaque, const uint8_t* data, int size);
    static int64_t seekCallback(void* opaque, int64_t offset, int whence);

    int fd_{-1};
    AVIOContext* avio_{nullptr};
    std::filesystem::path path_;
    int64_t position_{0};
    int64_t high_watermark_{0};
    int64_t hashed_until_{0};
    bool append_only_{true};
    bool finalized_{false};
    void* sha_{nullptr};
};

} // namespace velox::media::packet
#endif
