#include "velox/services/media_packet_output_sink.hpp"
#include "velox/services/io_counters.hpp"

#ifdef VELOX_ENABLE_LIBAV

extern "C" {
#include <libavutil/hash.h>
#include <libavutil/mem.h>
}

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

namespace fs = std::filesystem;

namespace {
int failures = 0;

void expect(bool condition, const std::string& message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << "\n";
        ++failures;
    }
}

std::string expectedSHA256(const std::string& data) {
    AVHashContext* hash = nullptr;
    if (av_hash_alloc(&hash, "sha256") < 0 || hash == nullptr) return {};
    av_hash_init(hash);
    av_hash_update(hash, reinterpret_cast<const unsigned char*>(data.data()), data.size());
    unsigned char digest[64]{};
    av_hash_final(hash, digest);
    av_free(hash);

    static constexpr char hex[] = "0123456789abcdef";
    std::string result;
    result.reserve(64);
    for (int i = 0; i < 32; ++i) {
        result.push_back(hex[digest[i] >> 4]);
        result.push_back(hex[digest[i] & 0x0f]);
    }
    return result;
}

fs::path uniquePath() {
    return fs::temp_directory_path() /
        ("velox_packet_sink_" + std::to_string(
            std::chrono::steady_clock::now().time_since_epoch().count()) + ".bin");
}

void testAppendOnlySHA() {
    const fs::path path = uniquePath();
    const std::string payload = "append-only packet output sink payload\n";
    velox::media::packet::PacketOutputSink sink;
    std::string error;
    expect(sink.open(path, error), "append-only sink opens: " + error);

    auto* avio = sink.avio();
    expect(avio != nullptr, "append-only sink exposes AVIO context");
    if (avio != nullptr) {
        avio_write(avio,
            reinterpret_cast<const unsigned char*>(payload.data()),
            static_cast<int>(payload.size()));
    }

    velox::media::packet::PacketOutputSinkResult result;
    expect(sink.finalize(result, error), "append-only sink finalizes: " + error);
    expect(result.output_size_bytes == static_cast<int64_t>(payload.size()),
           "append-only size matches payload");
    expect(!result.backward_seek_seen, "append-only output has no backward seek");
    expect(result.backward_seek_count == 0 && result.backward_seek_bytes == 0,
           "append-only output reports zero backward seek telemetry");
    expect(result.sha256_valid, "append-only SHA is valid");
    expect(result.sha256 == expectedSHA256(payload), "append-only SHA matches final bytes");
    velox::services::resetIOCounters();
    velox::services::recordFileFsync(3);
    velox::services::recordOutputRename(2);
    velox::services::recordDirectoryFsync(4);
    expect(velox::services::ioCounters().file_fsync_ms.load() == 3,
           "file fsync timing is recorded");
    expect(velox::services::ioCounters().output_rename_ms.load() == 2,
           "rename timing is recorded");
    expect(velox::services::ioCounters().directory_fsync_ms.load() == 4,
           "directory fsync timing is recorded");
    velox::services::recordOutputBackwardSeek(5);
    velox::services::recordOutputBackwardSeek(7);
    expect(velox::services::ioCounters().output_backward_seek_count.load() == 2 &&
               velox::services::ioCounters().output_backward_seek_bytes.load() == 12,
           "output backward seek count and bytes are recorded");
    sink.close();
    std::error_code ec;
    fs::remove(path, ec);
}

void testBackwardSeekInvalidatesSHA() {
    const fs::path path = uniquePath();
    const std::string first = "first append block";
    const std::string second = "second append block";
    velox::media::packet::PacketOutputSink sink;
    std::string error;
    expect(sink.open(path, error), "backward-seek sink opens: " + error);

    auto* avio = sink.avio();
    expect(avio != nullptr, "backward-seek sink exposes AVIO context");
    if (avio != nullptr) {
        avio_write(avio, reinterpret_cast<const unsigned char*>(first.data()),
                   static_cast<int>(first.size()));
        avio_flush(avio);
        expect(avio_seek(avio, 0, SEEK_SET) == 0,
               "backward seek is accepted by AVIO");
        avio_write(avio, reinterpret_cast<const unsigned char*>(second.data()),
                   static_cast<int>(second.size()));
    }

    velox::media::packet::PacketOutputSinkResult result;
    expect(sink.finalize(result, error), "backward-seek sink finalizes: " + error);
    expect(result.backward_seek_seen, "backward seek is reported");
    expect(result.backward_seek_count == 1 &&
               result.backward_seek_bytes == static_cast<int64_t>(first.size()),
           "one backward seek rewinds exactly the hashed prefix (count=" +
               std::to_string(result.backward_seek_count) + ", bytes=" +
               std::to_string(result.backward_seek_bytes) + ")");
    expect(!result.sha256_valid, "backward seek disables incremental SHA");
    expect(result.sha256.empty(), "invalid incremental SHA is not returned");
    sink.close();
    std::error_code ec;
    fs::remove(path, ec);
}

void testMultipleBackwardSeeksAccumulate() {
    const fs::path path = uniquePath();
    const std::string first = "first block";
    const std::string second = "bb";
    const std::string third = "cc";
    velox::media::packet::PacketOutputSink sink;
    std::string error;
    expect(sink.open(path, error), "multi-seek sink opens: " + error);

    auto* avio = sink.avio();
    expect(avio != nullptr, "multi-seek sink exposes AVIO context");
    if (avio != nullptr) {
        avio_write(avio, reinterpret_cast<const unsigned char*>(first.data()),
                   static_cast<int>(first.size()));
        expect(avio_seek(avio, 0, SEEK_SET) == 0,
               "first backward seek is accepted by AVIO");
        avio_write(avio, reinterpret_cast<const unsigned char*>(second.data()),
                   static_cast<int>(second.size()));
        expect(avio_seek(avio, 0, SEEK_SET) == 0,
               "second backward seek is accepted by AVIO");
        avio_write(avio, reinterpret_cast<const unsigned char*>(third.data()),
                   static_cast<int>(third.size()));
    }

    velox::media::packet::PacketOutputSinkResult result;
    expect(sink.finalize(result, error), "multi-seek sink finalizes: " + error);
    expect(result.backward_seek_seen, "multi-seek output reports backward seeks");
    expect(!result.sha256_valid, "multi-seek output disables incremental SHA");
    expect(result.backward_seek_count == 2,
           "every backward seek is counted (actual=" +
               std::to_string(result.backward_seek_count) + ")");
    expect(result.backward_seek_bytes == static_cast<int64_t>(2 * first.size()),
           "backward seek bytes accumulate the rewind distance (actual=" +
               std::to_string(result.backward_seek_bytes) + ")");
    sink.close();
    std::error_code ec;
    fs::remove(path, ec);
}
}

int main() {
    testAppendOnlySHA();
    testBackwardSeekInvalidatesSHA();
    testMultipleBackwardSeeksAccumulate();
    std::cerr << "summary: fail=" << failures << "\n";
    return failures == 0 ? 0 : 1;
}

#endif
