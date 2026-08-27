#ifdef VELOX_ENABLE_LIBAV

#include "velox/services/media_packet_output_sink.hpp"
#include "velox/services/io_counters.hpp"

extern "C" {
#include <libavutil/error.h>
#include <libavutil/hash.h>
#include <libavutil/mem.h>
}

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <sstream>
#include <cstdio>
#include <sys/stat.h>
#include <unistd.h>

namespace velox::media::packet {
namespace {
constexpr int kBufferSize = 32 * 1024;
}

PacketOutputSink::~PacketOutputSink() { close(); }

bool PacketOutputSink::open(const std::filesystem::path& path, std::string& error) {
    close();
    if (path.empty()) {
        error = "packet output sink requires a path";
        return false;
    }
    fd_ = ::open(path.c_str(), O_CREAT | O_TRUNC | O_RDWR, 0640);
    if (fd_ < 0) {
        error = "open packet output sink: " + std::string(std::strerror(errno));
        return false;
    }
    path_ = path;
    AVHashContext* hash = nullptr;
    if (av_hash_alloc(&hash, "sha256") < 0 || hash == nullptr) {
        error = "av_hash_alloc(sha256) failed";
        close();
        return false;
    }
    sha_ = hash;
    av_hash_init(static_cast<AVHashContext*>(sha_));
    avio_ = avio_alloc_context(
        static_cast<unsigned char*>(av_malloc(kBufferSize)), kBufferSize, 1,
        this, nullptr, &PacketOutputSink::writeCallback,
        &PacketOutputSink::seekCallback);
    if (avio_ == nullptr) {
        error = "avio_alloc_context failed";
        close();
        return false;
    }
    avio_->seekable = AVIO_SEEKABLE_NORMAL;
    return true;
}

int PacketOutputSink::writeCallback(void* opaque, const uint8_t* data, int size) {
    auto& sink = *static_cast<PacketOutputSink*>(opaque);
    if (size <= 0) return 0;
    if (sink.fd_ < 0) return AVERROR(EIO);

    const ssize_t written = ::pwrite(sink.fd_, data, static_cast<size_t>(size), sink.position_);
    if (written < 0) return AVERROR(errno);
    if (written != size) return AVERROR(EIO);

    if (sink.append_only_ && sink.position_ == sink.hashed_until_) {
        av_hash_update(static_cast<AVHashContext*>(sink.sha_), data, size);
        sink.hashed_until_ += size;
    } else {
        // A rewrite makes the incremental digest invalid; finalization will
        // deliberately return sha256_valid=false and callers use the normal
        // canonical manifest hashing path.
        sink.append_only_ = false;
    }
    sink.position_ += size;
    sink.high_watermark_ = std::max(sink.high_watermark_, sink.position_);
    return size;
}

int64_t PacketOutputSink::seekCallback(void* opaque, int64_t offset, int whence) {
    auto& sink = *static_cast<PacketOutputSink*>(opaque);
    if (sink.fd_ < 0) return AVERROR(EIO);
    if ((whence & AVSEEK_SIZE) != 0) return sink.high_watermark_;
    whence &= ~AVSEEK_FORCE;
    int64_t next = 0;
    if (whence == SEEK_SET) next = offset;
    else if (whence == SEEK_CUR) next = sink.position_ + offset;
    else if (whence == SEEK_END) next = sink.high_watermark_ + offset;
    else return AVERROR(EINVAL);
    if (next < 0) return AVERROR(EINVAL);
    if (next < sink.hashed_until_) {
        const int64_t rewound = sink.hashed_until_ - next;
        sink.append_only_ = false;
        ++sink.backward_seek_count_;
        sink.backward_seek_bytes_ += rewound;
        services::recordOutputBackwardSeek(rewound);
    }
    sink.position_ = next;
    return next;
}

bool PacketOutputSink::finalize(PacketOutputSinkResult& result, std::string& error) {
    result = PacketOutputSinkResult{};
    if (fd_ < 0 || sha_ == nullptr) {
        error = "packet output sink is not open";
        return false;
    }
    if (finalized_) {
        error = "packet output sink already finalized";
        return false;
    }
    if (avio_ != nullptr) avio_flush(avio_);
    if (::fsync(fd_) != 0) {
        error = "fsync packet output sink: " + std::string(std::strerror(errno));
        return false;
    }
    struct stat st{};
    if (::fstat(fd_, &st) != 0) {
        error = "fstat packet output sink: " + std::string(std::strerror(errno));
        return false;
    }
    result.output_size_bytes = static_cast<int64_t>(st.st_size);
    result.backward_seek_seen = !append_only_;
    result.backward_seek_count = backward_seek_count_;
    result.backward_seek_bytes = backward_seek_bytes_;
    if (append_only_ && hashed_until_ == result.output_size_bytes) {
        unsigned char digest[64]{};
        av_hash_final(static_cast<AVHashContext*>(sha_), digest);
        char hex[65]{};
        for (size_t i = 0; i < 32; ++i) {
            std::snprintf(hex + i * 2, 3, "%02x", digest[i]);
        }
        result.sha256 = hex;
        result.sha256_valid = true;
    }
    finalized_ = true;
    return true;
}

void PacketOutputSink::close() {
    if (avio_ != nullptr) {
        av_freep(&avio_->buffer);
        avio_context_free(&avio_);
    }
    if (sha_ != nullptr) {
        av_free(sha_);
        sha_ = nullptr;
    }
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
    path_.clear();
    position_ = 0;
    high_watermark_ = 0;
    hashed_until_ = 0;
    append_only_ = true;
    backward_seek_count_ = 0;
    backward_seek_bytes_ = 0;
    finalized_ = false;
}

} // namespace velox::media::packet

#endif
