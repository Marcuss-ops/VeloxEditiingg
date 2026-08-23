#pragma once

#ifdef VELOX_ENABLE_LIBAV

#include "frame_pipeline_support.hpp"

extern "C" {
#include <libavcodec/avcodec.h>
#include <libswscale/swscale.h>
}

#include <memory>
#include <string>

namespace velox::media::pipeline_detail {

enum class FilterBackend {
    Cpu,
    Cuda,
};

class FilterChain {
public:
    FilterChain() = default;
    ~FilterChain();

    FilterChain(const FilterChain&) = delete;
    FilterChain& operator=(const FilterChain&) = delete;

    bool init(FilterBackend backend, const AVCodecContext& decoder,
              const AVCodecContext& encoder, FramePool& pool,
              std::string& error);
    AVFrame* apply(AVFrame* source, int pool_index, FramePool& pool,
                   int source_height, int64_t& cpu_busy_ns,
                   std::string& error);
    bool bypass() const { return backend_ == FilterBackend::Cpu && scaler_ == nullptr; }
    FilterBackend backend() const { return backend_; }

private:
    FilterBackend backend_{FilterBackend::Cpu};
    SwsContext* scaler_{nullptr};
};

} // namespace velox::media::pipeline_detail

#endif // VELOX_ENABLE_LIBAV
