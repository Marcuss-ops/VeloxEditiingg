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

// FilterBackend names the frame filter implementation. CPU (libswscale) is
// the only implemented backend: the never-built CUDA branch was removed from
// the per-frame apply() path (audit P2: dead compare executed at frame rate).
// A future GPU backend must be re-introduced as a separate, explicit
// implementation — not a never-taken runtime compare.
enum class FilterBackend {
    Cpu,
};

class FilterChain {
public:
    FilterChain() = default;
    ~FilterChain();

    FilterChain(const FilterChain&) = delete;
    FilterChain& operator=(const FilterChain&) = delete;

    // `backend` exists so call sites state their intent explicitly; only
    // FilterBackend::Cpu is accepted (fail closed on anything else).
    bool init(FilterBackend backend, const AVCodecContext& decoder,
              const AVCodecContext& encoder,
              std::string& error);
    AVFrame* apply(AVFrame* source, int pool_index, FramePool& pool,
                   int source_height, int64_t& cpu_busy_ns,
                   std::string& error);
    bool bypass() const { return scaler_ == nullptr; }
    FilterBackend backend() const { return FilterBackend::Cpu; }

private:
    SwsContext* scaler_{nullptr};
};

} // namespace velox::media::pipeline_detail

#endif // VELOX_ENABLE_LIBAV
