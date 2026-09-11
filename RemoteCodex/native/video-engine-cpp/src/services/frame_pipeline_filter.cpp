#ifdef VELOX_ENABLE_LIBAV

#include "frame_pipeline_filter.hpp"

extern "C" {
#include <libavutil/error.h>
#include <libswscale/swscale.h>
}

#include <chrono>

namespace velox::media::pipeline_detail {

FilterChain::~FilterChain() {
    if (scaler_ != nullptr) {
        sws_freeContext(scaler_);
    }
}

bool FilterChain::init(FilterBackend backend, const AVCodecContext& decoder,
                       const AVCodecContext& encoder,
                       std::string& error) {
    // CPU is the only implemented filter backend; reject anything else so a
    // future mis-wiring fails closed instead of silently passing frames
    // through untransformed.
    if (backend != FilterBackend::Cpu) {
        error = "frame filter backend is not implemented";
        return false;
    }
    if (decoder.width == encoder.width && decoder.height == encoder.height &&
        decoder.pix_fmt == encoder.pix_fmt) {
        return true;
    }
    scaler_ = sws_getContext(
        decoder.width, decoder.height, decoder.pix_fmt,
        encoder.width, encoder.height, encoder.pix_fmt,
        SWS_BILINEAR, nullptr, nullptr, nullptr);
    if (scaler_ == nullptr) {
        error = "sws_getContext failed";
        return false;
    }
    return true;
}

AVFrame* FilterChain::apply(AVFrame* source, int pool_index, FramePool& pool,
                            int source_height, int64_t& cpu_busy_ns,
                            std::string& error) {
    if (scaler_ == nullptr) {
        return source;
    }
    AVFrame* scaled = pool.scaled(pool_index);
    const auto start = std::chrono::steady_clock::now();
    const int result = sws_scale(
        scaler_, source->data, source->linesize, 0, source_height,
        scaled->data, scaled->linesize);
    cpu_busy_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now() - start).count();
    if (result <= 0) {
        error = "sws_scale failed";
        return nullptr;
    }
    return scaled;
}

} // namespace velox::media::pipeline_detail

#endif // VELOX_ENABLE_LIBAV
