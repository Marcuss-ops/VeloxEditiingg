#pragma once

#ifdef VELOX_ENABLE_LIBAV

#include "velox/render/frame_graph.hpp"

extern "C" {
#include <libavutil/frame.h>
}

#include <cstdint>
#include <string>

namespace velox::media::pipeline_detail {

// CompositorBackend names the compositing implementation. CPU is the only
// implemented backend: the never-built CUDA branch was removed from the
// per-frame apply() path (audit P2: dead compare executed at frame rate).
// A future GPU compositor must be re-introduced as a separate, explicit
// implementation — not a never-taken runtime compare.
enum class CompositorBackend {
    Cpu,
};

class CompositorStage {
public:
    explicit CompositorStage(CompositorBackend backend = CompositorBackend::Cpu)
        : backend_(backend) {}

    bool apply(AVFrame* frame, int64_t frame_index,
               const velox::render::FrameGraph* graph,
               std::string& error, int* applied_ops = nullptr) const;
    CompositorBackend backend() const { return backend_; }

private:
    CompositorBackend backend_;
};

} // namespace velox::media::pipeline_detail

#endif // VELOX_ENABLE_LIBAV
