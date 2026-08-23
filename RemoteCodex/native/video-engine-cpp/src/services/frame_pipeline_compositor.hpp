#pragma once

#ifdef VELOX_ENABLE_LIBAV

#include "velox/render/frame_graph.hpp"

extern "C" {
#include <libavutil/frame.h>
}

#include <cstdint>
#include <string>

namespace velox::media::pipeline_detail {

enum class CompositorBackend {
    Cpu,
    Cuda,
};

class CompositorStage {
public:
    explicit CompositorStage(CompositorBackend backend = CompositorBackend::Cpu)
        : backend_(backend) {}

    bool apply(AVFrame* frame, int64_t frame_index,
               const velox::render::FrameGraph* graph,
               std::string& error) const;
    CompositorBackend backend() const { return backend_; }

private:
    CompositorBackend backend_;
};

} // namespace velox::media::pipeline_detail

#endif // VELOX_ENABLE_LIBAV
