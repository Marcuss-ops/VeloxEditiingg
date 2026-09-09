#ifdef VELOX_ENABLE_LIBAV

#include "frame_pipeline_compositor.hpp"

namespace velox::media::pipeline_detail {

bool CompositorStage::apply(AVFrame* frame, int64_t frame_index,
                            const velox::render::FrameGraph* graph,
                            std::string& error, int* applied_ops) const {
	if (applied_ops != nullptr) {
		*applied_ops = 0;
	}
    if (backend_ != CompositorBackend::Cpu) {
        error = "compositor backend is not implemented";
        return false;
    }
    if (graph == nullptr || graph->empty()) {
        return true;
    }
    velox::render::PixelFrame pixel_frame;
    pixel_frame.width = frame->width;
    pixel_frame.height = frame->height;
    pixel_frame.pixel_format = frame->format;
    for (int plane = 0; plane < 4; ++plane) {
        pixel_frame.planes[plane].data = frame->data[plane];
        pixel_frame.planes[plane].stride = frame->linesize[plane];
    }
    return graph->apply(pixel_frame, frame_index, applied_ops, &error);
}

} // namespace velox::media::pipeline_detail

#endif // VELOX_ENABLE_LIBAV
