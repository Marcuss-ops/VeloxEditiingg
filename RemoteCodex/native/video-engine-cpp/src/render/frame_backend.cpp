#include "velox/render/frame_backend.hpp"

namespace velox::render {

const char* frameBackendKindName(FrameBackendKind kind) {
    switch (kind) {
    case FrameBackendKind::Cpu:
        return "cpu";
    case FrameBackendKind::Gpu:
        return "gpu";
    }
    return "unknown";
}

FrameBackendRegistry::FrameBackendRegistry() = default;

} // namespace velox::render
