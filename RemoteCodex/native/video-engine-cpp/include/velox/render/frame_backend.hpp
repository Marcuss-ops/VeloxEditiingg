#pragma once

#include "velox/render/kernel_registry.hpp"

// frame_backend.hpp — the frame execution backend registry. CPU is the only
// implemented backend today and owns the PixelKernelRegistry. GPU is a
// declared-but-unimplemented slot so the registry shape is fixed now and the
// future NVIDIA NVDEC→composite→NVENC backend can be added without changing
// callers. Selecting an unimplemented backend fails closed (nullptr), never a
// silent CPU fallback.

namespace velox::render {

enum class FrameBackendKind {
    Cpu,
    Gpu,
};

const char* frameBackendKindName(FrameBackendKind kind);

class FrameBackendRegistry {
public:
    FrameBackendRegistry();

    // The CPU backend is always available and owns the pixel kernel registry.
    PixelKernelRegistry& cpu() { return cpu_; }
    const PixelKernelRegistry& cpu() const { return cpu_; }

    // The GPU backend is not implemented yet. It returns nullptr so callers
    // that request GPU fail closed rather than silently falling back to CPU.
    PixelKernelRegistry* gpu() { return nullptr; }
    const PixelKernelRegistry* gpu() const { return nullptr; }

private:
    PixelKernelRegistry cpu_;
};

} // namespace velox::render
