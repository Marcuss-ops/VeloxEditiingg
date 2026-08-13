#include "velox/render/kernel_registry.hpp"

#include <utility>

namespace velox::render {

bool PixelKernelRegistry::registerKernel(FrameOpType type,
                                         std::unique_ptr<PixelKernel> kernel) {
    if (!kernel) {
        return false;
    }
    if (has(type)) {
        return false;
    }
    kernels_.emplace(type, std::move(kernel));
    return true;
}

PixelKernel* PixelKernelRegistry::resolve(FrameOpType type) const {
    const auto it = kernels_.find(type);
    return it == kernels_.end() ? nullptr : it->second.get();
}

bool PixelKernelRegistry::has(FrameOpType type) const {
    return kernels_.find(type) != kernels_.end();
}

} // namespace velox::render
