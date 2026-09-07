#include "velox/render/kernel_registry.hpp"

namespace velox::render {

namespace {

constexpr std::size_t kInvalidOpType = 4;

// FrameOpType is a class enum with exactly four consecutive values; index()
// is the registry slot. Fail closed on any out-of-range value.
std::size_t opTypeIndex(FrameOpType type) {
    switch (type) {
    case FrameOpType::ImageOverlay:
        return 0;
    case FrameOpType::TextOverlay:
        return 1;
    case FrameOpType::Rectangle:
        return 2;
    case FrameOpType::AlphaBlend:
        return 3;
    }
    return kInvalidOpType;
}

} // namespace

bool PixelKernelRegistry::registerKernel(FrameOpType type,
                                         std::unique_ptr<PixelKernel> kernel) {
    const std::size_t index = opTypeIndex(type);
    if (index == kInvalidOpType || !kernel) {
        return false;
    }
    if (kernels_[index] != nullptr) {
        return false;
    }
    kernels_[index] = std::move(kernel);
    ++count_;
    return true;
}

PixelKernel* PixelKernelRegistry::resolve(FrameOpType type) const {
    const std::size_t index = opTypeIndex(type);
    return index == kInvalidOpType ? nullptr : kernels_[index].get();
}

bool PixelKernelRegistry::has(FrameOpType type) const {
    const std::size_t index = opTypeIndex(type);
    return index != kInvalidOpType && kernels_[index] != nullptr;
}

std::size_t PixelKernelRegistry::size() const { return count_; }

} // namespace velox::render
