#pragma once

#include <memory>
#include <unordered_map>

#include "velox/render/frame_graph.hpp"

// kernel_registry.hpp — the CPU pixel-kernel collection: exactly one kernel
// per FrameOpType. Registration is fail-closed: a null or duplicate kernel is
// rejected, and resolving an unregistered type returns nullptr so callers
// fail closed instead of substituting a noop. The compositor never falls
// back silently to an empty implementation.

namespace velox::render {

class PixelKernelRegistry {
public:
    // Registers kernel for type. Returns false for a null kernel or a type
    // that is already registered (no silent replacement).
    bool registerKernel(FrameOpType type, std::unique_ptr<PixelKernel> kernel);

    // Resolves the kernel for type, or nullptr when unregistered.
    PixelKernel* resolve(FrameOpType type) const;

    bool has(FrameOpType type) const;
    bool empty() const { return kernels_.empty(); }
    std::size_t size() const { return kernels_.size(); }

private:
    std::unordered_map<FrameOpType, std::unique_ptr<PixelKernel>> kernels_;
};

} // namespace velox::render
