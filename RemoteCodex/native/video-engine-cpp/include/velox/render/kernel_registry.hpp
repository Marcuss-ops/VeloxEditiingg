#pragma once

#include <array>
#include <memory>

#include "velox/render/frame_graph.hpp"

// kernel_registry.hpp — the CPU pixel-kernel collection: exactly one kernel
// per FrameOpType. Registration is fail-closed: a null or duplicate kernel is
// rejected, and resolving an unregistered type returns nullptr so callers
// fail closed instead of substituting a noop. The compositor never falls
// back silently to an empty implementation.
//
// Dispatch is a flat array indexed by the FrameOpType enum value — O(1) with
// no hashing and no per-frame cache miss on an unordered_map bucket chain.

namespace velox::render {

class PixelKernelRegistry {
public:
    PixelKernelRegistry() = default;

    // Move-only: the registry owns the kernel pointers.
    PixelKernelRegistry(PixelKernelRegistry&&) = default;
    PixelKernelRegistry& operator=(PixelKernelRegistry&&) = default;

    // Registers kernel for type. Returns false for a null kernel or a type
    // that is already registered (no silent replacement).
    bool registerKernel(FrameOpType type, std::unique_ptr<PixelKernel> kernel);

    // Resolves the kernel for type, or nullptr when unregistered.
    PixelKernel* resolve(FrameOpType type) const;

    bool has(FrameOpType type) const;
    bool empty() const { return size() == 0; }
    std::size_t size() const;

private:
    static constexpr std::size_t kOpTypeCount = 4;  // FrameOpType value count

    std::array<std::unique_ptr<PixelKernel>, kOpTypeCount> kernels_{};
    std::size_t count_{0};
};

} // namespace velox::render
