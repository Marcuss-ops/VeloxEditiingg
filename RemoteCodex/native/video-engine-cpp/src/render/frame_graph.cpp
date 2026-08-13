#include "velox/render/frame_graph.hpp"

#include <algorithm>

#include "velox/render/kernel_registry.hpp"

namespace velox::render {

const char* frameOpTypeName(FrameOpType type) {
    switch (type) {
    case FrameOpType::ImageOverlay:
        return "image_overlay";
    case FrameOpType::TextOverlay:
        return "text_overlay";
    case FrameOpType::Rectangle:
        return "rectangle";
    case FrameOpType::AlphaBlend:
        return "alpha_blend";
    }
    return "unknown";
}

bool Rect::contains(int px, int py) const {
    return px >= x && px < x + width && py >= y && py < y + height;
}

bool Rect::intersects(const Rect& other) const {
    if (empty() || other.empty()) {
        return false;
    }
    return x < other.x + other.width && other.x < x + width &&
           y < other.y + other.height && other.y < y + height;
}

Rect Rect::intersection(const Rect& other) const {
    if (!intersects(other)) {
        return Rect{};
    }
    const int left = std::max(x, other.x);
    const int top = std::max(y, other.y);
    const int right = std::min(x + width, other.x + other.width);
    const int bottom = std::min(y + height, other.y + other.height);
    return Rect{left, top, right - left, bottom - top};
}

bool FrameOp::activeAt(int64_t frame_number) const {
    if (frame_number < start_frame) {
        return false;
    }
    if (end_frame >= 0 && frame_number > end_frame) {
        return false;
    }
    return true;
}

FrameGraph::FrameGraph(const PixelKernelRegistry* kernels) : kernels_(kernels) {}

bool FrameGraph::add(const FrameOp& op) {
    if (op.start_frame < 0) {
        return false;
    }
    if (op.end_frame >= 0 && op.end_frame < op.start_frame) {
        return false;
    }
    ops_.push_back(op);
    return true;
}

std::vector<FrameOp> FrameGraph::opsActiveAt(int64_t frame_number) const {
    std::vector<FrameOp> active;
    active.reserve(ops_.size());
    for (const auto& op : ops_) {
        if (op.activeAt(frame_number)) {
            active.push_back(op);
        }
    }
    return active;
}

bool FrameGraph::apply(PixelFrame& frame, int64_t frame_number,
                       std::string* error) const {
    if (ops_.empty()) {
        return true;
    }
    if (kernels_ == nullptr) {
        if (error != nullptr) {
            *error = "frame graph has ops but no kernel registry";
        }
        return false;
    }
    for (const auto& op : ops_) {
        if (!op.activeAt(frame_number)) {
            continue;
        }
        PixelKernel* kernel = kernels_->resolve(op.type);
        if (kernel == nullptr) {
            if (error != nullptr) {
                *error = "no pixel kernel registered for " +
                         std::string(frameOpTypeName(op.type));
            }
            return false;
        }
        std::string kernel_error;
        if (!kernel->apply(frame, op, &kernel_error)) {
            if (error != nullptr) {
                *error = kernel_error.empty()
                    ? std::string("pixel kernel failed for ") +
                          frameOpTypeName(op.type)
                    : kernel_error;
            }
            return false;
        }
    }
    return true;
}

} // namespace velox::render
