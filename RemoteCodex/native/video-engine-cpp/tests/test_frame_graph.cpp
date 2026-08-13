#include "velox/render/frame_backend.hpp"
#include "velox/render/frame_graph.hpp"
#include "velox/render/kernel_registry.hpp"

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

namespace {

int failures = 0;

void expect(bool condition, const std::string& message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << "\n";
        ++failures;
    }
}

// A deterministic test kernel: records its tag on every apply and fills the
// Y plane with the tag byte inside the op's ROI (clamped to frame bounds).
class TaggedKernel : public velox::render::PixelKernel {
public:
    TaggedKernel(std::vector<int>* tags, int tag) : tags_(tags), tag_(tag) {}

    bool apply(velox::render::PixelFrame& frame,
               const velox::render::FrameOp& op,
               std::string* /*error*/) override {
        tags_->push_back(tag_);
        if (op.roi.empty() || frame.planes[0].data == nullptr) {
            return true;
        }
        const int x0 = std::max(0, op.roi.x);
        const int y0 = std::max(0, op.roi.y);
        const int x1 = std::min(frame.width, op.roi.x + op.roi.width);
        const int y1 = std::min(frame.height, op.roi.y + op.roi.height);
        for (int y = y0; y < y1; ++y) {
            auto* row = frame.planes[0].data +
                        static_cast<std::size_t>(y) * frame.planes[0].stride;
            for (int x = x0; x < x1; ++x) {
                row[x] = static_cast<std::uint8_t>(tag_);
            }
        }
        return true;
    }

private:
    std::vector<int>* tags_;
    int tag_;
};

} // namespace

int main() {
    using namespace velox::render;

    // ── Rect ──────────────────────────────────────────────────────────
    const Rect full{0, 0, 1920, 1080};
    expect(!full.empty(), "full rect is non-empty");
    expect(Rect{0, 0, 0, 10}.empty(), "zero-width rect is empty");
    expect(full.contains(0, 0), "rect contains its origin");
    expect(full.contains(1919, 1079), "rect contains its far corner");
    expect(!full.contains(1920, 1079), "rect excludes its far edge");

    const Rect inner{100, 200, 50, 60};
    expect(full.intersects(inner), "full rect intersects inner rect");
    expect(inner.intersects(full), "intersects is symmetric");
    const Rect clipped = full.intersection(inner);
    expect(clipped.x == 100 && clipped.y == 200 && clipped.width == 50 &&
               clipped.height == 60,
           "full ∩ inner == inner");

    const Rect disjoint{5000, 5000, 10, 10};
    expect(!full.intersects(disjoint), "disjoint rects do not intersect");
    expect(full.intersection(disjoint).empty(),
           "disjoint intersection is empty");

    // ── FrameOp range semantics ───────────────────────────────────────
    const FrameOp ranged{FrameOpType::ImageOverlay, 3000, 3300, {}, "logo", 1.0f};
    expect(!ranged.activeAt(2999), "op inactive before start");
    expect(ranged.activeAt(3000), "op active at inclusive start");
    expect(ranged.activeAt(3300), "op active at inclusive end");
    expect(!ranged.activeAt(3301), "op inactive after end");

    const FrameOp open_ended{FrameOpType::Rectangle, 100, -1, {}, "", 1.0f};
    expect(open_ended.activeAt(100), "open-ended op active at start");
    expect(open_ended.activeAt(9'000'000'000LL), "negative end means to the end");

    expect(std::string(frameOpTypeName(FrameOpType::ImageOverlay)) ==
               "image_overlay",
           "image overlay has a stable wire name");
    expect(std::string(frameOpTypeName(FrameOpType::TextOverlay)) ==
               "text_overlay",
           "text overlay has a stable wire name");

    // ── FrameGraph.add validation ─────────────────────────────────────
    FrameGraph graph;
    expect(graph.empty(), "new graph is empty");
    expect(!graph.add(FrameOp{FrameOpType::Rectangle, -1, 10, {}, "", 1.0f}),
           "negative start frame is rejected");
    expect(!graph.add(FrameOp{FrameOpType::Rectangle, 10, 5, {}, "", 1.0f}),
           "end frame before start is rejected");
    expect(graph.add(FrameOp{FrameOpType::Rectangle, 0, -1, {}, "", 1.0f}),
           "valid op is accepted");

    // ── opsActiveAt ordering + filtering ──────────────────────────────
    FrameGraph ordered;
    ordered.add(FrameOp{FrameOpType::ImageOverlay, 0, 10, {}, "a", 1.0f});
    ordered.add(FrameOp{FrameOpType::Rectangle, 5, 15, {}, "b", 1.0f});
    ordered.add(FrameOp{FrameOpType::AlphaBlend, 20, -1, {}, "c", 1.0f});
    auto at3 = ordered.opsActiveAt(3);
    expect(at3.size() == 1 && at3[0].asset_id == "a",
           "frame 3 activates only the first op");
    auto at7 = ordered.opsActiveAt(7);
    expect(at7.size() == 2 && at7[0].asset_id == "a" && at7[1].asset_id == "b",
           "frame 7 activates ops in insertion order");
    expect(ordered.opsActiveAt(16).empty(), "frame 16 activates no ops");

    // ── apply: empty graph is a pass-through without a registry ───────
    FrameGraph empty_graph;
    PixelFrame frame{};
    std::string error;
    expect(empty_graph.apply(frame, 0, &error), "empty graph applies cleanly");
    expect(error.empty(), "empty graph produces no error");

    // ── apply: dispatch order + ROI fill ──────────────────────────────
    PixelKernelRegistry registry;
    std::vector<int> tags;
    expect(registry.registerKernel(
               FrameOpType::ImageOverlay,
               std::make_unique<TaggedKernel>(&tags, 1)),
           "image overlay kernel registers");
    expect(registry.registerKernel(
               FrameOpType::Rectangle,
               std::make_unique<TaggedKernel>(&tags, 2)),
           "rectangle kernel registers");
    expect(registry.has(FrameOpType::ImageOverlay), "registry reports registered type");
    expect(!registry.has(FrameOpType::TextOverlay), "registry reports unregistered type");
    expect(registry.size() == 2, "registry counts two kernels");
    expect(registry.resolve(FrameOpType::Rectangle) != nullptr,
           "resolve returns a registered kernel");

    FrameGraph applied(&registry);
    applied.add(FrameOp{FrameOpType::ImageOverlay, 0, 10, {}, "a", 1.0f});
    applied.add(FrameOp{FrameOpType::Rectangle, 5, 15, {}, "b", 1.0f});
    expect(applied.apply(frame, 3, &error), "apply dispatches active ops");
    expect(tags == std::vector<int>({1}), "frame 3 dispatched only the image overlay");
    tags.clear();
    expect(applied.apply(frame, 7, &error), "apply dispatches both active ops");
    expect(tags == std::vector<int>({1, 2}), "frame 7 dispatched in insertion order");

    // ROI fill: an ImageOverlay with a 4x5 ROI at (2,3) writes only inside it.
    std::vector<std::uint8_t> buffer(10 * 10, 0);
    PixelFrame roi_frame;
    roi_frame.width = 10;
    roi_frame.height = 10;
    roi_frame.planes[0].data = buffer.data();
    roi_frame.planes[0].stride = 10;
    FrameGraph roi_graph(&registry);
    roi_graph.add(FrameOp{FrameOpType::ImageOverlay, 0, -1, Rect{2, 3, 4, 5}, "roi", 1.0f});
    tags.clear();
    expect(roi_graph.apply(roi_frame, 0, &error), "roi op applies");
    bool roi_correct = true;
    for (int y = 0; y < 10 && roi_correct; ++y) {
        for (int x = 0; x < 10; ++x) {
            const bool inside = x >= 2 && x < 6 && y >= 3 && y < 8;
            const std::uint8_t want = inside ? 1 : 0;
            if (buffer[static_cast<std::size_t>(y) * 10 + x] != want) {
                roi_correct = false;
                break;
            }
        }
    }
    expect(roi_correct, "kernel writes only inside the ROI");

    // ── apply: fail-closed on a missing kernel ────────────────────────
    FrameGraph missing_kernel(&registry);
    missing_kernel.add(FrameOp{FrameOpType::TextOverlay, 0, -1, {}, "t", 1.0f});
    expect(!missing_kernel.apply(frame, 0, &error),
           "unregistered op type fails closed");
    expect(error.find("text_overlay") != std::string::npos,
           "missing-kernel error names the type");

    // ── apply: fail-closed with no registry but active ops ────────────
    FrameGraph no_registry;
    no_registry.add(FrameOp{FrameOpType::ImageOverlay, 0, -1, {}, "x", 1.0f});
    expect(!no_registry.apply(frame, 0, &error),
           "ops without a registry fail closed");
    expect(error.find("kernel registry") != std::string::npos,
           "no-registry error names the registry");

    // ── registry: reject null + duplicate ─────────────────────────────
    PixelKernelRegistry strict;
    expect(!strict.registerKernel(FrameOpType::ImageOverlay, nullptr),
           "null kernel is rejected");
    expect(strict.registerKernel(
               FrameOpType::ImageOverlay,
               std::make_unique<TaggedKernel>(&tags, 9)),
           "first registration succeeds");
    expect(!strict.registerKernel(
               FrameOpType::ImageOverlay,
               std::make_unique<TaggedKernel>(&tags, 10)),
           "duplicate registration is rejected");
    expect(strict.size() == 1, "duplicate rejection keeps a single kernel");

    // ── FrameBackendRegistry ──────────────────────────────────────────
    FrameBackendRegistry backends;
    expect(backends.cpu().empty(), "new CPU backend starts empty");
    expect(backends.gpu() == nullptr, "GPU backend is not implemented yet");
    expect(backends.cpu().registerKernel(
               FrameOpType::AlphaBlend,
               std::make_unique<TaggedKernel>(&tags, 3)),
           "CPU backend accepts a kernel");
    expect(std::string(frameBackendKindName(FrameBackendKind::Cpu)) == "cpu",
           "cpu backend has a stable wire name");
    expect(std::string(frameBackendKindName(FrameBackendKind::Gpu)) == "gpu",
           "gpu backend has a stable wire name");

    return failures == 0 ? 0 : 1;
}
