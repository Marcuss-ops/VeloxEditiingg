#pragma once

#include <cstdint>
#include <string>
#include <vector>

// frame_graph.hpp — the compositor layer's value types and the single render
// hook. The frame pipeline calls FrameGraph::apply() once per frame between
// decode/render and encode; it never branches per overlay/logo/text feature.
// A feature is a FrameOp, and every FrameOp resolves to exactly one
// PixelKernel in a backend's PixelKernelRegistry.
//
// This header is LibAV-independent by design: the frame pipeline adapts its
// decoded AVFrame into the small planar PixelFrame view below, so the
// compositor and its tests never depend on libav* headers.

namespace velox::render {

class PixelKernelRegistry;

// The supported per-frame compositing operations. Each type maps to exactly
// one PixelKernel. Requesting an op whose kernel is not registered is a
// fail-closed condition, never a silent noop.
enum class FrameOpType {
    ImageOverlay,
    TextOverlay,
    Rectangle,
    AlphaBlend,
};

// Stable wire/telemetry name for a FrameOpType.
const char* frameOpTypeName(FrameOpType type);

// Axis-aligned integer region in frame coordinates. An empty ROI (width or
// height <= 0) means "the whole frame"; kernels must bound their work to the
// ROI so a small overlay never scans the full surface.
struct Rect {
    int x{0};
    int y{0};
    int width{0};
    int height{0};

    bool empty() const { return width <= 0 || height <= 0; }
    bool contains(int px, int py) const;
    bool intersects(const Rect& other) const;
    // The overlapping region, or an empty Rect when disjoint.
    Rect intersection(const Rect& other) const;
};

// One compositing operation with an inclusive frame range and an optional
// ROI. Frame numbers are output-frame indices (0-based).
struct FrameOp {
    FrameOpType type{FrameOpType::ImageOverlay};
    int64_t start_frame{0};
    // Inclusive end frame. A negative end_frame means "active to the end of
    // the timeline" (mirrors the DirtySpanResolver CLEAN/DIRTY/CLEAN spans).
    int64_t end_frame{-1};
    Rect roi;               // empty roi = whole frame
    std::string asset_id;   // the overlay/composite source asset
    float opacity{1.0f};

    // Active when frame_number >= start_frame AND (end_frame < 0 OR
    // frame_number <= end_frame).
    bool activeAt(int64_t frame_number) const;
};

// A minimal planar 8-bit surface a kernel reads/mutates. Plane indices are
// Y/U/V/A by convention; unused planes have data == nullptr and stride == 0.
// pixel_format carries the opaque source-format tag (an AV_PIX_FMT_* value in
// the LibAV frame pipeline) and is interpreted only by kernels that need it.
struct PixelFrame {
    struct Plane {
        std::uint8_t* data{nullptr};
        int stride{0};
    };

    int width{0};
    int height{0};
    int pixel_format{-1};
    Plane planes[4];
};

// A deterministic compositor kernel for one FrameOpType. Implementations
// must produce identical bytes for the same (frame, op) on every backend so a
// scalar/SIMD/GPU split stays bit-exact (see the compositor bit-exactness
// contract). Returns false and sets *error on failure.
class PixelKernel {
public:
    virtual ~PixelKernel() = default;
    virtual bool apply(PixelFrame& frame, const FrameOp& op,
                       std::string* error = nullptr) = 0;
};

// An ordered list of compositing ops applied between decode/render and
// encode. apply() is the single render hook; an empty graph is a pass-through
// and requires no registry. Missing kernels fail closed.
class FrameGraph {
public:
    // kernels may be null; a null registry only fails when an op is actually
    // active at the applied frame.
    explicit FrameGraph(const PixelKernelRegistry* kernels = nullptr);

    // Appends an op; ops apply in insertion order. Returns false for an op
    // that can never be active (negative start, or end_frame in [0, start)).
    bool add(const FrameOp& op);

    bool empty() const { return ops_.empty(); }
    const std::vector<FrameOp>& ops() const { return ops_; }

    // The active ops at frame_number, in insertion order.
    std::vector<FrameOp> opsActiveAt(int64_t frame_number) const;

    // Applies every op active at frame_number in insertion order. Returns
    // false (and sets *error) when a kernel is missing or a kernel fails.
    bool apply(PixelFrame& frame, int64_t frame_number,
               std::string* error = nullptr) const;

private:
    std::vector<FrameOp> ops_;
    const PixelKernelRegistry* kernels_;
};

} // namespace velox::render
