#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "velox/render/frame_graph.hpp"

// frame_overlay.hpp — the scalar image-overlay compositor. A logo/overlay is
// decoded, resized and converted exactly ONCE into a PreparedOverlay (planar
// YUV 4:2:0 + straight alpha); the kernel then blends that prepared surface
// into every frame without re-decoding, re-resizing or any YUV→RGB→YUV
// round-trip. The blend is direct on the Y/U/V planes with integer
// arithmetic, and every write is bounded to the placement ROI so a small
// overlay never scans the full frame.
//
// Bit-exactness contract: blendYuvOverlayScalar is the canonical scalar
// reference. Any future SIMD/GPU kernel must reproduce its output bytes
// identically for the same inputs (memcmp == 0), not just "close" results.

namespace velox::render {

// The only overlay pixel layout this scalar compositor understands: planar
// 4:2:0 YUV (luma full-res, chroma half-res) with a full-res straight
// (non-premultiplied) alpha plane. The value matches AV_PIX_FMT_YUV420P.
inline constexpr int kPixelFormatYuv420p = 0;

// An immutable, already-prepared overlay surface. Plane indices are
// Y(0)/U(1)/V(2)/A(3); U/V are half-resolution (width/2 x height/2), alpha
// is full-resolution. Pointers are const views: the producer/cache owns the
// backing buffers and must keep them alive for the kernel's lifetime.
struct PreparedOverlay {
    int width{0};
    int height{0};
    int pixel_format{kPixelFormatYuv420p};
    int compositor_version{0};

    struct Plane {
        const std::uint8_t* data{nullptr};
        int stride{0};
    };
    Plane planes[4];

    // True when the surface is blendable: positive even dimensions, 4:2:0
    // format, and all four planes present.
    bool valid() const;
};

// Blends overlay into dst at placement (which must match the overlay's
// dimensions), clipped to the frame. Straight-alpha integer blend per plane:
//
//     out = (src*alpha + dst*(255-alpha) + 127) / 255
//
// Y is blended at full resolution; U/V at half resolution with the alpha
// subsampled by a 2x2 box average. The compositor requires even frame and
// overlay dimensions and an even placement origin so the 4:2:0 chroma grid
// is unambiguous. Returns false and sets *error on any violation.
bool blendYuvOverlayScalar(PixelFrame& dst, const PreparedOverlay& overlay,
                           const Rect& placement, float opacity,
                           std::string* error = nullptr);

// A PixelKernel that blends one prepared overlay into every frame. The
// overlay is prepared once by the caller; apply() performs only the ROI
// blend using the op's ROI as placement and op.opacity as the global
// opacity multiplier.
class ImageOverlayKernel : public PixelKernel {
public:
    explicit ImageOverlayKernel(std::shared_ptr<const PreparedOverlay> overlay);

    bool apply(PixelFrame& frame, const FrameOp& op,
               std::string* error = nullptr) override;

private:
    std::shared_ptr<const PreparedOverlay> overlay_;
};

} // namespace velox::render
