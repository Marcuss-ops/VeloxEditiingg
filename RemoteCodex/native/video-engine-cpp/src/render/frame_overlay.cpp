#include "velox/render/frame_overlay.hpp"

#include <algorithm>
#include <utility>

namespace velox::render {

bool PreparedOverlay::valid() const {
    if (width <= 0 || height <= 0) {
        return false;
    }
    // 4:2:0 requires even dimensions for an unambiguous chroma grid.
    if ((width & 1) != 0 || (height & 1) != 0) {
        return false;
    }
    if (pixel_format != kPixelFormatYuv420p) {
        return false;
    }
    return planes[0].data != nullptr && planes[1].data != nullptr &&
           planes[2].data != nullptr && planes[3].data != nullptr;
}

namespace {

// The canonical straight-alpha blend. All intermediates fit comfortably in
// an int (max 255*255 + 255*255 + 127 = 130177).
std::uint8_t blendByte(std::uint8_t src, std::uint8_t dst, int alpha) {
    return static_cast<std::uint8_t>(
        (static_cast<int>(src) * alpha +
         static_cast<int>(dst) * (255 - alpha) + 127) /
        255);
}

} // namespace

bool blendYuvOverlayScalar(PixelFrame& dst, const PreparedOverlay& overlay,
                           const Rect& placement, float opacity,
                           std::string* error) {
    if (!overlay.valid()) {
        if (error != nullptr) {
            *error = "invalid prepared overlay";
        }
        return false;
    }
    if (dst.width <= 0 || dst.height <= 0 ||
        (dst.width & 1) != 0 || (dst.height & 1) != 0) {
        if (error != nullptr) {
            *error = "destination frame must have positive even dimensions";
        }
        return false;
    }
    if (dst.planes[0].data == nullptr || dst.planes[1].data == nullptr ||
        dst.planes[2].data == nullptr) {
        if (error != nullptr) {
            *error = "destination frame is missing a Y/U/V plane";
        }
        return false;
    }
    if (placement.width != overlay.width ||
        placement.height != overlay.height) {
        if (error != nullptr) {
            *error = "placement size does not match prepared overlay";
        }
        return false;
    }
    // The 4:2:0 chroma grid aligns to even luma coordinates; an odd origin
    // would introduce a chroma phase offset the scalar reference does not
    // define.
    if ((placement.x & 1) != 0 || (placement.y & 1) != 0) {
        if (error != nullptr) {
            *error = "overlay placement must use even coordinates for 4:2:0";
        }
        return false;
    }

    float o = opacity;
    if (o != o) {  // NaN
        o = 1.0f;
    }
    o = std::clamp(o, 0.0f, 1.0f);
    const int op = static_cast<int>(o * 255.0f + 0.5f);
    if (op == 0) {
        return true;  // fully transparent overlay is a no-op
    }

    const int W = dst.width;
    const int H = dst.height;
    const int ow = overlay.width;
    const int oh = overlay.height;
    const int px = placement.x;
    const int py = placement.y;

    // Clip the luma ROI to the frame so an off-frame overlay never writes
    // out of bounds.
    const int x0 = std::max(0, px);
    const int y0 = std::max(0, py);
    const int x1 = std::min(W, px + ow);
    const int y1 = std::min(H, py + oh);

    // ── Y plane (full resolution, full-res alpha) ─────────────────────
    {
        const auto* oy = overlay.planes[0].data;
        const auto* oa = overlay.planes[3].data;
        const int oys = overlay.planes[0].stride;
        const int oas = overlay.planes[3].stride;
        auto* dy = dst.planes[0].data;
        const int dys = dst.planes[0].stride;
        for (int y = y0; y < y1; ++y) {
            const int oyy = y - py;
            const auto* src_row = oy + static_cast<std::size_t>(oyy) * oys;
            const auto* a_row = oa + static_cast<std::size_t>(oyy) * oas;
            auto* dst_row = dy + static_cast<std::size_t>(y) * dys;
            for (int x = x0; x < x1; ++x) {
                const int oxx = x - px;
                const int alpha = (static_cast<int>(a_row[oxx]) * op + 127) / 255;
                dst_row[x] = blendByte(src_row[oxx], dst_row[x], alpha);
            }
        }
    }

    // ── U / V planes (half resolution, 2x2-box-subsampled alpha) ──────
    const int Wc = W / 2;
    const int Hc = H / 2;
    const int owc = ow / 2;
    const int ohc = oh / 2;
    const int pcx = px / 2;
    const int pcy = py / 2;
    const int cx0 = std::max(0, pcx);
    const int cy0 = std::max(0, pcy);
    const int cx1 = std::min(Wc, pcx + owc);
    const int cy1 = std::min(Hc, pcy + ohc);

    const auto* ou = overlay.planes[1].data;
    const auto* ov = overlay.planes[2].data;
    const auto* oa = overlay.planes[3].data;
    const int ous = overlay.planes[1].stride;
    const int ovs = overlay.planes[2].stride;
    const int oas = overlay.planes[3].stride;
    auto* du = dst.planes[1].data;
    auto* dv = dst.planes[2].data;
    const int dus = dst.planes[1].stride;
    const int dvs = dst.planes[2].stride;

    for (int cy = cy0; cy < cy1; ++cy) {
        const int ocy = cy - pcy;   // overlay chroma y
        const int oyy = ocy * 2;    // overlay luma y (top of the 2x2 block)
        const auto* srcu = ou + static_cast<std::size_t>(ocy) * ous;
        const auto* srcv = ov + static_cast<std::size_t>(ocy) * ovs;
        const auto* a_top = oa + static_cast<std::size_t>(oyy) * oas;
        const auto* a_bot = oa + static_cast<std::size_t>(oyy + 1) * oas;
        auto* dstu = du + static_cast<std::size_t>(cy) * dus;
        auto* dstv = dv + static_cast<std::size_t>(cy) * dvs;
        for (int cx = cx0; cx < cx1; ++cx) {
            const int ocx = cx - pcx;   // overlay chroma x
            const int oxx = ocx * 2;    // overlay luma x (left of the block)
            const int a_block = static_cast<int>(a_top[oxx]) +
                                static_cast<int>(a_top[oxx + 1]) +
                                static_cast<int>(a_bot[oxx]) +
                                static_cast<int>(a_bot[oxx + 1]);
            const int a_full = (a_block + 2) / 4;  // 2x2 box average
            const int alpha = (a_full * op + 127) / 255;
            dstu[cx] = blendByte(srcu[ocx], dstu[cx], alpha);
            dstv[cx] = blendByte(srcv[ocx], dstv[cx], alpha);
        }
    }

    return true;
}

ImageOverlayKernel::ImageOverlayKernel(std::shared_ptr<const PreparedOverlay> overlay)
    : overlay_(std::move(overlay)) {}

bool ImageOverlayKernel::apply(PixelFrame& frame, const FrameOp& op,
                               std::string* error) {
    if (!overlay_) {
        if (error != nullptr) {
            *error = "image overlay kernel has no prepared overlay";
        }
        return false;
    }
    if (op.type != FrameOpType::ImageOverlay) {
        if (error != nullptr) {
            *error = "image overlay kernel applied to " +
                     std::string(frameOpTypeName(op.type));
        }
        return false;
    }

    // An empty ROI means "use the overlay's natural dimensions"; otherwise
    // the ROI must match the prepared overlay exactly.
    Rect placement = op.roi;
    if (placement.width <= 0) {
        placement.width = overlay_->width;
    }
    if (placement.height <= 0) {
        placement.height = overlay_->height;
    }
    return blendYuvOverlayScalar(frame, *overlay_, placement, op.opacity, error);
}

} // namespace velox::render
