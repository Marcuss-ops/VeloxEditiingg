#include "velox/render/frame_overlay.hpp"

#include <algorithm>
#include <utility>

#if defined(__GNUC__) || defined(__clang__)
#include <immintrin.h>
#endif

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

// The canonical straight-alpha blend (scalar reference). All intermediates
// fit comfortably in an int (max 255*255 + 255*255 + 127 = 130177).
std::uint8_t blendByte(std::uint8_t src, std::uint8_t dst, int alpha) {
    return static_cast<std::uint8_t>(
        (static_cast<int>(src) * alpha +
         static_cast<int>(dst) * (255 - alpha) + 127) /
        255);
}

// Clipped ROI + opacity byte, computed ONCE and shared by the scalar and
// AVX2 kernels so the two paths can never disagree on where or how much to
// blend. This is the single source of truth for input validation.
struct BlendRegion {
    int op{0};
    int x0{0}, y0{0}, x1{0}, y1{0};       // luma ROI (clipped to frame)
    int cx0{0}, cy0{0}, cx1{0}, cy1{0};   // chroma ROI (clipped to frame)
};

bool validateBlend(const PixelFrame& dst, const PreparedOverlay& overlay,
                   const Rect& placement, float opacity, BlendRegion& region,
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
    region.op = static_cast<int>(o * 255.0f + 0.5f);

    const int W = dst.width;
    const int H = dst.height;
    const int ow = overlay.width;
    const int oh = overlay.height;
    const int px = placement.x;
    const int py = placement.y;

    region.x0 = std::max(0, px);
    region.y0 = std::max(0, py);
    region.x1 = std::min(W, px + ow);
    region.y1 = std::min(H, py + oh);

    region.cx0 = std::max(0, px / 2);
    region.cy0 = std::max(0, py / 2);
    region.cx1 = std::min(W / 2, px / 2 + ow / 2);
    region.cy1 = std::min(H / 2, py / 2 + oh / 2);
    return true;
}

// Chroma (U/V) blend: half resolution with a 2x2-box-subsampled alpha. It is
// shared by the scalar and AVX2 kernels (chroma is 1/4 of the pixels and is
// not the hot path).
void blendChromaScalar(PixelFrame& dst, const PreparedOverlay& overlay,
                       const Rect& placement, const BlendRegion& region) {
    const int pcx = placement.x / 2;
    const int pcy = placement.y / 2;
    const int op = region.op;

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

    for (int cy = region.cy0; cy < region.cy1; ++cy) {
        const int ocy = cy - pcy;   // overlay chroma y
        const int oyy = ocy * 2;    // overlay luma y (top of the 2x2 block)
        const auto* srcu = ou + static_cast<std::size_t>(ocy) * ous;
        const auto* srcv = ov + static_cast<std::size_t>(ocy) * ovs;
        const auto* a_top = oa + static_cast<std::size_t>(oyy) * oas;
        const auto* a_bot = oa + static_cast<std::size_t>(oyy + 1) * oas;
        auto* dstu = du + static_cast<std::size_t>(cy) * dus;
        auto* dstv = dv + static_cast<std::size_t>(cy) * dvs;
        for (int cx = region.cx0; cx < region.cx1; ++cx) {
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
}

} // namespace

bool blendYuvOverlayScalar(PixelFrame& dst, const PreparedOverlay& overlay,
                           const Rect& placement, float opacity,
                           std::string* error) {
    BlendRegion region;
    if (!validateBlend(dst, overlay, placement, opacity, region, error)) {
        return false;
    }
    if (region.op == 0) {
        return true;
    }

    const int px = placement.x;
    const int py = placement.y;
    const auto* oy = overlay.planes[0].data;
    const auto* oa = overlay.planes[3].data;
    const int oys = overlay.planes[0].stride;
    const int oas = overlay.planes[3].stride;
    auto* dy = dst.planes[0].data;
    const int dys = dst.planes[0].stride;

    for (int y = region.y0; y < region.y1; ++y) {
        const int oyy = y - py;
        const auto* src_row = oy + static_cast<std::size_t>(oyy) * oys;
        const auto* a_row = oa + static_cast<std::size_t>(oyy) * oas;
        auto* dst_row = dy + static_cast<std::size_t>(y) * dys;
        for (int x = region.x0; x < region.x1; ++x) {
            const int oxx = x - px;
            const int alpha =
                (static_cast<int>(a_row[oxx]) * region.op + 127) / 255;
            dst_row[x] = blendByte(src_row[oxx], dst_row[x], alpha);
        }
    }

    blendChromaScalar(dst, overlay, placement, region);
    return true;
}

#if defined(__GNUC__) || defined(__clang__)

// The AVX2 Y-plane kernel. 16 pixels per iteration in uint16 lanes. Both
// divisions by 255 use the exact 16-bit identity
//
//     x / 255 == ((x + 1) + ((x + 1) >> 8)) >> 8
//
// which holds when the numerator never exceeds 65152. Our numerators are
// bounded by 255*255 + 127 = 65152 (src/alpha/dst/op are all <= 255), so
// (x + 1) <= 65153 and the intermediate sum <= 65407 stay within uint16.
__attribute__((target("avx2")))
bool blendYuvOverlayAVX2(PixelFrame& dst, const PreparedOverlay& overlay,
                         const Rect& placement, float opacity,
                         std::string* error) {
    BlendRegion region;
    if (!validateBlend(dst, overlay, placement, opacity, region, error)) {
        return false;
    }
    if (region.op == 0) {
        return true;
    }

    const int px = placement.x;
    const int py = placement.y;
    const auto* oy = overlay.planes[0].data;
    const auto* oa = overlay.planes[3].data;
    const int oys = overlay.planes[0].stride;
    const int oas = overlay.planes[3].stride;
    auto* dy = dst.planes[0].data;
    const int dys = dst.planes[0].stride;

    const __m256i op_vec = _mm256_set1_epi16(static_cast<short>(region.op));
    const __m256i v127 = _mm256_set1_epi16(127);
    const __m256i v1 = _mm256_set1_epi16(1);
    const __m256i v255 = _mm256_set1_epi16(255);

    for (int y = region.y0; y < region.y1; ++y) {
        const int oyy = y - py;
        const auto* src_row = oy + static_cast<std::size_t>(oyy) * oys;
        const auto* a_row = oa + static_cast<std::size_t>(oyy) * oas;
        auto* dst_row = dy + static_cast<std::size_t>(y) * dys;
        int x = region.x0;
        for (; x + 16 <= region.x1; x += 16) {
            const int oxx = x - px;
            const __m128i src8 = _mm_loadu_si128(
                reinterpret_cast<const __m128i*>(src_row + oxx));
            const __m128i dst8 = _mm_loadu_si128(
                reinterpret_cast<const __m128i*>(dst_row + x));
            const __m128i a8 = _mm_loadu_si128(
                reinterpret_cast<const __m128i*>(a_row + oxx));

            const __m256i src16 = _mm256_cvtepu8_epi16(src8);
            const __m256i dst16 = _mm256_cvtepu8_epi16(dst8);
            const __m256i a16 = _mm256_cvtepu8_epi16(a8);

            // alpha = (a*op + 127) / 255
            __m256i alpha =
                _mm256_add_epi16(_mm256_mullo_epi16(a16, op_vec), v127);
            alpha = _mm256_add_epi16(alpha, v1);
            alpha = _mm256_add_epi16(alpha, _mm256_srli_epi16(alpha, 8));
            alpha = _mm256_srli_epi16(alpha, 8);

            // out = (src*alpha + dst*(255-alpha) + 127) / 255
            const __m256i inv = _mm256_sub_epi16(v255, alpha);
            __m256i out = _mm256_add_epi16(_mm256_mullo_epi16(src16, alpha),
                                           _mm256_mullo_epi16(dst16, inv));
            out = _mm256_add_epi16(out, v127);
            out = _mm256_add_epi16(out, v1);
            out = _mm256_add_epi16(out, _mm256_srli_epi16(out, 8));
            out = _mm256_srli_epi16(out, 8);

            const __m128i out_lo = _mm256_castsi256_si128(out);
            const __m128i out_hi = _mm256_extracti128_si256(out, 1);
            const __m128i packed = _mm_packus_epi16(out_lo, out_hi);
            _mm_storeu_si128(reinterpret_cast<__m128i*>(dst_row + x), packed);
        }
        for (; x < region.x1; ++x) {  // scalar tail (< 16 px)
            const int oxx = x - px;
            const int alpha =
                (static_cast<int>(a_row[oxx]) * region.op + 127) / 255;
            dst_row[x] = blendByte(src_row[oxx], dst_row[x], alpha);
        }
    }

    blendChromaScalar(dst, overlay, placement, region);
    return true;
}

#else  // !GNU/Clang

bool blendYuvOverlayAVX2(PixelFrame& dst, const PreparedOverlay& overlay,
                         const Rect& placement, float opacity,
                         std::string* error) {
    return blendYuvOverlayScalar(dst, overlay, placement, opacity, error);
}

#endif  // GNU/Clang

bool blendYuvOverlay(PixelFrame& dst, const PreparedOverlay& overlay,
                     const Rect& placement, float opacity,
                     std::string* error) {
#if defined(__GNUC__) || defined(__clang__)
    if (__builtin_cpu_supports("avx2")) {
        return blendYuvOverlayAVX2(dst, overlay, placement, opacity, error);
    }
#endif
    return blendYuvOverlayScalar(dst, overlay, placement, opacity, error);
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
    return blendYuvOverlay(frame, *overlay_, placement, op.opacity, error);
}

} // namespace velox::render
