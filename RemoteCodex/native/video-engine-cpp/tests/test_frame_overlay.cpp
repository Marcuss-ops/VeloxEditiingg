#include "velox/render/frame_overlay.hpp"

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

using velox::render::FrameOp;
using velox::render::FrameOpType;
using velox::render::PixelFrame;
using velox::render::PreparedOverlay;
using velox::render::Rect;

// A frame whose planes are owned vectors, with row-major strides.
struct FrameFixture {
    explicit FrameFixture(int w, int h, std::uint8_t yv, std::uint8_t uv,
                          std::uint8_t vv)
        : y(static_cast<std::size_t>(w) * h, yv),
          u(static_cast<std::size_t>(w / 2) * (h / 2), uv),
          v(static_cast<std::size_t>(w / 2) * (h / 2), vv) {
        frame.width = w;
        frame.height = h;
        frame.pixel_format = velox::render::kPixelFormatYuv420p;
        frame.planes[0] = {y.data(), w};
        frame.planes[1] = {u.data(), w / 2};
        frame.planes[2] = {v.data(), w / 2};
    }

    std::vector<std::uint8_t> y;
    std::vector<std::uint8_t> u;
    std::vector<std::uint8_t> v;
    PixelFrame frame;
};

// A prepared overlay whose planes are owned vectors (row-major strides).
struct OverlayFixture {
    OverlayFixture(int ow, int oh, std::uint8_t yv, std::uint8_t uv,
                   std::uint8_t vv, std::uint8_t av)
        : y(static_cast<std::size_t>(ow) * oh, yv),
          u(static_cast<std::size_t>(ow / 2) * (oh / 2), uv),
          v(static_cast<std::size_t>(ow / 2) * (oh / 2), vv),
          a(static_cast<std::size_t>(ow) * oh, av) {
        overlay.width = ow;
        overlay.height = oh;
        overlay.pixel_format = velox::render::kPixelFormatYuv420p;
        overlay.compositor_version = 1;
        overlay.planes[0] = {y.data(), ow};
        overlay.planes[1] = {u.data(), ow / 2};
        overlay.planes[2] = {v.data(), ow / 2};
        overlay.planes[3] = {a.data(), ow};
    }

    std::vector<std::uint8_t> y;
    std::vector<std::uint8_t> u;
    std::vector<std::uint8_t> v;
    std::vector<std::uint8_t> a;
    PreparedOverlay overlay;
};

} // namespace

int main() {
    using velox::render::blendYuvOverlayScalar;

    // ── Opaque overlay replaces the ROI on Y/U/V exactly ──────────────
    {
        FrameFixture f(4, 4, 100, 50, 60);
        OverlayFixture o(2, 2, 200, 180, 190, 255);
        o.y = {200, 210, 220, 230};
        std::string error;
        expect(blendYuvOverlayScalar(f.frame, o.overlay, Rect{2, 2, 2, 2}, 1.0f, &error),
               "opaque overlay blends");
        const std::vector<std::uint8_t> want_y = {
            100, 100, 100, 100,
            100, 100, 100, 100,
            100, 100, 200, 210,
            100, 100, 220, 230,
        };
        expect(f.y == want_y, "opaque overlay replaces Y inside ROI only");
        const std::vector<std::uint8_t> want_u = {50, 50, 50, 180};
        const std::vector<std::uint8_t> want_v = {60, 60, 60, 190};
        expect(f.u == want_u, "opaque overlay replaces U inside chroma ROI");
        expect(f.v == want_v, "opaque overlay replaces V inside chroma ROI");
    }

    // ── Fully transparent overlay leaves the frame untouched ──────────
    {
        FrameFixture f(4, 4, 100, 50, 60);
        OverlayFixture o(2, 2, 200, 180, 190, 0);
        std::string error;
        expect(blendYuvOverlayScalar(f.frame, o.overlay, Rect{0, 0, 2, 2}, 1.0f, &error),
               "transparent overlay blends (no-op)");
        expect(f.y == std::vector<std::uint8_t>(16, 100), "transparent Y is unchanged");
        expect(f.u == std::vector<std::uint8_t>(4, 50), "transparent U is unchanged");
        expect(f.v == std::vector<std::uint8_t>(4, 60), "transparent V is unchanged");
    }

    // ── Alpha 128 scales the blend (canonical straight-alpha) ─────────
    {
        FrameFixture f(4, 4, 100, 50, 60);
        OverlayFixture o(2, 2, 200, 180, 190, 128);
        std::string error;
        expect(blendYuvOverlayScalar(f.frame, o.overlay, Rect{0, 0, 2, 2}, 1.0f, &error),
               "half-alpha overlay blends");
        // out = (200*128 + 100*127 + 127)/255 = 150
        expect(f.y[0] == 150 && f.y[1] == 150 && f.y[4] == 150 && f.y[5] == 150,
               "half alpha produces the canonical 150 luma value");
        expect(f.y[2] == 100 && f.y[3] == 100 && f.y[15] == 100,
               "half alpha leaves pixels outside the ROI unchanged");
    }

    // ── opacity scales the effective alpha identically ────────────────
    {
        FrameFixture f(4, 4, 100, 50, 60);
        OverlayFixture o(2, 2, 200, 180, 190, 255);
        std::string error;
        expect(blendYuvOverlayScalar(f.frame, o.overlay, Rect{0, 0, 2, 2}, 0.5f, &error),
               "half-opacity overlay blends");
        // effective alpha = (255 * round(0.5*255)) / 255 = 128
        expect(f.y[0] == 150 && f.y[1] == 150 && f.y[4] == 150 && f.y[5] == 150,
               "half opacity produces the canonical 150 luma value");
    }

    // ── Chroma alpha is a 2x2 box average of the full-res alpha ───────
    {
        FrameFixture f(4, 4, 100, 50, 60);
        OverlayFixture o(2, 2, 200, 180, 190, 0);
        o.a = {0, 255, 0, 255};  // per-pixel Y alpha + 2x2 chroma average = 128
        std::string error;
        expect(blendYuvOverlayScalar(f.frame, o.overlay, Rect{0, 0, 2, 2}, 1.0f, &error),
               "subsampled-alpha overlay blends");
        // Y: a=0 -> unchanged, a=255 -> replaced by 200.
        const std::vector<std::uint8_t> want_y = {
            100, 200, 100, 100,
            100, 200, 100, 100,
            100, 100, 100, 100,
            100, 100, 100, 100,
        };
        expect(f.y == want_y, "per-pixel alpha drives the Y plane");
        // Chroma: box average (0+255+0+255+2)/4 = 128 → U=115, V=125.
        expect(f.u[0] == 115, "chroma U uses the 2x2-averaged alpha");
        expect(f.v[0] == 125, "chroma V uses the 2x2-averaged alpha");
        expect(f.u[1] == 50 && f.u[2] == 50 && f.u[3] == 50,
               "chroma outside the ROI is unchanged");
    }

    // ── Off-frame placement clips without an out-of-bounds write ──────
    {
        FrameFixture f(4, 4, 100, 50, 60);
        OverlayFixture o(4, 4, 200, 180, 190, 255);
        // Sentinel byte just past the Y buffer catches any overrun.
        f.y.push_back(0xAB);
        f.frame.planes[0] = {f.y.data(), 4};
        std::string error;
        expect(blendYuvOverlayScalar(f.frame, o.overlay, Rect{2, 2, 4, 4}, 1.0f, &error),
               "clipped overlay blends");
        const std::vector<std::uint8_t> want_y = {
            100, 100, 100, 100,
            100, 100, 100, 100,
            100, 100, 200, 200,
            100, 100, 200, 200,
        };
        expect(std::equal(want_y.begin(), want_y.end(), f.y.begin()),
               "clipped overlay writes only the in-frame portion");
        expect(f.y.back() == 0xAB, "clipped overlay never writes past the Y plane");
        const std::vector<std::uint8_t> want_u = {50, 50, 50, 180};
        const std::vector<std::uint8_t> want_v = {60, 60, 60, 190};
        expect(f.u == want_u, "clipped overlay writes only the in-frame chroma pixel");
        expect(f.v == want_v, "clipped overlay writes only the in-frame chroma pixel");
    }

    // ── Fail-closed validation ────────────────────────────────────────
    {
        FrameFixture f(4, 4, 100, 50, 60);
        OverlayFixture o(2, 2, 200, 180, 190, 255);
        std::string error;

        OverlayFixture odd(3, 3, 200, 180, 190, 255);
        expect(!blendYuvOverlayScalar(f.frame, odd.overlay, Rect{0, 0, 3, 3}, 1.0f, &error),
               "odd overlay dimensions are rejected");
        expect(error.find("invalid prepared overlay") != std::string::npos,
               "odd-dimension error is specific");

        expect(!blendYuvOverlayScalar(f.frame, o.overlay, Rect{0, 0, 3, 2}, 1.0f, &error),
               "placement size mismatch is rejected");

        expect(!blendYuvOverlayScalar(f.frame, o.overlay, Rect{1, 0, 2, 2}, 1.0f, &error),
               "odd placement origin is rejected");
        expect(error.find("even") != std::string::npos,
               "odd-placement error mentions the even-coordinate rule");

        PixelFrame no_y = f.frame;
        no_y.planes[0].data = nullptr;
        expect(!blendYuvOverlayScalar(no_y, o.overlay, Rect{0, 0, 2, 2}, 1.0f, &error),
               "frame missing the Y plane is rejected");
    }

    // ── ImageOverlayKernel integrates placement + opacity ─────────────
    {
        FrameFixture f(4, 4, 100, 50, 60);
        OverlayFixture o(2, 2, 200, 180, 190, 255);
        auto prepared = std::make_shared<const PreparedOverlay>(o.overlay);
        velox::render::ImageOverlayKernel kernel(prepared);

        std::string error;
        // Empty ROI (width/height 0) means the overlay's natural size.
        FrameOp op;
        op.type = FrameOpType::ImageOverlay;
        op.roi = Rect{2, 2, 0, 0};
        op.opacity = 1.0f;
        expect(kernel.apply(f.frame, op, &error), "kernel applies a natural-size overlay");
        expect(f.y[10] == 200 && f.y[11] == 200 && f.y[14] == 200 && f.y[15] == 200,
               "kernel blended into the bottom-right ROI");
        expect(f.y[0] == 100, "kernel left pixels outside the ROI untouched");

        FrameOp wrong;
        wrong.type = FrameOpType::Rectangle;
        wrong.roi = Rect{0, 0, 2, 2};
        expect(!kernel.apply(f.frame, wrong, &error), "kernel rejects a non-overlay op");
        expect(error.find("rectangle") != std::string::npos,
               "kernel error names the wrong op type");

        FrameOp mismatch;
        mismatch.type = FrameOpType::ImageOverlay;
        mismatch.roi = Rect{0, 0, 4, 2};
        expect(!kernel.apply(f.frame, mismatch, &error), "kernel rejects an ROI size mismatch");

        velox::render::ImageOverlayKernel empty(nullptr);
        expect(!empty.apply(f.frame, op, &error), "kernel without a prepared overlay fails closed");
        expect(error.find("no prepared overlay") != std::string::npos,
               "null-overlay error is specific");
    }

    return failures == 0 ? 0 : 1;
}
