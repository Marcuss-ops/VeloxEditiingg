#include "velox/render/frame_overlay.hpp"

#include <cstdint>
#include <cstring>
#include <iostream>
#include <random>
#include <string>
#include <vector>

// Bit-exactness contract (compositor plan §11): the AVX2 kernel and the
// runtime dispatcher must reproduce the scalar reference's bytes identically.
// This test drives the three entry points over randomized frames, overlays,
// placements and opacities and memcmp()s the full Y/U/V output. It is
// deterministic (fixed PRNG seed) and runs the AVX2 kernel directly only when
// the host CPU actually supports AVX2.

namespace {

int failures = 0;

void expect(bool condition, const std::string& message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << "\n";
        ++failures;
    }
}

bool cpuSupportsAvx2() {
#if defined(__GNUC__) || defined(__clang__)
    return __builtin_cpu_supports("avx2");
#else
    return false;
#endif
}

using velox::render::PixelFrame;
using velox::render::PreparedOverlay;
using velox::render::Rect;

// A frame whose planes are owned vectors; copy semantics rebind the planes to
// the copy's own buffers so cloned frames are truly independent.
struct FrameFixture {
    FrameFixture(int w, int h, std::mt19937& rng)
        : w_(w), h_(h),
          y(static_cast<std::size_t>(w) * h),
          u(static_cast<std::size_t>(w / 2) * (h / 2)),
          v(static_cast<std::size_t>(w / 2) * (h / 2)) {
        fill(rng);
        bind();
    }
    FrameFixture(const FrameFixture& other)
        : w_(other.w_), h_(other.h_), y(other.y), u(other.u), v(other.v) {
        bind();
    }

    void fill(std::mt19937& rng) {
        for (auto& b : y) b = static_cast<std::uint8_t>(rng());
        for (auto& b : u) b = static_cast<std::uint8_t>(rng());
        for (auto& b : v) b = static_cast<std::uint8_t>(rng());
    }
    void bind() {
        frame.width = w_;
        frame.height = h_;
        frame.pixel_format = velox::render::kPixelFormatYuv420p;
        frame.planes[0] = {y.data(), w_};
        frame.planes[1] = {u.data(), w_ / 2};
        frame.planes[2] = {v.data(), w_ / 2};
    }

    int w_{0};
    int h_{0};
    std::vector<std::uint8_t> y;
    std::vector<std::uint8_t> u;
    std::vector<std::uint8_t> v;
    PixelFrame frame;
};

struct OverlayFixture {
    OverlayFixture(int ow, int oh, std::mt19937& rng)
        : ow_(ow), oh_(oh),
          y(static_cast<std::size_t>(ow) * oh),
          u(static_cast<std::size_t>(ow / 2) * (oh / 2)),
          v(static_cast<std::size_t>(ow / 2) * (oh / 2)),
          a(static_cast<std::size_t>(ow) * oh) {
        for (auto& b : y) b = static_cast<std::uint8_t>(rng());
        for (auto& b : u) b = static_cast<std::uint8_t>(rng());
        for (auto& b : v) b = static_cast<std::uint8_t>(rng());
        for (auto& b : a) b = static_cast<std::uint8_t>(rng());
        overlay.width = ow;
        overlay.height = oh;
        overlay.pixel_format = velox::render::kPixelFormatYuv420p;
        overlay.compositor_version = 1;
        overlay.planes[0] = {y.data(), ow};
        overlay.planes[1] = {u.data(), ow / 2};
        overlay.planes[2] = {v.data(), ow / 2};
        overlay.planes[3] = {a.data(), ow};
    }

    int ow_{0};
    int oh_{0};
    std::vector<std::uint8_t> y;
    std::vector<std::uint8_t> u;
    std::vector<std::uint8_t> v;
    std::vector<std::uint8_t> a;
    PreparedOverlay overlay;
};

bool framesEqual(const FrameFixture& a, const FrameFixture& b) {
    return std::memcmp(a.y.data(), b.y.data(), a.y.size()) == 0 &&
           std::memcmp(a.u.data(), b.u.data(), a.u.size()) == 0 &&
           std::memcmp(a.v.data(), b.v.data(), a.v.size()) == 0;
}

} // namespace

int main() {
    using velox::render::blendYuvOverlay;
    using velox::render::blendYuvOverlayAVX2;
    using velox::render::blendYuvOverlayScalar;

    const bool avx2 = cpuSupportsAvx2();
    std::cout << (avx2 ? "host supports AVX2 — checking AVX2 vs scalar\n"
                       : "host lacks AVX2 — checking dispatcher vs scalar only\n");

    std::mt19937 rng(0x5EED2026u);
    const int iterations = 1000;

    for (int i = 0; i < iterations; ++i) {
        // Even dims (the 4:2:0 contract) with placements that may overrun
        // the frame to also exercise clipping on every path.
        const int W = static_cast<int>(rng() % 10) * 2 + 8;   // 8..26
        const int H = static_cast<int>(rng() % 10) * 2 + 8;
        const int ow = static_cast<int>(rng() % 5) * 2 + 2;   // 2..10
        const int oh = static_cast<int>(rng() % 5) * 2 + 2;
        const int px = static_cast<int>(rng() % (W + 6)) & ~1;
        const int py = static_cast<int>(rng() % (H + 6)) & ~1;
        const float opacity = static_cast<float>(rng() % 256) / 255.0f;

        FrameFixture scalar(W, H, rng);
        OverlayFixture overlay(ow, oh, rng);
        const Rect placement{px, py, ow, oh};

        FrameFixture dispatched = scalar;
        FrameFixture avx2_frame = scalar;

        std::string error;
        const bool ok_scalar =
            blendYuvOverlayScalar(scalar.frame, overlay.overlay, placement,
                                  opacity, &error);
        const bool ok_dispatch =
            blendYuvOverlay(dispatched.frame, overlay.overlay, placement,
                            opacity, &error);
        expect(ok_scalar, "scalar blend succeeded");
        expect(ok_dispatch, "dispatch blend succeeded");
        expect(framesEqual(scalar, dispatched),
               "dispatcher output is bit-exact with scalar");

        if (avx2) {
            const bool ok_avx2 =
                blendYuvOverlayAVX2(avx2_frame.frame, overlay.overlay,
                                    placement, opacity, &error);
            expect(ok_avx2, "AVX2 blend succeeded");
            expect(framesEqual(scalar, avx2_frame),
                   "AVX2 output is bit-exact with scalar (memcmp == 0)");
        }
    }

    // A deterministic, hand-checked spot case: half-alpha luma blend must
    // yield 150 = (200*128 + 100*127 + 127)/255 on both paths.
    {
        std::mt19937 spot(1);
        FrameFixture scalar(4, 4, spot);
        for (auto& b : scalar.y) b = 100;
        for (auto& b : scalar.u) b = 50;
        for (auto& b : scalar.v) b = 60;
        OverlayFixture overlay(2, 2, spot);
        overlay.y.assign(4, 200);
        overlay.u.assign(1, 180);
        overlay.v.assign(1, 190);
        overlay.a.assign(4, 128);

        FrameFixture dispatched = scalar;
        std::string error;
        blendYuvOverlayScalar(scalar.frame, overlay.overlay, Rect{0, 0, 2, 2},
                              1.0f, &error);
        blendYuvOverlay(dispatched.frame, overlay.overlay, Rect{0, 0, 2, 2},
                        1.0f, &error);
        expect(scalar.y[0] == 150 && scalar.y[1] == 150,
               "spot scalar half-alpha luma == 150");
        expect(dispatched.y[0] == 150 && dispatched.y[1] == 150,
               "spot dispatch half-alpha luma == 150");
    }

    return failures == 0 ? 0 : 1;
}
