// tests/test_ffmpeg_progress_parser.cpp — synthetic stream parser test.
//
// Builds an in-memory synthetic ffmpeg progress stream (chunked into
// varying sizes), feeds it through ProgressParser, asserts each parsed
// EngineProgress against expected values. Also exercises
// SidecarWriter::writeAtomic to confirm an atomic rename actually
// appears on disk.
//
// No gtest dependency — this project doesn't ship GoogleTest. Each
// sub-test prints PASS/FAIL with a clear assertion line. Exit code 0
// iff all tests pass.
//
// Run via the binary velox_ffmpeg_progress_parser_tests.

#include "velox/services/ffmpeg_progress_parser.hpp"

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

namespace fs = std::filesystem;
namespace sv = velox::services;

namespace {

int g_pass = 0;
int g_fail = 0;

#define EXPECT(cond, msg)                                                  \
    do {                                                                   \
        if (!(cond)) {                                                     \
            std::cerr << "FAIL " << __FILE__ << ":" << __LINE__            \
                      << ": " << msg << " (expected: " << #cond << ")\n";  \
            ++g_fail;                                                      \
            return;                                                        \
        }                                                                  \
    } while (0)

#define EXPECT_EQ_INT(actual, expected)                                    \
    do {                                                                   \
        auto _a = (actual);                                                \
        auto _e = (expected);                                              \
        if (_a != _e) {                                                    \
            std::cerr << "FAIL " << __FILE__ << ":" << __LINE__            \
                      << ": int mismatch (got=" << _a                       \
                      << " want=" << _e << ")\n";                          \
            ++g_fail;                                                      \
            return;                                                        \
        }                                                                  \
    } while (0)

#define EXPECT_EQ_STR(actual, expected)                                    \
    do {                                                                   \
        auto _a = (actual);                                                \
        auto _e = (expected);                                              \
        if (_a != _e) {                                                    \
            std::cerr << "FAIL " << __FILE__ << ":" << __LINE__            \
                      << ": str mismatch (got=\"" << _a                     \
                      << "\" want=\"" << _e << "\")\n";                    \
            ++g_fail;                                                      \
            return;                                                        \
        }                                                                  \
    } while (0)

#define SUBCASE(name)                                                      \
    do {                                                                   \
        std::cerr << "── sub-case: " << name << " ──\n";                   \
        ++g_pass;                                                          \
    } while (0)

// ─── synthetic stream helpers ──────────────────────────────────────────

struct SyntheticStream {
    std::vector<std::string> lines;
    size_t expectedBlocks{0};
};

static SyntheticStream buildCanonicalStream() {
    SyntheticStream s;
    s.lines = {
        "frame=0",
        "fps=0",
        "stream_0_0_q=28.0",
        "bitrate=N/A",
        "total_size=0",
        "out_time_us=0",
        "out_time_ms=0",
        "out_time=00:00:00.000000",
        "dup_frames=0",
        "drop_frames=0",
        "speed=0x",
        "progress=continue",

        "frame=120",
        "fps=30.5",
        "bitrate=2048kbits/s",
        "total_size=512000",
        "out_time_us=4003000",
        "out_time_ms=4003",
        "out_time=00:00:04.003000",
        "dup_frames=0",
        "drop_frames=0",
        "speed=1.5x",
        "progress=continue",

        "frame=300",
        "fps=29.97",
        "bitrate=4096kbits/s",
        "total_size=2048000",
        "out_time_us=10010000",
        "out_time_ms=10010",
        "out_time=00:00:10.010000",
        "dup_frames=2",
        "drop_frames=1",
        "speed=0.95x",
        "progress=end",
    };
    s.expectedBlocks = 3;
    return s;
}

static sv::EngineProgress parseWhole(const SyntheticStream& s, int64_t expected_us) {
    sv::ProgressParser parser;
    sv::EngineProgress last{};
    bool any = false;
    parser.setCallback([&](const sv::EngineProgress& p) {
        last = p;
        any = true;
    });
    parser.setExpectedDurationUs(expected_us);
    for (const auto& ln : s.lines) {
        parser.feed(ln + "\n");
    }
    parser.finish();
    (void)any;
    return last;
}

// ─── chunked feed: split each line mid-character to exercise partial lines

static sv::EngineProgress parseChunked(const SyntheticStream& s, int64_t expected_us, size_t chunkBytes) {
    sv::ProgressParser parser;
    sv::EngineProgress last{};
    parser.setCallback([&](const sv::EngineProgress& p) {
        last = p;
    });
    parser.setExpectedDurationUs(expected_us);

    // Concatenate all lines into one big string with \n separators.
    std::string joined;
    for (const auto& ln : s.lines) {
        joined += ln + "\n";
    }
    size_t i = 0;
    while (i < joined.size()) {
        size_t end = std::min(i + chunkBytes, joined.size());
        parser.feed(joined.substr(i, end - i));
        i = end;
    }
    parser.finish();
    return last;
}

// ─── sub-tests ─────────────────────────────────────────────────────────

void testCanonicalWholeStream() {
    SUBCASE("canonical whole stream yields expected final values");

    const int64_t expected_us = 10000000;  // 10s
    sv::EngineProgress p = parseWhole(buildCanonicalStream(), expected_us);

    EXPECT_EQ_INT(p.frame, 300);
    EXPECT(p.fps > 29.9 && p.fps < 30.0, "fps ~= 29.97");
    EXPECT_EQ_STR(p.speed, "0.95x");
    EXPECT(p.speed_x > 0.94 && p.speed_x < 0.96, "speed_x ~= 0.95");
    EXPECT_EQ_INT(p.out_time_us, 10010000);
    EXPECT_EQ_INT(p.out_time_ms, 10010);
    EXPECT_EQ_STR(p.out_time, "00:00:10.010000");
    EXPECT_EQ_INT(p.total_size, 2048000);
    EXPECT_EQ_INT(p.dup_frames, 2);
    EXPECT_EQ_INT(p.drop_frames, 1);
    EXPECT_EQ_INT(p.bitrate, 4096);
    EXPECT(p.finished == true, "progress=end → finished");
    EXPECT(p.progress_pct > 99.0 && p.progress_pct < 101.0, "progress_pct ≈ 100");
}

void testContinuesBeforeEnd() {
    SUBCASE("middle (continue) block carries intermediate values, finished=false");

    sv::ProgressParser parser;
    int seen = 0;
    sv::EngineProgress mid{};
    parser.setCallback([&](const sv::EngineProgress& p) {
        if (++seen == 2) mid = p;
    });
    parser.setExpectedDurationUs(15000000);
    for (const auto& ln : buildCanonicalStream().lines) {
        parser.feed(ln + "\n");
    }
    parser.finish();

    EXPECT_EQ_INT(seen, 3);
    EXPECT_EQ_INT(mid.frame, 120);
    EXPECT_EQ_STR(mid.speed, "1.5x");
    EXPECT(mid.finished == false, "progress=continue → finished=false");
}

void testObservedCount() {
    SUBCASE("observedCount increments on every block");
    sv::ProgressParser parser;
    parser.setCallback([](const sv::EngineProgress&) {});
    for (const auto& ln : buildCanonicalStream().lines) {
        parser.feed(ln + "\n");
    }
    parser.finish();
    EXPECT_EQ_INT(parser.observedCount(), 3);
}

void testChunkedFeedSmallChunks() {
    SUBCASE("chunked feed (1-3 byte chunks) reconstructs identical final block");
    const int64_t expected_us = 10000000;
    auto canonical = buildCanonicalStream();
    sv::EngineProgress p1 = parseChunked(canonical, expected_us, 1);
    sv::EngineProgress p2 = parseChunked(canonical, expected_us, 3);
    sv::EngineProgress p3 = parseChunked(canonical, expected_us, 17);
    EXPECT_EQ_INT(p1.frame, 300);
    EXPECT_EQ_INT(p2.frame, 300);
    EXPECT_EQ_INT(p3.frame, 300);
    EXPECT_EQ_INT(p1.out_time_us, 10010000);
    EXPECT(p1.finished && p2.finished && p3.finished, "all chunks → finished");
}

void testExpectedDurationZeroPct() {
    SUBCASE("expectedDurationUs=0 disables pct computation");
    sv::EngineProgress p = parseWhole(buildCanonicalStream(), 0);
    EXPECT_EQ_INT(static_cast<int>(p.progress_pct), 0);
    EXPECT(p.finished == true, "still finished based on progress=end");
}

void testMalformedValuesTolerated() {
    SUBCASE("malformed numeric values fall back to zero");
    std::vector<std::string> lines = {
        "frame=NaN",
        "fps=foo",
        "speed=N/A",
        "out_time_us=",
        "progress=end",
    };
    sv::ProgressParser parser;
    sv::EngineProgress p{};
    parser.setCallback([&](const sv::EngineProgress& s) { p = s; });
    for (const auto& ln : lines) parser.feed(ln + "\n");
    parser.finish();
    EXPECT_EQ_INT(p.frame, 0);
    EXPECT(p.speed.empty(), "speed stays empty for N/A");
    EXPECT_EQ_INT(p.out_time_us, 0);
    EXPECT(p.finished == true, "progress=end still parsed");
}

void testSidecarWriterAtomic() {
    SUBCASE("SidecarWriter::writeAtomic creates and replaces file atomically");
    fs::path tmpDir = fs::temp_directory_path() / "velox_video_engine_tests" / "sidecar";
    std::error_code ec;
    fs::remove_all(tmpDir, ec);
    fs::create_directories(tmpDir, ec);
    fs::path target = tmpDir / "video.mp4.progress.json";

    // Write first payload.
    EXPECT(sv::SidecarWriter::writeAtomic(target, "{\"progress\":100}"), "first write succeeds");
    EXPECT(fs::exists(target), "target exists");
    EXPECT(!fs::exists(fs::path(target.string() + ".tmp")), "no tmp left behind");

    // Verify content.
    {
        std::ifstream in(target);
        std::stringstream ss; ss << in.rdbuf();
        EXPECT_EQ_STR(ss.str(), "{\"progress\":100}");
    }

    // Overwrite.
    EXPECT(sv::SidecarWriter::writeAtomic(target, "{\"progress\":42}"), "second write succeeds");
    EXPECT(!fs::exists(fs::path(target.string() + ".tmp")), "no tmp after overwrite");
    {
        std::ifstream in(target);
        std::stringstream ss; ss << in.rdbuf();
        EXPECT_EQ_STR(ss.str(), "{\"progress\":42}");
    }
}

void testEscapeProgressJsonString() {
    SUBCASE("escapeProgressJsonString escapes quotes, backslashes, control chars");
    std::string raw = "a\"b\\c\nd\te";
    EXPECT_EQ_STR(
        sv::escapeProgressJsonString(raw),
        "a\\\"b\\\\c\\nd\\te");
    EXPECT_EQ_STR(sv::escapeProgressJsonString(""), "");
    EXPECT_EQ_STR(sv::escapeProgressJsonString("plain ASCII"), "plain ASCII");
}

} // namespace

int main() {
    std::cerr << "running ffmpeg_progress_parser tests\n";

    testCanonicalWholeStream();
    testContinuesBeforeEnd();
    testObservedCount();
    testChunkedFeedSmallChunks();
    testExpectedDurationZeroPct();
    testMalformedValuesTolerated();
    testSidecarWriterAtomic();
    testEscapeProgressJsonString();

    std::cerr << "\nsummary: pass=" << g_pass << " fail=" << g_fail << "\n";
    return g_fail == 0 ? 0 : 1;
}
