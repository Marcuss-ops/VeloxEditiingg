// Component-level tests for the named packet pipeline pieces:
//   Demuxer           in-process AVPacket source
//   PacketTrimmer     trim window enforcement inside rewritePacket
//   TimestampRewriter source_start removal, rescale, monotonic ordering
//
// The rewrite pass is exercised with pure synthetic AVPackets against
// minimal in-memory AVStreams (no media files), so every edge case is
// deterministic. The Demuxer and streamAndRewrite are exercised against one
// real ffmpeg-generated fixture (generated BEFORE the component test runs;
// the components themselves never spawn media processes).

#include "velox/services/file_utils.hpp"
#include "velox/services/io_counters.hpp"
#include "velox/services/media_packet_components.hpp"
#include "velox/services/media_packet_cursors.hpp"

extern "C" {
#include <libavformat/avformat.h>
#include <libavutil/avutil.h>
}

#include <chrono>
#include <cstdlib>
#include <algorithm>
#include <functional>
#include <vector>
#include <filesystem>
#include <iostream>
#include <sstream>
#include <string>

namespace fs = std::filesystem;

namespace {

int failures = 0;

void expect(bool condition, const std::string& message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << "\n";
        ++failures;
    }
}

std::string uniqueStem() {
    return "velox_packet_components_" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
}

bool makeMuxedVideo(const fs::path& output) {
    std::ostringstream command;
    command << "ffmpeg -y -hide_banner -loglevel error"
            << " -f lavfi -i " << velox::file::shellQuote("testsrc=size=64x64:rate=5:duration=1.2")
            << " -f lavfi -i " << velox::file::shellQuote("sine=frequency=440:sample_rate=48000")
            << " -map 0:v:0 -map 1:a:0 -c:v libx264 -preset ultrafast -pix_fmt yuv420p"
            << " -c:a aac -ar 48000 -ac 2 -shortest "
            << velox::file::shellQuote(output.string());
    return velox::file::runCommand(command.str());
}

// ── TimestampRewriter / PacketTrimmer pure unit harness ──────────────────
// Minimal in-memory streams: input ticks in milliseconds, output ticks in
// microseconds (the pipeline's canonical base).
struct RewriteHarness {
    AVFormatContext* context = nullptr;
    AVStream* input_stream = nullptr;
    AVStream* output_stream = nullptr;

    RewriteHarness() {
        context = avformat_alloc_context();
        input_stream = avformat_new_stream(context, nullptr);
        output_stream = avformat_new_stream(context, nullptr);
        input_stream->time_base = AVRational{1, 1000};        // 1 ms ticks
        output_stream->time_base = AVRational{1, 1'000'000};  // 1 us ticks
    }

    ~RewriteHarness() {
        avformat_free_context(context);
    }
};

void testRewriteSourceStartRemoval() {
    RewriteHarness h;
    h.input_stream->start_time = 500;  // 500 ms non-zero stream start
    velox::media::packet::TimestampState state;
    AVPacket pkt{};
    pkt.pts = 1500;  // 1.0 s relative
    pkt.dts = 1400;  // 0.9 s relative
    int64_t sort = AV_NOPTS_VALUE;
    const int64_t offset = 5'000'000;  // 5 s on the output timeline
    const int64_t duration = 10'000'000;
    expect(velox::media::packet::rewritePacket(
               pkt, h.input_stream, h.output_stream, h.input_stream->start_time,
               0, offset, duration, state, sort) ==
               velox::media::packet::PacketRewriteDecision::Accepted,
           "packet inside the window is accepted");
    expect(pkt.pts == 6'000'000,
           "pts is (1.5s - 0.5s) + 5s = 6s in microseconds (actual " + std::to_string(pkt.pts) + ")");
    expect(pkt.dts == 5'900'000,
           "dts is (1.4s - 0.5s) + 5s = 5.9s in microseconds (actual " + std::to_string(pkt.dts) + ")");
    expect(pkt.stream_index == h.output_stream->index,
           "accepted packet is bound to the output stream");
    expect(sort == pkt.dts, "sort key follows the rewritten dts");
    expect(state.last_pts == pkt.pts && state.last_dts == pkt.dts,
           "accepted packet advances the monotonic state");
}

void testRewriteBoundaryExclusion() {
    RewriteHarness h;
    velox::media::packet::TimestampState state;
    AVPacket pkt{};
    pkt.pts = 10'000;  // 10 s relative = exactly the segment end
    pkt.dts = 10'000;
    int64_t sort = AV_NOPTS_VALUE;
    expect(velox::media::packet::rewritePacket(
               pkt, h.input_stream, h.output_stream, 0, 0, 0, 10'000'000, state, sort) ==
               velox::media::packet::PacketRewriteDecision::AfterWindow,
           "packet exactly at the trim boundary is AfterWindow on both clocks");
    expect(state.last_pts == AV_NOPTS_VALUE && state.last_dts == AV_NOPTS_VALUE,
           "rejected packet does not advance the monotonic state");
}

void testRewriteNegativePrefixClamp() {
    RewriteHarness h;
    velox::media::packet::TimestampState state;
    AVPacket pkt{};
    pkt.pts = -200;  // decoder priming before time zero
    pkt.dts = -200;
    int64_t sort = AV_NOPTS_VALUE;
    expect(velox::media::packet::rewritePacket(
               pkt, h.input_stream, h.output_stream, 0, 0, 0, 10'000'000, state, sort) ==
               velox::media::packet::PacketRewriteDecision::Accepted,
           "negative-prefix packet inside the window is accepted");
    expect(pkt.pts == 0 && pkt.dts == 0,
           "negative timestamps are clamped to the timeline origin");
}

void testRewriteBFrameOrdering() {
    RewriteHarness h;
    velox::media::packet::TimestampState state;
    AVPacket pkt{};
    pkt.pts = 3000;  // B-frame: presentation before decoding order
    pkt.dts = 3500;
    int64_t sort = AV_NOPTS_VALUE;
    expect(velox::media::packet::rewritePacket(
               pkt, h.input_stream, h.output_stream, 0, 0, 0, 10'000'000, state, sort) ==
               velox::media::packet::PacketRewriteDecision::Accepted,
           "reordered packet inside the window is accepted");
    expect(pkt.pts == 3'500'000 && pkt.dts == 3'500'000,
           "pts below dts is clamped to dts (B-frame ordering preserved for the muxer)");
}

void testRewriteMonotonicDuplicates() {
    RewriteHarness h;
    velox::media::packet::TimestampState state;
    AVPacket first{};
    first.pts = 1000;
    first.dts = 1000;
    int64_t firstSort = AV_NOPTS_VALUE;
    expect(velox::media::packet::rewritePacket(
               first, h.input_stream, h.output_stream, 0, 0, 0, 10'000'000, state, firstSort) ==
               velox::media::packet::PacketRewriteDecision::Accepted,
           "first packet is accepted");
    AVPacket duplicate{};
    duplicate.pts = 1000;  // same dts/pts as the accepted packet
    duplicate.dts = 1000;
    int64_t duplicateSort = AV_NOPTS_VALUE;
    expect(velox::media::packet::rewritePacket(
               duplicate, h.input_stream, h.output_stream, 0, 0, 0, 10'000'000, state, duplicateSort) ==
               velox::media::packet::PacketRewriteDecision::Accepted,
           "duplicate-timestamp packet is accepted");
    expect(duplicate.pts == 1'000'001 && duplicate.dts == 1'000'001,
           "duplicate timestamps are bumped to keep the stream strictly monotonic");
}

void testRewriteDurationClampAtEnd() {
    RewriteHarness h;
    velox::media::packet::TimestampState state;
    AVPacket pkt{};
    pkt.pts = 9'990;  // 9.99 s
    pkt.dts = 9'990;
    pkt.duration = 200;  // 200 ms would cross the 10 s end
    int64_t sort = AV_NOPTS_VALUE;
    expect(velox::media::packet::rewritePacket(
               pkt, h.input_stream, h.output_stream, 0, 0, 0, 10'000'000, state, sort) ==
               velox::media::packet::PacketRewriteDecision::Accepted,
           "packet crossing the end is accepted with a clamped duration");
    expect(pkt.pts == 9'990'000 && pkt.duration == 10'000,
           "last packet duration is clamped to the segment end (actual duration " +
               std::to_string(pkt.duration) + ")");
}

void testRewriteTimelineOffsetAccumulation() {
    RewriteHarness h;
    velox::media::packet::TimestampState state;
    AVPacket segmentTwo{};
    segmentTwo.pts = 200;  // 0.2 s into the second 0.8 s segment
    segmentTwo.dts = 200;
    int64_t sort = AV_NOPTS_VALUE;
    const int64_t secondOffset = 800'000;  // segments are 0.8 s each
    expect(velox::media::packet::rewritePacket(
               segmentTwo, h.input_stream, h.output_stream, 0, 0,
               secondOffset, 800'000, state, sort) ==
               velox::media::packet::PacketRewriteDecision::Accepted,
           "second-segment packet inside its window is accepted");
    expect(segmentTwo.pts == 1'000'000,
           "second-segment packet lands at 0.8s + 0.2s = 1s on the shared timeline");
}

void testRewriteMissingTimestamps() {
    RewriteHarness h;
    velox::media::packet::TimestampState state;

    AVPacket ptsOnly{};
    ptsOnly.pts = 250;
    ptsOnly.dts = AV_NOPTS_VALUE;
    int64_t ptsOnlySort = AV_NOPTS_VALUE;
    expect(velox::media::packet::rewritePacket(
               ptsOnly, h.input_stream, h.output_stream, 0, 0, 0,
               10'000'000, state, ptsOnlySort) ==
               velox::media::packet::PacketRewriteDecision::Accepted,
           "pts-only packet uses pts as its trim reference");
    expect(ptsOnly.pts == 250'000 && ptsOnly.dts == AV_NOPTS_VALUE,
           "pts-only packet preserves missing dts and rescales pts");
    expect(ptsOnlySort == ptsOnly.pts,
           "pts-only packet uses pts as sort key");
    expect(state.last_pts == ptsOnly.pts && state.last_dts == AV_NOPTS_VALUE,
           "pts-only packet advances only the pts monotonic state");

    AVPacket dtsOnly{};
    dtsOnly.pts = AV_NOPTS_VALUE;
    dtsOnly.dts = 500;
    int64_t sort = AV_NOPTS_VALUE;
    expect(velox::media::packet::rewritePacket(
               dtsOnly, h.input_stream, h.output_stream, 0, 0, 0,
               10'000'000, state, sort) ==
               velox::media::packet::PacketRewriteDecision::Accepted,
           "dts-only packet uses dts as its trim reference");
    expect(dtsOnly.pts == AV_NOPTS_VALUE && dtsOnly.dts == 500'000,
           "dts-only packet preserves missing pts and rescales dts");
    expect(sort == dtsOnly.dts, "dts-only packet uses dts as sort key");

    AVPacket noTimestamp{};
    noTimestamp.pts = AV_NOPTS_VALUE;
    noTimestamp.dts = AV_NOPTS_VALUE;
    int64_t noTimestampSort = AV_NOPTS_VALUE;
    expect(velox::media::packet::rewritePacket(
               noTimestamp, h.input_stream, h.output_stream, 0, 0, 0,
               10'000'000, state, noTimestampSort) ==
               velox::media::packet::PacketRewriteDecision::BeforeWindow,
           "packet without pts and dts is rejected as BeforeWindow");
}

void testRewriteCombinedSourceAndTimelineOffsets() {
    RewriteHarness h;
    velox::media::packet::TimestampState state;
    AVPacket packet{};
    packet.pts = 1'700;
    packet.dts = 1'600;
    int64_t sort = AV_NOPTS_VALUE;
    expect(velox::media::packet::rewritePacket(
               packet, h.input_stream, h.output_stream, 500, 300'000,
               2'000'000, 1'000'000, state, sort) ==
               velox::media::packet::PacketRewriteDecision::Accepted,
           "packet with source and timeline offsets is accepted");
    expect(packet.pts == 2'900'000 && packet.dts == 2'800'000,
           "source start and trim are removed before timeline offset is added");
    expect(sort == packet.dts,
           "combined-offset packet keeps dts as its sort key");

    AVPacket outside{};
    outside.pts = 700;
    outside.dts = 700;
    int64_t outsideSort = AV_NOPTS_VALUE;
    expect(velox::media::packet::rewritePacket(
               outside, h.input_stream, h.output_stream, 500, 300'000,
               2'000'000, 1'000'000, state, outsideSort) ==
               velox::media::packet::PacketRewriteDecision::BeforeWindow,
           "packet before the trimmed source window is rejected as BeforeWindow");
    expect(outsideSort == AV_NOPTS_VALUE,
           "rejected offset packet does not receive a sort key");
}

void testRewriteSourceWindowBeforeStart() {
    RewriteHarness h;
    velox::media::packet::TimestampState state;
    AVPacket beforeWindow{};
    beforeWindow.pts = 100;
    beforeWindow.dts = 100;
    int64_t sort = AV_NOPTS_VALUE;
    expect(velox::media::packet::rewritePacket(
               beforeWindow, h.input_stream, h.output_stream, 0,
               200'000, 0, 800'000, state, sort) ==
               velox::media::packet::PacketRewriteDecision::BeforeWindow,
           "packet before a positive source window is rejected as BeforeWindow");
    expect(state.last_pts == AV_NOPTS_VALUE && state.last_dts == AV_NOPTS_VALUE,
           "packet before a source window does not update monotonic state");
}

// rewritePacket window classification: AfterWindow must fire ONLY when both
// the decode (dts) and presentation (pts) clocks are past the window end,
// so the demux can stop without dropping B-frame interleaves.
void testRewriteDecisionClassification() {
    RewriteHarness h;
    velox::media::packet::TimestampState state;

    // Both clocks past the window end -> the demux may stop.
    AVPacket past{};
    past.pts = 12'000;
    past.dts = 12'000;
    int64_t pastSort = AV_NOPTS_VALUE;
    expect(velox::media::packet::rewritePacket(
               past, h.input_stream, h.output_stream, 0, 0, 0,
               10'000'000, state, pastSort) ==
               velox::media::packet::PacketRewriteDecision::AfterWindow,
           "packet past the window on both clocks is AfterWindow");
    expect(state.last_pts == AV_NOPTS_VALUE && state.last_dts == AV_NOPTS_VALUE,
           "AfterWindow packet does not advance the monotonic state");

    // B-frame safety (anchor): presenting past the window (PTS >= end) while
    // decoding inside it (DTS < end). In decode order the B-frames that
    // present inside the window come AFTER this anchor, so the demux must
    // keep scanning: never AfterWindow on pts alone.
    AVPacket anchorPast{};
    anchorPast.pts = 3'000;
    anchorPast.dts = 1'000;
    int64_t anchorSort = AV_NOPTS_VALUE;
    expect(velox::media::packet::rewritePacket(
               anchorPast, h.input_stream, h.output_stream, 0, 0, 0,
               3'000'000, state, anchorSort) ==
               velox::media::packet::PacketRewriteDecision::BeforeWindow,
           "anchor past on pts but inside on dts stays BeforeWindow");

    // B-frame fully inside the window on both clocks is accepted (the
    // pts<dts clamp raises pts to the decode clock, still inside).
    AVPacket bframeInside{};
    bframeInside.pts = 1'000;
    bframeInside.dts = 2'000;
    int64_t bframeSort = AV_NOPTS_VALUE;
    expect(velox::media::packet::rewritePacket(
               bframeInside, h.input_stream, h.output_stream, 0, 0, 0,
               3'000'000, state, bframeSort) ==
               velox::media::packet::PacketRewriteDecision::Accepted,
           "B-frame inside on both clocks is accepted");
    expect(state.last_dts == 2'000'000 && state.last_pts == 2'000'000,
           "accepted B-frame advances the monotonic state to its clamped dts");

    // Trailing B-frame whose dts reaches the window end: the existing
    // pts<dts clamp raises pts to the end, so the packet is dropped and
    // classified AfterWindow — safe to stop because every later packet
    // presents at or past the end.
    AVPacket bframeTrailing{};
    bframeTrailing.pts = 2'000;
    bframeTrailing.dts = 3'000;
    int64_t trailingSort = AV_NOPTS_VALUE;
    expect(velox::media::packet::rewritePacket(
               bframeTrailing, h.input_stream, h.output_stream, 0, 0, 0,
               3'000'000, state, trailingSort) ==
               velox::media::packet::PacketRewriteDecision::AfterWindow,
           "trailing B-frame clamped to the window end is AfterWindow");
    expect(state.last_dts == 2'000'000,
           "AfterWindow rejection does not advance the monotonic state");

    // A pts-only packet past the window cannot prove the decode clock is
    // past it, so it stays BeforeWindow (conservative, keeps scanning).
    AVPacket ptsOnlyPast{};
    ptsOnlyPast.pts = 4'000;
    ptsOnlyPast.dts = AV_NOPTS_VALUE;
    int64_t ptsOnlySort = AV_NOPTS_VALUE;
    expect(velox::media::packet::rewritePacket(
               ptsOnlyPast, h.input_stream, h.output_stream, 0, 0, 0,
               3'000'000, state, ptsOnlySort) ==
               velox::media::packet::PacketRewriteDecision::BeforeWindow,
           "pts-only packet past the window stays BeforeWindow");
}

// ── Demuxer + streaming reader against a real fixture ────────────────────

void testDemuxer(const fs::path& fixture) {
    velox::media::packet::Demuxer demuxer;
    std::string error;
    expect(demuxer.open(fixture, error), "demuxer opens the fixture: " + error);
    expect(demuxer.isOpen(), "demuxer reports open after successful open");

    const int videoIndex = demuxer.firstStream(AVMEDIA_TYPE_VIDEO);
    const int audioIndex = demuxer.firstStream(AVMEDIA_TYPE_AUDIO);
    expect(videoIndex == 0, "first video stream is stream 0");
    expect(audioIndex == 1, "first audio stream is stream 1");
    expect(demuxer.stream(videoIndex) != nullptr &&
               demuxer.stream(videoIndex)->codecpar->codec_type == AVMEDIA_TYPE_VIDEO,
           "stream() resolves the video stream descriptor");
    expect(demuxer.stream(-1) == nullptr && demuxer.stream(99) == nullptr,
           "out-of-range stream lookup returns null");

    AVPacket* packet = av_packet_alloc();
    bool eof = false;
    int frames = 0;
    while (packet != nullptr && !eof) {
        std::string readError;
        if (!demuxer.readFrame(*packet, eof, readError)) {
            expect(false, "demuxer readFrame fails: " + readError);
            break;
        }
        if (!eof) {
            ++frames;
        }
        av_packet_unref(packet);
    }
    expect(frames > 0, "demuxer yields raw packets from the fixture");
    expect(eof, "demuxer reports end of input exactly once");
    av_packet_free(&packet);

    demuxer.close();
    expect(!demuxer.isOpen(), "demuxer closes cleanly");
    expect(demuxer.firstStream(AVMEDIA_TYPE_VIDEO) == -1, "closed demuxer resolves no streams");
    expect(!demuxer.open(fs::path("/nonexistent/velox-missing.mp4"), error) && !error.empty(),
           "opening a missing file fails with an error");
}

struct StreamingCapture {
    std::vector<int64_t> pts;
    std::vector<int64_t> dts;
    int max_ready{0};
};

bool captureStreamingPacket(velox::media::packet::PendingPacket& pending,
                            void* opaque, std::string&) {
    auto& capture = *static_cast<StreamingCapture*>(opaque);
    capture.max_ready = std::max(capture.max_ready, 1);
    capture.pts.push_back(pending.packet.pts);
    capture.dts.push_back(pending.packet.dts);
    return true;
}

void testBoundedStreamingCursor(const fs::path& fixture) {
    velox::media::packet::Demuxer demuxer;
    std::string error;
    expect(demuxer.open(fixture, error), "streaming cursor opens fixture: " + error);
    AVFormatContext* outputContext = avformat_alloc_context();
    AVStream* output = avformat_new_stream(outputContext, nullptr);
    output->time_base = velox::media::packet::kMicrosecondTimeBase;
    output->codecpar->codec_id = AV_CODEC_ID_H264;
    output->codecpar->codec_type = AVMEDIA_TYPE_VIDEO;
    StreamingCapture capture;
    velox::media::packet::TimestampState state;
    int64_t packetCount = 0;
    expect(velox::media::packet::streamAndRewrite(
               demuxer, fixture, AVMEDIA_TYPE_VIDEO, 0, output, 0, 0,
               1'000'000, state, captureStreamingPacket, &capture,
               packetCount, error),
           "bounded cursor streams packets without collecting them: " + error);
    expect(packetCount > 0 && static_cast<int64_t>(capture.pts.size()) == packetCount,
           "stream callback observes every accepted packet");
    expect(capture.max_ready == 1,
           "stream callback exposes at most one pending packet");
    expect(std::is_sorted(capture.dts.begin(), capture.dts.end()),
           "streamed DTS values are deterministic and ordered");
    avformat_free_context(outputContext);
}

void testStreamingBenchmark(const fs::path& fixture) {
    velox::media::services::resetIOCounters();
    velox::media::packet::Demuxer demuxer;
    std::string error;
    expect(demuxer.open(fixture, error), "benchmark cursor opens fixture: " + error);
    AVFormatContext* outputContext = avformat_alloc_context();
    AVStream* output = avformat_new_stream(outputContext, nullptr);
    output->time_base = velox::media::packet::kMicrosecondTimeBase;
    output->codecpar->codec_id = AV_CODEC_ID_H264;
    output->codecpar->codec_type = AVMEDIA_TYPE_VIDEO;
    StreamingCapture capture;
    velox::media::packet::TimestampState state;
    int64_t packetCount = 0;
    const auto started = std::chrono::steady_clock::now();
    expect(velox::media::packet::streamAndRewrite(
               demuxer, fixture, AVMEDIA_TYPE_VIDEO, 0, output, 0, 0,
               1'000'000, state, captureStreamingPacket, &capture,
               packetCount, error),
           "benchmark cursor completes: " + error);
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - started).count();
    expect(packetCount > 0 && elapsed >= 0, "bounded mux benchmark records a valid run");
    expect(velox::media::services::ioCounters().input_open_count.load() == 1,
           "bounded benchmark uses one input open");
    std::cerr << "bounded_mux_benchmark packets=" << packetCount
              << " elapsed_ms=" << elapsed << " max_pending=1\n";
    avformat_free_context(outputContext);
}

} // namespace

int main() {
    const fs::path root = fs::temp_directory_path() / uniqueStem();
    std::error_code ec;
    fs::create_directories(root, ec);
    expect(!ec, "temporary directory can be created");
    if (ec) return 1;

    struct Cleanup {
        fs::path root;
        ~Cleanup() {
            std::error_code ec;
            fs::remove_all(root, ec);
        }
    } cleanup{root};

    testRewriteSourceStartRemoval();
    testRewriteBoundaryExclusion();
    testRewriteNegativePrefixClamp();
    testRewriteBFrameOrdering();
    testRewriteMonotonicDuplicates();
    testRewriteDurationClampAtEnd();
    testRewriteTimelineOffsetAccumulation();
    testRewriteMissingTimestamps();
    testRewriteCombinedSourceAndTimelineOffsets();
    testRewriteSourceWindowBeforeStart();
    testRewriteDecisionClassification();

    const fs::path fixture = root / "fixture.mp4";
    expect(makeMuxedVideo(fixture), "muxed video/audio fixture can be created");
    testDemuxer(fixture);
    testBoundedStreamingCursor(fixture);
    testStreamingBenchmark(fixture);

    std::cerr << "summary: fail=" << failures << "\n";
    return failures == 0 ? 0 : 1;
}
