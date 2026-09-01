// test_streaming_packet_mux.cpp — certified bounded-mux test suite for the
// canonical packet-copy path (VideoTimelineCursor / AudioTimelineCursor /
// single interleaved writer) driven through the stable public boundary
// MediaPacketPipeline::muxCopyOnly.
//
// The streaming mux replaced the legacy collect-all-then-sort path. These
// tests pin the public contract AND the O(1)-memory invariants exposed on
// CopyOnlyMuxResult. Shared fixture/probe/decoder assertions live in
// streaming_packet_mux_test_support.hpp; this file owns the scenarios.

#include "streaming_packet_mux_test_support.hpp"

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

    const fs::path video = root / "video.mp4";
    const fs::path bframeVideo = root / "bframes.mp4";
    const fs::path audio = root / "audio.m4a";
    const fs::path videoWithAudio = root / "video-with-audio.mp4";
    const fs::path tbA = root / "timescale-90000.mp4";
    const fs::path tbB = root / "timescale-15360.mp4";
    expect(makeVideo(video, "64x64", 5), "video fixture can be created");
    expect(makeBFrameVideo(bframeVideo), "B-frame fixture can be created");
    expect(makeAudio(audio), "audio fixture can be created");
    expect(makeMuxedVideo(video, audio, videoWithAudio),
           "video-with-audio fixture can be created");
    expect(makeVideo(tbA, "64x64", 25, "-video_track_timescale 90000"),
           "90000-timescale video fixture can be created");
    expect(makeVideo(tbB, "64x64", 25, "-video_track_timescale 15360"),
           "15360-timescale video fixture can be created");

    const AVRational tbATimeBase = streamTimeBase(tbA, AVMEDIA_TYPE_VIDEO);
    const AVRational tbBTimeBase = streamTimeBase(tbB, AVMEDIA_TYPE_VIDEO);
    expect(!sameRational(tbATimeBase, tbBTimeBase),
           "timebase fixtures really differ (" + rationalString(tbATimeBase) +
               " vs " + rationalString(tbBTimeBase) + ")");
    const AVRational audioTimeBase = streamTimeBase(audio, AVMEDIA_TYPE_AUDIO);
    const AVRational videoTimeBase = streamTimeBase(video, AVMEDIA_TYPE_VIDEO);
    expect(!sameRational(audioTimeBase, videoTimeBase),
           "audio and video fixtures use different input timebases (" +
               rationalString(audioTimeBase) + " vs " + rationalString(videoTimeBase) + ")");

    // --- video only (also covers the repeated-source-clip case) -----------
    {
        const fs::path output = root / "video-only.mp4";
        velox::media::CopyOnlyMuxRequest request;
        request.video_segments = {{video, 0, 800'000}, {video, 0, 800'000}};
        request.output_path = output;
        velox::media::CopyOnlyMuxResult result;
        expect(velox::media::muxCopyOnly(request, &result),
               "video-only streaming mux succeeds");
        checkOutput(output, result, "video-only");
        const auto probe = velox::media::probeMediaInProcess(output);
        expectTimeline(result, probe, 1'600'000, "video-only");
        const auto inspected = inspectStreams(output);
        expect(inspected.video_packets > 0, "video-only mux writes video packets");
        expect(result.audio_packets == 0 && inspected.audio_packets == 0,
               "video-only mux writes no audio packets");
        checkStreams(inspected, result, "video-only");
    }

    // --- write progress callback fires with .partial path ------------------
    {
        const fs::path output = root / "progress-callback.mp4";
        velox::media::CopyOnlyMuxRequest request;
        request.video_segments = {{video, 0, 800'000}};
        request.output_path = output;

        int callbackCount = 0;
        int64_t lastBytes = 0;
        bool pathContainsPartial = false;
        bool pathIsAbsolute = false;
        bool bytesMonotonic = true;

        request.write_progress_callback = [&](const fs::path& path, int64_t bytes_written) {
            ++callbackCount;
            if (path.string().find(".partial") != std::string::npos) {
                pathContainsPartial = true;
            }
            pathIsAbsolute = path.is_absolute();
            if (bytes_written < lastBytes) {
                bytesMonotonic = false;
            }
            lastBytes = bytes_written;
        };

        velox::media::CopyOnlyMuxResult result;
        expect(velox::media::muxCopyOnly(request, &result),
               "progress-callback mux succeeds");
        checkOutput(output, result, "progress-callback");
        expect(callbackCount > 0,
               "progress-callback: callback invoked (count=" +
                   std::to_string(callbackCount) + ")");
        expect(pathContainsPartial,
               "progress-callback: path contains .partial");
        expect(pathIsAbsolute,
               "progress-callback: path is absolute");
        expect(bytesMonotonic,
               "progress-callback: bytes_written is monotonic");
        expect(lastBytes == result.output_size_bytes,
               "progress-callback: final bytes match output size (last=" +
                   std::to_string(lastBytes) + " output=" +
                   std::to_string(result.output_size_bytes) + ")");
        expect(fs::exists(output),
               "progress-callback: final output exists after mux");
    }

    // --- video + final voiceover ------------------------------------------
    {
        const fs::path output = root / "voiceover.mp4";
        velox::media::CopyOnlyMuxRequest request;
        request.video_segments = {{video, 0, 800'000}, {video, 0, 800'000}};
        request.audio = velox::media::CopyOnlyAudioTrack{audio, 0, 1'600'000};
        request.output_path = output;
        velox::media::CopyOnlyMuxResult result;
        expect(velox::media::muxCopyOnly(request, &result),
               "voiceover streaming mux succeeds");
        checkOutput(output, result, "voiceover");
        const auto probe = velox::media::probeMediaInProcess(output);
        expectTimeline(result, probe, 1'600'000, "voiceover");
        const auto inspected = inspectStreams(output);
        expect(inspected.audio_packets > 0 && result.audio_packets > 0,
               "voiceover mux writes the final audio track");
        checkStreams(inspected, result, "voiceover");
    }

    // --- video + source audio ---------------------------------------------
    {
        const fs::path output = root / "source-audio.mp4";
        velox::media::CopyOnlyMuxRequest request;
        request.video_segments = {
            {videoWithAudio, 0, 800'000, true},
            {videoWithAudio, 0, 800'000, true},
        };
        request.output_path = output;
        velox::media::CopyOnlyMuxResult result;
        expect(velox::media::muxCopyOnly(request, &result),
               "source-audio streaming mux succeeds");
        checkOutput(output, result, "source-audio");
        const auto probe = velox::media::probeMediaInProcess(output);
        expectTimeline(result, probe, 1'600'000, "source-audio");
        expect(probe.has_value() && probe->streams.size() == 2,
               "source-audio output has video and audio streams");
        const auto inspected = inspectStreams(output);
        expect(inspected.audio_packets > 0 && result.audio_packets > 0,
               "source-audio mux writes the per-segment audio");
        checkStreams(inspected, result, "source-audio");
    }

    // --- B-frames ----------------------------------------------------------
    {
        const fs::path output = root / "bframes.mp4";
        velox::media::CopyOnlyMuxRequest request;
        request.video_segments = {{bframeVideo, 0, 1'600'000}};
        request.output_path = output;
        velox::media::CopyOnlyMuxResult result;
        expect(velox::media::muxCopyOnly(request, &result),
               "B-frame streaming mux succeeds");
        checkOutput(output, result, "B-frame");
        const auto probe = velox::media::probeMediaInProcess(output);
        expectTimeline(result, probe, 1'600'000, "B-frame");
        const auto inspected = inspectStreams(output);
        expect(inspected.video_packets > 0,
               "B-frame mux writes video packets");
        checkStreams(inspected, result, "B-frame");
    }

    // --- different input timebases ----------------------------------------
    {
        const fs::path output = root / "timescale-same.mp4";
        velox::media::CopyOnlyMuxRequest request;
        request.video_segments = {{tbA, 0, 800'000}, {tbA, 0, 800'000}, {tbA, 0, 800'000}};
        request.output_path = output;
        velox::media::CopyOnlyMuxResult result;
        expect(velox::media::muxCopyOnly(request, &result),
               "non-default track timescale streaming mux succeeds");
        checkOutput(output, result, "timescale-same");
        const auto probe = velox::media::probeMediaInProcess(output);
        expectTimeline(result, probe, 2'400'000, "timescale-same");
        const auto inspected = inspectStreams(output);
        expect(inspected.video_packets > 0,
               "non-default timescale mux writes video packets");
        checkStreams(inspected, result, "timescale-same");
    }
    {
        const fs::path output = root / "timescale-mismatch.mp4";
        velox::media::CopyOnlyMuxRequest request;
        request.video_segments = {{tbA, 0, 800'000}, {tbB, 0, 800'000}};
        request.output_path = output;
        velox::media::CopyOnlyMuxResult result;
        expect(!velox::media::muxCopyOnly(request, &result),
               "incompatible video timebases fail closed");
        expect(!result.success && !result.error.empty(),
               "incompatible timebase failure includes an error");
        expect(result.error.find("media signature mismatch: time_base") != std::string::npos,
               "incompatible timebase failure explains the exact mismatch (actual=" +
                   result.error + ")");
        expect(!fs::exists(output),
               "incompatible timebase failure does not publish output");
    }

    // --- keyframe cuts ------------------------------------------------------
    {
        const fs::path output = root / "keyframe-cut.mp4";
        velox::media::CopyOnlyMuxRequest request;
        request.video_segments = {{video, 400'000, 400'000}};
        request.output_path = output;
        velox::media::CopyOnlyMuxResult result;
        expect(velox::media::muxCopyOnly(request, &result),
               "exact keyframe cut streaming mux succeeds");
        checkOutput(output, result, "keyframe-cut");
        const auto probe = velox::media::probeMediaInProcess(output);
        expectTimeline(result, probe, 400'000, "keyframe-cut");
        const auto inspected = inspectStreams(output);
        expect(inspected.video_packets > 0,
               "keyframe cut mux writes video packets");
        checkStreams(inspected, result, "keyframe-cut");
    }

    // --- non-keyframe reject -------------------------------------------------
    {
        const fs::path output = root / "non-keyframe.mp4";
        velox::media::CopyOnlyMuxRequest request;
        request.video_segments = {{video, 200'000, 400'000}};
        request.output_path = output;
        velox::media::CopyOnlyMuxResult result;
        expect(!velox::media::muxCopyOnly(request, &result),
               "non-keyframe source window fails closed");
        expect(!result.success && !result.error.empty(),
               "non-keyframe rejection includes an error");
        expect(result.error.find("keyframe") != std::string::npos,
               "non-keyframe rejection explains the correctness requirement");
        expect(!fs::exists(output),
               "non-keyframe rejection does not publish an output");
    }

    // --- tail extension ------------------------------------------------------
    {
        const fs::path output = root / "tail-extension.mp4";
        velox::media::CopyOnlyMuxRequest request;
        request.video_segments = {{video, 0, 1'800'000}};
        request.output_path = output;
        velox::media::CopyOnlyMuxResult result;
        expect(velox::media::muxCopyOnly(request, &result),
               "tail extension streaming mux succeeds");
        checkOutput(output, result, "tail-extension");
        expect(result.success && result.video_packets > 6,
               "tail extension appends decode-free video packets");
        const auto probe = velox::media::probeMediaInProcess(output);
        expect(probe.has_value() && probe->duration_verified &&
                   probe->duration_seconds > 1.55 && probe->duration_seconds < 1.95,
               "tail extension covers the requested duration (actual=" +
                   (probe.has_value() ? std::to_string(probe->duration_seconds)
                                      : std::string("none")) +
                   ")");
        const auto inspected = inspectStreams(output);
        checkStreams(inspected, result, "tail-extension");
    }

    // --- 20+ segments + determinism -------------------------------------------
    {
        const fs::path output = root / "many-segments.mp4";
        velox::media::CopyOnlyMuxRequest request;
        request.video_segments.reserve(25);
        for (int i = 0; i < 25; ++i) {
            request.video_segments.push_back({video, 0, 400'000});
        }
        request.output_path = output;
        velox::media::CopyOnlyMuxResult result;
        expect(velox::media::muxCopyOnly(request, &result),
               "25-segment streaming mux succeeds");
        checkOutput(output, result, "25-segment");
        const auto probe = velox::media::probeMediaInProcess(output);
        expectTimeline(result, probe, 10'000'000, "25-segment");
        const auto inspected = inspectStreams(output);
        checkStreams(inspected, result, "25-segment");

        const fs::path repeatOutput = root / "many-segments-repeat.mp4";
        auto repeatRequest = request;
        repeatRequest.output_path = repeatOutput;
        velox::media::CopyOnlyMuxResult repeatResult;
        expect(velox::media::muxCopyOnly(repeatRequest, &repeatResult),
               "25-segment streaming mux repeats deterministically");
        const auto repeatInspected = inspectStreams(repeatOutput);
        expect(repeatResult.video_packets == result.video_packets &&
                   repeatResult.audio_packets == result.audio_packets &&
                   repeatResult.duration_us == result.duration_us &&
                   repeatResult.max_buffered_packets == result.max_buffered_packets &&
                   repeatResult.packet_heap_allocations == result.packet_heap_allocations &&
                   repeatResult.global_sort_ms == result.global_sort_ms &&
                   repeatInspected.video_packets == inspected.video_packets &&
                   repeatInspected.audio_packets == inspected.audio_packets,
               "repeated 25-segment run is deterministic in accounting and invariants");
    }

    // --- 1000+ repeated segments ----------------------------------------------
    {
        const fs::path output = root / "thousand-segments.mp4";
        velox::media::CopyOnlyMuxRequest request;
        request.video_segments.reserve(1000);
        for (int i = 0; i < 1000; ++i) {
            request.video_segments.push_back({video, 0, 800'000});
        }
        request.output_path = output;
        velox::media::CopyOnlyMuxResult result;
        expect(velox::media::muxCopyOnly(request, &result),
               "1000-segment streaming mux succeeds");
        checkOutput(output, result, "1000-segment");
        expect(result.duration_us == 1000 * 800'000,
               "1000-segment mux reports the complete logical timeline");
        const auto inspected = inspectStreams(output);
        checkStreams(inspected, result, "1000-segment");
    }

    // --- B-frame semantic golden test -----------------------------------------
    {
        const fs::path source = root / "bframes-golden-src.mp4";
        const fs::path output = root / "bframes-golden-out.mp4";
        expect(makeBFrameVideo(source), "B-frame golden source fixture can be created");

        velox::media::CopyOnlyMuxRequest request;
        request.video_segments = {{source, 0, 1'600'000}};
        request.output_path = output;
        velox::media::CopyOnlyMuxResult result;
        expect(velox::media::muxCopyOnly(request, &result),
               "B-frame golden mux succeeds");
        checkOutput(output, result, "B-frame-golden");
        const auto probe = velox::media::probeMediaInProcess(output);
        expectTimeline(result, probe, 1'600'000, "B-frame-golden");

        const auto allSrcFrames = decodeAllVideoFrames(source);
        expect(allSrcFrames.size() > 0,
               "B-frame golden: source yields decoded frames (count=" +
                   std::to_string(allSrcFrames.size()) + ")");
        std::vector<uint32_t> srcChecksums;
        for (const auto& f : allSrcFrames) {
            if (f.pts_us >= 0 && f.pts_us < 1'600'000) {
                srcChecksums.push_back(f.luma_checksum);
            }
        }

        const auto outFrames = decodeAllVideoFrames(output);
        expect(outFrames.size() > 0,
               "B-frame golden: output yields decoded frames (count=" +
                   std::to_string(outFrames.size()) + ")");
        std::vector<uint32_t> outChecksums;
        for (const auto& f : outFrames) {
            outChecksums.push_back(f.luma_checksum);
        }

        expect(srcChecksums.size() == outChecksums.size(),
               "B-frame golden: decoded frame count matches window (src=" +
                   std::to_string(srcChecksums.size()) + " out=" +
                   std::to_string(outChecksums.size()) + ")");
        for (size_t i = 0; i < outChecksums.size(); ++i) {
            if (outChecksums[i] == 0) {
                expect(false,
                       "B-frame golden: frame " + std::to_string(i) +
                           " has zero luma checksum (corrupt or empty)");
                break;
            }
        }

        std::sort(srcChecksums.begin(), srcChecksums.end());
        std::sort(outChecksums.begin(), outChecksums.end());
        uint64_t totalDiff = 0;
        uint64_t maxSingleDiff = 0;
        int mismatchCount = 0;
        const size_t compareCount = std::min(srcChecksums.size(), outChecksums.size());
        for (size_t i = 0; i < compareCount; ++i) {
            const uint64_t diff = srcChecksums[i] > outChecksums[i]
                ? srcChecksums[i] - outChecksums[i]
                : outChecksums[i] - srcChecksums[i];
            totalDiff += diff;
            if (diff > maxSingleDiff) maxSingleDiff = diff;
            if (diff > 0) ++mismatchCount;
        }
        const uint64_t perFrameTolerance = 256;
        expect(maxSingleDiff <= perFrameTolerance,
               "B-frame golden: per-frame checksum delta within tolerance (max=" +
                   std::to_string(maxSingleDiff) +
                   " mismatches=" + std::to_string(mismatchCount) + "/" +
                   std::to_string(compareCount) + ")");
        std::cerr << "bframe_golden: total_frames=" << compareCount
                  << " mismatches=" << mismatchCount
                  << " total_diff=" << totalDiff
                  << " max_diff=" << maxSingleDiff << "\n";
    }

    // --- B-frame cut-boundary golden test ------------------------------------
    {
        const fs::path source = root / "bframes-cut-src.mp4";
        const fs::path output = root / "bframes-cut-out.mp4";
        expect(makeBFrameVideo(source), "B-frame cut source fixture can be created");

        velox::media::CopyOnlyMuxRequest request;
        request.video_segments = {{source, 0, 800'000}};
        request.output_path = output;
        velox::media::CopyOnlyMuxResult result;
        expect(velox::media::muxCopyOnly(request, &result),
               "B-frame cut-boundary mux succeeds");
        checkOutput(output, result, "B-frame-cut");
        const auto probe = velox::media::probeMediaInProcess(output);
        expectTimeline(result, probe, 800'000, "B-frame-cut");

        const auto allSrcFrames = decodeAllVideoFrames(source);
        const auto outFrames = decodeAllVideoFrames(output);
        expect(outFrames.size() > 0,
               "B-frame cut: output yields decoded frames");

        bool allWithinWindow = true;
        for (const auto& f : outFrames) {
            if (f.pts_us >= 800'000) {
                allWithinWindow = false;
                break;
            }
        }
        expect(allWithinWindow,
               "B-frame cut: all output frames present within the requested window");

        std::vector<uint32_t> srcChecksums;
        for (const auto& f : allSrcFrames) {
            if (f.pts_us >= 0 && f.pts_us < 800'000) {
                srcChecksums.push_back(f.luma_checksum);
            }
        }
        expect(outFrames.size() == srcChecksums.size(),
               "B-frame cut: output frame count matches source frames within window");

        std::vector<uint32_t> outChecksums;
        for (const auto& f : outFrames) {
            outChecksums.push_back(f.luma_checksum);
        }
        std::sort(srcChecksums.begin(), srcChecksums.end());
        std::sort(outChecksums.begin(), outChecksums.end());
        uint64_t maxSingleDiff = 0;
        int mismatchCount = 0;
        const size_t compareCount = std::min(srcChecksums.size(), outChecksums.size());
        for (size_t i = 0; i < compareCount; ++i) {
            const uint64_t diff = srcChecksums[i] > outChecksums[i]
                ? srcChecksums[i] - outChecksums[i]
                : outChecksums[i] - srcChecksums[i];
            if (diff > maxSingleDiff) maxSingleDiff = diff;
            if (diff > 0) ++mismatchCount;
        }
        const uint64_t perFrameTolerance = 256;
        expect(maxSingleDiff <= perFrameTolerance,
               "B-frame cut: per-frame checksum delta within tolerance (max=" +
                   std::to_string(maxSingleDiff) +
                   " mismatches=" + std::to_string(mismatchCount) + "/" +
                   std::to_string(compareCount) + ")");
    }

    // --- I-frame copy-only golden test ------------------------------------
    {
        const fs::path source = root / "iframe-golden-src.mp4";
        const fs::path output = root / "iframe-golden-out.mp4";
        expect(makeVideo(source, "64x64", 5, "-bf 0 -g 1"),
               "I-frame golden source fixture (bf=0 g=1) can be created");

        velox::media::CopyOnlyMuxRequest request;
        request.video_segments = {
            {source, 0, 600'000},
            {source, 0, 600'000},
        };
        request.output_path = output;
        velox::media::CopyOnlyMuxResult result;
        expect(velox::media::muxCopyOnly(request, &result),
               "I-frame golden mux succeeds");
        checkOutput(output, result, "I-frame-golden");
        const auto probe = velox::media::probeMediaInProcess(output);
        expectTimeline(result, probe, 1'200'000, "I-frame-golden");

        const auto outFrames = decodeAllVideoFrames(output);
        expect(outFrames.size() > 0,
               "I-frame golden: output yields decoded frames (count=" +
                   std::to_string(outFrames.size()) + ")");
        expect(outFrames.size() == 6,
               "I-frame golden: output has 6 decoded frames (actual=" +
                   std::to_string(outFrames.size()) + ")");
        bool allValid = true;
        for (size_t i = 0; i < outFrames.size(); ++i) {
            if (outFrames[i].luma_checksum == 0) {
                allValid = false;
                break;
            }
        }
        expect(allValid, "I-frame golden: all output frames have valid pixel data");
        bool ptsMonotonic = true;
        for (size_t i = 1; i < outFrames.size(); ++i) {
            if (outFrames[i].pts_us <= outFrames[i - 1].pts_us) {
                ptsMonotonic = false;
                break;
            }
        }
        expect(ptsMonotonic, "I-frame golden: output decoded PTS strictly monotonic");
    }

    std::cerr << "summary: fail=" << failures << "\n";
    return failures == 0 ? 0 : 1;
}
