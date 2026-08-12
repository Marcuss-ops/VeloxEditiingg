#pragma once
// media_packet_components.hpp — named in-process packet-pipeline components
// for the copy-only stream-copy path: AVPacket in, AVPacket out, never
// spawning ffmpeg/ffprobe per segment.
//
//   Demuxer            in-process AVPacket source (avformat demux)
//   TimestampState     per-stream monotonic timestamp state
//   rewritePacket      PacketTrimmer + TimestampRewriter in one pass
//   demuxAndRewrite    the per-segment reader the ConcatMuxer drives
//
// This header is LibAV-aware by design: it is included only by
// media_packet_pipeline.cpp and the libav-only component tests, both of
// which compile exclusively when VELOX_ENABLE_LIBAV=ON.
#ifndef VELOX_ENABLE_LIBAV
#error "media_packet_components.hpp requires -DVELOX_ENABLE_LIBAV=ON"
#endif

// The LibAV public headers must be included under C linkage. The project
// applies the same extern "C" wrapper around every libav include (see
// media_packet_pipeline.cpp / media_probe.cpp): some distro -dev packages
// ship headers without their own __cplusplus guard, so a bare include here
// would declare C++-mangled symbols and every pipeline link would fail.
extern "C" {
#include <libavcodec/avcodec.h>
#include <libavformat/avformat.h>
#include <libavutil/avutil.h>
}

#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

namespace velox::media::packet {

// Common microsecond time base shared by every rewritten packet.
constexpr AVRational kMicrosecondTimeBase{1, 1'000'000};

bool validTimestamp(int64_t value);

// Per-stream monotonic timestamp state carried across a whole source range.
// The first rewritten packet of a range starts with AV_NOPTS_VALUE.
struct TimestampState {
    int64_t last_dts{AV_NOPTS_VALUE};
    int64_t last_pts{AV_NOPTS_VALUE};
};

// RAII owner of one accepted AVPacket handed to the muxer for interleaved
// writing. Copied packets are moved in with av_packet_move_ref.
struct PacketHolder {
    AVPacket packet{};
    int output_stream_index{0};
    int64_t sort_dts{AV_NOPTS_VALUE};

    PacketHolder();
    ~PacketHolder();
    PacketHolder(const PacketHolder&) = delete;
    PacketHolder& operator=(const PacketHolder&) = delete;
};

// Demuxer — in-process AVPacket source. Opens one immutable local file
// through avformat, resolves the first stream of a requested media type and
// yields raw packets. No ffprobe or shell is involved.
class Demuxer {
public:
    // Opens `path` and runs stream discovery. Records the open in the
    // process-scoped I/O counters (a second open of the same path counts as
    // a reopen). Returns false with `error` set on failure; the instance
    // stays closed.
    bool open(const std::filesystem::path& path, std::string& error);

    // Index of the first stream of `type`, or -1 when absent.
    int firstStream(AVMediaType type) const;

    const AVStream* stream(int index) const;
    const AVFormatContext* raw() const;

    // Reads the next raw packet. Returns true on success; at end of input
    // returns true with *eof set; on a hard error returns false with
    // *eof=false and `error` set to the ffmpeg error text.
    bool readFrame(AVPacket& packet, bool& eof, std::string& error);

    void close();
    bool isOpen() const { return context_ != nullptr; }
    ~Demuxer();

private:
    AVFormatContext* context_{nullptr};
};

// PacketTrimmer + TimestampRewriter — one AVPacket -> AVPacket pass:
// subtracts the stream start and requested source_in_us, rescales to the
// microsecond timeline, trims to the [timeline_offset,
// timeline_offset + segment_duration) window,
// clamps negative prefixes, enforces per-stream monotonic ordering and
// clamps the last accepted packet's duration to the segment end.
//
// Returns true when the packet is accepted, mutating it in place and
// advancing `state`; a packet outside the window is rejected WITHOUT
// touching `state` (so a packet just past a segment boundary cannot move
// the next segment's baseline). `sort_dts` receives the packet's rewrite
// dts (or pts when dts is absent) for interleaved ordering.
bool rewritePacket(AVPacket& packet,
                   const AVStream* input_stream,
                   const AVStream* output_stream,
                   int64_t source_start,
                   int64_t source_in_us,
                   int64_t timeline_offset,
                   int64_t segment_duration,
                   TimestampState& state,
                   int64_t& sort_dts);

// Demuxes `path` through a fresh Demuxer and rewrites every accepted packet
// through rewritePacket into `packets`, counting accepted packets into
// `packet_count`. This is the per-segment zero-spawn reader the ConcatMuxer
// drives: one avformat open, one packet pass, no child process.
bool demuxAndRewrite(const std::filesystem::path& path,
                     AVMediaType type,
                     int input_stream_index,
                     AVStream* output_stream,
                     int64_t timeline_offset,
                     int64_t source_in_us,
                     int64_t duration_us,
                     TimestampState& state,
                     std::vector<std::unique_ptr<PacketHolder>>& packets,
                     int64_t& packet_count,
                     std::string& error);

// Returns true only when source_in_us identifies an exact video keyframe.
// Packet-copy never guesses for a non-keyframe cut: callers must route that
// segment to a decoder/transcoder or reject it.
bool sourceWindowStartsOnKeyframe(const std::filesystem::path& path,
                                  int input_stream_index,
                                  int64_t source_in_us,
                                  std::string& error);

} // namespace velox::media::packet
