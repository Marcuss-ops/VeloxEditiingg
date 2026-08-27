#pragma once

#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

// This header is the value-types-only public surface (the OFF-build
// fallback and RenderEngine include it). The LibAV-aware named components
// behind the copy-only path — Demuxer, PacketTrimmer, TimestampRewriter —
// live in media_packet_components.hpp (namespace velox::media::packet) and
// are compiled exclusively when VELOX_ENABLE_LIBAV=ON.
namespace velox::media {

struct CopyOnlyVideoSegment {
    std::filesystem::path path;
    int64_t source_in_us{0};
    int64_t source_duration_us{0};
    bool include_audio{false};
    // True when `path` is an already-normalized output in the canonical
    // profile. A normalized segment starts at its own frame zero
    // (source_in_us must be 0), so the mux skips the raw-source keyframe and
    // source-duration heuristics and only enforces the fail-closed
    // canonical-profile compatibility check. The copy-only mixed renderer
    // no longer produces normalized segments (it rejects instead of
    // re-encoding), but the flag is retained for the mux's fail-closed
    // handling of pre-normalized inputs.
    bool normalized{false};
};

struct CopyOnlyAudioTrack {
    std::filesystem::path path;
    int64_t start_offset_us{0};
    int64_t duration_us{0};
};

struct CopyOnlyMuxRequest {
    std::vector<CopyOnlyVideoSegment> video_segments;
    std::optional<CopyOnlyAudioTrack> audio;
    std::filesystem::path output_path;
};

struct CopyOnlyMuxResult {
    bool success{false};
    // True only when the output rename committed and the parent directory
    // fsync completed. A successful rename with unavailable directory fsync
    // is published but explicitly not durable.
    bool output_durable{false};
    std::string error;
    int64_t video_packets{0};
    int64_t audio_packets{0};
    int64_t duration_us{0};
    // Opportunistic SHA-256 of final output bytes. Callers must fall back to
    // their canonical manifest hash when sha256_valid is false.
    std::string sha256;
    bool sha256_valid{false};
    int64_t output_size_bytes{0};
    bool backward_seek_seen{false};
    // Certified bounded-path telemetry. The canonical mux uses two reusable
    // pending slots (video and audio), performs no global sort, and never
    // allocates a PacketHolder per packet.
    int64_t max_buffered_packets{0};
    int64_t packet_heap_allocations{0};
    int64_t global_sort_ms{0};
};

// Concatenate compatible local stream-copy inputs and optionally add one
// already-prepared audio track without decoding or encoding. Every input is
// demuxed in-process, trimmed to its declared duration, rewritten to a common
// microsecond timeline, and written through one MP4 muxer. The output is
// published atomically only after the trailer has been written and the
// temporary file has been synced.
//
// Normalized assembly: a video segment marked `normalized` is an
// already-normalized output (source_in_us == 0); such segments skip the
// raw-source keyframe/duration heuristics but still pass the fail-closed
// canonical-profile compatibility check before being concatenated by the
// same single mux.
class MediaPacketPipeline {
public:
    // Stable facade for packet-copy assembly. The implementation delegates
    // to the demux/session, rewrite/trim and mux stages without exposing
    // their ownership or LibAV details to callers.
    static bool muxCopyOnly(const CopyOnlyMuxRequest& request,
                            CopyOnlyMuxResult* result = nullptr);
};

// Backward-compatible free-function facade retained for existing callers.
bool muxCopyOnly(const CopyOnlyMuxRequest& request, CopyOnlyMuxResult* result = nullptr);

} // namespace velox::media
