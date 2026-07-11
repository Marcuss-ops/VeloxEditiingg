// FFmpeg `-progress pipe:1 -nostats` parser.
//
// FFmpeg emits machine-readable progress markers on stdout when invoked
// with `-progress pipe:1 -nostats`. Each "block" reports one frame's
// progress and ends with a `progress=continue|end` line. The format is:
//
//   frame=42
//   fps=29.97
//   stream_0_0_q=23.0
//   bitrate=2048kbits/s
//   total_size=123456
//   out_time_us=1402000
//   out_time_ms=1402
//   out_time=00:00:01.402000
//   dup_frames=0
//   drop_frames=0
//   speed=1.5x
//   progress=continue
//
// We parse this stream incrementally, feeding raw bytes (or whole
// lines) and invoking the user-supplied callback at the end of every
// "block" (the line that starts with `progress=`). `progress=end`
// signals ffmpeg has finished the current invocation.
//
// SCHEMA NOTE: FFmpeg emits `out_time_us` natively. The user's F5 spec
// references `out_time_ms`; the parser stores BOTH fields so callers
// can use whichever they prefer. The C++ sidecar JSON emits the
// derived `out_time_ms` (ms = us / 1000) so the worker F4 side does
// not have to do that conversion.

#pragma once

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <string>

namespace velox::services {

// EngineProgress is one fully-parsed FFmpeg progress block.
//
// All numeric fields are zero until the corresponding key=value line
// has been seen. `finished` is set when `progress=end` is observed.
struct EngineProgress {
    int64_t frame{0};
    double fps{0.0};
    std::string speed{};                 // raw "1.5x" form from ffmpeg
    double speed_x{0.0};                 // parsed multiplicative "1.5"
    int64_t out_time_us{0};              // microseconds since stream start
    int64_t out_time_ms{0};              // out_time_us / 1000
    std::string out_time{};              // human-readable "HH:MM:SS.uuuuuu"
    int64_t total_size{0};               // bytes written so far
    int64_t dup_frames{0};
    int64_t drop_frames{0};
    int64_t bitrate{0};                  // bits/s (best-effort)
    bool finished{false};                // progress=end observed
    double progress_pct{0.0};            // 0..100 (best-effort, see ExpectedDurationUs)
};

// ProgressCallback is invoked at the end of every parsed block.
// Receives the populated EngineProgress. Callback runs on the
// thread that called feed()/runFfmpegCapturingProgress; the user's
// callback must be thread-safe if multiple parsers feed it.
using ProgressCallback = std::function<void(const EngineProgress&)>;

// ProgressParser is a stateful incremental parser. Caller feeds raw
// bytes (or whole completed lines prefixed by `progress=`); the parser
// reassembles the buffer and invokes the callback when a block ends.
//
// Parser is single-shot: do not reuse across separate ffmpeg
// invocations (the `cur_` accumulator would carry stale state). Create
// one parser per process.
class ProgressParser {
public:
    // Construct with an empty callback. Caller MUST setCallback
    // before the first feed(), otherwise parsed blocks are silently
    // dropped.
    explicit ProgressParser(ProgressCallback cb = nullptr);

    // Set or replace the callback.
    void setCallback(ProgressCallback cb);

    // Hint for progress_pct: when the caller knows the expected
    // stream duration (e.g. voiceover probe result), set it before
    // feeding bytes. 0 disables pct computation; `progress_pct` stays
    // 0 in the EngineProgress payload.
    void setExpectedDurationUs(int64_t us);
    int64_t expectedDurationUs() const { return expected_duration_us_.load(); }

    // feed appends raw bytes to the internal buffer, parses ALL
    // complete lines, and emits callbacks for every block boundary.
    // Returns the number of complete lines consumed.
    //
    // Byte chunks may be arbitrarily sized (one byte at a time OK).
    // Partial lines remain buffered until the next feed/finish().
    size_t feed(const std::string& chunk);

    // finish() flushes a partial trailing line (if any). Used at EOF.
    // No further bytes are expected after this call.
    void finish();

    // observedCount is a debugging hook — count of completed blocks
    // (i.e. callback invocations) seen so far.
    size_t observedCount() const { return observed_count_.load(); }

private:
    void onBlockBoundary(EngineProgress& blk);

    std::string buffer_;
    ProgressCallback cb_;
    std::atomic<int64_t> expected_duration_us_{0};
    std::atomic<size_t> observed_count_{0};
    EngineProgress cur_{};
};

// runFfmpegCapturingProgress spawns `cmd` shell-style (via popen +
// sh -c) and streams its stdout through a line-based ProgressParser,
// invoking `cb` for every parsed block.
//
// Returns:
//   - true  if exit_code == 0
//   - false if popen failed OR cmd exited non-zero
//
// On failure, `stderr_out` receives the captured stderr (best-effort;
// stderr is a separate pipe, captured concurrently).
//
// expected_duration_us is forwarded to the parser for progress_pct
// computation. Pass 0 if the caller does not know the target duration.
//
// THREADING: std::thread is used to read stderr concurrently so the
// pipe never deadlocks on backpressure. The stdout reader runs on
// the calling thread (synchronous, single-threaded by design — ffmpeg
// emits progress at most every ~1s, far below polling latency, so a
// dedicated thread is unnecessary overhead).
bool runFfmpegCapturingProgress(
    const std::string& cmd,
    const std::filesystem::path& cwd,
    ProgressCallback cb,
    int64_t expected_duration_us,
    std::string& stderr_out,
    int& exit_code) noexcept;

// SidecarWriter lays out a robust atomic sidecar write helper used
// by the render engine. Writes to <path>.tmp first, then renames
// atomically so the worker sidecar reader never observes a
// half-written JSON.
class SidecarWriter {
public:
    static bool writeAtomic(
        const std::filesystem::path& final_path,
        const std::string& json_content) noexcept;
};

// escapeJsonString is a public re-export of the project's JSON
// escaper so callers don't pull in two different escape routines.
// Implementation lives in json_utils.hpp (already included via the
// render engine translation unit); this is a thin convenience wrapper
// that callers in the same TU can use without the include dance.
std::string escapeProgressJsonString(const std::string& s);

} // namespace velox::services
