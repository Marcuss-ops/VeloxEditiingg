// FFmpeg -progress pipe:1 parser — implementation.
//
// Algorithm summary:
//
//   * feed() appends bytes to buffer_, splits on '\n' (handling '\r'
//     in case ffmpeg prepends carriage returns on Windows — Linux
//     ffmpeg uses '\n' but be defensive).
//   * Each complete line is treated as `key=value` and routed to
//     applyKey().
//   * When `progress=` is observed, the block boundary fires: invoke
//     the callback with the accumulated EngineProgress, then reset
//     cur_ to zero before the next block.
//
// Edge cases handled:
//   - Buffers across feed() boundaries (partial lines).
//   - Trailing whitespace in key/value lines (defensive trim).
//   - `out_time_us=NaN` (some ffmpeg builds emit non-numeric for
//     indeterminate timestamps) — defaulted to 0.
//   - Duplicate keys within a single block (kept as the LAST value,
//     which matches ffmpeg's own overwrite semantics).
//   - `progress=continue` and `progress=end` both fire the callback;
//     only `progress=end` sets `finished=true`.

#include "velox/services/ffmpeg_progress_parser.hpp"
#include "json_utils.hpp"

#include <algorithm>
#include <atomic>
#include <charconv>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <future>
#include <iostream>
#include <sstream>
#include <string>
#include <string_view>
#include <sys/types.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>

namespace fs = std::filesystem;

namespace velox::services {

// Cap on stderr_buf to prevent OOM if a misbehaving ffmpeg spews >64 KiB
// (the default Linux pipe buffer) — that would otherwise deadlock the
// writer thread waiting for the reader to drain.
static constexpr size_t kMaxStderrBufferBytes = 256 * 1024;

// ─── escape ───────────────────────────────────────────────────────────

// Delegates to the canonical velox::json::escapeJsonString (json_utils.hpp)
// so progress lines and engine metadata share one escaping definition.
std::string escapeProgressJsonString(const std::string& s) {
    return velox::json::escapeJsonString(s);
}

// ─── trim helper ──────────────────────────────────────────────────────

static std::string_view trimView(std::string_view value) {
    while (!value.empty() && std::isspace(static_cast<unsigned char>(value.front()))) {
        value.remove_prefix(1);
    }
    while (!value.empty() && std::isspace(static_cast<unsigned char>(value.back()))) {
        value.remove_suffix(1);
    }
    return value;
}

// ─── ProgressParser ───────────────────────────────────────────────────

ProgressParser::ProgressParser(ProgressCallback cb)
    : cb_(std::move(cb)) {}

void ProgressParser::setCallback(ProgressCallback cb) {
    cb_ = std::move(cb);
}

void ProgressParser::setExpectedDurationUs(int64_t us) {
    expected_duration_us_.store(us);
}

static bool tryParseInt64(std::string_view v, int64_t& out) {
    if (v.empty()) return false;
    const auto result = std::from_chars(v.data(), v.data() + v.size(), out);
    return result.ec == std::errc{} && result.ptr == v.data() + v.size();
}

static bool tryParseDouble(std::string_view v, double& out) {
    if (v.empty()) return false;
    const auto result = std::from_chars(v.data(), v.data() + v.size(), out);
    return result.ec == std::errc{} && result.ptr == v.data() + v.size();
}

static double parseSpeedX(std::string_view v) {
    // ffmpeg emits "1.5x" or "0.95x" or "N/A" on indeterminate speed.
    if (v.empty() || v == "N/A") return 0.0;
    std::string_view trimmed = trimView(v);
    if (trimmed.empty()) return 0.0;
    if (trimmed.back() == 'x') trimmed.remove_suffix(1);
    double x = 0.0;
    if (tryParseDouble(trimmed, x)) {
        return x;
    }
    return 0.0;
}

static void applyKeyToProgress(std::string_view key, std::string_view value, EngineProgress& cur) {
    int64_t iv = 0;
    double dv = 0.0;
    if (key == "frame") {
        if (tryParseInt64(value, iv)) cur.frame = iv;
    } else if (key == "fps") {
        if (tryParseDouble(value, dv)) cur.fps = dv;
    } else if (key == "speed") {
        if (!value.empty() && value != "N/A") {
            cur.speed.assign(value);
            cur.speed_x = parseSpeedX(value);
        }
    } else if (key == "out_time_us") {
        if (tryParseInt64(value, iv)) {
            cur.out_time_us = iv;
            cur.out_time_ms = iv / 1000;
        }
    } else if (key == "out_time_ms") {
        // ffmpeg does not always emit out_time_ms — but if it does,
        // prefer the explicit value and back-derive out_time_us
        // when missing.
        if (tryParseInt64(value, iv)) {
            cur.out_time_ms = iv;
            if (cur.out_time_us == 0) cur.out_time_us = iv * 1000;
        }
    } else if (key == "out_time") {
        cur.out_time.assign(value);
    } else if (key == "total_size") {
        if (tryParseInt64(value, iv)) cur.total_size = iv;
    } else if (key == "dup_frames") {
        if (tryParseInt64(value, iv)) cur.dup_frames = iv;
    } else if (key == "drop_frames") {
        if (tryParseInt64(value, iv)) cur.drop_frames = iv;
    } else if (key == "bitrate") {
        // ffmpeg emits "2048kbits/s" — strip units, keep bits/s.
        std::size_t num_size = 0;
        for (char c : value) {
            if (std::isdigit(static_cast<unsigned char>(c)) || c == '.') {
                ++num_size;
            } else {
                break;
            }
        }
        if (num_size > 0) {
            double bps = 0.0;
            if (tryParseDouble(value.substr(0, num_size), bps)) {
                cur.bitrate = static_cast<int64_t>(bps);
            }
        }
    } else if (key == "progress") {
        // Block boundary — caller treats this via onBlockBoundary.
    }
}

void ProgressParser::onBlockBoundary(EngineProgress& blk) {
    if (blk.finished) {
        // no special handling beyond setting progress_pct below.
    } else {
        // progress=continue OR progress=end are both block boundaries.
        blk.finished = (blk.progress_pct >= 100.0);
    }
    // compute progress_pct
    const int64_t expected_us = expected_duration_us_.load();
    if (expected_us > 0 && blk.out_time_us > 0) {
        double ratio = static_cast<double>(blk.out_time_us) / static_cast<double>(expected_us);
        if (ratio < 0.0) ratio = 0.0;
        if (ratio > 1.0) ratio = 1.0;
        blk.progress_pct = ratio * 100.0;
    }
}

size_t ProgressParser::feed(const std::string& chunk) {
    if (chunk.empty()) return 0;
    buffer_.append(chunk);
    size_t consumed_lines = 0;

    size_t pos = 0;
    while (true) {
        size_t nl = buffer_.find('\n', pos);
        if (nl == std::string::npos) break;
        const std::string_view line = trimView(
            std::string_view(buffer_).substr(pos, nl - pos));
        pos = nl + 1;

        if (line.empty()) continue;

        size_t eq = line.find('=');
        if (eq == std::string::npos) continue;

        const std::string_view key = trimView(line.substr(0, eq));
        const std::string_view value = trimView(line.substr(eq + 1));
        applyKeyToProgress(key, value, cur_);

        if (key == "progress") {
            cur_.finished = (value == "end");
            onBlockBoundary(cur_);
            if (cb_) cb_(cur_);
            observed_count_.fetch_add(1);
            cur_ = EngineProgress{};  // reset for next block
        }
        ++consumed_lines;
    }

    // Compact buffer when fully consumed (avoid unbounded growth).
    if (pos > 0) {
        if (pos >= buffer_.size()) {
            buffer_.clear();
        } else {
            buffer_.erase(0, pos);
        }
    }
    return consumed_lines;
}

void ProgressParser::finish() {
    if (buffer_.empty()) return;
    const std::string_view tail = trimView(buffer_);
    if (tail.empty()) {
        buffer_.clear();
        return;
    }

    size_t eq = tail.find('=');
    if (eq == std::string::npos) {
        buffer_.clear();
        return;
    }
    const std::string_view key = trimView(tail.substr(0, eq));
    const std::string_view value = trimView(tail.substr(eq + 1));
    applyKeyToProgress(key, value, cur_);

    if (key == "progress") {
        cur_.finished = (value == "end");
        onBlockBoundary(cur_);
        if (cb_) cb_(cur_);
        observed_count_.fetch_add(1);
        cur_ = EngineProgress{};
    }
    buffer_.clear();
}

// ─── runFfmpegCapturingProgress ────────────────────────────────────────
//
// popen(3) "r" gives us a FILE* over a child shell's stdout via
// fork/exec `/bin/sh -c <cmd>`. We do NOT capture stderr through the
// same channel (mixing them would corrupt the -progress line layout
// when ffmpeg warns). Instead we redirect stderr to a pipe and read
// it on a background thread until EOF.
struct StderrCapture {
    int fd{-1};
    std::string buffer;
    void join(std::thread& t) {
        if (t.joinable()) t.join();
        if (fd >= 0) ::close(fd);
    }
};

static std::string runFfmpegInternal(
    const std::string& cmd,
    const fs::path& cwd,
    FILE*& out_fp,
    int& out_fd_stderr,
    std::thread& out_stderr_thread,
    pid_t& out_pid,
    int& out_exit_code,
    std::string& out_stderr_text
) {
    out_pid = -1;
    // Build a stderr pipe.
    int pipefd[2] = {-1, -1};
    if (::pipe(pipefd) != 0) {
        return std::string("pipe(stderr) failed: ") + std::strerror(errno);
    }

    // Make stdout pipe ourselves so we can use a plain FILE* + fd.
    int out_pipe[2] = {-1, -1};
    if (::pipe(out_pipe) != 0) {
        ::close(pipefd[0]); ::close(pipefd[1]);
        return std::string("pipe(stdout) failed: ") + std::strerror(errno);
    }

    const pid_t pid = ::fork();
    if (pid < 0) {
        ::close(pipefd[0]); ::close(pipefd[1]);
        ::close(out_pipe[0]); ::close(out_pipe[1]);
        return std::string("fork failed: ") + std::strerror(errno);
    }
    if (pid == 0) {
        // Child.
        // chdir if requested.
        if (!cwd.empty()) {
            ::chdir(cwd.string().c_str());
        }
        // Wire stdout → out_pipe[1], stderr → pipefd[1]; close reader ends.
        ::dup2(out_pipe[1], STDOUT_FILENO);
        ::dup2(pipefd[1],   STDERR_FILENO);
        ::close(out_pipe[0]);
        ::close(pipefd[0]);
        ::close(out_pipe[1]);
        ::close(pipefd[1]);
        // Exec /bin/sh -c <cmd>.
        execl("/bin/sh", "sh", "-c", cmd.c_str(), static_cast<char*>(nullptr));
        // On exec failure, exit nonzero so parent learns.
        _exit(127);
    }

    // Parent: close write ends in our hands; drain in our own thread.
    ::close(out_pipe[1]);
    ::close(pipefd[1]);
    out_fp = ::fdopen(out_pipe[0], "r");
    if (out_fp == nullptr) {
        ::close(out_pipe[0]);
        ::close(pipefd[0]);
        ::waitpid(pid, &out_exit_code, 0);
        return std::string("fdopen(stdout) failed: ") + std::strerror(errno);
    }
    out_pid = pid;
    out_fd_stderr = pipefd[0];

    auto captureStderr = [&]() {
        char buf[4096];
        while (true) {
            ssize_t n = ::read(out_fd_stderr, buf, sizeof(buf));
            if (n > 0) {
                // Cap stderr_buf to prevent OOM if ffmpeg spews >64 KiB
                // (default Linux pipe buffer) — would otherwise deadlock
                // the writer thread waiting on feedback.
                if (out_stderr_text.size() < kMaxStderrBufferBytes) {
                    size_t space = kMaxStderrBufferBytes - out_stderr_text.size();
                    size_t take = std::min(static_cast<size_t>(n), space);
                    out_stderr_text.append(buf, take);
                }
                // Once the diagnostic buffer is full, continue draining and
                // discard additional bytes. Stopping here would fill the
                // child's stderr pipe and deadlock the encoder.
            } else if (n == 0) {
                break;
            } else if (errno == EINTR) {
                continue;
            } else {
                break;
            }
        }
    };
    out_stderr_thread = std::thread(captureStderr);

    // The caller must drain stdout before waiting. Waiting here first allows
    // a verbose ffmpeg process to fill `-progress pipe:1` and deadlock while
    // the parent is blocked in waitpid().
    return {};
}

bool runFfmpegCapturingProgress(
    const std::string& cmd,
    const fs::path& cwd,
    ProgressCallback cb,
    int64_t expected_duration_us,
    std::string& stderr_out,
    int& exit_code
) noexcept {
    FILE* fp = nullptr;
    int stderr_fd = -1;
    std::thread stderr_thread;
    pid_t child_pid = -1;
    std::string stderr_buf;
    std::string err;

    try {
        err = runFfmpegInternal(
            cmd, cwd, fp, stderr_fd, stderr_thread, child_pid, exit_code, stderr_buf);
    } catch (...) {
        err = "runFfmpegInternal threw";
    }

    if (!err.empty()) {
        // Cannot start — set exit_code and bail.
        exit_code = 127;
        stderr_out = err;
        if (fp != nullptr) ::fclose(fp);
        if (stderr_fd >= 0) ::close(stderr_fd);
        if (stderr_thread.joinable()) stderr_thread.join();
        if (child_pid > 0) {
            int ignored_status = 0;
            ::waitpid(child_pid, &ignored_status, 0);
        }
        return false;
    }

    // Reader side: line-by-line feed() into the parser.
    ProgressParser parser(std::move(cb));
    parser.setExpectedDurationUs(expected_duration_us);

    char line[4096];
    std::string chunk;
    while (std::fgets(line, sizeof(line), fp) != nullptr) {
        chunk.assign(line);
        parser.feed(chunk);
    }
    parser.finish();

    ::fclose(fp);

    // Drain stderr thread.
    if (stderr_thread.joinable()) stderr_thread.join();
    if (stderr_fd >= 0) ::close(stderr_fd);

    int raw_status = 0;
    if (child_pid <= 0 || ::waitpid(child_pid, &raw_status, 0) < 0) {
        exit_code = 1;
    } else if (WIFEXITED(raw_status)) {
        exit_code = WEXITSTATUS(raw_status);
    } else if (WIFSIGNALED(raw_status)) {
        exit_code = 128 + WTERMSIG(raw_status);
    } else {
        exit_code = 1;
    }

    stderr_out = std::move(stderr_buf);
    return (exit_code == 0);
}

// ─── SidecarWriter ─────────────────────────────────────────────────────

bool SidecarWriter::writeAtomic(const fs::path& final_path, const std::string& json_content) noexcept {
    try {
        fs::path parent = final_path.parent_path();
        if (parent.empty()) parent = ".";
        {
            std::error_code ec;
            fs::create_directories(parent, ec);
            if (ec) return false;
        }
        fs::path tmp = final_path;
        tmp += ".tmp";
        {
            std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
            if (!out.is_open()) return false;
            out.write(json_content.data(), static_cast<std::streamsize>(json_content.size()));
            out.flush();
            if (!out.good()) {
                out.close();
                std::error_code ec;
                fs::remove(tmp, ec);
                return false;
            }
            out.close();
        }
        // fsync to durably land the sidecar content before rename.
        int fd = ::open(tmp.string().c_str(), O_RDONLY);
        if (fd < 0) {
            std::error_code cleanup_ec;
            fs::remove(tmp, cleanup_ec);
            return false;
        }
        const bool file_synced = ::fsync(fd) == 0;
        const bool file_closed = ::close(fd) == 0;
        if (!file_synced || !file_closed) {
            std::error_code cleanup_ec;
            fs::remove(tmp, cleanup_ec);
            return false;
        }
        std::error_code ec;
        fs::rename(tmp, final_path, ec);
        if (ec) {
            std::error_code cleanup_ec;
            fs::remove(tmp, cleanup_ec);
            return false;
        }
        // POSIX-correct durability: fsync the parent directory so
        // the rename's directory entry itself is durable. Without
        // this, a crash between rename and dirfsync could lose the
        // directory entry even though the file content is durable.
        int dir_fd = ::open(parent.string().c_str(), O_RDONLY | O_DIRECTORY);
        if (dir_fd < 0) {
            return false;
        }
        const bool dir_synced = ::fsync(dir_fd) == 0;
        const bool dir_closed = ::close(dir_fd) == 0;
        if (!dir_synced || !dir_closed) return false;
        return true;
    } catch (...) {
        return false;
    }
}

} // namespace velox::services
