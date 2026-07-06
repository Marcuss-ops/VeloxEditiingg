#pragma once

#include <chrono>
#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <vector>

namespace velox::core {

// SegmentTiming captures per-segment wall-clock, download, and encode
// counters. Populated inside RenderEngine::render() per timeline item;
// emitted as part of the sidecar segments[] array.
struct SegmentTiming {
    size_t index{0};
    size_t worker_index{0}; // always 0 in the single-threaded --render --plan path
    std::string source_type;
    double total_ms{0};
    double asset_download_ms{0};
    double ffmpeg_encode_ms{0};
    int64_t output_bytes{0};
};

// EngineMetrics is a thread-safe accumulator for named phase durations
// and per-segment timing records. Intended to be owned by RenderEngine
// and reset on every render() call. The SidecarWriter reads snapshots
// post-hoc so the engine never blocks on I/O inside timed phases.
class EngineMetrics {
public:
    // Accumulate `ms` into the named counter (calls with the same name
    // are summed, e.g. per-segment asset downloads).
    void addMs(const std::string& name, double ms) {
        std::lock_guard<std::mutex> lock(mu_);
        phase_ms_[name] += ms;
    }

    // Return a copy of the accumulated phase counters. Safe to call
    // from the sidecar writer on any thread after render() finishes.
    std::map<std::string, double> phaseSnapshot() const {
        std::lock_guard<std::mutex> lock(mu_);
        return phase_ms_;
    }

    // Append one segment timing record.
    void addSegment(const SegmentTiming& seg) {
        std::lock_guard<std::mutex> lock(mu_);
        segments_.push_back(seg);
    }

    // Return a copy of the segment timeline. Safe for the sidecar writer.
    std::vector<SegmentTiming> segmentsSnapshot() const {
        std::lock_guard<std::mutex> lock(mu_);
        return segments_;
    }

    // Clears all counters and segment records. Call at beginning of render().
    void reset() {
        std::lock_guard<std::mutex> lock(mu_);
        phase_ms_.clear();
        segments_.clear();
    }

private:
    mutable std::mutex mu_;
    std::map<std::string, double> phase_ms_;
    std::vector<SegmentTiming> segments_;
};

// ScopedTimer is an RAII helper that records elapsed wall-clock time
// (in milliseconds) into an EngineMetrics counter on destruction.
// Uncopyable, movable — intended to live on the stack inside a scope.
class ScopedTimer {
public:
    ScopedTimer(EngineMetrics& sink, std::string name)
        : sink_(&sink), name_(std::move(name)),
          start_(std::chrono::steady_clock::now()) {}

    ~ScopedTimer() {
        if (sink_) {
            auto end = std::chrono::steady_clock::now();
            double ms =
                std::chrono::duration<double, std::milli>(end - start_).count();
            sink_->addMs(name_, ms);
        }
    }

    // Movable, not copyable.
    // The moved-from timer has its sink_ nulled so the destructor is a no-op.
    ScopedTimer(ScopedTimer&& other) noexcept
        : sink_(other.sink_), name_(std::move(other.name_)), start_(other.start_) {
        other.sink_ = nullptr;
    }
    ScopedTimer& operator=(ScopedTimer&& other) noexcept {
        if (this != &other) {
            sink_ = other.sink_;
            name_ = std::move(other.name_);
            start_ = other.start_;
            other.sink_ = nullptr;
        }
        return *this;
    }

    ScopedTimer(const ScopedTimer&) = delete;
    ScopedTimer& operator=(const ScopedTimer&) = delete;

private:
    EngineMetrics* sink_;
    std::string name_;
    std::chrono::steady_clock::time_point start_;
};

} // namespace velox::core
