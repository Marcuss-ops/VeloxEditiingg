#pragma once

#include <cstddef>
#include <functional>
#include <string>
#include <vector>

namespace velox::media {

struct SegmentTaskResult {
    bool success{false};
    std::string error;
};

struct SegmentSchedulerConfig {
    // The default is deliberately conservative because each native segment
    // already owns decoder/render/encoder threads. Operators can raise this
    // through VELOX_NATIVE_SEGMENT_WORKERS after measuring the host.
    std::size_t max_parallel_segments{1};
};

class SegmentScheduler {
public:
    explicit SegmentScheduler(SegmentSchedulerConfig config = {});

    std::size_t workerCount() const { return worker_count_; }

    // Executes every segment at most once. Results are always returned in
    // segment-index order, regardless of completion order. The worker pool
    // is bounded by max_parallel_segments and is joined before returning.
    using Task = std::function<SegmentTaskResult(std::size_t segment_index)>;
    std::vector<SegmentTaskResult> run(std::size_t segment_count, const Task& task) const;

private:
    std::size_t worker_count_{1};
};

} // namespace velox::media
