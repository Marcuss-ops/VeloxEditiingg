#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <vector>

namespace velox::media {

struct SegmentTaskResult {
    bool success{false};
    std::string error;
};

// The resources one segment estimates it will consume while running. The
// scheduler never inspects how the estimate is produced; it only uses it to
// decide how many segments may run concurrently against the budget.
struct SegmentResourceClaim {
    int cpu_tokens{1};
    int64_t estimated_memory_bytes{0};
};

// The host-level execution budget the scheduler must respect. cpu_tokens and
// memory_bytes are the shared pools; 0 means "unbounded" for that dimension
// (only max_parallel_segments applies). encoder_threads_per_segment records
// how many internal encoder threads each segment is configured to use. The
// scheduler treats this as a minimum CPU claim, so callers cannot accidentally
// pass a small claim while enabling a larger encoder (e.g. 6 vCPU = 2
// segments × 2 encoder threads, never 4 segments × 6 encoder threads).
struct ExecutionBudget {
    int cpu_tokens{0};
    int64_t memory_bytes{0};
    std::size_t max_parallel_segments{1};
    int encoder_threads_per_segment{0};
};

struct SegmentSchedulerConfig {
    ExecutionBudget budget;
};

class SegmentScheduler {
public:
    explicit SegmentScheduler(SegmentSchedulerConfig config = {});

    std::size_t workerCount() const { return budget_.max_parallel_segments; }
    const ExecutionBudget& budget() const { return budget_; }

    // Executes every segment at most once. Results are always returned in
    // segment-index order, regardless of completion order. The worker pool
    // is bounded by max_parallel_segments and is joined before returning.
    using Task = std::function<SegmentTaskResult(std::size_t segment_index)>;
    using Claim = std::function<SegmentResourceClaim(std::size_t segment_index)>;

    // Every segment claims one CPU token and no memory. If the budget carries
    // encoder_threads_per_segment, that value is applied as the minimum CPU
    // claim by the scheduler.
    std::vector<SegmentTaskResult> run(std::size_t segment_count, const Task& task) const;

    // Resource-aware execution: before a segment starts, its declared claim
    // must fit the remaining budget. A claim larger than the budget in any
    // bounded dimension fails closed with a deterministic error instead of
    // blocking forever; otherwise workers wait until enough budget frees up.
    std::vector<SegmentTaskResult> run(std::size_t segment_count,
                                       const Claim& claim,
                                       const Task& task) const;

private:
    ExecutionBudget budget_;
};

} // namespace velox::media
