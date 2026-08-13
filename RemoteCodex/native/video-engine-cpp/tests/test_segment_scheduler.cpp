#include "velox/services/segment_scheduler.hpp"

#include <atomic>
#include <chrono>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>

namespace {

int failures = 0;

void expect(bool condition, const std::string& message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << "\n";
        ++failures;
    }
}

velox::media::SegmentSchedulerConfig budgetConfig(
    int cpu_tokens,
    int64_t memory_bytes,
    std::size_t max_parallel_segments) {
    return velox::media::SegmentSchedulerConfig{
        velox::media::ExecutionBudget{cpu_tokens, memory_bytes,
                                      max_parallel_segments, 0}};
}

} // namespace

int main() {
    using velox::media::SegmentResourceClaim;
    using velox::media::SegmentTaskResult;

    // ── Historical bounded-worker-pool behavior (default claim overload) ──
    std::atomic<int> active{0};
    std::atomic<int> peak{0};
    velox::media::SegmentScheduler scheduler(budgetConfig(0, 0, 2));
    const auto results = scheduler.run(8, [&](std::size_t index) {
        const int now = active.fetch_add(1) + 1;
        int observed = peak.load();
        while (now > observed && !peak.compare_exchange_weak(observed, now)) {
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        active.fetch_sub(1);
        return SegmentTaskResult{true, std::to_string(index)};
    });

    expect(scheduler.workerCount() == 2, "scheduler preserves configured worker bound");
    expect(peak.load() <= 2, "scheduler never exceeds configured concurrency");
    expect(results.size() == 8, "scheduler returns one result per segment");
    for (std::size_t index = 0; index < results.size(); ++index) {
        expect(results[index].success, "successful task remains successful at its index");
        expect(results[index].error == std::to_string(index),
               "results are deterministic by segment index");
    }

    const auto failure_results = scheduler.run(3, [](std::size_t index) {
        if (index == 1) {
            throw std::runtime_error("boom");
        }
        return SegmentTaskResult{true, std::to_string(index)};
    });
    expect(failure_results[0].success, "other tasks continue after one task throws");
    expect(!failure_results[1].success, "thrown task is converted into a failed result");
    expect(failure_results[1].error == "segment task threw: boom",
           "task failure keeps a deterministic error");
    expect(failure_results[2].success, "tasks after a failed index still complete");

    // ── cpu-token budget caps concurrency ────────────────────────────────
    std::atomic<int> cpu_peak{0};
    velox::media::SegmentScheduler cpu_scheduler(budgetConfig(4, 0, 8));
    cpu_scheduler.run(
        8,
        [](std::size_t) { return SegmentResourceClaim{2, 0}; },
        [&](std::size_t) {
            const int now = active.fetch_add(1) + 1;
            int observed = cpu_peak.load();
            while (now > observed && !cpu_peak.compare_exchange_weak(observed, now)) {
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
            active.fetch_sub(1);
            return SegmentTaskResult{true, {}};
        });
    expect(cpu_peak.load() <= 2,
           "cpu-token budget (4 tokens, 2/segment) caps concurrency at 2");

    // ── memory budget caps concurrency ───────────────────────────────────
    std::atomic<int> mem_peak{0};
    velox::media::SegmentScheduler mem_scheduler(budgetConfig(0, 10, 8));
    mem_scheduler.run(
        8,
        [](std::size_t) { return SegmentResourceClaim{1, 4}; },
        [&](std::size_t) {
            const int now = active.fetch_add(1) + 1;
            int observed = mem_peak.load();
            while (now > observed && !mem_peak.compare_exchange_weak(observed, now)) {
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
            active.fetch_sub(1);
            return SegmentTaskResult{true, {}};
        });
    expect(mem_peak.load() <= 2,
           "memory budget (10 bytes, 4/segment) caps concurrency at 2");

    // ── over-budget claim fails closed instead of deadlocking ───────────
    velox::media::SegmentScheduler over_scheduler(budgetConfig(4, 0, 8));
    const auto over_results = over_scheduler.run(
        3,
        [](std::size_t index) {
            return index == 1 ? SegmentResourceClaim{8, 0}
                              : SegmentResourceClaim{1, 0};
        },
        [](std::size_t index) { return SegmentTaskResult{true, std::to_string(index)}; });
    expect(over_results[0].success, "segments before an over-budget claim still complete");
    expect(!over_results[1].success, "over-budget cpu claim fails closed");
    expect(over_results[1].error == "segment resource claim exceeds execution budget",
           "over-budget claim keeps a deterministic error");
    expect(over_results[2].success, "segments after an over-budget claim still complete");

    return failures == 0 ? 0 : 1;
}
