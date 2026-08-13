#include "velox/services/segment_scheduler.hpp"

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <exception>
#include <mutex>
#include <string>
#include <thread>

namespace velox::media {

namespace {

SegmentTaskResult claimExceedsBudget(const SegmentResourceClaim& claim,
                                     const ExecutionBudget& budget) {
    const bool cpu_oversubscribed =
        budget.cpu_tokens > 0 && claim.cpu_tokens > budget.cpu_tokens;
    const bool memory_oversubscribed =
        budget.memory_bytes > 0 &&
        claim.estimated_memory_bytes > budget.memory_bytes;
    if (cpu_oversubscribed || memory_oversubscribed) {
        return SegmentTaskResult{
            false, "segment resource claim exceeds execution budget"};
    }
    return SegmentTaskResult{true, {}};
}

} // namespace

SegmentScheduler::SegmentScheduler(SegmentSchedulerConfig config)
    : budget_(config.budget) {
    budget_.max_parallel_segments =
        std::max<std::size_t>(1, budget_.max_parallel_segments);
}

std::vector<SegmentTaskResult> SegmentScheduler::run(
    std::size_t segment_count, const Task& task) const {
    static const auto default_claim = [](std::size_t) {
        return SegmentResourceClaim{};
    };
    return run(segment_count, default_claim, task);
}

std::vector<SegmentTaskResult> SegmentScheduler::run(
    std::size_t segment_count, const Claim& claim, const Task& task) const {
    std::vector<SegmentTaskResult> results(segment_count);
    if (segment_count == 0) {
        return results;
    }

    const std::size_t workers =
        std::min(budget_.max_parallel_segments, segment_count);
    std::atomic<std::size_t> next_index{0};

    // Shared admission state. cpu_tokens / memory_bytes are the remaining
    // budget; 0-valued budget dimensions are unbounded and are never reduced.
    std::mutex admission_mutex;
    std::condition_variable admission_cv;
    int64_t remaining_cpu = budget_.cpu_tokens;
    int64_t remaining_memory = budget_.memory_bytes;

    std::vector<std::thread> threads;
    threads.reserve(workers);
    for (std::size_t worker = 0; worker < workers; ++worker) {
        threads.emplace_back([&]() {
            while (true) {
                const std::size_t index = next_index.fetch_add(1);
                if (index >= segment_count) {
                    // No more segments will be admitted; release any waiter.
                    admission_cv.notify_all();
                    return;
                }

                SegmentResourceClaim segment_claim;
                try {
                    segment_claim = claim(index);
                } catch (const std::exception& error) {
                    results[index] = SegmentTaskResult{
                        false, std::string("segment claim threw: ") + error.what()};
                    continue;
                } catch (...) {
                    results[index] = SegmentTaskResult{
                        false, "segment claim threw an unknown exception"};
                    continue;
                }

                const SegmentTaskResult over = claimExceedsBudget(segment_claim, budget_);
                if (!over.success) {
                    results[index] = over;
                    continue;
                }

                {
                    std::unique_lock<std::mutex> lock(admission_mutex);
                    admission_cv.wait(lock, [&] {
                        const bool cpu_ok = budget_.cpu_tokens <= 0 ||
                            remaining_cpu >= segment_claim.cpu_tokens;
                        const bool memory_ok = budget_.memory_bytes <= 0 ||
                            remaining_memory >= segment_claim.estimated_memory_bytes;
                        return cpu_ok && memory_ok;
                    });
                    remaining_cpu -= segment_claim.cpu_tokens;
                    remaining_memory -= segment_claim.estimated_memory_bytes;
                }

                try {
                    results[index] = task(index);
                } catch (const std::exception& error) {
                    results[index] = SegmentTaskResult{
                        false, std::string("segment task threw: ") + error.what()};
                } catch (...) {
                    results[index] = SegmentTaskResult{
                        false, "segment task threw an unknown exception"};
                }

                {
                    std::lock_guard<std::mutex> lock(admission_mutex);
                    remaining_cpu += segment_claim.cpu_tokens;
                    remaining_memory += segment_claim.estimated_memory_bytes;
                }
                admission_cv.notify_all();
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    return results;
}

} // namespace velox::media
