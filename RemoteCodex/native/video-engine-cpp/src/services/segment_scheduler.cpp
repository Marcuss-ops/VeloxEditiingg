#include "velox/services/segment_scheduler.hpp"

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <exception>
#include <thread>

namespace velox::media {

SegmentScheduler::SegmentScheduler(SegmentSchedulerConfig config) {
    worker_count_ = std::max<std::size_t>(1, config.max_parallel_segments);
}

std::vector<SegmentTaskResult> SegmentScheduler::run(
    std::size_t segment_count, const Task& task) const {
    std::vector<SegmentTaskResult> results(segment_count);
    if (segment_count == 0) {
        return results;
    }

    const std::size_t workers = std::min(worker_count_, segment_count);
    std::atomic<std::size_t> next_index{0};
    std::vector<std::thread> threads;
    threads.reserve(workers);
    for (std::size_t worker = 0; worker < workers; ++worker) {
        threads.emplace_back([&]() {
            while (true) {
                const std::size_t index = next_index.fetch_add(1);
                if (index >= segment_count) {
                    return;
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
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    return results;
}

} // namespace velox::media
