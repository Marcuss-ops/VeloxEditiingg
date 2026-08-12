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

} // namespace

int main() {
    std::atomic<int> active{0};
    std::atomic<int> peak{0};
    velox::media::SegmentScheduler scheduler({2});
    const auto results = scheduler.run(8, [&](std::size_t index) {
        const int now = active.fetch_add(1) + 1;
        int observed = peak.load();
        while (now > observed && !peak.compare_exchange_weak(observed, now)) {
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        active.fetch_sub(1);
        return velox::media::SegmentTaskResult{true, std::to_string(index)};
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
        return velox::media::SegmentTaskResult{true, std::to_string(index)};
    });
    expect(failure_results[0].success, "other tasks continue after one task throws");
    expect(!failure_results[1].success, "thrown task is converted into a failed result");
    expect(failure_results[1].error == "segment task threw: boom",
           "task failure keeps a deterministic error");
    expect(failure_results[2].success, "tasks after a failed index still complete");

    return failures == 0 ? 0 : 1;
}
