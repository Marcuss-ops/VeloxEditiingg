#include "velox/core/render_engine.hpp"

#include <algorithm>

namespace velox::core {

void RenderEngine::recordFramePipeline(const media::FramePipelineResult& result) {
    const auto& metrics = result.pipeline_metrics;
    frame_pipeline_metrics_.producer_busy_ms += metrics.producer_busy_ms;
    frame_pipeline_metrics_.producer_wait_ms += metrics.producer_wait_ms;
    frame_pipeline_metrics_.consumer_busy_ms += metrics.consumer_busy_ms;
    frame_pipeline_metrics_.consumer_wait_ms += metrics.consumer_wait_ms;
    frame_pipeline_metrics_.queue_depth_avg += metrics.queue_depth_avg;
    frame_pipeline_metrics_.queue_depth_max = std::max(
        frame_pipeline_metrics_.queue_depth_max, metrics.queue_depth_max);
    frame_pipeline_metrics_.queue_empty_ms += metrics.queue_empty_ms;
    frame_pipeline_metrics_.queue_full_ms += metrics.queue_full_ms;
    ++frame_pipeline_runs_;

    const auto producer_total = frame_pipeline_metrics_.producer_busy_ms +
        frame_pipeline_metrics_.producer_wait_ms;
    const auto consumer_total = frame_pipeline_metrics_.consumer_busy_ms +
        frame_pipeline_metrics_.consumer_wait_ms;
    frame_pipeline_metrics_.producer_stall_ratio = producer_total > 0
        ? static_cast<double>(frame_pipeline_metrics_.producer_wait_ms) / producer_total
        : 0.0;
    frame_pipeline_metrics_.encoder_starvation_ratio = consumer_total > 0
        ? static_cast<double>(frame_pipeline_metrics_.consumer_wait_ms) / consumer_total
        : 0.0;
    frame_pipeline_metrics_.backpressure_ratio = producer_total > 0
        ? static_cast<double>(frame_pipeline_metrics_.queue_full_ms) / producer_total
        : 0.0;
}

} // namespace velox::core
