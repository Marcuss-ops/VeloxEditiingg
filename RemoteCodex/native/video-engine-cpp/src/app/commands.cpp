#include "velox/plan/render_plan_parser.hpp"
#include "velox/core/render_engine.hpp"
#include "velox/services/ffmpeg_progress_parser.hpp"
#include "velox/services/file_utils.hpp"
#include "velox/services/frame_pipeline.hpp"
#include "velox/services/media_probe.hpp"
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <sstream>
#include <string>

int cmdRenderPlan(int argc, char** argv) {
    std::string planPath;
    for (int i = 2; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--plan" && i + 1 < argc) {
            planPath = argv[++i];
        }
    }
    if (planPath.empty()) {
        std::cerr << "errore: --render richiede --plan <path>\n";
        return 1;
    }

    std::string content = velox::file::readFile(planPath);
    if (content.empty()) {
        std::cerr << "errore: impossibile leggere il file del piano di render: " << planPath << "\n";
        return 1;
    }

    auto planOpt = velox::plan::parseRenderPlan(content);
    if (!planOpt.has_value()) {
        std::cerr << "errore: parsing o validazione del RenderPlan fallita\n";
        return 1;
    }

    velox::core::RenderEngine engine;
    // The CLI has no caller-side progress consumer, but the engine still
    // needs a callback attached so it parses FFmpeg -progress blocks and
    // persists the final snapshot in <output>.progress.json. The worker
    // attaches its own callback through RenderClient; this no-op keeps the
    // direct canonical CLI path observably equivalent for smoke/tests.
    engine.setProgressCallback([](const velox::services::EngineProgress&) {});
    auto result = engine.render(planOpt.value());

    if (result.success) {
        std::cout << "{\"success\":true,\"job_id\":\"" << planOpt->job_id 
                  << "\",\"output_path\":\"" << result.output_path << "\"}" << std::endl;
        return 0;
    } else {
        std::cerr << "errore rendering: " << result.error << "\n";
        std::cout << "{\"success\":false,\"error\":\"" << result.error << "\"}" << std::endl;
        return 1;
    }
}

// --render-frames: the explicit in-process AVFrame producer-consumer encode
// path. Only jobs that genuinely need encoding invoke this; copy-only jobs
// stay on the zero-spawn packet path and never reach this command.
namespace {

// Emits the <output>.progress.json sidecar for the frame pipeline path so
// the Go worker's sidecar reader (engineSidecar) can project the §25
// producer-consumer health metrics exactly like the render-engine sidecar.
// The block name mirrors the C++ emitter: `frame_pipeline`.
void emitFramePipelineSidecar(const velox::media::FramePipelineConfig& config,
                               const velox::media::FramePipelineResult& pipeline,
                               bool output_durable) {
    std::error_code ec;
    const auto outputBytes = std::filesystem::file_size(config.output_path, ec);
    const int64_t totalSize = ec ? 0 : static_cast<int64_t>(outputBytes);
    double durationSeconds = 0.0;
    if (const auto probe = velox::media::probeMediaInProcess(config.input_path);
        probe.has_value()) {
        durationSeconds = probe->duration_seconds;
    }
    const auto& m = pipeline.pipeline_metrics;
    std::ostringstream s;
    s << "{";
    s << "\"frames\":" << pipeline.frames_encoded;
    s << ",\"frames_decoded\":" << pipeline.frames_decoded;
    s << ",\"zero_copy_decoded_frames\":" << pipeline.zero_copy_decoded_frames;
    s << ",\"transform_bypass_frames\":" << pipeline.transform_bypass_frames;
    s << ",\"frames_composited\":" << pipeline.frames_encoded;
    s << ",\"encode_passes\":1";
    s << ",\"concat_mode\":\"frame_pipeline\"";
    s << ",\"temp_bytes\":0";
    s << ",\"total_size\":" << totalSize;
    s << ",\"duration_seconds\":" << durationSeconds;
    s << ",\"output_durable\":" << (output_durable ? "true" : "false");
    s << ",\"frame_pipeline\":{";
    s << "\"producer_busy_ms\":" << m.producer_busy_ms;
    s << ",\"producer_wait_ms\":" << m.producer_wait_ms;
    s << ",\"consumer_busy_ms\":" << m.consumer_busy_ms;
    s << ",\"consumer_wait_ms\":" << m.consumer_wait_ms;
    s << ",\"queue_depth_avg\":" << m.queue_depth_avg;
    s << ",\"queue_depth_max\":" << m.queue_depth_max;
    s << ",\"queue_empty_ms\":" << m.queue_empty_ms;
    s << ",\"queue_full_ms\":" << m.queue_full_ms;
    s << ",\"producer_stall_ratio\":" << m.producer_stall_ratio;
    s << ",\"encoder_starvation_ratio\":" << m.encoder_starvation_ratio;
    s << ",\"backpressure_ratio\":" << m.backpressure_ratio;
    s << "}}";
    std::filesystem::path sidecar(config.output_path.string() + ".progress.json");
    if (!velox::services::SidecarWriter::writeAtomic(sidecar, s.str())) {
        std::cerr << "warning: failed to write frame pipeline sidecar at "
                  << sidecar << "\n";
    }
}

} // namespace

int cmdRenderFrames(int argc, char** argv) {
    velox::media::FramePipelineConfig config;
    for (int i = 2; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--input" && i + 1 < argc) {
            config.input_path = argv[++i];
        } else if (arg == "--output" && i + 1 < argc) {
            config.output_path = argv[++i];
        } else if (arg == "--width" && i + 1 < argc) {
            config.width = std::atoi(argv[++i]);
        } else if (arg == "--height" && i + 1 < argc) {
            config.height = std::atoi(argv[++i]);
        } else if (arg == "--fps" && i + 1 < argc) {
            config.fps_num = std::atoi(argv[++i]);
        } else if (arg == "--codec" && i + 1 < argc) {
            config.codec = argv[++i];
        } else if (arg == "--preset" && i + 1 < argc) {
            config.preset = argv[++i];
        } else if (arg == "--pool" && i + 1 < argc) {
            config.pool_capacity = std::atoi(argv[++i]);
        }
    }
    if (config.input_path.empty() || config.output_path.empty()) {
        std::cerr << "errore: --render-frames richiede --input e --output\n";
        return 1;
    }

    velox::media::FramePipelineResult pipeline;
    if (!velox::media::renderFrames(config, &pipeline)) {
        std::cerr << "errore frame pipeline: " << pipeline.error << "\n";
        std::cout << "{\"success\":false,\"error\":\"" << pipeline.error << "\"}" << std::endl;
        return 1;
    }
    emitFramePipelineSidecar(config, pipeline, pipeline.output_durable);
    const auto& m = pipeline.pipeline_metrics;
    std::cout << "{\"success\":true,\"output_path\":\""
              << config.output_path.string()
              << "\",\"frames_decoded\":" << pipeline.frames_decoded
              << ",\"frames_encoded\":" << pipeline.frames_encoded
              << ",\"zero_copy_decoded_frames\":" << pipeline.zero_copy_decoded_frames
              << ",\"transform_bypass_frames\":" << pipeline.transform_bypass_frames
              << ",\"encode_contexts\":" << pipeline.encode_contexts_created
              << ",\"peak_pool_usage\":" << pipeline.peak_pool_usage
              << ",\"output_durable\":"
              << (pipeline.output_durable ? "true" : "false")
              << ",\"pipeline_metrics\":{"
              << "\"producer_busy_ms\":" << m.producer_busy_ms
              << ",\"producer_wait_ms\":" << m.producer_wait_ms
              << ",\"consumer_busy_ms\":" << m.consumer_busy_ms
              << ",\"consumer_wait_ms\":" << m.consumer_wait_ms
              << ",\"queue_depth_avg\":" << m.queue_depth_avg
              << ",\"queue_depth_max\":" << m.queue_depth_max
              << ",\"queue_empty_ms\":" << m.queue_empty_ms
              << ",\"queue_full_ms\":" << m.queue_full_ms
              << ",\"producer_stall_ratio\":" << m.producer_stall_ratio
              << ",\"encoder_starvation_ratio\":" << m.encoder_starvation_ratio
              << ",\"backpressure_ratio\":" << m.backpressure_ratio
              << "}}" << std::endl;
    return 0;
}
