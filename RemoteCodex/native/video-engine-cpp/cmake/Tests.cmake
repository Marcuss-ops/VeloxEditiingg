enable_testing()

function(velox_configure_test target)
    target_include_directories(${target} PRIVATE include src include/velox)
    if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        target_compile_options(${target} PRIVATE -O2)
    endif()
endfunction()

add_executable(velox_ffmpeg_progress_parser_tests
    tests/test_ffmpeg_progress_parser.cpp
    src/services/ffmpeg_progress_parser.cpp)
velox_configure_test(velox_ffmpeg_progress_parser_tests)
add_test(NAME ffmpeg_progress_parser_tests COMMAND velox_ffmpeg_progress_parser_tests)

add_executable(velox_segment_execution_tests
    tests/test_segment_execution.cpp
    src/services/segment_execution.cpp)
velox_configure_test(velox_segment_execution_tests)
add_test(NAME segment_execution_tests COMMAND velox_segment_execution_tests)

add_executable(velox_segment_scheduler_tests
    tests/test_segment_scheduler.cpp
    src/services/segment_scheduler.cpp)
velox_configure_test(velox_segment_scheduler_tests)
add_test(NAME segment_scheduler_tests COMMAND velox_segment_scheduler_tests)

add_executable(velox_execution_plan_tests
    tests/test_execution_plan.cpp
    src/core/execution_plan.cpp
    src/services/segment_execution.cpp)
velox_configure_test(velox_execution_plan_tests)
add_test(NAME execution_plan_tests COMMAND velox_execution_plan_tests)

add_executable(velox_canonical_video_profile_tests
    tests/test_canonical_video_profile.cpp
    src/core/canonical_video_profile.cpp
    src/services/segment_execution.cpp)
velox_configure_test(velox_canonical_video_profile_tests)
add_test(NAME canonical_video_profile_tests COMMAND velox_canonical_video_profile_tests)

add_executable(velox_render_engine_helpers_tests
    tests/test_render_engine_helpers.cpp
    src/core/render_engine_helpers.cpp
    src/services/ffmpeg_progress_parser.cpp
    src/services/file_utils.cpp
    src/services/io_counters.cpp)
target_include_directories(velox_render_engine_helpers_tests PRIVATE include src src/core include/velox)
if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(velox_render_engine_helpers_tests PRIVATE -O2)
endif()
add_test(NAME render_engine_helpers_tests COMMAND velox_render_engine_helpers_tests)

add_executable(velox_frame_graph_tests
    tests/test_frame_graph.cpp
    ${VELOX_FRAME_GRAPH_TEST_SOURCES})
velox_configure_test(velox_frame_graph_tests)
add_test(NAME frame_graph_tests COMMAND velox_frame_graph_tests)

add_executable(velox_frame_overlay_tests
    tests/test_frame_overlay.cpp
    ${VELOX_FRAME_OVERLAY_TEST_SOURCES})
velox_configure_test(velox_frame_overlay_tests)
add_test(NAME frame_overlay_tests COMMAND velox_frame_overlay_tests)

add_executable(velox_frame_overlay_simd_tests
    tests/test_frame_overlay_simd.cpp
    ${VELOX_FRAME_OVERLAY_TEST_SOURCES})
velox_configure_test(velox_frame_overlay_simd_tests)
add_test(NAME frame_overlay_simd_tests COMMAND velox_frame_overlay_simd_tests)

add_executable(velox_audio_plan_tests
    tests/test_audio_plan.cpp
    src/audio/audio_plan.cpp)
velox_configure_test(velox_audio_plan_tests)
add_test(NAME audio_plan_tests COMMAND velox_audio_plan_tests)

add_executable(velox_audio_timeline_tests
    tests/test_audio_timeline.cpp
    src/audio/audio_plan.cpp
    src/audio/audio_benchmark.cpp
    ${VELOX_RENDER_ENGINE_SOURCES}
    ${VELOX_FRAME_PIPELINE_SOURCES}
    src/services/file_utils.cpp
    src/services/io_counters.cpp
    src/services/media_probe.cpp
    ${VELOX_PACKET_PIPELINE_FACADE_SOURCE}
    ${VELOX_PACKET_PIPELINE_COMPONENT_SOURCES}
    ${VELOX_SEGMENT_EXECUTION_LIBAV_SOURCES}
    ${VELOX_SEGMENT_EXECUTION_SOURCES}
    ${VELOX_MEDIA_UTILS_SOURCES}
    src/services/ffmpeg_progress_parser.cpp
    src/telemetry/emitter.cpp)
velox_configure_test(velox_audio_timeline_tests)
velox_link_ffmpeg(velox_audio_timeline_tests)
add_test(NAME audio_timeline_tests COMMAND velox_audio_timeline_tests)

add_executable(velox_emitter_tests
    tests/test_emitter.cpp
    src/audio/audio_plan.cpp
    src/audio/audio_benchmark.cpp
    src/telemetry/emitter.cpp
    ${VELOX_RENDER_ENGINE_SOURCES}
    ${VELOX_FRAME_PIPELINE_SOURCES}
    src/services/file_utils.cpp
    src/services/io_counters.cpp
    src/services/media_probe.cpp
    ${VELOX_PACKET_PIPELINE_FACADE_SOURCE}
    ${VELOX_PACKET_PIPELINE_COMPONENT_SOURCES}
    ${VELOX_SEGMENT_EXECUTION_LIBAV_SOURCES}
    ${VELOX_SEGMENT_EXECUTION_SOURCES}
    ${VELOX_MEDIA_UTILS_SOURCES}
    src/services/ffmpeg_progress_parser.cpp)
velox_configure_test(velox_emitter_tests)
velox_link_ffmpeg(velox_emitter_tests)
add_test(NAME emitter_tests COMMAND velox_emitter_tests)

add_executable(velox_looped_music_tests
    tests/test_looped_music.cpp
    src/audio/audio_plan.cpp
    src/audio/audio_benchmark.cpp
    ${VELOX_RENDER_ENGINE_SOURCES}
    ${VELOX_FRAME_PIPELINE_SOURCES}
    src/services/file_utils.cpp
    src/services/io_counters.cpp
    src/services/media_probe.cpp
    ${VELOX_PACKET_PIPELINE_FACADE_SOURCE}
    ${VELOX_PACKET_PIPELINE_COMPONENT_SOURCES}
    ${VELOX_SEGMENT_EXECUTION_LIBAV_SOURCES}
    ${VELOX_SEGMENT_EXECUTION_SOURCES}
    ${VELOX_MEDIA_UTILS_SOURCES}
    src/services/ffmpeg_progress_parser.cpp
    src/telemetry/emitter.cpp)
velox_configure_test(velox_looped_music_tests)
velox_link_ffmpeg(velox_looped_music_tests)
add_test(NAME looped_music_tests COMMAND velox_looped_music_tests)
set_tests_properties(looped_music_tests PROPERTIES ENVIRONMENT "VELOX_LOOPED_MUSIC_TEST=1")

if(VELOX_ENABLE_LIBAV)
    add_executable(velox_media_packet_output_sink_tests
        tests/test_media_packet_output_sink.cpp
        src/services/media_packet_output_sink.cpp
        src/services/io_counters.cpp)
    velox_configure_test(velox_media_packet_output_sink_tests)
    velox_link_ffmpeg(velox_media_packet_output_sink_tests)
    add_test(NAME media_packet_output_sink_tests COMMAND velox_media_packet_output_sink_tests)

    add_executable(velox_media_probe_tests
        tests/test_media_probe.cpp
        src/services/file_utils.cpp
        src/services/io_counters.cpp
        src/services/media_probe.cpp
        ${VELOX_PACKET_PIPELINE_FACADE_SOURCE}
        ${VELOX_PACKET_PIPELINE_COMPONENT_SOURCES}
        ${VELOX_SEGMENT_EXECUTION_LIBAV_SOURCES}
        ${VELOX_SEGMENT_EXECUTION_SOURCES}
        ${VELOX_MEDIA_UTILS_SOURCES})
    velox_configure_test(velox_media_probe_tests)
    velox_link_ffmpeg(velox_media_probe_tests)
    add_test(NAME media_probe_tests COMMAND velox_media_probe_tests)

    add_executable(velox_media_packet_pipeline_tests
        tests/test_media_packet_pipeline.cpp
        src/services/file_utils.cpp
        src/services/io_counters.cpp
        src/services/media_probe.cpp
        ${VELOX_PACKET_PIPELINE_FACADE_SOURCE}
        ${VELOX_PACKET_PIPELINE_COMPONENT_SOURCES}
        ${VELOX_SEGMENT_EXECUTION_LIBAV_SOURCES}
        src/services/segment_execution.cpp)
    velox_configure_test(velox_media_packet_pipeline_tests)
    velox_link_ffmpeg(velox_media_packet_pipeline_tests)
    add_test(NAME media_packet_pipeline_tests COMMAND velox_media_packet_pipeline_tests)

    add_executable(velox_segment_execution_libav_tests
        tests/test_segment_execution_libav.cpp
        src/services/file_utils.cpp
        src/services/io_counters.cpp
        src/services/media_probe.cpp
        ${VELOX_PACKET_PIPELINE_FACADE_SOURCE}
        ${VELOX_PACKET_PIPELINE_COMPONENT_SOURCES}
        ${VELOX_SEGMENT_EXECUTION_LIBAV_SOURCES}
        src/services/segment_execution.cpp)
    velox_configure_test(velox_segment_execution_libav_tests)
    velox_link_ffmpeg(velox_segment_execution_libav_tests)
    add_test(NAME segment_execution_libav_tests COMMAND velox_segment_execution_libav_tests)

    add_executable(velox_packet_components_tests
        tests/test_packet_components.cpp
        src/services/file_utils.cpp
        src/services/io_counters.cpp
        src/services/media_probe.cpp
        ${VELOX_PACKET_PIPELINE_FACADE_SOURCE}
        ${VELOX_PACKET_PIPELINE_COMPONENT_SOURCES}
        ${VELOX_SEGMENT_EXECUTION_LIBAV_SOURCES}
        src/services/segment_execution.cpp)
    velox_configure_test(velox_packet_components_tests)
    velox_link_ffmpeg(velox_packet_components_tests)
    add_test(NAME packet_components_tests COMMAND velox_packet_components_tests)

    add_executable(velox_render_copy_only_zero_intermediates_tests
        tests/test_render_copy_only_zero_intermediates.cpp
        src/audio/audio_plan.cpp
        src/audio/audio_benchmark.cpp
        ${VELOX_RENDER_ENGINE_SOURCES}
        ${VELOX_FRAME_PIPELINE_SOURCES}
        src/services/file_utils.cpp
        src/services/io_counters.cpp
        src/services/media_probe.cpp
        ${VELOX_PACKET_PIPELINE_FACADE_SOURCE}
        ${VELOX_PACKET_PIPELINE_COMPONENT_SOURCES}
        ${VELOX_SEGMENT_EXECUTION_LIBAV_SOURCES}
        ${VELOX_SEGMENT_EXECUTION_SOURCES}
        ${VELOX_MEDIA_UTILS_SOURCES}
        src/services/ffmpeg_progress_parser.cpp
        src/telemetry/emitter.cpp)
    velox_configure_test(velox_render_copy_only_zero_intermediates_tests)
    velox_link_ffmpeg(velox_render_copy_only_zero_intermediates_tests)
    add_test(NAME render_copy_only_zero_intermediates_tests COMMAND velox_render_copy_only_zero_intermediates_tests)

    add_executable(velox_render_mixed_tests
        tests/test_render_mixed.cpp
        src/audio/audio_plan.cpp
        src/audio/audio_benchmark.cpp
        ${VELOX_RENDER_ENGINE_SOURCES}
        ${VELOX_FRAME_PIPELINE_SOURCES}
        src/services/file_utils.cpp
        src/services/io_counters.cpp
        src/services/media_probe.cpp
        ${VELOX_PACKET_PIPELINE_FACADE_SOURCE}
        ${VELOX_PACKET_PIPELINE_COMPONENT_SOURCES}
        ${VELOX_SEGMENT_EXECUTION_LIBAV_SOURCES}
        ${VELOX_SEGMENT_EXECUTION_SOURCES}
        ${VELOX_MEDIA_UTILS_SOURCES}
        src/services/ffmpeg_progress_parser.cpp
        src/telemetry/emitter.cpp)
    velox_configure_test(velox_render_mixed_tests)
    velox_link_ffmpeg(velox_render_mixed_tests)
    add_test(NAME render_mixed_tests COMMAND velox_render_mixed_tests)

    set(VELOX_RENDER_PLAN_PARSER_SOURCES
        src/plan/render_plan_parser.cpp
        src/plan/render_plan_parser_helpers.cpp
        src/plan/render_plan_parser_v1.cpp
        src/plan/render_plan_parser_v2.cpp)

    add_executable(velox_render_plan_v2_tests
        tests/test_render_plan_v2.cpp
        ${VELOX_RENDER_PLAN_PARSER_SOURCES}
        src/audio/audio_plan.cpp
        src/audio/audio_benchmark.cpp
        ${VELOX_RENDER_ENGINE_SOURCES}
        ${VELOX_FRAME_PIPELINE_SOURCES}
        src/services/file_utils.cpp
        src/services/io_counters.cpp
        src/services/media_probe.cpp
        ${VELOX_PACKET_PIPELINE_FACADE_SOURCE}
        ${VELOX_PACKET_PIPELINE_COMPONENT_SOURCES}
        ${VELOX_SEGMENT_EXECUTION_LIBAV_SOURCES}
        ${VELOX_SEGMENT_EXECUTION_SOURCES}
        ${VELOX_MEDIA_UTILS_SOURCES}
        src/services/ffmpeg_progress_parser.cpp
        src/telemetry/emitter.cpp)
    velox_configure_test(velox_render_plan_v2_tests)
    velox_link_ffmpeg(velox_render_plan_v2_tests)
    add_test(NAME render_plan_v2_tests COMMAND velox_render_plan_v2_tests)

    add_executable(velox_render_visual_replacement_golden_tests
        tests/test_render_visual_replacement_golden.cpp
        ${VELOX_RENDER_PLAN_PARSER_SOURCES}
        src/audio/audio_plan.cpp
        src/audio/audio_benchmark.cpp
        ${VELOX_RENDER_ENGINE_SOURCES}
        ${VELOX_FRAME_PIPELINE_SOURCES}
        src/services/file_utils.cpp
        src/services/io_counters.cpp
        src/services/media_probe.cpp
        ${VELOX_PACKET_PIPELINE_FACADE_SOURCE}
        ${VELOX_PACKET_PIPELINE_COMPONENT_SOURCES}
        ${VELOX_SEGMENT_EXECUTION_LIBAV_SOURCES}
        ${VELOX_SEGMENT_EXECUTION_SOURCES}
        ${VELOX_MEDIA_UTILS_SOURCES}
        src/services/ffmpeg_progress_parser.cpp
        src/telemetry/emitter.cpp)
    velox_configure_test(velox_render_visual_replacement_golden_tests)
    velox_link_ffmpeg(velox_render_visual_replacement_golden_tests)
    add_test(NAME render_visual_replacement_golden_tests COMMAND velox_render_visual_replacement_golden_tests)

    add_executable(velox_render_visual_replacement_benchmark_tests
        tests/test_render_visual_replacement_benchmark.cpp
        ${VELOX_RENDER_PLAN_PARSER_SOURCES}
        src/audio/audio_plan.cpp
        src/audio/audio_benchmark.cpp
        ${VELOX_RENDER_ENGINE_SOURCES}
        ${VELOX_FRAME_PIPELINE_SOURCES}
        src/services/file_utils.cpp
        src/services/io_counters.cpp
        src/services/media_probe.cpp
        ${VELOX_PACKET_PIPELINE_FACADE_SOURCE}
        ${VELOX_PACKET_PIPELINE_COMPONENT_SOURCES}
        ${VELOX_SEGMENT_EXECUTION_LIBAV_SOURCES}
        ${VELOX_SEGMENT_EXECUTION_SOURCES}
        ${VELOX_MEDIA_UTILS_SOURCES}
        src/services/ffmpeg_progress_parser.cpp
        src/telemetry/emitter.cpp)
    velox_configure_test(velox_render_visual_replacement_benchmark_tests)
    velox_link_ffmpeg(velox_render_visual_replacement_benchmark_tests)
    add_test(NAME render_visual_replacement_benchmark_tests COMMAND velox_render_visual_replacement_benchmark_tests)

    add_executable(velox_frame_pipeline_tests
        tests/test_frame_pipeline.cpp
        ${VELOX_FRAME_PIPELINE_SOURCES}
        ${VELOX_PACKET_PIPELINE_FACADE_SOURCE}
        ${VELOX_PACKET_PIPELINE_COMPONENT_SOURCES}
        ${VELOX_SEGMENT_EXECUTION_LIBAV_SOURCES}
        src/services/segment_execution.cpp
        src/services/media_probe.cpp
        src/services/file_utils.cpp
        src/services/io_counters.cpp)
    velox_configure_test(velox_frame_pipeline_tests)
    velox_link_ffmpeg(velox_frame_pipeline_tests)
    add_test(NAME frame_pipeline_tests COMMAND velox_frame_pipeline_tests)
endif()

foreach(velox_target
        velox_video_engine
        velox_audio_timeline_tests
        velox_emitter_tests
        velox_looped_music_tests)
    add_dependencies(${velox_target} velox_telemetry_catalog_codegen)
endforeach()
foreach(velox_target
        velox_media_probe_tests
        velox_media_packet_output_sink_tests
        velox_media_packet_pipeline_tests
        velox_render_copy_only_zero_intermediates_tests
        velox_render_mixed_tests
        velox_render_plan_v2_tests
        velox_render_visual_replacement_golden_tests
        velox_render_visual_replacement_benchmark_tests)
    if(TARGET ${velox_target})
        add_dependencies(${velox_target} velox_telemetry_catalog_codegen)
    endif()
endforeach()
