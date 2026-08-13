cmake_minimum_required(VERSION 3.20)
project(velox_video_engine_cpp LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# ─── Optional in-process LibAV backend ───────────────────────────────────
# Default OFF: builds without libavformat/libavcodec/libavutil use the
# ffprobe CLI for media probing and the legacy segment/concat render path
# (the pre-LibAV behavior). Enable with -DVELOX_ENABLE_LIBAV=ON to get the
# in-process MediaProbe (no ffprobe children), the zero-spawn copy-only
# packet mux and the FINAL_AUDIO_COPY transport verification.
#
# ON vs OFF parity note for copy_only plans: with the packet branch active
# (ON) the renderer rejects subtitle tracks and non-video sources; in the
# legacy fallback (OFF) those same plans are handled by the classic loop
# (subtitle burn is a full re-encode, image sources are encoded as
# segments). OFF is intentionally the pre-LibAV contract, not the strict
# packet contract.
#
# FINAL_AUDIO_COPY gate (ON only): the copy-only packet path now requires
# the prepared final audio to be a verified MP4-AAC track covering the
# video timeline and fails closed with copy_only_audio_not_final_copy
# otherwise (e.g. MP3/PCM/WAV final audio). Upstream callers that feed
# non-AAC final audio to copy_only plans must repair the audio upstream;
# there is no in-process re-encode on this path.
option(VELOX_ENABLE_LIBAV
    "Build with in-process libavformat/libavcodec/libavutil (MediaProbe + packet mux)"
    OFF)

if(VELOX_ENABLE_LIBAV)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(FFMPEG REQUIRED IMPORTED_TARGET
        libavformat
        libavcodec
        libavutil
        libswscale
    )
endif()

function(velox_link_ffmpeg target)
    if(VELOX_ENABLE_LIBAV)
        target_link_libraries(${target} PRIVATE PkgConfig::FFMPEG)
    endif()
endfunction()

# Propagate the build flag to every engine/test target below (the libav
# sources compile to the ffprobe fallback when the flag is absent).
add_compile_definitions($<$<BOOL:${VELOX_ENABLE_LIBAV}>:VELOX_ENABLE_LIBAV>)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

# Deterministic, portable release flags. We deliberately avoid
# -march=native / -mtune=native: those pin the binary to the build host's
# CPU, which breaks the worker bundle's reproducibility contract (the
# Linux build host may differ from the worker host's CPU). -O3 + LTO is
# the highest-impact portable optimisation mix for the FFmpeg-heavy
# pipeline we link against.
if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    add_compile_options(
        -O3
        -ffile-prefix-map=${CMAKE_CURRENT_SOURCE_DIR}=.
        -ffile-prefix-map=${CMAKE_CURRENT_BINARY_DIR}=.
    )
    add_link_options(-flto -Wl,--build-id=none)
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION_RELEASE ON)
endif()

# The language-neutral catalog is the only telemetry taxonomy source. Build
# into the binary tree so CMake never mutates the source checkout. Full source
# checkouts regenerate the binding with Go; minimal release builders may use
# the checked-in generated header, whose freshness is enforced by
# scripts/ci/check-telemetry-catalog.sh.
find_program(VELOX_GO_EXECUTABLE go)
set(VELOX_TELEMETRY_GENERATED_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")
set(VELOX_TELEMETRY_GENERATED_HEADER
    "${VELOX_TELEMETRY_GENERATED_DIR}/velox/telemetry/catalog_generated.hpp")
if(VELOX_GO_EXECUTABLE)
    set(VELOX_SHARED_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../../../shared")
    add_custom_command(
        OUTPUT "${VELOX_TELEMETRY_GENERATED_HEADER}"
        COMMAND "${VELOX_GO_EXECUTABLE}" run ./telemetry/cmd/cataloggen
                -input telemetry/schema/catalog.json
                -output "${VELOX_TELEMETRY_GENERATED_HEADER}"
        WORKING_DIRECTORY "${VELOX_SHARED_DIR}"
        DEPENDS
            "${VELOX_SHARED_DIR}/telemetry/schema/catalog.json"
            "${VELOX_SHARED_DIR}/telemetry/catalog.go"
            "${VELOX_SHARED_DIR}/telemetry/catalog_source.go"
            "${VELOX_SHARED_DIR}/telemetry/descriptor.go"
            "${VELOX_SHARED_DIR}/telemetry/fact_owner.go"
            "${VELOX_SHARED_DIR}/telemetry/cmd/cataloggen/main.go"
        COMMENT "Generating C++ telemetry catalog binding"
        VERBATIM)
else()
    set(VELOX_CHECKED_IN_TELEMETRY_HEADER
        "${CMAKE_CURRENT_SOURCE_DIR}/include/velox/telemetry/catalog_generated.hpp")
    if(NOT EXISTS "${VELOX_CHECKED_IN_TELEMETRY_HEADER}")
        message(FATAL_ERROR
            "Go is unavailable and the checked-in telemetry catalog binding is missing: "
            "${VELOX_CHECKED_IN_TELEMETRY_HEADER}")
    endif()
    add_custom_command(
        OUTPUT "${VELOX_TELEMETRY_GENERATED_HEADER}"
        COMMAND "${CMAKE_COMMAND}" -E make_directory
                "${VELOX_TELEMETRY_GENERATED_DIR}/velox/telemetry"
        COMMAND "${CMAKE_COMMAND}" -E copy_if_different
                "${VELOX_CHECKED_IN_TELEMETRY_HEADER}"
                "${VELOX_TELEMETRY_GENERATED_HEADER}"
        DEPENDS "${VELOX_CHECKED_IN_TELEMETRY_HEADER}"
        COMMENT "Copying checked-in C++ telemetry catalog binding"
        VERBATIM)
endif()
add_custom_target(velox_telemetry_catalog_codegen
    DEPENDS "${VELOX_TELEMETRY_GENERATED_HEADER}")
include_directories(BEFORE "${VELOX_TELEMETRY_GENERATED_DIR}")

add_executable(velox_video_engine
    src/main.cpp
    src/cmd_full_video.cpp
    src/video_builder.cpp
    src/plan/render_plan_parser.cpp
    src/app/commands.cpp
    src/services/file_utils.cpp
    src/services/io_counters.cpp
    src/services/media_probe.cpp
    src/services/media_packet_pipeline.cpp
    src/services/segment_execution_libav.cpp
    src/services/segment_execution.cpp
    src/services/segment_scheduler.cpp
    src/services/media_utils.cpp
    src/services/frame_pipeline.cpp
    src/render/frame_graph.cpp
    src/render/kernel_registry.cpp
    src/audio/audio_plan.cpp
    src/audio/audio_benchmark.cpp
    src/services/ffmpeg_progress_parser.cpp
    src/core/render_engine.cpp
    src/core/render_engine_helpers.cpp
    src/core/execution_plan.cpp
    src/core/canonical_video_profile.cpp
    src/telemetry/emitter.cpp
)

target_include_directories(velox_video_engine PRIVATE include src include/velox)
velox_link_ffmpeg(velox_video_engine)

# ─── F5 unit tests ──────────────────────────────────────────────────────
# Standalone test binary for the FFmpeg progress parser + sidecar
# writer. No GoogleTest dependency — runs the small in-repo expect()
# macros. CI gates on this target's exit code.
enable_testing()

add_executable(velox_ffmpeg_progress_parser_tests
    tests/test_ffmpeg_progress_parser.cpp
    src/services/ffmpeg_progress_parser.cpp
)

target_include_directories(velox_ffmpeg_progress_parser_tests
    PRIVATE include src include/velox)

# Tests must not require LTO/IPO from the main executable — keep the
# standalone binary small and stand-alone, exercised on every run.
if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(velox_ffmpeg_progress_parser_tests PRIVATE -O2)
endif()

add_test(NAME ffmpeg_progress_parser_tests
         COMMAND velox_ffmpeg_progress_parser_tests)

# Canonical segment execution resolver. This value-only test verifies the
# PacketCopy/NativeTranscode/LegacyFallback contract without a media fixture.
add_executable(velox_segment_execution_tests
    tests/test_segment_execution.cpp
    src/services/segment_execution.cpp
)

target_include_directories(velox_segment_execution_tests
    PRIVATE include src include/velox)

if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(velox_segment_execution_tests PRIVATE -O2)
endif()

add_test(NAME segment_execution_tests
         COMMAND velox_segment_execution_tests)

add_executable(velox_segment_scheduler_tests
    tests/test_segment_scheduler.cpp
    src/services/segment_scheduler.cpp
)

target_include_directories(velox_segment_scheduler_tests
    PRIVATE include src include/velox)

if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(velox_segment_scheduler_tests PRIVATE -O2)
endif()

add_test(NAME segment_scheduler_tests
         COMMAND velox_segment_scheduler_tests)

# RuntimeExecutionPlanCompiler: one execution decision per segment becomes
# the runtime SSOT. Value-only test; no LibAV or media fixture required.
add_executable(velox_execution_plan_tests
    tests/test_execution_plan.cpp
    src/core/execution_plan.cpp
    src/services/segment_execution.cpp
)

target_include_directories(velox_execution_plan_tests
    PRIVATE include src include/velox)

if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(velox_execution_plan_tests PRIVATE -O2)
endif()

add_test(NAME execution_plan_tests
         COMMAND velox_execution_plan_tests)

# CanonicalVideoProfileV1: the canonical mixed-renderer output identity and
# its MediaSignature bridge. Value-only test; no LibAV or media fixture.
add_executable(velox_canonical_video_profile_tests
    tests/test_canonical_video_profile.cpp
    src/core/canonical_video_profile.cpp
    src/services/segment_execution.cpp
)

target_include_directories(velox_canonical_video_profile_tests
    PRIVATE include src include/velox)

if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(velox_canonical_video_profile_tests PRIVATE -O2)
endif()

add_test(NAME canonical_video_profile_tests
         COMMAND velox_canonical_video_profile_tests)

# Focused tests for the private render-engine command/parsing helpers.
add_executable(velox_render_engine_helpers_tests
    tests/test_render_engine_helpers.cpp
    src/core/render_engine_helpers.cpp
    src/services/ffmpeg_progress_parser.cpp
    src/services/file_utils.cpp
    src/services/io_counters.cpp
)

target_include_directories(velox_render_engine_helpers_tests
    PRIVATE include src src/core include/velox)

if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(velox_render_engine_helpers_tests PRIVATE -O2)
endif()

add_test(NAME render_engine_helpers_tests
         COMMAND velox_render_engine_helpers_tests)

add_executable(velox_audio_plan_tests
    tests/test_audio_plan.cpp
    src/audio/audio_plan.cpp
)

target_include_directories(velox_audio_plan_tests
    PRIVATE include src include/velox)

if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(velox_audio_plan_tests PRIVATE -O2)
endif()

add_test(NAME audio_plan_tests
         COMMAND velox_audio_plan_tests)

add_executable(velox_audio_timeline_tests
    tests/test_audio_timeline.cpp
    src/audio/audio_plan.cpp
    src/audio/audio_benchmark.cpp
    src/core/render_engine.cpp
    src/core/canonical_video_profile.cpp
    src/core/render_engine_helpers.cpp
    src/services/frame_pipeline.cpp
    src/render/frame_graph.cpp
    src/render/kernel_registry.cpp
    src/services/file_utils.cpp
    src/services/io_counters.cpp
    src/services/media_probe.cpp
    src/services/media_packet_pipeline.cpp
    src/services/segment_execution_libav.cpp
    src/services/segment_execution.cpp
    src/services/segment_scheduler.cpp
    src/services/media_utils.cpp
    src/services/ffmpeg_progress_parser.cpp
    src/telemetry/emitter.cpp
)

target_include_directories(velox_audio_timeline_tests
    PRIVATE include src include/velox)

if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(velox_audio_timeline_tests PRIVATE -O2)
endif()
velox_link_ffmpeg(velox_audio_timeline_tests)

add_test(NAME audio_timeline_tests
         COMMAND velox_audio_timeline_tests)

# ─── Block-1 phase recorder tests ──────────────────────────────────────
# Standalone test binary for the telemetry PhaseRecorder + ScopedPhase
# RAII helper. No GoogleTest dependency — the in-repo expect() macros.
add_executable(velox_emitter_tests
    tests/test_emitter.cpp
    src/audio/audio_plan.cpp
    src/audio/audio_benchmark.cpp
    src/telemetry/emitter.cpp
    src/core/render_engine.cpp
    src/core/canonical_video_profile.cpp
    src/core/render_engine_helpers.cpp
    src/services/frame_pipeline.cpp
    src/render/frame_graph.cpp
    src/render/kernel_registry.cpp
    src/services/file_utils.cpp
    src/services/io_counters.cpp
    src/services/media_probe.cpp
    src/services/media_packet_pipeline.cpp
    src/services/segment_execution_libav.cpp
    src/services/segment_execution.cpp
    src/services/segment_scheduler.cpp
    src/services/media_utils.cpp
    src/services/ffmpeg_progress_parser.cpp
)

target_include_directories(velox_emitter_tests
    PRIVATE include src include/velox)

if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(velox_emitter_tests PRIVATE -O2)
endif()
velox_link_ffmpeg(velox_emitter_tests)

add_test(NAME emitter_tests
         COMMAND velox_emitter_tests)

# Integration smoke test for the mandatory looped-music contract. It uses
# the system FFmpeg binary to render a short source with loop=true and an
# omitted duration_seconds, then checks the 95-second output duration and
# process cleanup. The canonical Docker builder installs FFmpeg; local developers
# must provide FFmpeg and pgrep to run the full CTest suite.
add_executable(velox_looped_music_tests
    tests/test_looped_music.cpp
    src/audio/audio_plan.cpp
    src/audio/audio_benchmark.cpp
    src/core/render_engine.cpp
    src/core/canonical_video_profile.cpp
    src/core/render_engine_helpers.cpp
    src/services/frame_pipeline.cpp
    src/render/frame_graph.cpp
    src/render/kernel_registry.cpp
    src/services/file_utils.cpp
    src/services/io_counters.cpp
    src/services/media_probe.cpp
    src/services/media_packet_pipeline.cpp
    src/services/segment_execution_libav.cpp
    src/services/segment_execution.cpp
    src/services/segment_scheduler.cpp
    src/services/media_utils.cpp
    src/services/ffmpeg_progress_parser.cpp
    src/telemetry/emitter.cpp
)

target_include_directories(velox_looped_music_tests
    PRIVATE include src include/velox)

if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(velox_looped_music_tests PRIVATE -O2)
endif()
velox_link_ffmpeg(velox_looped_music_tests)

add_test(NAME looped_music_tests
         COMMAND velox_looped_music_tests)
set_tests_properties(looped_music_tests PROPERTIES
    ENVIRONMENT "VELOX_LOOPED_MUSIC_TEST=1")

# In-process LibAV probe tests. The test hides ffprobe from PATH after
# generating fixtures, proving metadata validation no longer depends on a
# child process. Only meaningful with the LibAV backend enabled: without it
# there is no in-process probe to exercise.
if(VELOX_ENABLE_LIBAV)
    add_executable(velox_media_probe_tests
        tests/test_media_probe.cpp
        src/services/file_utils.cpp
        src/services/io_counters.cpp
        src/services/media_probe.cpp
        src/services/media_packet_pipeline.cpp
        src/services/segment_execution_libav.cpp
        src/services/segment_execution.cpp
        src/services/segment_scheduler.cpp
        src/services/media_utils.cpp
    )

    target_include_directories(velox_media_probe_tests
        PRIVATE include src include/velox)

    if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        target_compile_options(velox_media_probe_tests PRIVATE -O2)
    endif()
    velox_link_ffmpeg(velox_media_probe_tests)

    add_test(NAME media_probe_tests
             COMMAND velox_media_probe_tests)

    # In-process packet copy pipeline tests. Fixtures are generated before
    # the test hides ffmpeg/ffprobe from PATH; the mux itself must execute
    # no child media process and must publish one timestamp-rewritten MP4
    # atomically.
    add_executable(velox_media_packet_pipeline_tests
        tests/test_media_packet_pipeline.cpp
        src/services/file_utils.cpp
        src/services/io_counters.cpp
        src/services/media_probe.cpp
        src/services/media_packet_pipeline.cpp
        src/services/segment_execution_libav.cpp
        src/services/segment_execution.cpp
    )

    target_include_directories(velox_media_packet_pipeline_tests
        PRIVATE include src include/velox)

    if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        target_compile_options(velox_media_packet_pipeline_tests PRIVATE -O2)
    endif()
    velox_link_ffmpeg(velox_media_packet_pipeline_tests)

    add_test(NAME media_packet_pipeline_tests
             COMMAND velox_media_packet_pipeline_tests)

    # probeSegmentForExecution: the AVStream -> MediaSignature bridge plus
    # keyframe-safety probe the mixed renderer uses to resolve each segment.
    add_executable(velox_segment_execution_libav_tests
        tests/test_segment_execution_libav.cpp
        src/services/file_utils.cpp
        src/services/io_counters.cpp
        src/services/media_probe.cpp
        src/services/media_packet_pipeline.cpp
        src/services/segment_execution_libav.cpp
        src/services/segment_execution.cpp
    )

    target_include_directories(velox_segment_execution_libav_tests
        PRIVATE include src include/velox)

    if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        target_compile_options(velox_segment_execution_libav_tests PRIVATE -O2)
    endif()
    velox_link_ffmpeg(velox_segment_execution_libav_tests)

    add_test(NAME segment_execution_libav_tests
             COMMAND velox_segment_execution_libav_tests)

    # Component-level unit tests for the named packet pipeline pieces
    # (Demuxer, PacketTrimmer, TimestampRewriter): pure AVPacket harnesses
    # for the rewrite pass plus a real-fixture Demuxer/demuxAndRewrite
    # exercise. The rewrite unit tests never touch disk; the fixture is
    # generated before the components themselves run.
    add_executable(velox_packet_components_tests
        tests/test_packet_components.cpp
        src/services/file_utils.cpp
        src/services/io_counters.cpp
        src/services/media_probe.cpp
        src/services/media_packet_pipeline.cpp
        src/services/segment_execution_libav.cpp
        src/services/segment_execution.cpp
    )

    target_include_directories(velox_packet_components_tests
        PRIVATE include src include/velox)

    if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        target_compile_options(velox_packet_components_tests PRIVATE -O2)
    endif()
    velox_link_ffmpeg(velox_packet_components_tests)

    add_test(NAME packet_components_tests
             COMMAND velox_packet_components_tests)

    # Render-level integration proof of the Zero-Spawn Copy Pipeline: a
    # copy_only RenderPlan is rendered through the public RenderEngine
    # entry point with a sentinel PATH whose ffmpeg/ffprobe fail hard.
    # Asserts zero temp bytes (no segment_N.mp4 / video_only.mp4 /
    # segments.txt), zero file copies (no cache -> tmp asset copies),
    # zero child media processes, concat_mode=packet_copy and a single
    # in-process packet mux phase in the sidecar telemetry.
    add_executable(velox_render_copy_only_zero_intermediates_tests
        tests/test_render_copy_only_zero_intermediates.cpp
        src/audio/audio_plan.cpp
        src/audio/audio_benchmark.cpp
        src/core/render_engine.cpp
        src/core/canonical_video_profile.cpp
        src/core/render_engine_helpers.cpp
        src/services/frame_pipeline.cpp
        src/render/frame_graph.cpp
        src/render/kernel_registry.cpp
        src/services/file_utils.cpp
        src/services/io_counters.cpp
        src/services/media_probe.cpp
        src/services/media_packet_pipeline.cpp
        src/services/segment_execution_libav.cpp
        src/services/segment_execution.cpp
        src/services/segment_scheduler.cpp
        src/services/media_utils.cpp
        src/services/ffmpeg_progress_parser.cpp
        src/telemetry/emitter.cpp
    )

    target_include_directories(velox_render_copy_only_zero_intermediates_tests
        PRIVATE include src include/velox)

    if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        target_compile_options(velox_render_copy_only_zero_intermediates_tests PRIVATE -O2)
    endif()
    velox_link_ffmpeg(velox_render_copy_only_zero_intermediates_tests)

    add_test(NAME render_copy_only_zero_intermediates_tests
             COMMAND velox_render_copy_only_zero_intermediates_tests)

    # Mixed renderer: PACKET_COPY + NATIVE_TRANSCODE assembled through the
    # single packet mux, with the produced-segment canonical-profile
    # fail-closed gate. Render-level: canonical + non-canonical fixtures;
    # the sentinel PATH proves the whole mixed path stays in-process.
    add_executable(velox_render_mixed_tests
        tests/test_render_mixed.cpp
        src/audio/audio_plan.cpp
        src/audio/audio_benchmark.cpp
        src/core/render_engine.cpp
        src/core/canonical_video_profile.cpp
        src/core/render_engine_helpers.cpp
        src/services/frame_pipeline.cpp
        src/render/frame_graph.cpp
        src/render/kernel_registry.cpp
        src/services/file_utils.cpp
        src/services/io_counters.cpp
        src/services/media_probe.cpp
        src/services/media_packet_pipeline.cpp
        src/services/segment_execution_libav.cpp
        src/services/segment_execution.cpp
        src/services/segment_scheduler.cpp
        src/services/media_utils.cpp
        src/services/ffmpeg_progress_parser.cpp
        src/telemetry/emitter.cpp
    )

    target_include_directories(velox_render_mixed_tests
        PRIVATE include src include/velox)

    if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        target_compile_options(velox_render_mixed_tests PRIVATE -O2)
    endif()
    velox_link_ffmpeg(velox_render_mixed_tests)

    add_test(NAME render_mixed_tests
             COMMAND velox_render_mixed_tests)

    # CompiledRenderPlanV2 receiver: parser unit tests (int64 frames/us,
    # float rejection, contiguity, frame-exactness, bindings) plus a
    # render-level zero-spawn proof of the V2 envelope through
    # RenderEngine::render() under a sentinel PATH.
    add_executable(velox_render_plan_v2_tests
        tests/test_render_plan_v2.cpp
        src/plan/render_plan_parser.cpp
        src/audio/audio_plan.cpp
        src/audio/audio_benchmark.cpp
        src/core/render_engine.cpp
        src/core/canonical_video_profile.cpp
        src/core/render_engine_helpers.cpp
        src/services/frame_pipeline.cpp
        src/render/frame_graph.cpp
        src/render/kernel_registry.cpp
        src/services/file_utils.cpp
        src/services/io_counters.cpp
        src/services/media_probe.cpp
        src/services/media_packet_pipeline.cpp
        src/services/segment_execution_libav.cpp
        src/services/segment_execution.cpp
        src/services/segment_scheduler.cpp
        src/services/media_utils.cpp
        src/services/ffmpeg_progress_parser.cpp
        src/telemetry/emitter.cpp
    )

    target_include_directories(velox_render_plan_v2_tests
        PRIVATE include src include/velox)

    if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        target_compile_options(velox_render_plan_v2_tests PRIVATE -O2)
    endif()
    velox_link_ffmpeg(velox_render_plan_v2_tests)

    add_test(NAME render_plan_v2_tests
             COMMAND velox_render_plan_v2_tests)

    # Phase-3 AVFrame producer-consumer pipeline: bounded pool, three
    # decode/render/encode threads and exactly one persistent encoder
    # AVCodecContext. Unit + end-to-end encode test (libav-only).
    add_executable(velox_frame_pipeline_tests
        tests/test_frame_pipeline.cpp
        src/services/frame_pipeline.cpp
        src/render/frame_graph.cpp
        src/render/kernel_registry.cpp
        src/services/media_packet_pipeline.cpp
        src/services/segment_execution_libav.cpp
        src/services/segment_execution.cpp
        src/services/media_probe.cpp
        src/services/file_utils.cpp
        src/services/io_counters.cpp
    )

    target_include_directories(velox_frame_pipeline_tests
        PRIVATE include src include/velox)

    if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        target_compile_options(velox_frame_pipeline_tests PRIVATE -O2)
    endif()
    velox_link_ffmpeg(velox_frame_pipeline_tests)

    add_test(NAME frame_pipeline_tests
             COMMAND velox_frame_pipeline_tests)
endif()

# Every target that can include PhaseRecorder must consume the freshly
# generated binding before compilation. The source-tree header remains the
# checked-in projection for non-CMake consumers; CMake builds use the binary
# tree projection above and therefore cannot silently compile stale metadata.
foreach(velox_target
        velox_video_engine
        velox_audio_timeline_tests
        velox_emitter_tests
        velox_looped_music_tests)
    add_dependencies(${velox_target} velox_telemetry_catalog_codegen)
endforeach()
if(TARGET velox_media_probe_tests)
    add_dependencies(velox_media_probe_tests velox_telemetry_catalog_codegen)
endif()
if(TARGET velox_media_packet_pipeline_tests)
    add_dependencies(velox_media_packet_pipeline_tests velox_telemetry_catalog_codegen)
endif()
if(TARGET velox_render_copy_only_zero_intermediates_tests)
    add_dependencies(velox_render_copy_only_zero_intermediates_tests velox_telemetry_catalog_codegen)
endif()
if(TARGET velox_render_mixed_tests)
    add_dependencies(velox_render_mixed_tests velox_telemetry_catalog_codegen)
endif()
if(TARGET velox_render_plan_v2_tests)
    add_dependencies(velox_render_plan_v2_tests velox_telemetry_catalog_codegen)
endif()
