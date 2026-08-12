cmake_minimum_required(VERSION 3.20)
project(velox_video_engine_cpp LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(PkgConfig REQUIRED)
pkg_check_modules(FFMPEG REQUIRED IMPORTED_TARGET
    libavformat
    libavcodec
    libavutil
)

function(velox_link_ffmpeg target)
    target_link_libraries(${target} PRIVATE PkgConfig::FFMPEG)
endfunction()

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

add_executable(velox_video_engine
    src/main.cpp
    src/cmd_full_video.cpp
    src/video_builder.cpp
    src/plan/render_plan_parser.cpp
    src/app/commands.cpp
    src/services/file_utils.cpp
    src/services/media_probe.cpp
    src/services/media_utils.cpp
    src/audio/audio_plan.cpp
    src/audio/audio_benchmark.cpp
    src/services/ffmpeg_progress_parser.cpp
    src/core/render_engine.cpp
    src/core/render_engine_helpers.cpp
    src/telemetry/phase_recorder.cpp
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

# Focused tests for the private render-engine command/parsing helpers.
add_executable(velox_render_engine_helpers_tests
    tests/test_render_engine_helpers.cpp
    src/core/render_engine_helpers.cpp
    src/services/ffmpeg_progress_parser.cpp
    src/services/file_utils.cpp
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
    src/core/render_engine_helpers.cpp
    src/services/file_utils.cpp
    src/services/media_probe.cpp
    src/services/media_utils.cpp
    src/services/ffmpeg_progress_parser.cpp
    src/telemetry/phase_recorder.cpp
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
add_executable(velox_phase_recorder_tests
    tests/test_phase_recorder.cpp
    src/audio/audio_plan.cpp
    src/audio/audio_benchmark.cpp
    src/telemetry/phase_recorder.cpp
    src/core/render_engine.cpp
    src/core/render_engine_helpers.cpp
    src/services/file_utils.cpp
    src/services/media_probe.cpp
    src/services/media_utils.cpp
    src/services/ffmpeg_progress_parser.cpp
)

target_include_directories(velox_phase_recorder_tests
    PRIVATE include src include/velox)

if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(velox_phase_recorder_tests PRIVATE -O2)
endif()
velox_link_ffmpeg(velox_phase_recorder_tests)

add_test(NAME phase_recorder_tests
         COMMAND velox_phase_recorder_tests)

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
    src/core/render_engine_helpers.cpp
    src/services/file_utils.cpp
    src/services/media_probe.cpp
    src/services/media_utils.cpp
    src/services/ffmpeg_progress_parser.cpp
    src/telemetry/phase_recorder.cpp
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
# child process.
add_executable(velox_media_probe_tests
    tests/test_media_probe.cpp
    src/services/file_utils.cpp
    src/services/media_probe.cpp
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
