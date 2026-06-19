cmake_minimum_required(VERSION 3.20)
project(velox_video_engine_cpp LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

# Release artifacts must run on every compatible host of the target
# architecture. -march=native and -mtune=native bake the builder CPU feature
# set into the binary and can cause illegal-instruction failures elsewhere.
if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    add_compile_options(-O3)
    add_link_options(-flto)
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION_RELEASE ON)
endif()

add_executable(velox_video_engine
    src/main.cpp
    src/cmd_full_video.cpp
    src/video_builder.cpp
)

target_include_directories(velox_video_engine PRIVATE include src)
