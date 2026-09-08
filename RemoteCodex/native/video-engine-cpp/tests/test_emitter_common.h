// tests/test_emitter_common.h — shared harness for the emitter test split.
// Extracted from the original monolithic test_emitter.cpp so each test
// translation unit stays under 500 LOC.
#pragma once

// Run via the binary velox_emitter_tests.

#include "velox/core/render_engine.hpp"
#include "velox/services/file_utils.hpp"
#include "velox/services/io_counters.hpp"
#include "velox/services/media_utils.hpp"
#include "velox/telemetry/emitter.hpp"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace vt = velox::telemetry;



inline int g_pass = 0;
inline int g_fail = 0;

#define EXPECT(cond, msg)                                                  \
    do {                                                                   \
        if (!(cond)) {                                                     \
            std::cerr << "FAIL " << __FILE__ << ":" << __LINE__            \
                      << ": " << msg << " (expected: " << #cond << ")\n";  \
            ++g_fail;                                                      \
            return;                                                        \
        }                                                                  \
    } while (0)
#define EXPECT_EQ_INT(actual, expected)                                    \
    do {                                                                   \
        auto _a = (actual);                                                \
        auto _e = (expected);                                              \
        if (_a != _e) {                                                    \
            std::cerr << "FAIL " << __FILE__ << ":" << __LINE__            \
                      << ": int mismatch (got=" << _a                       \
                      << " want=" << _e << ")\n";                          \
            ++g_fail;                                                      \
            return;                                                        \
        }                                                                  \
    } while (0)

#define EXPECT_EQ_STR(actual, expected)                                    \
    do {                                                                   \
        auto _a = (actual);                                                \
        auto _e = (expected);                                              \
        if (_a != _e) {                                                    \
            std::cerr << "FAIL " << __FILE__ << ":" << __LINE__            \
                      << ": str mismatch (got=\"" << _a                     \
                      << "\" want=\"" << _e << "\")\n";                    \
            ++g_fail;                                                      \
            return;                                                        \
        }                                                                  \
    } while (0)

#define SUBCASE(name)                                                      \
    do {                                                                   \
        std::cerr << "── sub-case: " << name << " ──\n";                   \
        ++g_pass;                                                          \
    } while (0)
