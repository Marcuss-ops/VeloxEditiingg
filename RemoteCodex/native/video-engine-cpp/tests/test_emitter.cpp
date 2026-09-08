// tests/test_emitter.cpp — block-1 telemetry emitter tests (entry point).
//
// Test bodies live in test_emitter_core.cpp / test_emitter_json.cpp /
// test_emitter_integration.cpp; shared harness in test_emitter_common.h.

// Run via the binary velox_emitter_tests.

#include "test_emitter_common.h"

namespace vt = velox::telemetry;


void testCatalogBinding();
void testBeginComplete();
void testPerOriginIndexes();
void testAbortAndNormalize();
void testUnknownTokenNoop();
void testScopedPhaseRaii();
void testScopedPhaseMove();
void testFinalAudioModeResolver();
void testFinalAudioModePacketResolver();
void testAppendJson();
void testAppendJsonMetadataValidationAndDetailedFields();
void testAppendJsonEscapesStrings();
void testCompleteSidecarSchema();
void testIOCountersEmission();
void testProcessCounters();
void testRenderEngineIntegration();
void testCopyOnlyTelemetryDoesNotClaimVideoEncoding();
void testReset();
void testCanonicalEnums();
void testFailClosedGate();

int main() {
    std::cerr << "running emitter tests\n";

    testCatalogBinding();
    testBeginComplete();
    testPerOriginIndexes();
    testAbortAndNormalize();
    testUnknownTokenNoop();
    testScopedPhaseRaii();
    testScopedPhaseMove();
    testFinalAudioModeResolver();
    testFinalAudioModePacketResolver();
    testAppendJson();
    testAppendJsonMetadataValidationAndDetailedFields();
    testAppendJsonEscapesStrings();
    testCompleteSidecarSchema();
    testIOCountersEmission();
    testProcessCounters();
    testRenderEngineIntegration();
    testCopyOnlyTelemetryDoesNotClaimVideoEncoding();
    testReset();
    testCanonicalEnums();
    testFailClosedGate();

    std::cerr << "\nsummary: pass=" << g_pass << " fail=" << g_fail << "\n";
    return g_fail == 0 ? 0 : 1;
}
