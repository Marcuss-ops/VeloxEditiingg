// tests/test_emitter_core.cpp — emitter phase lifecycle tests (split block 1).
#include "test_emitter_common.h"

void testCatalogBinding() {
    SUBCASE("generated catalog accepts known events and rejects unknown events");
    EXPECT(vt::IsCanonicalEvent("engine.encode", "setup"),
           "known catalog event must be discoverable");
    EXPECT(!vt::IsCanonicalEvent("engine.encode", "invented"),
           "unknown catalog event must be detectable");
    const auto* descriptor = vt::catalog::FindEvent("engine.mux", "packet_write");
    EXPECT(descriptor != nullptr, "mux packet event descriptor must exist");
    EXPECT_EQ_STR(std::string(descriptor->kind), "counter");
    EXPECT_EQ_STR(std::string(descriptor->unit), "count");
    EXPECT_EQ_STR(std::string(descriptor->owner), "muxer");
}
void testBeginComplete() {
    SUBCASE("Begin/Complete measures monotonic duration + UTC stamps");
    vt::PhaseRecorder r;
    int64_t tok = r.Begin(vt::kOriginEngine, vt::kScopeAttempt, "engine", "render", "render");
    EXPECT(tok >= 0, "token must be non-negative");
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
    r.Complete(tok, 0, 0, 0, vt::kStatusOk);

    auto events = r.Snapshot();
    EXPECT_EQ_INT(static_cast<int>(events.size()), 1);
    const auto& e = events[0];
    EXPECT_EQ_STR(e.origin, "engine");
    EXPECT_EQ_STR(e.scope, "attempt");
    EXPECT_EQ_STR(e.component, "engine");
    EXPECT_EQ_STR(e.status, "ok");
    EXPECT_EQ_STR(e.event_type, "completed");
    EXPECT_EQ_INT(e.event_index, 0);
    EXPECT(e.duration_ms >= 1, "duration_ms must be >= 1 after sleep");
    EXPECT(!e.started_at.empty(), "started_at must be set");
    EXPECT(!e.completed_at.empty(), "completed_at must be set");
    EXPECT_EQ_INT(static_cast<int>(e.started_at.back()), 'Z'); // UTC suffix
}

void testPerOriginIndexes() {
    SUBCASE("event_index increments per origin");
    vt::PhaseRecorder r;
    r.Emit(vt::kOriginWorker, vt::kScopeAttempt, "runner", "execute", "", "ok");
    r.Emit(vt::kOriginEngine, vt::kScopeAttempt, "engine", "render", "", "ok");
    r.Emit(vt::kOriginWorker, vt::kScopeAttempt, "runner", "report", "", "ok");

    auto events = r.Snapshot();
    EXPECT_EQ_INT(static_cast<int>(events.size()), 3);
    EXPECT_EQ_INT(events[0].event_index, 0);
    EXPECT_EQ_INT(events[1].event_index, 0);
    EXPECT_EQ_INT(events[2].event_index, 1);
}

void testAbortAndNormalize() {
    SUBCASE("Abort yields failed event; non-canonical values coerced");
    vt::PhaseRecorder r;
    int64_t tok = r.Begin("bogus", "nope", "engine", "render", "render");
    r.Abort(tok, "EIO", "disk full");
    auto events = r.Snapshot();
    EXPECT_EQ_INT(static_cast<int>(events.size()), 1);
    EXPECT_EQ_STR(events[0].origin, "engine"); // coerced
    EXPECT_EQ_STR(events[0].scope, "attempt"); // coerced
    EXPECT_EQ_STR(events[0].status, "failed");
    EXPECT_EQ_STR(events[0].event_type, "failed");
    EXPECT_EQ_STR(events[0].error_code, "EIO");
    EXPECT_EQ_STR(events[0].error_message, "disk full");
}

void testUnknownTokenNoop() {
    SUBCASE("Complete/Abort on unknown token is a no-op");
    vt::PhaseRecorder r;
    r.Complete(999, 0, 0, 0, "ok");
    r.Abort(998, "x", "y");
    EXPECT_EQ_INT(static_cast<int>(r.Snapshot().size()), 0);
}

void testScopedPhaseRaii() {
    SUBCASE("ScopedPhase completes on scope exit, aborts on failure");
    vt::PhaseRecorder r;
    {
        vt::ScopedPhase okPhase(r, vt::kOriginFFmpeg, vt::kScopeSegment,
                                "ffmpeg", "encode_segment", "encode", "",
                                "encode_segment_0");
        // destructor completes as ok
    }
    {
        vt::ScopedPhase failPhase(r, vt::kOriginFFmpeg, vt::kScopeSegment,
                                  "ffmpeg", "encode_segment", "encode", "",
                                  "encode_segment_1");
        failPhase.Abort("encode_failed", "boom");
    }
    auto events = r.Snapshot();
    EXPECT_EQ_INT(static_cast<int>(events.size()), 2);
    EXPECT_EQ_STR(events[0].status, "ok");
    EXPECT_EQ_STR(events[1].status, "failed");
    EXPECT_EQ_STR(events[1].error_code, "encode_failed");
    // Per-origin indexes contiguous across both events.
    EXPECT_EQ_INT(events[0].event_index, 0);
    EXPECT_EQ_INT(events[1].event_index, 1);
}

void testScopedPhaseMove() {
    SUBCASE("ScopedPhase move transfers ownership; moved-from is inert");
    vt::PhaseRecorder r;
    {
        vt::ScopedPhase a(r, vt::kOriginEngine, vt::kScopeAttempt, "engine", "render", "render");
        vt::ScopedPhase b = std::move(a); // a is now inert
    }
    auto events = r.Snapshot();
    EXPECT_EQ_INT(static_cast<int>(events.size()), 1);
    EXPECT_EQ_STR(events[0].status, "ok");
}

void testFinalAudioModeResolver() {
    SUBCASE("final audio COPY requires a verified AAC final mix");
    velox::media::FinalAudioMetadata verified{
        true, "aac", 24000, 1, "mono", 698.923, 0.0, true, true,
        "mov,mp4,m4a", true, true};
    auto copy = velox::media::resolveFinalAudioMode(verified, true, 698.92, 1.0, 0.0);
    EXPECT_EQ_STR(velox::media::finalAudioModeName(copy.mode), "COPY");
    EXPECT_EQ_STR(copy.reason, "verified_final_mix");

    auto filtered = velox::media::resolveFinalAudioMode(verified, true, 698.92, 0.8, 0.0);
    EXPECT_EQ_STR(velox::media::finalAudioModeName(filtered.mode), "ENCODE");
    EXPECT_EQ_STR(filtered.reason, "final_audio_filter_required");

    auto negativeOffset = velox::media::resolveFinalAudioMode(verified, true, 698.92, 1.0, -0.1);
    EXPECT_EQ_STR(velox::media::finalAudioModeName(negativeOffset.mode), "ENCODE");
    EXPECT_EQ_STR(negativeOffset.reason, "final_audio_filter_required");

    auto unverified = velox::media::resolveFinalAudioMode(
        velox::media::FinalAudioMetadata{}, true, 698.92, 1.0, 0.0);
    EXPECT_EQ_STR(velox::media::finalAudioModeName(unverified.mode), "ENCODE");
    EXPECT_EQ_STR(unverified.reason, "audio_metadata_unverified");

    verified.duration_verified = false;
    auto missingDuration = velox::media::resolveFinalAudioMode(verified, true, 698.92, 1.0, 0.0);
    EXPECT_EQ_STR(velox::media::finalAudioModeName(missingDuration.mode), "ENCODE");
    EXPECT_EQ_STR(missingDuration.reason, "audio_metadata_unverified");

    verified.duration_verified = true;
    verified.codec = "mp3";
    auto wrongCodec = velox::media::resolveFinalAudioMode(verified, true, 698.92, 1.0, 0.0);
    EXPECT_EQ_STR(velox::media::finalAudioModeName(wrongCodec.mode), "ENCODE");
    EXPECT_EQ_STR(wrongCodec.reason, "audio_codec_not_aac");

    verified.codec = "aac";
    auto zeroExpectedDuration = velox::media::resolveFinalAudioMode(
        verified, true, 0.0, 1.0, 0.0);
    EXPECT_EQ_STR(velox::media::finalAudioModeName(zeroExpectedDuration.mode), "ENCODE");
    EXPECT_EQ_STR(zeroExpectedDuration.reason, "audio_duration_mismatch");

    verified.duration_seconds = 10.25;
    auto exactDurationTolerance = velox::media::resolveFinalAudioMode(
        verified, true, 10.0, 1.0, 0.0);
    EXPECT_EQ_STR(velox::media::finalAudioModeName(exactDurationTolerance.mode), "COPY");
    EXPECT_EQ_STR(exactDurationTolerance.reason, "verified_final_mix");

    verified.duration_seconds = 10.251;
    auto beyondDurationTolerance = velox::media::resolveFinalAudioMode(
        verified, true, 10.0, 1.0, 0.0);
    EXPECT_EQ_STR(velox::media::finalAudioModeName(beyondDurationTolerance.mode), "ENCODE");
    EXPECT_EQ_STR(beyondDurationTolerance.reason, "audio_duration_mismatch");

    verified.duration_seconds = 10.0;
    verified.start_time_seconds = 0.05;
    auto exactStartTolerance = velox::media::resolveFinalAudioMode(
        verified, true, 10.0, 1.0, 0.0);
    EXPECT_EQ_STR(velox::media::finalAudioModeName(exactStartTolerance.mode), "COPY");
    EXPECT_EQ_STR(exactStartTolerance.reason, "verified_final_mix");

    verified.start_time_seconds = 0.051;
    auto beyondStartTolerance = velox::media::resolveFinalAudioMode(
        verified, true, 10.0, 1.0, 0.0);
    EXPECT_EQ_STR(velox::media::finalAudioModeName(beyondStartTolerance.mode), "ENCODE");
    EXPECT_EQ_STR(beyondStartTolerance.reason, "audio_start_time_mismatch");

    verified.start_time_seconds = 0.0;
    auto nearNeutralVolume = velox::media::resolveFinalAudioMode(
        verified, true, 10.0, 0.999, 0.0);
    EXPECT_EQ_STR(velox::media::finalAudioModeName(nearNeutralVolume.mode), "ENCODE");
    EXPECT_EQ_STR(nearNeutralVolume.reason, "final_audio_filter_required");

    auto positiveOffset = velox::media::resolveFinalAudioMode(
        verified, true, 10.0, 1.0, 0.001);
    EXPECT_EQ_STR(velox::media::finalAudioModeName(positiveOffset.mode), "ENCODE");
    EXPECT_EQ_STR(positiveOffset.reason, "final_audio_filter_required");
}

void testFinalAudioModePacketResolver() {
    SUBCASE("packet-mode FINAL_AUDIO_COPY requires a verified MP4-AAC final mix");
    velox::media::FinalAudioMetadata verified{
        true, "aac", 48000, 2, "stereo", 1.6, 0.0, true, true,
        "mov,mp4,m4a", true, true};
    auto copy = velox::media::resolveFinalAudioModePacket(verified, true, 1.6);
    EXPECT_EQ_STR(velox::media::finalAudioModeName(copy.mode), "COPY");
    EXPECT_EQ_STR(copy.reason, "verified_final_mix");

    // The packet mux trims trailing audio to the video timeline, so a
    // longer prepared track is still COPY (coverage, not exact equality).
    auto longer = verified;
    longer.duration_seconds = 2.0;
    auto covering = velox::media::resolveFinalAudioModePacket(longer, true, 1.6);
    EXPECT_EQ_STR(velox::media::finalAudioModeName(covering.mode), "COPY");

    auto notFinal = velox::media::resolveFinalAudioModePacket(verified, false, 1.6);
    EXPECT_EQ_STR(velox::media::finalAudioModeName(notFinal.mode), "ENCODE");
    EXPECT_EQ_STR(notFinal.reason, "not_final_mix");

    auto unverified = velox::media::resolveFinalAudioModePacket(
        velox::media::FinalAudioMetadata{}, true, 1.6);
    EXPECT_EQ_STR(velox::media::finalAudioModeName(unverified.mode), "ENCODE");
    EXPECT_EQ_STR(unverified.reason, "audio_metadata_unverified");

    verified.container_verified = false;
    auto rawContainer = velox::media::resolveFinalAudioModePacket(verified, true, 1.6);
    EXPECT_EQ_STR(velox::media::finalAudioModeName(rawContainer.mode), "ENCODE");
    EXPECT_EQ_STR(rawContainer.reason, "audio_transport_unverified");

    verified.container_verified = true;
    verified.codec = "mp3";
    auto wrongCodec = velox::media::resolveFinalAudioModePacket(verified, true, 1.6);
    EXPECT_EQ_STR(velox::media::finalAudioModeName(wrongCodec.mode), "ENCODE");
    EXPECT_EQ_STR(wrongCodec.reason, "audio_codec_not_aac");

    verified.codec = "aac";
    verified.start_time_seconds = 4.0;
    auto shiftedPacket = velox::media::resolveFinalAudioModePacket(verified, true, 1.6);
    EXPECT_EQ_STR(velox::media::finalAudioModeName(shiftedPacket.mode), "COPY");
    EXPECT_EQ_STR(shiftedPacket.reason, "verified_final_mix");

    verified.start_time_seconds = 0.0;
    verified.duration_seconds = 1.2;
    auto tooShort = velox::media::resolveFinalAudioModePacket(verified, true, 1.6);
    EXPECT_EQ_STR(velox::media::finalAudioModeName(tooShort.mode), "ENCODE");
    EXPECT_EQ_STR(tooShort.reason, "audio_duration_mismatch");

    verified.duration_seconds = 1.55;
    auto packetTolerance = velox::media::resolveFinalAudioModePacket(verified, true, 1.6);
    EXPECT_EQ_STR(velox::media::finalAudioModeName(packetTolerance.mode), "COPY");
    EXPECT_EQ_STR(packetTolerance.reason, "verified_final_mix");

    auto packetZeroExpected = velox::media::resolveFinalAudioModePacket(
        verified, true, 0.0);
    EXPECT_EQ_STR(velox::media::finalAudioModeName(packetZeroExpected.mode), "ENCODE");
    EXPECT_EQ_STR(packetZeroExpected.reason, "audio_duration_mismatch");

    verified.duration_seconds = 1.55;
    auto packetExactTolerance = velox::media::resolveFinalAudioModePacket(
        verified, true, 1.6);
    EXPECT_EQ_STR(velox::media::finalAudioModeName(packetExactTolerance.mode), "COPY");
    EXPECT_EQ_STR(packetExactTolerance.reason, "verified_final_mix");

    verified.duration_seconds = 1.549;
    auto packetBelowTolerance = velox::media::resolveFinalAudioModePacket(
        verified, true, 1.6);
    EXPECT_EQ_STR(velox::media::finalAudioModeName(packetBelowTolerance.mode), "ENCODE");
    EXPECT_EQ_STR(packetBelowTolerance.reason, "audio_duration_mismatch");

    // Packet mode intentionally ignores source start_time: rewriting shifts
    // timestamps in the muxer, so a verified track with a non-zero start is
    // still COPY when it covers the requested timeline.
    verified.duration_seconds = 1.6;
    verified.start_time_seconds = -12.0;
    auto packetShiftedSource = velox::media::resolveFinalAudioModePacket(
        verified, true, 1.6);
    EXPECT_EQ_STR(velox::media::finalAudioModeName(packetShiftedSource.mode), "COPY");
    EXPECT_EQ_STR(packetShiftedSource.reason, "verified_final_mix");
}
