// tests/test_emitter_json.cpp — sidecar JSON + counters emission tests (split block 2).
#include "test_emitter_common.h"

void testAppendJson() {
    SUBCASE("AppendJson emits a complete phases[] element");
    vt::PhaseRecorder r;
    int64_t tok = r.Begin(vt::kOriginEngine, vt::kScopeAttempt, "engine", "render", "render",
                          "completed", "render_v1");
    r.Complete(tok, 100, 200, 30, vt::kStatusOk);

    auto events = r.Snapshot();
    EXPECT_EQ_INT(static_cast<int>(events.size()), 1);
    std::ostringstream out;
    events[0].AppendJson(out);
    std::string json = out.str();
    EXPECT(std::strstr(json.c_str(), "\"origin\":\"engine\"") != nullptr, "origin in json");
    EXPECT(std::strstr(json.c_str(), "\"event_index\":0") != nullptr, "event_index in json");
    EXPECT(std::strstr(json.c_str(), "\"duration_ms\":") != nullptr, "duration_ms in json");
    EXPECT(std::strstr(json.c_str(), "\"bytes_in\":100") != nullptr, "bytes_in in json");
    EXPECT(std::strstr(json.c_str(), "\"bytes_out\":200") != nullptr, "bytes_out in json");
    EXPECT(std::strstr(json.c_str(), "\"frames\":30") != nullptr, "frames in json");
    EXPECT(std::strstr(json.c_str(), "\"started_at\":") != nullptr, "started_at in json");
}

void testAppendJsonMetadataValidationAndDetailedFields() {
    SUBCASE("AppendJson omits invalid metadata and keeps detailed fields");
    vt::PhaseRecorder r;
    int64_t token = r.Begin(vt::kOriginEngine, vt::kScopeSegment,
                            "engine.encode", "setup", "encode");
    r.SetMetadataJSON(token, "{\"codec\":]");
    r.SetDetailedMetrics(token, 3, "video", 1, 1.25, 8.5, 42.0, 2.0, 31, 30);
    r.Complete(token, 100, 200, 30, vt::kStatusOk);
    auto events = r.Snapshot();
    EXPECT_EQ_INT(static_cast<int>(events.size()), 1);
    std::ostringstream out;
    events[0].AppendJson(out);
    const std::string invalidJson = out.str();
    EXPECT(std::strstr(invalidJson.c_str(), "\"metadata\"") == nullptr,
           "invalid metadata omitted");
    EXPECT(std::strstr(invalidJson.c_str(), "\"segment_index\":3") != nullptr,
           "segment index emitted");
    EXPECT(std::strstr(invalidJson.c_str(), "\"track_kind\":\"video\"") != nullptr,
           "track kind emitted");
    EXPECT(std::strstr(invalidJson.c_str(), "\"frames_in\":31") != nullptr,
           "frames in emitted");

    vt::PhaseRecorder validRecorder;
    int64_t validToken = validRecorder.Begin(vt::kOriginEngine, vt::kScopeSegment,
                                             "engine.encode", "setup", "encode");
    validRecorder.SetMetadataJSON(validToken, "{\"codec\":\"h264\",\"stream\":{\"index\":0},\"tags\":[\"main\"]}");
    validRecorder.Complete(validToken, 0, 0, 0, vt::kStatusOk);
    std::ostringstream validOut;
    validRecorder.Snapshot()[0].AppendJson(validOut);
    EXPECT(std::strstr(validOut.str().c_str(), "\"metadata\":{\"codec\":\"h264\",\"stream\":{\"index\":0},\"tags\":[\"main\"]}") != nullptr,
           "valid nested metadata emitted");
}

void testAppendJsonEscapesStrings() {
    SUBCASE("AppendJson escapes detailed event strings so phases[] remains valid JSON");
    vt::PhaseRecorder r;
    int64_t token = r.Begin(vt::kOriginEngine, vt::kScopeSegment,
                            "engine.encode", "setup", "encode", "", "segment\"0");
    r.Abort(token, "E\\\"IO", "line 1\nline 2");
    auto events = r.Snapshot();
    EXPECT_EQ_INT(static_cast<int>(events.size()), 1);
    std::ostringstream out;
    events[0].AppendJson(out);
    const std::string json = out.str();
    EXPECT(std::strstr(json.c_str(), "segment\\\"0") != nullptr, "event_name quote escaped");
    EXPECT(std::strstr(json.c_str(), "E\\\\\\\"IO") != nullptr, "error_code quote escaped");
    EXPECT(std::strstr(json.c_str(), "line 1\\nline 2") != nullptr, "error newline escaped");

    vt::PhaseRecorder controls;
    int64_t controlToken = controls.Begin(vt::kOriginEngine, vt::kScopeAttempt,
                                           "engine", "render", "render", "", "controls");
    controls.Abort(controlToken, "E\\b\\f", "control\b\f\x01");
    std::ostringstream controlOut;
    controls.Snapshot()[0].AppendJson(controlOut);
    const std::string controlJson = controlOut.str();
    EXPECT(std::strstr(controlJson.c_str(), "E\\\\b\\\\f") != nullptr,
           "backspace and form-feed escapes emitted");
    EXPECT(std::strstr(controlJson.c_str(), "control\\b\\f\\u0001") != nullptr,
           "ASCII control characters emitted as JSON escapes");
}

void testCompleteSidecarSchema() {
    SUBCASE("sidecarJson keeps phase_ms, segments[] and phases[] in one payload");
    velox::core::RenderEngine engine;
    engine.metrics().addMs("encode", 12.5);
    velox::core::SegmentTiming segment;
    segment.index = 2;
    segment.source_type = "video";
    segment.total_ms = 20.0;
    segment.codec = "h264";
    segment.status = vt::kStatusOk;
    segment.started_offset_ms = 1.5;
    segment.finished_offset_ms = 21.5;
    engine.metrics().addSegment(segment);

    int64_t token = engine.recorder().Begin(
        vt::kOriginEngine, vt::kScopeSegment, "engine.encode", "frame_submit", "encode");
    engine.recorder().SetDetailedMetrics(token, 2, "video", 0, 1.5, 21.0, 18.0, 0.5, 120, 118);
    engine.recorder().Complete(token, 1000, 2000, 118, vt::kStatusOk);

    int64_t tempToken = engine.recorder().Begin(
        vt::kOriginEngine, vt::kScopeAttempt, "worker.temp", "create", "render");
    engine.recorder().Complete(tempToken, 0, 0, 0, vt::kStatusOk);
    int64_t assetToken = engine.recorder().Begin(
        vt::kOriginEngine, vt::kScopeSegment, "worker.asset", "transfer", "download");
    engine.recorder().Complete(assetToken, 1000, 1000, 0, vt::kStatusOk);
    int64_t concatToken = engine.recorder().Begin(
        vt::kOriginEngine, vt::kScopeAttempt, "engine", "concat", "finalize");
    engine.recorder().Complete(concatToken, 1000, 2000, 120, vt::kStatusOk);
    int64_t audioToken = engine.recorder().Begin(
        vt::kOriginEngine, vt::kScopeAudioTrack, "engine.audio", "mix", "composite");
    engine.recorder().Complete(audioToken, 300, 400, 0, vt::kStatusOk);
    int64_t muxToken = engine.recorder().Begin(
        vt::kOriginEngine, vt::kScopeAudioTrack, "engine.mux", "audio", "encode");
    engine.recorder().Complete(muxToken, 300, 400, 0, vt::kStatusOk);
    int64_t subtitleToken = engine.recorder().Begin(
        vt::kOriginValidation, vt::kScopeSubtitleTrack, "subtitle", "burn_in", "composite");
    engine.recorder().Complete(subtitleToken, 0, 0, 0, vt::kStatusOk);
    int64_t qualityToken = engine.recorder().Begin(
        vt::kOriginValidation, vt::kScopeArtifact, "quality", "ffprobe", "finalize");
    engine.recorder().Complete(qualityToken, 0, 0, 0, vt::kStatusOk);
    int64_t failedAssetToken = engine.recorder().Begin(
        vt::kOriginWorker, vt::kScopeArtifact, "worker.asset", "disk_write", "download");
    engine.recorder().Complete(failedAssetToken, 512, 0, 0, vt::kStatusFailed, "EIO", "disk full");
    engine.recorder().Emit(
        vt::kOriginWorker, vt::kScopeAttempt, "attempt", "retry", "retry", vt::kStatusOk);

    const std::string json = engine.sidecarJson("/tmp/render-output.mp4");
    EXPECT(json.size() > 0, "sidecar JSON is not empty");
    EXPECT(json.front() == '{' && json.back() == '}', "sidecar JSON is object-shaped");
    EXPECT(vt::IsValidJsonObject(json), "complete sidecar JSON parses as an object");
    EXPECT(std::strstr(json.c_str(), "\"phase_ms\":{\"encode\":12.5}") != nullptr,
           "phase_ms summary emitted");
    EXPECT(std::strstr(json.c_str(), "\"segments\":[") != nullptr,
           "segments array emitted");
    EXPECT(std::strstr(json.c_str(), "\"codec\":\"h264\"") != nullptr,
           "segment codec emitted");
    EXPECT(std::strstr(json.c_str(), "\"phases\":[") != nullptr,
           "phases array emitted");
    EXPECT(std::strstr(json.c_str(), "\"component\":\"engine.encode\"") != nullptr,
           "detailed phase emitted");
    EXPECT(std::strstr(json.c_str(), "\"component\":\"worker.temp\"") != nullptr,
           "temp phase emitted");
    EXPECT(std::strstr(json.c_str(), "\"component\":\"worker.asset\"") != nullptr,
           "asset phase emitted");
    EXPECT(std::strstr(json.c_str(), "\"action\":\"concat\"") != nullptr,
           "concat phase emitted");
    EXPECT(std::strstr(json.c_str(), "\"component\":\"engine.mux\"") != nullptr,
           "mux phase emitted");
    EXPECT(std::strstr(json.c_str(), "\"segment_index\":2") != nullptr,
           "detailed segment index emitted");
    EXPECT(std::strstr(json.c_str(), "\"cpu_ms\":18") != nullptr,
           "detailed cpu timing emitted");
    EXPECT(std::strstr(json.c_str(), "\"frames_in\":120") != nullptr,
           "detailed frame counters emitted");
    EXPECT(std::strstr(json.c_str(), "\"observability\":{") != nullptr,
           "observability rollup emitted");
    EXPECT(std::strstr(json.c_str(), "\"audio\":{\"events\":2") != nullptr,
           "audio rollup includes mix and mux events");
    EXPECT(std::strstr(json.c_str(), "\"subtitle\":{\"events\":1") != nullptr,
           "subtitle rollup emitted");
    EXPECT(std::strstr(json.c_str(), "\"quality\":{\"events\":1") != nullptr,
           "quality rollup emitted");
    EXPECT(std::strstr(json.c_str(), "\"retry\":{\"count\":1}") != nullptr,
           "retry rollup emitted");
    EXPECT(std::strstr(json.c_str(), "\"wasted_download_bytes\":512") != nullptr,
           "wasted download rollup emitted");
    EXPECT(std::strstr(json.c_str(), "\"io_counters\":{") != nullptr,
           "io_counters block emitted");
    EXPECT(std::strstr(json.c_str(), "\"input_open_count\":0") != nullptr,
           "io_counters defaults to zero");
}

void testIOCountersEmission() {
    SUBCASE("recorded I/O appears in the sidecar io_counters block");
    velox::services::resetIOCounters();
    velox::services::recordFileCopy(4096);
    velox::services::recordAssetCopy(4096);
    velox::services::recordInputOpen("/assets/clip_a.mp4");
    velox::services::recordInputOpen("/assets/clip_a.mp4"); // reopen
    velox::services::recordInputOpen("/assets/clip_b.mp4");

    velox::core::RenderEngine engine;
    const std::string json = engine.sidecarJson("/tmp/render-output.mp4");
    EXPECT(std::strstr(json.c_str(), "\"file_copy_count\":1") != nullptr,
           "one copy recorded");
    EXPECT(std::strstr(json.c_str(), "\"file_copy_bytes\":4096") != nullptr,
           "copy bytes recorded");
    EXPECT(std::strstr(json.c_str(), "\"asset_bytes_copied\":4096") != nullptr,
           "asset materialization bytes recorded");
    EXPECT(std::strstr(json.c_str(), "\"input_open_count\":3") != nullptr,
           "three input opens recorded");
    EXPECT(std::strstr(json.c_str(), "\"input_reopen_count\":1") != nullptr,
           "second open of the same path recorded as reopen");
    velox::services::resetIOCounters();
}
void testProcessCounters() {
    SUBCASE("recordExternalSpawn classifies by leading token");
    velox::services::resetIOCounters();
    velox::services::recordExternalSpawn("ffmpeg -y -i in.mp4 out.mp4");
    velox::services::recordExternalSpawn("ffprobe -v error -show_format in.mp4");
    velox::services::recordExternalSpawn("curl -s https://example.com/a.bin");
    velox::services::recordExternalSpawn("cp /a /b");
    velox::services::recordExternalSpawn("sh -c 'echo hi'");
    const auto& io = velox::services::ioCounters();
    EXPECT_EQ_INT(static_cast<int>(io.external_spawn_count.load()), 5);
    EXPECT_EQ_INT(static_cast<int>(io.ffmpeg_spawn_count.load()), 1);
    EXPECT_EQ_INT(static_cast<int>(io.ffprobe_spawn_count.load()), 1);
    EXPECT_EQ_INT(static_cast<int>(io.curl_spawn_count.load()), 1);
    EXPECT_EQ_INT(static_cast<int>(io.shell_spawn_count.load()), 2);

    SUBCASE("sidecar emits the engine-declared process_counters block");
    velox::core::RenderEngine engine;
    const std::string json = engine.sidecarJson("/tmp/render-output.mp4");
    EXPECT(std::strstr(json.c_str(), "\"process_counters\":{") != nullptr,
           "process_counters block emitted");
    EXPECT(std::strstr(json.c_str(), "\"external_spawn_count\":5") != nullptr,
           "external spawn total emitted");
    EXPECT(std::strstr(json.c_str(), "\"ffmpeg_spawn_count\":1") != nullptr,
           "ffmpeg spawn count emitted");
    EXPECT(std::strstr(json.c_str(), "\"ffprobe_spawn_count\":1") != nullptr,
           "ffprobe spawn count emitted");
    EXPECT(std::strstr(json.c_str(), "\"shell_spawn_count\":2") != nullptr,
           "shell spawn count emitted");
    EXPECT(std::strstr(json.c_str(), "\"curl_spawn_count\":1") != nullptr,
           "curl spawn count emitted");
    // getrusage facts are keyed and numeric (values are process-dependent
    // and can legitimately be 0 in a tiny test binary, so only presence
    // and shape are asserted).
    EXPECT(std::strstr(json.c_str(), "\"cpu_user_ms\":") != nullptr,
           "engine cpu_user_ms key emitted");
    EXPECT(std::strstr(json.c_str(), "\"cpu_system_ms\":") != nullptr,
           "engine cpu_system_ms key emitted");
    EXPECT(std::strstr(json.c_str(), "\"voluntary_context_switches\":") != nullptr,
           "voluntary context switches key emitted");
    EXPECT(std::strstr(json.c_str(), "\"involuntary_context_switches\":") != nullptr,
           "involuntary context switches key emitted");
    EXPECT(std::strstr(json.c_str(), "\"minor_page_faults\":") != nullptr,
           "minor page faults key emitted");
    EXPECT(std::strstr(json.c_str(), "\"major_page_faults\":") != nullptr,
           "major page faults key emitted");
    // The zero-spawn invariant: after reset with no spawns the block is
    // all-zero, which is exactly what a copy-only render must report.
    velox::services::resetIOCounters();
    const std::string zeroJson = engine.sidecarJson("/tmp/render-output.mp4");
    EXPECT(std::strstr(zeroJson.c_str(), "\"external_spawn_count\":0") != nullptr,
           "zero-spawn invariant: external_spawn_count resets to 0");
    EXPECT(std::strstr(zeroJson.c_str(), "\"ffmpeg_spawn_count\":0") != nullptr,
           "zero-spawn invariant: ffmpeg_spawn_count resets to 0");
    EXPECT(std::strstr(zeroJson.c_str(), "\"ffprobe_spawn_count\":0") != nullptr,
           "zero-spawn invariant: ffprobe_spawn_count resets to 0");
    velox::services::resetIOCounters();
}
