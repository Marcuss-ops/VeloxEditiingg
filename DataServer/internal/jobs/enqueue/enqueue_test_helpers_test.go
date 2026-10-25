package enqueue

import (
	"context"
	"strings"
	"testing"
	"velox-server/internal/deliverystore"

	"velox-server/internal/deliveries"
	"velox-server/internal/store"
	"velox-shared/contract"
)

// testCompiledV2Payload removes retired scene-era inputs and binds a valid
// producer-owned plan so enqueue lifecycle tests exercise their intended
// persistence/failure path rather than the old Master compiler.
func testCompiledV2Payload(payload map[string]interface{}) map[string]interface{} {
	planJSON, planSHA := testCompiledV2Plan()
	for _, key := range []string{
		"scenes", "scenes_json", "render_manifest", "manifest_ref", "layers", "overlays",
		"visual_replacements", "items", "clips", "images", "clip_segments", "intro_clip_paths", "stock_clip_paths",
		"voiceover_paths", "voiceover_path", "audio_path",
	} {
		delete(payload, key)
	}
	payload[contract.PayloadKeyCompiledRenderPlanJSON] = planJSON
	payload[contract.PayloadKeyCompiledRenderPlanSHA] = planSHA
	return payload
}

func testCompiledV2Plan() (string, string) {
	const durationUS int64 = 1_000_000
	const revision int64 = 1
	timelineSHA := strings.Repeat("c", 64)
	videoSHA := strings.Repeat("b", 64)
	audioSHA := strings.Repeat("a", 64)
	profile := contract.CanonicalVideoProfileFMP4StreamV1Default
	plan := &contract.CompiledRenderPlanV2{
		PlanVersion: contract.CompiledPlanVersionV2, TimelineRevision: revision,
		TimelineSHA256: timelineSHA, DurationUS: durationUS,
		Output: contract.OutputContractV2{
			Container: profile.Container, VideoCodec: profile.Codec, Width: profile.Width, Height: profile.Height,
			FPSNum: profile.FPSNum, FPSDen: profile.FPSDen, PixelFormat: profile.PixelFormat,
			ProfileID: profile.ProfileID, CodecProfile: profile.CodecProfile, CodecLevel: profile.CodecLevel,
			GOPSize: profile.GOPSize, BFrames: profile.BFrames, ClosedGOP: profile.ClosedGOP,
			TimeBaseNum: profile.TimeBaseNum, TimeBaseDen: profile.TimeBaseDen,
		},
		FinalAudio: contract.FinalAudioV2{
			Mode: contract.AudioModeFinalAudioCopy, AssetID: "audio-final", SHA256: audioSHA, SizeBytes: 200,
			Codec: "aac", SampleRateHz: 48_000, Channels: 2, DurationUS: durationUS,
			TimelineRevision: revision, TimelineSHA256: timelineSHA,
		},
		VideoTracks: []contract.VideoTrackV2{{TrackID: "main", Segments: []contract.VideoSegmentV2{{
			SegmentID: "segment-0", AssetID: "video-source", SHA256: videoSHA, TimelineStartFrame: 0,
			FrameCount: 24, SourceInUS: 0, SourceDurationUS: durationUS,
		}}}},
		Assets: []contract.AssetRefV2{
			{AssetID: "audio-final", SHA256: audioSHA, SizeBytes: 200, Kind: "final_audio", MIME: "audio/mp4", DurationUS: durationUS},
			{AssetID: "video-source", SHA256: videoSHA, SizeBytes: 100, Kind: "video", DurationUS: durationUS, Width: profile.Width, Height: profile.Height},
		},
	}
	if err := contract.ValidateCompiledRenderPlanV2(plan); err != nil {
		panic(err)
	}
	canonical, err := plan.CanonicalJSON()
	if err != nil {
		panic(err)
	}
	sha, err := plan.PlanSHA256()
	if err != nil {
		panic(err)
	}
	return string(canonical), sha
}

// newTestEnqueuer creates an Enqueuer backed by an in-memory SQLite store
// for integration-level testing of the atomic creation path. The
// PlanResolver is the happy-path mock so non-precondition tests do not
// have to configure it. Precondition tests use a custom mockPlanResolver
// directly via NewEnqueuer.
//
// Seeds "drive-main" + "destination-main" into delivery_destinations so
// store.validateDeliveryDestinationTx (called inside AtomicJobTaskCreator
// during the per-destination JWT check) has a stable set to query
// against. The seed is also the reason helper-using tests like
// TestEnqueueCreatesJobAndTaskAtomically / TestEnqueueDefaultsPreserved /
// TestEnqueueWithForwardingKey that pass `delivery_plan: [{destination_id:
// "drive-main", ...}]` stop failing with "destination_id \"drive-main\"
// does not exist" — the seeded row makes validateDeliveryDestinationTx
// return nil for those ids.
func newTestEnqueuer(t *testing.T) *Enqueuer {
	t.Helper()
	db, err := store.NewSQLiteStore(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatalf("sqlite store: %v", err)
	}
	seedDestinations(t, db, map[string]bool{
		"drive-main": true,
	})
	jobRepo := store.NewSQLiteJobRepository(db)
	atomic := store.NewAtomicJobTaskCreator(db)
	return NewEnqueuer(atomic, jobRepo, nil, newTestPlanResolver())
}

// seedDestinations seeds the delivery_destinations table with the
// given (id, enabled) pairs so the per-destination validator
// (store.validateDeliveryDestinationTx, called inside the atomic
// creator's parse-time plan check) has a stable set to query against.
//
// Mirrors the helper of the same name in
// DataServer/internal/store/atomic_job_task_test.go. Duplicated here
// (not exported) because:
//   - importing store's test internals would couple this file to
//     non-production symbols,
//   - tests in this package need different default seeds than tests
//     in store (e.g. "drive-main" vs the multi-dest IDs the store
//     package tests exercise).
func seedDestinations(t *testing.T, db *store.SQLiteStore, pairs map[string]bool) {
	t.Helper()
	for id, enabled := range pairs {
		if err := db.Delivery().InsertDeliveryDestination(&deliverystore.DeliveryDestination{
			DestinationID: id,
			Provider:      "drive",
			Name:          id,
			Enabled:       enabled,
		}); err != nil {
			t.Fatalf("seed destination %q: %v", id, err)
		}
	}
}

// =============================================================================
// Tests for the enqueue-time plan resolver. The mockPlanResolver is a
// hand-rolled PlanResolver that returns a configured plan/error without
// any DB interaction so the precondition can be unit-tested in isolation
// from the deliveries stack.
// =============================================================================

// mockPlanResolver implements PlanResolver for tests. It returns the
// configured plan or error verbatim, with a defensive copy of the
// destinations slice so tests cannot accidentally mutate shared state.
type mockPlanResolver struct {
	plan  *ResolvedPlan
	err   error
	calls int
}

func (m *mockPlanResolver) ResolvePlan(_ context.Context, _, _ string) (*ResolvedPlan, error) {
	m.calls++
	if m.err != nil {
		return nil, m.err
	}
	if m.plan == nil {
		return nil, nil
	}
	out := &ResolvedPlan{JobID: m.plan.JobID}
	out.Destinations = append(out.Destinations, m.plan.Destinations...)
	return out, nil
}

// newTestPlanResolver returns a happy-path PlanResolver (single
// destination, retry_budget=5) used by the existing non-precondition
// tests. Precondition tests construct their own mockPlanResolver to
// exercise the rejection paths.
func newTestPlanResolver() PlanResolver {
	return &mockPlanResolver{
		plan: &ResolvedPlan{
			JobID: "test-job",
			Destinations: []PlanDestination{
				{DestinationID: "destination-main", Priority: 0, RetryBudget: 5},
			},
		},
	}
}

// =============================================================================
// Integration test that uses the REAL DB-backed
// *deliveries.SQLiteDeliveryPlanResolver (not a hand-rolled mock) via
// a local planResolverAdapter. This proves the precondition reads
// from the real job_delivery_plans table, propagates max(retry_budget)
// to job.MaxRetries, and surfaces the production ErrNoExplicitPlan
// path when no plan rows exist. The adapter mirrors the production
// one in cmd/server/bootstrap_modules.go to avoid an import cycle
// between enqueue and deliveries.
// =============================================================================

// planResolverAdapter bridges *deliveries.SQLiteDeliveryPlanResolver to
// enqueue.PlanResolver for the integration test. It is the test-side
// twin of deliveryPlanResolverAdapter in cmd/server/bootstrap_modules.go;
// the duplication is intentional (composition-root adapter for prod, local
// adapter for the test) to keep the enqueue package decoupled from
// deliveries.
type planResolverAdapter struct {
	inner *deliveries.SQLiteDeliveryPlanResolver
}

func (a *planResolverAdapter) ResolvePlan(ctx context.Context, jobID, artifactID string) (*ResolvedPlan, error) {
	if a == nil || a.inner == nil {
		return nil, nil
	}
	plan, err := a.inner.ResolvePlan(ctx, jobID, artifactID)
	if err != nil {
		return nil, err
	}
	if plan == nil {
		return nil, nil
	}
	out := &ResolvedPlan{JobID: plan.JobID}
	for _, d := range plan.Destinations {
		out.Destinations = append(out.Destinations, PlanDestination{
			DestinationID: d.DestinationID,
			Priority:      d.Priority,
			RetryBudget:   d.RetryBudget,
		})
	}
	return out, nil
}
