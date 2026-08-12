package performance

// benchmark_fixtures.go owns the canonical BenchmarkFixtureRegistry: the
// immutable, versioned benchmark fixtures every Velox optimization is
// measured against. A fixture pins WHAT the attempt is about (kind,
// cache mode, duration, clip count), WHAT makes it reproducible (asset
// manifest SHA, render-plan SHA, expected artifact SHA) and WHAT it is
// allowed to do (correctness / architecture / performance / I/O
// budgets).
//
// Rule (the "Formula 1 test track" contract): a fixture is IMMUTABLE.
// The registry is built once, exposes no mutating API, and Fixture()
// returns a copy by value — so "commit A → COPY_5M_HIGH" and "commit B
// → COPY_5M_HIGH" are genuinely comparable: the fixture (and its
// DigestSHA256) never changes under you.
//
// Budget thresholds use BudgetMax (Set + Value): Set=false means the
// threshold is not fixed yet — the plan explicitly says timing numbers
// are pinned only AFTER the new baseline is measured — and is never
// enforced; Set=true with Value=0 is a REAL zero invariant (e.g.
// ffmpeg_exec_max=0 for copy-only), enforced by EvaluateFixture.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
)

// BenchmarkFixtureID is the canonical registry key of one fixture.
type BenchmarkFixtureID string

// Canonical fixture IDs (plan: "benchmark fixtures" set).
const (
	FixtureCopy5MLow             BenchmarkFixtureID = "COPY_5M_LOW"
	FixtureCopy5MHigh            BenchmarkFixtureID = "COPY_5M_HIGH"
	FixtureCopy10MLow            BenchmarkFixtureID = "COPY_10M_LOW"
	FixtureCopy10MHigh           BenchmarkFixtureID = "COPY_10M_HIGH"
	FixtureFinalAudio5M          BenchmarkFixtureID = "FINAL_AUDIO_5M"
	FixtureComposite5MLow        BenchmarkFixtureID = "COMPOSITE_5M_LOW"
	FixtureComposite5MHigh       BenchmarkFixtureID = "COMPOSITE_5M_HIGH"
	FixtureCopyOnlyCanonical5MV1 BenchmarkFixtureID = "COPY_ONLY_CANONICAL_5M_V1"
)

// FixtureKind is the rendering class the fixture exercises.
type FixtureKind string

const (
	FixtureKindCopyOnly   FixtureKind = "copy_only"   // packet-level stream copy, zero decode/encode
	FixtureKindFinalAudio FixtureKind = "final_audio" // copy-only + final audio copy (no re-encode)
	FixtureKindComposite  FixtureKind = "composite"   // decode → compose → encode (zoom/text/filters)
)

// CacheMode is the asset-cache state the fixture demands.
type CacheMode string

const (
	CacheModeWarm CacheMode = "warm" // all assets warm in the cache
	CacheModeCold CacheMode = "cold" // first-touch cold fetch
)

// BudgetMax is one budget threshold with an explicit Set flag and an
// explicit direction.
//
//	Set=false → "TBD after baseline" (never enforced)
//	Set=true  → enforced, even when Value is 0 (a real zero invariant)
//	Min=true  → the KPI must be AT LEAST Value (violated when below);
//	           default false = upper bound (violated when above).
type BudgetMax struct {
	Set   bool    `json:"set"`
	Value float64 `json:"value"`
	Min   bool    `json:"min,omitempty"`
}

// MaxInt64 builds an enforced upper-bound integer threshold.
func MaxInt64(v int64) BudgetMax { return BudgetMax{Set: true, Value: float64(v)} }

// MaxFloat builds an enforced upper-bound float threshold.
func MaxFloat(v float64) BudgetMax { return BudgetMax{Set: true, Value: v} }

// MinFloat builds an enforced lower-bound float threshold (e.g. a
// minimum throughput); violated when actual < Value.
func MinFloat(v float64) BudgetMax { return BudgetMax{Set: true, Value: v, Min: true} }

// UnsetMax builds a "TBD after baseline" threshold that is never enforced.
func UnsetMax() BudgetMax { return BudgetMax{} }

// CorrectnessBudget pins the determinism/correctness invariants.
type CorrectnessBudget struct {
	// ArtifactSHARequired marks the fixture as requiring a deterministic
	// artifact; the expected SHA lives on BenchmarkFixture and is
	// compared by the BenchmarkRunner against the produced file (the
	// receipt itself carries no artifact SHA).
	ArtifactSHARequired  bool      `json:"artifact_sha_required"`
	VideoDecodeFramesMax BudgetMax `json:"video_decode_frames_max"`
	VideoEncodeFramesMax BudgetMax `json:"video_encode_frames_max"`
	AudioEncodePassesMax BudgetMax `json:"audio_encode_passes_max"`
}

// ArchitectureBudget pins the process/spawn invariants.
type ArchitectureBudget struct {
	FfmpegExecMax       BudgetMax `json:"ffmpeg_exec_max"`
	FfprobeExecMax      BudgetMax `json:"ffprobe_exec_max"`
	TempSegmentFilesMax BudgetMax `json:"temp_segment_files_max"`
	// ExternalExecMax caps the total external execve the engine spawned
	// (process-tree external process count — the C++ engine spawn itself
	// is the backend, not an external execve). Phase-1 copy-only: 0.
	ExternalExecMax BudgetMax `json:"external_exec_max"`
}

// PerformanceBudget pins the timing/throughput/CPU ceilings. P50 is a
// many-run aggregate (CheckPerformanceBudgets on the dedicated
// benchmark worker); P95WallMSMax is the run-level upper bound.
// MinThroughput is a LOWER bound (content-seconds rendered per
// wall-second — the realtime factor: DurationSec / wall_seconds);
// MaxCPUWallRatio caps the CPU/wall ratio (<<1 = not CPU-bound, >1 =
// CPU-parallel). All performance budgets are TBD until the Phase-1
// zero-spawn baseline is measured, and they are ONLY enforced on the
// dedicated self-hosted benchmark worker (plan §17) — never on shared
// CI runners.
type PerformanceBudget struct {
	P50WallMSMax    BudgetMax `json:"p50_wall_ms_max"`
	P95WallMSMax    BudgetMax `json:"p95_wall_ms_max"`
	MinThroughput   BudgetMax `json:"min_throughput"`
	MaxCPUWallRatio BudgetMax `json:"max_cpu_wall_ratio"`
}

// IOBudget pins the byte-amplification ceilings.
type IOBudget struct {
	ReadAmplificationMax  BudgetMax `json:"read_amplification_max"`
	WriteAmplificationMax BudgetMax `json:"write_amplification_max"`
}

// FixtureBudget is the complete per-fixture budget (plan §16).
type FixtureBudget struct {
	Correctness  CorrectnessBudget  `json:"correctness"`
	Architecture ArchitectureBudget `json:"architecture"`
	Performance  PerformanceBudget  `json:"performance"`
	IO           IOBudget           `json:"io"`
}

// BenchmarkFixture is ONE immutable benchmark specification.
//
// AssetSHA256 / PlanSHA256 / ExpectedArtifactSHA256 are pinned when the
// canonical fixture assets are registered (the benchmark harness
// verifies the actual asset manifest against these before every run);
// they are empty until then, and an empty SHA never changes the fixture
// identity — the digest below is computed from the whole fixture.
//
// On COPY_ONLY_CANONICAL_5M_V1, AssetSHA256 is the canonical spec
// digest (see canonical_fixture.go): a cross-host-deterministic
// identity of the track's asset SET. Byte-level per-asset SHAs depend
// on the encoder build and live in the locally-generated manifest
// (fixture_manifest.go), which must match this digest before a run.
type BenchmarkFixture struct {
	ID                     BenchmarkFixtureID `json:"id"`
	Kind                   FixtureKind        `json:"kind"`
	CacheMode              CacheMode          `json:"cache_mode"`
	DurationSec            int                `json:"duration_sec"`
	ClipCount              int                `json:"clip_count"`
	AssetSHA256            string             `json:"asset_sha256,omitempty"`
	PlanSHA256             string             `json:"plan_sha256,omitempty"`
	ExpectedArtifactSHA256 string             `json:"expected_artifact_sha256,omitempty"`
	ExpectedBehavior       string             `json:"expected_behavior"`
	Budget                 FixtureBudget      `json:"budget"`
}

// DigestSHA256 returns the stable content digest of the WHOLE fixture
// (identity + SHAs + behavior + budgets). Two commits are comparable
// against the same fixture only while the digest is unchanged — editing
// a budget changes the digest and forces a new fixture version, which
// is exactly the immutability the plan requires.
func (f BenchmarkFixture) DigestSHA256() string {
	data, err := json.Marshal(f)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// BenchmarkFixtureRegistry is the canonical, immutable fixture set. It
// exposes no mutating API: NewBenchmarkFixtureRegistry builds the
// canonical fixtures once and Fixture() returns copies by value, so a
// caller can never corrupt the registry.
type BenchmarkFixtureRegistry struct {
	fixtures map[BenchmarkFixtureID]BenchmarkFixture
}

// NewBenchmarkFixtureRegistry returns the registry pre-loaded with the
// canonical fixtures. Panics if a fixture would collide — a duplicate
// ID is a programming error that must fail at construction.
func NewBenchmarkFixtureRegistry() *BenchmarkFixtureRegistry {
	r := &BenchmarkFixtureRegistry{fixtures: map[BenchmarkFixtureID]BenchmarkFixture{}}
	// Copy-only fixtures: the Phase-1 invariants are already known —
	// zero decode/encode, zero external ffmpeg/ffprobe, zero temp
	// segments, write amplification close to 1.x. Timing ceilings stay
	// TBD until the new zero-spawn baseline is measured.
	zeroInvariants := FixtureBudget{
		Correctness: CorrectnessBudget{
			ArtifactSHARequired:  true,
			VideoDecodeFramesMax: MaxInt64(0),
			VideoEncodeFramesMax: MaxInt64(0),
			AudioEncodePassesMax: MaxInt64(0),
		},
		Architecture: ArchitectureBudget{
			FfmpegExecMax:       MaxInt64(0),
			FfprobeExecMax:      MaxInt64(0),
			TempSegmentFilesMax: MaxInt64(0),
			ExternalExecMax:     MaxInt64(0),
		},
		Performance: PerformanceBudget{P50WallMSMax: UnsetMax(), P95WallMSMax: UnsetMax(), MinThroughput: UnsetMax(), MaxCPUWallRatio: UnsetMax()},
		IO:          IOBudget{WriteAmplificationMax: MaxFloat(1.5), ReadAmplificationMax: UnsetMax()},
	}
	r.register(BenchmarkFixture{
		ID: FixtureCopy5MLow, Kind: FixtureKindCopyOnly, CacheMode: CacheModeWarm,
		DurationSec: 300, ClipCount: 5,
		ExpectedBehavior: "copy-only ~5m, low clip density: stream copy, zero decode/encode, zero external ffmpeg/ffprobe, single mux pass, deterministic artifact",
		Budget:           zeroInvariants,
	})
	r.register(BenchmarkFixture{
		ID: FixtureCopy5MHigh, Kind: FixtureKindCopyOnly, CacheMode: CacheModeWarm,
		DurationSec: 300, ClipCount: 24,
		ExpectedBehavior: "copy-only ~5m, high clip density: stream copy, zero decode/encode, zero external ffmpeg/ffprobe, single mux pass, deterministic artifact",
		Budget:           zeroInvariants,
	})
	r.register(BenchmarkFixture{
		ID: FixtureCopy10MLow, Kind: FixtureKindCopyOnly, CacheMode: CacheModeWarm,
		DurationSec: 600, ClipCount: 10,
		ExpectedBehavior: "copy-only ~10m, low clip density: stream copy, zero decode/encode, zero external ffmpeg/ffprobe, single mux pass, deterministic artifact",
		Budget:           zeroInvariants,
	})
	r.register(BenchmarkFixture{
		ID: FixtureCopy10MHigh, Kind: FixtureKindCopyOnly, CacheMode: CacheModeWarm,
		DurationSec: 600, ClipCount: 48,
		ExpectedBehavior: "copy-only ~10m, high clip density: stream copy, zero decode/encode, zero external ffmpeg/ffprobe, single mux pass, deterministic artifact",
		Budget:           zeroInvariants,
	})
	// FINAL_AUDIO_5M: the Phase-0 priority-1 optimization — copy-only
	// with the final audio stream copied, so even audio_encode_passes
	// must be zero. Same zero invariants as the plain copy fixtures.
	r.register(BenchmarkFixture{
		ID: FixtureFinalAudio5M, Kind: FixtureKindFinalAudio, CacheMode: CacheModeWarm,
		DurationSec: 300, ClipCount: 5,
		ExpectedBehavior: "copy-only ~5m + final-audio-copy: zero audio re-encode, zero decode/encode, zero external ffmpeg/ffprobe, deterministic artifact",
		Budget:           zeroInvariants,
	})
	// Composite fixtures: decode → compose → encode is LEGITIMATE work,
	// so no zero invariants apply; every budget stays TBD until the
	// Phase-1 copy-only baseline is measured and a composite baseline
	// can be pinned. ArtifactSHARequired is explicitly false so the
	// serialized fixture is self-documenting.
	compositeTBD := FixtureBudget{Correctness: CorrectnessBudget{ArtifactSHARequired: false}}
	r.register(BenchmarkFixture{
		ID: FixtureComposite5MLow, Kind: FixtureKindComposite, CacheMode: CacheModeWarm,
		DurationSec: 300, ClipCount: 5,
		ExpectedBehavior: "composite ~5m, low clip density: decode → compose → encode (filters/effects); budgets TBD after Phase-1 baseline",
		Budget:           compositeTBD,
	})
	r.register(BenchmarkFixture{
		ID: FixtureComposite5MHigh, Kind: FixtureKindComposite, CacheMode: CacheModeWarm,
		DurationSec: 300, ClipCount: 24,
		ExpectedBehavior: "composite ~5m, high clip density: decode → compose → encode (filters/effects); budgets TBD after Phase-1 baseline",
		Budget:           compositeTBD,
	})
	// COPY_ONLY_CANONICAL_5M_V1: the "Formula 1 test track" (plan §14).
	// Every media-engine change is benchmarked on this fixture first.
	// AssetSHA256 is pinned to the canonical spec digest, so the track
	// is byte-identifiable across hosts; the per-asset manifest is
	// generated by cmd/velox-fixture-gen and must match this digest.
	canonicalSpec := CanonicalFixtureSpecV1()
	r.register(BenchmarkFixture{
		ID: FixtureCopyOnlyCanonical5MV1, Kind: FixtureKindCopyOnly, CacheMode: CacheModeWarm,
		DurationSec: 300, ClipCount: 24,
		AssetSHA256:      canonicalSpec.SpecSHA256(),
		ExpectedBehavior: "canonical Phase-1 copy-only fixture: ~5m, 24 H264 1080p CFR clips, AAC 48kHz stereo final audio, zero filters/subtitles/transformations, all assets warm cache, single mux pass, deterministic artifact",
		Budget:           zeroInvariants,
	})
	return r
}

func (r *BenchmarkFixtureRegistry) register(f BenchmarkFixture) {
	if _, exists := r.fixtures[f.ID]; exists {
		panic(fmt.Sprintf("performance: duplicate benchmark fixture id %q", f.ID))
	}
	r.fixtures[f.ID] = f
}

// Fixture returns a COPY of the fixture with the given ID. Returning a
// value (not a pointer) is the immutability boundary: mutating the copy
// can never affect the registry.
//
// Future-proofing note: this copy-by-value guarantee holds ONLY while
// BenchmarkFixture stays a struct of value types. If a slice/map/pointer
// field is ever added, Fixture must deep-copy — add a test that pins
// the registry content unchanged after mutating a returned copy.
func (r *BenchmarkFixtureRegistry) Fixture(id BenchmarkFixtureID) (BenchmarkFixture, bool) {
	f, ok := r.fixtures[id]
	return f, ok
}

// All returns copies of every registered fixture, sorted by ID for
// deterministic output.
func (r *BenchmarkFixtureRegistry) All() []BenchmarkFixture {
	ids := make([]string, 0, len(r.fixtures))
	for id := range r.fixtures {
		ids = append(ids, string(id))
	}
	sort.Strings(ids)
	out := make([]BenchmarkFixture, 0, len(ids))
	for _, id := range ids {
		out = append(out, r.fixtures[BenchmarkFixtureID(id)])
	}
	return out
}

// Count returns the number of registered fixtures.
func (r *BenchmarkFixtureRegistry) Count() int { return len(r.fixtures) }

// EvaluateFixture checks a SINGLE PerformanceReceiptV1 against the
// fixture's enforced budgets and returns the violations (reuses
// BudgetViolation, the same type CheckDerivedBudgets uses). Only
// thresholds with Set=true are evaluated; zero invariants (Set=true,
// Value=0) are enforced like any other threshold. TBD thresholds
// (Set=false) are skipped — the plan pins them only after the new
// baseline is measured.
//
// NOTE (two-tier gate, plan §17): this is the single-receipt budget
// evaluation, a library/unit-test surface. The run-level performance
// gate the dedicated benchmark worker actually runs is
// CheckPerformanceBudgets (gate_tiers.go) — never wire EvaluateFixture
// into shared CI: its wall-clock and amplification thresholds are too
// noisy on shared runners.
//
// Scope note: artifact-SHA and temp-segment-file correctness cannot be
// expressed by the receipt (the receipt is pre-manifest and carries no
// filesystem snapshot); the BenchmarkRunner verifies both against the
// fixture before comparing receipts.
func EvaluateFixture(fixture BenchmarkFixture, receipt *PerformanceReceiptV1) []BudgetViolation {
	if receipt == nil {
		return nil
	}
	var v []BudgetViolation
	b := fixture.Budget

	v = evalThreshold(v, "correctness.video_decode_frames", float64(receipt.Media.FramesDecoded), b.Correctness.VideoDecodeFramesMax, "copy-only invariant: no frame decoding")
	v = evalThreshold(v, "correctness.video_encode_frames", float64(receipt.Media.Frames), b.Correctness.VideoEncodeFramesMax, "copy-only invariant: no frame encoding")
	v = evalThreshold(v, "correctness.audio_encode_passes", float64(receipt.Media.EncodePasses), b.Correctness.AudioEncodePassesMax, "final-audio invariant: no audio re-encode")
	v = evalThreshold(v, "architecture.ffmpeg_exec", float64(receipt.Process.FfmpegExecCount), b.Architecture.FfmpegExecMax, "copy-only invariant: no external ffmpeg processes")
	v = evalThreshold(v, "architecture.ffprobe_exec", float64(receipt.Process.FfprobeExecCount), b.Architecture.FfprobeExecMax, "copy-only invariant: no external ffprobe processes")
	v = evalThreshold(v, "architecture.execve", float64(receipt.Process.ExternalProcessCount), b.Architecture.ExternalExecMax, "copy-only invariant: no external execve")
	v = evalThreshold(v, "performance.wall_ms", float64(receipt.Timing.WallMs), b.Performance.P95WallMSMax, "per-run wall-clock upper bound (p95 budget)")
	v = evalThreshold(v, "io.read_amplification", receipt.Derived.ReadAmplification, b.IO.ReadAmplificationMax, "bytes read per output byte")
	v = evalThreshold(v, "io.write_amplification", receipt.Derived.WriteAmplification, b.IO.WriteAmplificationMax, "bytes written per output byte")
	return v
}

// evalThreshold appends a violation when the threshold is enforced and
// the observed value violates it (above an upper bound, below a
// lower bound).
func evalThreshold(v []BudgetViolation, kpi string, actual float64, max BudgetMax, msg string) []BudgetViolation {
	if violated, enforced := enforcedViolated(max, actual); enforced && violated {
		return append(v, BudgetViolation{KPI: kpi, Value: actual, Target: max.Value, Message: msg})
	}
	return v
}

// enforcedViolated is the ONE place that owns the BudgetMax Set/Min
// semantics, shared by the budget tier (evalThreshold), the CI gate
// (fixture_gate.go) and the performance tier (gate_tiers.go): an unset
// threshold is never enforced; a set upper bound is violated when
// actual > Value; a set lower bound (Min) when actual < Value.
func enforcedViolated(max BudgetMax, actual float64) (violated, enforced bool) {
	if !max.Set {
		return false, false
	}
	if max.Min {
		return actual < max.Value, true
	}
	return actual > max.Value, true
}
