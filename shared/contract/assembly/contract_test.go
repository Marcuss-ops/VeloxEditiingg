package assembly

import (
	"strings"
	"testing"
	"time"

	videoContract "velox-shared/contract"
)

func TestNormalizeDispatch(t *testing.T) {
	if got := NormalizeDispatch(ExternalDispatch{}); got.Mode != ModeNever || got.StartPrepare {
		t.Fatalf("disabled dispatch = %#v", got)
	}
	if got := NormalizeDispatch(ExternalDispatch{SendToVelox: true}); got.Mode != ModeEager || !got.StartPrepare {
		t.Fatalf("enabled dispatch = %#v", got)
	}
}

func TestAssemblyJobV1ValidateAndDeriveWaitingState(t *testing.T) {
	job := AssemblyJobV1{
		ContractVersion: ContractVersion, JobID: "job-123", TimelineRevision: 1,
		TimelineHash: "sha256:timeline", Dispatch: NormalizeDispatch(ExternalDispatch{SendToVelox: true}),
		Output: Output{ProfileID: "velox-h264-copy-v1"},
		Assets: []AssetRequirement{
			{AssetID: "clip-1", Kind: KindSourceClip, Availability: AvailabilityKnown, URL: "https://example.test/clip.mp4", SHA256: strings.Repeat("a", 64), Required: true, State: AssetReady},
			{AssetID: "scene-7", Kind: KindPreparedScene, Availability: AvailabilityRuntime, Producer: ProducerRenderingGen, Required: true, State: AssetWaitingProducer},
		},
		Timeline: []TimelineItem{{SceneID: "scene-1", AssetID: "clip-1"}, {SceneID: "scene-7", AssetID: "scene-7"}},
	}
	if err := job.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if got := job.DeriveState(); got != StateWaitingRuntime {
		t.Fatalf("DeriveState() = %q, want %q", got, StateWaitingRuntime)
	}
	job.Assets[1].State = AssetReady
	if got := job.DeriveState(); got != StateReadyToFinalize {
		t.Fatalf("ready DeriveState() = %q, want %q", got, StateReadyToFinalize)
	}
}

func TestAssemblyJobV1RejectsUnknownVersionAndIncompleteKnownAsset(t *testing.T) {
	base := AssemblyJobV1{ContractVersion: ContractVersion, JobID: "job", TimelineRevision: 1, TimelineHash: "hash", Dispatch: NormalizeDispatch(ExternalDispatch{}), Output: Output{ProfileID: "profile"}}
	base.Assets = []AssetRequirement{{AssetID: "clip", Kind: KindSourceClip, Availability: AvailabilityKnown, Required: true, State: AssetDeclared}}
	if err := base.Validate(); err == nil {
		t.Fatal("expected incomplete known asset to be rejected")
	}
	base.Assets[0].URL = "https://example.test/clip"
	base.Assets[0].SHA256 = strings.Repeat("z", 64)
	if err := base.Validate(); err == nil {
		t.Fatal("expected malformed sha256 to be rejected")
	}
	base.Assets[0].SHA256 = strings.Repeat("b", 64)
	base.ContractVersion = "velox.assembly.v2"
	if err := base.Validate(); err == nil {
		t.Fatal("expected unknown contract version to be rejected")
	}
}

func TestPreparationManifestHashIsOrderIndependent(t *testing.T) {
	assets := []AssetRequirement{
		{AssetID: "b", Kind: KindSourceClip, Availability: AvailabilityKnown, URL: "https://example.test/b", SHA256: strings.Repeat("b", 64), SizeBytes: 1, Required: true, State: AssetDeclared},
		{AssetID: "a", Kind: KindSourceClip, Availability: AvailabilityKnown, URL: "https://example.test/a", SHA256: strings.Repeat("a", 64), SizeBytes: 1, Required: true, State: AssetDeclared},
	}
	one := PreparationManifest{ContractVersion: ContractVersion, JobID: "job", TimelineRevision: 1, Dispatch: NormalizeDispatch(ExternalDispatch{SendToVelox: true}), ExpectedProfile: CanonicalAssemblyProfileLegacyV1, Assets: assets}
	two := one
	two.Assets = []AssetRequirement{assets[1], assets[0]}
	if one.ComputePreparationHash() != two.ComputePreparationHash() {
		t.Fatal("preparation hash depends on asset ordering")
	}
	one.PreparationHash = one.ComputePreparationHash()
	if err := one.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func earlyAssemblyManifestFixture() EarlyAssemblyManifest {
	profile := videoContract.CanonicalVideoProfileV1Default
	manifest := EarlyAssemblyManifest{
		ContractVersion: CanonicalAssemblyContractVersionV1,
		JobID:           "early-job", Revision: 3,
		Dispatch:        NormalizeDispatch(ExternalDispatch{SendToVelox: true}),
		ExpectedProfile: profile.ProfileID, Profile: &profile,
		Assets: []AssetRequirement{{
			AssetID: "clip-1", Kind: KindSourceClip, Availability: AvailabilityKnown,
			URL: "https://example.test/clip", SHA256: strings.Repeat("a", 64), SizeBytes: 42,
			Required: true, State: AssetDeclared,
		}},
	}
	manifest.PreparationHash = manifest.ComputePreparationHash()
	return manifest
}

func TestEarlyAssemblyManifestValidatesCanonicalProfileAndRevision(t *testing.T) {
	manifest := earlyAssemblyManifestFixture()
	if err := manifest.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	got, err := manifest.EffectiveRevision()
	if err != nil || got != 3 {
		t.Fatalf("EffectiveRevision() = %d, %v; want 3", got, err)
	}
	if !strings.Contains(manifest.RevisionIdentity(), "early-job:3:") {
		t.Fatalf("RevisionIdentity() = %q", manifest.RevisionIdentity())
	}
	manifest.Profile.Codec = "vp9"
	if err := manifest.Validate(); err == nil {
		t.Fatal("invalid canonical profile must be rejected")
	}
}

func TestEarlyAssemblyManifestRevisionsAreIdempotent(t *testing.T) {
	previous := earlyAssemblyManifestFixture()
	replay := previous
	if disposition, err := replay.ReconcileRevision(&previous); err != nil || disposition != RevisionReplay {
		t.Fatalf("same revision replay = %q, %v; want idempotent replay", disposition, err)
	}

	next := previous
	next.Revision = 4
	next.PreparationHash = next.ComputePreparationHash()
	if disposition, err := next.ReconcileRevision(&previous); err != nil || disposition != RevisionApplied {
		t.Fatalf("new revision = %q, %v; want applied", disposition, err)
	}

	stale := previous
	stale.Revision = 2
	stale.PreparationHash = stale.ComputePreparationHash()
	if disposition, err := stale.ReconcileRevision(&next); err != nil || disposition != RevisionStale {
		t.Fatalf("stale revision = %q, %v; want stale ignored", disposition, err)
	}

	conflict := previous
	conflict.Assets = append([]AssetRequirement(nil), previous.Assets...)
	conflict.Assets[0].SHA256 = strings.Repeat("b", 64)
	conflict.PreparationHash = conflict.ComputePreparationHash()
	if _, err := conflict.ReconcileRevision(&previous); err == nil || !strings.Contains(err.Error(), "different preparation_hash") {
		t.Fatalf("same revision conflict = %v, want hash conflict", err)
	}
}

func TestEarlyAssemblyManifestRejectsRevisionAliasDriftAndMissingHash(t *testing.T) {
	manifest := earlyAssemblyManifestFixture()
	manifest.TimelineRevision = 2
	if err := manifest.Validate(); err == nil || !strings.Contains(err.Error(), "disagree") {
		t.Fatalf("revision alias drift error = %v", err)
	}
	manifest = earlyAssemblyManifestFixture()
	manifest.PreparationHash = ""
	if err := manifest.Validate(); err == nil || !strings.Contains(err.Error(), "preparation_hash is required") {
		t.Fatalf("missing hash error = %v", err)
	}
}

func TestExternalAssemblyRequestNormalizesEarlyManifest(t *testing.T) {
	manifest := earlyAssemblyManifestFixture()
	request := ExternalAssemblyRequest{EarlyManifest: &manifest}
	originalJobID := manifest.JobID
	job, err := request.Normalize(originalJobID)
	if err != nil {
		t.Fatalf("Normalize() error = %v", err)
	}
	if job == nil || job.PreparationHash != manifest.PreparationHash || job.TimelineRevision != manifest.Revision {
		t.Fatalf("normalized early manifest = %#v", job)
	}
	request.EarlyManifest.JobID = "another-job"
	if _, err := request.Normalize(originalJobID); err == nil || !strings.Contains(err.Error(), "must match idempotency key") {
		t.Fatalf("mismatched early manifest job id error = %v", err)
	}
}

func TestLeaseKindsSeparatePreparationFromExecution(t *testing.T) {
	prep := PreparationLease{LeaseID: "p", JobID: "job", WorkerID: "worker", PreparationHash: "sha256:hash", Kind: LeasePreparation, ExpiresAt: "2026-08-23T00:00:00Z"}
	if err := prep.Validate(); err != nil {
		t.Fatalf("preparation lease Validate() error = %v", err)
	}
	exec := ExecutionLease{LeaseID: "e", JobID: "job", WorkerID: "worker", Kind: LeaseExecution}
	if err := exec.Validate(); err != nil {
		t.Fatalf("execution lease Validate() error = %v", err)
	}
}

func TestSelectPreferredWorkerIsCacheAwareAndStable(t *testing.T) {
	request := PlacementRequest{RequiredCapabilities: []string{"assembly.v1"}, AssetSHA256: []string{"a", "b", "c"}}
	decision, err := SelectPreferredWorker([]WorkerPlacementSnapshot{
		{WorkerID: "worker-c", Available: true, CapacityAuthoritative: true, ActiveExecutionSlots: 0, MaxExecutionSlots: 1, FreeDiskBytes: 100, Capabilities: []string{"assembly.v1"}, CachedSHA256: []string{"a", "b"}},
		{WorkerID: "worker-b", Available: true, CapacityAuthoritative: true, ActiveExecutionSlots: 0, MaxExecutionSlots: 1, FreeDiskBytes: 100, Capabilities: []string{"assembly.v1"}, CachedSHA256: []string{"a", "b", "c"}},
		{WorkerID: "worker-a", Available: true, CapacityAuthoritative: false, MaxExecutionSlots: 1, Capabilities: []string{"assembly.v1"}, CachedSHA256: []string{"a", "b", "c"}},
	}, request)
	if err != nil {
		t.Fatalf("SelectPreferredWorker() error = %v", err)
	}
	if decision.WorkerID != "worker-b" || decision.CachedAssets != 3 || decision.MissingAssets != 0 {
		t.Fatalf("decision = %#v, want cache-complete worker-b", decision)
	}
}

func TestSelectPreferredWorkerFailsClosedForUnknownDiskAndCapacity(t *testing.T) {
	request := PlacementRequest{AssetSHA256: []string{"clip"}, AssetSizes: map[string]uint64{"clip": 2 << 30}, MinimumFreeDiskBytes: 2 << 30}
	_, err := SelectPreferredWorker([]WorkerPlacementSnapshot{
		{WorkerID: "unknown-disk", Available: true, CapacityAuthoritative: true, MaxExecutionSlots: 1, DiskAuthoritative: false, FreeDiskBytes: 10 << 30},
		{WorkerID: "full", Available: true, CapacityAuthoritative: true, MaxExecutionSlots: 1, ActiveExecutionSlots: 1, DiskAuthoritative: true, FreeDiskBytes: 10 << 30},
	}, request)
	if err == nil {
		t.Fatal("expected no eligible worker when disk or capacity authority is missing")
	}
}

func TestSelectPreferredWorkerUsesColdFallbackWhenNoCacheMatches(t *testing.T) {
	request := PlacementRequest{AssetSHA256: []string{"a", "b"}}
	decision, err := SelectPreferredWorker([]WorkerPlacementSnapshot{
		{WorkerID: "worker-z", Available: true, CapacityAuthoritative: true, MaxExecutionSlots: 1, FreeDiskBytes: 1 << 30},
		{WorkerID: "worker-a", Available: true, CapacityAuthoritative: true, MaxExecutionSlots: 1, FreeDiskBytes: 1 << 30},
	}, request)
	if err != nil {
		t.Fatalf("cold fallback should remain eligible: %v", err)
	}
	if decision.WorkerID != "worker-a" {
		t.Fatalf("cold tie should use deterministic worker_id ordering, got %q", decision.WorkerID)
	}
	if decision.CachedAssets != 0 || decision.MissingAssets != 2 {
		t.Fatalf("cold decision = %#v", decision)
	}
}

func TestSelectPreferredWorkerLoadAndAvailabilityBreakWarmTie(t *testing.T) {
	request := PlacementRequest{AssetSHA256: []string{"a"}}
	decision, err := SelectPreferredWorker([]WorkerPlacementSnapshot{
		{WorkerID: "busy", Available: true, CapacityAuthoritative: true, MaxExecutionSlots: 2, ActiveExecutionSlots: 0, LoadRatio: 0.9, EstimatedAvailableMS: 20_000, CachedSHA256: []string{"a"}},
		{WorkerID: "ready", Available: true, CapacityAuthoritative: true, MaxExecutionSlots: 2, ActiveExecutionSlots: 0, LoadRatio: 0.1, EstimatedAvailableMS: 100, CachedSHA256: []string{"a"}},
	}, request)
	if err != nil {
		t.Fatalf("SelectPreferredWorker() error = %v", err)
	}
	if decision.WorkerID != "ready" {
		t.Fatalf("load/availability should prefer ready worker, got %#v", decision)
	}
}

func TestPreparationLeaseLifecycleRenewalAndExpiry(t *testing.T) {
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	lease, err := NewPreparationLease("prep-1", "job-1", "worker-a", "sha256:prep", now, time.Minute)
	if err != nil {
		t.Fatalf("NewPreparationLease() error = %v", err)
	}
	if lease.State != PreparationAssigned || lease.Revision != 1 {
		t.Fatalf("new preparation lease = %#v", lease)
	}
	lease, err = lease.Transition(PreparationPrefetching, now)
	if err != nil {
		t.Fatalf("assigned -> prefetching: %v", err)
	}
	lease, err = lease.Transition(PreparationPrepared, now)
	if err != nil {
		t.Fatalf("prefetching -> prepared: %v", err)
	}
	lease, err = lease.Transition(PreparationWaiting, now)
	if err != nil {
		t.Fatalf("prepared -> waiting: %v", err)
	}
	lease, err = lease.Renew(now.Add(30*time.Second), time.Minute)
	if err != nil {
		t.Fatalf("Renew() error = %v", err)
	}
	if lease.Revision != 5 {
		t.Fatalf("renewed revision = %d, want 5", lease.Revision)
	}
	if _, err := lease.Expire(now.Add(89 * time.Second)); err == nil {
		t.Fatal("renewed lease must not expire before its new deadline")
	}
	lease, err = lease.Expire(now.Add(2*time.Minute + 1*time.Second))
	if err != nil {
		t.Fatalf("Expire() error = %v", err)
	}
	if lease.State != PreparationExpired {
		t.Fatalf("expired state = %q", lease.State)
	}
	replay, err := lease.Expire(now.Add(3 * time.Minute))
	if err != nil || replay.Revision != lease.Revision {
		t.Fatalf("expiry replay = %#v, %v; want idempotent", replay, err)
	}
	if _, err := lease.Renew(now.Add(3*time.Minute), time.Minute); err == nil {
		t.Fatal("expired lease must not renew")
	}
}

func TestExecutionLeaseLifecycleRejectsIllegalTransitions(t *testing.T) {
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	lease, err := NewExecutionLease("exec-1", "job-1", "worker-a", "prep-1", now, time.Minute)
	if err != nil {
		t.Fatalf("NewExecutionLease() error = %v", err)
	}
	if _, err := lease.Transition(ExecutionCompleted, now); err == nil {
		t.Fatal("pending -> completed must be rejected")
	}
	lease, err = lease.Transition(ExecutionActive, now)
	if err != nil {
		t.Fatalf("pending -> active: %v", err)
	}
	lease, err = lease.Renew(now.Add(10*time.Second), time.Minute)
	if err != nil {
		t.Fatalf("execution Renew(): %v", err)
	}
	lease, err = lease.Transition(ExecutionCompleted, now.Add(20*time.Second))
	if err != nil {
		t.Fatalf("active -> completed: %v", err)
	}
	if _, err := lease.Expire(now.Add(2 * time.Minute)); err == nil {
		t.Fatal("completed execution lease must not expire")
	}
}

func TestPromotePreparationToExecutionFallsBackWithoutChangingWorkerIdentity(t *testing.T) {
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	preparation, err := NewPreparationLease("prep-1", "job-1", "worker-a", "sha256:prep", now, 10*time.Minute)
	if err != nil {
		t.Fatalf("NewPreparationLease() error = %v", err)
	}
	preparation, err = preparation.Transition(PreparationPrefetching, now)
	if err != nil {
		t.Fatal(err)
	}
	preparation, err = preparation.Transition(PreparationPrepared, now)
	if err != nil {
		t.Fatal(err)
	}
	workers := []WorkerPlacementSnapshot{
		{WorkerID: "worker-a", Available: false, CapacityAuthoritative: true, MaxExecutionSlots: 1, CachedSHA256: []string{"clip"}},
		{WorkerID: "worker-b", Available: true, CapacityAuthoritative: true, MaxExecutionSlots: 1, CachedSHA256: []string{}},
	}
	lease, decision, err := PromotePreparationToExecution(preparation, "exec-1", now, time.Minute, workers, PlacementRequest{AssetSHA256: []string{"clip"}})
	if err != nil {
		t.Fatalf("PromotePreparationToExecution() error = %v", err)
	}
	if decision.WorkerID != "worker-b" || lease.WorkerID != "worker-b" {
		t.Fatalf("fallback decision/lease = %#v / %#v", decision, lease)
	}
	if lease.FallbackFromWorkerID != "worker-a" || lease.PreparationLeaseID != "prep-1" {
		t.Fatalf("fallback metadata = %#v", lease)
	}
	if preparation.WorkerID != "worker-a" {
		t.Fatalf("preparation worker identity changed to %q", preparation.WorkerID)
	}
}

func TestPromotePreparationToExecutionFailsClosedWhenNoAlternativeExists(t *testing.T) {
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	preparation, _ := NewPreparationLease("prep-1", "job-1", "worker-a", "sha256:prep", now, time.Minute)
	preparation, _ = preparation.Transition(PreparationPrefetching, now)
	_, _, err := PromotePreparationToExecution(preparation, "exec-1", now, time.Minute, []WorkerPlacementSnapshot{
		{WorkerID: "worker-a", Available: false, CapacityAuthoritative: true, MaxExecutionSlots: 1},
	}, PlacementRequest{})
	if err == nil {
		t.Fatal("expected fail-closed error when preferred worker has no eligible alternative")
	}
}
