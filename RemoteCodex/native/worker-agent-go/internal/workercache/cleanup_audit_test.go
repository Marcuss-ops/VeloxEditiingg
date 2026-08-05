package workercache

import (
	"context"
	"testing"
	"time"
)

func TestCleanupWithPolicy_AuditsProtectedSharedAndRemovedAssets(t *testing.T) {
	f := newPolicyFixture(t)
	ctx := context.Background()
	t0 := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	now := t0.Add(10 * time.Second)

	seedRow(t, f.cache, f.dir, "PROTECTED-CLIP", t0.Add(-10*time.Minute))
	seedRow(t, f.cache, f.dir, "SHARED-STOCK", t0.Add(-10*time.Minute))
	seedRow(t, f.cache, f.dir, "RECENT-MUSIC", t0)
	seedRow(t, f.cache, f.dir, "OLD-VOICEOVER", t0.Add(-10*time.Minute))
	if err := f.cache.Acquire(ctx, "SHARED-STOCK", "job-shared-42"); err != nil {
		t.Fatalf("Acquire shared asset: %v", err)
	}
	if err := f.cache.Acquire(ctx, "SHARED-STOCK", "job-shared-43"); err != nil {
		t.Fatalf("Acquire second shared lease: %v", err)
	}

	var events []CleanerAuditEvent
	policy := CleanupPolicy{
		RecentUseGrace: 3 * time.Minute,
		SnapshotMaxAge: 2 * time.Minute,
		AuditLogger: func(event CleanerAuditEvent) {
			events = append(events, event)
		},
		AssetMetadata: map[string]CleanerAssetMetadata{
			"PROTECTED-CLIP": {Role: "scene_clip", FutureReferenceCount: 2},
			"SHARED-STOCK":   {Role: "shared_stock", FutureReferenceCount: 5},
			"RECENT-MUSIC":   {Role: "music", FutureReferenceCount: 1},
			"OLD-VOICEOVER":  {Role: "voiceover", FutureReferenceCount: 0},
		},
	}

	stats, err := CleanupWithPolicy(ctx, f.cache, now, []string{"PROTECTED-CLIP"}, policy, now)
	if err != nil {
		t.Fatalf("CleanupWithPolicy: %v", err)
	}
	if stats.Inspected != 4 || len(events) != 4 {
		t.Fatalf("inspected=%d events=%d, want 4/4", stats.Inspected, len(events))
	}

	byAsset := make(map[string]CleanerAuditEvent, len(events))
	for _, event := range events {
		if event.Event != cleanerAuditEventName {
			t.Errorf("event name=%q, want %q", event.Event, cleanerAuditEventName)
		}
		if event.Timestamp.IsZero() {
			t.Errorf("%s has zero timestamp", event.AssetKey)
		}
		byAsset[event.AssetKey] = event
	}

	assertAudit := func(asset, role, decision, reason, lease string, futureRefs, activeLeaseCount int, sizeBytes int64) {
		t.Helper()
		event, ok := byAsset[asset]
		if !ok {
			t.Fatalf("missing audit event for %s", asset)
		}
		if event.Role != role || event.Decision != decision || event.Reason != reason || event.Lease != lease || event.FutureReferenceCount != futureRefs || event.ActiveLeaseCount != activeLeaseCount || event.SizeBytes != sizeBytes {
			t.Errorf("audit[%s]=%+v, want role=%q decision=%q reason=%q lease=%q active_leases=%d future_refs=%d size=%d", asset, event, role, decision, reason, lease, activeLeaseCount, futureRefs, sizeBytes)
		}
	}

	assertAudit("PROTECTED-CLIP", "scene_clip", "kept", "protected_snapshot", "", 2, 0, int64(len("FAKE VIDEO BYTES PROTECTED-CLIP")))
	assertAudit("SHARED-STOCK", "shared_stock", "kept", "active_lease", "job-shared-42", 5, 2, int64(len("FAKE VIDEO BYTES SHARED-STOCK")))
	assertAudit("RECENT-MUSIC", "music", "kept", "recent_use_grace", "", 1, 0, int64(len("FAKE VIDEO BYTES RECENT-MUSIC")))
	assertAudit("OLD-VOICEOVER", "voiceover", "removed", "not_protected_and_grace_expired", "", 0, 0, int64(len("FAKE VIDEO BYTES OLD-VOICEOVER")))

	if _, ok, _ := f.cache.Find(ctx, "PROTECTED-CLIP"); !ok {
		t.Fatal("protected asset was removed")
	}
	if _, ok, _ := f.cache.Find(ctx, "SHARED-STOCK"); !ok {
		t.Fatal("shared leased asset was removed")
	}
	if _, ok, _ := f.cache.Find(ctx, "RECENT-MUSIC"); !ok {
		t.Fatal("recent asset was removed")
	}
	if _, ok, _ := f.cache.Find(ctx, "OLD-VOICEOVER"); ok {
		t.Fatal("expired asset remains indexed")
	}
}

func TestCleanupWithAudit_AuditsInFlightAsset(t *testing.T) {
	cache, dir := cleanupFixture(t)
	ctx := context.Background()
	storeSeeded(t, cache, dir, "IN-FLIGHT")
	if _, err := cache.DB().ExecContext(ctx, `UPDATE cached_assets SET download_complete = 0 WHERE asset_key = ?`, "IN-FLIGHT"); err != nil {
		t.Fatalf("mark in-flight: %v", err)
	}

	var got CleanerAuditEvent
	stats, err := CleanupWithAudit(ctx, cache, nil, func(event CleanerAuditEvent) { got = event }, nil)
	if err != nil {
		t.Fatalf("CleanupWithAudit: %v", err)
	}
	if stats.SkippedInFlight != 1 || got.Decision != "kept" || got.Reason != "download_in_flight" {
		t.Fatalf("stats=%+v audit=%+v, want in-flight kept audit", stats, got)
	}
}
