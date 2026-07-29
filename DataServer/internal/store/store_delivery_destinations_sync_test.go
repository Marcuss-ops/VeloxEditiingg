package store

import (
	"context"
	"testing"
)

// TestUpsertSyncedDeliveryDestination_InsertEnabledRow pins the
// canonical initial-refresh path: a Velox POST /api/v1/publishing/targets
// call that returns a target with CanPost=true MUST persist
// delivery_destinations.enabled=1 + the opaque external_destination_id.
func TestUpsertSyncedDeliveryDestination_InsertEnabledRow(t *testing.T) {
	t.Parallel()
	s := openTestDB(t)
	defer s.Close()

	dest := DeliveryDestination{
		DestinationID:         "instaedit_extdst_01JREADY",
		Provider:              "social_gateway",
		ExternalDestinationID: "extdst_01JREADY",
		Name:                  "Wrestling Discovery",
		Enabled:               true,
		ConfigurationJSON:     `{"external_destination_id":"extdst_01JREADY"}`,
	}
	if err := s.UpsertSyncedDeliveryDestination(context.Background(), dest); err != nil {
		t.Fatalf("UpsertSyncedDeliveryDestination(enabled=true) initial insert: %v", err)
	}

	got, err := s.GetDeliveryDestinationByExternalID(context.Background(), "extdst_01JREADY")
	if err != nil {
		t.Fatalf("GetDeliveryDestinationByExternalID: %v", err)
	}
	if got == nil {
		t.Fatal("GetDeliveryDestinationByExternalID returned nil after insert")
	}
	if !got.Enabled {
		t.Fatalf("enabled flag after insert: want true; got false")
	}
	if got.ExternalDestinationID != "extdst_01JREADY" {
		t.Fatalf("external_destination_id: want extdst_01JREADY; got %q", got.ExternalDestinationID)
	}
	if got.Provider != "social_gateway" {
		t.Fatalf("provider: want social_gateway; got %q", got.Provider)
	}
	if got.Name != "Wrestling Discovery" {
		t.Fatalf("name: want Wrestling Discovery; got %q", got.Name)
	}
}

// TestUpsertSyncedDeliveryDestination_FlipsEnabledFalse pins the
// critical reauth-driven transition: a SECOND catalog refresh after
// the channel flips to reauth_required MUST update the existing row's
// enabled flag from 1 → 0. This is the cross-repo invariant the
// publishing_flow smoke script depends on — without this update, the
// destination would remain enabled in Velox even though the catalog
// now returns can_post=false, and Velox would dispatch jobs to a
// channel whose OAuth grant has been revoked.
func TestUpsertSyncedDeliveryDestination_FlipsEnabledFalse(t *testing.T) {
	t.Parallel()
	s := openTestDB(t)
	defer s.Close()

	const (
		destID       = "instaedit_extdst_01JFLIP"
		externalID   = "extdst_01JFLIP"
		channelName  = "Channel Reauth"
		providerName = "social_gateway"
	)

	// Phase 1 — initial refresh (channel healthy).
	if err := s.UpsertSyncedDeliveryDestination(context.Background(), DeliveryDestination{
		DestinationID:         destID,
		Provider:              providerName,
		ExternalDestinationID: externalID,
		Name:                  channelName,
		Enabled:               true,
		ConfigurationJSON:     `{"phase":"initial"}`,
	}); err != nil {
		t.Fatalf("initial upsert (enabled=true): %v", err)
	}
	row, err := s.GetDeliveryDestinationByExternalID(context.Background(), externalID)
	if err != nil || row == nil {
		t.Fatalf("post-initial lookup: row=%v err=%v", row, err)
	}
	if !row.Enabled {
		t.Fatalf("post-initial enabled: want true; got false (initial upsert did not write enabled=1)")
	}

	// Phase 2 — second refresh AFTER channel flipped to reauth_required.
	// The Velox handler at publishing_targets.go:131 writes
	// Enabled: target.CanPost, so the second call passes enabled=false.
	if err := s.UpsertSyncedDeliveryDestination(context.Background(), DeliveryDestination{
		DestinationID:         destID,
		Provider:              providerName,
		ExternalDestinationID: externalID,
		Name:                  channelName,
		Enabled:               false,
		ConfigurationJSON:     `{"phase":"reauth"}`,
	}); err != nil {
		t.Fatalf("second upsert (enabled=false): %v", err)
	}
	row, err = s.GetDeliveryDestinationByExternalID(context.Background(), externalID)
	if err != nil || row == nil {
		t.Fatalf("post-reauth lookup: row=%v err=%v", row, err)
	}
	if row.Enabled {
		t.Fatalf("post-reauth enabled: want false (catalog said can_post=false → Velox MUST flip enabled=0); got true — operator action: a reauth row would still dispatch jobs")
	}

	// Pin that the second upsert was an UPDATE, not an INSERT (i.e.,
	// external_destination_id is unchanged and the row count is still 1).
	rowByDest, err := s.GetDeliveryDestination(context.Background(), destID)
	if err != nil || rowByDest == nil {
		t.Fatalf("GetDeliveryDestination post-reauth: row=%v err=%v", rowByDest, err)
	}
	if rowByDest.ExternalDestinationID != externalID {
		t.Fatalf("external_destination_id drift after reauth: want %q; got %q", externalID, rowByDest.ExternalDestinationID)
	}
}

// TestUpsertSyncedDeliveryDestination_FlipsEnabledTrue is the
// re-enable companion: a channel that re-consents the OAuth flow MUST
// be re-flipped from disabled to enabled on the next catalog refresh.
func TestUpsertSyncedDeliveryDestination_FlipsEnabledTrue(t *testing.T) {
	t.Parallel()
	s := openTestDB(t)
	defer s.Close()

	const (
		destID     = "instaedit_extdst_01JREENABLE"
		externalID = "extdst_01JREENABLE"
	)

	// Phase 1 — disabled (operator initially disabled channel).
	if err := s.UpsertSyncedDeliveryDestination(context.Background(), DeliveryDestination{
		DestinationID:         destID,
		Provider:              "social_gateway",
		ExternalDestinationID: externalID,
		Enabled:               false,
	}); err != nil {
		t.Fatalf("initial upsert (enabled=false): %v", err)
	}

	// Phase 2 — re-enabled after OAuth re-consent.
	if err := s.UpsertSyncedDeliveryDestination(context.Background(), DeliveryDestination{
		DestinationID:         destID,
		Provider:              "social_gateway",
		ExternalDestinationID: externalID,
		Enabled:               true,
	}); err != nil {
		t.Fatalf("re-enable upsert (enabled=true): %v", err)
	}
	row, err := s.GetDeliveryDestinationByExternalID(context.Background(), externalID)
	if err != nil || row == nil {
		t.Fatalf("post-reenable lookup: row=%v err=%v", row, err)
	}
	if !row.Enabled {
		t.Fatalf("post-reenable enabled: want true; got false (catalog said can_post=true again — Velox MUST flip enabled=1 back)")
	}
}

// TestUpsertSyncedDeliveryDestination_UpdatesProviderOnConflict pins
// that name/provider/configuration_json DO get updated alongside
// enabled on conflict. A regression where the upsert only touched
// enabled would silently leave stale metadata visible to operators.
func TestUpsertSyncedDeliveryDestination_UpdatesProviderOnConflict(t *testing.T) {
	t.Parallel()
	s := openTestDB(t)
	defer s.Close()

	const destID = "instaedit_extdst_01JPROV"
	const externalID = "extdst_01JPROV"

	if err := s.UpsertSyncedDeliveryDestination(context.Background(), DeliveryDestination{
		DestinationID:         destID,
		Provider:              "social_gateway",
		ExternalDestinationID: externalID,
		Name:                  "Old Name",
		Enabled:               true,
		ConfigurationJSON:     `{"old":true}`,
	}); err != nil {
		t.Fatalf("initial: %v", err)
	}
	if err := s.UpsertSyncedDeliveryDestination(context.Background(), DeliveryDestination{
		DestinationID:         destID,
		Provider:              "social_gateway",
		ExternalDestinationID: externalID,
		Name:                  "New Name",
		Enabled:               false,
		ConfigurationJSON:     `{"new":true}`,
	}); err != nil {
		t.Fatalf("conflict: %v", err)
	}
	row, err := s.GetDeliveryDestination(context.Background(), destID)
	if err != nil || row == nil {
		t.Fatalf("post-conflict: row=%v err=%v", row, err)
	}
	if row.Name != "New Name" {
		t.Errorf("name: want New Name; got %q", row.Name)
	}
	if row.ConfigurationJSON != `{"new":true}` {
		t.Errorf("configuration_json: want {\"new\":true}; got %q", row.ConfigurationJSON)
	}
	if row.Enabled {
		t.Errorf("enabled: want false; got true")
	}
}
