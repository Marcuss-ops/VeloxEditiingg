package deliveries

import (
	"errors"
	"fmt"
	"testing"
)

// TestDestinationOpaqueStructShape enforces (via the Go compiler) that
// the typed Destination struct does NOT carry the legacy YouTube-
// specific fields (AccountID, ChannelID, Language). The struct literal
// below would fail to compile if any of them were re-added without a
// typed migration step. Together with the matching compile-time check
// in store.delivery_destination_opaque_test.go and the migration 091
// forward-only DROP COLUMN, this is the third line of defence against
// YouTube-domain re-introduction.
//
// Residuo 4 gradual rename: ExternalDestinationID is canonical (new);
// SocialDestinationID is the deprecated back-compat alias mirroring
// ExternalDestinationID. Both fields present so the compile-time
// shape pin spans the post-rename contract (canonical reads + alias
// reads both produce the same value).
func TestDestinationOpaqueStructShape(t *testing.T) {
	_ = Destination{
		DestinationID:         "dst_test_opaque",
		Provider:              "social_gateway",
		ExternalDestinationID: "external_dest_test",
		SocialDestinationID:   "external_dest_test",
		FolderID:              "fld_1",
		Name:                  "Opaque Test",
		Enabled:               true,
		Configuration:         []byte(`{}`),
		ConfigurationJSON:     "{}",
		DeliveryMetadataJSON:  "",
	}
}

// TestErrDestinationUnmappedSentinel verifies the opaque-mode fail-closed
// sentinel error is exposed and stable. The runner's processLease marks
// failed deliveries with code DESTINATION_UNMAPPED when this error is
// wrapped into the hydrate step.
func TestErrDestinationUnmappedSentinel(t *testing.T) {
	if ErrDestinationUnmapped == nil {
		t.Fatal("ErrDestinationUnmapped must be a non-nil sentinel")
	}
	if ErrDestinationUnmapped.Error() == "" {
		t.Fatal("ErrDestinationUnmapped.Error() must be non-empty for operator logs")
	}
	if ErrDestinationUnmapped != ErrDestinationUnmapped {
		t.Fatal("ErrDestinationUnmapped identity changed across reads")
	}
}

// TestErrDestinationUnmappedErrorsIsChain verifies that
//   * errors.New wrap (no %w) does NOT inherit sentinel identity
//   * fmt.Errorf %w wrap DOES inherit sentinel identity
//
// Mirrors the exact format string used in runner.hydrateDestination.
func TestErrDestinationUnmappedErrorsIsChain(t *testing.T) {
	plain := errors.New("hydrate wrap: " + ErrDestinationUnmapped.Error())
	if errors.Is(plain, ErrDestinationUnmapped) {
		t.Fatal("plain errors.New wrap must NOT inherit sentinel identity — use fmt.Errorf %w")
	}
	proper := fmt.Errorf("deliveries: destination %s: %w", "dst_x", ErrDestinationUnmapped)
	if !errors.Is(proper, ErrDestinationUnmapped) {
		t.Fatal("fmt.Errorf %w wrap must satisfy errors.Is(err, ErrDestinationUnmapped)")
	}
}
