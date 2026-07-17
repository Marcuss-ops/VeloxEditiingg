package store

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestDeliveryDestinationOpaqueStructShape enforces (via the Go
// compiler) that the DeliveryDestination struct does NOT carry the
// legacy YouTube-specific fields (AccountID, ChannelID, Language). The
// struct literal below would fail to compile if any of them were
// re-added without a typed migration step.
//
// Together with migration 091 (forward-only DROP COLUMN) and the
// destination_opaque_test.go in the deliveries package, this is the
// tri-layered guard against YouTube-domain reintroduction.
func TestDeliveryDestinationOpaqueStructShape(t *testing.T) {
	_ = DeliveryDestination{
		DestinationID:       "dst_test_opaque",
		Provider:            "social_gateway",
		SocialDestinationID: "social_dest_test",
		FolderID:            "fld_1",
		Name:                "Opaque Test",
		Enabled:             true,
		ConfigurationJSON:   "{}",
		CreatedAt:           "2026-07-17T00:00:00Z",
		UpdatedAt:           "2026-07-17T00:00:00Z",
	}
}

// TestDeliveryDestinationJSONOpaqueKeys verifies the JSON serialisation
// of the opaque DeliveryDestination model. Required snake_case keys
// must appear; legacy YouTube keys must NOT appear in any form.
func TestDeliveryDestinationJSONOpaqueKeys(t *testing.T) {
	in := DeliveryDestination{
		DestinationID:       "dst_test_opaque",
		Provider:            "social_gateway",
		SocialDestinationID: "social_dest_opaque_42",
		FolderID:            "fld_42",
		Name:                "Opaque Test",
		Enabled:             true,
		ConfigurationJSON:   `{"platform":"youtube"}`,
		CreatedAt:           "2026-07-17T00:00:00Z",
		UpdatedAt:           "2026-07-17T00:00:00Z",
	}
	blob, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var raw map[string]any
	if err := json.Unmarshal(blob, &raw); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	requiredKeys := []string{
		"destination_id", "provider", "social_destination_id",
		"folder_id", "name", "enabled",
		"configuration_json", "created_at", "updated_at",
	}
	for _, k := range requiredKeys {
		if _, ok := raw[k]; !ok {
			t.Errorf("required key %q missing in opaque JSON; got=%v", k, raw)
		}
	}
	legacyKeys := []string{"account_id", "channel_id", "language"}
	for _, k := range legacyKeys {
		if _, ok := raw[k]; ok {
			t.Errorf("LEGACY key %q leaked in opaque JSON — must NOT; blob=%s",
				k, string(blob))
		}
	}
	for k := range raw {
		if k != strings.ToLower(k) {
			t.Errorf("JSON key %q is not lowercase canonical", k)
		}
	}
}

// TestDeliveryDestinationEmptySocialDestinationIDOmitEmpty verifies
// the omitempty tag on social_destination_id: an empty value must NOT
// leak into the wire contract (operators reading the JSON must not see
// "social_destination_id":"" — they should see the key absent and
// infer the row is unmapped).
func TestDeliveryDestinationEmptySocialDestinationIDOmitEmpty(t *testing.T) {
	in := DeliveryDestination{
		DestinationID:       "dst_unmapped",
		Provider:            "social_gateway",
		SocialDestinationID: "", // unmapped
		Name:                "Unmapped",
		Enabled:             true,
		ConfigurationJSON:   "{}",
		CreatedAt:           "2026-07-17T00:00:00Z",
		UpdatedAt:           "2026-07-17T00:00:00Z",
	}
	blob, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(blob), `"social_destination_id"`) {
		t.Errorf("empty SocialDestinationID must be omitempty; blob=%s", string(blob))
	}
}
