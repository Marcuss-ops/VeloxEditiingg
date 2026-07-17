package store

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestDeliveryDestinationOpaqueStructShape enforces (via the Go
// compiler) that the DeliveryDestination struct does NOT carry the
// legacy YouTube-specific fields (AccountID, ChannelID, Language).
//
// Residuo 4 gradual rename: the struct now carries BOTH
// ExternalDestinationID (canonical, json:"external_destination_id,omitempty")
// AND SocialDestinationID (deprecated back-compat alias,
// json:"-"). Both are populated here to mirror what SQL readers
// produce after migration 092 lands.
func TestDeliveryDestinationOpaqueStructShape(t *testing.T) {
	_ = DeliveryDestination{
		DestinationID:         "dst_test_opaque",
		Provider:              "social_gateway",
		ExternalDestinationID: "external_dest_test",
		SocialDestinationID:   "external_dest_test", // deprecated alias mirrors canonical
		FolderID:              "fld_1",
		Name:                  "Opaque Test",
		Enabled:               true,
		ConfigurationJSON:     "{}",
		CreatedAt:             "2026-07-17T00:00:00Z",
		UpdatedAt:             "2026-07-17T00:00:00Z",
	}
}

// TestDeliveryDestinationJSONOpaqueKeys verifies the JSON serialisation
// of the opaque DeliveryDestination model after the Residuo 4 gradual
// rename:
//   - Required top-level key: external_destination_id (canonical,
//     post-rename).
//   - Legacy YouTube keys MUST NOT appear (Residuo 2 invariant).
//   - The deprecated SocialDestinationID alias is json:"-" so the
//     social_destination_id key MUST NOT appear in the serialized
//     JSON either; the field is back-compat reads only.
func TestDeliveryDestinationJSONOpaqueKeys(t *testing.T) {
	in := DeliveryDestination{
		DestinationID:         "dst_test_opaque",
		Provider:              "social_gateway",
		ExternalDestinationID: "external_dest_opaque_42",
		SocialDestinationID:   "external_dest_opaque_42", // mirrors canonical after SQL read
		FolderID:              "fld_42",
		Name:                  "Opaque Test",
		Enabled:               true,
		ConfigurationJSON:     `{"platform":"youtube"}`,
		CreatedAt:             "2026-07-17T00:00:00Z",
		UpdatedAt:             "2026-07-17T00:00:00Z",
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
		"destination_id", "provider", "external_destination_id",
		"folder_id", "name", "enabled",
		"configuration_json", "created_at", "updated_at",
	}
	for _, k := range requiredKeys {
		if _, ok := raw[k]; !ok {
			t.Errorf("required key %q missing in opaque JSON; got=%v", k, raw)
		}
	}
	// Legacy key set spans BOTH Residuo 2 (YouTube) and Residuo 4
	// (alias suppression during the gradual rename).
	// social_destination_id MUST NOT appear in the wire JSON even when
	// the deprecated alias field is populated, because the field is
	// json:"-".
	legacyKeys := []string{"account_id", "channel_id", "language", "social_destination_id"}
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

// TestDeliveryDestinationEmptyExternalDestinationIDOmitEmpty verifies
// the omitempty tag on external_destination_id (canonical, Renamed in
// Residuo 4): an empty value MUST NOT leak into the wire contract.
// Also verifies that the deprecated SocialDestinationID alias is
// json:"-" so it never serializes regardless of its value.
func TestDeliveryDestinationEmptyExternalDestinationIDOmitEmpty(t *testing.T) {
	in := DeliveryDestination{
		DestinationID:         "dst_unmapped",
		Provider:              "social_gateway",
		ExternalDestinationID: "", // unmapped, canonical empty
		SocialDestinationID:   "should-not-leak", // alias populated; json:"-" suppresses
		Name:                  "Unmapped",
		Enabled:               true,
		ConfigurationJSON:     "{}",
		CreatedAt:             "2026-07-17T00:00:00Z",
		UpdatedAt:             "2026-07-17T00:00:00Z",
	}
	blob, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(blob), `"external_destination_id"`) {
		t.Errorf("empty ExternalDestinationID must be omitempty; blob=%s", string(blob))
	}
	if strings.Contains(string(blob), `"social_destination_id"`) {
		t.Errorf("SocialDestinationID must be json:\"-\" (alias must not leak into wire JSON); blob=%s", string(blob))
	}
}
