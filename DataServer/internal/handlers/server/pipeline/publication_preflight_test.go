package pipeline

import (
	"strings"
	"testing"
)

func TestValidatePublicationCapabilitiesRequiresProvider(t *testing.T) {
	req := SubmitJobRequest{
		Publications: []SubmitPublication{{
			PublicationID: "pub-1",
			Destinations: []SubmitPublicationDestination{{
				DestinationID:   "dest-1",
				ProviderOptions: nil,
			}},
		}},
	}

	// Missing provider must fail closed, never default to a platform.
	if err := validatePublicationCapabilities(req); err == nil || !strings.Contains(err.Error(), "PROVIDER_REQUIRED") {
		t.Fatalf("missing provider must fail closed with PROVIDER_REQUIRED, got: %v", err)
	}

	// Empty/whitespace provider is treated as missing too.
	req.Publications[0].Destinations[0].ProviderOptions = map[string]any{"provider": "  "}
	if err := validatePublicationCapabilities(req); err == nil || !strings.Contains(err.Error(), "PROVIDER_REQUIRED") {
		t.Fatalf("whitespace-only provider must fail closed, got: %v", err)
	}

	// A provider Velox owns validates against its declared capability.
	req.Publications[0].Destinations[0].ProviderOptions = map[string]any{"provider": "drive"}
	if err := validatePublicationCapabilities(req); err != nil {
		t.Fatalf("owned provider with valid metadata rejected: %v", err)
	}

	// A social platform provider is no longer owned by Velox: fail closed.
	req.Publications[0].Destinations[0].ProviderOptions = map[string]any{"provider": "youtube"}
	if err := validatePublicationCapabilities(req); err == nil {
		t.Fatal("social provider must fail closed as unknown")
	}
}
