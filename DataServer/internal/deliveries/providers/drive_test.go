package providers

import (
	"errors"
	"testing"

	"velox-server/internal/deliveries"
	drive "velox-server/internal/integrations/drive"
)

func TestDriveFolderReferenceUsesPerJobMetadata(t *testing.T) {
	destination := &deliveries.Destination{
		FolderID:             "configured-folder",
		DeliveryMetadataJSON: `{"folder_id":"https://drive.google.com/drive/u/1/folders/job-folder?usp=sharing"}`,
	}
	if got := driveFolderReference(destination); got != "job-folder" {
		t.Fatalf("driveFolderReference() = %q, want job-folder", got)
	}
}

func TestClassifyDriveErrorAuthentication(t *testing.T) {
	missing := errors.New("no token set - authenticate first")
	if got := classifyDriveError(errors.Join(drive.ErrNotAuthenticated, missing)); !errors.Is(got, deliveries.ErrProviderAuth) {
		t.Fatalf("missing token error = %v, want ErrProviderAuth", got)
	}
	if got := classifyDriveError(&drive.APIError{StatusCode: 401, Message: "unauthorized"}); !errors.Is(got, deliveries.ErrProviderAuth) {
		t.Fatalf("401 error = %v, want ErrProviderAuth", got)
	}
	if got := classifyDriveError(&drive.APIError{StatusCode: 500, Message: "server error"}); errors.Is(got, deliveries.ErrProviderAuth) {
		t.Fatalf("500 error = %v, must remain non-auth", got)
	}
}

func TestDriveFolderReferenceFallsBackToDestination(t *testing.T) {
	destination := &deliveries.Destination{FolderID: "configured-folder"}
	if got := driveFolderReference(destination); got != "configured-folder" {
		t.Fatalf("driveFolderReference() = %q, want configured-folder", got)
	}
}
