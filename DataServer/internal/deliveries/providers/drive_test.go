package providers

import (
	"testing"

	"velox-server/internal/deliveries"
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

func TestDriveFolderReferenceFallsBackToDestination(t *testing.T) {
	destination := &deliveries.Destination{FolderID: "configured-folder"}
	if got := driveFolderReference(destination); got != "configured-folder" {
		t.Fatalf("driveFolderReference() = %q, want configured-folder", got)
	}
}
