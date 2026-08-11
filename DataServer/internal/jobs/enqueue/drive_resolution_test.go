package enqueue

import (
	"context"
	"errors"
	"testing"
)

type fakeDriveFolderResolver struct {
	value string
	err   error
}

func (f fakeDriveFolderResolver) ResolveDriveFolderReference(context.Context, string) (string, error) {
	return f.value, f.err
}

func TestResolveDriveOutputFolderReferenceUsesTypedResolver(t *testing.T) {
	got, err := ResolveDriveOutputFolderReference(context.Background(), " rap ", fakeDriveFolderResolver{value: "folder-rap"})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got != "folder-rap" {
		t.Fatalf("resolved alias = %q, want folder-rap", got)
	}
}

func TestResolveDriveOutputFolderReferencePropagatesResolverError(t *testing.T) {
	wantErr := errors.New("database unavailable")
	_, err := ResolveDriveOutputFolderReference(context.Background(), "rap", fakeDriveFolderResolver{err: wantErr})
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
}

func TestResolveDriveOutputFolderReferenceWithoutResolverPreservesReference(t *testing.T) {
	const ref = "unknown-folder-alias"
	got, err := ResolveDriveOutputFolderReference(context.Background(), ref, nil)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got != ref {
		t.Fatalf("nil resolver result = %q, want original ref %q", got, ref)
	}
}

func TestResolveDriveOutputFolderReferenceDirectReferenceDoesNotNeedResolver(t *testing.T) {
	for _, ref := range []string{
		"https://drive.google.com/drive/u/2/folders/folder-direct?usp=sharing",
		"folder-id-longer-than-15",
	} {
		got, err := ResolveDriveOutputFolderReference(context.Background(), ref, nil)
		if err != nil {
			t.Fatalf("resolve %q: %v", ref, err)
		}
		if got != ref {
			t.Fatalf("direct reference %q changed to %q", ref, got)
		}
	}
}
