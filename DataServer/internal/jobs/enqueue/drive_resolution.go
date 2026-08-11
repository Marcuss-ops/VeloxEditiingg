package enqueue

import "context"

// DriveFolderResolver is the enqueue boundary for Drive master-folder
// resolution. The enqueue package owns the port; the store package owns the
// SQL adapter. This keeps payload construction independent from SQLite.
type DriveFolderResolver interface {
	ResolveDriveFolderReference(context.Context, string) (string, error)
}

// ResolveDriveOutputFolderReference resolves a Drive target through the
// supplied repository. A nil resolver is valid for callers that only accept
// direct references or do not have the optional Drive lookup wired; the raw
// reference is then preserved. Repository failures are returned so payload
// builders cannot silently turn a database outage into a different target.
func ResolveDriveOutputFolderReference(ctx context.Context, ref string, resolver DriveFolderResolver) (string, error) {
	if resolver == nil {
		return ref, nil
	}
	return resolver.ResolveDriveFolderReference(ctx, ref)
}
