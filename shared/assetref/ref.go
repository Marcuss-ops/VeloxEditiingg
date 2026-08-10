package assetref

import (
	"encoding/json"
	"fmt"
	"strings"
)

// RefKind identifies where an asset reference is expected to be materialized.
// The kind is an internal boundary annotation; the legacy payload still carries
// a string URL or velox-asset:// URI.
type RefKind string

const (
	RefKindLocal         RefKind = "local"
	RefKindRemote        RefKind = "remote"
	RefKindDeferredDrive RefKind = "deferred_drive"
)

// AssetRef is the typed representation of an asset reference at acquisition
// boundaries. It deliberately marshals as the existing string wire value so
// embedding it in map[string]interface{} cannot change payload shape.
type AssetRef struct {
	kind  RefKind
	value string
	wire  string
}

// NewLocal creates a canonical local asset reference. value may be an existing
// velox-asset:// URI or its asset ID.
func NewLocal(value string) (AssetRef, error) {
	id, err := canonicalAssetID(value)
	if err != nil {
		return AssetRef{}, err
	}
	return AssetRef{kind: RefKindLocal, value: id, wire: "velox-asset://" + id}, nil
}

// NewDeferredDrive creates a Drive asset reference that will be materialized
// by the worker through the authenticated master asset bridge.
func NewDeferredDrive(fileID string) (AssetRef, error) {
	fileID, err := canonicalAssetID(fileID)
	if err != nil {
		return AssetRef{}, fmt.Errorf("assetref: invalid deferred Drive file ID %q: %w", fileID, err)
	}
	return AssetRef{kind: RefKindDeferredDrive, value: fileID, wire: "velox-asset://" + fileID}, nil
}

// NewRemote creates a remote HTTP(S) reference.
func NewRemote(rawURL string) (AssetRef, error) {
	rawURL = strings.TrimSpace(rawURL)
	lower := strings.ToLower(rawURL)
	if !strings.HasPrefix(lower, "http://") && !strings.HasPrefix(lower, "https://") {
		return AssetRef{}, fmt.Errorf("assetref: remote reference must use http(s): %q", rawURL)
	}
	return AssetRef{kind: RefKindRemote, value: rawURL, wire: rawURL}, nil
}

// Parse classifies a raw source reference before it is converted to the
// canonical payload. Google Drive file URLs become explicitly deferred;
// canonical velox-asset references are local unless a payload envelope carries
// the deferred_drive annotation.
func Parse(raw string) (AssetRef, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return AssetRef{}, ErrEmpty
	}
	if strings.HasPrefix(strings.ToLower(raw), "velox-asset://") {
		return NewLocal(raw)
	}
	if id, err := DriveFileID(raw); err == nil {
		return NewDeferredDrive(id)
	}
	if strings.HasPrefix(strings.ToLower(raw), "http://") || strings.HasPrefix(strings.ToLower(raw), "https://") {
		return NewRemote(raw)
	}
	id, err := canonicalAssetID(raw)
	if err != nil {
		return AssetRef{}, err
	}
	return AssetRef{kind: RefKindLocal, value: id, wire: raw}, nil
}

// ParseWire classifies a canonical wire URI using an optional explicit kind
// annotation. It is the inverse boundary used by worker materialization.
func ParseWire(raw string, kind RefKind) (AssetRef, error) {
	raw = strings.TrimSpace(raw)
	if kind == RefKindDeferredDrive {
		id, err := canonicalAssetID(raw)
		if err != nil {
			return AssetRef{}, err
		}
		return NewDeferredDrive(id)
	}
	if kind == RefKindRemote {
		return NewRemote(raw)
	}
	if kind == "" || kind == RefKindLocal {
		return NewLocal(raw)
	}
	return AssetRef{}, fmt.Errorf("assetref: unknown reference kind %q", kind)
}

// Kind reports the explicit acquisition kind.
func (r AssetRef) Kind() RefKind { return r.kind }

// ID returns the canonical asset ID for local/deferred references.
func (r AssetRef) ID() string {
	if r.kind == RefKindRemote {
		return ""
	}
	return r.value
}

// Value returns the underlying source value.
func (r AssetRef) Value() string { return r.value }

// IsDeferredDrive reports whether the worker must materialize a Drive file.
func (r AssetRef) IsDeferredDrive() bool { return r.kind == RefKindDeferredDrive }

// Wire returns the legacy payload representation.
func (r AssetRef) Wire() string {
	if r.wire != "" {
		return r.wire
	}
	if r.kind == RefKindRemote {
		return r.value
	}
	return "velox-asset://" + r.value
}

// MarshalJSON preserves the pre-existing string wire shape.
func (r AssetRef) MarshalJSON() ([]byte, error) {
	if r.value == "" {
		return []byte("null"), nil
	}
	return json.Marshal(r.Wire())
}

// UnmarshalJSON accepts the legacy string wire shape and classifies it.
func (r *AssetRef) UnmarshalJSON(data []byte) error {
	if r == nil {
		return fmt.Errorf("assetref: nil AssetRef receiver")
	}
	var raw string
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	parsed, err := Parse(raw)
	if err != nil {
		return err
	}
	*r = parsed
	return nil
}

func canonicalAssetID(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if strings.HasPrefix(strings.ToLower(raw), "velox-asset://") {
		raw = strings.TrimSpace(raw[len("velox-asset://"):])
	}
	if raw == "" || strings.ContainsAny(raw, "\\\x00\r\n") {
		return "", fmt.Errorf("assetref: empty or invalid asset ID")
	}
	for _, segment := range strings.Split(raw, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return "", fmt.Errorf("assetref: invalid asset ID %q", raw)
		}
	}
	return raw, nil
}

// IsLikelyDriveFileID is intentionally conservative. It is used only when a
// legacy canonical payload has no source annotation and the master must retain
// the existing deferred-Drive compatibility path.
func IsLikelyDriveFileID(value string) bool {
	value = strings.TrimSpace(value)
	if len(value) < 16 || len(value) > 128 {
		return false
	}
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			continue
		}
		return false
	}
	return true
}
