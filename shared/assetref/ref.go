package assetref

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Wire schemes. The scheme IS the wire-level kind: a canonical reference is
// self-sufficient — `velox-asset://<id>` is a local content-addressed asset,
// `velox-drive://<fileID>` is a deferred Drive file materialized by the
// worker through the authenticated master bridge, and an http(s) URL is a
// remote reference (must be pre-resolved before worker dispatch). There is
// no sibling annotation and no heuristic: the scheme alone classifies.
const (
	SchemeVeloxAsset = "velox-asset" // local, content-addressed asset
	SchemeVeloxDrive = "velox-drive" // deferred Drive file (worker bridge)
)

// RefKind identifies where an asset reference is expected to be materialized.
// It mirrors the wire scheme 1:1 and is retained as the in-memory boundary
// annotation between typed code and the wire string.
type RefKind string

const (
	RefKindLocal         RefKind = "local"
	RefKindRemote        RefKind = "remote"
	RefKindDeferredDrive RefKind = "deferred_drive"
)

// AssetRef is the typed representation of an asset reference at acquisition
// boundaries. It marshals as the self-sufficient string wire value so
// embedding it in map[string]interface{} cannot change payload shape: the
// scheme carries the kind (velox-asset:// local, velox-drive:// deferred
// Drive, http(s):// remote).
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
	return AssetRef{kind: RefKindLocal, value: id, wire: SchemeVeloxAsset + "://" + id}, nil
}

// NewDeferredDrive creates a Drive asset reference that will be materialized
// by the worker through the authenticated master asset bridge. The wire is
// self-sufficient: `velox-drive://<fileID>`.
func NewDeferredDrive(fileID string) (AssetRef, error) {
	fileID, err := canonicalAssetID(fileID)
	if err != nil {
		return AssetRef{}, fmt.Errorf("assetref: invalid deferred Drive file ID %q: %w", fileID, err)
	}
	return AssetRef{kind: RefKindDeferredDrive, value: fileID, wire: SchemeVeloxDrive + "://" + fileID}, nil
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

// WireAssetID extracts the asset ID from a canonical velox wire reference.
// It reports ok=true only for the two velox schemes (local and deferred
// Drive); http(s) URLs and bare values are not canonical wire references.
func WireAssetID(reference string) (id string, ok bool) {
	trimmed := strings.TrimSpace(reference)
	lower := strings.ToLower(trimmed)
	for _, scheme := range []string{SchemeVeloxAsset, SchemeVeloxDrive} {
		prefix := scheme + "://"
		if strings.HasPrefix(lower, prefix) {
			id := strings.TrimSpace(trimmed[len(prefix):])
			return id, id != ""
		}
	}
	return "", false
}

// Parse classifies a raw source reference before it is converted to the
// canonical payload. Canonical wire schemes (velox-asset:// local,
// velox-drive:// deferred) are authoritative; Google Drive file URLs become
// explicitly deferred; http(s) URLs become remote.
func Parse(raw string) (AssetRef, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return AssetRef{}, ErrEmpty
	}
	lower := strings.ToLower(raw)
	if strings.HasPrefix(lower, SchemeVeloxDrive+"://") {
		id, err := canonicalAssetID(raw[len(SchemeVeloxDrive+"://"):])
		if err != nil {
			return AssetRef{}, err
		}
		return NewDeferredDrive(id)
	}
	if strings.HasPrefix(lower, SchemeVeloxAsset+"://") {
		return NewLocal(raw)
	}
	if id, err := DriveFileID(raw); err == nil {
		return NewDeferredDrive(id)
	}
	if strings.HasPrefix(lower, "http://") || strings.HasPrefix(lower, "https://") {
		return NewRemote(raw)
	}
	id, err := canonicalAssetID(raw)
	if err != nil {
		return AssetRef{}, err
	}
	return AssetRef{kind: RefKindLocal, value: id, wire: raw}, nil
}

// ParseWire classifies a canonical wire reference. The scheme is
// authoritative (velox-drive:// is always deferred); the explicit kind
// argument remains only for legacy callers that carry an external annotation
// for a bare velox-asset:// reference.
func ParseWire(raw string, kind RefKind) (AssetRef, error) {
	raw = strings.TrimSpace(raw)
	if strings.HasPrefix(strings.ToLower(raw), SchemeVeloxDrive+"://") {
		id, err := canonicalAssetID(raw[len(SchemeVeloxDrive+"://"):])
		if err != nil {
			return AssetRef{}, err
		}
		return NewDeferredDrive(id)
	}
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

// Wire returns the self-sufficient wire representation. For local and
// deferred references the scheme encodes the kind (velox-asset://,
// velox-drive://); remote references are their http(s) URL.
func (r AssetRef) Wire() string {
	if r.wire != "" {
		return r.wire
	}
	if r.kind == RefKindRemote {
		return r.value
	}
	if r.kind == RefKindDeferredDrive {
		return SchemeVeloxDrive + "://" + r.value
	}
	return SchemeVeloxAsset + "://" + r.value
}

// MarshalJSON emits the self-sufficient string wire shape.
func (r AssetRef) MarshalJSON() ([]byte, error) {
	if r.value == "" {
		return []byte("null"), nil
	}
	return json.Marshal(r.Wire())
}

// UnmarshalJSON accepts the wire string and classifies it by scheme.
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
	if id, ok := WireAssetID(raw); ok {
		raw = id
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
