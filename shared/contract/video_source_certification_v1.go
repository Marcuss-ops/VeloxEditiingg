package contract

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

const (
	VideoSourceCertificationPayloadKey = "video_source_certification"
	VideoSourceCertificationVersion    = 1
	VideoSourceCertificationExecutorID = "video.source.certify.v1"
)

// VideoSourceCertificationV1 asks a worker to verify and inspect one complete
// immutable source. It has no timeline window and produces no media artifact.
type VideoSourceCertificationV1 struct {
	Version               int        `json:"version"`
	Source                AssetRefV2 `json:"source"`
	OutputProfileID       string     `json:"output_profile_id"`
	OutputProfileRevision string     `json:"output_profile_revision"`
}

func (r VideoSourceCertificationV1) Validate() error {
	if r.Version != VideoSourceCertificationVersion {
		return fmt.Errorf("video.source.certify.v1: unsupported version %d", r.Version)
	}
	if strings.TrimSpace(r.Source.AssetID) == "" || r.Source.Kind != "video" || r.Source.SizeBytes <= 0 {
		return fmt.Errorf("video.source.certify.v1: source asset identity, video kind and size are required")
	}
	if r.Source.SHA256 != "" && !isLowerSHA256(r.Source.SHA256) {
		return fmt.Errorf("video.source.certify.v1: source SHA-256 is invalid")
	}
	if r.Source.SHA256 == "" && strings.TrimSpace(r.Source.DriveFileID) == "" {
		return fmt.Errorf("video.source.certify.v1: unknown source digest requires an authorized provider locator")
	}
	profile, err := KnownCanonicalVideoProfileV1(r.OutputProfileID)
	if err != nil {
		return err
	}
	revision, err := VideoWindowPreparationRevision(profile, CanonicalPreparationQualityPolicyDefault)
	if err != nil || revision != r.OutputProfileRevision {
		return fmt.Errorf("video.source.certify.v1: unsupported canonical profile revision")
	}
	return nil
}

// VideoSourceCertificationIdentity is content-addressed when SHA is known.
// Unknown identities are locator-scoped and are never suitable for cross-job
// certificate reuse; the worker replaces this provisional key after hashing.
func VideoSourceCertificationIdentity(r VideoSourceCertificationV1) (string, error) {
	if err := r.Validate(); err != nil {
		return "", err
	}
	identity := struct {
		SHA256    string `json:"sha256,omitempty"`
		SizeBytes int64  `json:"size_bytes"`
		Revision  string `json:"revision"`
		Locator   string `json:"locator,omitempty"`
	}{SHA256: r.Source.SHA256, SizeBytes: r.Source.SizeBytes, Revision: r.OutputProfileRevision}
	if identity.SHA256 == "" {
		identity.Locator = strings.TrimSpace(r.Source.DriveFileID)
	}
	encoded, err := json.Marshal(identity)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	prefix := "source:"
	if identity.SHA256 == "" {
		prefix = "discovery:"
	}
	return prefix + hex.EncodeToString(sum[:]), nil
}
