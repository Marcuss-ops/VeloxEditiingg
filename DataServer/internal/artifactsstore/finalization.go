package artifactsstore

import (
	"context"
	"database/sql"
	"encoding/hex"
	"strings"
	"time"

	"velox-server/internal/deliverycontract"
	"velox-server/internal/repository"
)

// finalization.go owns the verified artifact finalization surface: the
// FinalizeVerifiedParams projection, the SQLiteArtifactFinalizer type (the
// sole writer for the finalization transaction), and the canonical-SHA
// validation helper. The finalization transaction itself lives in
// finalization_finalize.go.

// FinalizeVerifiedParams is the persistence projection for the verified
// artifact finalization transaction. It lives in the artifactsstore leaf so
// internal/artifacts can depend on the leaf without importing internal/store.
type FinalizeVerifiedParams struct {
	UploadID             string
	ArtifactID           string
	JobID                string
	AttemptID            string
	WorkerID             string
	LeaseID              string
	AttemptNumber        int
	ExpectedRevision     int
	StorageProvider      string
	StorageKey           string
	SHA256               string
	SizeBytes            int64
	MIMEType             string
	VerifiedAt           time.Time
	DestinationID        string
	AsyncProbe           bool
	ExpectedAudioStreams int
}

// SQLiteArtifactFinalizer is the sole writer for the verified artifact
// finalization transaction. It owns the transaction boundary and exposes no
// *sql.Tx to application packages.
type SQLiteArtifactFinalizer struct {
	db       *sql.DB
	resolver deliverycontract.DeliveryPlanResolver
}

func NewSQLiteArtifactFinalizer(db *sql.DB, resolver deliverycontract.DeliveryPlanResolver) *SQLiteArtifactFinalizer {
	if db == nil {
		panic("artifactsstore: NewSQLiteArtifactFinalizer requires a non-nil database")
	}
	return &SQLiteArtifactFinalizer{db: db, resolver: resolver}
}

func isCanonicalSHA256(value string) bool {
	if len(value) != 64 || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

var _ interface {
	FinalizeVerified(context.Context, FinalizeVerifiedParams) (*repository.Artifact, error)
} = (*SQLiteArtifactFinalizer)(nil)
