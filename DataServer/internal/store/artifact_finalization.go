package store

import (
	"context"
	"database/sql"
	"encoding/hex"
	"strings"
	"time"

	"velox-server/internal/deliverycontract"
)

// artifact_finalization.go owns the verified artifact finalization surface:
// the FinalizeVerifiedParams projection, the SQLiteArtifactFinalizer type
// (the sole store-owned writer for the finalization transaction), and the
// canonical-SHA validation helper. The finalization transaction itself lives
// in artifact_finalization_finalize.go.

// FinalizeVerifiedParams is the store-owned persistence projection for the
// verified artifact finalization transaction. Keeping it in store prevents
// the SQL gateway from importing the artifacts orchestration package.
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

// SQLiteArtifactFinalizer is the sole store-owned writer for the verified
// artifact finalization transaction. It owns the transaction boundary and
// exposes no *sql.Tx to application packages.
type SQLiteArtifactFinalizer struct {
	db       *sql.DB
	resolver deliverycontract.DeliveryPlanResolver
}

func NewSQLiteArtifactFinalizer(db *sql.DB, resolver deliverycontract.DeliveryPlanResolver) *SQLiteArtifactFinalizer {
	if db == nil {
		panic("store: NewSQLiteArtifactFinalizer requires a non-nil database")
	}
	return &SQLiteArtifactFinalizer{db: db, resolver: resolver}
}

// NewSQLiteArtifactFinalizerFromStore binds the finalizer to the canonical
// SQLiteStore. The transaction remains entirely store-owned while the
// application composition root no longer passes a raw database handle into
// an artifact adapter.
func NewSQLiteArtifactFinalizerFromStore(s *SQLiteStore, resolver deliverycontract.DeliveryPlanResolver) *SQLiteArtifactFinalizer {
	if s == nil || s.db == nil {
		panic("store: NewSQLiteArtifactFinalizerFromStore requires a non-nil SQLiteStore")
	}
	return &SQLiteArtifactFinalizer{db: s.db, resolver: resolver}
}

func isCanonicalSHA256(value string) bool {
	if len(value) != 64 || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

var _ interface {
	FinalizeVerified(context.Context, FinalizeVerifiedParams) (*Artifact, error)
} = (*SQLiteArtifactFinalizer)(nil)
