// Package deliveryrelay is the SQLite persistence for drive_relay_sessions
// and the Drive-relay destination lookup. It lives under internal/store (the
// canonical SQL gateway) because the deliveries/providers package must not
// couple to SQL directly (scripts/ci/ratchet-sql.sh). The relay keeps all
// business policy (chunking, resumable upload, Drive API); this package owns
// only the rows.
package deliveryrelay

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// sessionLifetime is how long a prepared resumable session stays usable
// before it must be re-prepared (Drive resumable URIs expire after one week).
const sessionLifetime = 7 * 24 * time.Hour

// Session is one persisted resumable-upload session row.
type Session struct {
	UploadID      string
	ArtifactID    string
	DestinationID string
	PublicationID string
	FolderID      string
	SessionURI    string
	PartSize      int64
	NextOffset    int64
	InFlight      bool
	State         string
	RemoteID      string
	RemoteURL     string
}

// RelayDestination is one enabled Drive delivery destination resolved for a
// job (joined from job_delivery_plans, delivery_destinations and jobs).
type RelayDestination struct {
	DestinationID string
	PublicationID string
	ParentFolder  string
	Metadata      string
	VideoTitle    string
	RequestJSON   string
}

// Evidence is the verified-relay fact set read back for publication receipts.
type Evidence struct {
	ArtifactID    string
	DestinationID string
	PublicationID string
	RemoteID      string
	RemoteURL     string
	FolderID      string
	SHA256        string
	SizeBytes     int64
}

// SessionStore owns the drive_relay_sessions rows for one *sql.DB.
type SessionStore struct {
	db *sql.DB
}

// New wraps an existing *sql.DB. It panics on nil, mirroring the other store
// leaf constructors: a nil DB is a bootstrap programming error, not a
// runtime condition.
func New(db *sql.DB) *SessionStore {
	if db == nil {
		panic("deliveryrelay: New requires a non-nil *sql.DB")
	}
	return &SessionStore{db: db}
}

// LoadDestinations resolves the enabled Drive destinations of a job in
// priority order, with the job title and request JSON needed for naming.
func (s *SessionStore) LoadDestinations(ctx context.Context, jobID string) ([]RelayDestination, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT jdp.destination_id, COALESCE(jdp.publication_id,''), COALESCE(dd.folder_id,''),
		       COALESCE(jdp.metadata_json,'{}'), COALESCE(j.video_name,''), COALESCE(j.request_json,'')
		FROM job_delivery_plans jdp
		JOIN delivery_destinations dd ON dd.destination_id=jdp.destination_id
		JOIN jobs j ON j.job_id=jdp.job_id
		WHERE jdp.job_id=? AND jdp.enabled=1 AND dd.enabled=1
		  AND lower(dd.provider) IN ('drive','google_drive')
		ORDER BY jdp.priority ASC, jdp.destination_id ASC`, jobID)
	if err != nil {
		return nil, fmt.Errorf("load Drive relay destinations: %w", err)
	}
	defer rows.Close()
	var out []RelayDestination
	for rows.Next() {
		var t RelayDestination
		if err := rows.Scan(&t.DestinationID, &t.PublicationID, &t.ParentFolder, &t.Metadata, &t.VideoTitle, &t.RequestJSON); err != nil {
			return nil, fmt.Errorf("scan Drive relay destination: %w", err)
		}
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read Drive relay destinations: %w", err)
	}
	return out, nil
}

// SessionExpiresAt reports the persisted expiry of a session row. ok is false
// when the row does not exist; a parse failure surfaces as an error.
func (s *SessionStore) SessionExpiresAt(ctx context.Context, uploadID, destinationID, publicationID string) (time.Time, bool, error) {
	var expires string
	err := s.db.QueryRowContext(ctx,
		`SELECT expires_at FROM drive_relay_sessions WHERE upload_id=? AND destination_id=? AND publication_id=?`,
		uploadID, destinationID, publicationID).Scan(&expires)
	if errors.Is(err, sql.ErrNoRows) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, err
	}
	until, parseErr := time.Parse(time.RFC3339Nano, expires)
	if parseErr != nil {
		return time.Time{}, false, fmt.Errorf("parse Drive relay session expiry: %w", parseErr)
	}
	return until, true, nil
}

// GetSession reads one session row. It returns (nil, nil) when absent, so the
// relay can treat "no session yet" and "no error" identically.
func (s *SessionStore) GetSession(ctx context.Context, uploadID, destinationID, publicationID string) (*Session, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT upload_id,artifact_id,destination_id,publication_id,folder_id,session_uri,part_size,next_offset,in_flight,state,COALESCE(remote_id,''),COALESCE(remote_url,'')
		FROM drive_relay_sessions WHERE upload_id=? AND destination_id=? AND publication_id=?`, uploadID, destinationID, publicationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, rows.Err()
	}
	sess, err := scanSession(rows)
	if err != nil {
		return nil, err
	}
	return &sess, nil
}

// ListSessions reads the active (non-FAILED, non-VERIFIED) sessions of an
// upload in stable destination/publication order.
func (s *SessionStore) ListSessions(ctx context.Context, uploadID string) ([]Session, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT upload_id,artifact_id,destination_id,publication_id,folder_id,session_uri,part_size,next_offset,in_flight,state,COALESCE(remote_id,''),COALESCE(remote_url,'')
		FROM drive_relay_sessions WHERE upload_id=? AND state NOT IN ('FAILED','VERIFIED') ORDER BY destination_id,publication_id`, uploadID)
	if err != nil {
		return nil, fmt.Errorf("load Drive relay sessions: %w", err)
	}
	defer rows.Close()
	var out []Session
	for rows.Next() {
		sess, err := scanSession(rows)
		if err != nil {
			return nil, fmt.Errorf("scan Drive relay session: %w", err)
		}
		out = append(out, sess)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate Drive relay sessions: %w", err)
	}
	return out, nil
}

// SaveSession upserts a PREPARED session: any prior progress, remote identity
// or verification evidence for the same (upload, destination, publication)
// key is reset, because a new resumable URI invalidates the old one. job_id
// is backfilled from artifact_uploads so the row stays self-contained.
func (s *SessionStore) SaveSession(ctx context.Context, sess Session) error {
	now := time.Now().UTC()
	if _, err := s.db.ExecContext(ctx, `INSERT INTO drive_relay_sessions
		(upload_id,artifact_id,job_id,destination_id,publication_id,folder_id,session_uri,expires_at,part_size,state,created_at,updated_at)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(upload_id,destination_id,publication_id) DO UPDATE SET
		artifact_id=excluded.artifact_id,job_id=excluded.job_id,folder_id=excluded.folder_id,session_uri=excluded.session_uri,
		expires_at=excluded.expires_at,part_size=excluded.part_size,next_offset=0,in_flight=0,state='PREPARED',remote_id=NULL,remote_url=NULL,
		verified_sha256=NULL,verified_size=NULL,last_error=NULL,updated_at=excluded.updated_at`,
		sess.UploadID, sess.ArtifactID, "", sess.DestinationID, sess.PublicationID, sess.FolderID, sess.SessionURI,
		now.Add(sessionLifetime).Format(time.RFC3339Nano), sess.PartSize, sess.State, now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano)); err != nil {
		return err
	}
	// job_id is filled from artifact_uploads so the row remains self-contained.
	_, err := s.db.ExecContext(ctx, `UPDATE drive_relay_sessions SET job_id=(SELECT job_id FROM artifact_uploads WHERE upload_id=?) WHERE upload_id=? AND destination_id=? AND publication_id=?`,
		sess.UploadID, sess.UploadID, sess.DestinationID, sess.PublicationID)
	return err
}

// MarkInFlight claims a session for one part send with a CAS on the idle
// flag. A second concurrent claim for the same offset fails closed.
func (s *SessionStore) MarkInFlight(ctx context.Context, sess Session) error {
	result, err := s.db.ExecContext(ctx, `UPDATE drive_relay_sessions SET in_flight=1,updated_at=? WHERE upload_id=? AND destination_id=? AND publication_id=? AND state IN ('PREPARED','UPLOADING') AND next_offset=? AND in_flight=0`,
		time.Now().UTC().Format(time.RFC3339Nano), sess.UploadID, sess.DestinationID, sess.PublicationID, sess.NextOffset)
	if err != nil {
		return fmt.Errorf("mark Drive relay part in flight: %w", err)
	}
	n, err := result.RowsAffected()
	if err != nil || n != 1 {
		return fmt.Errorf("Drive relay session changed before part send")
	}
	return nil
}

// SaveProgress persists the post-part state: next offset, flight flag, state
// and remote identity. Empty strings clear the nullable identity columns.
func (s *SessionStore) SaveProgress(ctx context.Context, sess Session, next int64, inFlight bool, state, remoteID, remoteURL, lastError string) error {
	flight := 0
	if inFlight {
		flight = 1
	}
	_, err := s.db.ExecContext(ctx, `UPDATE drive_relay_sessions SET next_offset=?,in_flight=?,state=?,remote_id=NULLIF(?,'') ,remote_url=NULLIF(?,'') ,last_error=NULLIF(?,'') ,updated_at=?
		WHERE upload_id=? AND destination_id=? AND publication_id=?`,
		next, flight, state, remoteID, remoteURL, lastError, time.Now().UTC().Format(time.RFC3339Nano), sess.UploadID, sess.DestinationID, sess.PublicationID)
	if err != nil {
		return fmt.Errorf("persist Drive relay progress: %w", err)
	}
	return nil
}

// FailSession marks a session FAILED unless it already reached a terminal
// (VERIFIED / UPLOADED_UNVERIFIED) state; the cause becomes last_error.
func (s *SessionStore) FailSession(ctx context.Context, sess Session, cause error) error {
	_, err := s.db.ExecContext(ctx, `UPDATE drive_relay_sessions SET state='FAILED',last_error=?,updated_at=? WHERE upload_id=? AND destination_id=? AND publication_id=? AND state NOT IN ('VERIFIED','UPLOADED_UNVERIFIED')`,
		cause.Error(), time.Now().UTC().Format(time.RFC3339Nano), sess.UploadID, sess.DestinationID, sess.PublicationID)
	return err
}

// MarkVerified flips every UPLOADED_UNVERIFIED session of the upload whose
// next_offset matches the verified size. It returns the number of updated
// rows so the relay can fail closed when nothing matched.
func (s *SessionStore) MarkVerified(ctx context.Context, uploadID, sha256Hex string, sizeBytes int64) (int64, error) {
	result, err := s.db.ExecContext(ctx, `UPDATE drive_relay_sessions
		SET state='VERIFIED', verified_sha256=?, verified_size=?, updated_at=?
		WHERE upload_id=? AND state='UPLOADED_UNVERIFIED' AND next_offset=? AND remote_id IS NOT NULL`,
		sha256Hex, sizeBytes, time.Now().UTC().Format(time.RFC3339Nano), uploadID, sizeBytes)
	if err != nil {
		return 0, fmt.Errorf("verify Drive relay sessions: %w", err)
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("count verified Drive relay sessions: %w", err)
	}
	return updated, nil
}

// AbortSessions marks every non-terminal session of an upload FAILED after a
// master-stream abort, clearing the session URI.
func (s *SessionStore) AbortSessions(ctx context.Context, uploadID string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE drive_relay_sessions SET state='FAILED', session_uri='', last_error='master stream aborted', updated_at=? WHERE upload_id=? AND state NOT IN ('VERIFIED','FAILED')`,
		time.Now().UTC().Format(time.RFC3339Nano), uploadID)
	if err != nil {
		return fmt.Errorf("abort Drive relay sessions: %w", err)
	}
	return nil
}

// GetVerifiedEvidence reads the newest VERIFIED evidence row for an artifact
// at one destination/publication. It returns (nil, nil) when none exists.
func (s *SessionStore) GetVerifiedEvidence(ctx context.Context, artifactID, destinationID, publicationID string) (*Evidence, error) {
	var out Evidence
	err := s.db.QueryRowContext(ctx, `SELECT remote_id, COALESCE(remote_url,''), folder_id, verified_sha256, verified_size
		FROM drive_relay_sessions WHERE artifact_id=? AND destination_id=? AND publication_id=? AND state='VERIFIED'
		ORDER BY updated_at DESC LIMIT 1`, artifactID, destinationID, publicationID).
		Scan(&out.RemoteID, &out.RemoteURL, &out.FolderID, &out.SHA256, &out.SizeBytes)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read verified Drive relay evidence: %w", err)
	}
	out.ArtifactID, out.DestinationID, out.PublicationID = artifactID, destinationID, publicationID
	return &out, nil
}

func scanSession(rows *sql.Rows) (Session, error) {
	var s Session
	var inFlight int
	if err := rows.Scan(&s.UploadID, &s.ArtifactID, &s.DestinationID, &s.PublicationID, &s.FolderID, &s.SessionURI, &s.PartSize, &s.NextOffset, &inFlight, &s.State, &s.RemoteID, &s.RemoteURL); err != nil {
		return Session{}, err
	}
	s.InFlight = inFlight != 0
	return s, nil
}
