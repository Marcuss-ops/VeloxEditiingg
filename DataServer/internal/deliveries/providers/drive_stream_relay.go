package providers

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"velox-server/internal/deliveries"
	driveapi "velox-server/internal/integrations/drive"
	"velox-server/internal/repository"
)

const driveRelaySessionLifetime = 7 * 24 * time.Hour

// DriveStreamRelay forwards only durable master-stream chunks to Drive. It
// owns no worker credentials; session URIs remain in the server database.
type DriveStreamRelay struct {
	db        *sql.DB
	uploads   repository.UploadRepository
	blobStore repository.BlobStore
	drive     *driveapi.Service
	partSize  int64
	locks     relayLockPool
}

type relaySession struct {
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

func NewDriveStreamRelay(db *sql.DB, uploads repository.UploadRepository, blobStore repository.BlobStore, driveService *driveapi.Service, partSize int64) (*DriveStreamRelay, error) {
	if db == nil || uploads == nil || blobStore == nil || driveService == nil || partSize < 256*1024 || partSize%(256*1024) != 0 {
		return nil, fmt.Errorf("drive relay requires database, upload repository, blob store, Drive service and a 256-KiB-aligned part size")
	}
	return &DriveStreamRelay{db: db, uploads: uploads, blobStore: blobStore, drive: driveService, partSize: partSize}, nil
}

var _ deliveries.DriveStreamRelay = (*DriveStreamRelay)(nil)
var _ deliveries.DriveRelayEvidenceReader = (*DriveStreamRelay)(nil)

func (r *DriveStreamRelay) PrepareArtifact(ctx context.Context, uploadID, artifactID, jobID string) error {
	return r.withUploadLock(uploadID, func() error {
		return r.prepareArtifactLocked(ctx, uploadID, artifactID, jobID)
	})
}

func (r *DriveStreamRelay) prepareArtifactLocked(ctx context.Context, uploadID, artifactID, jobID string) error {
	if r == nil || r.db == nil || r.drive == nil || strings.TrimSpace(uploadID) == "" || strings.TrimSpace(artifactID) == "" || strings.TrimSpace(jobID) == "" {
		return fmt.Errorf("drive relay prepare requires upload, artifact and job identities")
	}
	upload, err := r.uploads.GetUploadSession(ctx, uploadID)
	if err != nil {
		return fmt.Errorf("read relay artifact upload: %w", err)
	}
	if upload == nil || upload.ArtifactID != artifactID || upload.JobID != jobID {
		return fmt.Errorf("drive relay upload identity mismatch")
	}
	if !strings.Contains(strings.ToLower(upload.ExpectedMIME), "video/") && !strings.Contains(strings.ToLower(upload.Kind), "video") && upload.Kind != "final_video" {
		return nil
	}
	type target struct {
		destinationID string
		publicationID string
		parentFolder  string
		metadata      string
		videoTitle    string
		requestJSON   string
	}
	rows, err := r.db.QueryContext(ctx, `
		SELECT jdp.destination_id, COALESCE(jdp.publication_id,''), COALESCE(dd.folder_id,''),
		       COALESCE(jdp.metadata_json,'{}'), COALESCE(j.video_name,''), COALESCE(j.request_json,'')
		FROM job_delivery_plans jdp
		JOIN delivery_destinations dd ON dd.destination_id=jdp.destination_id
		JOIN jobs j ON j.job_id=jdp.job_id
		WHERE jdp.job_id=? AND jdp.enabled=1 AND dd.enabled=1
		  AND lower(dd.provider) IN ('drive','google_drive')
		ORDER BY jdp.priority ASC, jdp.destination_id ASC`, jobID)
	if err != nil {
		return fmt.Errorf("load Drive relay destinations: %w", err)
	}
	var targets []target
	for rows.Next() {
		var t target
		if err := rows.Scan(&t.destinationID, &t.publicationID, &t.parentFolder, &t.metadata, &t.videoTitle, &t.requestJSON); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan Drive relay destination: %w", err)
		}
		targets = append(targets, t)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("read Drive relay destinations: %w", err)
	}
	_ = rows.Close()
	for _, t := range targets {
		parent := driveRelayFolderID(t.parentFolder, t.metadata)
		if parent == "" {
			continue
		}
		existing, lookupErr := r.getSession(ctx, uploadID, t.destinationID, t.publicationID)
		if lookupErr != nil {
			return fmt.Errorf("read existing Drive relay session: %w", lookupErr)
		}
		if existing != nil {
			if existing.State != "FAILED" && existing.SessionURI != "" {
				var expires string
				if e := r.db.QueryRowContext(ctx, `SELECT expires_at FROM drive_relay_sessions WHERE upload_id=? AND destination_id=? AND publication_id=?`, uploadID, t.destinationID, t.publicationID).Scan(&expires); e == nil {
					if until, parseErr := time.Parse(time.RFC3339Nano, expires); parseErr == nil && time.Now().Before(until) {
						continue
					}
				}
			}
		}
		folder, err := r.drive.GetOrCreateFolder(ctx, artifactID, parent)
		if err != nil {
			return fmt.Errorf("create Drive relay project folder: %w", err)
		}
		title := relayTitle(t.videoTitle, t.requestJSON, t.metadata)
		if strings.TrimSpace(title) == "" {
			slog.Warn("Drive relay using opaque artifact ID as filename",
				"job_id", jobID, "video_name", t.videoTitle, "artifact_id", artifactID)
		}
		fileName := relayFileName(title, artifactID)
		sessionURI, err := r.drive.InitiateRelaySession(ctx, fileName, folder.ID, artifactID, t.destinationID, t.publicationID)
		if err != nil {
			return fmt.Errorf("initialize Drive relay session: %w", err)
		}
		if err := r.saveSession(ctx, relaySession{
			UploadID: uploadID, ArtifactID: artifactID, DestinationID: t.destinationID,
			PublicationID: t.publicationID, FolderID: folder.ID, SessionURI: sessionURI,
			PartSize: r.partSize, State: "PREPARED",
		}); err != nil {
			return fmt.Errorf("persist Drive relay session: %w", err)
		}
	}
	return nil
}

func relayTitle(videoTitle, requestJSON, metadataJSON string) string {
	var metadata map[string]any
	if json.Unmarshal([]byte(metadataJSON), &metadata) == nil {
		for _, key := range []string{"file_name", "title"} {
			if name, ok := metadata[key].(string); ok && strings.TrimSpace(name) != "" {
				if resolved := repository.ResolveArtifactTitle(strings.TrimSpace(name), requestJSON); resolved != "" {
					return resolved
				}
			}
		}
	}
	return repository.ResolveArtifactTitle(videoTitle, requestJSON)
}

func (r *DriveStreamRelay) RelayAvailableChunks(ctx context.Context, uploadID string) error {
	return r.withUploadLock(uploadID, func() error {
		chunks, err := r.uploads.ListChunks(ctx, uploadID)
		if err != nil {
			return fmt.Errorf("list durable relay chunks: %w", err)
		}
		rows, err := r.listSessions(ctx, uploadID)
		if err != nil {
			return err
		}
		for _, session := range rows {
			if session.State == "FAILED" || session.State == "VERIFIED" || session.State == "UPLOADED_UNVERIFIED" {
				continue
			}
			if err := r.reconcileInFlight(ctx, &session, 0); err != nil {
				_ = r.failSession(ctx, session, err)
				continue
			}
			for {
				chunk, next, chunkStart, ok := relayChunkAtOffset(chunks, session.NextOffset)
				if !ok || next == nil { // last chunk may still be growing, so hold it until CompleteRelay.
					break
				}
				if err := r.sendChunk(ctx, session, chunk, chunkStart, 0, false); err != nil {
					_ = r.failSession(ctx, session, err)
					break
				}
			}
		}
		return nil
	})
}

func (r *DriveStreamRelay) CompleteRelay(ctx context.Context, uploadID string) error {
	return r.withUploadLock(uploadID, func() error {
		chunks, err := r.uploads.ListChunks(ctx, uploadID)
		if err != nil {
			return fmt.Errorf("list final relay chunks: %w", err)
		}
		if len(chunks) == 0 {
			return nil
		}
		var total int64
		for i, c := range chunks {
			if c.ChunkIndex != i {
				return fmt.Errorf("Drive relay cannot complete with missing chunk %d", i)
			}
			total += c.SizeBytes
		}
		rows, err := r.listSessions(ctx, uploadID)
		if err != nil {
			return err
		}
		for _, session := range rows {
			if session.State == "FAILED" || session.State == "VERIFIED" || session.State == "UPLOADED_UNVERIFIED" {
				continue
			}
			if err := r.reconcileInFlight(ctx, &session, total); err != nil {
				_ = r.failSession(ctx, session, err)
				continue
			}
			for session.NextOffset < total {
				chunk, _, chunkStart, ok := relayChunkAtOffset(chunks, session.NextOffset)
				if !ok {
					err := fmt.Errorf("Drive relay offset %d does not align to a persisted chunk", session.NextOffset)
					_ = r.failSession(ctx, session, err)
					break
				}
				last := chunkStart+chunk.SizeBytes == total
				if err := r.sendChunk(ctx, session, chunk, chunkStart, total, last); err != nil {
					_ = r.failSession(ctx, session, err)
					break
				}
			}
		}
		return nil
	})
}

func (r *DriveStreamRelay) VerifyRelay(ctx context.Context, uploadID, sha256Hex string, sizeBytes int64) error {
	if len(sha256Hex) != 64 || sizeBytes <= 0 {
		return fmt.Errorf("Drive relay verification requires a SHA-256 and positive size")
	}
	_, err := hex.DecodeString(sha256Hex)
	if err != nil {
		return fmt.Errorf("Drive relay verification SHA-256 is invalid")
	}
	result, err := r.db.ExecContext(ctx, `UPDATE drive_relay_sessions
		SET state='VERIFIED', verified_sha256=?, verified_size=?, updated_at=?
		WHERE upload_id=? AND state='UPLOADED_UNVERIFIED' AND next_offset=? AND remote_id IS NOT NULL`,
		strings.ToLower(sha256Hex), sizeBytes, time.Now().UTC().Format(time.RFC3339Nano), uploadID, sizeBytes)
	if err != nil {
		return fmt.Errorf("verify Drive relay sessions: %w", err)
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("count verified Drive relay sessions: %w", err)
	}
	if updated == 0 {
		return fmt.Errorf("no completed Drive relay session matched verified artifact size %d", sizeBytes)
	}
	return nil
}

func (r *DriveStreamRelay) AbortRelay(ctx context.Context, uploadID string) error {
	_, err := r.db.ExecContext(ctx, `UPDATE drive_relay_sessions SET state='FAILED', session_uri='', last_error='master stream aborted', updated_at=? WHERE upload_id=? AND state NOT IN ('VERIFIED','FAILED')`, time.Now().UTC().Format(time.RFC3339Nano), uploadID)
	if err != nil {
		return fmt.Errorf("abort Drive relay sessions: %w", err)
	}
	return nil
}

func (r *DriveStreamRelay) GetVerifiedDriveRelay(ctx context.Context, artifactID, destinationID, publicationID string) (*deliveries.DriveRelayEvidence, error) {
	var out deliveries.DriveRelayEvidence
	err := r.db.QueryRowContext(ctx, `SELECT remote_id, COALESCE(remote_url,''), folder_id, verified_sha256, verified_size
		FROM drive_relay_sessions WHERE artifact_id=? AND destination_id=? AND publication_id=? AND state='VERIFIED'
		ORDER BY updated_at DESC LIMIT 1`, artifactID, destinationID, publicationID).
		Scan(&out.RemoteID, &out.RemoteURL, &out.FolderID, &out.SHA256, &out.SizeBytes)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read verified Drive relay evidence: %w", err)
	}
	out.ArtifactID, out.DestinationID, out.PublicationID = artifactID, destinationID, publicationID
	return &out, nil
}

func (r *DriveStreamRelay) sendChunk(ctx context.Context, session relaySession, chunk repository.ChunkRecord, chunkStart, total int64, final bool) error {
	if chunk.StorageKey == "" || chunk.SizeBytes <= 0 {
		return fmt.Errorf("Drive relay chunk %d has no durable bytes", chunk.ChunkIndex)
	}
	file, err := r.blobStore.OpenStagedRead(chunk.StorageKey)
	if err != nil {
		return fmt.Errorf("open durable Drive relay chunk %d: %w", chunk.ChunkIndex, err)
	}
	data, readErr := io.ReadAll(file)
	_ = file.Close()
	if readErr != nil {
		return fmt.Errorf("read durable Drive relay chunk %d: %w", chunk.ChunkIndex, readErr)
	}
	if int64(len(data)) != chunk.SizeBytes {
		return fmt.Errorf("Drive relay chunk %d size drift: have %d want %d", chunk.ChunkIndex, len(data), chunk.SizeBytes)
	}
	h := sha256.Sum256(data)
	if chunk.SHA256 != "" && !strings.EqualFold(hex.EncodeToString(h[:]), chunk.SHA256) {
		return fmt.Errorf("Drive relay chunk %d failed its durable SHA-256 check", chunk.ChunkIndex)
	}
	start := session.NextOffset
	inside := start - chunkStart
	if inside < 0 || inside >= int64(len(data)) {
		return fmt.Errorf("Drive relay offset %d is outside chunk %d", start, chunk.ChunkIndex)
	}
	data = data[inside:]
	if err := r.setInFlight(ctx, session); err != nil {
		return err
	}
	partTotal := int64(0)
	if final {
		partTotal = total
	}
	next, completed, err := r.drive.UploadResumablePart(ctx, session.SessionURI, start, partTotal, data)
	if err != nil {
		return err
	}
	if next <= start || next > start+int64(len(data)) {
		return fmt.Errorf("Drive relay made invalid offset progress %d -> %d", start, next)
	}
	state := "UPLOADING"
	remoteID, remoteURL := session.RemoteID, session.RemoteURL
	if completed != nil {
		state, remoteID, remoteURL = "UPLOADED_UNVERIFIED", completed.FileID, completed.WebViewLink
	}
	if err := r.saveProgress(ctx, session, next, false, state, remoteID, remoteURL, ""); err != nil {
		return err
	}
	session.NextOffset, session.InFlight = next, false
	session.State, session.RemoteID, session.RemoteURL = state, remoteID, remoteURL
	return nil
}

func (r *DriveStreamRelay) reconcileInFlight(ctx context.Context, session *relaySession, total int64) error {
	if !session.InFlight {
		return nil
	}
	next, completed, err := r.drive.QueryResumableUploadOffset(ctx, session.SessionURI, total)
	if err != nil {
		return err
	}
	state := "UPLOADING"
	remoteID, remoteURL := "", ""
	if completed != nil {
		state, remoteID, remoteURL = "UPLOADED_UNVERIFIED", completed.FileID, completed.WebViewLink
	}
	if err := r.saveProgress(ctx, *session, next, false, state, remoteID, remoteURL, ""); err != nil {
		return err
	}
	session.NextOffset, session.InFlight = next, false
	session.State, session.RemoteID, session.RemoteURL = state, remoteID, remoteURL
	return nil
}

func (r *DriveStreamRelay) getSession(ctx context.Context, uploadID, destinationID, publicationID string) (*relaySession, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT upload_id,artifact_id,destination_id,publication_id,folder_id,session_uri,part_size,next_offset,in_flight,state,COALESCE(remote_id,''),COALESCE(remote_url,'')
		FROM drive_relay_sessions WHERE upload_id=? AND destination_id=? AND publication_id=?`, uploadID, destinationID, publicationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, rows.Err()
	}
	var s relaySession
	var inFlight int
	if err := rows.Scan(&s.UploadID, &s.ArtifactID, &s.DestinationID, &s.PublicationID, &s.FolderID, &s.SessionURI, &s.PartSize, &s.NextOffset, &inFlight, &s.State, &s.RemoteID, &s.RemoteURL); err != nil {
		return nil, err
	}
	s.InFlight = inFlight != 0
	return &s, nil
}

func (r *DriveStreamRelay) listSessions(ctx context.Context, uploadID string) ([]relaySession, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT upload_id,artifact_id,destination_id,publication_id,folder_id,session_uri,part_size,next_offset,in_flight,state,COALESCE(remote_id,''),COALESCE(remote_url,'')
		FROM drive_relay_sessions WHERE upload_id=? AND state NOT IN ('FAILED','VERIFIED') ORDER BY destination_id,publication_id`, uploadID)
	if err != nil {
		return nil, fmt.Errorf("load Drive relay sessions: %w", err)
	}
	defer rows.Close()
	var out []relaySession
	for rows.Next() {
		var s relaySession
		var inFlight int
		if err := rows.Scan(&s.UploadID, &s.ArtifactID, &s.DestinationID, &s.PublicationID, &s.FolderID, &s.SessionURI, &s.PartSize, &s.NextOffset, &inFlight, &s.State, &s.RemoteID, &s.RemoteURL); err != nil {
			return nil, fmt.Errorf("scan Drive relay session: %w", err)
		}
		s.InFlight = inFlight != 0
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate Drive relay sessions: %w", err)
	}
	return out, nil
}

func (r *DriveStreamRelay) saveSession(ctx context.Context, s relaySession) error {
	now := time.Now().UTC()
	_, err := r.db.ExecContext(ctx, `INSERT INTO drive_relay_sessions
		(upload_id,artifact_id,job_id,destination_id,publication_id,folder_id,session_uri,expires_at,part_size,state,created_at,updated_at)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(upload_id,destination_id,publication_id) DO UPDATE SET
		artifact_id=excluded.artifact_id,job_id=excluded.job_id,folder_id=excluded.folder_id,session_uri=excluded.session_uri,
		expires_at=excluded.expires_at,part_size=excluded.part_size,next_offset=0,in_flight=0,state='PREPARED',remote_id=NULL,remote_url=NULL,
		verified_sha256=NULL,verified_size=NULL,last_error=NULL,updated_at=excluded.updated_at`,
		s.UploadID, s.ArtifactID, "", s.DestinationID, s.PublicationID, s.FolderID, s.SessionURI,
		now.Add(driveRelaySessionLifetime).Format(time.RFC3339Nano), s.PartSize, s.State, now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano))
	if err != nil {
		return err
	}
	// job_id is filled from artifact_uploads so the row remains self-contained.
	_, err = r.db.ExecContext(ctx, `UPDATE drive_relay_sessions SET job_id=(SELECT job_id FROM artifact_uploads WHERE upload_id=?) WHERE upload_id=? AND destination_id=? AND publication_id=?`, s.UploadID, s.UploadID, s.DestinationID, s.PublicationID)
	return err
}

func (r *DriveStreamRelay) setInFlight(ctx context.Context, s relaySession) error {
	result, err := r.db.ExecContext(ctx, `UPDATE drive_relay_sessions SET in_flight=1,updated_at=? WHERE upload_id=? AND destination_id=? AND publication_id=? AND state IN ('PREPARED','UPLOADING') AND next_offset=? AND in_flight=0`, time.Now().UTC().Format(time.RFC3339Nano), s.UploadID, s.DestinationID, s.PublicationID, s.NextOffset)
	if err != nil {
		return fmt.Errorf("mark Drive relay part in flight: %w", err)
	}
	n, err := result.RowsAffected()
	if err != nil || n != 1 {
		return fmt.Errorf("Drive relay session changed before part send")
	}
	return nil
}

func (r *DriveStreamRelay) saveProgress(ctx context.Context, s relaySession, next int64, inFlight bool, state, remoteID, remoteURL, lastError string) error {
	flight := 0
	if inFlight {
		flight = 1
	}
	_, err := r.db.ExecContext(ctx, `UPDATE drive_relay_sessions SET next_offset=?,in_flight=?,state=?,remote_id=NULLIF(?,'') ,remote_url=NULLIF(?,'') ,last_error=NULLIF(?,'') ,updated_at=?
		WHERE upload_id=? AND destination_id=? AND publication_id=?`, next, flight, state, remoteID, remoteURL, lastError, time.Now().UTC().Format(time.RFC3339Nano), s.UploadID, s.DestinationID, s.PublicationID)
	if err != nil {
		return fmt.Errorf("persist Drive relay progress: %w", err)
	}
	return nil
}

func (r *DriveStreamRelay) failSession(ctx context.Context, s relaySession, cause error) error {
	_, err := r.db.ExecContext(ctx, `UPDATE drive_relay_sessions SET state='FAILED',last_error=?,updated_at=? WHERE upload_id=? AND destination_id=? AND publication_id=? AND state NOT IN ('VERIFIED','UPLOADED_UNVERIFIED')`, cause.Error(), time.Now().UTC().Format(time.RFC3339Nano), s.UploadID, s.DestinationID, s.PublicationID)
	return err
}

func relayChunkAtOffset(chunks []repository.ChunkRecord, offset int64) (repository.ChunkRecord, *repository.ChunkRecord, int64, bool) {
	var current, next *repository.ChunkRecord
	var pos int64
	for i := range chunks {
		c := &chunks[i]
		if c.ChunkIndex < 0 || c.SizeBytes <= 0 {
			return repository.ChunkRecord{}, nil, 0, false
		}
		if pos <= offset && offset < pos+c.SizeBytes {
			current = c
			if i+1 < len(chunks) && chunks[i+1].ChunkIndex == c.ChunkIndex+1 {
				next = &chunks[i+1]
			}
			return *current, next, pos, true
		}
		pos += c.SizeBytes
	}
	return repository.ChunkRecord{}, nil, 0, false
}

func (r *DriveStreamRelay) withUploadLock(uploadID string, fn func() error) error {
	release := r.locks.acquire(uploadID)
	defer release()
	return fn()
}

type relayLockPool struct {
	mu      sync.Mutex
	entries map[string]*relayLockEntry
}

type relayLockEntry struct {
	mu   sync.Mutex
	refs int
}

func (p *relayLockPool) acquire(key string) func() {
	p.mu.Lock()
	if p.entries == nil {
		p.entries = make(map[string]*relayLockEntry)
	}
	e := p.entries[key]
	if e == nil {
		e = &relayLockEntry{}
		p.entries[key] = e
	}
	e.refs++
	p.mu.Unlock()
	e.mu.Lock()
	return func() {
		e.mu.Unlock()
		p.mu.Lock()
		e.refs--
		if e.refs == 0 {
			delete(p.entries, key)
		}
		p.mu.Unlock()
	}
}

func relayFileName(title, fallback string) string {
	name := strings.TrimSpace(title)
	if name == "" {
		name = fallback
	}
	name = filepath.Base(strings.ReplaceAll(name, `\`, "/"))
	if !strings.EqualFold(filepath.Ext(name), ".mp4") {
		name += ".mp4"
	}
	return name
}

func driveRelayFolderID(fallback, metadataJSON string) string {
	folder := strings.TrimSpace(fallback)
	var metadata map[string]interface{}
	if json.Unmarshal([]byte(metadataJSON), &metadata) == nil {
		if requested, ok := metadata["folder_id"].(string); ok && strings.TrimSpace(requested) != "" {
			folder = strings.TrimSpace(requested)
		}
	}
	if parsed, err := url.Parse(folder); err == nil && parsed.Host != "" {
		parts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
		for i := 0; i+1 < len(parts); i++ {
			if parts[i] == "folders" {
				folder = parts[i+1]
				break
			}
		}
	}
	if query := strings.IndexByte(folder, '?'); query >= 0 {
		folder = folder[:query]
	}
	return strings.Trim(folder, "/")
}
