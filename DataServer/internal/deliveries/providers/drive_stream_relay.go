package providers

import (
	"context"
	"crypto/sha256"
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
	"velox-server/internal/store/deliveryrelay"
	"velox-shared/paths"
)

// DriveStreamRelay forwards only durable master-stream chunks to Drive. It
// owns no worker credentials; session URIs remain in the server database.
// Persistence lives in the store leaf internal/store/deliveryrelay (the
// canonical SQL gateway); this type owns chunking, resumable-upload policy
// and the per-upload lock pool.
type DriveStreamRelay struct {
	sessions  *deliveryrelay.SessionStore
	uploads   repository.UploadRepository
	blobStore repository.BlobStore
	drive     *driveapi.Service
	partSize  int64
	locks     relayLockPool
}

func NewDriveStreamRelay(sessions *deliveryrelay.SessionStore, uploads repository.UploadRepository, blobStore repository.BlobStore, driveService *driveapi.Service, partSize int64) (*DriveStreamRelay, error) {
	if sessions == nil || uploads == nil || blobStore == nil || driveService == nil || partSize < 256*1024 || partSize%(256*1024) != 0 {
		return nil, fmt.Errorf("drive relay requires session store, upload repository, blob store, Drive service and a 256-KiB-aligned part size")
	}
	return &DriveStreamRelay{sessions: sessions, uploads: uploads, blobStore: blobStore, drive: driveService, partSize: partSize}, nil
}

var _ deliveries.DriveStreamRelay = (*DriveStreamRelay)(nil)
var _ deliveries.DriveRelayEvidenceReader = (*DriveStreamRelay)(nil)

func (r *DriveStreamRelay) PrepareArtifact(ctx context.Context, uploadID, artifactID, jobID string) error {
	return r.withUploadLock(uploadID, func() error {
		return r.prepareArtifactLocked(ctx, uploadID, artifactID, jobID)
	})
}

func (r *DriveStreamRelay) prepareArtifactLocked(ctx context.Context, uploadID, artifactID, jobID string) error {
	if r == nil || r.drive == nil || strings.TrimSpace(uploadID) == "" || strings.TrimSpace(artifactID) == "" || strings.TrimSpace(jobID) == "" {
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
	targets, err := r.sessions.LoadDestinations(ctx, jobID)
	if err != nil {
		return fmt.Errorf("load Drive relay destinations: %w", err)
	}
	for _, t := range targets {
		parent := driveRelayFolderID(t.ParentFolder, t.Metadata)
		if parent == "" {
			continue
		}
		existing, lookupErr := r.sessions.GetSession(ctx, uploadID, t.DestinationID, t.PublicationID)
		if lookupErr != nil {
			return fmt.Errorf("read existing Drive relay session: %w", lookupErr)
		}
		if existing != nil {
			if existing.State != "FAILED" && existing.SessionURI != "" {
				if until, ok, expErr := r.sessions.SessionExpiresAt(ctx, uploadID, t.DestinationID, t.PublicationID); expErr == nil && ok && time.Now().UTC().Before(until) {
					continue
				}
			}
		}
		title := relayTitle(t.VideoTitle, t.RequestJSON, t.Metadata)
		// Folder naming mirrors the MP4 naming (relayFileName): the Drive
		// subfolder takes the human-readable video title instead of the
		// opaque artifact id, so both land under the same online name.
		folderName := relayFolderName(title, artifactID)
		folder, err := r.drive.GetOrCreateFolder(ctx, folderName, parent)
		if err != nil {
			return fmt.Errorf("create Drive relay project folder: %w", err)
		}
		if strings.TrimSpace(title) == "" {
			slog.Warn("Drive relay has no submitted title; using a job-based filename",
				"job_id", jobID, "video_name", t.VideoTitle, "artifact_id", artifactID)
			title = "Video " + jobID
		}
		fileName := relayFileName(title, artifactID)
		sessionURI, err := r.drive.InitiateRelaySession(ctx, fileName, folder.ID, artifactID, t.DestinationID, t.PublicationID)
		if err != nil {
			return fmt.Errorf("initialize Drive relay session: %w", err)
		}
		if err := r.sessions.SaveSession(ctx, deliveryrelay.Session{
			UploadID: uploadID, ArtifactID: artifactID, DestinationID: t.DestinationID,
			PublicationID: t.PublicationID, FolderID: folder.ID, SessionURI: sessionURI,
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
		rows, err := r.sessions.ListSessions(ctx, uploadID)
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
		rows, err := r.sessions.ListSessions(ctx, uploadID)
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
	if _, err := hex.DecodeString(sha256Hex); err != nil {
		return fmt.Errorf("Drive relay verification SHA-256 is invalid")
	}
	updated, err := r.sessions.MarkVerified(ctx, uploadID, strings.ToLower(sha256Hex), sizeBytes)
	if err != nil {
		return err
	}
	if updated == 0 {
		return fmt.Errorf("no completed Drive relay session matched verified artifact size %d", sizeBytes)
	}
	return nil
}

func (r *DriveStreamRelay) AbortRelay(ctx context.Context, uploadID string) error {
	return r.sessions.AbortSessions(ctx, uploadID)
}

func (r *DriveStreamRelay) GetVerifiedDriveRelay(ctx context.Context, artifactID, destinationID, publicationID string) (*deliveries.DriveRelayEvidence, error) {
	ev, err := r.sessions.GetVerifiedEvidence(ctx, artifactID, destinationID, publicationID)
	if err != nil || ev == nil {
		return nil, err
	}
	return &deliveries.DriveRelayEvidence{
		ArtifactID:    ev.ArtifactID,
		DestinationID: ev.DestinationID,
		PublicationID: ev.PublicationID,
		RemoteID:      ev.RemoteID,
		RemoteURL:     ev.RemoteURL,
		FolderID:      ev.FolderID,
		SHA256:        ev.SHA256,
		SizeBytes:     ev.SizeBytes,
	}, nil
}

func (r *DriveStreamRelay) sendChunk(ctx context.Context, session deliveryrelay.Session, chunk repository.ChunkRecord, chunkStart, total int64, final bool) error {
	if chunk.StorageKey == "" || chunk.SizeBytes <= 0 {
		return fmt.Errorf("Drive relay chunk %d has no durable bytes", chunk.ChunkIndex)
	}
	file, err := r.blobStore.OpenStagedRead(chunk.StorageKey)
	if err != nil {
		return fmt.Errorf("open durable Drive relay chunk %d: %w", chunk.ChunkIndex, err)
	}
	info, statErr := file.Stat()
	if statErr != nil {
		_ = file.Close()
		return fmt.Errorf("stat durable Drive relay chunk %d: %w", chunk.ChunkIndex, statErr)
	}
	if info.Size() != chunk.SizeBytes {
		_ = file.Close()
		return fmt.Errorf("Drive relay chunk %d size drift: have %d want %d", chunk.ChunkIndex, info.Size(), chunk.SizeBytes)
	}
	h := sha256.New()
	if _, err := io.Copy(h, file); err != nil {
		_ = file.Close()
		return fmt.Errorf("hash durable Drive relay chunk %d: %w", chunk.ChunkIndex, err)
	}
	if chunk.SHA256 != "" && !strings.EqualFold(hex.EncodeToString(h.Sum(nil)), chunk.SHA256) {
		_ = file.Close()
		return fmt.Errorf("Drive relay chunk %d failed its durable SHA-256 check", chunk.ChunkIndex)
	}
	start := session.NextOffset
	inside := start - chunkStart
	if inside < 0 || inside >= chunk.SizeBytes {
		_ = file.Close()
		return fmt.Errorf("Drive relay offset %d is outside chunk %d", start, chunk.ChunkIndex)
	}
	partSize := chunk.SizeBytes - inside
	if err := r.sessions.MarkInFlight(ctx, session); err != nil {
		_ = file.Close()
		return err
	}
	partTotal := int64(0)
	if final {
		partTotal = total
	}
	next, completed, err := r.drive.UploadResumablePartReader(ctx, session.SessionURI, start, partTotal, io.NewSectionReader(file, inside, partSize), partSize)
	_ = file.Close()
	if err != nil {
		return err
	}
	if next <= start || next > start+partSize {
		return fmt.Errorf("Drive relay made invalid offset progress %d -> %d", start, next)
	}
	state := "UPLOADING"
	remoteID, remoteURL := session.RemoteID, session.RemoteURL
	if completed != nil {
		state, remoteID, remoteURL = "UPLOADED_UNVERIFIED", completed.FileID, completed.WebViewLink
	}
	if err := r.sessions.SaveProgress(ctx, session, next, false, state, remoteID, remoteURL, ""); err != nil {
		return err
	}
	session.NextOffset, session.InFlight = next, false
	session.State, session.RemoteID, session.RemoteURL = state, remoteID, remoteURL
	return nil
}

func (r *DriveStreamRelay) reconcileInFlight(ctx context.Context, session *deliveryrelay.Session, total int64) error {
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
	if err := r.sessions.SaveProgress(ctx, *session, next, false, state, remoteID, remoteURL, ""); err != nil {
		return err
	}
	session.NextOffset, session.InFlight = next, false
	session.State, session.RemoteID, session.RemoteURL = state, remoteID, remoteURL
	return nil
}

func (r *DriveStreamRelay) failSession(ctx context.Context, s deliveryrelay.Session, cause error) error {
	return r.sessions.FailSession(ctx, s, cause)
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

// relayFolderName derives the Drive subfolder name from the video title,
// so the project folder on Drive carries the same name as the MP4 saved in
// it. Titles are sanitized like every other Drive folder name; the opaque
// artifact id stays as the deterministic fallback when no title exists.
func relayFolderName(title, fallback string) string {
	if sanitized := paths.SanitizeDriveFolderName(title); sanitized != "" {
		return sanitized
	}
	return strings.TrimSpace(fallback)
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
