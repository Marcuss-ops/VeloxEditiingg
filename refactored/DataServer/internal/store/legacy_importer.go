// Package store provides database access layers for Velox.
//
// This file implements the legacy JSON→SQLite importer, which discovers
// legacy JSON files on disk, computes SHA-256 checksums for idempotency,
// creates backups, imports data into the appropriate SQLite tables, and
// records each operation in the legacy_imports tracking table.
//
// The importer runs automatically at startup after schema migrations, via
// SQLiteStore.ImportLegacyJSON(), ensuring a smooth transition from the
// file-based persistence era to the SQLite-first architecture.
package store

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
	"velox-shared/payload"
)

// ============================================================
// JSON source descriptors
// ============================================================

// jsonSource describes a legacy JSON file that can be imported.
type jsonSource struct {
	Name     string // human-readable name for logging
	Path     string // relative to dataDir
	AltPath  string // alternative path (fallback if Path doesn't exist)
	Domain   string // domain name used in legacy_imports.source_name
}

// legacyJSONSources returns the canonical list of legacy JSON files to import,
// ordered so that dependencies (e.g. channels before groups) are respected.
func legacyJSONSources() []jsonSource {
	return []jsonSource{
		{Name: "Workers", Path: "workers.json", Domain: "workers"},
		{Name: "YouTube Channels", Path: "youtube/channels/channels.json", Domain: "youtube_channels"},
		{Name: "YouTube Groups", Path: "youtube/groups.json", Domain: "youtube_groups"},
		{Name: "YouTube Manager", Path: "youtube/GroupYoutubeManager/ChannelsSaved.json", Domain: "youtube_manager"},
		{Name: "Ansible Computers", Path: "ansible/ansible_computers.json", Domain: "ansible_hosts"},
		{Name: "Ansible Runs", Path: "ansible_runs.json", Domain: "ansible_runs", AltPath: "ansible/ansible_runs.json"},
		{Name: "Analytics Cache", Path: "analytics/analytics_cache.json", Domain: "analytics_cache"},
		{Name: "YouTube API Cache", Path: "analytics/youtube_api_cache.json", Domain: "youtube_cache"},
		{Name: "Drive Links", Path: "drive/drive_links.yaml", AltPath: "drive/drive_links.json", Domain: "drive_links"},
	}
}

// ============================================================
// Import result
// ============================================================

// LegacyImportResult describes the outcome of importing a single JSON source.
type LegacyImportResult struct {
	Source    jsonSource `json:"-"`
	Status    string     `json:"status"`   // "imported", "skipped", "error"
	SHA256    string     `json:"sha256"`   // first 16 hex chars
	Records   int        `json:"records"`  // total records in file
	Imported  int        `json:"imported"` // successfully imported
	Backup    string     `json:"backup"`   // backup file path, if created
	Error     string     `json:"error,omitempty"`
}

// ============================================================
// Public API
// ============================================================

// ImportLegacyJSON discovers legacy JSON files in dataDir, checks
// the legacy_imports table for idempotency, creates backups,
// imports data into SQLite tables, and archives the source files
// to legacy_archive/ after a successful import.
//
// Run this once at startup, after schema migrations are applied.
// Errors are logged but do not block startup — the server continues
// with whatever data was available. Missing JSON files are silently skipped.
// If any import fails, an aggregate error is returned.
func (s *SQLiteStore) ImportLegacyJSON(dataDir string) ([]LegacyImportResult, error) {
	if dataDir == "" {
		return nil, nil
	}

	var results []LegacyImportResult

	for _, src := range legacyJSONSources() {
		absPath := filepath.Join(dataDir, src.Path)
		usedAlt := false
		info, err := os.Stat(absPath)
		if err != nil {
			if os.IsNotExist(err) && src.AltPath != "" {
				// Try alternative path
				absPath = filepath.Join(dataDir, src.AltPath)
				info, err = os.Stat(absPath)
				usedAlt = true
			}
			if err != nil {
				if os.IsNotExist(err) {
					continue
				}
				results = append(results, LegacyImportResult{
					Source: src,
					Status: "error",
					Error:  fmt.Sprintf("stat: %v", err),
				})
				continue
			}
		}
		if info.IsDir() || info.Size() == 0 {
			continue
		}

		result := s.importSource(absPath, src)

		// After successful import, archive the source file to legacy_archive/
		if result.Status == "imported" {
			if err := archiveLegacyJSON(absPath, dataDir); err != nil {
				log.Printf("[IMPORT] Warning: failed to archive %s: %v", absPath, err)
			} else {
				log.Printf("[IMPORT] Archived %s → legacy_archive/", absPath)
				// Update the source path to reflect the new location
				if usedAlt {
					src.Path = src.AltPath
				}
			}
		}

		results = append(results, result)
	}

	// Log summary
	imported := 0
	skipped := 0
	errors := 0
	hadErrors := false
	for _, r := range results {
		switch r.Status {
		case "imported":
			imported++
			log.Printf("[IMPORT] %s → %s (%d records, backup: %s)", r.Source.Name, r.Status, r.Imported, r.Backup)
		case "skipped":
			skipped++
			log.Printf("[IMPORT] %s → %s (checksum match, %d records already imported)", r.Source.Name, r.Status, r.Records)
		case "error":
			errors++
			hadErrors = true
			log.Printf("[IMPORT] %s → error: %s", r.Source.Name, r.Error)
		}
	}
	log.Printf("[IMPORT] Summary: %d imported, %d skipped (already up-to-date), %d errors", imported, skipped, errors)

	if hadErrors {
		var errs []string
		for _, r := range results {
			if r.Status == "error" && r.Error != "" {
				errs = append(errs, fmt.Sprintf("%s: %s", r.Source.Name, r.Error))
			}
		}
		return results, fmt.Errorf("legacy import completed with %d error(s): %s", errors, strings.Join(errs, "; "))
	}

	return results, nil
}

// ============================================================
// Single-source import logic
// ============================================================

// importerVersion tracks the format/strategy of the import logic.
// Increment when the import logic changes in a way that should
// re-import data even if the file checksum hasn't changed.
const importerVersion = 1

// importSource handles a single JSON source: checksum check, backup, import.
func (s *SQLiteStore) importSource(absPath string, src jsonSource) LegacyImportResult {
	// 1. Read file and compute SHA-256
	data, err := os.ReadFile(absPath)
	if err != nil {
		return LegacyImportResult{Source: src, Status: "error", Error: fmt.Sprintf("read: %v", err)}
	}

	hash := sha256.Sum256(data)
	shaHex := fmt.Sprintf("%x", hash)
	shaShort := shaHex[:16]

	// 2. Check legacy_imports table for idempotency
	alreadyImported, err := s.isAlreadyImported(src.Domain, shaHex)
	if err != nil {
		return LegacyImportResult{Source: src, Status: "error", Error: fmt.Sprintf("checksum check: %v", err)}
	}
	if alreadyImported {
		return LegacyImportResult{
			Source:  src,
			Status:  "skipped",
			SHA256:  shaShort,
			Records: 0,
		}
	}

	// 3. Parse and count records
	count, err := countJSONRecords(src.Domain, data)
	if err != nil {
		return LegacyImportResult{Source: src, Status: "error", SHA256: shaShort, Error: fmt.Sprintf("parse: %v", err)}
	}

	// 4. Create backup before importing (pass data to avoid re-reading)
	backupPath, err := createJSONBackup(absPath, data)
	if err != nil {
		// Non-fatal — log but continue
		log.Printf("[IMPORT] Warning: backup failed for %s: %v", absPath, err)
	}

	// 5. Import data into SQLite tables
	imported, err := importJSONData(s, src.Domain, data, absPath)
	if err != nil {
		// Record failed import
		_ = s.recordLegacyImport(src.Domain, absPath, shaHex, importerVersion, "rejected", count, 0, err.Error())
		return LegacyImportResult{
			Source:   src,
			Status:   "error",
			SHA256:   shaShort,
			Records:  count,
			Imported: imported,
			Backup:   backupPath,
			Error:    fmt.Sprintf("import: %v", err),
		}
	}

	// 6. Record successful import
	if err := s.recordLegacyImport(src.Domain, absPath, shaHex, importerVersion, "applied", count, imported, ""); err != nil {
		log.Printf("[IMPORT] Warning: failed to record import in legacy_imports: %v", err)
	}

	return LegacyImportResult{
		Source:   src,
		Status:   "imported",
		SHA256:   shaShort,
		Records:  count,
		Imported: imported,
		Backup:   backupPath,
	}
}

// ============================================================
// Idempotency check via legacy_imports table
// ============================================================

// isAlreadyImported checks if a file with the same (domain, sha256, version)
// has already been successfully imported.
func (s *SQLiteStore) isAlreadyImported(domain, sha256 string) (bool, error) {
	var count int
	err := s.db.QueryRow(
		`SELECT COUNT(*) FROM legacy_imports
		 WHERE source_name = ? AND source_sha256 = ? AND importer_version = ? AND status = 'applied'`,
		domain, sha256, importerVersion,
	).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// recordLegacyImport inserts a row into legacy_imports tracking table.
func (s *SQLiteStore) recordLegacyImport(sourceName, sourcePath, sha256 string, version int, status string, totalRows, importedRows int, errorMsg string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	rejectedRows := totalRows - importedRows
	_, err := s.db.Exec(
		`INSERT INTO legacy_imports
		 (source_name, source_path, source_sha256, importer_version, status,
		  imported_rows, rejected_rows, conflict_rows, error_message, imported_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
		 ON CONFLICT(source_name, source_sha256, importer_version) DO UPDATE SET
		   status=excluded.status,
		   imported_rows=excluded.imported_rows,
		   rejected_rows=excluded.rejected_rows,
		   error_message=COALESCE(NULLIF(excluded.error_message, ''), legacy_imports.error_message),
		   imported_at=excluded.imported_at`,
		sourceName, sourcePath, sha256, version, status,
		importedRows, rejectedRows, errorMsg, now,
	)
	return err
}

// ============================================================
// JSON backup
// ============================================================

// createJSONBackup creates a timestamped backup copy of a JSON file
// in the same directory, using the already-read data. Returns the backup path.
func createJSONBackup(absPath string, data []byte) (string, error) {
	dir := filepath.Dir(absPath)
	base := filepath.Base(absPath)
	ext := filepath.Ext(base)
	name := base[:len(base)-len(ext)]
	timestamp := time.Now().UTC().Format("20060102T150405Z")
	backupName := fmt.Sprintf("%s.%s.bak", name, timestamp)
	backupPath := filepath.Join(dir, backupName)

	if err := os.WriteFile(backupPath, data, 0644); err != nil {
		return "", fmt.Errorf("write backup: %w", err)
	}

	return backupPath, nil
}

// ============================================================
// JSON record counting
// ============================================================

// countJSONRecords returns the number of top-level records in a JSON file.
func countJSONRecords(domain string, data []byte) (int, error) {
	// Special case: drive_links can be YAML array
	if domain == "drive_links" {
		var list []map[string]any
		if err := json.Unmarshal(data, &list); err == nil {
			return len(list), nil
		}
		if err := yaml.Unmarshal(data, &list); err == nil {
			return len(list), nil
		}
		return 0, nil
	}

	// Special case: workers domain uses { "workers": { ... }, "revoked": [...] }
	// -> count the workers sub-object, not the top-level keys
	if domain == "workers" {
		var wf legacyWorkersFile
		if err := json.Unmarshal(data, &wf); err == nil && len(wf.Workers) > 0 {
			return len(wf.Workers), nil
		}
		// Fall through to generic map/array handling for flat format
	}

	// Try map first (most domains: workers flat, channels, ansible)
	var m map[string]any
	if err := json.Unmarshal(data, &m); err == nil {
		return len(m), nil
	}

	// Fallback to array (youtube_groups, etc.)
	var a []any
	if err := json.Unmarshal(data, &a); err == nil {
		return len(a), nil
	}

	return 0, nil
}

// ============================================================
// Domain-specific import functions
// ============================================================

// importJSONData dispatches to the appropriate import function based on domain.
// absPath is the absolute path to the source JSON file (needed by some importers).
func importJSONData(s *SQLiteStore, domain string, data []byte, absPath string) (int, error) {
	switch domain {
	case "workers":
		return importWorkersJSON(s, data)
	case "youtube_channels":
		return importYouTubeChannelsJSON(s, data)
	case "youtube_groups":
		return importYouTubeGroupsJSON(s, data)
	case "youtube_manager":
		return importYouTubeManagerJSON(s, data)
	case "ansible_hosts":
		return importAnsibleHostsJSON(s, data, absPath)
	case "ansible_runs":
		return importAnsibleRunsJSON(s, data)
	case "analytics_cache":
		return importAnalyticsCacheJSON(s, data)
	case "youtube_cache":
		return importYouTubeCacheJSON(s, data)
	case "drive_links":
		return importDriveLinksJSON(s, data, absPath)
	default:
		return 0, fmt.Errorf("unknown import domain: %s", domain)
	}
}

// --- Workers ---

// legacyWorkersFile represents the real workers.json format.
type legacyWorkersFile struct {
	Workers map[string]any    `json:"workers"`
	Revoked []string          `json:"revoked"`
}

// importWorkersJSON imports workers.json into the workers table.
// Real format: { "workers": { "worker_id": { ... }, ... }, "revoked": ["worker_id", ...] }
// Fallback: { "worker_id": { ... }, ... } (flat format)
func importWorkersJSON(s *SQLiteStore, data []byte) (int, error) {
	var wf legacyWorkersFile
	if err := json.Unmarshal(data, &wf); err != nil || len(wf.Workers) == 0 {
		// Fallback: try flat format { "worker_id": { ... }, ... }
		var m map[string]any
		if err2 := json.Unmarshal(data, &m); err2 != nil {
			if err != nil {
				return 0, fmt.Errorf("unmarshal workers: %w", err)
			}
			return 0, fmt.Errorf("unmarshal workers: %w", err2)
		}
		return importWorkersFlat(s, m)
	}

	imported := 0

	// Import workers from the "workers" sub-object
	for _, raw := range wf.Workers {
		b, err := json.Marshal(raw)
		if err != nil {
			continue
		}
		if err := s.UpsertWorker(b); err != nil {
			log.Printf("[IMPORT] worker: skip record: %v", err)
			continue
		}
		imported++
	}

	// Import revoked list into worker_flags
	for _, id := range wf.Revoked {
		if id == "" {
			continue
		}
		if err := s.SetWorkerRevoked(id, true); err != nil {
			log.Printf("[IMPORT] revoked worker %s: %v", id, err)
		}
	}

	return imported, nil
}

// importWorkersFlat handles the simple flat format { "worker_id": { ... }, ... }
// The map key is the worker_id if not already present in the worker object.
func importWorkersFlat(s *SQLiteStore, m map[string]any) (int, error) {
	imported := 0
	for key, raw := range m {
		// Skip special keys used by the real {workers, revoked} format
		if key == "workers" || key == "revoked" {
			continue
		}
		workerObj, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		// Inject key as worker_id if not already present
		if _, exists := workerObj["worker_id"]; !exists {
			workerObj["worker_id"] = key
		}
		b, err := json.Marshal(workerObj)
		if err != nil {
			continue
		}
		if err := s.UpsertWorker(b); err != nil {
			log.Printf("[IMPORT] worker: skip record: %v", err)
			continue
		}
		imported++
	}
	return imported, nil
}

// --- YouTube Channels (canonical) ---

// importYouTubeChannelsJSON imports youtube/channels/channels.json into youtube_channels.
// Format: { "channel_id": { "title": "...", "display_name": "...", ... }, ... }
func importYouTubeChannelsJSON(s *SQLiteStore, data []byte) (int, error) {
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		return 0, fmt.Errorf("unmarshal youtube channels: %w", err)
	}

	imported := 0
	for id, raw := range m {
		ch, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		title, _ := ch["title"].(string)
		displayName, _ := ch["display_name"].(string)
		if displayName == "" {
			displayName = title
		}
		channelURL, _ := ch["url"].(string)
		language, _ := ch["language"].(string)
		thumbnailURL, _ := ch["thumbnail_url"].(string)

		// Convert view_count and sub_count
		viewCount := toInt64(ch["view_count"])
		subCount := toInt64(ch["subscriber_count"])

		if err := s.UpsertYouTubeChannel(id, title, displayName, channelURL, thumbnailURL, language, "", viewCount, subCount, "", "", "{}"); err != nil {
			log.Printf("[IMPORT] youtube channel %s: %v", id[:8], err)
			continue
		}
		imported++
	}
	return imported, nil
}

// --- YouTube Groups (canonical v2) ---

// importYouTubeGroupsJSON imports youtube/groups.json into youtube_groups_v2.
// Format: [ { "name": "...", "description": "...", "privacy": "...", "channels": [...] }, ... ]
func importYouTubeGroupsJSON(s *SQLiteStore, data []byte) (int, error) {
	var groups []map[string]any
	if err := json.Unmarshal(data, &groups); err != nil {
		return 0, fmt.Errorf("unmarshal youtube groups: %w", err)
	}

	imported := 0
	for _, g := range groups {
		name, _ := g["name"].(string)
		if name == "" {
			continue
		}
		desc, _ := g["description"].(string)
		privacy, _ := g["privacy"].(string)

		// Import group into youtube_groups_v2 with group_type="upload"
		groupID, err := s.UpsertYouTubeGroupV2(name, "upload", desc, privacy)
		if err != nil {
			log.Printf("[IMPORT] youtube group %q: %v", name, err)
			continue
		}

		// Add channel memberships if present
		if channelsRaw, ok := g["channels"].([]any); ok {
			for _, chRaw := range channelsRaw {
				chID, _ := chRaw.(string)
				if chID == "" {
					continue
				}
				_ = s.AddChannelToGroupV2(groupID, chID)
			}
		}
		imported++
	}
	return imported, nil
}

// --- YouTube Manager Channels (canonical) ---

// importYouTubeManagerJSON imports youtube/GroupYoutubeManager/ChannelsSaved.json
// into youtube_channels (canonical), creating groups in youtube_groups_v2
// and linking channels via youtube_group_channels.
//
// Supports two formats:
//   - Flat map: { "channel_id": { "title": "...", "group": "...", ... }, ... }
//   - Groups object: { "groups": { "name": { "name": "...", "channels": [...], ... }, ... } }
func importYouTubeManagerJSON(s *SQLiteStore, data []byte) (int, error) {
	var root map[string]any
	if err := json.Unmarshal(data, &root); err != nil {
		return 0, fmt.Errorf("unmarshal youtube manager: %w", err)
	}

	// Detect format: "groups" key means grouped format
	if groupsRaw, ok := root["groups"].(map[string]any); ok {
		return importYouTubeManagerGroupsFormat(s, groupsRaw)
	}

	// Otherwise treat as flat map
	return importYouTubeManagerFlatFormat(s, root)
}

// importYouTubeManagerGroupsFormat handles: { "groups": { "name": { "channels": [...], ... } } }
func importYouTubeManagerGroupsFormat(s *SQLiteStore, groupsRaw map[string]any) (int, error) {
	imported := 0
	for gname, gdataRaw := range groupsRaw {
		gdata, ok := gdataRaw.(map[string]any)
		if !ok {
			continue
		}

		desc, _ := gdata["description"].(string)
		privacy, _ := gdata["privacy"].(string)

		groupID, err := s.UpsertYouTubeGroupV2(gname, "manager", desc, privacy)
		if err != nil {
			log.Printf("[IMPORT] youtube manager group %q: %v", gname, err)
			continue
		}

		channels, ok := gdata["channels"].([]any)
		if !ok {
			continue
		}

		for _, chRaw := range channels {
			ch, ok := chRaw.(map[string]any)
			if !ok {
				continue
			}

			id, _ := ch["id"].(string)
			if id == "" {
				continue
			}

			title, _ := ch["title"].(string)
			name, _ := ch["name"].(string)
			url, _ := ch["url"].(string)
			thumbnail, _ := ch["thumbnail"].(string)
			language, _ := ch["language"].(string)

			displayName := name
			if displayName == "" {
				displayName = title
			}
			if err := s.UpsertYouTubeChannel(id, title, displayName, url, thumbnail, language, "", 0, 0, "", "", "{}"); err != nil {
				log.Printf("[IMPORT] youtube manager channel %s: %v", id[:min(8, len(id))], err)
				continue
			}

			_ = s.AddChannelToGroupV2(groupID, id)
			imported++
		}
	}
	return imported, nil
}

// importYouTubeManagerFlatFormat handles: { "channel_id": { "title": "...", "group": "...", ... } }
func importYouTubeManagerFlatFormat(s *SQLiteStore, m map[string]any) (int, error) {
	groupIDs := make(map[string]int64)

	imported := 0
	for id, raw := range m {
		ch, ok := raw.(map[string]any)
		if !ok {
			continue
		}

		title, _ := ch["title"].(string)
		url, _ := ch["url"].(string)
		groupName, _ := ch["group"].(string)
		channelName, _ := ch["name"].(string)
		thumbnail, _ := ch["thumbnail"].(string)
		language, _ := ch["language"].(string)
		viewCount := toInt64(ch["view_count"])
		subCount := toInt64(ch["sub_count"])

		displayName := channelName
		if displayName == "" {
			displayName = title
		}
		if err := s.UpsertYouTubeChannel(id, title, displayName, url, thumbnail, language, "", viewCount, subCount, "", "", "{}"); err != nil {
			log.Printf("[IMPORT] youtube manager channel %s: %v", id[:min(8, len(id))], err)
			continue
		}

		if groupName != "" {
			gid, exists := groupIDs[groupName]
			if !exists {
				var err error
				gid, err = s.UpsertYouTubeGroupV2(groupName, "manager", "", "")
				if err != nil {
					log.Printf("[IMPORT] youtube manager group %q: %v", groupName, err)
				} else {
					groupIDs[groupName] = gid
				}
			}
			if gid > 0 {
				_ = s.AddChannelToGroupV2(gid, id)
			}
		}

		imported++
	}
	return imported, nil
}

// --- Ansible Hosts (canonical) ---

// importAnsibleHostsJSON imports ansible/ansible_computers.json into ansible_hosts.
// Format: { "host_name": { "host": "...", "ansible_user": "...", ... }, ... }
// absPath is the absolute path of the JSON file, used to derive the secrets directory.
func importAnsibleHostsJSON(s *SQLiteStore, data []byte, absPath string) (int, error) {
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		return 0, fmt.Errorf("unmarshal ansible hosts: %w", err)
	}

	imported := 0
	for _, raw := range m {
		c, ok := raw.(map[string]any)
		if !ok {
			continue
		}

		host, _ := c["host"].(string)
		if host == "" {
			continue
		}

		user, _ := c["ansible_user"].(string)
		group, _ := c["group"].(string)
		sshKeyPath, _ := c["ssh_key_path"].(string)
		enabled := true
		if e, ok := c["enabled"].(bool); ok {
			enabled = e
		}

		// Migrate plaintext password to secret_ref
		var secretRef string
		if sshPassword, ok := c["ssh_password"].(string); ok && sshPassword != "" {
			// Derive secrets dir from the JSON file path.
			// If the file is at /data/ansible/ansible_computers.json,
			// secrets go to /data/secrets/ansible/
			dataDir := filepath.Dir(filepath.Dir(absPath)) // up two levels: ansible_computers.json → ansible → dataDir
			secretsDir := filepath.Join(dataDir, "secrets", "ansible")
			resolver := newAnsibleSecretResolver(secretsDir)
			ref, err := resolver.StoreSSHPassword(host, sshPassword)
			if err != nil {
				log.Printf("[IMPORT] Warning: failed to store SSH password for %s: %v — host will lack password auth", host, err)
			} else if ref != "" {
				secretRef = ref
			}
		}

		fields := AnsibleHostFields{
			Host:        host,
			AnsibleUser: payload.FirstNonEmpty(user, "pierone"),
			SSHKeyPath:  sshKeyPath,
			SecretRef:   secretRef,
			Enabled:     enabled,
			Group:       group,
		}
		if err := s.UpsertAnsibleHost(fields); err != nil {
			log.Printf("[IMPORT] ansible host %s: %v", host, err)
			continue
		}
		imported++
	}
	return imported, nil
}

// --- Ansible Runs ---

// importAnsibleRunsJSON imports ansible/ansible_runs.json into ansible_runs.
// Format: { "run_id": { "run_id": "...", "action": "...", ... }, ... }
func importAnsibleRunsJSON(s *SQLiteStore, data []byte) (int, error) {
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		return 0, fmt.Errorf("unmarshal ansible runs: %w", err)
	}

	imported := 0
	for _, raw := range m {
		r, ok := raw.(map[string]any)
		if !ok {
			continue
		}

		runID, _ := r["run_id"].(string)
		if runID == "" {
			if id, ok := r["id"].(string); ok {
				runID = id
			}
		}
		if runID == "" {
			continue
		}

		action, _ := r["action"].(string)
		status, _ := r["status"].(string)
		playbook, _ := r["playbook"].(string)
		output, _ := r["output"].(string)
		startedAt := toInt64(r["started_at"])
		endedAt := toInt64(r["ended_at"])
		returnCode := int(toInt64(r["return_code"]))

		if err := s.UpsertAnsibleRun(runID, action, playbook, status, startedAt, endedAt, returnCode, "[]", output, "", "", ""); err != nil {
			log.Printf("[IMPORT] ansible run %s: %v", runID[:8], err)
			continue
		}

		// Link hosts if available
		if hostsRaw, ok := r["hosts"].([]any); ok {
			for _, hRaw := range hostsRaw {
				if host, ok := hRaw.(string); ok {
					_ = s.AddAnsibleRunHost(runID, host)
				}
			}
		}

		imported++
	}
	return imported, nil
}

// --- Analytics Cache ---

// importAnalyticsCacheJSON imports analytics/analytics_cache.json into analytics_cache.
// Format: { "cache_key": { ... data ... }, ... }
func importAnalyticsCacheJSON(s *SQLiteStore, data []byte) (int, error) {
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		return 0, fmt.Errorf("unmarshal analytics cache: %w", err)
	}

	imported := 0
	for key, raw := range m {
		b, err := json.Marshal(raw)
		if err != nil {
			continue
		}
		if err := s.UpsertAnalyticsCache(key, float64(time.Now().Unix()), b); err != nil {
			log.Printf("[IMPORT] analytics cache %s: %v", key[:16], err)
			continue
		}
		imported++
	}
	return imported, nil
}

// --- YouTube API Cache ---

// importYouTubeCacheJSON imports analytics/youtube_api_cache.json into youtube_api_cache.
// Format: { "cache_key": { "timestamp": 1234567890, "data": { ... } }, ... }
func importYouTubeCacheJSON(s *SQLiteStore, data []byte) (int, error) {
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		return 0, fmt.Errorf("unmarshal youtube cache: %w", err)
	}

	imported := 0
	for key, raw := range m {
		entry, ok := raw.(map[string]any)
		if !ok {
			continue
		}

		ts := toInt64(entry["timestamp"])
		dataJSON, err := json.Marshal(entry["data"])
		if err != nil {
			continue
		}

		if err := s.SetYouTubeCache(key, ts, string(dataJSON)); err != nil {
			log.Printf("[IMPORT] youtube cache %s: %v", key[:16], err)
			continue
		}
		imported++
	}
	return imported, nil
}

// --- Drive Links (YAML/JSON) ---

// importDriveLinksJSON imports drive/drive_links.yaml (or .json) into drive_links.
// Format: array of { "id": "...", "name": "...", "link": "...", "parentId": "...", "language": "..." }
func importDriveLinksJSON(s *SQLiteStore, data []byte, absPath string) (int, error) {
	var list []map[string]any

	// Try JSON first, then YAML
	if err := json.Unmarshal(data, &list); err != nil {
		if yamlErr := yaml.Unmarshal(data, &list); yamlErr != nil {
			return 0, fmt.Errorf("unmarshal drive links: json=%v, yaml=%v", err, yamlErr)
		}
	}

	if len(list) == 0 {
		return 0, nil
	}

	imported, err := s.MigrateDriveLinksFromJSON(list)
	if err != nil {
		return 0, fmt.Errorf("import drive links: %w", err)
	}
	return imported, nil
}

// ============================================================
// JSON archiving (move to legacy_archive/ after successful import)
// ============================================================

// archiveLegacyJSON moves a successfully imported JSON file to a legacy_archive/
// subdirectory, organized by date. This prevents re-import loops and allows
// the post-import data layer audit to pass without false positives.
//
// The archive directory structure is:
//   <dataDir>/legacy_archive/<YYYY-MM-DD>/<filename>
func archiveLegacyJSON(absPath, dataDir string) error {
	archiveDir := filepath.Join(dataDir, "legacy_archive", time.Now().UTC().Format("2006-01-02"))
	if err := os.MkdirAll(archiveDir, 0755); err != nil {
		return fmt.Errorf("create archive dir: %w", err)
	}

	destPath := filepath.Join(archiveDir, filepath.Base(absPath))
	// If destination already exists, add a suffix
	if _, err := os.Stat(destPath); err == nil {
		ext := filepath.Ext(absPath)
		base := absPath[:len(absPath)-len(ext)]
		destPath = filepath.Join(archiveDir, filepath.Base(base)+"_"+time.Now().UTC().Format("150405")+ext)
	}

	if err := os.Rename(absPath, destPath); err != nil {
		return fmt.Errorf("move to archive: %w", err)
	}

	return nil
}

// ============================================================
// Helpers
// ============================================================

// toInt64 converts a JSON value to int64.
func toInt64(v any) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case int64:
		return n
	case int:
		return int64(n)
	case json.Number:
		if i, err := n.Int64(); err == nil {
			return i
		}
	}
	return 0
}

// newAnsibleSecretResolver creates a SecretResolver for password migration.
// This is a lightweight helper to avoid importing the ansible handler package.
func newAnsibleSecretResolver(secretsDir string) *ansibleSecretHelper {
	return &ansibleSecretHelper{secretsDir: secretsDir}
}

type ansibleSecretHelper struct {
	secretsDir string
}

func (a *ansibleSecretHelper) StoreSSHPassword(host, password string) (string, error) {
	if a.secretsDir == "" {
		return "", fmt.Errorf("secrets directory not configured")
	}
	if err := os.MkdirAll(a.secretsDir, 0700); err != nil {
		return "", fmt.Errorf("create secrets dir: %w", err)
	}
	filename := fmt.Sprintf("ssh_host_%s", sanitizeFilename(host))
	path := filepath.Join(a.secretsDir, filename)
	if err := os.WriteFile(path, []byte(password), 0600); err != nil {
		return "", fmt.Errorf("write secret file: %w", err)
	}
	return fmt.Sprintf("file:%s", filename), nil
}

func sanitizeFilename(s string) string {
	replacer := strings.NewReplacer(":", "_", "/", "_", "\\", "_", " ", "_")
	return replacer.Replace(s)
}
