package workers

import (
	"archive/zip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"velox-server/internal/config"
	workersreg "velox-server/internal/workers"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// WorkerUpdateHandler handles worker update pipeline operations
type WorkerUpdateHandler struct {
	cfg          *config.Config
	reg          *workersreg.Registry
	persistedReg *workersreg.WorkerRegistry
	cmdMgr       *workersreg.CommandManager
	updateMgr    *workersreg.UpdateManager
	tokenMgr     *workersreg.TokenManager
	dataDir      string
	bundleDir    string
	codeVersion  string
}

func (h *WorkerUpdateHandler) authorizeWorkerRequest(c *gin.Context, workerID string) bool {
	token := workersreg.ExtractBearerToken(
		c.GetHeader("Authorization"),
		c.GetHeader("X-Admin-Token"),
		c.Query("token"),
	)
	if !workersreg.AuthorizeWorkerToken(h.tokenMgr, token, workerID, c.ClientIP()) {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid worker token"})
		return false
	}
	return true
}

// PendingUpdateState tracks the state of a pending update
type PendingUpdateState struct {
	WorkerID          string               `json:"worker_id"`
	TargetVersion     string               `json:"target_version"`
	TargetArtifactSHA string               `json:"target_artifact_sha256,omitempty"`
	RequestedAt       time.Time            `json:"requested_at"`
	UpdateState       string               `json:"update_state,omitempty"`
	UpdateStateTime   map[string]time.Time `json:"update_state_time,omitempty"`
	ArtifactSHA256    string               `json:"artifact_sha256,omitempty"`
	AckVersion        string               `json:"ack_version,omitempty"`
	Error             string               `json:"error,omitempty"`
}

type updateAllRequest struct {
	ExcludeLocal *bool `json:"exclude_local"`
	DryRun       *bool `json:"dry_run"`
}

func buildSmokeJobPayload(workerID string) map[string]interface{} {
	now := time.Now().UTC()
	jobID := fmt.Sprintf("smoke-%s-%d", workerID, now.Unix())
	return map[string]interface{}{
		"job_id":          jobID,
		"job_type":        "health_check",
		"priority":        0,
		"timeout_secs":    60,
		"created_at":      now.Format(time.RFC3339),
		"expect_callback": true,
	}
}

// bundleDirCandidates returns candidate paths for the worker bundle (first that contains worker_code.zip wins)
func bundleDirCandidates(dataDir string) []string {
	if dataDir == "" {
		return []string{"worker_downloads"}
	}
	// dataDir is typically refactored/DataServer/data → refactored/worker_downloads is ../../worker_downloads
	base := filepath.Join(dataDir, "..", "..")
	return []string{
		filepath.Join(base, "worker_downloads"), // refactored/worker_downloads (zip lives here)
		filepath.Join(base, "BundleRemote", "worker_downloads"),
		filepath.Join(base, "BundleRemote"),
		filepath.Join(dataDir, "worker_downloads"),
	}
}

func computeStringSHA256Hex(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

func readTextFileTrim(path string) (string, bool) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", false
	}
	return strings.TrimSpace(string(b)), true
}

func resolveBundlePath(bundleDir, platform, arch string) (string, os.FileInfo, error) {
	bundleName := fmt.Sprintf("worker_code_%s_%s.zip", platform, arch)
	bundlePath := filepath.Join(bundleDir, bundleName)
	if _, err := os.Stat(bundlePath); os.IsNotExist(err) {
		bundlePath = filepath.Join(bundleDir, "worker_code.zip")
	}
	info, err := os.Stat(bundlePath)
	if err != nil {
		return "", nil, err
	}
	return bundlePath, info, nil
}

func listZipFilesWithHashes(bundlePath string) ([]gin.H, map[string]string, error) {
	r, err := zip.OpenReader(bundlePath)
	if err != nil {
		return nil, nil, err
	}
	defer r.Close()

	type fileEntry struct {
		Name string
		Size int64
		Hash string
		Top  string
	}
	entries := make([]fileEntry, 0, len(r.File))
	for _, f := range r.File {
		if f.FileInfo().IsDir() {
			continue
		}
		rc, err := f.Open()
		if err != nil {
			continue
		}
		h := sha256.New()
		_, _ = io.Copy(h, rc)
		_ = rc.Close()
		sum := hex.EncodeToString(h.Sum(nil))
		name := f.Name
		top := strings.SplitN(strings.TrimLeft(name, "/"), "/", 2)[0]
		entries = append(entries, fileEntry{
			Name: name,
			Size: int64(f.UncompressedSize64),
			Hash: sum,
			Top:  top,
		})
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name < entries[j].Name
	})

	dirHash := make(map[string]hash.Hash)
	for _, e := range entries {
		if _, ok := dirHash[e.Top]; !ok {
			dirHash[e.Top] = sha256.New()
		}
		dirHash[e.Top].Write([]byte(e.Name))
		dirHash[e.Top].Write([]byte(e.Hash))
	}

	files := make([]gin.H, 0, len(entries))
	for _, e := range entries {
		files = append(files, gin.H{
			"path":   e.Name,
			"size":   e.Size,
			"sha256": e.Hash,
		})
	}

	dirHashOut := make(map[string]string)
	for k, h := range dirHash {
		dirHashOut[k] = hex.EncodeToString(h.Sum(nil))
	}
	return files, dirHashOut, nil
}

func findRepoRootFrom(start string) string {
	dir, err := filepath.Abs(start)
	if err != nil {
		dir = start
	}
	for i := 0; i < 6; i++ {
		candidate := filepath.Join(dir, "DataServer")
		if stat, err := os.Stat(candidate); err == nil && stat.IsDir() {
			return dir
		}
		candidateAlternative := filepath.Join(dir, "refactored", "DataServer")
		if stat, err := os.Stat(candidateAlternative); err == nil && stat.IsDir() {
			return filepath.Join(dir, "refactored")
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return ""
}

// NewWorkerUpdateHandler creates a new worker update handler
func NewWorkerUpdateHandler(cfg *config.Config, reg *workersreg.Registry, persistedReg *workersreg.WorkerRegistry, cmdMgr *workersreg.CommandManager, updateMgr *workersreg.UpdateManager, tokenMgr *workersreg.TokenManager, dataDir string) *WorkerUpdateHandler {
	bundleDir := cfg.WorkerBundleDir
	if bundleDir == "" {
		for _, d := range bundleDirCandidates(dataDir) {
			if abs, err := filepath.Abs(d); err == nil {
				d = abs
			}
			if _, err := os.Stat(filepath.Join(d, "worker_code.zip")); err == nil {
				bundleDir = d
				break
			}
		}
		if bundleDir == "" {
			bundleDir = filepath.Join(dataDir, "worker_downloads")
		}
	}

	log.Printf("📦 WORKER UPDATE: Using bundle directory: %s", bundleDir)

	return &WorkerUpdateHandler{
		cfg:          cfg,
		reg:          reg,
		persistedReg: persistedReg,
		cmdMgr:       cmdMgr,
		updateMgr:    updateMgr,
		tokenMgr:     tokenMgr,
		dataDir:      dataDir,
		bundleDir:    bundleDir,
		codeVersion:  cfg.CodeVersion,
	}
}

// SendCommandHandler handles POST /worker/send_command
func (h *WorkerUpdateHandler) SendCommandHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		workerID := c.Query("worker_id")
		command := c.Query("command")

		if workerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "worker_id required"})
			return
		}

		allowedCommands := map[string]bool{
			"restart_worker": true,
			"update_code":    true,
			"reboot_host":    true,
			"run_smoke_job":  true,
		}
		if !allowedCommands[command] {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "Invalid command. Allowed: restart_worker, update_code, reboot_host",
			})
			return
		}

		// Check if worker is revoked
		if h.persistedReg != nil && h.persistedReg.IsRevoked(workerID) {
			c.JSON(http.StatusForbidden, gin.H{"error": "Worker is revoked"})
			return
		}

		// Push command
		params := map[string]interface{}{}
		if command == "update_code" {
			params["version"] = h.codeVersion
			h.updateMgr.RequestUpdate(workerID, h.codeVersion)
		}
		if command == "run_smoke_job" {
			params = buildSmokeJobPayload(workerID)
		}
		h.cmdMgr.PushCommand(workerID, command, params)

		log.Printf("📤 Command '%s' queued for worker %s", command, workerID[:min(16, len(workerID))]+"...")

		c.JSON(http.StatusOK, gin.H{
			"status":    "queued",
			"worker_id": workerID,
			"command":   command,
		})
	}
}

// SendCommandBulkHandler handles POST /workers/send_command_bulk
func (h *WorkerUpdateHandler) SendCommandBulkHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			WorkerIDs      []string               `json:"worker_ids"`
			Workers        []string               `json:"workers"`
			Command        string                 `json:"command"`
			ExcludeRevoked bool                   `json:"exclude_revoked"`
			Payload        map[string]interface{} `json:"payload"`
		}

		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid JSON body"})
			return
		}

		// Accept both worker_ids and workers
		workerIDs := body.WorkerIDs
		if len(workerIDs) == 0 {
			workerIDs = body.Workers
		}

		if len(workerIDs) == 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "worker_ids required (non-empty list)"})
			return
		}

		if body.Command == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "command required"})
			return
		}

		allowedCommands := map[string]bool{
			"restart_worker": true,
			"update_code":    true,
			"reboot_host":    true,
			"run_smoke_job":  true,
		}
		if !allowedCommands[body.Command] {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "Invalid command. Allowed: restart_worker, update_code, reboot_host",
			})
			return
		}

		excludeRevoked := body.ExcludeRevoked
		if !excludeRevoked {
			excludeRevoked = true // Default to true
		}

		// Calculate target artifact SHA256 for update_code
		targetArtifactSHA := ""
		if body.Command == "update_code" {
			targetArtifactSHA = h.computeBundleSHA256()
		}

		queued := []string{}
		skipped := []string{}
		invalid := []string{}

		for _, wid := range workerIDs {
			if wid == "" {
				invalid = append(invalid, wid)
				continue
			}

			// Check if revoked
			if excludeRevoked && h.persistedReg != nil && h.persistedReg.IsRevoked(wid) {
				skipped = append(skipped, wid)
				continue
			}

			// Push command
			params := map[string]interface{}{}
			if body.Command == "update_code" {
				params["version"] = h.codeVersion
				params["target_artifact_sha256"] = targetArtifactSHA
				h.updateMgr.RequestUpdate(wid, h.codeVersion)
			} else if body.Command == "run_smoke_job" {
				if len(body.Payload) > 0 {
					params = body.Payload
				} else {
					params = buildSmokeJobPayload(wid)
				}
			}
			h.cmdMgr.PushCommand(wid, body.Command, params)
			queued = append(queued, wid)
		}

		log.Printf("📤 Bulk command '%s': queued=%d skipped=%d invalid=%d",
			body.Command, len(queued), len(skipped), len(invalid))

		c.JSON(http.StatusOK, gin.H{
			"status":             "queued",
			"command":            body.Command,
			"queued_count":       len(queued),
			"queued_worker_ids":  queued,
			"skipped_worker_ids": skipped,
			"invalid_worker_ids": invalid,
		})
	}
}

func (h *WorkerUpdateHandler) eligibleWorkers(ctx context.Context, excludeLocal bool) []string {
	if h.reg == nil {
		return []string{}
	}

	allWorkers := h.reg.List(ctx)
	eligible := make([]string, 0, len(allWorkers))
	for _, info := range allWorkers {
		if h.persistedReg != nil && h.persistedReg.IsRevoked(info.WorkerID) {
			continue
		}
		if info.Drain {
			continue
		}
		if excludeLocal {
			name := info.WorkerName
			if len(name) > 12 && name[:12] == "Local-Worker" {
				continue
			}
		}
		eligible = append(eligible, info.WorkerID)
	}
	return eligible
}

func (h *WorkerUpdateHandler) readUpdateAllOptions(c *gin.Context) (excludeLocal bool, dryRun bool) {
	excludeLocal = c.Query("exclude_local") != "false"
	dryRun = c.Query("dry_run") == "true"

	if c.Request != nil && c.Request.ContentLength != 0 {
		var body updateAllRequest
		if err := c.ShouldBindJSON(&body); err == nil {
			if body.ExcludeLocal != nil {
				excludeLocal = *body.ExcludeLocal
			}
			if body.DryRun != nil {
				dryRun = *body.DryRun
			}
		}
	}

	return excludeLocal, dryRun
}

// FullUpdateLinuxHandler handles POST /workers/full_update_linux
func (h *WorkerUpdateHandler) FullUpdateLinuxHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		excludeLocal, dryRun := h.readUpdateAllOptions(c)
		eligible := h.eligibleWorkers(c.Request.Context(), excludeLocal)

		if len(eligible) == 0 {
			c.JSON(http.StatusOK, gin.H{
				"status":  "no_workers",
				"queued":  0,
				"message": "No eligible workers",
			})
			return
		}

		maintenanceID := uuid.New().String()
		commandsQueued := 0
		targetArtifactSHA := h.computeBundleSHA256()

		for _, wid := range eligible {
			// Queue: maintenance_full_update_linux, update_code, restart_worker
			h.cmdMgr.PushCommand(wid, "maintenance_full_update_linux", map[string]interface{}{
				"id":        maintenanceID,
				"dry_run":   dryRun,
				"requested": time.Now().Unix(),
			})
			commandsQueued++

			h.cmdMgr.PushCommand(wid, "update_code", map[string]interface{}{
				"version":                h.codeVersion,
				"target_artifact_sha256": targetArtifactSHA,
			})
			h.updateMgr.RequestUpdate(wid, h.codeVersion)
			commandsQueued++

			h.cmdMgr.PushCommand(wid, "restart_worker", nil)
			commandsQueued++

			// Post-update smoke test callback (health_check payload from legacy flow).
			h.cmdMgr.PushCommand(wid, "run_smoke_job", buildSmokeJobPayload(wid))
			commandsQueued++
		}

		log.Printf("🔄 Full update Linux: %d workers, %d commands, maintenance_id=%s",
			len(eligible), commandsQueued, maintenanceID)

		c.JSON(http.StatusOK, gin.H{
			"status":          "queued",
			"maintenance_id":  maintenanceID,
			"queued":          len(eligible),
			"total_eligible":  len(eligible),
			"commands_queued": commandsQueued,
			"worker_ids":      eligible,
			"updated_workers": eligible,
			"updated_count":   len(eligible),
		})
	}
}

// UpdateAllHandler handles POST /workers/update_all
func (h *WorkerUpdateHandler) UpdateAllHandler() gin.HandlerFunc {
	return h.FullUpdateLinuxHandler()
}

// RestartAllHandler handles POST /workers/restart_all
func (h *WorkerUpdateHandler) RestartAllHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		excludeLocal, _ := h.readUpdateAllOptions(c)
		eligible := h.eligibleWorkers(c.Request.Context(), excludeLocal)

		if len(eligible) == 0 {
			c.JSON(http.StatusOK, gin.H{
				"status":  "no_workers",
				"queued":  0,
				"message": "No eligible workers",
			})
			return
		}

		for _, wid := range eligible {
			h.cmdMgr.PushCommand(wid, "restart_worker", nil)
		}

		log.Printf("🔄 Restart all queued for %d workers", len(eligible))

		c.JSON(http.StatusOK, gin.H{
			"status":            "queued",
			"queued":            len(eligible),
			"worker_ids":        eligible,
			"restarted_workers": eligible,
			"restarted_count":   len(eligible),
			"command":           "restart_worker",
		})
	}
}

// RolloutUpdateHandler handles POST /workers/rollout_update
func (h *WorkerUpdateHandler) RolloutUpdateHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			BatchSize     int     `json:"batch_size"`
			CanaryPercent float64 `json:"canary_percent"`
			ExcludeLocal  bool    `json:"exclude_local"`
			RolloutID     string  `json:"rollout_id"`
		}

		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid JSON body"})
			return
		}

		batchSize := body.BatchSize
		if batchSize <= 0 {
			batchSize = 10
		}

		canaryPercent := body.CanaryPercent
		if canaryPercent <= 0 {
			canaryPercent = 1.0
		}

		rolloutID := body.RolloutID
		if rolloutID == "" {
			rolloutID = uuid.New().String()
		}

		// Get eligible workers
		ctx := c.Request.Context()
		allWorkers := h.reg.List(ctx)

		eligible := []string{}
		for _, info := range allWorkers {
			if h.persistedReg != nil && h.persistedReg.IsRevoked(info.WorkerID) {
				continue
			}
			if info.Drain {
				continue
			}
			if body.ExcludeLocal {
				name := info.WorkerName
				if len(name) > 12 && name[:12] == "Local-Worker" {
					continue
				}
			}
			eligible = append(eligible, info.WorkerID)
		}

		if len(eligible) == 0 {
			c.JSON(http.StatusOK, gin.H{
				"status":     "no_workers",
				"message":    "No eligible workers for rollout",
				"rollout_id": rolloutID,
			})
			return
		}

		// Calculate canary batch
		totalWorkers := len(eligible)
		canaryCount := int(float64(totalWorkers) * canaryPercent / 100.0)
		if canaryCount < 1 {
			canaryCount = 1
		}

		canaryWorkers := eligible[:canaryCount]
		remainingWorkers := eligible[canaryCount:]

		// Build subsequent batches
		batches := [][]string{}
		for i := 0; i < len(remainingWorkers); i += batchSize {
			end := i + batchSize
			if end > len(remainingWorkers) {
				end = len(remainingWorkers)
			}
			batches = append(batches, remainingWorkers[i:end])
		}

		// Launch canary batch
		targetArtifactSHA := h.computeBundleSHA256()
		for _, wid := range canaryWorkers {
			h.cmdMgr.PushCommand(wid, "update_code", map[string]interface{}{
				"version":                h.codeVersion,
				"target_artifact_sha256": targetArtifactSHA,
			})
			h.cmdMgr.PushCommand(wid, "restart_worker", nil)
			h.cmdMgr.PushCommand(wid, "run_smoke_job", buildSmokeJobPayload(wid))
			h.updateMgr.RequestUpdate(wid, h.codeVersion)
		}

		log.Printf("🚀 Rollout update started (rollout_id=%s)", rolloutID)
		log.Printf("   Total eligible: %d, Canary: %d, Batches: %d", totalWorkers, len(canaryWorkers), len(batches))

		c.JSON(http.StatusOK, gin.H{
			"status":         "queued",
			"rollout_id":     rolloutID,
			"target_version": h.codeVersion,
			"canary_workers": canaryWorkers,
			"batches":        batches,
			"total_workers":  totalWorkers,
			"batch_size":     batchSize,
			"canary_percent": canaryPercent,
		})
	}
}

// UpdateStateHandler handles POST /worker/update_state
func (h *WorkerUpdateHandler) UpdateStateHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			WorkerID       string                 `json:"worker_id"`
			State          string                 `json:"state"`
			ArtifactSHA256 string                 `json:"artifact_sha256"`
			Version        string                 `json:"version"`
			Error          string                 `json:"error"`
			UpdateInfo     map[string]interface{} `json:"update_info"`
			NumeroEntita   int                    `json:"numero_entita"`
		}

		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid JSON body"})
			return
		}

		if body.WorkerID == "" || body.State == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "worker_id and state required"})
			return
		}
		if !h.authorizeWorkerRequest(c, body.WorkerID) {
			return
		}

		// Check if worker is revoked
		if h.persistedReg != nil && h.persistedReg.IsRevoked(body.WorkerID) {
			c.Status(http.StatusNoContent)
			return
		}

		// Get worker name for logging
		ctx := c.Request.Context()
		worker := h.reg.GetWorker(ctx, body.WorkerID)
		workerName := body.WorkerID[:min(16, len(body.WorkerID))] + "..."
		if worker != nil && worker.WorkerName != "" {
			workerName = worker.WorkerName
		}

		// Get target artifact SHA
		targetArtifactSHA := h.computeBundleSHA256()

		// Log based on state
		switch body.State {
		case "UPDATE_DOWNLOADED":
			artifactPreview := "N/A"
			if body.ArtifactSHA256 != "" {
				if len(body.ArtifactSHA256) > 16 {
					artifactPreview = body.ArtifactSHA256[:16] + "..."
				} else {
					artifactPreview = body.ArtifactSHA256
				}
			}
			log.Printf("📥 Worker %s: UPDATE_DOWNLOADED - zip downloaded, hash=%s", workerName, artifactPreview)

		case "UPDATE_APPLIED":
			log.Printf("✅ Worker %s: UPDATE_APPLIED - symlink updated, waiting for restart", workerName)
			if body.UpdateInfo != nil {
				log.Printf("   📁 Dirs updated: %v, Files updated: %v",
					body.UpdateInfo["dirs_updated"], body.UpdateInfo["files_updated"])
			}

		case "WORKER_ONLINE":
			// Check alignment
			isAligned := body.ArtifactSHA256 != "" && body.ArtifactSHA256 == targetArtifactSHA
			if isAligned {
				log.Printf("")
				log.Printf("✅ ========================================")
				log.Printf("🎉 Worker %s UPDATED AND ONLINE!", workerName)
				log.Printf("✅ ========================================")
				log.Printf("   📦 Artifact: %s...", body.ArtifactSHA256[:min(16, len(body.ArtifactSHA256))])
				log.Printf("   ✅ Aligned: YES")
				log.Printf("")
				// Clear pending update
				h.updateMgr.ClearUpdate(body.WorkerID)
			} else {
				log.Printf("Worker %s online with different artifact (not yet updated)", workerName)
			}

		case "UPDATE_FAILED":
			log.Printf("❌ Worker %s: UPDATE_FAILED - %s", workerName, body.Error)
		}

		c.JSON(http.StatusOK, gin.H{
			"status":    "ok",
			"worker_id": body.WorkerID,
			"state":     body.State,
		})
	}
}

// UpdateAckHandler handles POST /worker/update_ack (deprecated, backward compatibility)
func (h *WorkerUpdateHandler) UpdateAckHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			WorkerID     string `json:"worker_id"`
			LocalVersion string `json:"local_version"`
		}

		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid JSON body"})
			return
		}
		if body.WorkerID != "" && !h.authorizeWorkerRequest(c, body.WorkerID) {
			return
		}

		// Convert legacy ACK to UPDATE_APPLIED state
		if body.WorkerID != "" && body.LocalVersion != "" {
			// Mark update as acknowledged
			h.updateMgr.AckUpdate(body.WorkerID, body.LocalVersion)

			ctx := c.Request.Context()
			worker := h.reg.GetWorker(ctx, body.WorkerID)
			workerName := body.WorkerID[:min(16, len(body.WorkerID))] + "..."
			if worker != nil && worker.WorkerName != "" {
				workerName = worker.WorkerName
			}
			log.Printf("📝 Worker %s: Legacy ACK received (version: %s)", workerName, body.LocalVersion)
		}

		c.JSON(http.StatusOK, gin.H{
			"status":    "ack",
			"worker_id": body.WorkerID,
			"version":   body.LocalVersion,
		})
	}
}

// GetUpdateStatusHandler handles GET /workers/update_status
func (h *WorkerUpdateHandler) GetUpdateStatusHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := c.Request.Context()
		allWorkers := h.reg.List(ctx)

		status := make(map[string]interface{})
		targetArtifactSHA := h.computeBundleSHA256()

		for _, info := range allWorkers {
			pending := h.updateMgr.GetPendingUpdate(info.WorkerID)
			if pending != nil {
				status[info.WorkerID] = map[string]interface{}{
					"worker_name":            info.WorkerName,
					"target_version":         pending.Version,
					"target_artifact_sha256": targetArtifactSHA,
					"requested_at":           pending.RequestedAt,
					"ack":                    pending.Ack,
					"ack_version":            pending.AckVersion,
				}
			}
		}

		c.JSON(http.StatusOK, gin.H{
			"target_version":         h.codeVersion,
			"target_artifact_sha256": targetArtifactSHA,
			"updates":                status,
		})
	}
}

// Config returns the runtime config used by worker update handlers.
func (h *WorkerUpdateHandler) Config() *config.Config {
	return h.cfg
}

// GetBundleDownloadHandler handles GET /api/worker/bundle
func (h *WorkerUpdateHandler) GetBundleDownloadHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		platform := c.Query("platform")
		arch := c.Query("arch")

		if platform == "" {
			platform = "linux"
		}
		if arch == "" {
			arch = "x86_64"
		}

		// Look for bundle file
		bundleName := fmt.Sprintf("worker_code_%s_%s.zip", platform, arch)
		bundlePath := filepath.Join(h.bundleDir, bundleName)

		// Fallback to generic bundle
		if _, err := os.Stat(bundlePath); os.IsNotExist(err) {
			bundlePath = filepath.Join(h.bundleDir, "worker_code.zip")
		}

		if _, err := os.Stat(bundlePath); os.IsNotExist(err) {
			c.JSON(http.StatusNotFound, gin.H{"error": "Bundle not found"})
			return
		}

		c.FileAttachment(bundlePath, filepath.Base(bundlePath))
	}
}

// formatSize formats bytes to human readable string
func formatSize(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

// GetBundleFilesHandler handles GET /api/worker/bundle/files
func (h *WorkerUpdateHandler) GetBundleFilesHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		platform := c.DefaultQuery("platform", "linux")
		arch := c.DefaultQuery("arch", "x86_64")
		searchPath := c.Query("path")
		if searchPath == "" {
			searchPath = c.Query("prefix")
		}

		// Look for bundle file
		bundleName := fmt.Sprintf("worker_code_%s_%s.zip", platform, arch)
		bundlePath := filepath.Join(h.bundleDir, bundleName)
		if _, err := os.Stat(bundlePath); os.IsNotExist(err) {
			bundlePath = filepath.Join(h.bundleDir, "worker_code.zip")
		}

		r, err := zip.OpenReader(bundlePath)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "Bundle not found or invalid"})
			return
		}
		defer r.Close()

		var results []gin.H
		for _, f := range r.File {
			// If path is provided, only show files in that directory
			if searchPath != "" {
				rel, err := filepath.Rel(searchPath, f.Name)
				if err != nil || rel == ".." || filepath.IsAbs(rel) {
					continue
				}
				// Only show immediate children if we want a tree-like view,
				// but the frontend seems to expect full paths or we filter here.
				// For now let's just return matches starting with path
			}

			if searchPath != "" && !filepath.HasPrefix(f.Name, searchPath) {
				continue
			}

			results = append(results, gin.H{
				"name":           f.Name,
				"size":           f.UncompressedSize64,
				"size_formatted": formatSize(int64(f.UncompressedSize64)),
				"compressed":     f.CompressedSize64,
			})

			if len(results) >= 1000 { // Safety limit
				break
			}
		}

		c.JSON(http.StatusOK, gin.H{
			"files":       results,
			"total_count": len(results),
		})
	}
}

// GetLatestBundleHandler handles GET /install_worker/latest
// Returns current bundle hash and manifest URL.
func (h *WorkerUpdateHandler) GetLatestBundleHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		platform := c.DefaultQuery("platform", "linux")
		arch := c.DefaultQuery("arch", "x86_64")
		bundlePath, info, err := resolveBundlePath(h.bundleDir, platform, arch)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "Bundle not found"})
			return
		}
		bundleHash := computeFileSHA256(bundlePath)
		c.JSON(http.StatusOK, gin.H{
			"bundle_hash":  bundleHash,
			"manifest_url": fmt.Sprintf("/install_worker/manifest/%s?platform=%s&arch=%s", bundleHash, platform, arch),
			"updated_at":   info.ModTime().UTC().Format(time.RFC3339),
			"filename":     filepath.Base(bundlePath),
		})
	}
}

// GetBundleManifestByHashHandler handles GET /install_worker/manifest/{bundle_hash}
// Returns file list + dir_hash. If hash does not match current bundle, returns 404.
func (h *WorkerUpdateHandler) GetBundleManifestByHashHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		platform := c.DefaultQuery("platform", "linux")
		arch := c.DefaultQuery("arch", "x86_64")
		bundleHash := strings.TrimSpace(c.Param("bundle_hash"))
		if bundleHash == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "bundle_hash required"})
			return
		}
		bundlePath, info, err := resolveBundlePath(h.bundleDir, platform, arch)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "Bundle not found"})
			return
		}
		currentHash := computeFileSHA256(bundlePath)
		if currentHash != bundleHash {
			c.JSON(http.StatusNotFound, gin.H{"error": "bundle hash mismatch", "current": currentHash})
			return
		}

		files, dirHashes, err := listZipFilesWithHashes(bundlePath)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to read bundle"})
			return
		}
		c.JSON(http.StatusOK, gin.H{
			"bundle_hash": bundleHash,
			"platform":    platform,
			"arch":        arch,
			"file_count":  len(files),
			"files":       files,
			"dir_hash":    dirHashes,
			"updated_at":  info.ModTime().UTC().Format(time.RFC3339),
		})
	}
}

// ForceRegenerateZipHandler handles POST /install_worker/force_regenerate_zip
// It triggers a background rebuild of worker_code.zip using the local tool.
func (h *WorkerUpdateHandler) ForceRegenerateZipHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		wait := c.DefaultQuery("wait", "0") == "1"
		log.Printf("🔍 DEBUG: h.bundleDir = %q", h.bundleDir)
		repoRoot := findRepoRootFrom(h.bundleDir)
		log.Printf("🔍 DEBUG: repoRoot = %q", repoRoot)
		if repoRoot == "" {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "repo root not found for rebuild tool", "bundleDir": h.bundleDir})
			return
		}
		scriptPath := filepath.Join(repoRoot, "DataServer", "cmd", "velox-bundler", "main.go")
		if _, err := os.Stat(scriptPath); err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "velox-bundler entrypoint not found", "path": scriptPath})
			return
		}

		run := func() (string, error) {
			outputDir := h.bundleDir
			cmd := exec.Command("go", "run", "./cmd/velox-bundler", "--source", repoRoot, "--output", outputDir)
			cmd.Dir = filepath.Join(repoRoot, "DataServer")

			out, err := cmd.CombinedOutput()
			if err != nil {
				log.Printf("❌ rebuild bundle failed: %v | %s", err, strings.TrimSpace(string(out)))
				return "", err
			}
			log.Printf("✅ rebuild bundle V2 completed: %s", strings.TrimSpace(string(out)))
			bundlePath, _, err := resolveBundlePath(h.bundleDir, "linux", "x86_64")
			if err != nil {
				return "", err
			}
			return computeFileSHA256(bundlePath), nil
		}

		if wait {
			newHash, err := run()
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "rebuild failed"})
				return
			}
			c.JSON(http.StatusOK, gin.H{
				"ok":              true,
				"message":         "bundle rebuild completed",
				"new_bundle_hash": newHash,
				"script":          scriptPath,
			})
			return
		}

		go func() { _, _ = run() }()
		c.JSON(http.StatusAccepted, gin.H{
			"ok":      true,
			"message": "bundle rebuild started",
			"script":  scriptPath,
		})
	}
}

// computeBundleSHA256 computes SHA256 of the worker bundle
func (h *WorkerUpdateHandler) computeBundleSHA256() string {
	bundlePath := filepath.Join(h.bundleDir, "worker_code.zip")

	file, err := os.Open(bundlePath)
	if err != nil {
		return ""
	}
	defer file.Close()

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return ""
	}

	return hex.EncodeToString(hash.Sum(nil))
}

// computeFileSHA256 computes SHA256 of any file path
func computeFileSHA256(path string) string {
	file, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer file.Close()

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return ""
	}
	return hex.EncodeToString(hash.Sum(nil))
}

// bundleInspection holds result of inspecting zip contents (file count, top dirs, runtime flags).
type bundleInspection struct {
	FileCount int                    `json:"file_count"`
	TopDirs   []gin.H                `json:"top_dirs"`
	Runtime   map[string]interface{} `json:"runtime"`
}

// inspectBundleZip opens the zip and returns file count, top-level dirs, and runtime presence flags.
func inspectBundleZip(bundlePath string) (bundleInspection, error) {
	out := bundleInspection{
		TopDirs: []gin.H{},
		Runtime: map[string]interface{}{
			"node": false, "npm": false,
		},
	}
	r, err := zip.OpenReader(bundlePath)
	if err != nil {
		return out, err
	}
	defer r.Close()

	// Top-level dir name -> { uncompressed size, file count }
	dirSizes := make(map[string]int64)
	dirCounts := make(map[string]int)

	for _, f := range r.File {
		out.FileCount++
		name := strings.TrimPrefix(filepath.ToSlash(f.Name), "./")
		if name == "" || strings.HasSuffix(name, "/") {
			continue
		}
		parts := strings.SplitN(name, "/", 2)
		top := parts[0]
		dirSizes[top] += int64(f.UncompressedSize64)
		dirCounts[top]++

		lower := strings.ToLower(name)
		if strings.Contains(lower, "runtime/node") || strings.HasPrefix(lower, "node/") || top == "node" {
			out.Runtime["node"] = true
		}
		if strings.Contains(lower, "node_modules") || strings.Contains(lower, "package.json") || top == "npm" {
			out.Runtime["npm"] = true
		}
		if strings.Contains(lower, "voiceover") || strings.Contains(lower, "voices") {
			out.Runtime["voiceover_deps"] = true
		}
		if strings.HasPrefix(lower, "refactored/") || top == "refactored" {
			out.Runtime["refactored_root"] = true
		}
	}

	for name, size := range dirSizes {
		out.TopDirs = append(out.TopDirs, gin.H{
			"name":           name,
			"type":           "folder",
			"size":           size,
			"size_formatted": formatSize(size),
			"file_count":     dirCounts[name],
		})
	}

	return out, nil
}

// GetBundleManifestHandler handles GET /bundle_manifest.json
func (h *WorkerUpdateHandler) GetBundleManifestHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		platform := c.DefaultQuery("platform", "linux")
		arch := c.DefaultQuery("arch", "x86_64")

		bundleName := fmt.Sprintf("worker_code_%s_%s.zip", platform, arch)
		bundlePath := filepath.Join(h.bundleDir, bundleName)
		if _, err := os.Stat(bundlePath); os.IsNotExist(err) {
			bundlePath = filepath.Join(h.bundleDir, "worker_code.zip")
		}
		info, err := os.Stat(bundlePath)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "Bundle not found"})
			return
		}
		actualSHA := computeFileSHA256(bundlePath)

		manifest := gin.H{
			"version":        h.cfg.VersionNumber,
			"code_version":   h.codeVersion,
			"platform":       platform,
			"arch":           arch,
			"filename":       filepath.Base(bundlePath),
			"url":            fmt.Sprintf("/api/worker/bundle?platform=%s&arch=%s", platform, arch),
			"sha256":         actualSHA,
			"size":           info.Size(),
			"size_formatted": formatSize(info.Size()),
			"updated_at":     info.ModTime().UTC().Format(time.RFC3339),
			"created_at":     info.ModTime().UTC().Format(time.RFC3339),
		}

		// Inspect zip to get file_count, top_dirs, runtime (so frontend does not show "missing")
		if insp, err := inspectBundleZip(bundlePath); err == nil {
			manifest["file_count"] = insp.FileCount
			manifest["top_dirs"] = insp.TopDirs
			manifest["runtime"] = insp.Runtime
		}

		// Read V2 manifest generated by Go bundler
		manifestV2Path := filepath.Join(h.bundleDir, "manifest_v2.json")
		if raw, err := os.ReadFile(manifestV2Path); err == nil {
			var v2 map[string]interface{}
			if err := json.Unmarshal(raw, &v2); err == nil {
				if v, ok := v2["version"]; ok {
					manifest["version"] = v
				}
				if v, ok := v2["build_hash"]; ok {
					manifest["build_id"] = v
					manifest["bundle_sha256"] = v
				}
				if v, ok := v2["timestamp"]; ok {
					manifest["created_at"] = v
					manifest["updated_at"] = v
				}
			}
		} else {
			// Fallback: Optional metadata enrichment from release.json if present.
			releasePath := filepath.Join(h.bundleDir, "release.json")
			if raw, err := os.ReadFile(releasePath); err == nil {
				var rel map[string]interface{}
				if err := json.Unmarshal(raw, &rel); err == nil {
					if v, ok := rel["version"]; ok {
						manifest["version"] = v
					}
					if v, ok := rel["build_id"]; ok {
						manifest["build_id"] = v
					}
					if v, ok := rel["created_at"]; ok {
						manifest["created_at"] = v
					}
				}
			}
		}

		// If current source hash differs from bundle source hash, force mismatch
		if currentHash, ok := readTextFileTrim(filepath.Join(h.bundleDir, "source_hash.txt")); ok {
			manifest["source_hash_current"] = currentHash
			if bundleHash, ok2 := manifest["source_hash_bundle"].(string); ok2 && bundleHash != "" && bundleHash != currentHash {
				manifest["bundle_stale"] = true
				manifest["bundle_sha256_actual"] = actualSHA
				manifest["sha256"] = computeStringSHA256Hex("force-mismatch:" + currentHash)
			}
		}

		c.JSON(http.StatusOK, manifest)
	}
}

// splitVersion splits a version string like "1.0.0|abc123" into parts
func splitVersion(v string) []string {
	parts := []string{}
	current := ""
	for _, c := range v {
		if c == '|' {
			parts = append(parts, current)
			current = ""
		} else {
			current += string(c)
		}
	}
	if current != "" {
		parts = append(parts, current)
	}
	return parts
}

// SavePendingUpdates saves pending updates to a JSON file
func (h *WorkerUpdateHandler) SavePendingUpdates() error {
	// This would save to pending_updates.json
	// For now, updates are kept in memory
	return nil
}

// LoadPendingUpdates loads pending updates from a JSON file
func (h *WorkerUpdateHandler) LoadPendingUpdates() error {
	// This would load from pending_updates.json
	// For now, updates are kept in memory
	return nil
}

// GetManifestV2Handler handles GET /api/worker/v2/manifest
func (h *WorkerUpdateHandler) GetManifestV2Handler() gin.HandlerFunc {
	return func(c *gin.Context) {
		manifestPath := filepath.Join(h.bundleDir, "manifest_v2.json")
		if _, err := os.Stat(manifestPath); os.IsNotExist(err) {
			c.JSON(http.StatusNotFound, gin.H{"error": "Manifest V2 not found"})
			return
		}
		c.File(manifestPath)
	}
}

// GetChunkV2Handler handles GET /api/worker/v2/chunk/:chunkName
func (h *WorkerUpdateHandler) GetChunkV2Handler() gin.HandlerFunc {
	return func(c *gin.Context) {
		chunkName := c.Param("chunkName")
		if chunkName == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Chunk name required"})
			return
		}

		if strings.Contains(chunkName, "/") || strings.Contains(chunkName, "\\") {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid chunk name"})
			return
		}

		chunkPath := filepath.Join(h.bundleDir, chunkName)
		stat, err := os.Stat(chunkPath)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "Chunk not found"})
			return
		}

		if stat.IsDir() {
			c.JSON(http.StatusForbidden, gin.H{"error": "Cannot serve directory"})
			return
		}

		file, err := os.Open(chunkPath)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to read chunk"})
			return
		}
		defer file.Close()

		etag := fmt.Sprintf(`"%x-%x"`, stat.Size(), stat.ModTime().UnixNano())
		c.Header("ETag", etag)

		if match := c.GetHeader("If-None-Match"); match == etag {
			c.Status(http.StatusNotModified)
			return
		}

		http.ServeContent(c.Writer, c.Request, filepath.Base(chunkPath), stat.ModTime(), file)
	}
}

// marshalJSON is a helper for JSON marshaling
func marshalJSON(v interface{}) string {
	data, _ := json.Marshal(v)
	return string(data)
}
