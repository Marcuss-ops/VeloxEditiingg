package workers

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"velox-server/internal/config"
	workersreg "velox-server/internal/workers"

	"github.com/gin-gonic/gin"
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

func bundleDirCandidates(dataDir string) []string {
	if dataDir == "" {
		return []string{"worker_downloads"}
	}
	base := filepath.Join(dataDir, "..", "..")
	return []string{
		filepath.Join(base, "worker_downloads"),
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

// Config returns the runtime config used by worker update handlers.
func (h *WorkerUpdateHandler) Config() *config.Config {
	return h.cfg
}

// SavePendingUpdates saves pending updates to a JSON file
func (h *WorkerUpdateHandler) SavePendingUpdates() error {
	return nil
}

// LoadPendingUpdates loads pending updates from a JSON file
func (h *WorkerUpdateHandler) LoadPendingUpdates() error {
	return nil
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

// marshalJSON is a helper for JSON marshaling
func marshalJSON(v interface{}) string {
	data, _ := json.Marshal(v)
	return string(data)
}
