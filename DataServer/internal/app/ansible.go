package app

import (
	"log"
	"os"
	"strings"

	"github.com/gin-gonic/gin"

	"velox-server/internal/config"
	remoteansible "velox-server/internal/handlers/remote/ansible"
	"velox-server/internal/store"
)

// AnsibleModule provides Ansible deployment endpoints.
type AnsibleModule struct {
	cfg       *config.Config
	dataDir   string
	adminAuth gin.HandlerFunc
	handlers  *remoteansible.AnsibleHandlers
	masterURL string
	store     *store.SQLiteStore
}

func NewAnsibleModule(cfg *config.Config, dataDir string, adminAuth gin.HandlerFunc, sqliteStore *store.SQLiteStore) *AnsibleModule {
	masterURL := cfg.Workers.MasterURL
	if strings.TrimSpace(masterURL) == "" {
		masterURL = config.GetAnsibleMasterURL()
	}
	if strings.TrimSpace(masterURL) == "" {
		masterURL = remoteansible.DetectLocalMasterURL()
	}
	return &AnsibleModule{
		cfg:       cfg,
		dataDir:   dataDir,
		adminAuth: adminAuth,
		masterURL: masterURL,
		store:     sqliteStore,
	}
}

func (m *AnsibleModule) Name() string {
	return "ansible"
}

func (m *AnsibleModule) Handlers() *remoteansible.AnsibleHandlers {
	return m.handlers
}

func (m *AnsibleModule) RegisterRoutes(r *gin.Engine) {
	if err := os.MkdirAll(m.cfg.Ansible.PlaybookDir, 0755); err != nil {
		log.Printf("[ANSIBLE] Cannot create playbook dir %s: %v", m.cfg.Ansible.PlaybookDir, err)
		return
	}

	// PR-ANSIBLE-SOT: both managers now take the SQLite store at
	// construction. SetStore + LoadComputers + loadRuns are gone;
	// every read hits SQLite on-the-fly so the canonical query
	// results are never shadowed by a stale in-RAM mirror.
	ansibleManager := remoteansible.NewAnsibleRunManager(m.cfg.Ansible.PlaybookDir, m.dataDir, m.store)
	computerMgr := remoteansible.NewAnsibleComputerManager(m.dataDir, m.store)
	ansibleManager.SetComputerManager(computerMgr)

	handlers := remoteansible.NewAnsibleHandlers(ansibleManager)
	handlers.SetComputerManager(computerMgr, m.dataDir)
	handlers.SetMasterURL(m.masterURL)
	m.handlers = handlers

	v1Admin := r.Group("/api/v1/admin")
	if m.adminAuth != nil {
		v1Admin.Use(m.adminAuth)
	}
	v1Admin.GET("/ansible/computers/summary", m.handlers.AnsibleComputersSummaryHandler)
	v1Admin.GET("/ansible/computers/list", m.handlers.AnsibleComputersListHandler)
	v1Admin.POST("/ansible/computers", m.handlers.AnsibleComputersSaveHandler)
	v1Admin.DELETE("/ansible/computers/:id", m.handlers.AnsibleComputersDeleteHandler)
	v1Admin.GET("/ansible/computers/logs/:id", m.handlers.AnsibleComputersLogsHandler)
	v1Admin.POST("/ansible/computers/run_action", m.handlers.RunActionHandler)
	v1Admin.POST("/ansible/computers/test_ssh", m.handlers.TestSSHHandler)
	v1Admin.POST("/ansible/shell", m.handlers.RunShellHandler)
	v1Admin.GET("/ansible/capabilities", m.handlers.GetCapabilitiesHandler)
	v1Admin.GET("/ansible/runs", m.handlers.GetRunsHandler)
	v1Admin.GET("/ansible/runs/:id", m.handlers.GetRunHandler)

	if ansibleManager.Ready() {
		log.Printf("[ANSIBLE] Routes registered (playbooks: %s)", m.cfg.Ansible.PlaybookDir)
	} else {
		log.Printf("[ANSIBLE] Routes registered (ansible-playbook not found)")
	}
}
