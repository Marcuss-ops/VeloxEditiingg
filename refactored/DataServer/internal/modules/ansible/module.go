package ansible

import (
	"context"
	"log"
	"os"

	"github.com/gin-gonic/gin"
	"velox-server/internal/app"
	"velox-server/internal/config"
	remoteansible "velox-server/internal/handlers/remote/ansible"
)

// Module provides Ansible deployment endpoints.
type Module struct {
	app.BaseModule
	cfg            *config.Config
	dataDir        string
	handlers       *remoteansible.AnsibleHandlers
	masterURL      string
}

// New creates a new Ansible module.
func New(cfg *config.Config, dataDir string) *Module {
	return &Module{
		cfg:       cfg,
		dataDir:   dataDir,
		masterURL: cfg.MasterURL,
	}
}

// Name returns the module identifier.
func (m *Module) Name() string {
	return "ansible"
}

// Handlers returns the Ansible handlers (for use by other modules).
func (m *Module) Handlers() *remoteansible.AnsibleHandlers {
	return m.handlers
}

// RegisterRoutes registers Ansible endpoints.
func (m *Module) RegisterRoutes(r *gin.Engine) {
	// Initialize Ansible handlers
	if err := os.MkdirAll(m.cfg.PlaybookDir, 0755); err != nil {
		log.Printf("[ANSIBLE] Cannot create playbook dir %s: %v", m.cfg.PlaybookDir, err)
		return
	}

	ansibleManager := remoteansible.NewAnsibleRunManager(m.cfg.PlaybookDir, m.dataDir)
	computerMgr := remoteansible.NewAnsibleComputerManager(m.dataDir)
	if err := computerMgr.LoadComputers(); err != nil {
		log.Printf("[ANSIBLE] Failed to load computers: %v", err)
	}

	handlers := remoteansible.NewAnsibleHandlers(ansibleManager)
	handlers.SetComputerManager(computerMgr, m.dataDir)
	handlers.SetMasterURL(m.masterURL)
	m.handlers = handlers

	// Register Ansible routes (admin only)
	v1Admin := r.Group("/api/v1/admin")
	// Note: admin auth middleware should be applied here
	v1Admin.GET("/ansible/computers/summary", m.handlers.AnsibleComputersSummaryHandler)
	v1Admin.GET("/ansible/computers/list", m.handlers.AnsibleComputersListHandler)
	v1Admin.POST("/ansible/computers", m.handlers.AnsibleComputersSaveHandler)
	v1Admin.DELETE("/ansible/computers/:id", m.handlers.AnsibleComputersDeleteHandler)
	v1Admin.GET("/ansible/computers/logs/:id", m.handlers.AnsibleComputersLogsHandler)

	if ansibleManager.Ready() {
		log.Printf("[ANSIBLE] Routes registered (playbooks: %s)", m.cfg.PlaybookDir)
	} else {
		log.Printf("[ANSIBLE] Routes registered (ansible-playbook not found)")
	}
}

// Start initializes the module.
func (m *Module) Start(ctx context.Context) error {
	log.Printf("[ANSIBLE] Module started")
	return nil
}

// Stop gracefully shuts down the module.
func (m *Module) Stop(ctx context.Context) error {
	log.Printf("[ANSIBLE] Module stopped")
	return nil
}
