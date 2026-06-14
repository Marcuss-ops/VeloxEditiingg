package ansible

import (
	"context"
	"log"
	"os"
	"strings"

	"github.com/gin-gonic/gin"
	"velox-server/internal/app"
	"velox-server/internal/config"
	remoteansible "velox-server/internal/handlers/remote/ansible"
	"velox-server/internal/store"
)

// Module provides Ansible deployment endpoints.
type Module struct {
	app.BaseModule
	cfg       *config.Config
	dataDir   string
	adminAuth gin.HandlerFunc
	handlers  *remoteansible.AnsibleHandlers
	masterURL string
	store     *store.SQLiteStore
}

func New(cfg *config.Config, dataDir string, adminAuth gin.HandlerFunc, sqliteStore *store.SQLiteStore) *Module {
	masterURL := cfg.MasterURL
	if strings.TrimSpace(masterURL) == "" {
		masterURL = os.Getenv("VELOX_MASTER_URL")
	}
	if strings.TrimSpace(masterURL) == "" {
		masterURL = os.Getenv("VELOX_MASTER_SERVER_URL")
	}
	if strings.TrimSpace(masterURL) == "" {
		masterURL = remoteansible.DetectLocalMasterURL()
	}
	return &Module{
		cfg:       cfg,
		dataDir:   dataDir,
		adminAuth: adminAuth,
		masterURL: masterURL,
		store:     sqliteStore,
	}
}

func (m *Module) Name() string {
	return "ansible"
}

func (m *Module) Handlers() *remoteansible.AnsibleHandlers {
	return m.handlers
}

func (m *Module) RegisterRoutes(r *gin.Engine) {
	if err := os.MkdirAll(m.cfg.PlaybookDir, 0755); err != nil {
		log.Printf("[ANSIBLE] Cannot create playbook dir %s: %v", m.cfg.PlaybookDir, err)
		return
	}

	ansibleManager := remoteansible.NewAnsibleRunManager(m.cfg.PlaybookDir, m.dataDir, m.store)
	computerMgr := remoteansible.NewAnsibleComputerManager(m.dataDir)
	if m.store != nil {
		computerMgr.SetStore(m.store)
	}
	ansibleManager.SetComputerManager(computerMgr)
	if err := computerMgr.LoadComputers(); err != nil {
		log.Printf("[ANSIBLE] Failed to load computers: %v", err)
	}

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

	if ansibleManager.Ready() {
		log.Printf("[ANSIBLE] Routes registered (playbooks: %s)", m.cfg.PlaybookDir)
	} else {
		log.Printf("[ANSIBLE] Routes registered (ansible-playbook not found)")
	}
}

func (m *Module) Start(ctx context.Context) error {
	log.Printf("[ANSIBLE] Module started")
	return nil
}

func (m *Module) Stop(ctx context.Context) error {
	log.Printf("[ANSIBLE] Module stopped")
	return nil
}
