package jobs

import (
	"context"
	"log"

	"github.com/gin-gonic/gin"
	"velox-server/internal/app"
	"velox-server/internal/config"
	jobapi "velox-server/internal/handlers/server/jobs"
	"velox-server/internal/queue"
)

// Module provides job management endpoints.
type Module struct {
	app.BaseModule
	cfg           *config.Config
	fileQ         *queue.FileQueue
	jobAPI        *jobapi.JobAPI
	submitHandler *jobapi.JobSubmissionHandler
}

// New creates a new jobs module.
func New(cfg *config.Config, fileQ *queue.FileQueue, jobAPI *jobapi.JobAPI, submitHandler *jobapi.JobSubmissionHandler) *Module {
	return &Module{
		cfg:           cfg,
		fileQ:         fileQ,
		jobAPI:        jobAPI,
		submitHandler: submitHandler,
	}
}

// Name returns the module identifier.
func (m *Module) Name() string {
	return "jobs"
}

// RegisterRoutes registers job management endpoints.
func (m *Module) RegisterRoutes(r *gin.Engine) {
	// Job CRUD endpoints (admin protected)
	v1 := r.Group("/api/v1/jobs")
	// Note: admin auth middleware should be applied here
	// For now, we keep the existing pattern from router.go

	// These will be registered through the existing API v1 routes
	// This module provides the core job management logic
	log.Printf("[JOBS MODULE] Routes registered")
}

// Start initializes the module.
func (m *Module) Start(ctx context.Context) error {
	log.Printf("[JOBS MODULE] Started")
	return nil
}

// Stop gracefully shuts down the module.
func (m *Module) Stop(ctx context.Context) error {
	log.Printf("[JOBS MODULE] Stopped")
	return nil
}
