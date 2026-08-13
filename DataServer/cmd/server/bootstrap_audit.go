package main

import (
	"context"
	"path/filepath"

	"velox-server/internal/audit"
	"velox-server/internal/config"
	"velox-server/internal/logging"
)

func runDataLayerAudit(cfg *config.Config) error {
	dataDir := cfg.Runtime.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	secretsDir := filepath.Join(dataDir, "secrets")
	auditor := audit.NewDataLayerAuditor(dataDir, secretsDir, cfg.Database.DBPath)
	result := auditor.Audit()
	if !result.Passed {
		logServerf(context.Background(), logging.LevelError, logging.CodeServerAuditError, "[AUDIT] Data layer audit FAILED with %d errors", len(result.Errors))
		for _, e := range result.Errors {
			logServerf(context.Background(), logging.LevelError, logging.CodeServerAuditError, "[AUDIT] ERROR: %s", e)
		}
		return result.FailOnError()
	}
	if len(result.Warnings) > 0 {
		logServerf(context.Background(), logging.LevelWarn, logging.CodeServerAuditWarn, "[AUDIT] Data layer audit passed with %d warnings", len(result.Warnings))
		for _, w := range result.Warnings {
			logServerf(context.Background(), logging.LevelWarn, logging.CodeServerAuditWarn, "[AUDIT] WARNING: %s", w)
		}
	} else {
		logServerf(context.Background(), logging.LevelInfo, logging.CodeServerAudit, "[AUDIT] Data layer audit PASSED")
	}
	return nil
}
