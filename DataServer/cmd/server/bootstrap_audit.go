package main

import (
	"log"
	"path/filepath"

	"velox-server/internal/audit"
	"velox-server/internal/config"
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
		log.Printf("[AUDIT] Data layer audit FAILED with %d errors", len(result.Errors))
		for _, e := range result.Errors {
			log.Printf("[AUDIT] ERROR: %s", e)
		}
		return result.FailOnError()
	}
	if len(result.Warnings) > 0 {
		log.Printf("[AUDIT] Data layer audit passed with %d warnings", len(result.Warnings))
		for _, w := range result.Warnings {
			log.Printf("[AUDIT] WARNING: %s", w)
		}
	} else {
		log.Printf("[AUDIT] Data layer audit PASSED")
	}
	return nil
}
