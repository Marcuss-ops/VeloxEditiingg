package config

import "os"

// loadPipelineConfig populates PipelineConfig from environment variables.
// Spec §8: cfg.Pipeline.JobMasterURL replaces the previously-root Config.JobMasterURL.
func loadPipelineConfig() PipelineConfig {
	return PipelineConfig{
		JobMasterURL: os.Getenv("VELOX_JOB_MASTER_URL"),
	}
}
