package config

import "os"

// ── NVIDIAConfig ────────────────────────────────────────────────────────

func loadNVIDIAConfig() NVIDIAConfig {
	return NVIDIAConfig{
		APIKey:  os.Getenv("VELOX_NVIDIA_API_KEY"),
		TextURL: os.Getenv("VELOX_NVIDIA_TEXT_URL"),
	}
}

// ── AuthConfig ──────────────────────────────────────────────────────────

func loadAuthConfig() AuthConfig {
	c := AuthConfig{
		AdminToken: os.Getenv("VELOX_ADMIN_TOKEN"),
	}
	if c.AdminToken == "" {
		c.AdminToken = os.Getenv("MASTER_ADMIN_TOKEN")
	}
	// InstaEdit→Velox JWT secret. Distinct from SOCIAL_API_TOKEN
	// (which authenticates the reverse direction). Empty means the
	// instaeditauth verifier is not configured; the middleware
	// surfaces 503 so a misconfigured deployment fails loudly.
	c.InstaeditControlJWTSecret = os.Getenv("INSTAEDIT_CONTROL_JWT_SECRET")
	return c
}

// ── PipelineConfig ──────────────────────────────────────────────────────

// loadPipelineConfig populates PipelineConfig from environment variables.
// Spec §8: cfg.Pipeline.JobMasterURL replaces the previously-root Config.JobMasterURL.
func loadPipelineConfig() PipelineConfig {
	return PipelineConfig{
		JobMasterURL: os.Getenv("VELOX_JOB_MASTER_URL"),
		OllamaURL:    firstNonEmpty(os.Getenv("OLLAMA_ADDR"), "http://127.0.0.1:11434"),
		OllamaModel:  firstNonEmpty(os.Getenv("OLLAMA_MODEL"), "gemma4:e4b"),
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

// ── FrontendConfig ─────────────────────────────────────────────────────

func loadFrontendConfig() FrontendConfig {
	c := FrontendConfig{
		SPADir:          os.Getenv("VELOX_SPA_DIR"),
		GradioAppURL:    os.Getenv("VELOX_GRADIO_APP_URL"),
		DarkEditorDir:   os.Getenv("VELOX_DARK_EDITOR_DIR"),
		DarkEditorProxy: os.Getenv("VELOX_DARK_EDITOR_PROXY_URL"),
	}
	if c.GradioAppURL == "" {
		c.GradioAppURL = "http://127.0.0.1:7860"
	}
	return c
}
