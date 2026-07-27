package config

import (
	"os"
	"strings"
)

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

// ── M2MConfig ──────────────────────────────────────────────────────────

// loadM2MConfig populates M2MConfig from environment variables. Each
// knob is independent (an operator can override only one without
// re-declaring the others) and the loads use the same intFromEnv /
// floatFromEnv helpers the rest of the codebase uses so a malformed
// value produces a loud boot-time failure rather than runtime drift.
//
// Default values are conservative enough to be safe-by-default:
//
//   - DefaultRPS: 5 req/sec sustained. Lower than the per-handler
//     measurable cost (`ValidateSubmitJobRequest` +
//     creatorflow.Resolver hash-compute) so a single client cannot
//     saturate the master.
//   - DefaultBurst: 10 (≈ 2s at rps=5). Legitimate scripted bursts
//     (an automation flushing queued events) fit; sustained spikes
//     get throttled.
//   - MaxScenesPerRequest: 1000. A real video composition rarely
//     exceeds this; 10× anything serves a misconfigured / abusive
//     load. The validator's own MaxScenes (10000) is an EVEN HIGHER
//     hard cap that catches malformed bodies before this quota —
//     the two surfaces are intentionally layered.
//   - MaxTotalDurationSecondsPerRequest: 3600s (1h). Again
//     misconfiguration-vs-abuse trade-off; legitimate producers
//     handle full-day videos via a sequence of submits, not one.
func loadM2MConfig() M2MConfig {
	return M2MConfig{
		DefaultRPS:                         intFromEnv("VELOX_M2M_DEFAULT_RPS", 5, 1),
		DefaultBurst:                       intFromEnv("VELOX_M2M_DEFAULT_BURST", 10, 1),
		MaxScenesPerRequest:                intFromEnv("VELOX_M2M_MAX_SCENES_PER_REQUEST", 1000, 1),
		MaxTotalDurationSecondsPerRequest:  floatFromEnv("VELOX_M2M_MAX_TOTAL_DURATION_SECS", 3600, 0),
	}
}

// loadAllowedExternalDomains parses the VELOX_ALLOWED_EXTERNAL_DOMAINS
// CSV into a slice. Each entry is trimmed + lowercased at load so the
// validator's `hostnameAllowed` can match without re-checking for case.
// Empty strings and whitespace-only entries are filtered out (the
// validator already does its own trim, but skipping here is cheaper).
//
// A wildcard entry `*.foo.com` is preserved as-is — the matcher in
// ssrf_url.go treats the literal `*.` prefix as a wildcard token.
func loadAllowedExternalDomains() []string {
	raw := os.Getenv("VELOX_ALLOWED_EXTERNAL_DOMAINS")
	if raw = strings.TrimSpace(raw); raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		p = strings.ToLower(p)
		if p == "" {
			continue
		}
		out = append(out, p)
	}
	return out
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
