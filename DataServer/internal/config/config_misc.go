package config

import (
	"strings"
)

// ── AuthConfig ──────────────────────────────────────────────────────────

func loadAuthConfig(raw RawConfig) AuthConfig {
	c := AuthConfig{
		AdminToken:                   raw.Get("VELOX_ADMIN_TOKEN"),
		ProjectBridgeContractVersion: firstNonEmpty(raw.Get("VELOX_PROJECT_BRIDGE_CONTRACT_VERSION"), "instaedit.velox.project-bridge.v1"),
	}
	if c.AdminToken == "" {
		c.AdminToken = raw.Get("MASTER_ADMIN_TOKEN")
	}
	// InstaEdit→Velox JWT secret. Distinct from SOCIAL_API_TOKEN
	// (which authenticates the reverse direction). Empty means the
	// instaeditauth verifier is not configured; the middleware
	// surfaces 503 so a misconfigured deployment fails loudly.
	c.InstaeditControlJWTSecret = raw.Get("INSTAEDIT_CONTROL_JWT_SECRET")
	c.VeloxWebhookSecret = raw.Get("VELOX_WEBHOOK_SECRET")
	return c
}

// ── PipelineConfig ──────────────────────────────────────────────────────

// loadPipelineConfig populates the live pipeline settings from the
// captured environment snapshot. Job-master proxy routing is retired;
// only the native translation settings remain here.
func loadPipelineConfig(raw RawConfig) PipelineConfig {
	return PipelineConfig{
		OllamaURL:   firstNonEmpty(raw.Get("OLLAMA_ADDR"), "http://127.0.0.1:11434"),
		OllamaModel: firstNonEmpty(raw.Get("OLLAMA_MODEL"), "gemma4:e4b"),
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
func loadM2MConfig(raw RawConfig) M2MConfig {
	return M2MConfig{
		DefaultRPS:                        raw.Int("VELOX_M2M_DEFAULT_RPS", 5, 1),
		DefaultBurst:                      raw.Int("VELOX_M2M_DEFAULT_BURST", 10, 1),
		MaxScenesPerRequest:               raw.Int("VELOX_M2M_MAX_SCENES_PER_REQUEST", 1000, 1),
		MaxTotalDurationSecondsPerRequest: raw.Float("VELOX_M2M_MAX_TOTAL_DURATION_SECS", 3600, 0),
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
func loadAllowedExternalDomains(raw RawConfig) []string {
	rawValue := strings.TrimSpace(raw.Get("VELOX_ALLOWED_EXTERNAL_DOMAINS"))
	if rawValue == "" {
		return nil
	}
	parts := strings.Split(rawValue, ",")
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
