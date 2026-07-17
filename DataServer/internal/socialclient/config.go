// Package socialclient is the typed Velox-side boundary against the
// external Social API. The contract is OWNED by the social_repo and
// formatted below: Velox sends artifact metadata + idempotency + a
// destination ID to `POST {BaseURL}/internal/v1/deliveries` and
// receives a `social_delivery_id` that the DeliveryRunner persists on
// `job_deliveries.remote_id`.
//
// Velox does NOT know anything about OAuth, channels, tokens, quota or
// publishing state — those concerns are owned by the social_repo and
// surfaced here only via the typed result fields.
//
// The package is intentionally minimal:
//   - config.go    — typed configuration + env loading
//   - requests.go  — request/response typed structs
//   - client.go    — HTTP transport: single attempt, caller-side retry
//   - errors.go    — typed errors so the provider can map back to the
//     deliveries sentinels (Permanent / Transient /
//     Auth / RateLimit / NotConfigured).
//
// retry semantics: socialclient.DeliverArtifact does NOT retry
// internally. The deliveries.DeliveryRunner applies retry_budget per
// (artifact_id, destination_id) and a per-runner BackoffSchedule.
// socialclient must therefore be safe to call over and over without
// stateful side effects on the remote side; idempotency_key is
// callback-provided for that purpose.
package socialclient

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config is the typed view of the operator-facing configuration for the
// Social API gateway. All fields map 1:1 to `SOCIAL_API_*` env vars;
// legacy `SOCIAL_GATEWAY_*` vars are honored for one deprecation cycle
// to ease the rollout on existing operators.
type Config struct {
	// BaseURL is the Social API base, e.g. "https://social.example.com".
	// Empty BaseURL means Velox should refuse to publish via this
	// provider — the provider maps the call to ErrNotConfigured and the
	// runner surfaces the destination as FAILED in dev, or quietly
	// skips registration in production.
	BaseURL string

	// APIKey is the optional bearer token sent as `Authorization:
	// Bearer <APIKey>`. Empty disables auth (dev mode).
	APIKey string

	// CallbackBaseURL is Velox's publicly reachable URL, used to build
	// the artifact download_url (`<CallbackBaseURL>/api/internal/artifacts/<id>/download`)
	// and the post-publish callback URL
	// (`<CallbackBaseURL>/api/internal/deliveries/<deliveryID>/callback`).
	// Empty disables both derived URLs.
	CallbackBaseURL string

	// Timeout bounds a single DeliverArtifact call. Zero means 30s.
	Timeout time.Duration

	// MaxRetries is currently unused at the client level (the caller
	// retries per the runner's BackoffSchedule). It is preserved here
	// so a future socialclient-side retry policy can be added without
	// breaking the public API.
	MaxRetries int
}

// Validate ensures the Config is internally consistent. Empty BaseURL
// is allowed (the social_repo may be optional in dev); the caller
// decides what to do with an empty config (typically ErrNotConfigured).
func (c Config) Validate() error {
	if c.Timeout < 0 {
		return errors.New("socialclient: Timeout must be >= 0")
	}
	if c.MaxRetries < 0 {
		return errors.New("socialclient: MaxRetries must be >= 0")
	}
	return nil
}

// ConfigFromEnv reads the Social API configuration from the process
// environment. The canonical variable names are `SOCIAL_API_*`; legacy
// `SOCIAL_GATEWAY_*` env vars are honored ONLY when the canonical is
// unset so a freshly-cloned operator who still has the old vars in
// /etc/velox-server.env does not silently lose the provider.
//
// Variable map (canonical → legacy fallback):
//
//	SOCIAL_API_URL                  ← SOCIAL_GATEWAY_URL
//	SOCIAL_API_TOKEN                ← SOCIAL_GATEWAY_API_KEY
//	SOCIAL_CALLBACK_BASE_URL        ← SOCIAL_GATEWAY_CALLBACK_BASE_URL
//	SOCIAL_API_TIMEOUT_MS           (no legacy)
//	SOCIAL_API_RETRIES              (no legacy)
//
// SOCIAL_API_TIMEOUT_MS = 30000 by default; SOCIAL_API_RETRIES = 0
// by default (Velox-runnner-driven retry is canonical).
func ConfigFromEnv() Config {
	c := Config{
		BaseURL:         firstNonEmpty(os.Getenv("SOCIAL_API_URL"), os.Getenv("SOCIAL_GATEWAY_URL")),
		APIKey:          firstNonEmpty(os.Getenv("SOCIAL_API_TOKEN"), os.Getenv("SOCIAL_GATEWAY_API_KEY")),
		CallbackBaseURL: firstNonEmpty(os.Getenv("SOCIAL_CALLBACK_BASE_URL"), os.Getenv("SOCIAL_GATEWAY_CALLBACK_BASE_URL")),
		Timeout:         parseDurationMillis(os.Getenv("SOCIAL_API_TIMEOUT_MS"), 30*time.Second),
		MaxRetries:      parseInt(os.Getenv("SOCIAL_API_RETRIES"), 0),
	}
	return c
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

func parseDurationMillis(raw string, def time.Duration) time.Duration {
	if raw == "" {
		return def
	}
	ms, err := strconv.Atoi(raw)
	if err != nil || ms < 0 {
		return def
	}
	return time.Duration(ms) * time.Millisecond
}

func parseInt(raw string, def int) int {
	if raw == "" {
		return def
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v < 0 {
		return def
	}
	return v
}

// String returns a redacted summary suitable for logs (APIKey is masked).
func (c Config) String() string {
	masked := "<unset>"
	if c.APIKey != "" {
		masked = "<set>"
	}
	return fmt.Sprintf("socialclient.Config{BaseURL=%q APIKey=%s CallbackBaseURL=%q Timeout=%s MaxRetries=%d}",
		c.BaseURL, masked, c.CallbackBaseURL, c.Timeout, c.MaxRetries)
}
