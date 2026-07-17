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
// Social API gateway. Each field maps 1:1 to a `SOCIAL_API_*` env var;
// no legacy alias is honored — operators that still carry a
// `SOCIAL_GATEWAY_*` env in /etc/velox-server.env will see the provider
// as not-configured (BaseURL="", ErrNotConfigured at DeliverArtifact time)
// and must rename to the canonical `SOCIAL_API_*` form.
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
// environment. Every field maps 1:1 to a `SOCIAL_API_*` env var. The
// prior `SOCIAL_GATEWAY_*` deprecation cycle is over and the legacy
// aliases are intentionally NOT honored: an operator that still
// carries only the legacy env in `/etc/velox-server.env` will observe
// an empty BaseURL here and the delivery provider will surface
// ErrNotConfigured at run time, which is the fail-closed shape we want.
//
// Variable map (canonical only):
//
//	SOCIAL_API_URL                      required for live publish
//	SOCIAL_API_TOKEN                    optional bearer (empty = dev mode)
//	SOCIAL_API_TIMEOUT_MS               single-attempt HTTP timeout (default 30000)
//	SOCIAL_API_RETRIES                  hint only; Velox-runnner-driven retry is canonical
//	SOCIAL_CALLBACK_BASE_URL            Velox public URL for download_url / callback_url
//
// SOCIAL_API_TIMEOUT_MS defaults to 30000; SOCIAL_API_RETRIES defaults
// to 0.
func ConfigFromEnv() Config {
	c := Config{
		BaseURL:         os.Getenv("SOCIAL_API_URL"),
		APIKey:          os.Getenv("SOCIAL_API_TOKEN"),
		CallbackBaseURL: os.Getenv("SOCIAL_CALLBACK_BASE_URL"),
		Timeout:         parseDurationMillis(os.Getenv("SOCIAL_API_TIMEOUT_MS"), 30*time.Second),
		MaxRetries:      parseInt(os.Getenv("SOCIAL_API_RETRIES"), 0),
	}
	return c
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
