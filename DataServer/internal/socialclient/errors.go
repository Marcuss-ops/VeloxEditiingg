package socialclient

import "errors"

// Sentinel errors. The SocialGatewayProvider wraps these with the
// matching `deliveries.ErrProvider*` sentinel at the provider boundary,
// so the DeliveryRunner can apply the right retry classification
// without ever seeing a socialclient-typed error directly.
//
// Mapped classification:
//
//	ErrNotConfigured   → deliveries.ErrProviderNotConfigured (terminal)
//	ErrAuth            → deliveries.ErrProviderAuth       (terminal, BLOCKED_AUTH)
//	ErrRateLimit       → deliveries.ErrProviderRateLimit  (retry w/ backoff)
//	ErrTransient       → deliveries.ErrProviderTransient  (retry w/ backoff)
//	ErrPermanent       → deliveries.ErrProviderPermanent  (terminal FAILED)
var (
	// ErrNotConfigured is returned when BaseURL is empty or the Client
	// was constructed from a nil receiver. The provider maps this to
	// deliveries.ErrProviderNotConfigured (terminal).
	ErrNotConfigured = errors.New("socialclient: not configured")

	// ErrAuth is returned on HTTP 401/403. The provider maps this to
	// deliveries.ErrProviderAuth (terminal, BLOCKED_AUTH).
	ErrAuth = errors.New("socialclient: authentication/authorization error")

	// ErrRateLimit is returned on HTTP 429. The provider maps this to
	// deliveries.ErrProviderRateLimit (retry w/ default backoff).
	// Retry-After is NOT parsed here; the runner has a single
	// BackoffSchedule and does not honor per-delivery custom values.
	ErrRateLimit = errors.New("socialclient: rate limit exceeded")

	// ErrTransient is returned on 5xx, network errors, decode errors,
	// and ctx cancellations. The provider maps this to
	// deliveries.ErrProviderTransient (retry w/ backoff).
	ErrTransient = errors.New("socialclient: transient error")

	// ErrPermanent is returned on 4xx other than 401/403/429, on
	// marshal errors, and on responses missing `social_delivery_id`.
	// The provider maps this to deliveries.ErrProviderPermanent
	// (terminal FAILED — no retry).
	ErrPermanent = errors.New("socialclient: permanent error")
)
