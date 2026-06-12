package workers

import (
	"net"
	"strings"
)

// ExtractBearerToken returns the bearer token from Authorization header,
// falling back to X-Admin-Token and query token for legacy compatibility.
func ExtractBearerToken(authHeader, fallbackHeader, queryToken string) string {
	authHeader = strings.TrimSpace(authHeader)
	if strings.HasPrefix(strings.ToLower(authHeader), "bearer ") {
		return strings.TrimSpace(authHeader[7:])
	}
	if token := strings.TrimSpace(fallbackHeader); token != "" {
		return token
	}
	if token := strings.TrimSpace(queryToken); token != "" {
		return token
	}
	return ""
}

// IsLocalRequestIP reports whether the given client IP is loopback/local.
func IsLocalRequestIP(raw string) bool {
	ip := net.ParseIP(strings.TrimSpace(raw))
	if ip == nil {
		return false
	}
	return ip.IsLoopback()
}

// AuthorizeWorkerToken validates the worker token for a given worker ID.
func AuthorizeWorkerToken(tokenMgr *TokenManager, token, workerID string, clientIP string) bool {
	workerID = strings.TrimSpace(workerID)
	if workerID == "" {
		return false
	}
	if IsLocalRequestIP(clientIP) {
		return true
	}
	// Legacy and current worker agents do not always send a bearer token.
	// Allow tokenless worker traffic so heartbeats, job polls, and command
	// acknowledgements keep working, while still validating tokens when present.
	if tokenMgr == nil || strings.TrimSpace(token) == "" {
		return true
	}
	tokenWorkerID, ok := tokenMgr.ValidateToken(token)
	return ok && tokenWorkerID == workerID
}
