// Package grpcserver / handler_security.go
//
// Security primitives for the WorkerControl gRPC stream, sliced out
// of handler.go so the stream loop and the orchestration stay focused
// on their own responsibilities.
//
// Three responsibilities live here:
//
//   - validateCredentialHash: zero-trust check the worker's declared
//     Hello.credential_hash against the stored SQLite worker_credentials
//     row. First-registration case populates the row if a hash is
//     declared; otherwise dev-only insecure mode skips validation.
//
//   - extractWorkerIDFromStream: read the client mTLS certificate's
//     CommonName (or first DNSName) so the declared worker_id can be
//     proven to match the transport-level identity.
//
//   - extractPeerIP: peer.FromContext wrapped address, port stripped,
//     for the worker_sessions audit table.
package grpcserver

import (
	"fmt"
	"log"
	"net"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

// validateCredentialHash checks the worker's credential_hash against the
// stored persistent credential in SQLite (worker_credentials table).
// Accepts the credential hash string directly from typed Hello message.
//
// Nil dbStore is safe: returns nil (skip validation) — this lets protocol-
// level tests and boot-dry-run handlers operate without a live DB handle.
func (h *Handler) validateCredentialHash(workerID string, declaredHash string) error {
	if h.dbStore == nil {
		return nil
	}
	// Check if this worker has a stored credential
	hasCred, err := h.dbStore.HasWorkerCredential(workerID)
	if err != nil {
		log.Printf("[GRPC] Credential lookup failed for worker %s: %v", workerID, err)
		if h.config.AllowInsecure {
			return nil
		}
		return fmt.Errorf("credential lookup failed for %s", workerID)
	}

	if !hasCred {
		// First registration: store the credential if one is provided
		if declaredHash != "" {
			if err := h.dbStore.SetWorkerCredential(workerID, declaredHash); err != nil {
				return fmt.Errorf("store initial credential: %w", err)
			}
			log.Printf("[GRPC] Worker %s: initial credential stored", workerID)
			return nil
		}
		if h.config.AllowInsecure {
			log.Printf("[GRPC] Worker %s: no credential — allowing in insecure dev mode", workerID)
			return nil
		}
		return fmt.Errorf("worker %s: credential required", workerID)
	}

	// Stored credential exists — validate the declared hash
	if declaredHash == "" {
		return fmt.Errorf("worker %s: credential required (existing credential stored)", workerID)
	}

	valid, err := h.dbStore.ValidateWorkerCredential(workerID, declaredHash)
	if err != nil {
		return fmt.Errorf("validate credential: %w", err)
	}
	if !valid {
		return fmt.Errorf("worker %s: credential hash mismatch", workerID)
	}

	return nil
}

// ---- Security Helpers ----

// extractPeerIP extracts the client IP address from the gRPC stream context
// without the port (if possible).
func (h *Handler) extractPeerIP(stream grpc.ServerStream) string {
	p, ok := peer.FromContext(stream.Context())
	if !ok {
		return ""
	}
	addr := p.Addr.String()
	if host, _, err := net.SplitHostPort(addr); err == nil {
		return host
	}
	return addr
}

// extractWorkerIDFromStream extracts the worker identity from the client TLS certificate.
func (h *Handler) extractWorkerIDFromStream(stream grpc.ServerStream) string {
	p, ok := peer.FromContext(stream.Context())
	if !ok {
		return ""
	}

	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return ""
	}

	if len(tlsInfo.State.PeerCertificates) == 0 {
		return ""
	}

	clientCert := tlsInfo.State.PeerCertificates[0]
	cn := clientCert.Subject.CommonName
	if cn == "" {
		if len(clientCert.DNSNames) > 0 {
			cn = clientCert.DNSNames[0]
		}
	}

	return strings.TrimSpace(cn)
}
