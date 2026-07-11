package doctor

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"time"

	"velox-worker-agent/pkg/config"
)

// minCertValidityDoctor is the floor for certificate residual validity
// checked by the doctor (RW-PROD-002 §2 item 3). Must stay aligned with
// config.minCertValidity (= 14d) from RW-PROD-001 A1.
const minCertValidityDoctor = 14 * 24 * time.Hour

// CertExpiryValidator parses the TLS leaf certificate and verifies it
// has at least minCertValidityDoctor remaining before expiry.
// RW-PROD-002 §2 item 3.
//
// This validator is best-effort: if no TLS cert is configured, it skips
// with PASS. The EnvironmentValidator enforces that production must have
// TLS; this validator catches near-expiry certs before they cause
// handshake failures mid-task.
type CertExpiryValidator struct{}

func (v *CertExpiryValidator) ID() string { return "cert.expiry" }

func (v *CertExpiryValidator) Run(_ context.Context, cfg *config.WorkerConfig) Result {
	tlsCfg := cfg.GRPCTLS()
	if trim(tlsCfg.CertFile) == "" {
		return pass("cert.expiry", "no TLS cert configured; expiry check skipped")
	}

	cert, err := tls.LoadX509KeyPair(tlsCfg.CertFile, tlsCfg.KeyFile)
	if err != nil {
		return fail("cert.expiry",
			fmt.Sprintf("cannot load cert/key pair: %v", err),
			"verify cert and key exist and match at the configured paths")
	}

	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return fail("cert.expiry",
			fmt.Sprintf("cannot parse leaf certificate: %v", err),
			"the PEM file may be malformed; re-generate the cert")
	}

	now := time.Now().UTC()
	switch {
	case now.After(leaf.NotAfter):
		return fail("cert.expiry",
			fmt.Sprintf("certificate expired at %s (now=%s)", leaf.NotAfter.UTC().Format(time.RFC3339), now.Format(time.RFC3339)),
			"re-generate the worker certificate and re-deploy")
	case leaf.NotAfter.Sub(now) < minCertValidityDoctor:
		remaining := leaf.NotAfter.Sub(now).Round(time.Second)
		return fail("cert.expiry",
			fmt.Sprintf("certificate expires in %s (floor=%s, not_after=%s)", remaining, minCertValidityDoctor, leaf.NotAfter.UTC().Format(time.RFC3339)),
			"rotate the certificate now or within the next 14 days")
	}

	remaining := leaf.NotAfter.Sub(now).Round(time.Hour)
	return pass("cert.expiry",
		fmt.Sprintf("certificate valid for %s (not_after=%s)", remaining, leaf.NotAfter.UTC().Format(time.RFC3339)))
}
