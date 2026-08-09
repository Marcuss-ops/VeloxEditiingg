package doctor

// Production validators deliberately consume live runtime evidence instead
// of treating a tag, bundle file, or a successful config parse as proof that
// the worker is certified. The FleetController publishes the desired digest
// and ledger verdict to the worker environment; the worker's health endpoint
// supplies the authenticated-session/readiness evidence.

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	"velox-worker-agent/pkg/config"
)

const immutableImagePattern = `^ghcr\.io/[a-z0-9._-]+/[a-z0-9._/-]+@sha256:[a-f0-9]{64}$`

var immutableImageRE = regexp.MustCompile(immutableImagePattern)

// DefaultProductionValidators is the fail-closed certification set. It does
// not silently downgrade missing Master/deployment evidence to a warning.
func DefaultProductionValidators() []Validator {
	return []Validator{
		&ProductionMTLSValidator{},
		&ProductionCertificateValidator{},
		&ProductionChainValidator{},
		&ProductionHealthValidator{},
		&ProductionProtocolValidator{},
		&ProductionImageValidator{},
		&ProductionDigestValidator{},
		&ProductionReleaseValidator{},
		&ProductionBundleValidator{},
		&ProductionRollbackValidator{},
		&ProductionRebuildValidator{},
		&ProductionLedgerValidator{},
	}
}

type ProductionMTLSValidator struct{}

func (*ProductionMTLSValidator) ID() string { return "pki.mtls_required" }
func (*ProductionMTLSValidator) Run(_ context.Context, cfg *config.WorkerConfig) Result {
	if cfg == nil {
		return fail("pki.mtls_required", "worker config is missing", "provide the canonical worker_config.json")
	}
	tlsCfg := cfg.GRPCTLS()
	if cfg.Environment != "production" || tlsCfg.AllowInsecureDev {
		return fail("pki.mtls_required", "production requires environment=production and insecure gRPC disabled", "remove VELOX_ALLOW_INSECURE_GRPC_DEV and configure the full TLS triple")
	}
	if tlsCfg.CertFile == "" || tlsCfg.KeyFile == "" || tlsCfg.CAFile == "" {
		return fail("pki.mtls_required", "client certificate, private key, and CA are all required", "configure tls_cert_file, tls_key_file, and tls_ca_file")
	}
	if _, err := tls.LoadX509KeyPair(tlsCfg.CertFile, tlsCfg.KeyFile); err != nil {
		return fail("pki.mtls_required", fmt.Sprintf("client certificate/key cannot be loaded: %v", err), "renew or replace the worker certificate pair")
	}
	return pass("pki.mtls_required", "production mTLS triple is configured and the key pairs with the certificate")
}

type ProductionCertificateValidator struct{}

func (*ProductionCertificateValidator) ID() string { return "pki.certificate_valid" }
func (*ProductionCertificateValidator) Run(_ context.Context, cfg *config.WorkerConfig) Result {
	if cfg == nil || cfg.TLSCertFile == "" || cfg.TLSKeyFile == "" {
		return fail("pki.certificate_valid", "worker certificate material is missing", "configure the OpenBao-issued certificate and key")
	}
	pair, err := tls.LoadX509KeyPair(cfg.TLSCertFile, cfg.TLSKeyFile)
	if err != nil || len(pair.Certificate) == 0 {
		return fail("pki.certificate_valid", fmt.Sprintf("certificate cannot be parsed: %v", err), "renew the worker mTLS certificate")
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return fail("pki.certificate_valid", fmt.Sprintf("certificate leaf cannot be parsed: %v", err), "renew the worker mTLS certificate")
	}
	if leaf.Subject.CommonName != cfg.WorkerID {
		return fail("pki.certificate_valid", fmt.Sprintf("certificate CN=%q does not match immutable worker_id=%q", leaf.Subject.CommonName, cfg.WorkerID), "issue the certificate for the immutable worker_id")
	}
	now := time.Now()
	if now.Before(leaf.NotBefore) || now.After(leaf.NotAfter) {
		return fail("pki.certificate_valid", fmt.Sprintf("certificate is outside its validity window (%s..%s)", leaf.NotBefore.UTC().Format(time.RFC3339), leaf.NotAfter.UTC().Format(time.RFC3339)), "renew the worker mTLS certificate")
	}
	if leaf.NotAfter.Sub(now) < 14*24*time.Hour {
		return fail("pki.certificate_valid", fmt.Sprintf("certificate expires in %s, below the 14-day production floor", leaf.NotAfter.Sub(now).Round(time.Second)), "renew the worker mTLS certificate before rollout")
	}
	return pass("pki.certificate_valid", fmt.Sprintf("certificate valid until %s", leaf.NotAfter.UTC().Format(time.RFC3339)))
}

type ProductionChainValidator struct{}

func (*ProductionChainValidator) ID() string { return "pki.ca_chain_trusted" }
func (*ProductionChainValidator) Run(_ context.Context, cfg *config.WorkerConfig) Result {
	if cfg == nil || cfg.TLSCertFile == "" || cfg.TLSCAFile == "" {
		return fail("pki.ca_chain_trusted", "certificate or CA file is missing", "mount the OpenBao CA and worker certificate")
	}
	pair, err := tls.LoadX509KeyPair(cfg.TLSCertFile, cfg.TLSKeyFile)
	if err != nil || len(pair.Certificate) == 0 {
		return fail("pki.ca_chain_trusted", fmt.Sprintf("certificate chain cannot be loaded: %v", err), "renew the worker mTLS certificate")
	}
	caPEM, err := os.ReadFile(cfg.TLSCAFile)
	if err != nil {
		return fail("pki.ca_chain_trusted", fmt.Sprintf("CA file cannot be read: %v", err), "mount the trusted OpenBao CA")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return fail("pki.ca_chain_trusted", "CA file contains no parseable certificate", "replace the CA file with PEM-encoded trust material")
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return fail("pki.ca_chain_trusted", fmt.Sprintf("leaf cannot be parsed: %v", err), "renew the worker mTLS certificate")
	}
	intermediates := x509.NewCertPool()
	for _, der := range pair.Certificate[1:] {
		cert, parseErr := x509.ParseCertificate(der)
		if parseErr != nil {
			return fail("pki.ca_chain_trusted", fmt.Sprintf("intermediate cannot be parsed: %v", parseErr), "renew the worker certificate chain")
		}
		intermediates.AddCert(cert)
	}
	if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}); err != nil {
		return fail("pki.ca_chain_trusted", fmt.Sprintf("worker certificate is not trusted by the configured CA: %v", err), "use the CA chain that issued this worker certificate")
	}
	return pass("pki.ca_chain_trusted", "worker certificate chain verifies against the configured CA")
}

type ProductionHealthValidator struct{}

func (*ProductionHealthValidator) ID() string { return "worker.health_and_connection" }
func (*ProductionHealthValidator) Run(ctx context.Context, cfg *config.WorkerConfig) Result {
	if cfg == nil || cfg.HealthPort <= 0 {
		return fail("worker.health_and_connection", "health endpoint is disabled", "enable the canonical health port")
	}
	path := cfg.ReadyzEndpoint
	if path == "" {
		path = "/health/ready"
	}
	requestCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d%s", cfg.HealthPort, path), nil)
	if err != nil {
		return fail("worker.health_and_connection", fmt.Sprintf("cannot create readiness request: %v", err), "restore the canonical local health endpoint")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fail("worker.health_and_connection", fmt.Sprintf("readiness endpoint unavailable: %v", err), "ensure velox-worker-agent is running and registered")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fail("worker.health_and_connection", fmt.Sprintf("readiness returned HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body))), "resolve the readiness reasons before certification")
	}
	return pass("worker.health_and_connection", "local readiness is healthy; registration/session evidence is reported by the readiness endpoint")
}

type ProductionProtocolValidator struct{}

func (*ProductionProtocolValidator) ID() string { return "runtime.protocol" }
func (*ProductionProtocolValidator) Run(_ context.Context, cfg *config.WorkerConfig) Result {
	if cfg == nil || strings.TrimSpace(cfg.ProtocolVersion) != "v3" {
		return fail("runtime.protocol", fmt.Sprintf("protocol=%q, expected v3", value(cfg, func(c *config.WorkerConfig) string { return c.ProtocolVersion })), "deploy a v3 worker runtime")
	}
	return pass("runtime.protocol", "protocol v3")
}

type ProductionImageValidator struct{}

func (*ProductionImageValidator) ID() string { return "runtime.image_immutable" }
func (*ProductionImageValidator) Run(_ context.Context, _ *config.WorkerConfig) Result {
	image := strings.TrimSpace(os.Getenv("VELOX_WORKER_IMAGE"))
	if !immutableImageRE.MatchString(image) {
		return fail("runtime.image_immutable", fmt.Sprintf("VELOX_WORKER_IMAGE is not an immutable GHCR digest: %q", image), "activate ghcr.io/...@sha256:<64hex>; tags are not release identity")
	}
	return pass("runtime.image_immutable", "running image reference is pinned to a GHCR sha256 digest")
}

type ProductionDigestValidator struct{}

func (*ProductionDigestValidator) ID() string { return "runtime.desired_equals_running" }
func (*ProductionDigestValidator) Run(_ context.Context, _ *config.WorkerConfig) Result {
	desired := strings.TrimSpace(os.Getenv("VELOX_MASTER_DESIRED_IMAGE"))
	running := strings.TrimSpace(os.Getenv("VELOX_WORKER_IMAGE"))
	if !immutableImageRE.MatchString(desired) || !immutableImageRE.MatchString(running) {
		return fail("runtime.desired_equals_running", "desired and running immutable image evidence is incomplete", "project the Master ledger target and the activated worker digest into the runtime")
	}
	if digest(desired) != digest(running) {
		return fail("runtime.desired_equals_running", fmt.Sprintf("desired=%s running=%s", digest(desired), digest(running)), "activate the exact digest recorded by the Master ledger")
	}
	return pass("runtime.desired_equals_running", fmt.Sprintf("desired and running digest=%s", digest(running)))
}

type ProductionReleaseValidator struct{}

func (*ProductionReleaseValidator) ID() string { return "runtime.release_coherent" }
func (*ProductionReleaseValidator) Run(_ context.Context, cfg *config.WorkerConfig) Result {
	if cfg == nil || strings.TrimSpace(cfg.BundleVersion) == "" || strings.TrimSpace(cfg.EngineVersion) == "" || strings.TrimSpace(cfg.ProtocolVersion) == "" || strings.TrimSpace(os.Getenv("VELOX_WORKER_IMAGE_VERSION")) == "" {
		return fail("runtime.release_coherent", "version metadata is incomplete", "publish a complete worker release and report its metadata")
	}
	return pass("runtime.release_coherent", fmt.Sprintf("bundle=%s engine=%s protocol=%s", cfg.BundleVersion, cfg.EngineVersion, cfg.ProtocolVersion))
}

type ProductionBundleValidator struct{}

func (*ProductionBundleValidator) ID() string { return "runtime.bundle_metadata_coherent" }
func (*ProductionBundleValidator) Run(_ context.Context, cfg *config.WorkerConfig) Result {
	expected := strings.TrimSpace(cfgValue(cfg, func(c *config.WorkerConfig) string { return c.BundleHash }))
	observed := strings.TrimSpace(os.Getenv("VELOX_RUNTIME_BUNDLE_HASH"))
	if expected == "" || observed == "" || expected != observed {
		return fail("runtime.bundle_metadata_coherent", "bundle metadata is missing or differs from the configured runtime metadata", "repair the runtime metadata; it is diagnostic only and never selects the image")
	}
	return pass("runtime.bundle_metadata_coherent", "bundle metadata matches; image digest remains the release authority")
}

type ProductionRollbackValidator struct{}

func (*ProductionRollbackValidator) ID() string { return "deployment.rollback_digest" }
func (*ProductionRollbackValidator) Run(_ context.Context, _ *config.WorkerConfig) Result {
	rollback := strings.TrimSpace(os.Getenv("VELOX_ROLLBACK_IMAGE"))
	if !immutableImageRE.MatchString(rollback) {
		return fail("deployment.rollback_digest", "no immutable rollback image is available", "retain the previous GHCR digest during activation")
	}
	return pass("deployment.rollback_digest", fmt.Sprintf("rollback digest=%s", digest(rollback)))
}

type ProductionRebuildValidator struct{}

func (*ProductionRebuildValidator) ID() string { return "deployment.remote_rebuild_disabled" }
func (*ProductionRebuildValidator) Run(_ context.Context, _ *config.WorkerConfig) Result {
	if strings.ToLower(strings.TrimSpace(os.Getenv("VELOX_REMOTE_REBUILD_DISABLED"))) != "true" {
		return fail("deployment.remote_rebuild_disabled", "remote Docker rebuild is not positively disabled", "activate only a pulled immutable digest through FleetController")
	}
	return pass("deployment.remote_rebuild_disabled", "remote Docker rebuild is disabled")
}

type ProductionLedgerValidator struct{}

func (*ProductionLedgerValidator) ID() string { return "deployment.ledger_coherent" }
func (*ProductionLedgerValidator) Run(_ context.Context, cfg *config.WorkerConfig) Result {
	state := strings.TrimSpace(os.Getenv("VELOX_DEPLOYMENT_LEDGER_STATE"))
	workerID := strings.TrimSpace(os.Getenv("VELOX_DEPLOYMENT_LEDGER_WORKER_ID"))
	if state != "SUCCEEDED" || cfg == nil || workerID == "" || workerID != cfg.WorkerID {
		return fail("deployment.ledger_coherent", fmt.Sprintf("ledger state=%q worker_id=%q; expected SUCCEEDED for %q", state, workerID, value(cfg, func(c *config.WorkerConfig) string { return c.WorkerID })), "complete the FleetController operation and project its ledger evidence")
	}
	return pass("deployment.ledger_coherent", "deployment ledger is SUCCEEDED for this immutable worker_id")
}

func digest(ref string) string {
	if at := strings.Index(ref, "@sha256:"); at >= 0 {
		return ref[at+1:]
	}
	return ref
}

func cfgValue(cfg *config.WorkerConfig, f func(*config.WorkerConfig) string) string {
	if cfg == nil {
		return ""
	}
	return f(cfg)
}

func value(cfg *config.WorkerConfig, f func(*config.WorkerConfig) string) string {
	return cfgValue(cfg, f)
}
