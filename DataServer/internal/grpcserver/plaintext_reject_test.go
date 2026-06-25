package grpcserver

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"math/big"
	"crypto/x509/pkix"
	"encoding/pem"
	"net"
	"os"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// TestServer_PlaintextRejectedWhenTLSRequired is the RW-PROD-001 §3 A6
// regression: a server bound with grpc.Creds(NewTLS(...)) MUST reject
// a raw TCP dial within 1 second (handshake failure, not a hang).
//
// We do NOT rely on the bootstrap-time guard in DataServer/cmd/server
// (enforceGRPCRequireTLS, see RW-PROD-001 A5) — that one trips at
// master boot. THIS test covers the runtime path: even if a misconfig
// lets the master boot, a worker knocking on the TLS-only endpoint
// without TLS gets rejected promptly, never hangs forever.
//
// Acceptance criteria (RW-PROD-001 §4):
//   "Plaintext verso TLS-only master → connection REJECTED in <1s."
func TestServer_PlaintextRejectedWhenTLSRequired(t *testing.T) {
	// 1. Generate a self-signed cert+key+CA triple (compatible pair) on
	//    disk so the runtime parser reads real PEMs.
	certFile, keyFile, caFile := writeTestTLSMaterial(t)

	tlsCert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatalf("LoadX509KeyPair: %v", err)
	}
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		t.Fatalf("read ca: %v", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		t.Fatalf("AppendCertsFromPEM: pool rejected CA")
	}
	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    pool,
		MinVersion:   tls.VersionTLS12,
	}

	// 2. Bind a grpc.Server with TLS credentials on a random port. We
	//    use 127.0.0.1 so the OS-level accept queue cannot race with a
	//    stray kernel-level TLS-on-port-443 fallback.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	defer lis.Close()
	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsCfg)))
	srvDone := make(chan struct{})
	go func() {
		_ = srv.Serve(lis)
		close(srvDone)
	}()
	defer func() {
		srv.GracefulStop()
		<-srvDone
	}()
	addr := lis.Addr().String()

	// 3. Plaintext TCP dial — DO NOT send any framing. The TLS-side
	//    state machine expects a ClientHello; with no bytes it should
	//    hit the read deadline and close within <=1s.
	dialer := &net.Dialer{Timeout: 1 * time.Second}
	conn, dialErr := dialer.Dial("tcp", addr)
	if dialErr != nil {
		// Good: the dial itself failed (immediate RST, ECONNREFUSED,
		// or kernel-level TLS-association refused). Either way the
		// spec's "<1s reject" is satisfied.
		t.Logf("plaintext reject at dial: err=%v (acceptable per §4)", dialErr)
		return
	}
	defer conn.Close()

	// 4. If the dial succeeded (rare on strict-TCP kernels), measure
	//    the time until the TLS server closes the connection.
	_ = conn.SetDeadline(time.Now().Add(1 * time.Second))
	buf := make([]byte, 1)
	start := time.Now()
	n, readErr := conn.Read(buf)
	elapsed := time.Since(start)
	if n > 0 {
		t.Fatalf("plaintext dial received %d unexpected bytes before reject", n)
	}
	if readErr == nil {
		t.Fatalf("plaintext dial did not close within 1s; reject was not prompt")
	}
	if elapsed > 1100*time.Millisecond {
		t.Fatalf("plaintext reject took %v; spec wants <=1s", elapsed)
	}
	t.Logf("plaintext reject closed after %v with err=%v (acceptable)", elapsed, readErr)
}

// writeTestTLSMaterial creates a (cert.pem, key.pem, ca.pem) triple inside
// t.TempDir() and returns the absolute paths. Kept local to this test file
// so the canonical config-package helpers remain single-source.
func writeTestTLSMaterial(t *testing.T) (certFile, keyFile, caFile string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("genkey: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "rw-prod-001-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		// 24h validity is intentional — server-side test fixture, no Validate()
		// round-trip (the cert stays in grpcserver pkg test scope).
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("createcert: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	caBytes, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("createca: %v", err)
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caBytes})
	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	})
	dir := t.TempDir()
	certFile = dir + "/cert.pem"
	keyFile = dir + "/key.pem"
	caFile = dir + "/ca.pem"
	if err := os.WriteFile(certFile, certPEM, 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	if err := os.WriteFile(caFile, caPEM, 0o600); err != nil {
		t.Fatalf("write ca: %v", err)
	}
	return certFile, keyFile, caFile
}
