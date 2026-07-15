package transport

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// ---- mTLS Integration Tests (Phase 7) ----

// generateTestCertsDir generates CA, server, and client certificates in a temp
// directory for mTLS testing. This avoids dependency on committed cert files.
func generateTestCertsDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()

	// Generate CA key and cert
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("Generate CA key: %v", err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("Create CA cert: %v", err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("Parse CA cert: %v", err)
	}

	// Write CA cert and key
	os.WriteFile(filepath.Join(dir, "ca.crt"), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0o644)
	caKeyBytes, _ := x509.MarshalPKCS8PrivateKey(caKey)
	os.WriteFile(filepath.Join(dir, "ca.key"), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: caKeyBytes}), 0o644)

	// Generate server cert signed by CA
	serverKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("Generate server key: %v", err)
	}
	serverTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "Test Server"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	serverDER, err := x509.CreateCertificate(rand.Reader, serverTemplate, caCert, &serverKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("Create server cert: %v", err)
	}
	os.WriteFile(filepath.Join(dir, "server.crt"), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverDER}), 0o644)
	serverKeyBytes, _ := x509.MarshalPKCS8PrivateKey(serverKey)
	os.WriteFile(filepath.Join(dir, "server.key"), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: serverKeyBytes}), 0o644)

	// Generate client cert signed by CA with worker ID in CommonName
	clientKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("Generate client key: %v", err)
	}
	clientTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: "test-worker-mtls"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	clientDER, err := x509.CreateCertificate(rand.Reader, clientTemplate, caCert, &clientKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("Create client cert: %v", err)
	}
	os.WriteFile(filepath.Join(dir, "client.crt"), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientDER}), 0o644)
	clientKeyBytes, _ := x509.MarshalPKCS8PrivateKey(clientKey)
	os.WriteFile(filepath.Join(dir, "client.key"), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: clientKeyBytes}), 0o644)

	return dir
}

// startTestMTLSServer creates a gRPC server with mTLS requiring client
// certificates signed by the test CA. Certificates are generated dynamically.
// Returns the certs directory so callers can configure client transport with
// the same CA.
func startTestMTLSServer(t *testing.T, srv pb.WorkerControlServer) (*grpc.Server, string, string) {
	t.Helper()

	certsDir := generateTestCertsDir(t)

	serverCert, err := tls.LoadX509KeyPair(
		filepath.Join(certsDir, "server.crt"),
		filepath.Join(certsDir, "server.key"),
	)
	if err != nil {
		t.Fatalf("Load server cert: %v", err)
	}

	caPEM, err := os.ReadFile(filepath.Join(certsDir, "ca.crt"))
	if err != nil {
		t.Fatalf("Read CA cert: %v", err)
	}
	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(caPEM) {
		t.Fatal("Failed to parse CA cert")
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    certPool,
		MinVersion:   tls.VersionTLS12,
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}

	gsrv := grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsConfig)))
	pb.RegisterWorkerControlServer(gsrv, srv)

	go func() {
		_ = gsrv.Serve(lis)
	}()

	return gsrv, lis.Addr().String(), certsDir
}

// TestGRPCStreamTransport_mTLS_Handshake verifies the full mTLS handshake.
func TestGRPCStreamTransport_mTLS_Handshake(t *testing.T) {
	ts := newTestStreamServer()
	srv, addr, certsDir := startTestMTLSServer(t, ts)
	defer srv.Stop()

	transport := NewGRPCStreamTransport(addr, "test-worker-mtls-001")
	if err := transport.WithTLS(
		filepath.Join(certsDir, "client.crt"),
		filepath.Join(certsDir, "client.key"),
		filepath.Join(certsDir, "ca.crt"),
	); err != nil {
		t.Fatalf("WithTLS failed: %v", err)
	}
	defer transport.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	hello := controltransport.WorkerHello{
		WorkerID:        "test-worker-mtls-001",
		WorkerName:      "test-worker-mtls",
		Hostname:        "mtls-test-host",
		Version:         "1.0.0",
		ProtocolVersion: controltransport.ProtocolVersionCurrent,
	}

	if err := transport.Connect(ctx, hello); err != nil {
		t.Fatalf("mTLS Connect failed: %v", err)
	}

	// Verify server received Hello
	ts.mu.Lock()
	lastHello := ts.lastHello
	ts.mu.Unlock()

	if lastHello == nil {
		t.Fatal("Server did not receive Hello over mTLS")
	}
	if lastHello.GetWorkerName() != "test-worker-mtls" {
		t.Errorf("Hello WorkerName = %q, want %q", lastHello.GetWorkerName(), "test-worker-mtls")
	}

	// Verify transport is ready
	if transport.state != stateReady {
		t.Errorf("Transport state = %v, want stateReady", transport.state)
	}
}

// TestGRPCStreamTransport_mTLS_NoClientCert verifies that a client without a
// certificate is rejected by the mTLS server.
func TestGRPCStreamTransport_mTLS_NoClientCert(t *testing.T) {
	ts := newTestStreamServer()
	srv, addr, _ := startTestMTLSServer(t, ts)
	defer srv.Stop()

	// Transport WITHOUT TLS — uses insecure credentials
	transport := NewGRPCStreamTransport(addr, "test-worker-nocert")
	defer transport.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	hello := controltransport.WorkerHello{
		WorkerID:        "test-worker-nocert",
		ProtocolVersion: controltransport.ProtocolVersionCurrent,
	}

	err := transport.Connect(ctx, hello)
	if err == nil {
		t.Error("Expected connection rejection for client without cert, got nil error")
	}
}

// TestGRPCStreamTransport_mTLS_WrongCA verifies that a client with a
// certificate signed by a different CA is rejected.
func TestGRPCStreamTransport_mTLS_WrongCA(t *testing.T) {
	ts := newTestStreamServer()
	srv, addr, certsDir := startTestMTLSServer(t, ts)
	defer srv.Stop()

	// Generate a self-signed cert NOT signed by the test CA
	wrongCert := generateSelfSignedCert(t)

	transport := NewGRPCStreamTransport(addr, "test-worker-wrongca")

	// Trust the server's CA (needed to verify the server during handshake)
	caPEM, err := os.ReadFile(filepath.Join(certsDir, "ca.crt"))
	if err != nil {
		t.Fatalf("Read CA cert: %v", err)
	}
	serverCAPool := x509.NewCertPool()
	if !serverCAPool.AppendCertsFromPEM(caPEM) {
		t.Fatal("Failed to parse CA cert for server trust")
	}

	// Present a self-signed client cert (NOT signed by test CA)
	transport.tlsConfig = &tls.Config{
		Certificates: []tls.Certificate{wrongCert},
		RootCAs:      serverCAPool,
		MinVersion:   tls.VersionTLS12,
	}
	defer transport.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	hello := controltransport.WorkerHello{
		WorkerID:        "test-worker-wrongca",
		ProtocolVersion: controltransport.ProtocolVersionCurrent,
	}

	err = transport.Connect(ctx, hello)
	if err == nil {
		t.Error("Expected connection rejection for client with self-signed cert (wrong CA), got nil error")
	}
}

// generateSelfSignedCert creates a real self-signed TLS certificate.
func generateSelfSignedCert(t *testing.T) tls.Certificate {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("Generate key: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "wrong-ca-client"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("Create certificate: %v", err)
	}

	return tls.Certificate{
		Certificate: [][]byte{certDER},
		PrivateKey:  key,
	}
}

// TestGRPCStreamTransport_mTLS_HeartbeatSend verifies heartbeat over mTLS.
func TestGRPCStreamTransport_mTLS_HeartbeatSend(t *testing.T) {
	ts := newTestStreamServer()
	srv, addr, certsDir := startTestMTLSServer(t, ts)
	defer srv.Stop()

	transport := NewGRPCStreamTransport(addr, "test-worker-mtls-hb")
	if err := transport.WithTLS(
		filepath.Join(certsDir, "client.crt"),
		filepath.Join(certsDir, "client.key"),
		filepath.Join(certsDir, "ca.crt"),
	); err != nil {
		t.Fatalf("WithTLS failed: %v", err)
	}
	defer transport.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	hello := controltransport.WorkerHello{
		WorkerID:        "test-worker-mtls-hb",
		WorkerName:      "test-worker-mtls",
		Hostname:        "mtls-test-host",
		Version:         "1.0.0",
		ProtocolVersion: controltransport.ProtocolVersionCurrent,
	}
	if err := transport.Connect(ctx, hello); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	heartbeatMsg := controltransport.ControlMessage{
		MessageID:       "hb-mtls-001",
		Type:            controltransport.MsgHeartbeat,
		WorkerID:        "test-worker-mtls-hb",
		SentAt:          time.Now().UTC(),
		ProtocolVersion: controltransport.ProtocolVersionCurrent,
		TypedPayload: &pb.Heartbeat{
			Status: "idle",
		},
	}

	if err := transport.Send(ctx, heartbeatMsg); err != nil {
		t.Fatalf("Send heartbeat over mTLS failed: %v", err)
	}

	// Wait for server to process the heartbeat
	select {
	case <-ts.heartbeatCh:
	case <-time.After(3 * time.Second):
		t.Fatal("Timed out waiting for heartbeat over mTLS")
	}

	ts.mu.Lock()
	numHeartbeats := len(ts.heartbeats)
	ts.mu.Unlock()
	if numHeartbeats != 1 {
		t.Fatalf("Expected 1 heartbeat over mTLS, got %d", numHeartbeats)
	}
}
