package config

import "testing"

func TestLoadEndpointConfigUsesCanonicalValuesAndCompatibilityAliases(t *testing.T) {
	raw := NewRawConfig(map[string]string{
		"VELOX_CONTROL_PLANE_REST_PUBLIC_URL":   "https://public.example",
		"VELOX_CONTROL_PLANE_REST_INTERNAL_URL": "http://internal:8000",
		"VELOX_CONTROL_PLANE_GRPC_URL":          "grpc.example:9000",
		"VELOX_FRONTEND_PUBLIC_URL":             "https://frontend.example",
		"MASTER_PUBLIC_URL":                     "https://legacy.example",
	})
	control, frontend := loadEndpointConfig(raw)
	if control.RESTPublic != "https://public.example" || control.RESTInternal != "http://internal:8000" || control.GRPCControl != "grpc.example:9000" {
		t.Fatalf("unexpected control-plane endpoints: %+v", control)
	}
	if frontend.Public != "https://frontend.example" {
		t.Fatalf("unexpected frontend endpoint: %q", frontend.Public)
	}

	legacyControl, _ := loadEndpointConfig(NewRawConfig(map[string]string{
		"MASTER_PUBLIC_URL":     "https://legacy.example",
		"VELOX_GRPC_MASTER_URL": "legacy.example:9000",
	}))
	if legacyControl.RESTPublic != "https://legacy.example" || legacyControl.GRPCControl != "legacy.example:9000" {
		t.Fatalf("legacy aliases not mapped: %+v", legacyControl)
	}
}

func TestValidateConfiguredEndpointsRejectsMalformedValues(t *testing.T) {
	cfg := &Config{ControlPlane: ControlPlaneEndpoints{
		RESTPublic:  "not-a-url",
		GRPCControl: "grpc.example:not-a-port",
	}}
	if err := validateConfiguredEndpoints(cfg); err == nil {
		t.Fatal("malformed endpoint accepted")
	}
	cfg.ControlPlane.RESTPublic = "https://master.example"
	cfg.ControlPlane.GRPCControl = "grpc.example:9000"
	if err := validateConfiguredEndpoints(cfg); err != nil {
		t.Fatalf("valid endpoints rejected: %v", err)
	}
	cfg.ControlPlane.GRPCControl = "https://grpc.example:9000/path"
	if err := validateConfiguredEndpoints(cfg); err == nil {
		t.Fatal("URL-shaped gRPC endpoint accepted")
	}
}

func TestValidateBootstrapEndpointsRequiresPublicAndGRPCForPush(t *testing.T) {
	cfg := &Config{Server: ServerConfig{GRPCPushMode: true}}
	if err := validateBootstrapEndpoints(cfg); err == nil {
		t.Fatal("missing public endpoint accepted")
	}
	cfg.ControlPlane.RESTPublic = "https://master.example"
	if err := validateBootstrapEndpoints(cfg); err == nil {
		t.Fatal("missing grpc endpoint accepted in push mode")
	}
	cfg.ControlPlane.GRPCControl = "master.example:9000"
	if err := validateBootstrapEndpoints(cfg); err == nil {
		t.Fatal("push mode accepted with GRPCPort=0")
	}
	cfg.Server.GRPCPort = 9000
	if err := validateBootstrapEndpoints(cfg); err != nil {
		t.Fatalf("valid endpoint set rejected: %v", err)
	}
}
