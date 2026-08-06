package config

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
)

// EndpointURL is a validated HTTP(S) endpoint used by the control plane or
// the externally hosted frontend.
type EndpointURL string

// GRPCEndpoint is a validated host:port endpoint for the worker control
// channel. It intentionally does not derive from an HTTP URL.
type GRPCEndpoint string

// ControlPlaneEndpoints is the single endpoint contract shared by bootstrap
// and runtime consumers. REST public and REST internal are independent values;
// neither is derived from the other.
type ControlPlaneEndpoints struct {
	RESTInternal EndpointURL
	RESTPublic   EndpointURL
	GRPCControl  GRPCEndpoint
}

// FrontendEndpoints contains externally advertised UI endpoints. The master
// is headless, so this value is optional unless a consumer explicitly needs a
// frontend URL; it is never inferred from a control-plane endpoint.
type FrontendEndpoints struct {
	Public EndpointURL
}

func loadEndpointConfig(raw RawConfig) (ControlPlaneEndpoints, FrontendEndpoints) {
	return ControlPlaneEndpoints{
			RESTInternal: EndpointURL(firstNonBlank(raw.Get("VELOX_CONTROL_PLANE_REST_INTERNAL_URL"), raw.Get("VELOX_MASTER_SERVER_URL"), raw.Get("VELOX_REMOTE_WORKER_URL"))),
			RESTPublic:   EndpointURL(firstNonBlank(raw.Get("VELOX_CONTROL_PLANE_REST_PUBLIC_URL"), raw.Get("MASTER_PUBLIC_URL"), raw.Get("VELOX_MASTER_URL"), raw.Get("MASTER_URL"))),
			GRPCControl:  GRPCEndpoint(firstNonBlank(raw.Get("VELOX_CONTROL_PLANE_GRPC_URL"), raw.Get("VELOX_GRPC_MASTER_URL"))),
		}, FrontendEndpoints{
			Public: EndpointURL(firstNonBlank(raw.Get("VELOX_FRONTEND_PUBLIC_URL"), raw.Get("INSTAEDIT_FRONTEND_PUBLIC_URL"))),
		}
}

func firstNonBlank(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

// validateConfiguredEndpoints checks values when present. This lets manually
// constructed test Config values remain valid while malformed loaded values
// still fail before modules are built.
func validateConfiguredEndpoints(c *Config) error {
	if c == nil {
		return fmt.Errorf("config: nil Config")
	}
	if value := strings.TrimSpace(string(c.ControlPlane.RESTInternal)); value != "" {
		if err := validateHTTPSEndpoint(value, "VELOX_CONTROL_PLANE_REST_INTERNAL_URL"); err != nil {
			return err
		}
	}
	if value := strings.TrimSpace(string(c.ControlPlane.RESTPublic)); value != "" {
		if err := validateHTTPSEndpoint(value, "VELOX_CONTROL_PLANE_REST_PUBLIC_URL"); err != nil {
			return err
		}
	}
	if value := strings.TrimSpace(string(c.Frontend.Public)); value != "" {
		if err := validateHTTPSEndpoint(value, "VELOX_FRONTEND_PUBLIC_URL"); err != nil {
			return err
		}
	}
	if value := strings.TrimSpace(string(c.ControlPlane.GRPCControl)); value != "" {
		if err := validateGRPCEndpoint(value); err != nil {
			return err
		}
	}
	return nil
}

// validateBootstrapEndpoints enforces the endpoints required by the running
// master. REST public is always required for worker/API advertisement. The
// worker control endpoint is required only when push mode is enabled.
func validateBootstrapEndpoints(c *Config) error {
	if err := validateConfiguredEndpoints(c); err != nil {
		return err
	}
	if strings.TrimSpace(string(c.ControlPlane.RESTPublic)) == "" {
		return fmt.Errorf("config: control-plane REST public endpoint is required (VELOX_CONTROL_PLANE_REST_PUBLIC_URL)")
	}
	if c.Server.GRPCPushMode && strings.TrimSpace(string(c.ControlPlane.GRPCControl)) == "" {
		return fmt.Errorf("config: gRPC control endpoint is required when GRPCPushMode=true (VELOX_CONTROL_PLANE_GRPC_URL)")
	}
	return nil
}

func validateHTTPSEndpoint(raw, name string) error {
	parsed, err := url.ParseRequestURI(raw)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" {
		return fmt.Errorf("config: %s must be an absolute http(s) URL, got %q", name, raw)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("config: %s must use http or https, got %q", name, parsed.Scheme)
	}
	return nil
}

func validateGRPCEndpoint(raw string) error {
	if strings.Contains(raw, "://") || strings.ContainsAny(raw, "/?#") {
		return fmt.Errorf("config: VELOX_CONTROL_PLANE_GRPC_URL must be host:port, got %q", raw)
	}
	host, port, err := net.SplitHostPort(raw)
	if err != nil || strings.TrimSpace(host) == "" {
		return fmt.Errorf("config: VELOX_CONTROL_PLANE_GRPC_URL must be host:port, got %q", raw)
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return fmt.Errorf("config: VELOX_CONTROL_PLANE_GRPC_URL has invalid port in %q", raw)
	}
	return nil
}
