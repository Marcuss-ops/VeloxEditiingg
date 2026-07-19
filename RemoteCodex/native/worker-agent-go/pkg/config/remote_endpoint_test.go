package config

import "testing"

func TestValidateRemoteMasterEndpointAllowsDevelopmentLoopback(t *testing.T) {
	c := devValidBase()
	c.MasterURL = "http://127.0.0.1:8180"
	c.ControlGRPCURL = "127.0.0.1:51851"
	if err := ValidateRemoteMasterEndpoint(c); err != nil {
		t.Fatalf("development loopback rejected: %v", err)
	}
}

func TestValidateRemoteMasterEndpointRejectsLoopbackOutsideDevelopment(t *testing.T) {
	for _, env := range []string{"staging", "production"} {
		t.Run(env, func(t *testing.T) {
			c := devValidBase()
			c.Environment = env
			c.MasterURL = "http://localhost:8180"
			c.ControlGRPCURL = "localhost:51851"
			if err := ValidateRemoteMasterEndpoint(c); err == nil {
				t.Fatal("loopback master unexpectedly accepted")
			}
		})
	}
}
