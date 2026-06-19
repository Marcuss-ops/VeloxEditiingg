package config

import "os"

func loadAuthConfig() AuthConfig {
	c := AuthConfig{
		AdminToken: os.Getenv("VELOX_ADMIN_TOKEN"),
	}
	if c.AdminToken == "" {
		c.AdminToken = os.Getenv("MASTER_ADMIN_TOKEN")
	}
	return c
}
