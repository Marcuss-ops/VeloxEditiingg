package config

import (
	"os"
	"strconv"
)

func loadRenderConfig() RenderConfig {
	c := RenderConfig{
		RemoteEngineURL:   os.Getenv("VELOX_REMOTE_ENGINE_URL"),
		RemoteEngineToken: os.Getenv("VELOX_REMOTE_ENGINE_TOKEN"),
	}
	c.RemoteEngineTimeoutMS = 60000
	if n, _ := strconv.Atoi(os.Getenv("VELOX_REMOTE_ENGINE_TIMEOUT_MS")); n > 0 {
		c.RemoteEngineTimeoutMS = n
	}
	c.RemoteEngineRetries = 3
	if n, _ := strconv.Atoi(os.Getenv("VELOX_REMOTE_ENGINE_RETRIES")); n > 0 {
		c.RemoteEngineRetries = n
	}
	c.RemoteEnginePollInterval = 30
	if n, _ := strconv.Atoi(os.Getenv("VELOX_REMOTE_ENGINE_POLL_INTERVAL")); n >= 5 {
		c.RemoteEnginePollInterval = n
	}
	return c
}
