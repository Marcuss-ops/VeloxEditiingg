package config

import "os"

func loadRenderConfig() RenderConfig {
	c := RenderConfig{
		RemoteEngineURL:   os.Getenv("VELOX_REMOTE_ENGINE_URL"),
		RemoteEngineToken: os.Getenv("VELOX_REMOTE_ENGINE_TOKEN"),
	}
	c.RemoteEngineTimeoutMS = intFromEnv("VELOX_REMOTE_ENGINE_TIMEOUT_MS", 60000, 1)
	c.RemoteEngineRetries = intFromEnv("VELOX_REMOTE_ENGINE_RETRIES", 3, 1)
	c.RemoteEnginePollInterval = intFromEnv("VELOX_REMOTE_ENGINE_POLL_INTERVAL", 30, 5)
	return c
}
