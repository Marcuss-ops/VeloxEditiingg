package remoteengine

import "velox-server/internal/config"

// DefaultConfig returns the canonical defaults without reading process
// environment. Server bootstrap should pass config.Config.Render to callers.
func DefaultConfig() Config {
	return Config{TimeoutMS: 60000, Retries: 3}
}

// ConfigFromRuntime adapts the centrally parsed render configuration.
func ConfigFromRuntime(c config.RenderConfig) Config {
	return Config{
		URL:       c.RemoteEngineURL,
		Token:     c.RemoteEngineToken,
		TimeoutMS: c.RemoteEngineTimeoutMS,
		Retries:   c.RemoteEngineRetries,
	}
}
